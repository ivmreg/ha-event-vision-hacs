"""Generic media frame extractor supporting validated images and video frame sampling.

Features:
- Pure Python (zero Home Assistant dependencies)
- Image input: Validates JPEG (SOI, SOF frame dimensions, EOI) and PNG (IHDR dimensions, IEND)
- Bounds enforcement: max 10MB source bytes, max 4096px width/height
- Video input: Samples up to 16 chronological frames using ffmpeg subprocess
- Robust subprocess reaping on timeout or cancellation (zero zombie processes, re-raises CancelledError)
- Generic: No assumptions about physical source, camera type, or event kind
"""

from __future__ import annotations

import asyncio
import logging
import shutil
from pathlib import Path

from ..const import (
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_MAX_FRAMES,
    MAX_FRAMES_BUDGET,
    MAX_IMAGE_DIMENSION,
    MAX_IMAGE_SOURCE_BYTES,
)
from .cache import FrameCache, compute_source_fingerprint
from .models import ExtractedFrame, FrameExtractionError

_LOGGER = logging.getLogger(__name__)

JPEG_SOI = b"\xff\xd8"
JPEG_EOI = b"\xff\xd9"
PNG_MAGIC = b"\x89PNG\r\n\x1a\n"
PNG_IEND = b"IEND"


def validate_image_bytes(data: bytes) -> str:
    """Validate image bytes, parse dimensions, enforce bounds, and return mime type.

    Raises FrameExtractionError if bytes do not conform to valid bounded JPEG or PNG.
    """
    if len(data) > MAX_IMAGE_SOURCE_BYTES:
        raise FrameExtractionError(
            "IMAGE_TOO_LARGE",
            f"Image payload size ({len(data)} bytes) exceeds maximum bound of {MAX_IMAGE_SOURCE_BYTES} bytes",
        )

    if len(data) < 16:
        raise FrameExtractionError("INVALID_IMAGE", "Image payload too small to be valid")

    try:
        import io

        from PIL import Image

        with Image.open(io.BytesIO(data)) as image:
            width, height = image.size
            if not (0 < width <= MAX_IMAGE_DIMENSION and 0 < height <= MAX_IMAGE_DIMENSION):
                raise FrameExtractionError("IMAGE_TOO_LARGE", "Image dimensions exceed bounds")
            fmt = image.format
            if fmt not in ("JPEG", "PNG"):
                raise FrameExtractionError("INVALID_IMAGE", "Expected JPEG or PNG")
            image.verify()
        # verify checks containers; load also checks the actual compressed pixels.
        with Image.open(io.BytesIO(data)) as image:
            image.load()
        return "image/jpeg" if fmt == "JPEG" else "image/png"
    except FrameExtractionError:
        raise
    except Exception as err:
        raise FrameExtractionError("INVALID_IMAGE", "Image decoding failed") from err


class FrameExtractor:
    """Extracts anchored frames from verified media paths."""

    def __init__(self, ffmpeg_bin: str | None = None, cache: FrameCache | None = None) -> None:
        self._ffmpeg_bin = ffmpeg_bin
        self._cache = cache

    @property
    def cache(self) -> FrameCache | None:
        """Return attached FrameCache instance if configured."""
        return self._cache

    def _resolve_ffmpeg(self) -> str | None:
        """Resolve valid ffmpeg executable path."""
        if self._ffmpeg_bin:
            return shutil.which(self._ffmpeg_bin)
        return shutil.which("ffmpeg")

    async def extract_from_path(
        self,
        media_path: Path,
        kind: str | None = None,
        mime_type: str | None = None,
        duration: float | None = None,
        max_frames: int = DEFAULT_MAX_FRAMES,
        timestamps: list[float] | None = None,
        timeout_seconds: float = DEFAULT_EXTRACTION_TIMEOUT,
        media_id: str | None = None,
        timestamp_quality: str = "unknown",
        offset_quality: str = "exact",
        media_revision: int = 1,
        source_fingerprint: str | None = None,
    ) -> list[ExtractedFrame]:
        """Extract validated frames from an existing media file path.

        Bounded to max_frames <= 16.
        """
        if not await asyncio.to_thread(media_path.is_file):
            raise FrameExtractionError("MEDIA_UNAVAILABLE", f"Media file not found: {media_path.name}")

        max_frames = max(1, min(max_frames, MAX_FRAMES_BUDGET))

        stat_res = await asyncio.to_thread(media_path.stat)
        computed_fp = compute_source_fingerprint(media_path, stat_res)
        if source_fingerprint is not None and source_fingerprint != computed_fp:
            _LOGGER.debug("Caller source_fingerprint mismatch, using validated on-disk fingerprint")
        source_fingerprint = computed_fp

        # Determine if media is an image
        is_image = False
        if kind == "image":
            is_image = True
        elif mime_type in ("image/jpeg", "image/png", "image/jpg"):
            is_image = True
        elif media_path.suffix.lower() in (".jpg", ".jpeg", ".png"):
            is_image = True

        if is_image:
            if stat_res.st_size > MAX_IMAGE_SOURCE_BYTES:
                raise FrameExtractionError("IMAGE_TOO_LARGE", "Image source exceeds byte limit")
            return await self._extract_image(
                media_path,
                media_id=media_id,
                timestamp_quality=timestamp_quality,
                offset_quality=offset_quality,
                media_revision=media_revision,
                source_fingerprint=source_fingerprint,
            )

        # Video frame extraction
        return await self._extract_video_frames(
            media_path,
            duration=duration,
            max_frames=max_frames,
            timestamps=timestamps,
            timeout_seconds=timeout_seconds,
            media_id=media_id,
            timestamp_quality=timestamp_quality,
            offset_quality=offset_quality,
            media_revision=media_revision,
            source_fingerprint=source_fingerprint,
        )

    async def _extract_image(
        self,
        media_path: Path,
        media_id: str | None = None,
        timestamp_quality: str = "unknown",
        offset_quality: str = "exact",
        media_revision: int = 1,
        source_fingerprint: str | None = None,
    ) -> list[ExtractedFrame]:
        """Read and validate image file bytes with optional cache reuse."""
        cache_key = None
        if self._cache and source_fingerprint:
            cache_key = self._cache.make_key(source_fingerprint, media_revision, 0.0)
            cached = await self._cache.get(cache_key)
            if cached is not None:
                return [
                    ExtractedFrame(
                        data=cached.data,
                        mime_type=cached.mime_type,
                        media_id=media_id,
                        offset_seconds=0.0,
                        timestamp_quality=timestamp_quality,
                        offset_quality=offset_quality,
                    )
                ]

        try:
            raw_bytes = await asyncio.to_thread(media_path.read_bytes)
        except OSError as err:
            raise FrameExtractionError("MEDIA_UNAVAILABLE", f"Cannot read media file: {err}") from err

        detected_mime = await asyncio.to_thread(validate_image_bytes, raw_bytes)

        if self._cache and cache_key:
            await self._cache.put(
                key=cache_key,
                frame_data=raw_bytes,
                mime_type=detected_mime,
                canonical_seek=0.0,
                source_fingerprint=source_fingerprint,
                media_revision=media_revision,
            )

        return [
            ExtractedFrame(
                data=raw_bytes,
                mime_type=detected_mime,
                media_id=media_id,
                offset_seconds=0.0,
                timestamp_quality=timestamp_quality,
                offset_quality=offset_quality,
            )
        ]

    async def _extract_video_frames(
        self,
        media_path: Path,
        duration: float | None = None,
        max_frames: int = DEFAULT_MAX_FRAMES,
        timestamps: list[float] | None = None,
        timeout_seconds: float = DEFAULT_EXTRACTION_TIMEOUT,
        media_id: str | None = None,
        timestamp_quality: str = "unknown",
        offset_quality: str = "exact",
        media_revision: int = 1,
        source_fingerprint: str | None = None,
    ) -> list[ExtractedFrame]:
        """Extract chronological frames from a video file using ffmpeg with cache and stampede guard."""
        ffmpeg_exec = await asyncio.to_thread(self._resolve_ffmpeg)
        if not ffmpeg_exec:
            raise FrameExtractionError("FFMPEG_NOT_FOUND", "ffmpeg binary not found; failing closed")

        if timestamps is not None:
            ts_list = timestamps[:max_frames]
        else:
            ts_list = self._calculate_anchors(duration=duration, max_frames=max_frames)

        frames: list[ExtractedFrame] = []

        for ts in ts_list:
            if self._cache and source_fingerprint:
                cache_key = self._cache.make_key(source_fingerprint, media_revision, ts)

                async def _extract_fn(t_seek: float = ts) -> bytes | None:
                    assert ffmpeg_exec is not None
                    return await self._run_ffmpeg_extract_frame(
                        ffmpeg_exec, media_path, t_seek, timeout_seconds=timeout_seconds
                    )

                frame_data = await self._cache.get_or_extract(
                    key=cache_key,
                    extract_fn=_extract_fn,
                    mime_type="image/jpeg",
                    canonical_seek=ts,
                    source_fingerprint=source_fingerprint,
                    media_revision=media_revision,
                )
            else:
                frame_data = await self._run_ffmpeg_extract_frame(
                    ffmpeg_exec, media_path, ts, timeout_seconds=timeout_seconds
                )

            if frame_data:
                frames.append(
                    ExtractedFrame(
                        data=frame_data,
                        mime_type="image/jpeg",
                        media_id=media_id,
                        offset_seconds=ts,
                        timestamp_quality=timestamp_quality,
                        offset_quality=offset_quality,
                    )
                )

        if not frames:
            raise FrameExtractionError("FRAME_EXTRACTION_FAILED", "Failed to extract any valid frames from video")

        return frames

    def _calculate_anchors(self, duration: float | None, max_frames: int) -> list[float]:
        """Calculate chronological timestamps bounded <= 16 frames."""
        dur = duration if duration and duration > 0 else 1.0
        max_frames = min(max_frames, MAX_FRAMES_BUDGET)

        if max_frames == 1:
            return [min(0.5, dur * 0.1)]

        step = dur / (max_frames + 1)
        return [round((i + 1) * step, 2) for i in range(max_frames)]

    async def _run_ffmpeg_extract_frame(
        self,
        ffmpeg_exec: str,
        media_path: Path,
        timestamp: float,
        timeout_seconds: float = DEFAULT_EXTRACTION_TIMEOUT,
    ) -> bytes | None:
        """Run ffmpeg subprocess to extract one frame, strictly reaping on timeout/cancel."""
        cmd = [
            ffmpeg_exec,
            "-y",
            "-ss",
            f"{timestamp:.2f}",
            "-i",
            str(media_path),
            "-vf",
            "scale=1280:1280:force_original_aspect_ratio=decrease",
            "-frames:v",
            "1",
            "-f",
            "image2",
            "-q:v",
            "3",
            "pipe:1",
        ]
        proc = None
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=timeout_seconds)
            if proc.returncode == 0 and stdout and stdout.startswith(JPEG_SOI):
                return stdout
        except asyncio.CancelledError:
            # Re-raise CancelledError; child process is reaped in finally
            raise
        except TimeoutError:
            _LOGGER.warning("FFmpeg frame extraction timed out at %ss", timestamp)
        except Exception as err:
            _LOGGER.warning("FFmpeg frame extraction process error: %s", err)
        finally:
            if proc and proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
                try:
                    await proc.wait()
                except Exception:
                    pass
        return None
