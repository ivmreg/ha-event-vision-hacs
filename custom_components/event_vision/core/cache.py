"""Bounded in-memory LRU cache for extracted video and image frames.

Strict boundary: Pure Python, zero Home Assistant imports, zero network calls.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

DEFAULT_EXTRACTION_SETTINGS = "1280:1280:decrease:q3"


@dataclass(frozen=True)
class CachedFrame:
    """Immutable cached frame payload and canonical sampling metadata."""

    data: bytes
    mime_type: str
    canonical_seek: float
    created_at: float
    size_bytes: int
    source_fingerprint: str | None = None
    media_revision: int | None = None
    extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS


def is_valid_frame_bytes(data: bytes, mime_type: str = "image/jpeg") -> bool:
    """Validate that raw bytes represent a plausible image payload."""
    if not isinstance(data, (bytes, bytearray)) or len(data) < 16:
        return False
    if "jpeg" in mime_type or "jpg" in mime_type:
        return data[:2] == b"\xff\xd8"
    if "png" in mime_type:
        return data[:8] == b"\x89PNG\r\n\x1a\n"
    if "webp" in mime_type:
        return len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP"
    return True


def compute_source_fingerprint(
    media_path: Path,
    stat_result: Any,
    media_id: str | None = None,
) -> str:
    """Compute deterministic cryptographic fingerprint of media file identity and content state.

    Uses resolved canonical path hash, file size, mtime_ns, ctime_ns, inode/dev, and media_id.
    Does NOT leak directory paths or camera/entity IDs in diagnostics.
    """
    try:
        resolved = str(media_path.resolve())
    except Exception:
        resolved = str(media_path)
    path_hash = hashlib.sha256(resolved.encode("utf-8")).hexdigest()[:16]
    st_size = getattr(stat_result, "st_size", 0)
    st_mtime_ns = getattr(stat_result, "st_mtime_ns", 0)
    st_ctime_ns = getattr(stat_result, "st_ctime_ns", 0)
    st_ino = getattr(stat_result, "st_ino", 0)
    st_dev = getattr(stat_result, "st_dev", 0)
    m_id = str(media_id) if media_id else ""
    ident = f"{path_hash}:{st_size}:{st_mtime_ns}:{st_ctime_ns}:{st_ino}:{st_dev}:{m_id}"
    return hashlib.sha256(ident.encode("utf-8")).hexdigest()[:16]


def format_canonical_seek(timestamp: float) -> str:
    """Format canonical ffmpeg seek timestamp to 2 decimal places."""
    return f"{round(float(timestamp), 2):.2f}"


class FrameCache:
    """Thread-safe bounded in-memory LRU frame cache with TTL and single-flight stampede protection."""

    def __init__(
        self,
        max_bytes: int = 64 * 1024 * 1024,
        ttl_seconds: float = 300.0,
    ) -> None:
        self._max_bytes = max(0, int(max_bytes))
        self._ttl_seconds = max(0.0, float(ttl_seconds))
        self._entries: OrderedDict[str, CachedFrame] = OrderedDict()
        self._current_bytes: int = 0
        self._hits: int = 0
        self._misses: int = 0
        self._evictions: int = 0
        self._epoch: int = 0
        self._closed: bool = False
        self._in_flight: dict[str, asyncio.Future[bytes | None]] = {}
        self._lock = asyncio.Lock()

    @property
    def enabled(self) -> bool:
        """Return True if cache is enabled (byte budget > 0)."""
        return self._max_bytes > 0

    @property
    def max_bytes(self) -> int:
        """Return configured maximum byte budget."""
        return self._max_bytes

    @property
    def ttl_seconds(self) -> float:
        """Return configured TTL in seconds."""
        return self._ttl_seconds

    @staticmethod
    def make_key(
        source_fingerprint: str,
        media_revision: int,
        canonical_seek: float,
        extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS,
    ) -> str:
        """Build deterministic cache key."""
        seek_str = format_canonical_seek(canonical_seek)
        return f"{source_fingerprint}:{media_revision}:{seek_str}:{extraction_settings}"

    async def get(self, key: str) -> CachedFrame | None:
        """Retrieve cached frame if present and not expired."""
        if not self.enabled:
            self._misses += 1
            return None

        async with self._lock:
            frame = self._entries.get(key)
            if frame is None:
                self._misses += 1
                return None

            now = time.monotonic()
            if self._ttl_seconds > 0 and (now - frame.created_at) > self._ttl_seconds:
                # Expired entry
                self._entries.pop(key, None)
                self._current_bytes -= frame.size_bytes
                self._evictions += 1
                self._misses += 1
                return None

            # LRU hit: move to end
            self._entries.move_to_end(key)
            self._hits += 1
            return frame

    def _put_locked(
        self,
        key: str,
        frame_data: bytes,
        mime_type: str,
        canonical_seek: float,
        source_fingerprint: str | None = None,
        media_revision: int | None = None,
        extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS,
    ) -> bool:
        size = len(frame_data)
        if size > self._max_bytes or self._closed:
            return False

        # If entry already exists, subtract its size
        existing = self._entries.pop(key, None)
        if existing:
            self._current_bytes -= existing.size_bytes

        # Evict oldest entries until within budget
        while (self._current_bytes + size) > self._max_bytes and self._entries:
            _, oldest = self._entries.popitem(last=False)
            self._current_bytes -= oldest.size_bytes
            self._evictions += 1

        now = time.monotonic()
        entry = CachedFrame(
            data=frame_data,
            mime_type=mime_type,
            canonical_seek=round(float(canonical_seek), 2),
            created_at=now,
            size_bytes=size,
            source_fingerprint=source_fingerprint,
            media_revision=media_revision,
            extraction_settings=extraction_settings,
        )
        self._entries[key] = entry
        self._current_bytes += size
        return True

    async def put(
        self,
        key: str,
        frame_data: bytes,
        mime_type: str,
        canonical_seek: float,
        source_fingerprint: str | None = None,
        media_revision: int | None = None,
        extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS,
    ) -> bool:
        """Store validated raw frame bytes in LRU cache respecting byte bound."""
        if not self.enabled or self._closed:
            return False

        if not is_valid_frame_bytes(frame_data, mime_type):
            return False

        size = len(frame_data)
        if size > self._max_bytes:
            return False

        async with self._lock:
            return self._put_locked(
                key=key,
                frame_data=frame_data,
                mime_type=mime_type,
                canonical_seek=canonical_seek,
                source_fingerprint=source_fingerprint,
                media_revision=media_revision,
                extraction_settings=extraction_settings,
            )

    async def get_or_extract(
        self,
        key: str,
        extract_fn: Callable[[], Awaitable[bytes | None]],
        mime_type: str,
        canonical_seek: float,
        source_fingerprint: str | None = None,
        media_revision: int | None = None,
        extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS,
    ) -> bytes | None:
        """Retrieve from cache or execute single-flight extraction coalescing concurrent calls."""
        if not self.enabled or self._closed:
            raw = await extract_fn()
            return raw if (raw and is_valid_frame_bytes(raw, mime_type)) else None

        # Fast path: check existing cached entry
        cached = await self.get(key)
        if cached is not None:
            return cached.data

        # Single-flight stampede coalescing with waiter protection
        entry_epoch: int
        leader = False
        future: asyncio.Future[bytes | None]

        async with self._lock:
            entry_epoch = self._epoch
            # Recheck cache after acquiring lock
            recheck = self._entries.get(key)
            if recheck is not None:
                now = time.monotonic()
                if self._ttl_seconds <= 0 or (now - recheck.created_at) <= self._ttl_seconds:
                    self._entries.move_to_end(key)
                    self._hits += 1
                    return recheck.data

            if key in self._in_flight:
                future = self._in_flight[key]
            else:
                loop = asyncio.get_running_loop()
                future = loop.create_future()
                self._in_flight[key] = future
                leader = True

        if not leader:
            # Waiter: await the leader's extraction result shielded from cancellation
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # Waiter cancellation must NOT poison the leader or other waiters
                raise
            except Exception:
                return None

        # Leader executes extraction
        try:
            result = await extract_fn()
            valid = (result is not None) and is_valid_frame_bytes(result, mime_type)
            final_data = result if valid else None

            async with self._lock:
                # Epoch guard: ensure clear() did not happen during extraction
                if self._epoch == entry_epoch and not self._closed and final_data is not None:
                    self._put_locked(
                        key=key,
                        frame_data=final_data,
                        mime_type=mime_type,
                        canonical_seek=canonical_seek,
                        source_fingerprint=source_fingerprint,
                        media_revision=media_revision,
                        extraction_settings=extraction_settings,
                    )

            if not future.done():
                future.set_result(final_data)
            return final_data
        except asyncio.CancelledError as err:
            if not future.done():
                future.set_exception(err)
                future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
            raise
        except Exception as err:
            if not future.done():
                future.set_exception(err)
                future.add_done_callback(lambda f: f.exception() if not f.cancelled() else None)
            raise
        finally:
            async with self._lock:
                if self._in_flight.get(key) is future:
                    self._in_flight.pop(key, None)

    async def get_cached_anchors(
        self,
        source_fingerprint: str,
        media_revision: int,
        extraction_settings: str = DEFAULT_EXTRACTION_SETTINGS,
    ) -> list[tuple[float, bytes, str]]:
        """Return non-expired cached (canonical_seek, data, mime_type) tuples matching extraction settings."""
        if not self.enabled or self._closed:
            return []

        now = time.monotonic()
        results: list[tuple[float, bytes, str]] = []

        async with self._lock:
            for entry in self._entries.values():
                if (
                    entry.source_fingerprint == source_fingerprint
                    and entry.media_revision == media_revision
                    and entry.extraction_settings == extraction_settings
                ):
                    if self._ttl_seconds > 0 and (now - entry.created_at) > self._ttl_seconds:
                        continue
                    results.append((entry.canonical_seek, entry.data, entry.mime_type))

            if results:
                self._hits += len(results)
            else:
                self._misses += 1

        results.sort(key=lambda x: x[0])
        return results

    @staticmethod
    def select_compatible_subset(
        cached_anchors: list[tuple[float, bytes, str]],
        needed_count: int,
    ) -> list[tuple[float, bytes, str]]:
        """Select a monotonic chronological subset of cached anchors matching needed_count.

        Preserves chronological ordering and distributes sampling across the available anchors.
        """
        if not cached_anchors or needed_count <= 0:
            return []
        if len(cached_anchors) <= needed_count:
            return list(cached_anchors)

        n = len(cached_anchors)
        k = needed_count
        if k == 1:
            # Pick middle anchor
            return [cached_anchors[n // 2]]

        indices = [round(i * (n - 1) / (k - 1)) for i in range(k)]
        # Ensure strictly ascending unique indices in rare rounding edge cases
        unique_indices: list[int] = []
        for idx in indices:
            if not unique_indices or idx > unique_indices[-1]:
                unique_indices.append(idx)
        # If deduplication dropped any, pad up to k
        if len(unique_indices) < k:
            step = n / k
            unique_indices = [int(i * step) for i in range(k)]

        return [cached_anchors[i] for i in unique_indices]

    async def clear(self) -> None:
        """Clear all entries, advance epoch, and cancel any in-flight requests."""
        async with self._lock:
            self._epoch += 1
            self._entries.clear()
            self._current_bytes = 0
            for fut in list(self._in_flight.values()):
                if not fut.done():
                    fut.cancel()
            self._in_flight.clear()

    async def close(self) -> None:
        """Close cache and mark as closed."""
        async with self._lock:
            self._closed = True
        await self.clear()

    async def get_diagnostics(self) -> dict[str, Any]:
        """Return safe, non-sensitive cache metrics."""
        async with self._lock:
            return {
                "enabled": self.enabled,
                "entries": len(self._entries),
                "bytes": self._current_bytes,
                "max_bytes": self._max_bytes,
                "ttl_seconds": self._ttl_seconds,
                "hits": self._hits,
                "misses": self._misses,
                "evictions": self._evictions,
                "in_flight": len(self._in_flight),
            }
