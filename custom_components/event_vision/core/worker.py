"""Durable AnalysisWorker and Coordinator for Event Vision pure core.

Features:
- Pure Python (zero Home Assistant dependencies)
- Media pin lease acquired with bound <= 300s and strictly released in finally
- Reserve annotation with generation tracking and 180s deadline
- CAS annotation commits for status 'processing', 'complete', 'failed', 'skipped'
- Rejection of stale generations or changed media revisions
- Bounded concurrency with asyncio.Semaphore
- Retry backoff (>= 1.0s) and permanent vs retryable failure classification
- Immediate recording generation invalidation on manual retry before ACK
- Clean task lifecycle shutdown and rebind support
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import replace
from typing import Any

from ..const import (
    ANNOTATION_NAMESPACE,
    BASE_BACKOFF_SECONDS,
    CONSUMER_ID,
    LEASE_PIN_SECONDS,
    MAX_ATTEMPTS,
    MAX_FRAMES,
    MAX_PROCESS_DEADLINE,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PROCESSING,
    STATUS_SKIPPED,
)
from .extractor import FrameExtractor
from .gemini import GeminiClient
from .models import (
    AnalysisAnnotation,
    AnalysisError,
    AnalysisJob,
    ExtractedFrame,
    compute_profile_digest,
    format_iso,
    sanitize_error_code,
    utc_now,
)
from .store import AnalysisJobStore

_LOGGER = logging.getLogger(__name__)


class AnalysisWorker:
    """Background worker executing durable analysis jobs with media leases and CAS."""

    def __init__(
        self,
        job_store: AnalysisJobStore,
        gemini_client: GeminiClient | None,
        frame_extractor: FrameExtractor,
        recording_api: Any,
        worker_id: str = "worker-1",
        concurrency: int = 1,
        poll_interval_seconds: float = 3.0,
        prompt_instructions: str | None = None,
        model: str = "gemini-flash-lite-latest",
    ) -> None:
        self._job_store = job_store
        self._gemini = gemini_client
        self._model = (
            gemini_client.model
            if (gemini_client and hasattr(gemini_client, "model") and gemini_client.model)
            else model
        )
        self._extractor = frame_extractor
        self._rec_api = recording_api
        self._worker_id = worker_id
        self._poll_interval = poll_interval_seconds
        self._prompt_instructions = prompt_instructions
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._wake_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(max(1, min(concurrency, 4)))
        self._background_tasks: set[asyncio.Task[None]] = set()
        self.event_locks: dict[str, asyncio.Lock] = {}

    def event_lock(self, event_id: str) -> asyncio.Lock:
        """Serialize short queue/reservation transitions with manual retries."""
        return self.event_locks.setdefault(event_id, asyncio.Lock())

    @property
    def worker_id(self) -> str:
        """Return worker identifier."""
        return self._worker_id

    def set_recording_api(self, recording_api: Any) -> None:
        """Update recording API facade instance cleanly."""
        self._rec_api = recording_api

    def notify(self) -> None:
        """Wake up worker loop immediately."""
        self._wake_event.set()

    def start(self) -> None:
        """Start worker background loop."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        """Stop worker loop cleanly and await task cancellation."""
        self._running = False
        self._wake_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        for bg_task in list(self._background_tasks):
            if not bg_task.done():
                bg_task.cancel()
        if self._background_tasks:
            await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()

    async def drain(self) -> None:
        """Wait for in-flight background analysis tasks to complete."""
        while self._background_tasks:
            await asyncio.gather(*list(self._background_tasks), return_exceptions=True)

    async def _run_loop(self) -> None:
        while self._running:
            try:
                processed = await self.process_next_job()
                if not processed:
                    try:
                        await asyncio.wait_for(self._wake_event.wait(), timeout=self._poll_interval)
                    except TimeoutError:
                        pass
                    self._wake_event.clear()
            except asyncio.CancelledError:
                break
            except Exception as err:
                _LOGGER.error("Unexpected error in analysis worker loop: %s", sanitize_error_code(err))
                await asyncio.sleep(self._poll_interval)

    async def process_next_job(self) -> bool:
        """Attempt to lease and execute one eligible job."""
        if not self._rec_api.store.is_open:
            return False
        if not self._semaphore.locked():
            job = await self._job_store.lease_job(self._worker_id, lease_seconds=MAX_PROCESS_DEADLINE)
            if not job:
                return False

            task = asyncio.create_task(self._guarded_execute(job))
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            return True
        return False

    async def _guarded_execute(self, job: AnalysisJob) -> None:
        async with self._semaphore:
            try:
                await asyncio.wait_for(self._execute_job(job), timeout=MAX_PROCESS_DEADLINE)
            except TimeoutError:
                _LOGGER.warning("Job %s timed out after %ss deadline", job.event_id, MAX_PROCESS_DEADLINE)
                await self._handle_job_failure(job, AnalysisError("TIMEOUT", "Execution deadline exceeded", retryable=True))
            except asyncio.CancelledError:
                _LOGGER.info("Job %s cancelled during execution; leaving recoverable", job.event_id)
                raise
            except Exception as err:
                _LOGGER.error("Execution error on job %s: %s", job.event_id, sanitize_error_code(err))
                await self._handle_job_failure(job, err)

    async def _execute_job(self, job: AnalysisJob) -> None:
        """Execute single analysis job with media lease pin and CAS protection."""
        if not await self._job_store.is_job_current(job.id, job.generation):
            _LOGGER.info("Job %s (gen %s) is no longer current; aborting execution", job.event_id, job.generation)
            return

        lease_pin = None
        try:
            # 1. Acquire media lease pin with bound 300s
            lease_pin = await self._rec_api.store.pin_media_lease(
                event_id=job.event_id,
                lease_seconds=LEASE_PIN_SECONDS,
                reason="event_vision analysis",
            )

            # 2. Annotation generation handling
            # Use job's reserved generation; fallback to reserve only if unset
            async with self.event_lock(job.event_id):
                if not await self._job_store.is_job_current(job.id, job.generation):
                    return
                # Every attempt receives a fresh reservation. The shared lock prevents
                # an old attempt from invalidating a concurrently accepted manual retry.
                annotation_gen = await self._rec_api.store.reserve_annotation(
                    event_id=job.event_id,
                    namespace=ANNOTATION_NAMESPACE,
                    expected_media_revision=job.media_revision,
                    worker_id=self._worker_id,
                    ttl_seconds=180.0,
                )
                if annotation_gen is None:
                    await self._job_store.supersede_job(job.id, job.generation)
                    return
                await self._job_store.update_job_annotation_generation(job.id, job.generation, annotation_gen)
                job = replace(job, annotation_generation=annotation_gen)
                proc_annotation = AnalysisAnnotation(
                    status=STATUS_PROCESSING, media_revision=job.media_revision,
                    generation=annotation_gen, model=self._model,
                    updated_at=format_iso(utc_now()),
                )
                if not await self._rec_api.store.set_annotation_cas(
                    event_id=job.event_id,
                    expected_media_revision=job.media_revision,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=proc_annotation.to_dict(),
                    expected_generation=annotation_gen,
                ):
                    return

            if self._gemini is None:
                raise AnalysisError("MISSING_API_KEY", "Gemini API key is not configured", retryable=False)

            # 4. Fetch event and inspect media items
            event = await self._rec_api.get_event(job.event_id)
            if not event or event.media_revision != job.media_revision:
                _LOGGER.warning("Event %s changed media revision mid-run", job.event_id)
                await self._job_store.supersede_job(job.id, job.generation)
                return

            available_media = [m for m in event.media if getattr(m, "available", True)]
            if not available_media:
                if not await self._job_store.is_job_current(job.id, job.generation):
                    return
                skip_annotation = AnalysisAnnotation(
                    status=STATUS_SKIPPED,
                    media_revision=job.media_revision,
                    generation=annotation_gen,
                    error_code="MEDIA_UNAVAILABLE",
                    model=self._gemini.model,
                    updated_at=format_iso(utc_now()),
                )
                await self._rec_api.store.set_annotation_cas(
                    event_id=job.event_id,
                    expected_media_revision=job.media_revision,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=skip_annotation.to_dict(),
                    expected_generation=annotation_gen,
                )
                await self._job_store.skip_job(job.id, job.generation, "MEDIA_UNAVAILABLE")
                return

            # 5. Extract frames strictly across all media items chronologically (bounded <= 6)
            num_media = len(available_media)
            accumulated_frames: list[ExtractedFrame] = []

            if num_media <= MAX_FRAMES:
                frames_per_item = max(1, MAX_FRAMES // num_media)
                for media_item in available_media:
                    if len(accumulated_frames) >= MAX_FRAMES:
                        break
                    try:
                        resolved_path = await self._rec_api.resolve_media(job.event_id, media_item.id)
                    except Exception as err:
                        _LOGGER.warning(
                            "Failed to resolve media %s for event %s: %s",
                            media_item.id,
                            job.event_id,
                            sanitize_error_code(err),
                        )
                        continue
                    needed = MAX_FRAMES - len(accumulated_frames)
                    try:
                        frames = await self._extractor.extract_from_path(
                            media_path=resolved_path,
                            kind=getattr(media_item, "kind", None),
                            mime_type=getattr(media_item, "mime_type", None),
                            duration=getattr(media_item, "duration", None),
                            max_frames=min(frames_per_item, needed),
                        )
                        accumulated_frames.extend(frames)
                    except Exception as err:
                        _LOGGER.warning("Frame extraction error on media %s: %s", media_item.id, sanitize_error_code(err))
            else:
                # Distribute 6 anchors chronologically across all media, preserving first and last
                anchor_indices = [
                    round(i * (num_media - 1) / (MAX_FRAMES - 1))
                    for i in range(MAX_FRAMES)
                ]
                selected_indices = list(dict.fromkeys(anchor_indices))
                for idx in selected_indices:
                    if len(accumulated_frames) >= MAX_FRAMES:
                        break
                    media_item = available_media[idx]
                    try:
                        resolved_path = await self._rec_api.resolve_media(job.event_id, media_item.id)
                    except Exception as err:
                        _LOGGER.warning(
                            "Failed to resolve media %s for event %s: %s",
                            media_item.id,
                            job.event_id,
                            sanitize_error_code(err),
                        )
                        continue
                    needed = MAX_FRAMES - len(accumulated_frames)
                    try:
                        frames = await self._extractor.extract_from_path(
                            media_path=resolved_path,
                            kind=getattr(media_item, "kind", None),
                            mime_type=getattr(media_item, "mime_type", None),
                            duration=getattr(media_item, "duration", None),
                            max_frames=1,
                        )
                        accumulated_frames.extend(frames[:needed])
                    except Exception as err:
                        _LOGGER.warning("Frame extraction error on media %s: %s", media_item.id, sanitize_error_code(err))

            if not accumulated_frames:
                raise AnalysisError("FRAME_EXTRACTION_FAILED", "No valid frames could be extracted", retryable=False)

            # 6. Call direct Gemini REST generateContent
            result = await self._gemini.generate_analysis(
                frames=accumulated_frames,
                prompt_instructions=self._prompt_instructions,
                timeout_seconds=60.0,
            )

            # 7. Post CAS complete state
            if not await self._job_store.is_job_current(job.id, job.generation):
                _LOGGER.warning("Job %s superseded before complete commit; skipping CAS", job.event_id)
                return

            complete_annotation = AnalysisAnnotation(
                status=STATUS_COMPLETE,
                media_revision=job.media_revision,
                generation=annotation_gen,
                summary=result.summary,
                activity=result.activity,
                objects=result.objects,
                model=self._gemini.model,
                updated_at=format_iso(utc_now()),
            )

            committed = await self._rec_api.store.set_annotation_cas(
                event_id=job.event_id,
                expected_media_revision=job.media_revision,
                namespace=ANNOTATION_NAMESPACE,
                payload=complete_annotation.to_dict(),
                expected_generation=annotation_gen,
            )

            if committed:
                await self._job_store.complete_job(job.id, job.generation)
                _LOGGER.info(
                    "Successfully analyzed and committed event %s (media_rev=%s, gen=%s)",
                    job.event_id,
                    job.media_revision,
                    annotation_gen,
                )
            else:
                _LOGGER.warning("CAS commit rejected on completion for %s; stale generation or media revision", job.event_id)
                await self._job_store.supersede_job(job.id, job.generation)

        except Exception as err:
            if not self._rec_api.store.is_open:
                await self._job_store.defer_job(job.id, job.generation)
            else:
                await self._handle_job_failure(job, err)
        finally:
            # 8. Release media lease pin in finally block
            if lease_pin and hasattr(lease_pin, "lease_token"):
                try:
                    await self._rec_api.store.release_media_pin(lease_pin.lease_token)
                except Exception as err:
                    _LOGGER.warning("Error releasing media pin for %s: %s", job.event_id, sanitize_error_code(err))

    async def _handle_job_failure(self, job: AnalysisJob, err: Exception) -> None:
        """Handle failure, classify permanent vs retryable, and update CAS if permanent/exhausted."""
        retryable = True
        error_code = "INTERNAL_ERROR"

        if isinstance(err, AnalysisError):
            retryable = err.retryable
            error_code = err.code
        elif isinstance(err, TimeoutError):
            retryable = True
            error_code = "TIMEOUT"

        clean_code = sanitize_error_code(error_code)
        backoff = BASE_BACKOFF_SECONDS * (2**job.attempts)

        # Check if job is still current in our store
        if not await self._job_store.is_job_current(job.id, job.generation):
            _LOGGER.info("Job %s was superseded; ignoring failure handler", job.event_id)
            return

        current = await self._job_store.get_job(job.event_id, job.media_revision, job.profile_digest)
        if current is None or current.generation != job.generation:
            return
        job = current
        # Update local job store
        await self._job_store.fail_job(
            job.id,
            job.generation,
            error_code=clean_code,
            retryable=retryable,
            backoff_seconds=backoff,
        )

        # If permanent or exhausted 3 attempts, update recording CAS to status='failed'
        # NEVER reserve a new generation from failure handler!
        # Use originally claimed job.annotation_generation with CAS
        if not retryable or (job.attempts + 1) >= MAX_ATTEMPTS:
            ann_gen = job.annotation_generation
            if ann_gen and ann_gen > 0:
                try:
                    fail_ann = AnalysisAnnotation(
                        status=STATUS_FAILED,
                        media_revision=job.media_revision,
                        generation=ann_gen,
                        error_code=clean_code,
                        model=self._gemini.model,
                        updated_at=format_iso(utc_now()),
                    )
                    await self._rec_api.store.set_annotation_cas(
                        event_id=job.event_id,
                        expected_media_revision=job.media_revision,
                        namespace=ANNOTATION_NAMESPACE,
                        payload=fail_ann.to_dict(),
                        expected_generation=ann_gen,
                    )
                except Exception as post_err:
                    _LOGGER.warning("Could not post failed annotation for %s: %s", job.event_id, sanitize_error_code(post_err))


class VisionCoordinator:
    """Manages catchup scanning, subscription dispatch, and manual retry for event_vision."""

    def __init__(
        self,
        job_store: AnalysisJobStore,
        worker: AnalysisWorker,
        recording_api: Any,
        auto_analyze: bool = True,
        model: str = "gemini-flash-lite-latest",
        prompt_instructions: str | None = None,
    ) -> None:
        self.job_store = job_store
        self.worker = worker
        self.rec_api = recording_api
        self.auto_analyze = auto_analyze
        self.model = model
        self.prompt_instructions = prompt_instructions
        self._unsub_recording: Any = None
        self._unsub_consumer: Any = None
        self._notification_tasks: set[asyncio.Task[None]] = set()

    def _get_event_lock(self, event_id: str) -> asyncio.Lock:
        return self.worker.event_lock(event_id)

    @property
    def profile_digest(self) -> str:
        """Return profile digest for current settings."""
        return compute_profile_digest(self.model, self.prompt_instructions)

    def bind_recording_api(self, rec_api: Any, available: bool = True) -> None:
        """Bind or rebind recording API facade and setup subscriptions cleanly."""
        self.unbind_recording_api()
        self.rec_api = rec_api
        self.worker.set_recording_api(rec_api)

        async def async_consumer_retry(event_id: str) -> bool:
            return await self.async_manual_analyze(event_id)

        self._unsub_consumer = rec_api.register_consumer(
            consumer_id=CONSUMER_ID,
            retry_callback=async_consumer_retry,
            available=available,
        )

        def _on_event_change(event_id: str, revision: int) -> None:
            task = asyncio.create_task(self.async_handle_event_notification(event_id, revision))
            self._notification_tasks.add(task)
            task.add_done_callback(self._notification_tasks.discard)

        self._unsub_recording = rec_api.subscribe(_on_event_change)

    def unbind_recording_api(self) -> None:
        """Unbind and unsubscribe from recording API facade."""
        if self._unsub_recording:
            try:
                self._unsub_recording()
            except Exception:
                pass
            self._unsub_recording = None

        if self._unsub_consumer:
            try:
                self._unsub_consumer()
            except Exception:
                pass
            self._unsub_consumer = None

    def check_and_rebind_recording_api(self, current_api: Any, available: bool = True) -> bool:
        """Rebind if recording API instance has changed."""
        if current_api is not None and current_api is not self.rec_api:
            _LOGGER.info("Recording API instance replaced; rebinding consumer and subscriptions")
            self.bind_recording_api(current_api, available=available)
            return True
        return False

    async def async_startup(self) -> None:
        """Initialize worker and run startup catchup scan if auto_analyze is enabled."""
        self.worker.start()

        if self.auto_analyze:
            await self.async_scan_unprocessed_media()

    async def async_scan_unprocessed_media(self) -> int:
        """Scan retained finalized media keyset lacking event_vision annotation."""
        if not self.rec_api.store.is_open:
            return 0
        enqueued_count = 0
        cursor = None
        digest = self.profile_digest

        while True:
            items, next_cursor = await self.rec_api.store.list_finalized_media(
                cursor=cursor, limit=50, namespace_lacking=ANNOTATION_NAMESPACE
            )
            for item in items:
                event_id = item["event_id"]
                media_rev = item.get("media_revision", 1)
                async with self._get_event_lock(event_id):
                    if await self.job_store.get_job(event_id, media_rev, digest):
                        continue
                    # Invalidate/reserve recording slot
                    ann_gen = await self.rec_api.store.reserve_annotation(
                        event_id=event_id,
                        namespace=ANNOTATION_NAMESPACE,
                        expected_media_revision=media_rev,
                        worker_id=self.worker.worker_id,
                        ttl_seconds=MAX_PROCESS_DEADLINE,
                    )
                    if ann_gen is None:
                        continue

                    _, enqueued = await self.job_store.enqueue_job(
                        event_id=event_id,
                        media_revision=media_rev,
                        profile_digest=digest,
                        is_manual=False,
                        annotation_generation=ann_gen,
                    )
                    if enqueued:
                        enqueued_count += 1

            if not next_cursor or not items:
                break
            cursor = next_cursor

        if enqueued_count > 0:
            self.worker.notify()

        return enqueued_count

    async def async_handle_event_notification(self, event_id: str, revision: int) -> None:
        """Handle post-commit recording subscription update without infinite loop."""
        if not self.auto_analyze or not self.rec_api.store.is_open:
            return

        async with self._get_event_lock(event_id):
            event = await self.rec_api.get_event(event_id)
            if not event:
                return

            # Only analyze finalized events
            if event.capture_state not in ("ready", "partial"):
                return

            # Check existing annotation to avoid annotation-caused infinite loop
            ann_dict = event.annotations.get(ANNOTATION_NAMESPACE, {})
            if ann_dict:
                ann_status = ann_dict.get("status")
                ann_media_rev = ann_dict.get("media_revision")
                # If already complete, skipped, or failed for this exact media revision, don't re-enqueue
                if ann_status in (STATUS_COMPLETE, STATUS_SKIPPED, STATUS_FAILED) and ann_media_rev == event.media_revision:
                    return

            digest = self.profile_digest
            if await self.job_store.get_job(event_id, event.media_revision, digest):
                return
            ann_gen = await self.rec_api.store.reserve_annotation(
                event_id=event_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_media_revision=event.media_revision,
                worker_id=self.worker.worker_id,
                ttl_seconds=MAX_PROCESS_DEADLINE,
            )
            if ann_gen is None:
                return

            _, enqueued = await self.job_store.enqueue_job(
                event_id=event_id,
                media_revision=event.media_revision,
                profile_digest=digest,
                is_manual=False,
                annotation_generation=ann_gen,
            )
            if enqueued:
                self.worker.notify()

    async def async_manual_analyze(self, event_id: str) -> bool:
        """Handle manual retry: supersedes existing generation atomically."""
        if not self.rec_api.store.is_open:
            return False
        async with self._get_event_lock(event_id):
            event = await self.rec_api.get_event(event_id)
            if not event:
                return False

            # Invalidate recording namespace generation IMMEDIATELY on accept manual retry before ACK
            ann_gen = await self.rec_api.store.reserve_annotation(
                event_id=event_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_media_revision=event.media_revision,
                worker_id=self.worker.worker_id,
                ttl_seconds=MAX_PROCESS_DEADLINE,
            )
            if ann_gen is None:
                _LOGGER.warning("Failed to reserve annotation for manual analyze of event %s", event_id)
                return False

            digest = self.profile_digest
            _, enqueued = await self.job_store.enqueue_job(
                event_id=event_id,
                media_revision=event.media_revision,
                profile_digest=digest,
                is_manual=True,
                annotation_generation=ann_gen,
            )
            if enqueued:
                self.worker.notify()
            return enqueued

    async def async_shutdown(self) -> None:
        """Cleanly unsubscribe, cancel background tasks, and stop worker."""
        self.unbind_recording_api()
        for t in list(self._notification_tasks):
            if not t.done():
                t.cancel()
        if self._notification_tasks:
            await asyncio.gather(*self._notification_tasks, return_exceptions=True)
            self._notification_tasks.clear()
        await self.worker.stop()
        await self.job_store.close()
