"""Durable AnalysisWorker and Coordinator for Event Vision pure core.

Features:
- Pure Python (zero Home Assistant dependencies)
- Media pin lease acquired with bound derived from job deadline and strictly released in finally
- Reserve annotation with generation tracking and deadline-derived TTL
- CAS annotation commits for status 'processing', 'complete', 'failed', 'skipped'
- Rejection of stale generations or changed media revisions
- Bounded configurable sampling (sparse_anchors, trigger_relevant) up to 16 frames
- Per-frame media_id/offset/timestamp_quality provenance without leaking entity names or keys
- Bounded concurrency with asyncio.Semaphore
- Retry backoff (>= 1.0s) and permanent vs retryable failure classification
- Immediate recording generation invalidation on manual retry before ACK
- Clean task lifecycle shutdown and rebind support
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import replace
from typing import Any

from ..const import (
    ANNOTATION_NAMESPACE,
    BASE_BACKOFF_SECONDS,
    CONSUMER_ID,
    DEFAULT_ENABLE_FUSION,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_FUSION_JOB_DEADLINE,
    DEFAULT_FUSION_MAX_FRAMES,
    DEFAULT_FUSION_PROMPT,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    GENERIC_MEDIA_PROMPT,
    MAX_FRAMES_BUDGET,
    MAX_FUSION_MAX_FRAMES,
    MIN_FRAMES,
    MIN_FUSION_MAX_FRAMES,
    STATUS_COMPLETE,
    STATUS_FAILED,
    STATUS_PENDING,
    STATUS_PROCESSING,
    STATUS_SKIPPED,
    VALID_SAMPLING_POLICIES,
)
from .extractor import FrameExtractor
from .fusion_store import FusionJobStore
from .gemini import GeminiClient
from .models import (
    AnalysisAnnotation,
    AnalysisError,
    AnalysisJob,
    ExtractedFrame,
    FusionAnnotation,
    FusionJob,
    FusionViewFrame,
    FusionViewInput,
    IncidentInputs,
    build_fusion_job_payload,
    build_job_payload,
    canonical_profile_json,
    compute_fusion_profile_digest,
    compute_profile_digest,
    derive_lease_pin_seconds,
    derive_queue_lease_seconds,
    derive_reservation_ttl,
    format_iso,
    sanitize_error_code,
    utc_now,
    validate_fusion_profile_payload,
    validate_profile_payload,
)
from .sampling import plan_event_sampling
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
        max_frames: int = DEFAULT_MAX_FRAMES,
        sampling_policy: str = DEFAULT_SAMPLING_POLICY,
        job_deadline: float = DEFAULT_JOB_DEADLINE,
        provider_timeout: float = DEFAULT_PROVIDER_TIMEOUT,
        extraction_timeout: float = DEFAULT_EXTRACTION_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        fusion_job_store: FusionJobStore | None = None,
        enable_fusion: bool = False,
        fusion_max_frames: int = DEFAULT_FUSION_MAX_FRAMES,
        fusion_prompt_instructions: str | None = None,
        fusion_job_deadline: float = DEFAULT_FUSION_JOB_DEADLINE,
    ) -> None:
        self._job_store = job_store
        self._fusion_job_store = fusion_job_store
        self._enable_fusion = bool(enable_fusion)
        self._fusion_max_frames = max(
            MIN_FUSION_MAX_FRAMES, min(int(fusion_max_frames), MAX_FUSION_MAX_FRAMES)
        )
        self._fusion_prompt_instructions = fusion_prompt_instructions
        self._fusion_job_deadline = float(fusion_job_deadline)
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
        self._max_frames = max(MIN_FRAMES, min(max_frames, MAX_FRAMES_BUDGET))
        self._sampling_policy = (
            sampling_policy
            if sampling_policy in VALID_SAMPLING_POLICIES
            else DEFAULT_SAMPLING_POLICY
        )
        self._job_deadline = job_deadline
        self._provider_timeout = provider_timeout
        self._extraction_timeout = extraction_timeout
        self._max_attempts = max_attempts

        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._wake_event = asyncio.Event()
        self._semaphore = asyncio.Semaphore(max(1, min(concurrency, 4)))
        self._background_tasks: set[asyncio.Task[None]] = set()
        self.event_locks: dict[str, asyncio.Lock] = {}
        self.incident_locks: dict[str, asyncio.Lock] = {}

    def event_lock(self, event_id: str) -> asyncio.Lock:
        """Serialize short queue/reservation transitions with manual retries."""
        return self.event_locks.setdefault(event_id, asyncio.Lock())

    def incident_lock(self, incident_id: str) -> asyncio.Lock:
        """Serialize short queue/reservation transitions for an incident."""
        return self.incident_locks.setdefault(incident_id, asyncio.Lock())

    @property
    def enable_fusion(self) -> bool:
        return self._enable_fusion

    @property
    def fusion_max_frames(self) -> int:
        return self._fusion_max_frames

    @property
    def fusion_job_deadline(self) -> float:
        return self._fusion_job_deadline

    @property
    def background_tasks(self) -> set[asyncio.Task[None]]:
        return self._background_tasks

    @property
    def worker_id(self) -> str:
        """Return worker identifier."""
        return self._worker_id

    @property
    def max_frames(self) -> int:
        return self._max_frames

    @property
    def sampling_policy(self) -> str:
        return self._sampling_policy

    @property
    def job_deadline(self) -> float:
        return self._job_deadline

    @property
    def provider_timeout(self) -> float:
        return self._provider_timeout

    @property
    def extraction_timeout(self) -> float:
        return self._extraction_timeout

    @property
    def max_attempts(self) -> int:
        return self._max_attempts

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
                _LOGGER.error(
                    "Unexpected error in analysis worker loop: %s", sanitize_error_code(err)
                )
                await asyncio.sleep(self._poll_interval)

    async def process_next_job(self) -> bool:
        """Attempt to lease and execute one eligible job with bounded concurrency."""
        if not self._rec_api.store.is_open:
            return False
        if self._semaphore.locked():
            return False

        try:
            await self._semaphore.acquire()
        except Exception:
            return False

        permit_acquired = True
        try:
            lease_sec = derive_queue_lease_seconds(self._job_deadline)
            job = await self._job_store.lease_job(self._worker_id, lease_seconds=lease_sec)
            if job:
                task = asyncio.create_task(self._guarded_execute(job))
                self._background_tasks.add(task)
                task.add_done_callback(self._background_tasks.discard)
                return True

            if self._enable_fusion and self._fusion_job_store:
                flease_sec = derive_queue_lease_seconds(self._fusion_job_deadline)
                fjob = await self._fusion_job_store.lease_fusion_job(
                    self._worker_id, lease_seconds=flease_sec
                )
                if fjob:
                    task = asyncio.create_task(self._guarded_execute_fusion(fjob))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                    return True

            self._semaphore.release()
            permit_acquired = False
            return False
        except BaseException:
            if permit_acquired:
                self._semaphore.release()
            raise

    async def _guarded_execute(self, job: AnalysisJob) -> None:
        try:
            job_deadline = self._job_deadline
            if job.profile_payload:
                try:
                    p_data = json.loads(job.profile_payload)
                    b_data = p_data.get("budget") if isinstance(p_data, dict) else None
                    if isinstance(b_data, dict) and "job_deadline" in b_data:
                        job_deadline = float(b_data["job_deadline"])
                    elif isinstance(p_data, dict) and "max_frames" in p_data:
                        m_frames = int(p_data["max_frames"])
                        derived_dead = (
                            (m_frames * DEFAULT_EXTRACTION_TIMEOUT)
                            + DEFAULT_PROVIDER_TIMEOUT
                            + 10.0
                        )
                        job_deadline = max(job_deadline, derived_dead)
                except Exception:
                    pass

            try:
                await asyncio.wait_for(self._execute_job(job), timeout=job_deadline)
            except TimeoutError:
                _LOGGER.warning("Job %s timed out after %ss deadline", job.event_id, job_deadline)
                await self._handle_job_failure(
                    job, AnalysisError("TIMEOUT", "Execution deadline exceeded", retryable=True)
                )
            except asyncio.CancelledError:
                _LOGGER.info("Job %s cancelled during execution; leaving recoverable", job.event_id)
                raise
            except Exception as err:
                _LOGGER.error(
                    "Execution error on job %s: %s", job.event_id, sanitize_error_code(err)
                )
                await self._handle_job_failure(job, err)
        finally:
            self._semaphore.release()

    async def _execute_job(self, job: AnalysisJob) -> None:
        """Execute single analysis job with derived media lease pin and CAS protection."""
        if not await self._job_store.is_job_current(job.id, job.generation):
            _LOGGER.info(
                "Job %s (gen %s) is no longer current; aborting execution",
                job.event_id,
                job.generation,
            )
            return

        # 1. Ensure annotation reservation exists under per-event lock BEFORE payload validation
        async with self.event_lock(job.event_id):
            if not await self._job_store.is_job_current(job.id, job.generation):
                return
            event = await self._rec_api.get_event(job.event_id)
            if not event or event.media_revision != job.media_revision:
                await self._job_store.supersede_job(job.id, job.generation)
                return
            existing_ann = event.annotations.get(ANNOTATION_NAMESPACE, {})
            existing_gen = existing_ann.get("generation", 0)

            annotation_gen = job.annotation_generation
            if annotation_gen is None or existing_gen == 0:
                res_ttl = derive_reservation_ttl(self._job_deadline)
                annotation_gen = await self._rec_api.store.reserve_annotation(
                    event_id=job.event_id,
                    namespace=ANNOTATION_NAMESPACE,
                    expected_media_revision=job.media_revision,
                    worker_id=self._worker_id,
                    ttl_seconds=res_ttl,
                )
                if annotation_gen is None:
                    await self._job_store.supersede_job(job.id, job.generation)
                    return
                await self._job_store.update_job_annotation_generation(
                    job.id, job.generation, annotation_gen
                )
                job = replace(job, annotation_generation=annotation_gen)
            else:
                annotation_gen = existing_gen
                if job.annotation_generation != annotation_gen:
                    job = replace(job, annotation_generation=annotation_gen)

        # 2. Resolve effective profile parameters strictly from durable job snapshot
        if not job.profile_payload:
            current_digest = compute_profile_digest(
                model=self._gemini.model if self._gemini else self._model,
                instructions=self._prompt_instructions,
                max_frames=self._max_frames,
                sampling_policy=self._sampling_policy,
            )
            # Safe explicit migration for legacy pending jobs matching exact current worker digest
            if job.profile_digest == current_digest:
                migrated_payload_dict = build_job_payload(
                    model=self._gemini.model if self._gemini else self._model,
                    max_frames=self._max_frames,
                    sampling_policy=self._sampling_policy,
                    instructions=self._prompt_instructions,
                    base_prompt=GENERIC_MEDIA_PROMPT,
                    job_deadline=max(DEFAULT_JOB_DEADLINE, float(self._job_deadline)),
                    provider_timeout=self._provider_timeout,
                    extraction_timeout=self._extraction_timeout,
                    max_attempts=self._max_attempts,
                )
                migrated_payload = canonical_profile_json(migrated_payload_dict)
                await self._job_store.update_job_payload(job.id, migrated_payload)
                job = replace(job, profile_payload=migrated_payload)
            else:
                raise AnalysisError(
                    "INVALID_PROFILE",
                    f"Legacy job digest {job.profile_digest} does not match current profile digest {current_digest}",
                    retryable=False,
                )

        semantic_profile, budget = validate_profile_payload(
            job.profile_payload,
            expected_digest=job.profile_digest,
        )

        job_model = semantic_profile["model"]
        job_instructions = semantic_profile["instructions"]
        job_base_prompt = semantic_profile["base_prompt"]
        job_max_frames = semantic_profile["max_frames"]
        job_policy = semantic_profile["sampling_policy"]

        job_deadline = budget["job_deadline"]
        job_provider_timeout = budget["provider_timeout"]
        job_extraction_timeout = budget["extraction_timeout"]

        lease_pin = None
        try:
            # 3. Acquire media lease pin with derived duration bound
            lease_duration = derive_lease_pin_seconds(job_deadline)
            lease_pin = await self._rec_api.store.pin_media_lease(
                event_id=job.event_id,
                lease_seconds=lease_duration,
                reason="event_vision analysis",
            )

            # 4. Set processing status via CAS using claimed generation
            async with self.event_lock(job.event_id):
                if not await self._job_store.is_job_current(job.id, job.generation):
                    return

                proc_annotation = AnalysisAnnotation(
                    status=STATUS_PROCESSING,
                    media_revision=job.media_revision,
                    generation=annotation_gen,
                    model=job_model,
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
                raise AnalysisError(
                    "MISSING_API_KEY", "Gemini API key is not configured", retryable=False
                )

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

            # 5. Plan frame sampling across media items
            planned_anchors = plan_event_sampling(
                media_items=available_media,
                policy=job_policy,
                max_frames=job_max_frames,
                event_start_at=event.start_at,
                source_events=event.source_events,
            )

            # Group planned anchors by media_id
            anchors_by_media: dict[str, list[Any]] = {}
            for a in planned_anchors:
                anchors_by_media.setdefault(a.media_id, []).append(a)

            media_by_id = {m.id: m for m in available_media}
            accumulated_frames: list[ExtractedFrame] = []

            for m_id, item_anchors in anchors_by_media.items():
                media_item = media_by_id.get(m_id)
                if not media_item:
                    continue

                try:
                    resolved_path = await self._rec_api.resolve_media(job.event_id, m_id)
                except Exception as err:
                    _LOGGER.warning(
                        "Failed to resolve media %s for event %s: %s",
                        m_id,
                        job.event_id,
                        sanitize_error_code(err),
                    )
                    continue

                ts_list = [a.offset_seconds for a in item_anchors]
                quality = item_anchors[0].timestamp_quality if item_anchors else "unknown"
                offset_q = (
                    getattr(item_anchors[0], "offset_quality", "exact") if item_anchors else "exact"
                )

                try:
                    frames = await self._extractor.extract_from_path(
                        media_path=resolved_path,
                        kind=getattr(media_item, "kind", None),
                        mime_type=getattr(media_item, "mime_type", None),
                        duration=getattr(media_item, "duration", None),
                        max_frames=len(ts_list),
                        timestamps=ts_list,
                        timeout_seconds=job_extraction_timeout,
                        media_id=m_id,
                        timestamp_quality=quality,
                        offset_quality=offset_q,
                    )
                    accumulated_frames.extend(frames)
                except Exception as err:
                    _LOGGER.warning(
                        "Frame extraction error on media %s: %s", m_id, sanitize_error_code(err)
                    )

            if not accumulated_frames:
                raise AnalysisError(
                    "FRAME_EXTRACTION_FAILED", "No valid frames could be extracted", retryable=False
                )

            # Trim strictly to configured max_frames
            accumulated_frames = accumulated_frames[:job_max_frames]

            # 6. Call direct Gemini REST generateContent
            result = await self._gemini.generate_analysis(
                frames=accumulated_frames,
                prompt_instructions=job_instructions,
                timeout_seconds=job_provider_timeout,
                model=job_model,
                base_prompt=job_base_prompt,
            )

            # 7. Post CAS complete state with frame provenance and returned modelVersion
            if not await self._job_store.is_job_current(job.id, job.generation):
                _LOGGER.warning(
                    "Job %s superseded before complete commit; skipping CAS", job.event_id
                )
                return

            frames_provenance = [
                {
                    "media_id": f.media_id or "unknown",
                    "offset": round(f.offset_seconds, 3),
                    "timestamp_quality": f.timestamp_quality,
                }
                for f in accumulated_frames
            ]

            complete_annotation = AnalysisAnnotation(
                status=STATUS_COMPLETE,
                media_revision=job.media_revision,
                generation=annotation_gen,
                summary=result.summary,
                activity=result.activity,
                objects=result.objects,
                model=job_model,
                model_version=getattr(result, "model_version", None),
                updated_at=format_iso(utc_now()),
                frames=frames_provenance,
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
                _LOGGER.warning(
                    "CAS commit rejected on completion for %s; stale generation or media revision",
                    job.event_id,
                )
                await self._job_store.supersede_job(job.id, job.generation)

        except Exception as err:
            if not self._rec_api.store.is_open:
                await self._job_store.defer_job(job.id, job.generation)
            else:
                await self._handle_job_failure(job, err)
        finally:
            # 8. Release media lease pin in finally block
            await self._cleanup_media_pins([lease_pin] if lease_pin else [])

    async def _cleanup_media_pins(self, pins: list[Any]) -> None:
        """Release acquired media pins under an owned task, awaiting settlement on cancel."""
        tokens: list[str] = []
        for p in pins:
            t = getattr(p, "lease_token", None) or (p if isinstance(p, str) else None)
            if t:
                tokens.append(t)
        if not tokens or not self._rec_api:
            return

        async def _do_release() -> None:
            for token in tokens:
                try:
                    await self._rec_api.store.release_media_pin(token)
                except Exception as err:
                    _LOGGER.warning("Error releasing media pin: %s", sanitize_error_code(err))

        cleanup_task = asyncio.create_task(_do_release())
        cancelled: asyncio.CancelledError | None = None
        while not cleanup_task.done():
            try:
                await asyncio.shield(cleanup_task)
            except asyncio.CancelledError as exc:
                cancelled = exc
        if cancelled is not None:
            raise cancelled

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

        current = await self._job_store.get_job(
            job.event_id, job.media_revision, job.profile_digest
        )
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

        job_max_attempts = self._max_attempts
        job_model = (
            self._gemini.model
            if self._gemini and isinstance(getattr(self._gemini, "model", None), str)
            else (self._model if isinstance(self._model, str) else "gemini-flash-lite-latest")
        )
        if job.profile_payload:
            try:
                p_data = json.loads(job.profile_payload)
                if isinstance(p_data, dict):
                    if p_data.get("model") and isinstance(p_data["model"], str):
                        job_model = p_data["model"].strip()
                    b_data = p_data.get("budget")
                    if isinstance(b_data, dict) and "max_attempts" in b_data:
                        job_max_attempts = int(b_data["max_attempts"])
            except Exception:
                pass

        # If permanent or exhausted attempts, update recording CAS to status='failed'
        if not retryable or (job.attempts + 1) >= job_max_attempts:
            async with self.event_lock(job.event_id):
                try:
                    # Check whether job was superseded or newer job exists for this event/revision
                    latest_job = await self._job_store.get_latest_job_for_event(
                        job.event_id, job.media_revision
                    )
                    if (
                        latest_job is None
                        or latest_job.id != job.id
                        or latest_job.generation != job.generation
                        or latest_job.status == "superseded"
                    ):
                        _LOGGER.info(
                            "Job %s was superseded or retried before lock acquired; skipping failure post",
                            job.event_id,
                        )
                        return

                    claimed_gen = job.annotation_generation
                    if not claimed_gen:
                        # No claimed generation to post failure to; NEVER reserve fresh generation in failure handler
                        return

                    event = await self._rec_api.get_event(job.event_id)
                    if not event or event.media_revision != job.media_revision:
                        return
                    existing_ann = event.annotations.get(ANNOTATION_NAMESPACE, {})
                    if existing_ann.get("status") in (STATUS_COMPLETE, STATUS_SKIPPED):
                        return

                    existing_gen = existing_ann.get("generation", 0)
                    if existing_gen != claimed_gen:
                        _LOGGER.info(
                            "Recording annotation generation %s differs from job claimed gen %s; skipping stale failure",
                            existing_gen,
                            claimed_gen,
                        )
                        return

                    fail_ann = AnalysisAnnotation(
                        status=STATUS_FAILED,
                        media_revision=job.media_revision,
                        generation=claimed_gen,
                        error_code=clean_code,
                        model=job_model,
                        updated_at=format_iso(utc_now()),
                    )
                    posted = False
                    try:
                        posted = await self._rec_api.store.set_annotation_cas(
                            event_id=job.event_id,
                            expected_media_revision=job.media_revision,
                            namespace=ANNOTATION_NAMESPACE,
                            payload=fail_ann.to_dict(),
                            expected_generation=claimed_gen,
                        )
                    except Exception:
                        posted = False

                    # If CAS rejected, DO NOT fallback to reserve a new generation!
                    if not posted:
                        _LOGGER.info(
                            "CAS rejected for failed annotation on job %s (gen %s); avoiding stale reservation",
                            job.event_id,
                            claimed_gen,
                        )
                        return
                except Exception as post_err:
                    _LOGGER.warning(
                        "Could not post failed annotation for %s: %s",
                        job.event_id,
                        sanitize_error_code(post_err),
                    )

    async def _extract_frames_for_event(
        self,
        event: Any,
        max_frames: int,
        policy: str,
        extraction_timeout: float,
    ) -> list[ExtractedFrame]:
        available_media = [
            m for m in getattr(event, "media", []) if getattr(m, "available", True)
        ]
        if not available_media:
            return []

        planned_anchors = plan_event_sampling(
            media_items=available_media,
            policy=policy,
            max_frames=max_frames,
            event_start_at=getattr(event, "start_at", None),
            source_events=getattr(event, "source_events", ()),
        )

        anchors_by_media: dict[str, list[Any]] = {}
        for a in planned_anchors:
            anchors_by_media.setdefault(a.media_id, []).append(a)

        media_by_id = {m.id: m for m in available_media}
        accumulated_frames: list[ExtractedFrame] = []

        for m_id, item_anchors in anchors_by_media.items():
            media_item = media_by_id.get(m_id)
            if not media_item:
                continue

            try:
                resolved_path = await self._rec_api.resolve_media(event.event_id, m_id)
            except Exception as err:
                _LOGGER.warning(
                    "Failed to resolve media %s for event %s: %s",
                    m_id,
                    event.event_id,
                    sanitize_error_code(err),
                )
                continue

            ts_list = [a.offset_seconds for a in item_anchors]
            quality = item_anchors[0].timestamp_quality if item_anchors else "unknown"
            offset_q = (
                getattr(item_anchors[0], "offset_quality", "exact")
                if item_anchors
                else "exact"
            )

            try:
                frames = await self._extractor.extract_from_path(
                    media_path=resolved_path,
                    kind=getattr(media_item, "kind", None),
                    mime_type=getattr(media_item, "mime_type", None),
                    duration=getattr(media_item, "duration", None),
                    max_frames=len(ts_list),
                    timestamps=ts_list,
                    timeout_seconds=extraction_timeout,
                    media_id=m_id,
                    timestamp_quality=quality,
                    offset_quality=offset_q,
                )
                accumulated_frames.extend(frames)
            except Exception as err:
                _LOGGER.warning(
                    "Frame extraction error on media %s: %s", m_id, sanitize_error_code(err)
                )

        return accumulated_frames[:max_frames]

    async def _guarded_execute_fusion(self, job: FusionJob) -> None:
        try:
            job_deadline = self._fusion_job_deadline
            if job.profile_payload:
                try:
                    _, budget = validate_fusion_profile_payload(job.profile_payload)
                    job_deadline = budget["job_deadline"]
                except Exception:
                    pass

            try:
                await asyncio.wait_for(self._execute_fusion_job(job), timeout=job_deadline)
            except TimeoutError:
                _LOGGER.warning(
                    "Fusion job %s timed out after %ss deadline", job.incident_id, job_deadline
                )
                await self._handle_fusion_job_failure(
                    job,
                    AnalysisError("TIMEOUT", "Fusion execution deadline exceeded", retryable=True),
                )
            except asyncio.CancelledError:
                _LOGGER.info(
                    "Fusion job %s cancelled during execution; leaving recoverable",
                    job.incident_id,
                )
                raise
            except Exception as err:
                _LOGGER.error(
                    "Execution error on fusion job %s: %s", job.incident_id, sanitize_error_code(err)
                )
                await self._handle_fusion_job_failure(job, err)
        finally:
            self._semaphore.release()

    async def _execute_fusion_job(self, job: FusionJob) -> None:
        """Execute single multi-camera incident visual fusion job with CAS protection."""
        if not self._fusion_job_store or not await self._fusion_job_store.is_fusion_job_current(
            job.id, job.generation
        ):
            _LOGGER.info(
                "Fusion job %s (gen %s) is no longer current; aborting execution",
                job.incident_id,
                job.generation,
            )
            return

        if not self._enable_fusion:
            await self._fusion_job_store.skip_fusion_job(job.id, job.generation, "FUSION_DISABLED")
            return

        if self._gemini is None:
            await self._fusion_job_store.fail_fusion_job(
                job.id, job.generation, "MISSING_API_KEY", retryable=False
            )
            return

        if not hasattr(self._rec_api, "get_incident_inputs"):
            await self._fusion_job_store.fail_fusion_job(
                job.id, job.generation, "INCIDENT_API_UNAVAILABLE", retryable=False
            )
            return

        async with self.incident_lock(job.incident_id):
            if not await self._fusion_job_store.is_fusion_job_current(job.id, job.generation):
                return

            semantic, budget = validate_fusion_profile_payload(
                job.profile_payload, expected_digest=job.profile_digest
            )
            job_model = semantic["model"]
            job_instructions = semantic["instructions"]
            job_fusion_prompt = semantic["fusion_prompt"]
            job_max_frames = semantic["max_frames"]
            job_policy = semantic["sampling_policy"]
            job_deadline = budget["job_deadline"]
            job_provider_timeout = budget["provider_timeout"]
            job_extraction_timeout = budget["extraction_timeout"]

            inputs: IncidentInputs | None = await self._rec_api.get_incident_inputs(
                job.incident_id, ANNOTATION_NAMESPACE
            )
            if inputs is None:
                await self._fusion_job_store.supersede_fusion_job(job.id, job.generation)
                return

            if inputs.input_token != job.input_token:
                _LOGGER.info(
                    "Fusion job %s input token %s superseded before provider call by %s",
                    job.incident_id,
                    job.input_token,
                    inputs.input_token,
                )
                await self._fusion_job_store.supersede_fusion_job(job.id, job.generation)
                return

            events = inputs.events
            # Check if any member has pending or processing analysis
            in_flight = False
            for ev in events:
                ev_ann = getattr(ev, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}
                if ev_ann.get("status") in (STATUS_PENDING, STATUS_PROCESSING):
                    in_flight = True
                    break
            if in_flight:
                await self._fusion_job_store.defer_fusion_job(job.id, job.generation)
                return

            # Available completed member events with media
            available_events = [
                ev
                for ev in events
                if (
                    getattr(ev, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}
                ).get("status")
                == STATUS_COMPLETE
                and getattr(ev, "media", None)
                and any(getattr(m, "available", True) for m in ev.media)
            ]

            ann_gen = job.annotation_generation or 1

            # Group available events by unique camera visual view
            # Uses resource_key if available; falls back to source_id (non-camera imports),
            # or event_id if neither is present.
            # Two events from the same physical camera = ONE view.
            # Two sources without camera IDs are still two distinct declared views.
            views_map: dict[str, list[Any]] = {}
            for ev in available_events:
                r_key = getattr(ev, "resource_key", None)
                if r_key and isinstance(r_key, str) and r_key.strip():
                    v_key = r_key.strip()
                else:
                    s_id = getattr(ev, "source_id", None)
                    if s_id and isinstance(s_id, str) and s_id.strip():
                        v_key = s_id.strip()
                    else:
                        v_key = getattr(ev, "event_id", "")
                views_map.setdefault(v_key, []).append(ev)

            num_views = len(views_map)

            if num_views < 2:
                _LOGGER.info(
                    "Incident %s has %s available visual views (< 2); skipping fusion",
                    job.incident_id,
                    num_views,
                )
                skip_ann = FusionAnnotation(
                    status=STATUS_SKIPPED,
                    input_token=job.input_token,
                    generation=ann_gen,
                    profile_digest=job.profile_digest,
                    skipped_reason="INSUFFICIENT_VIEWS",
                    updated_at=format_iso(utc_now()),
                    member_events=[getattr(ev, "event_id", "") for ev in events],
                )
                await self._rec_api.set_incident_annotation_cas(
                    incident_id=job.incident_id,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=skip_ann.to_dict(),
                    expected_generation=ann_gen,
                    expected_input_token=job.input_token,
                    profile_digest=job.profile_digest,
                )
                await self._fusion_job_store.skip_fusion_job(
                    job.id, job.generation, "INSUFFICIENT_VIEWS"
                )
                return

            if num_views > job_max_frames:
                _LOGGER.info(
                    "Incident %s visual views %s exceed frame budget %s; skipping fusion",
                    job.incident_id,
                    num_views,
                    job_max_frames,
                )
                skip_ann = FusionAnnotation(
                    status=STATUS_SKIPPED,
                    input_token=job.input_token,
                    generation=ann_gen,
                    profile_digest=job.profile_digest,
                    skipped_reason="TOO_MANY_VIEWS_FOR_BUDGET",
                    updated_at=format_iso(utc_now()),
                    member_events=[getattr(ev, "event_id", "") for ev in events],
                )
                await self._rec_api.set_incident_annotation_cas(
                    incident_id=job.incident_id,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=skip_ann.to_dict(),
                    expected_generation=ann_gen,
                    expected_input_token=job.input_token,
                    profile_digest=job.profile_digest,
                )
                await self._fusion_job_store.skip_fusion_job(
                    job.id, job.generation, "TOO_MANY_VIEWS_FOR_BUDGET"
                )
                return


        # Pin all member media
        pins: list[Any] = []
        try:
            lease_duration = derive_lease_pin_seconds(job_deadline)
            for ev in available_events:
                pin = await self._rec_api.store.pin_media_lease(
                    event_id=ev.event_id,
                    lease_seconds=lease_duration,
                    reason="event_vision incident fusion",
                )
                if pin is None:
                    raise AnalysisError(
                        "MEDIA_PIN_FAILED",
                        f"Failed to acquire media lease pin for member event {ev.event_id}",
                        retryable=True,
                    )
                pins.append(pin)

            frames_per_view = job_max_frames // num_views
            remainder = job_max_frames % num_views

            views: list[FusionViewInput] = []
            all_view_frame_metas: list[dict[str, Any]] = []

            for idx, (v_key, ev_list) in enumerate(views_map.items(), start=1):
                view_budget = frames_per_view + (1 if idx <= remainder else 0)
                view_budget = max(1, view_budget)
                view_label = f"View {idx}"

                # Extract frames across member events in this visual view
                view_extracted_frames: list[tuple[Any, ExtractedFrame]] = []
                frames_needed = view_budget
                for ev in ev_list:
                    if frames_needed <= 0:
                        break
                    per_ev_limit = (
                        max(1, frames_needed // len(ev_list))
                        if len(ev_list) > 1
                        else frames_needed
                    )
                    raw_frames = await self._extract_frames_for_event(
                        event=ev,
                        max_frames=per_ev_limit,
                        policy=job_policy,
                        extraction_timeout=job_extraction_timeout,
                    )
                    for rf in raw_frames:
                        view_extracted_frames.append((ev, rf))
                        if len(view_extracted_frames) >= view_budget:
                            break
                    frames_needed = view_budget - len(view_extracted_frames)

                if not view_extracted_frames:
                    raise AnalysisError(
                        "NO_FRAMES_IN_VIEW",
                        f"Failed to extract frames for member view {view_label}",
                        retryable=False,
                    )

                v_frames: list[FusionViewFrame] = []
                frame_qualities: list[str] = []

                for f_idx, (frame_ev, rf) in enumerate(view_extracted_frames):
                    f_ref = f"view{idx}_frame{f_idx}"
                    rf_offset = float(rf.offset_seconds)
                    rf_ms = int(rf_offset * 1000.0)

                    # Frame timing quality: derive from ExtractedFrame.timestamp_quality
                    # Only exact timestamp on qualified clock is known_utc.
                    # Logical event trigger time does NOT upgrade unknown/estimated media to known_utc.
                    if rf.timestamp_quality == "exact":
                        f_qual = "known_utc"
                    elif rf.timestamp_quality == "estimated":
                        f_qual = "estimated"
                    elif rf.timestamp_quality == "relative_fallback":
                        f_qual = "relative"
                    else:
                        f_qual = "unknown"

                    frame_qualities.append(f_qual)

                    v_frames.append(
                        FusionViewFrame(
                            frame_ref=f_ref,
                            data=rf.data,
                            mime_type=rf.mime_type,
                            timestamp_ms=rf_ms,
                            time_quality=f_qual,
                            offset_seconds=rf_offset,
                        )
                    )
                    all_view_frame_metas.append(
                        {
                            "view_index": idx,
                            "frame_ref": f_ref,
                            "event_id": frame_ev.event_id,
                            "media_id": rf.media_id or "unknown",
                            "offset_seconds": rf_offset,
                            "timestamp_ms": rf_ms,
                            "time_quality": f_qual,
                        }
                    )

                # Per-view time quality: explicit uniform or mixed
                unique_q = set(frame_qualities)
                if len(unique_q) == 1:
                    v_time_quality = next(iter(unique_q))
                else:
                    v_time_quality = "mixed"

                # Observations: aggregate across all relevant selected events for this view
                obs_parts: list[str] = []
                for ev in ev_list:
                    prior_ann = (
                        getattr(ev, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}
                    )
                    prior_summary = prior_ann.get("summary")
                    if prior_summary and isinstance(prior_summary, str) and prior_summary.strip():
                        obs_parts.append(prior_summary.strip())
                view_observations = " | ".join(obs_parts) if obs_parts else None

                views.append(
                    FusionViewInput(
                        view_index=idx,
                        view_label=view_label,
                        time_quality=v_time_quality,
                        observations=view_observations,
                        frames=v_frames,
                    )
                )

            # Check input freshness and job currency immediately BEFORE calling provider HTTP
            async with self.incident_lock(job.incident_id):
                if not await self._fusion_job_store.is_fusion_job_current(
                    job.id, job.generation
                ):
                    _LOGGER.info(
                        "Fusion job %s superseded during extraction; aborting before provider",
                        job.incident_id,
                    )
                    return

                inputs_now = await self._rec_api.get_incident_inputs(
                    job.incident_id, ANNOTATION_NAMESPACE
                )
                if inputs_now is None or inputs_now.input_token != job.input_token:
                    _LOGGER.info(
                        "Incident %s inputs changed during extraction (%s -> %s); superseding before provider",
                        job.incident_id,
                        job.input_token,
                        getattr(inputs_now, "input_token", None),
                    )
                    await self._fusion_job_store.supersede_fusion_job(job.id, job.generation)
                    return

            # Call provider
            fusion_result = await self._gemini.generate_fusion_analysis(
                views=views,
                prompt_instructions=job_instructions,
                timeout_seconds=job_provider_timeout,
                model=job_model,
                fusion_prompt=job_fusion_prompt,
            )

            # CAS commit
            async with self.incident_lock(job.incident_id):
                if not await self._fusion_job_store.is_fusion_job_current(
                    job.id, job.generation
                ):
                    return

                final_ann = FusionAnnotation(
                    status=STATUS_COMPLETE,
                    input_token=job.input_token,
                    generation=ann_gen,
                    profile_digest=job.profile_digest,
                    narrative=fusion_result.narrative,
                    activity=fusion_result.activity,
                    possible_cross_view_matches=fusion_result.possible_cross_view_matches,
                    model=job_model,
                    model_version=fusion_result.model_version,
                    updated_at=format_iso(utc_now()),
                    member_events=[ev.event_id for ev in available_events],
                    view_frames=all_view_frame_metas,
                )
                cas_ok = await self._rec_api.set_incident_annotation_cas(
                    incident_id=job.incident_id,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=final_ann.to_dict(),
                    expected_generation=ann_gen,
                    expected_input_token=job.input_token,
                    profile_digest=job.profile_digest,
                )
                if cas_ok:
                    await self._fusion_job_store.complete_fusion_job(job.id, job.generation)
                else:
                    _LOGGER.warning(
                        "CAS commit failed for fusion job %s; superseding", job.incident_id
                    )
                    await self._fusion_job_store.supersede_fusion_job(job.id, job.generation)
        finally:
            await self._cleanup_media_pins(pins)

    async def _handle_fusion_job_failure(self, job: FusionJob, err: Exception) -> None:
        """Handle fusion job failure with generation check, retry backoff, and CAS annotation."""
        latest_job = await self._fusion_job_store.get_latest_fusion_job(job.incident_id)
        if (
            latest_job is None
            or latest_job.id != job.id
            or latest_job.generation != job.generation
            or latest_job.status == "superseded"
        ):
            _LOGGER.info(
                "Fusion job %s (gen %s) is obsolete; skipping failure handling",
                job.incident_id,
                job.generation,
            )
            return

        err_code = err.code if isinstance(err, AnalysisError) else sanitize_error_code(err)
        retryable = err.retryable if isinstance(err, AnalysisError) else False

        job_max_attempts = self._max_attempts
        if job.profile_payload:
            try:
                _, budget = validate_fusion_profile_payload(job.profile_payload)
                job_max_attempts = budget["max_attempts"]
            except Exception:
                pass

        if retryable and (job.attempts + 1) < job_max_attempts:
            backoff = BASE_BACKOFF_SECONDS * (2**job.attempts)
            await self._fusion_job_store.fail_fusion_job(
                job.id, job.generation, err_code, retryable=True, backoff_seconds=backoff
            )
        else:
            transitioned = await self._fusion_job_store.fail_fusion_job(
                job.id, job.generation, err_code, retryable=False
            )
            if transitioned:
                # The store transition confirms this generation was still leased;
                # the annotation CAS also rejects a newer retry/input reservation.
                async with self.incident_lock(job.incident_id):
                    fail_ann = FusionAnnotation(
                        status=STATUS_FAILED,
                        input_token=job.input_token,
                        generation=job.annotation_generation or 1,
                        profile_digest=job.profile_digest,
                        error_code=err_code,
                        updated_at=format_iso(utc_now()),
                    )
                    try:
                        await self._rec_api.set_incident_annotation_cas(
                            incident_id=job.incident_id,
                            namespace=ANNOTATION_NAMESPACE,
                            payload=fail_ann.to_dict(),
                            expected_generation=job.annotation_generation or 1,
                            expected_input_token=job.input_token,
                            profile_digest=job.profile_digest,
                        )
                    except Exception as post_err:
                        _LOGGER.warning(
                            "Failed to post failed annotation for fusion job %s: %s",
                            job.incident_id,
                            sanitize_error_code(post_err),
                        )


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
        max_frames: int = DEFAULT_MAX_FRAMES,
        sampling_policy: str = DEFAULT_SAMPLING_POLICY,
        job_deadline: float = DEFAULT_JOB_DEADLINE,
        provider_timeout: float = DEFAULT_PROVIDER_TIMEOUT,
        extraction_timeout: float = DEFAULT_EXTRACTION_TIMEOUT,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        fusion_job_store: FusionJobStore | None = None,
        enable_fusion: bool = DEFAULT_ENABLE_FUSION,
        fusion_max_frames: int = DEFAULT_FUSION_MAX_FRAMES,
        fusion_prompt_instructions: str | None = None,
        fusion_job_deadline: float = DEFAULT_FUSION_JOB_DEADLINE,
    ) -> None:
        self.job_store = job_store
        self.fusion_job_store = fusion_job_store
        self.worker = worker
        self.rec_api = recording_api
        self.auto_analyze = auto_analyze
        self.model = model
        self.prompt_instructions = prompt_instructions
        self.max_frames = max(MIN_FRAMES, min(max_frames, MAX_FRAMES_BUDGET))
        self.sampling_policy = (
            sampling_policy
            if sampling_policy in VALID_SAMPLING_POLICIES
            else DEFAULT_SAMPLING_POLICY
        )
        self.job_deadline = job_deadline
        self.provider_timeout = provider_timeout
        self.extraction_timeout = extraction_timeout
        self.max_attempts = max_attempts

        self.enable_fusion = bool(enable_fusion)
        self.fusion_max_frames = max(
            MIN_FUSION_MAX_FRAMES, min(int(fusion_max_frames), MAX_FUSION_MAX_FRAMES)
        )
        self.fusion_prompt_instructions = fusion_prompt_instructions or ""
        self.fusion_job_deadline = float(fusion_job_deadline)

        self._unsub_recording: Any = None
        self._unsub_incidents: Any = None
        self._unsub_consumer: Any = None
        self._notification_tasks: set[asyncio.Task[None]] = set()

    def _get_event_lock(self, event_id: str) -> asyncio.Lock:
        return self.worker.event_lock(event_id)

    def _get_incident_lock(self, incident_id: str) -> asyncio.Lock:
        return self.worker.incident_lock(incident_id)

    @property
    def profile_payload(self) -> str:
        """Return canonical JSON profile payload with semantic and operational budget snapshot."""
        payload = build_job_payload(
            model=self.model,
            instructions=self.prompt_instructions,
            max_frames=self.max_frames,
            sampling_policy=self.sampling_policy,
            base_prompt=GENERIC_MEDIA_PROMPT,
            job_deadline=self.job_deadline,
            provider_timeout=self.provider_timeout,
            extraction_timeout=self.extraction_timeout,
            max_attempts=self.max_attempts,
        )
        return canonical_profile_json(payload)

    @property
    def profile_digest(self) -> str:
        """Return profile digest including all actual semantic sampling and analysis settings."""
        return compute_profile_digest(
            model=self.model,
            instructions=self.prompt_instructions,
            max_frames=self.max_frames,
            sampling_policy=self.sampling_policy,
        )

    @property
    def fusion_profile_payload(self) -> str:
        """Return canonical JSON profile payload for fusion jobs."""
        payload = build_fusion_job_payload(
            model=self.model,
            instructions=self.fusion_prompt_instructions,
            max_frames=self.fusion_max_frames,
            sampling_policy=self.sampling_policy,
            fusion_prompt=DEFAULT_FUSION_PROMPT,
            job_deadline=self.fusion_job_deadline,
            provider_timeout=self.provider_timeout,
            extraction_timeout=self.extraction_timeout,
            max_attempts=self.max_attempts,
        )
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    @property
    def fusion_profile_digest(self) -> str:
        """Return deterministic fusion profile digest."""
        return compute_fusion_profile_digest(
            model=self.model,
            instructions=self.fusion_prompt_instructions,
            max_frames=self.fusion_max_frames,
            sampling_policy=self.sampling_policy,
            fusion_prompt=DEFAULT_FUSION_PROMPT,
        )

    def bind_recording_api(self, rec_api: Any, available: bool = True) -> None:
        """Bind or rebind recording API facade and setup subscriptions cleanly."""
        self.unbind_recording_api()
        self.rec_api = rec_api
        self.worker.set_recording_api(rec_api)

        async def async_consumer_retry(event_id: str) -> bool:
            return await self.async_manual_analyze(event_id)

        async def _is_consumer_busy() -> bool:
            event_busy = await self.job_store.has_pending_or_leased_jobs()
            fusion_busy = (
                await self.fusion_job_store.has_pending_or_leased_jobs()
                if self.fusion_job_store
                else False
            )
            active_busy = len(self.worker.background_tasks) > 0
            return event_busy or fusion_busy or active_busy

        self._unsub_consumer = rec_api.register_consumer(
            consumer_id=CONSUMER_ID,
            retry_callback=async_consumer_retry,
            available=available,
            is_busy=_is_consumer_busy,
        )

        def _on_event_change(event_id: str, revision: int) -> None:
            task = asyncio.create_task(self.async_handle_event_notification(event_id, revision))
            self._notification_tasks.add(task)
            task.add_done_callback(self._notification_tasks.discard)

        self._unsub_recording = rec_api.subscribe(_on_event_change)

        if hasattr(rec_api, "subscribe_incidents"):
            def _on_incident_change(incident_id: str, revision: int) -> None:
                task = asyncio.create_task(
                    self.async_handle_incident_notification(incident_id, revision)
                )
                self._notification_tasks.add(task)
                task.add_done_callback(self._notification_tasks.discard)

            self._unsub_incidents = rec_api.subscribe_incidents(_on_incident_change)

    def unbind_recording_api(self) -> None:
        """Unbind and unsubscribe from recording API facade."""
        if self._unsub_recording:
            try:
                self._unsub_recording()
            except Exception:
                pass
            self._unsub_recording = None

        if self._unsub_incidents:
            try:
                self._unsub_incidents()
            except Exception:
                pass
            self._unsub_incidents = None

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
        # Migrate pending legacy jobs to current profile without touching completed archive
        await self.job_store.migrate_legacy_pending_jobs(
            profile_digest=self.profile_digest,
            profile_payload=self.profile_payload,
        )
        if self.fusion_job_store:
            await self.fusion_job_store.open()
        self.worker.start()

        if self.auto_analyze:
            await self.async_scan_unprocessed_media()

    async def async_scan_unprocessed_media(self) -> int:
        """Scan retained finalized media keyset lacking event_vision annotation.

        Never performs automatic archive sweeps of already-annotated media.
        """
        if not self.rec_api.store.is_open:
            return 0
        enqueued_count = 0
        cursor = None
        digest = self.profile_digest
        res_ttl = derive_reservation_ttl(self.job_deadline)

        while True:
            items, next_cursor = await self.rec_api.store.list_finalized_media(
                cursor=cursor, limit=50, namespace_lacking=ANNOTATION_NAMESPACE
            )
            for item in items:
                event_id = item["event_id"]
                media_rev = item.get("media_revision", 1)
                async with self._get_event_lock(event_id):
                    # Check under lock whether this event is eligible for auto enqueue BEFORE reserving annotation
                    if not await self.job_store.should_auto_enqueue(event_id, media_rev):
                        continue

                    # Also inspect actual event annotations to verify it lacks event_vision completion/skipping
                    event = await self.rec_api.get_event(event_id)
                    if not event or event.media_revision != media_rev:
                        continue
                    ann = event.annotations.get(ANNOTATION_NAMESPACE, {})
                    if ann.get("status") in (STATUS_COMPLETE, STATUS_SKIPPED, STATUS_FAILED):
                        continue

                    # Reserve recording slot only when confirmed a new job will be queued
                    ann_gen = await self.rec_api.store.reserve_annotation(
                        event_id=event_id,
                        namespace=ANNOTATION_NAMESPACE,
                        expected_media_revision=media_rev,
                        worker_id=self.worker.worker_id,
                        ttl_seconds=res_ttl,
                    )
                    if ann_gen is None:
                        continue

                    _, enqueued = await self.job_store.enqueue_job(
                        event_id=event_id,
                        media_revision=media_rev,
                        profile_digest=digest,
                        is_manual=False,
                        annotation_generation=ann_gen,
                        profile_payload=self.profile_payload,
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
        if not self.rec_api.store.is_open:
            return

        if self.auto_analyze:
            async with self._get_event_lock(event_id):
                event = await self.rec_api.get_event(event_id)
                if event and event.capture_state in ("ready", "partial"):
                    ann_dict = event.annotations.get(ANNOTATION_NAMESPACE, {})
                    should_enqueue = True
                    if ann_dict:
                        ann_status = ann_dict.get("status")
                        ann_media_rev = ann_dict.get("media_revision")
                        if (
                            ann_status in (STATUS_COMPLETE, STATUS_SKIPPED, STATUS_FAILED)
                            and ann_media_rev == event.media_revision
                        ):
                            should_enqueue = False

                    if should_enqueue and await self.job_store.should_auto_enqueue(
                        event_id, event.media_revision
                    ):
                        res_ttl = derive_reservation_ttl(self.job_deadline)
                        ann_gen = await self.rec_api.store.reserve_annotation(
                            event_id=event_id,
                            namespace=ANNOTATION_NAMESPACE,
                            expected_media_revision=event.media_revision,
                            worker_id=self.worker.worker_id,
                            ttl_seconds=res_ttl,
                        )
                        if ann_gen is not None:
                            _, enqueued = await self.job_store.enqueue_job(
                                event_id=event_id,
                                media_revision=event.media_revision,
                                profile_digest=self.profile_digest,
                                is_manual=False,
                                annotation_generation=ann_gen,
                                profile_payload=self.profile_payload,
                            )
                            if enqueued:
                                self.worker.notify()

        # Check if this event belongs to incidents and notify them to re-evaluate fusion
        if self.enable_fusion and hasattr(self.rec_api, "list_event_incidents"):
            try:
                member_incidents = await self.rec_api.list_event_incidents(event_id)
                for inc in member_incidents:
                    task = asyncio.create_task(
                        self.async_handle_incident_notification(
                            inc.incident_id, inc.membership_revision
                        )
                    )
                    self._notification_tasks.add(task)
                    task.add_done_callback(self._notification_tasks.discard)
            except Exception as err:
                _LOGGER.warning(
                    "Error checking incident membership for event %s: %s", event_id, err
                )

    async def async_handle_incident_notification(self, incident_id: str, revision: int) -> None:
        """Handle post-commit incident notification to enqueue visual fusion when eligible."""
        if not self.enable_fusion or not self.rec_api.store.is_open or not self.fusion_job_store:
            return

        async with self._get_incident_lock(incident_id):
            inputs = await self.rec_api.get_incident_inputs(incident_id, ANNOTATION_NAMESPACE)
            if not inputs:
                return

            incident = inputs.incident
            ann = getattr(incident, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}
            payload_status = ann.get("status")
            if isinstance(ann.get("payload"), dict):
                payload_status = ann["payload"].get("status", payload_status)
            if (
                payload_status in (STATUS_COMPLETE, STATUS_SKIPPED, STATUS_FAILED)
                and ann.get("input_token") == inputs.input_token
                and ann.get("profile_digest") == self.fusion_profile_digest
            ):
                return

            # Guard queue: if job already exists for this exact input state, do not reserve or re-enqueue
            existing_job = await self.fusion_job_store.get_job_by_inputs(
                incident_id, inputs.input_token, self.fusion_profile_digest
            )
            if existing_job and existing_job.status in ("pending", "leased", "complete", "skipped", "failed"):
                return

            res_ttl = derive_reservation_ttl(self.fusion_job_deadline)
            ann_gen = await self.rec_api.reserve_incident_annotation(
                incident_id=incident_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_input_token=inputs.input_token,
                input_namespace=ANNOTATION_NAMESPACE,
                profile_digest=self.fusion_profile_digest,
                worker_id=self.worker.worker_id,
                ttl_seconds=res_ttl,
            )
            if ann_gen is None:
                return

            _, enqueued = await self.fusion_job_store.enqueue_fusion_job(
                incident_id=incident_id,
                input_token=inputs.input_token,
                profile_digest=self.fusion_profile_digest,
                is_manual=False,
                annotation_generation=ann_gen,
                profile_payload=self.fusion_profile_payload,
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

            res_ttl = derive_reservation_ttl(self.job_deadline)
            ann_gen = await self.rec_api.store.reserve_annotation(
                event_id=event_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_media_revision=event.media_revision,
                worker_id=self.worker.worker_id,
                ttl_seconds=res_ttl,
            )
            if ann_gen is None:
                _LOGGER.warning(
                    "Failed to reserve annotation for manual analyze of event %s", event_id
                )
                return False

            digest = self.profile_digest
            _, enqueued = await self.job_store.enqueue_job(
                event_id=event_id,
                media_revision=event.media_revision,
                profile_digest=digest,
                is_manual=True,
                annotation_generation=ann_gen,
                profile_payload=self.profile_payload,
            )
            if enqueued:
                self.worker.notify()
            return enqueued

    async def async_manual_analyze_incident(self, incident_id: str) -> bool:
        """Handle manual retry for incident: reserves annotation and enqueues manual job."""
        if not self.enable_fusion or not self.rec_api.store.is_open or not self.fusion_job_store:
            return False

        async with self._get_incident_lock(incident_id):
            inputs = await self.rec_api.get_incident_inputs(incident_id, ANNOTATION_NAMESPACE)
            if not inputs:
                return False

            res_ttl = derive_reservation_ttl(self.fusion_job_deadline)
            ann_gen = await self.rec_api.reserve_incident_annotation(
                incident_id=incident_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_input_token=inputs.input_token,
                input_namespace=ANNOTATION_NAMESPACE,
                profile_digest=self.fusion_profile_digest,
                worker_id=self.worker.worker_id,
                ttl_seconds=res_ttl,
            )
            if ann_gen is None:
                _LOGGER.warning(
                    "Failed to reserve annotation for manual analyze of incident %s", incident_id
                )
                return False

            _, enqueued = await self.fusion_job_store.enqueue_fusion_job(
                incident_id=incident_id,
                input_token=inputs.input_token,
                profile_digest=self.fusion_profile_digest,
                is_manual=True,
                annotation_generation=ann_gen,
                profile_payload=self.fusion_profile_payload,
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
        if self.fusion_job_store:
            await self.fusion_job_store.close()
