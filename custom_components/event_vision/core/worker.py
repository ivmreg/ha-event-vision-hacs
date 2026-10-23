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
import logging
import math
import random
from collections.abc import Awaitable
from dataclasses import replace
from datetime import timedelta
from typing import Any
from weakref import WeakValueDictionary

from custom_components.event_recording.contracts import RecordingConsumerAPI

from ..const import (
    ANNOTATION_NAMESPACE,
    BASE_BACKOFF_SECONDS,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_FUSION_JOB_DEADLINE,
    DEFAULT_FUSION_MAX_FRAMES,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    GENERIC_MEDIA_PROMPT,
    MAX_BACKOFF_SECONDS,
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
from .activity_details import build_activity_details
from .cache import FrameCache
from .extractor import FrameExtractor
from .fusion_store import FusionJobStore
from .gemini import GeminiClient
from .models import (
    AnalysisAnnotation,
    AnalysisError,
    AnalysisJob,
    ExtractedFrame,
    FrameExtractionError,
    FusionAnnotation,
    FusionJob,
    FusionViewFrame,
    FusionViewInput,
    IncidentInputs,
    ObservationCoverage,
    build_job_payload,
    canonical_profile_json,
    compute_profile_digest,
    derive_lease_pin_seconds,
    derive_queue_lease_seconds,
    derive_reservation_ttl,
    format_iso,
    sanitize_error_code,
    utc_now,
)
from .profiles import load_profile
from .sampling import plan_event_sampling
from .store import AnalysisJobStore

_LOGGER = logging.getLogger(__name__)


async def _settled_operation(operation: Awaitable[Any]) -> Any:
    """Keep ownership until a critical asynchronous mutation physically settles.

    Cancelling an awaiter does not stop a recorder's SQLite executor thread.
    Drain that operation before allowing runtime recovery to reuse the claim.
    """
    task = asyncio.ensure_future(operation)
    cancelled = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            cancelled = True
        except Exception:
            break
    if cancelled:
        if not task.cancelled():
            task.exception()  # Retrieve a failure even when cancellation wins.
        raise asyncio.CancelledError
    return task.result()


def is_retryable_provider_error(err: Exception) -> bool:
    """Return True only for retryable provider/network classes.

    Recognizes RATE_LIMIT (429), HTTP_5xx server errors, NETWORK_ERROR,
    and provider timeout (explicitly tagged via timeout_source='provider').
    Excludes job deadline timeout (timeout_source='job'), extraction errors,
    media unavailable, invalid profile, and permanent client errors (400, 401, 403).
    """
    if not isinstance(err, AnalysisError):
        return False
    if not err.retryable or isinstance(err, FrameExtractionError):
        return False

    timeout_source = getattr(err, "timeout_source", None)
    if timeout_source is not None:
        return timeout_source == "provider"

    code = err.code.upper()
    if code in (
        "JOB_TIMEOUT",
        "FRAME_EXTRACTION_FAILED",
        "MEDIA_UNAVAILABLE",
        "INVALID_PROFILE",
        "SCHEMA_ERROR",
        "PRIVACY_LEAK_PREVENTED",
    ):
        return False
    if "RATE_LIMIT" in code or "429" in code:
        return True
    if any(code.startswith(f"HTTP_{s}") for s in (500, 502, 503, 504)) or code == "HTTP_5XX":
        return True
    if "NETWORK" in code or "CONNECTION" in code:
        return True
    if code in ("PROVIDER_TIMEOUT", "TIMEOUT"):
        return True
    return False


def compute_retry_delay(
    attempts: int,
    retry_delay: float | None = None,
    base_backoff: float = BASE_BACKOFF_SECONDS,
    max_backoff: float = MAX_BACKOFF_SECONDS,
    apply_jitter: bool = True,
) -> float:
    """Compute bounded retry delay honoring provider Retry-After or exponential backoff + jitter."""
    if isinstance(base_backoff, bool) or not isinstance(base_backoff, (int, float)):
        raise ValueError("base_backoff must be a finite positive number")
    if isinstance(max_backoff, bool) or not isinstance(max_backoff, (int, float)):
        raise ValueError("max_backoff must be a finite positive number")
    base_val = float(base_backoff)
    max_val = float(max_backoff)
    if math.isnan(base_val) or not math.isfinite(base_val) or base_val <= 0.0:
        raise ValueError("base_backoff must be a finite positive number")
    if math.isnan(max_val) or not math.isfinite(max_val) or max_val <= 0.0:
        raise ValueError("max_backoff must be a finite positive number")
    effective_max = min(max_val, float(MAX_BACKOFF_SECONDS))

    if retry_delay is not None:
        if (
            not isinstance(retry_delay, bool)
            and isinstance(retry_delay, (int, float))
            and math.isfinite(retry_delay)
            and not math.isnan(retry_delay)
        ):
            if retry_delay > 0:
                return min(effective_max, float(retry_delay))

    exp_delay = min(effective_max, base_val * (2.0 ** max(0, attempts)))
    if apply_jitter:
        jitter = random.uniform(0.0, min(10.0, 0.1 * exp_delay))
        return min(effective_max, exp_delay + jitter)
    return exp_delay


class AnalysisWorker:
    """Background worker executing durable analysis jobs with media leases and CAS."""

    def __init__(
        self,
        job_store: AnalysisJobStore,
        gemini_client: GeminiClient | None,
        frame_extractor: FrameExtractor,
        recording_api: RecordingConsumerAPI,
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
        cache: FrameCache | None = None,
    ) -> None:
        self._job_store = job_store
        self._fusion_job_store = fusion_job_store
        self._cache = cache or getattr(frame_extractor, "cache", None)
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
        self._claim_lock = asyncio.Lock()
        self._event_claims: dict[asyncio.Task[None], AnalysisJob] = {}
        self._prefer_fusion = False
        self.event_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()
        self.incident_locks: WeakValueDictionary[str, asyncio.Lock] = WeakValueDictionary()

    def event_lock(self, event_id: str) -> asyncio.Lock:
        """Serialize short queue/reservation transitions with manual retries."""
        lock = self.event_locks.get(event_id)
        if lock is None:
            lock = asyncio.Lock()
            self.event_locks[event_id] = lock
        return lock

    def incident_lock(self, incident_id: str) -> asyncio.Lock:
        """Serialize short queue/reservation transitions for an incident."""
        lock = self.incident_locks.get(incident_id)
        if lock is None:
            lock = asyncio.Lock()
            self.incident_locks[incident_id] = lock
        return lock

    @property
    def cache(self) -> FrameCache | None:
        """Return attached FrameCache instance if configured."""
        return self._cache

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

    def set_recording_api(self, recording_api: RecordingConsumerAPI) -> None:
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
        async with self._claim_lock:
            # Repair even while cooldown/slots/recorder availability block new work.
            # Owners remain protected through deadline cancellation and cleanup.
            await _settled_operation(
                self._job_store.recover_expired_leases(
                    exclude_job_ids=tuple({job.id for job in self._event_claims.values()}),
                    limit=50,
                )
            )
            return await self._claim_next_job()

    async def _claim_next_job(self) -> bool:
        """Lease and register ownership under the same admission lock as repair."""
        if not self._rec_api.is_open:
            return False
        if self._semaphore.locked():
            return False

        # Check shared provider cooldown before leasing
        cooldown_until = await self._job_store.get_provider_cooldown()
        if cooldown_until is not None and cooldown_until > utc_now():
            return False

        try:
            await self._semaphore.acquire()
        except Exception:
            return False

        permit_acquired = True
        try:
            queues = [False, True] if not self._prefer_fusion else [True, False]
            for fusion in queues:
                if fusion:
                    if not self._enable_fusion or not self._fusion_job_store:
                        continue
                    job = await self._fusion_job_store.lease_fusion_job(
                        self._worker_id,
                        lease_seconds=derive_queue_lease_seconds(self._fusion_job_deadline),
                    )
                else:
                    job = await self._job_store.lease_job(
                        self._worker_id,
                        lease_seconds=derive_queue_lease_seconds(self._job_deadline),
                    )
                if job:
                    self._prefer_fusion = not fusion
                    execute = self._guarded_execute_fusion if fusion else self._guarded_execute
                    task = asyncio.create_task(execute(job))
                    self._background_tasks.add(task)
                    if not fusion:
                        self._event_claims[task] = job
                    task.add_done_callback(self._claim_finished)
                    return True

            self._semaphore.release()
            permit_acquired = False
            return False
        except BaseException:
            if permit_acquired:
                self._semaphore.release()
            raise

    def _claim_finished(self, task: asyncio.Task[None]) -> None:
        self._background_tasks.discard(task)
        self._event_claims.pop(task, None)
        if not task.cancelled() and (error := task.exception()) is not None:
            _LOGGER.error("Analysis task exited: %s", sanitize_error_code(error))

    async def _settle_unfinished_claim(self, job: AnalysisJob, cancelled: bool) -> None:
        """A normal early exit must not leave a valid lease without an owner."""
        try:
            if cancelled or not self._rec_api.is_open:
                await self._job_store.defer_job(job.id, job.generation, claim_token=job.claim_token)
            else:
                await self._job_store.fail_job(
                    job.id,
                    job.generation,
                    "WORKER_EXITED",
                    retryable=True,
                    backoff_seconds=compute_retry_delay(job.attempts),
                    claim_token=job.claim_token,
                )
        except Exception as err:
            # Leave the durable lease for the next bounded repair pass.
            _LOGGER.warning("Analysis claim settlement deferred: %s", sanitize_error_code(err))

    async def _guarded_execute(self, job: AnalysisJob) -> None:
        await self._run_claimed(job, fusion=False)

    async def _run_claimed(self, job: AnalysisJob | FusionJob, *, fusion: bool) -> None:
        """Shared deadline, failure, cancellation, and concurrency ownership."""
        execute = self._execute_fusion_job if fusion else self._execute_job
        failure = self._handle_fusion_job_failure if fusion else self._handle_job_failure
        deadline = self._fusion_job_deadline if fusion else self._job_deadline
        cancelled = False
        if job.profile_payload:
            try:
                deadline = load_profile(
                    job.profile_payload, job.profile_digest, fusion=fusion
                ).deadline
            except AnalysisError:
                pass  # Execution publishes the invalid-profile failure with its CAS identity.
        try:
            try:
                await asyncio.wait_for(execute(job), timeout=deadline)
            except TimeoutError:
                await failure(
                    job,
                    AnalysisError(
                        "TIMEOUT",
                        "Execution deadline exceeded",
                        retryable=True,
                        timeout_source="job",
                    ),
                )
            except asyncio.CancelledError:
                cancelled = True
                raise  # Durable claims remain recoverable; input cleanup runs inside execute.
            except Exception as err:
                await failure(job, err)
        except Exception as err:
            _LOGGER.error("Analysis failure persistence deferred: %s", sanitize_error_code(err))
        finally:
            try:
                if not fusion:
                    await _settled_operation(self._settle_unfinished_claim(job, cancelled))
            finally:
                self._semaphore.release()

    async def _execute_job(self, job: AnalysisJob) -> None:
        """Execute single analysis job with derived media lease pin and CAS protection."""
        if not await self._job_store.is_job_current(
            job.id, job.generation, claim_token=job.claim_token
        ):
            _LOGGER.info(
                "Job %s (gen %s) is no longer current; aborting execution",
                job.event_id,
                job.generation,
            )
            return

        res_deadline = self._job_deadline
        if job.profile_payload:
            try:
                res_deadline = load_profile(job.profile_payload, job.profile_digest).deadline
            except AnalysisError:
                pass
        res_ttl = derive_reservation_ttl(res_deadline)

        # 1. Ensure or renew annotation reservation under per-event lock BEFORE payload validation
        async with self.event_lock(job.event_id):
            if not await self._job_store.is_job_current(
                job.id, job.generation, claim_token=job.claim_token
            ):
                return
            event = await self._rec_api.get_event(job.event_id)
            if not event or event.media_revision != job.media_revision:
                await self._job_store.supersede_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                return
            existing_ann = event.annotations.get(ANNOTATION_NAMESPACE, {})
            existing_gen = existing_ann.get("generation", 0)

            annotation_gen = job.annotation_generation
            if existing_gen == 0:
                if annotation_gen not in (None, 1):
                    await self._job_store.supersede_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
                    return
                annotation_gen = await _settled_operation(
                    self._rec_api.reserve_annotation(
                        event_id=job.event_id,
                        namespace=ANNOTATION_NAMESPACE,
                        expected_media_revision=job.media_revision,
                        worker_id=self._worker_id,
                        ttl_seconds=res_ttl,
                    )
                )
                if annotation_gen is None:
                    await self._job_store.supersede_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
                    return
                await self._job_store.update_job_annotation_generation(
                    job.id, job.generation, annotation_gen, claim_token=job.claim_token
                )
                job = replace(job, annotation_generation=annotation_gen)
            else:
                if annotation_gen is None or existing_gen != annotation_gen:
                    await self._job_store.supersede_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
                    return
                renewed = await _settled_operation(
                    self._rec_api.renew_annotation_reservation(
                        event_id=job.event_id,
                        namespace=ANNOTATION_NAMESPACE,
                        expected_media_revision=job.media_revision,
                        expected_generation=annotation_gen,
                        worker_id=self._worker_id,
                        ttl_seconds=res_ttl,
                    )
                )
                if not renewed:
                    await self._job_store.supersede_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
                    return

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

        profile = load_profile(job.profile_payload, job.profile_digest)
        semantic_profile, budget = profile.semantic, profile.budget

        job_model = semantic_profile["model"]
        job_instructions = semantic_profile["instructions"]
        job_base_prompt = semantic_profile["base_prompt"]
        job_max_frames = semantic_profile["max_frames"]
        job_policy = semantic_profile["sampling_policy"]
        job_details_version = semantic_profile.get("activity_details_version", 0)

        job_deadline = budget["job_deadline"]
        job_provider_timeout = budget["provider_timeout"]
        job_extraction_timeout = budget["extraction_timeout"]

        _LOGGER.debug(
            "Starting execution for job %s: event=%s, gen=%s, model=%s, policy=%s, max_frames=%d, deadline=%.1fs",
            job.id,
            job.event_id,
            job.generation,
            job_model,
            job_policy,
            job_max_frames,
            job_deadline,
        )

        lease_pin = None
        try:
            # 3. Acquire media lease pin with derived duration bound
            lease_duration = derive_lease_pin_seconds(job_deadline)
            lease_pin = await self._rec_api.pin_media_lease(
                event_id=job.event_id,
                lease_seconds=lease_duration,
                reason="event_vision analysis",
            )
            _LOGGER.debug(
                "Acquired media lease pin for event %s (pin=%s, duration=%.1fs)",
                job.event_id,
                lease_pin,
                lease_duration,
            )

            # 4. Set processing status via CAS using claimed generation
            async with self.event_lock(job.event_id):
                if not await self._job_store.is_job_current(
                    job.id, job.generation, claim_token=job.claim_token
                ):
                    return

                proc_annotation = AnalysisAnnotation(
                    status=STATUS_PROCESSING,
                    media_revision=job.media_revision,
                    generation=annotation_gen,
                    model=job_model,
                    updated_at=format_iso(utc_now()),
                )
                if not await _settled_operation(
                    self._rec_api.set_annotation_cas(
                        event_id=job.event_id,
                        expected_media_revision=job.media_revision,
                        namespace=ANNOTATION_NAMESPACE,
                        payload=proc_annotation.to_dict(),
                        expected_generation=annotation_gen,
                    )
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
                await self._job_store.supersede_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                return

            available_media = [m for m in event.media if getattr(m, "available", True)]
            if not available_media:
                if not await self._job_store.is_job_current(
                    job.id, job.generation, claim_token=job.claim_token
                ):
                    return
                skip_annotation = AnalysisAnnotation(
                    status=STATUS_SKIPPED,
                    media_revision=job.media_revision,
                    generation=annotation_gen,
                    error_code="MEDIA_UNAVAILABLE",
                    model=self._gemini.model,
                    updated_at=format_iso(utc_now()),
                )
                await _settled_operation(
                    self._rec_api.set_annotation_cas(
                        event_id=job.event_id,
                        expected_media_revision=job.media_revision,
                        namespace=ANNOTATION_NAMESPACE,
                        payload=skip_annotation.to_dict(),
                        expected_generation=annotation_gen,
                    )
                )
                await self._job_store.skip_job(
                    job.id, job.generation, "MEDIA_UNAVAILABLE", claim_token=job.claim_token
                )
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

            available_clips_count = len(available_media)
            planned_frames_count = min(len(planned_anchors), job_max_frames)
            planned_clips_count = len(anchors_by_media)
            unselected_clips_count = max(0, available_clips_count - planned_clips_count)
            sampled_media_ids: set[str] = set()

            _LOGGER.debug(
                "Planned %d frame anchors across %d clips for event %s (policy=%s, available_clips=%d)",
                planned_frames_count,
                planned_clips_count,
                job.event_id,
                job_policy,
                available_clips_count,
            )

            accumulated_frames = await self._extract_frames_for_event(
                event,
                job_max_frames,
                job_policy,
                job_extraction_timeout,
                planned_anchors=planned_anchors,
                sampled_media_ids=sampled_media_ids,
            )

            if not accumulated_frames:
                raise AnalysisError(
                    "FRAME_EXTRACTION_FAILED", "No valid frames could be extracted", retryable=False
                )

            # Trim strictly to configured max_frames
            accumulated_frames = accumulated_frames[:job_max_frames]
            extracted_frames_count = len(accumulated_frames)
            sampled_clips_count = len(sampled_media_ids)
            missing_clips_count = max(0, planned_clips_count - sampled_clips_count)
            is_complete_obs = (
                extracted_frames_count == planned_frames_count and missing_clips_count == 0
            )
            obs_state = "complete" if is_complete_obs else "partial"

            capture_state = getattr(event, "capture_state", None)
            uncertainties = [
                getattr(m, "time_uncertainty_seconds", None)
                for m in available_media
                if getattr(m, "time_uncertainty_seconds", None) is not None
            ]
            capture_uncertainty = max(uncertainties) if uncertainties else None

            coverage_meta = ObservationCoverage(
                planned_frames=planned_frames_count,
                extracted_frames=extracted_frames_count,
                planned_clips=planned_clips_count,
                sampled_clips=sampled_clips_count,
                missing_clips=missing_clips_count,
                observation_state=obs_state,
                is_partial=not is_complete_obs,
                available_clips=available_clips_count,
                unselected_clips=unselected_clips_count,
                source_capture_state=capture_state,
                source_time_uncertainty_seconds=capture_uncertainty,
            )

            _LOGGER.debug(
                "Frame extraction complete for event %s: extracted=%d/%d frames across %d/%d clips (state=%s)",
                job.event_id,
                extracted_frames_count,
                planned_frames_count,
                sampled_clips_count,
                planned_clips_count,
                obs_state,
            )

            # 6. Call direct Gemini REST generateContent
            details_options = (
                {"activity_details_version": job_details_version} if job_details_version else {}
            )
            result = await self._gemini.generate_analysis(
                frames=accumulated_frames,
                prompt_instructions=job_instructions,
                timeout_seconds=job_provider_timeout,
                model=job_model,
                base_prompt=job_base_prompt,
                **details_options,
            )

            # 7. Post CAS complete state with frame provenance and returned modelVersion
            if not await self._job_store.is_job_current(
                job.id, job.generation, claim_token=job.claim_token
            ):
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
                coverage=coverage_meta,
                activity_details=(
                    build_activity_details(
                        getattr(result, "activity_details", None),
                        accumulated_frames,
                        available_media,
                        version=job_details_version,
                    )
                    if result.activity is True
                    else None
                ),
            )

            committed = await _settled_operation(
                self._rec_api.set_annotation_cas(
                    event_id=job.event_id,
                    expected_media_revision=job.media_revision,
                    namespace=ANNOTATION_NAMESPACE,
                    payload=complete_annotation.to_dict(),
                    expected_generation=annotation_gen,
                )
            )

            if committed:
                await self._job_store.complete_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                _LOGGER.info(
                    "Successfully analyzed and committed event %s (media_rev=%s, gen=%s)",
                    job.event_id,
                    job.media_revision,
                    annotation_gen,
                )
                _LOGGER.debug(
                    "Analysis complete committed for event %s: summary_len=%d, activity=%s, objects_count=%d",
                    job.event_id,
                    len(result.summary),
                    result.activity,
                    len(result.objects),
                )
            else:
                _LOGGER.warning(
                    "CAS commit rejected on completion for %s; stale generation or media revision",
                    job.event_id,
                )
                await self._job_store.supersede_job(
                    job.id, job.generation, claim_token=job.claim_token
                )

        except Exception as err:
            if not self._rec_api.is_open:
                await self._job_store.defer_job(job.id, job.generation, claim_token=job.claim_token)
            else:
                await self._handle_job_failure(job, err)
        finally:
            # 8. Release media lease pin in finally block
            await self._cleanup_media_pins([lease_pin] if lease_pin else [])
            if lease_pin:
                _LOGGER.debug("Released media lease pin %s for event %s", lease_pin, job.event_id)

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
                    await self._rec_api.release_media_pin(token)
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
        retry_delay = getattr(err, "retry_delay", None) if isinstance(err, AnalysisError) else None

        if is_retryable_provider_error(err):
            backoff = compute_retry_delay(job.attempts, retry_delay=retry_delay)
            cooldown_until = utc_now() + timedelta(seconds=backoff)
            await self._job_store.extend_provider_cooldown(cooldown_until)
        else:
            backoff = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2**job.attempts))

        # Check if job is still current in our store
        if not await self._job_store.is_job_current(
            job.id, job.generation, claim_token=job.claim_token
        ):
            _LOGGER.info("Job %s was superseded; ignoring failure handler", job.event_id)
            return

        current = await self._job_store.get_job(
            job.event_id, job.media_revision, job.profile_digest
        )
        if current is None or current.generation != job.generation:
            return
        token = job.claim_token
        job = current
        # Update local job store with per-claim fence check
        failed_ok = await self._job_store.fail_job(
            job.id,
            job.generation,
            error_code=clean_code,
            retryable=retryable,
            backoff_seconds=backoff,
            claim_token=token,
        )
        if not failed_ok:
            return

        job_max_attempts = self._max_attempts
        job_model = (
            self._gemini.model
            if self._gemini and isinstance(getattr(self._gemini, "model", None), str)
            else (self._model if isinstance(self._model, str) else "gemini-flash-lite-latest")
        )
        if job.profile_payload:
            try:
                profile = load_profile(job.profile_payload, job.profile_digest)
                job_model = profile.semantic["model"]
                job_max_attempts = profile.budget["max_attempts"]
            except AnalysisError:
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
                        posted = await _settled_operation(
                            self._rec_api.set_annotation_cas(
                                event_id=job.event_id,
                                expected_media_revision=job.media_revision,
                                namespace=ANNOTATION_NAMESPACE,
                                payload=fail_ann.to_dict(),
                                expected_generation=claimed_gen,
                            )
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
        *,
        planned_anchors: list[Any] | None = None,
        sampled_media_ids: set[str] | None = None,
        reuse_compatible: bool = False,
    ) -> list[ExtractedFrame]:
        available_media = [m for m in getattr(event, "media", []) if getattr(m, "available", True)]
        if not available_media:
            return []

        event_rev = getattr(event, "media_revision", 1)

        # Plan event sampling across ALL available media items upfront (never chunk 1 alone consuming entire budget)
        planned_anchors = (
            planned_anchors
            if planned_anchors is not None
            else plan_event_sampling(
                media_items=available_media,
                policy=policy,
                max_frames=max_frames,
                event_start_at=getattr(event, "start_at", None),
                source_events=getattr(event, "source_events", ()),
            )
        )
        if not planned_anchors:
            return []

        anchors_by_media: dict[str, list[Any]] = {}
        for a in planned_anchors:
            anchors_by_media.setdefault(a.media_id, []).append(a)

        media_by_id = {m.id: m for m in available_media}
        accumulated_frames: list[ExtractedFrame] = []

        for m_id, item_anchors in anchors_by_media.items():
            if len(accumulated_frames) >= max_frames:
                break

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
                getattr(item_anchors[0], "offset_quality", "unknown") if item_anchors else "unknown"
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
                    media_revision=event_rev,
                    reuse_compatible=reuse_compatible,
                )
                accumulated_frames.extend(frames)
                if frames and sampled_media_ids is not None:
                    sampled_media_ids.add(m_id)
            except Exception as err:
                _LOGGER.warning(
                    "Frame extraction error on media %s: %s", m_id, sanitize_error_code(err)
                )

        return accumulated_frames[:max_frames]

    async def _guarded_execute_fusion(self, job: FusionJob) -> None:
        await self._run_claimed(job, fusion=True)

    async def _execute_fusion_job(self, job: FusionJob) -> None:
        """Execute single multi-camera incident visual fusion job with CAS protection."""
        if not self._fusion_job_store or not await self._fusion_job_store.is_fusion_job_current(
            job.id, job.generation, claim_token=job.claim_token
        ):
            _LOGGER.info(
                "Fusion job %s (gen %s) is no longer current; aborting execution",
                job.incident_id,
                job.generation,
            )
            return

        if not self._enable_fusion:
            await self._fusion_job_store.skip_fusion_job(
                job.id, job.generation, "FUSION_DISABLED", claim_token=job.claim_token
            )
            return

        if self._gemini is None:
            await self._fusion_job_store.fail_fusion_job(
                job.id,
                job.generation,
                "MISSING_API_KEY",
                retryable=False,
                claim_token=job.claim_token,
            )
            return

        if not hasattr(self._rec_api, "get_incident_inputs"):
            await self._fusion_job_store.fail_fusion_job(
                job.id,
                job.generation,
                "INCIDENT_API_UNAVAILABLE",
                retryable=False,
                claim_token=job.claim_token,
            )
            return

        async with self.incident_lock(job.incident_id):
            if not await self._fusion_job_store.is_fusion_job_current(
                job.id, job.generation, claim_token=job.claim_token
            ):
                return

            profile = load_profile(job.profile_payload, job.profile_digest, fusion=True)
            semantic, budget = profile.semantic, profile.budget
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
                await self._fusion_job_store.supersede_fusion_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                return

            if inputs.input_token != job.input_token:
                _LOGGER.info(
                    "Fusion job %s input token %s superseded before provider call by %s",
                    job.incident_id,
                    job.input_token,
                    inputs.input_token,
                )
                await self._fusion_job_store.supersede_fusion_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
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
                await self._fusion_job_store.defer_fusion_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                return

            # Available completed member events with media
            available_events = [
                ev
                for ev in events
                if (getattr(ev, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}).get(
                    "status"
                )
                == STATUS_COMPLETE
                and getattr(ev, "media", None)
                and any(getattr(m, "available", True) for m in ev.media)
            ]

            ann_gen = job.annotation_generation or 1
            renewed = await self._rec_api.renew_incident_annotation_reservation(
                incident_id=job.incident_id,
                namespace=ANNOTATION_NAMESPACE,
                expected_generation=ann_gen,
                expected_input_token=job.input_token,
                profile_digest=job.profile_digest,
                worker_id=self._worker_id,
                ttl_seconds=derive_reservation_ttl(job_deadline),
            )
            if not renewed:
                await self._fusion_job_store.supersede_fusion_job(
                    job.id, job.generation, claim_token=job.claim_token
                )
                return

            # Group available events by unique camera visual view
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
                    job.id, job.generation, "INSUFFICIENT_VIEWS", claim_token=job.claim_token
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
                    job.id, job.generation, "TOO_MANY_VIEWS_FOR_BUDGET", claim_token=job.claim_token
                )
                return

        # Pin all member media
        pins: list[Any] = []
        try:
            lease_duration = derive_lease_pin_seconds(job_deadline)
            for ev in available_events:
                pin = await self._rec_api.pin_media_lease(
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
                        max(1, frames_needed // len(ev_list)) if len(ev_list) > 1 else frames_needed
                    )
                    raw_frames = await self._extract_frames_for_event(
                        event=ev,
                        max_frames=per_ev_limit,
                        policy=job_policy,
                        extraction_timeout=job_extraction_timeout,
                        reuse_compatible=True,
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
                    prior_ann = getattr(ev, "annotations", {}).get(ANNOTATION_NAMESPACE, {}) or {}
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
                    job.id, job.generation, claim_token=job.claim_token
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
                    await self._fusion_job_store.supersede_fusion_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
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
                    job.id, job.generation, claim_token=job.claim_token
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
                    await self._fusion_job_store.complete_fusion_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
                else:
                    _LOGGER.warning(
                        "CAS commit failed for fusion job %s; superseding", job.incident_id
                    )
                    await self._fusion_job_store.supersede_fusion_job(
                        job.id, job.generation, claim_token=job.claim_token
                    )
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
        retry_delay = getattr(err, "retry_delay", None) if isinstance(err, AnalysisError) else None

        job_max_attempts = self._max_attempts
        if job.profile_payload:
            try:
                budget = load_profile(job.profile_payload, job.profile_digest, fusion=True).budget
                job_max_attempts = budget["max_attempts"]
            except Exception:
                pass

        if is_retryable_provider_error(err):
            backoff = compute_retry_delay(job.attempts, retry_delay=retry_delay)
            cooldown_until = utc_now() + timedelta(seconds=backoff)
            await self._job_store.extend_provider_cooldown(cooldown_until)
        else:
            backoff = min(MAX_BACKOFF_SECONDS, BASE_BACKOFF_SECONDS * (2**job.attempts))

        if retryable and (job.attempts + 1) < job_max_attempts:
            await self._fusion_job_store.fail_fusion_job(
                job.id,
                job.generation,
                err_code,
                retryable=True,
                backoff_seconds=backoff,
                claim_token=job.claim_token,
            )
        else:
            transitioned = await self._fusion_job_store.fail_fusion_job(
                job.id,
                job.generation,
                err_code,
                retryable=False,
                claim_token=job.claim_token,
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


# Re-export VisionCoordinator from .coordinator for backward-compatible imports and test seams
from .coordinator import VisionCoordinator as VisionCoordinator  # noqa: E402
