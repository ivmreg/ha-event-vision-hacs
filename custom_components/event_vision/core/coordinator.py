"""Durable VisionCoordinator for Event Vision pure core.

Strict boundary: Pure Python, zero Home Assistant imports, zero network calls.
Manages catchup scanning, subscription dispatch, and manual retry.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from ..const import (
    ANNOTATION_NAMESPACE,
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
    STATUS_SKIPPED,
    VALID_SAMPLING_POLICIES,
)
from .fusion_store import FusionJobStore
from .models import (
    build_fusion_job_payload,
    build_job_payload,
    canonical_profile_json,
    compute_fusion_profile_digest,
    compute_profile_digest,
    derive_reservation_ttl,
)
from .store import AnalysisJobStore
from .worker import AnalysisWorker

_LOGGER = logging.getLogger(__name__)


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
                    if not await self.job_store.should_auto_enqueue(event_id, media_rev):
                        continue

                    event = await self.rec_api.get_event(event_id)
                    if not event or event.media_revision != media_rev:
                        continue
                    ann = event.annotations.get(ANNOTATION_NAMESPACE, {})
                    if ann.get("status") in (STATUS_COMPLETE, STATUS_SKIPPED, STATUS_FAILED):
                        continue

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
