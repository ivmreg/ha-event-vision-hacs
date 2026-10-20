"""Persistent SQLite AnalysisJobStore for Event Vision pure core.

Features:
- Pure Python (zero Home Assistant dependencies)
- Dedicated SQLite database at .storage/event_vision/<entry>/analysis.db
- Thread-safe off-loop execution using asyncio.to_thread
- Job identity: (event_id, media_revision, profile_digest)
- Atomic generation bumping on manual retry with synchronous=FULL
- Bounded concurrency, retry backoff (attempts <= max_attempts, backoff >= 1.0s)
- Lease-based worker claims with configurable deadline and restart recovery
- Backward-compatible with previously queued jobs under older profile digests
- Backed by common DurableQueueEngine core
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from ..const import (
    BASE_BACKOFF_SECONDS,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
)
from .models import (
    AnalysisJob,
    ensure_utc,
    utc_now,
)
from .queue import DurableQueueEngine, QueueSpec, normalize_claim_token

_LOGGER = logging.getLogger(__name__)


class AnalysisJobStore:
    """Independent SQLite store managing durable analysis jobs and leases."""

    def __init__(self, db_path: str | Path, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> None:
        spec = QueueSpec(
            table_name="analysis_jobs",
            target_column="event_id",
            revision_column="media_revision",
            revision_type="INTEGER",
            default_annotation_generation=1,
            has_manual_priority=True,
            order_by_sql="is_manual DESC, id ASC",
            extra_index_cols=(),
            recover_on_lease=False,
            increment_attempts_on_lease_expiry=False,
        )
        self._engine = DurableQueueEngine(
            db_path=db_path,
            spec=spec,
            default_max_attempts=max_attempts,
            base_backoff_seconds=BASE_BACKOFF_SECONDS,
        )

    @property
    def is_open(self) -> bool:
        """Return whether SQLite connection is active."""
        return self._engine.is_open

    @property
    def db_path(self) -> Path:
        """Return database path."""
        return self._engine.db_path

    async def open(self) -> None:
        """Open database connection off the event loop and initialize schema."""
        await self._engine.open()

    async def close(self) -> None:
        """Close SQLite connection cleanly."""
        await self._engine.close()

    async def get_provider_cooldown(self, provider: str = "default") -> datetime | None:
        """Return the datetime until which provider requests are cooled down, or None."""
        return await self._engine.get_cooldown(key=provider)

    async def extend_provider_cooldown(
        self,
        until_or_delay: datetime | str | float | int,
        provider: str = "default",
    ) -> datetime:
        """Extend provider cooldown to max(existing, new_until), never shortening concurrent cooldown."""
        return await self._engine.extend_cooldown(until_or_delay, key=provider)

    async def clear_provider_cooldown(self, provider: str = "default") -> None:
        """Clear provider cooldown for testing or administrative resets."""
        await self._engine.clear_cooldown(key=provider)

    # Public aliases for convenient caller access
    get_cooldown = get_provider_cooldown
    extend_cooldown = extend_provider_cooldown
    clear_cooldown = clear_provider_cooldown

    async def enqueue_job(
        self,
        event_id: str,
        media_revision: int,
        profile_digest: str,
        is_manual: bool = False,
        annotation_generation: int | None = None,
        profile_payload: str | dict[str, Any] | None = None,
    ) -> tuple[AnalysisJob, bool]:
        """Durable enqueue with atomic generation bump on manual retry.

        Returns (job, enqueued_boolean).
        """
        row, enqueued = await self._engine.enqueue(
            target_id=event_id,
            revision=media_revision,
            profile_digest=profile_digest,
            is_manual=is_manual,
            annotation_generation=annotation_generation,
            profile_payload=profile_payload,
            supersede_older_check=True,
        )
        return self._row_to_job(row), enqueued

    async def lease_job(
        self,
        worker_id: str,
        lease_seconds: float = DEFAULT_JOB_DEADLINE,
    ) -> AnalysisJob | None:
        """Lease the highest priority eligible pending job atomically."""
        row = await self._engine.lease(worker_id=worker_id, lease_seconds=lease_seconds)
        return self._row_to_job(row) if row else None

    async def complete_job(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Mark job as complete with transactional CAS verification."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.complete(
            job_id=job_id,
            generation=generation,
            claim_token=effective_token,
        )

    async def fail_job(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool = True,
        backoff_seconds: float = BASE_BACKOFF_SECONDS,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Fail job with CAS verification and exponential retry backoff."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.fail(
            job_id=job_id,
            generation=generation,
            error_code=error_code,
            retryable=retryable,
            backoff_seconds=backoff_seconds,
            claim_token=effective_token,
        )

    async def skip_job(
        self,
        job_id: int,
        generation: int,
        reason: str,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Mark job as skipped with CAS verification."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.skip(
            job_id=job_id,
            generation=generation,
            reason=reason,
            claim_token=effective_token,
        )

    async def defer_job(
        self,
        job_id: int,
        generation: int,
        delay_seconds: float | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Defer job to pending without burning an attempt."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.defer(
            job_id=job_id,
            generation=generation,
            delay_seconds=delay_seconds,
            claim_token=effective_token,
        )

    async def update_job_annotation_generation(
        self,
        job_id: int,
        generation: int,
        annotation_generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Update annotation generation with CAS generation check."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.update_annotation_generation(
            job_id=job_id,
            generation=generation,
            annotation_generation=annotation_generation,
            claim_token=effective_token,
        )

    async def update_job_payload(self, job_id: int, payload: str | dict[str, Any]) -> bool:
        """Update profile payload JSON for an existing job."""
        return await self._engine.update_payload(job_id=job_id, payload=payload)

    async def supersede_older_revisions(self, event_id: str, current_media_revision: int) -> int:
        return await self._engine.supersede_older_revisions(event_id, current_media_revision)

    async def supersede_job(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Retire only the rejected attempt, preserving newer jobs for the same media."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.supersede(
            job_id=job_id,
            generation=generation,
            claim_token=effective_token,
        )

    async def get_job(
        self, event_id: str, media_revision: int, profile_digest: str
    ) -> AnalysisJob | None:
        row = await self._engine.get_job_by_inputs(event_id, media_revision, profile_digest)
        return self._row_to_job(row) if row else None

    async def get_latest_job_for_event(
        self, event_id: str, media_revision: int | None = None
    ) -> AnalysisJob | None:
        row = await self._engine.get_latest_for_target(event_id, revision=media_revision)
        return self._row_to_job(row) if row else None

    async def is_job_current(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Return True if job is still active under the specified generation."""
        effective_token = normalize_claim_token(claim_token, lease_token, token)
        return await self._engine.is_current(
            job_id=job_id,
            generation=generation,
            claim_token=effective_token,
        )

    async def recover_expired_leases(self, force_all: bool = False) -> int:
        """Recover expired or in-flight leases preserving attempt counts."""
        return await self._engine.recover_expired_leases(force_all=force_all)

    async def get_stats(self) -> dict[str, int]:
        """Get aggregate counts of jobs by status."""
        return await self._engine.get_stats()

    async def has_pending_or_leased_jobs(self) -> bool:
        """Return True if any pending or leased jobs exist."""
        return await self._engine.has_pending_or_leased()

    async def migrate_legacy_pending_jobs(self, profile_digest: str, profile_payload: str) -> int:
        """Migrate legacy pending or leased jobs without profile payload to current profile key.

        Leaves historical completed, skipped, or failed archive results untouched.
        """
        return await self._engine.migrate_legacy_pending(profile_digest, profile_payload)

    async def should_auto_enqueue(self, event_id: str, media_revision: int) -> bool:
        return await self._engine.get_latest_for_target(event_id, revision=media_revision) is None

    async def compact(self) -> int:
        return await self._engine.compact_terminal()

    def _row_to_job(self, row: dict[str, Any]) -> AnalysisJob:
        return AnalysisJob(
            id=row["id"],
            event_id=row["event_id"],
            media_revision=row["media_revision"],
            profile_digest=row["profile_digest"],
            generation=row["generation"],
            annotation_generation=row.get("annotation_generation", row["generation"]),
            claim_token=row.get("claim_token"),
            status=row["status"],
            attempts=row["attempts"],
            leased_by=row["leased_by"],
            leased_until=ensure_utc(row["leased_until"]) if row.get("leased_until") else None,
            next_attempt_at=ensure_utc(row["next_attempt_at"])
            if row.get("next_attempt_at")
            else None,
            last_error=row["last_error"],
            is_manual=bool(row["is_manual"]),
            created_at=ensure_utc(row["created_at"]) if row.get("created_at") else utc_now(),
            updated_at=ensure_utc(row["updated_at"]) if row.get("updated_at") else utc_now(),
            profile_payload=row.get("profile_payload"),
        )
