"""Durable SQLite storage and queue management for Event Vision incident fusion jobs.

Strict boundary: Zero Home Assistant imports, zero network calls.
Backed by common DurableQueueEngine core.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import threading
from pathlib import Path
from typing import Any

from ..const import (
    BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
)
from .models import (
    FusionJob,
    ensure_utc,
    utc_now,
)
from .queue import DurableQueueEngine, QueueSpec

_LOGGER = logging.getLogger(__name__)


def validate_safe_id(val: str, field_name: str) -> None:
    """Validate that identifier is non-empty string within safe length bound."""
    if not isinstance(val, str) or not val.strip() or len(val) > 255:
        raise ValueError(f"Invalid {field_name}: must be non-empty string up to 255 characters")


def validate_bounded_string(val: str, field_name: str, max_len: int = 512) -> None:
    """Validate string length bounds."""
    if not isinstance(val, str) or not val.strip() or len(val) > max_len:
        raise ValueError(f"Invalid {field_name}: must be non-empty string up to {max_len} characters")


class FusionJobStore:
    """Durable SQLite persistent storage for multi-camera incident visual fusion jobs."""

    def __init__(self, db_path: Path, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> None:
        spec = QueueSpec(
            table_name="fusion_jobs",
            target_column="incident_id",
            revision_column="input_token",
            revision_type="TEXT",
            default_annotation_generation=None,
            has_manual_priority=True,
            order_by_sql="is_manual DESC, attempts ASC, id ASC",
            extra_index_cols=("incident_id", "id"),
            recover_on_lease=True,
            increment_attempts_on_lease_expiry=True,
        )
        self._engine = DurableQueueEngine(
            db_path=db_path,
            spec=spec,
            default_max_attempts=max_attempts,
            base_backoff_seconds=BASE_BACKOFF_SECONDS,
        )

    @property
    def is_open(self) -> bool:
        """Return True if the database connection is established."""
        return self._engine.is_open

    @property
    def db_path(self) -> Path:
        """Return database path."""
        return self._engine.db_path

    @property
    def _lock(self) -> threading.RLock:
        """Return thread lock for backward compatibility with tests."""
        return self._engine.lock

    @property
    def _conn(self) -> sqlite3.Connection | None:
        """Return connection for backward compatibility with tests."""
        return self._engine.conn

    async def open(self) -> None:
        """Open the SQLite connection, apply pragmas, and ensure tables exist."""
        await self._engine.open()

    async def close(self) -> None:
        """Close SQLite connection cleanly."""
        await self._engine.close()

    async def enqueue_fusion_job(
        self,
        incident_id: str,
        input_token: str,
        profile_digest: str,
        is_manual: bool = False,
        annotation_generation: int | None = None,
        profile_payload: str | dict[str, Any] | None = None,
    ) -> tuple[FusionJob, bool]:
        """Atomically enqueue or retry a fusion job."""
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(input_token, "input_token")
        validate_bounded_string(profile_digest, "profile_digest")

        row, enqueued = await self._engine.enqueue(
            target_id=incident_id,
            revision=input_token,
            profile_digest=profile_digest,
            is_manual=is_manual,
            annotation_generation=annotation_generation,
            profile_payload=profile_payload,
            supersede_older_check=True,
        )
        return self._row_to_job(row), enqueued

    async def get_job_by_inputs(
        self, incident_id: str, input_token: str, profile_digest: str
    ) -> FusionJob | None:
        """Fetch existing job matching incident_id, input_token, and profile_digest."""
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(input_token, "input_token")
        validate_bounded_string(profile_digest, "profile_digest")
        return await asyncio.to_thread(
            self._sync_get_job_by_inputs, incident_id, input_token, profile_digest
        )

    def _sync_get_job_by_inputs(
        self, incident_id: str, input_token: str, profile_digest: str
    ) -> FusionJob | None:
        with self._engine.lock:
            conn = self._engine.conn
            assert conn is not None
            cur = conn.cursor()
            cur.execute(
                """
                SELECT * FROM fusion_jobs
                WHERE incident_id = ? AND input_token = ? AND profile_digest = ?;
                """,
                (incident_id, input_token, profile_digest),
            )
            row = cur.fetchone()
            if not row:
                return None
            return self._row_to_job(row)

    async def lease_fusion_job(
        self, worker_id: str, lease_seconds: float = 300.0
    ) -> FusionJob | None:
        """Atomically lease one eligible pending fusion job."""
        validate_bounded_string(worker_id, "worker_id")
        row = await self._engine.lease(worker_id=worker_id, lease_seconds=lease_seconds)
        return self._row_to_job(row) if row else None

    async def complete_fusion_job(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Atomically transition a leased fusion job to complete."""
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.complete(
            job_id=job_id, generation=generation, claim_token=effective_token, lease_token=effective_token
        )

    async def fail_fusion_job(
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
        """Mark fusion job as failed or schedule backoff retry with CAS protection."""
        validate_bounded_string(error_code, "error_code")
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.fail(
            job_id=job_id,
            generation=generation,
            error_code=error_code,
            retryable=retryable,
            backoff_seconds=backoff_seconds,
            claim_token=effective_token,
            lease_token=effective_token,
        )

    async def skip_fusion_job(
        self,
        job_id: int,
        generation: int,
        reason: str,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Mark fusion job as skipped."""
        validate_bounded_string(reason, "reason")
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.skip(
            job_id=job_id, generation=generation, reason=reason, claim_token=effective_token, lease_token=effective_token
        )

    async def supersede_fusion_job(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Mark fusion job as superseded with CAS generation protection."""
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.supersede(
            job_id=job_id, generation=generation, claim_token=effective_token, lease_token=effective_token
        )

    async def defer_fusion_job(
        self,
        job_id: int,
        generation: int,
        defer_seconds: float = 5.0,
        delay_seconds: float | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Defer fusion job to pending with a short retry delay without consuming attempts."""
        effective_delay = delay_seconds if delay_seconds is not None else defer_seconds
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.defer(
            job_id=job_id,
            generation=generation,
            delay_seconds=effective_delay,
            claim_token=effective_token,
            lease_token=effective_token,
        )

    async def is_fusion_job_current(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
        token: str | None = None,
    ) -> bool:
        """Return True if job is still active under the specified generation."""
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.is_current(
            job_id=job_id, generation=generation, claim_token=effective_token, lease_token=effective_token
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
        effective_token = token if token is not None else (lease_token if lease_token is not None else claim_token)
        return await self._engine.update_annotation_generation(
            job_id=job_id,
            generation=generation,
            annotation_generation=annotation_generation,
            claim_token=effective_token,
            lease_token=effective_token,
        )

    async def recover_expired_leases(self, force_all: bool = False) -> int:
        """Recover expired leases or reset in-flight jobs on clean restart."""
        return await self._engine.recover_expired_leases(force_all=force_all)

    async def get_fusion_job(
        self,
        incident_id: str,
        input_token: str,
        profile_digest: str,
    ) -> FusionJob | None:
        """Fetch job matching compound primary key."""
        return await self.get_job_by_inputs(incident_id, input_token, profile_digest)

    async def get_latest_fusion_job(self, incident_id: str) -> FusionJob | None:
        """Retrieve the latest fusion job record for an incident."""
        row = await self._engine.get_latest_for_target(incident_id)
        return self._row_to_job(row) if row else None

    async def has_pending_or_leased_jobs(self) -> bool:
        """Return True if there are any jobs currently pending or leased."""
        return await self._engine.has_pending_or_leased()

    async def get_job_counts(self) -> dict[str, int]:
        """Return dictionary of count of jobs grouped by status."""
        return await self._engine.get_stats()

    def _row_to_job(self, row: dict[str, Any]) -> FusionJob:
        return FusionJob(
            id=row["id"],
            incident_id=row["incident_id"],
            input_token=row["input_token"],
            profile_digest=row["profile_digest"],
            generation=row["generation"],
            annotation_generation=row.get("annotation_generation"),
            claim_token=row.get("claim_token"),
            status=row["status"],
            attempts=row["attempts"],
            leased_by=row["leased_by"],
            leased_until=ensure_utc(row["leased_until"]) if row.get("leased_until") else None,
            next_attempt_at=ensure_utc(row["next_attempt_at"]) if row.get("next_attempt_at") else None,
            last_error=row["last_error"],
            is_manual=bool(row["is_manual"]),
            created_at=ensure_utc(row["created_at"]) if row.get("created_at") else utc_now(),
            updated_at=ensure_utc(row["updated_at"]) if row.get("updated_at") else utc_now(),
            profile_payload=row.get("profile_payload"),
        )
