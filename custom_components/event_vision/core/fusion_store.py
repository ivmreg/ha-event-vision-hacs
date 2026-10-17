"""Durable SQLite storage and queue management for Event Vision incident fusion jobs.

Strict boundary: Zero Home Assistant imports, zero network calls.
"""

from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

from ..const import (
    BASE_BACKOFF_SECONDS,
    DEFAULT_MAX_ATTEMPTS,
    MAX_FUSION_JOB_DEADLINE,
)
from .models import (
    FusionJob,
    derive_queue_lease_seconds,
    ensure_utc,
    format_iso,
    sanitize_error_code,
    utc_now,
)
from .store import extract_payload_max_attempts


def validate_safe_id(val: str, field_name: str) -> None:
    """Validate that identifier is non-empty string within safe length bound."""
    if not isinstance(val, str) or not val.strip() or len(val) > 255:
        raise ValueError(f"Invalid {field_name}: must be non-empty string up to 255 characters")


def validate_bounded_string(val: str, field_name: str, max_len: int = 512) -> None:
    """Validate string length bounds."""
    if not isinstance(val, str) or not val.strip() or len(val) > max_len:
        raise ValueError(f"Invalid {field_name}: must be non-empty string up to {max_len} characters")


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    return {col[0]: row[idx] for idx, col in enumerate(cursor.description)}


class FusionJobStore:
    """Durable SQLite persistent storage for multi-camera incident visual fusion jobs."""

    def __init__(self, db_path: Path, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> None:
        self._db_path = db_path
        self._max_attempts = max_attempts
        self._conn: sqlite3.Connection | None = None
        self._lock = threading.RLock()
        self._is_open = False

    @property
    def is_open(self) -> bool:
        """Return True if the database connection is established."""
        return self._is_open

    async def open(self) -> None:
        """Open the SQLite connection, apply pragmas, and ensure tables exist."""
        await asyncio.to_thread(self._sync_open)

    def _sync_open(self) -> None:
        with self._lock:
            if self._conn is not None:
                return

            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(self._db_path),
                timeout=30.0,
                check_same_thread=False,
            )
            self._conn.row_factory = _dict_factory
            cur = self._conn.cursor()

            # Enforce WAL mode, full durability, and busy timeout
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("PRAGMA busy_timeout = 30000;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS fusion_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    incident_id TEXT NOT NULL,
                    input_token TEXT NOT NULL,
                    profile_digest TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 1,
                    annotation_generation INTEGER,
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    leased_by TEXT,
                    leased_until TEXT,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    is_manual INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    profile_payload TEXT,
                    UNIQUE(incident_id, input_token, profile_digest)
                );
                """
            )

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fusion_jobs_pending
                ON fusion_jobs (status, next_attempt_at, attempts);
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fusion_jobs_incident
                ON fusion_jobs (incident_id, id);
                """
            )
            self._conn.commit()
            self._is_open = True

            # Recover any in-flight or expired leases from previous restarts
            self._sync_recover_expired_leases(force_all=True)

    async def close(self) -> None:
        """Close SQLite connection cleanly."""
        await asyncio.to_thread(self._sync_close)

    def _sync_close(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception:
                    pass
                finally:
                    self._conn = None
                    self._is_open = False

    async def enqueue_fusion_job(
        self,
        incident_id: str,
        input_token: str,
        profile_digest: str,
        is_manual: bool = False,
        annotation_generation: int | None = None,
        profile_payload: str | None = None,
    ) -> tuple[FusionJob, bool]:
        """Durable enqueue with atomic generation bump on manual retry.

        Returns (job, enqueued_boolean).
        """
        validate_safe_id(incident_id, "incident_id")
        validate_bounded_string(input_token, "input_token")
        validate_bounded_string(profile_digest, "profile_digest")
        return await asyncio.to_thread(
            self._sync_enqueue_job,
            incident_id,
            input_token,
            profile_digest,
            is_manual,
            annotation_generation,
            profile_payload,
        )

    def _sync_enqueue_job(
        self,
        incident_id: str,
        input_token: str,
        profile_digest: str,
        is_manual: bool,
        annotation_generation: int | None,
        profile_payload: str | dict[str, Any] | None = None,
    ) -> tuple[FusionJob, bool]:
        if isinstance(profile_payload, dict):
            profile_payload = json.dumps(profile_payload)
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # 1. Supersede any older in-flight jobs for this incident if new input_token arrives
                cur.execute(
                    """
                    UPDATE fusion_jobs
                    SET status = 'superseded', updated_at = ?
                    WHERE incident_id = ? AND input_token != ? AND status IN ('pending', 'leased');
                    """,
                    (now_str, incident_id, input_token),
                )

                # 2. Check for existing job for this exact incident_id, input_token, and profile_digest
                cur.execute(
                    """
                    SELECT * FROM fusion_jobs
                    WHERE incident_id = ? AND input_token = ? AND profile_digest = ?;
                    """,
                    (incident_id, input_token, profile_digest),
                )
                existing = cur.fetchone()

                if existing:
                    if is_manual:
                        # Manual retry atomically bumps generation and resets status to pending
                        new_gen = existing["generation"] + 1
                        new_ann_gen = annotation_generation or (
                            (existing["annotation_generation"] or 0) + 1
                        )
                        cur.execute(
                            """
                            UPDATE fusion_jobs
                            SET generation = ?, annotation_generation = ?, status = 'pending', attempts = 0,
                                leased_by = NULL, leased_until = NULL, next_attempt_at = NULL,
                                last_error = NULL, is_manual = 1, profile_payload = COALESCE(?, profile_payload),
                                updated_at = ?
                            WHERE id = ?;
                            """,
                            (new_gen, new_ann_gen, profile_payload, now_str, existing["id"]),
                        )
                        cur.execute("SELECT * FROM fusion_jobs WHERE id = ?;", (existing["id"],))
                        updated_row = cur.fetchone()
                        cur.execute("COMMIT;")
                        return self._row_to_job(updated_row), True

                    # Automatic enqueue rules
                    st = existing["status"]
                    if st in ("complete", "skipped"):
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False

                    if st in ("failed", "superseded"):
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False

                    if st in ("pending", "leased"):
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False
                else:
                    # Check backward-compatibility / profile changes for this incident and input_token
                    cur.execute(
                        """
                        SELECT * FROM fusion_jobs
                        WHERE incident_id = ? AND input_token = ?
                        ORDER BY id DESC LIMIT 1;
                        """,
                        (incident_id, input_token),
                    )
                    prior = cur.fetchone()
                    if prior:
                        if not is_manual:
                            if prior["status"] in ("complete", "pending", "leased", "skipped"):
                                cur.execute("COMMIT;")
                                return self._row_to_job(prior), False
                        else:
                            cur.execute(
                                """
                                UPDATE fusion_jobs
                                SET status = 'superseded', updated_at = ?
                                WHERE id = ?;
                                """,
                                (now_str, prior["id"]),
                            )

                # Insert new fusion job
                cur.execute(
                    """
                    INSERT INTO fusion_jobs (
                        incident_id, input_token, profile_digest, generation,
                        annotation_generation, status, attempts, is_manual,
                        profile_payload, created_at, updated_at
                    ) VALUES (?, ?, ?, 1, ?, 'pending', 0, ?, ?, ?, ?);
                    """,
                    (
                        incident_id,
                        input_token,
                        profile_digest,
                        annotation_generation,
                        1 if is_manual else 0,
                        profile_payload,
                        now_str,
                        now_str,
                    ),
                )
                new_id = cur.lastrowid
                cur.execute("SELECT * FROM fusion_jobs WHERE id = ?;", (new_id,))
                row = cur.fetchone()
                cur.execute("COMMIT;")
                return self._row_to_job(row), True
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

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
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
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
        return await asyncio.to_thread(self._sync_lease_job, worker_id, lease_seconds)

    def _sync_lease_job(self, worker_id: str, lease_seconds: float) -> FusionJob | None:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # Recover expired leases first
                self._sync_recover_expired_leases(force_all=False, commit=False)

                cur.execute(
                    """
                    SELECT id, attempts, profile_payload, is_manual FROM fusion_jobs
                    WHERE status = 'pending'
                      AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                    ORDER BY is_manual DESC, attempts ASC, id ASC
                    LIMIT 1;
                    """,
                    (now_str,),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute("COMMIT;")
                    return None

                job_id = row["id"]
                effective_lease = float(lease_seconds)
                try:
                    payload = json.loads(row["profile_payload"] or "{}")
                    budget = payload.get("budget", {}) if isinstance(payload, dict) else {}
                    deadline = budget.get("job_deadline") if isinstance(budget, dict) else None
                    if (
                        isinstance(deadline, (int, float))
                        and not isinstance(deadline, bool)
                        and math.isfinite(deadline)
                        and 0.0 < float(deadline) <= MAX_FUSION_JOB_DEADLINE
                    ):
                        effective_lease = max(effective_lease, derive_queue_lease_seconds(float(deadline)))
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass
                lease_until_str = format_iso(
                    utc_now() + timedelta(seconds=effective_lease)
                ) or ""
                max_att = extract_payload_max_attempts(row["profile_payload"], self._max_attempts)
                if row["attempts"] >= max_att:
                    cur.execute(
                        """
                        UPDATE fusion_jobs
                        SET status = 'failed', last_error = 'MAX_ATTEMPTS_EXCEEDED', updated_at = ?
                        WHERE id = ?;
                        """,
                        (now_str, job_id),
                    )
                    cur.execute("COMMIT;")
                    return None

                cur.execute(
                    """
                    UPDATE fusion_jobs
                    SET status = 'leased', leased_by = ?, leased_until = ?, updated_at = ?
                    WHERE id = ? AND status = 'pending';
                    """,
                    (worker_id, lease_until_str, now_str, job_id),
                )
                if cur.rowcount == 0:
                    cur.execute("COMMIT;")
                    return None

                cur.execute("SELECT * FROM fusion_jobs WHERE id = ?;", (job_id,))
                leased_row = cur.fetchone()
                cur.execute("COMMIT;")
                return self._row_to_job(leased_row)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def complete_fusion_job(self, job_id: int, generation: int) -> bool:
        """Mark fusion job complete."""
        return await asyncio.to_thread(self._sync_complete_job, job_id, generation)

    def _sync_complete_job(self, job_id: int, generation: int) -> bool:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    UPDATE fusion_jobs
                    SET status = 'complete', leased_by = NULL, leased_until = NULL,
                        next_attempt_at = NULL, last_error = NULL, updated_at = ?
                    WHERE id = ? AND generation = ?;
                    """,
                    (now_str, job_id, generation),
                )
                ok = cur.rowcount > 0
                cur.execute("COMMIT;")
                return ok
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def fail_fusion_job(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool,
        backoff_seconds: float = BASE_BACKOFF_SECONDS,
    ) -> bool:
        """Record fusion job attempt failure with exponential backoff and retry bounds."""
        return await asyncio.to_thread(
            self._sync_fail_job,
            job_id,
            generation,
            error_code,
            retryable,
            backoff_seconds,
        )

    def _sync_fail_job(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool,
        backoff_seconds: float,
    ) -> bool:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        clean_code = sanitize_error_code(error_code)

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    "SELECT attempts, profile_payload FROM fusion_jobs "
                    "WHERE id = ? AND generation = ? AND status = 'leased';",
                    (job_id, generation),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute("ROLLBACK;")
                    return False

                job_max_attempts = extract_payload_max_attempts(
                    row["profile_payload"], self._max_attempts
                )
                new_attempts = row["attempts"] + 1

                if retryable and new_attempts < job_max_attempts:
                    backoff = max(1.0, backoff_seconds)
                    next_attempt_str = format_iso(now_dt + timedelta(seconds=backoff))
                    cur.execute(
                        """
                        UPDATE fusion_jobs
                        SET status = 'pending', attempts = ?, next_attempt_at = ?,
                            last_error = ?, leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND status = 'leased';
                        """,
                        (new_attempts, next_attempt_str, clean_code, now_str, job_id, generation),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE fusion_jobs
                        SET status = 'failed', attempts = ?, next_attempt_at = NULL,
                            last_error = ?, leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND status = 'leased';
                        """,
                        (new_attempts, clean_code, now_str, job_id, generation),
                    )

                transitioned = cur.rowcount > 0
                cur.execute("COMMIT;")
                return transitioned
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def skip_fusion_job(self, job_id: int, generation: int, reason: str) -> bool:
        """Mark fusion job skipped."""
        return await asyncio.to_thread(self._sync_skip_job, job_id, generation, reason)

    def _sync_skip_job(self, job_id: int, generation: int, reason: str) -> bool:
        now_str = format_iso(utc_now()) or ""
        clean_reason = sanitize_error_code(reason)
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    UPDATE fusion_jobs
                    SET status = 'skipped', leased_by = NULL, leased_until = NULL,
                        next_attempt_at = NULL, last_error = ?, updated_at = ?
                    WHERE id = ? AND generation = ?;
                    """,
                    (clean_reason, now_str, job_id, generation),
                )
                ok = cur.rowcount > 0
                cur.execute("COMMIT;")
                return ok
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def supersede_fusion_job(self, job_id: int, generation: int) -> bool:
        """Mark fusion job superseded."""
        return await asyncio.to_thread(self._sync_supersede_job, job_id, generation)

    def _sync_supersede_job(self, job_id: int, generation: int) -> bool:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                cur.execute(
                    """
                    UPDATE fusion_jobs
                    SET status = 'superseded', leased_by = NULL, leased_until = NULL, updated_at = ?
                    WHERE id = ? AND generation = ?;
                    """,
                    (now_str, job_id, generation),
                )
                ok = cur.rowcount > 0
                cur.execute("COMMIT;")
                return ok
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def defer_fusion_job(
        self, job_id: int, generation: int, defer_seconds: float = 5.0
    ) -> None:
        """Release leased fusion job with a bounded delay and no consumed attempt."""
        next_attempt = format_iso(utc_now() + timedelta(seconds=max(1.0, defer_seconds))) or ""

        def defer() -> None:
            with self._lock:
                assert self._conn is not None
                self._conn.execute(
                    "UPDATE fusion_jobs SET status='pending', leased_by=NULL, leased_until=NULL, "
                    "next_attempt_at=?, updated_at=? "
                    "WHERE id=? AND generation=? AND status='leased';",
                    (next_attempt, format_iso(utc_now()) or "", job_id, generation),
                )
                self._conn.commit()

        await asyncio.to_thread(defer)

    async def is_fusion_job_current(self, job_id: int, generation: int) -> bool:
        """Check if job exists, matches generation, and is still in leased status."""
        return await asyncio.to_thread(self._sync_is_job_current, job_id, generation)

    def _sync_is_job_current(self, job_id: int, generation: int) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                "SELECT id FROM fusion_jobs WHERE id = ? AND generation = ? AND status = 'leased';",
                (job_id, generation),
            )
            return cur.fetchone() is not None

    async def update_job_annotation_generation(
        self, job_id: int, generation: int, annotation_generation: int
    ) -> bool:
        """Update annotation generation tied to an active fusion job."""
        return await asyncio.to_thread(
            self._sync_update_annotation_gen, job_id, generation, annotation_generation
        )

    def _sync_update_annotation_gen(
        self, job_id: int, generation: int, annotation_generation: int
    ) -> bool:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE fusion_jobs
                SET annotation_generation = ?, updated_at = ?
                WHERE id = ? AND generation = ?;
                """,
                (annotation_generation, now_str, job_id, generation),
            )
            self._conn.commit()
            return cur.rowcount > 0

    async def recover_expired_leases(self, force_all: bool = False) -> int:
        """Reset or fail expired worker leases."""
        return await asyncio.to_thread(self._sync_recover_expired_leases, force_all)

    def _sync_recover_expired_leases(
        self, force_all: bool = False, commit: bool = True
    ) -> int:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            if not self._conn:
                return 0
            cur = self._conn.cursor()
            if force_all:
                cur.execute(
                    """
                    SELECT id, attempts, profile_payload FROM fusion_jobs
                    WHERE status = 'leased';
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT id, attempts, profile_payload FROM fusion_jobs
                    WHERE status = 'leased' AND leased_until < ?;
                    """,
                    (now_str,),
                )
            rows = cur.fetchall()
            recovered = 0
            for r in rows:
                j_id = r["id"]
                max_att = extract_payload_max_attempts(r["profile_payload"], self._max_attempts)
                new_att = r["attempts"] if force_all else r["attempts"] + 1
                if not force_all and new_att >= max_att:
                    cur.execute(
                        """
                        UPDATE fusion_jobs
                        SET status = 'failed', attempts = ?, last_error = 'LEASE_TIMEOUT',
                            leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ?;
                        """,
                        (new_att, now_str, j_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE fusion_jobs
                        SET status = 'pending', attempts = ?,
                            last_error = CASE WHEN ? THEN last_error ELSE 'LEASE_TIMEOUT' END,
                            leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ?;
                        """,
                        (new_att, 1 if force_all else 0, now_str, j_id),
                    )
                recovered += 1
            if commit:
                self._conn.commit()
            return recovered

    async def get_fusion_job(
        self, incident_id: str, input_token: str, profile_digest: str
    ) -> FusionJob | None:
        """Retrieve a specific fusion job record."""
        return await asyncio.to_thread(
            self._sync_get_job, incident_id, input_token, profile_digest
        )

    def _sync_get_job(
        self, incident_id: str, input_token: str, profile_digest: str
    ) -> FusionJob | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM fusion_jobs
                WHERE incident_id = ? AND input_token = ? AND profile_digest = ?;
                """,
                (incident_id, input_token, profile_digest),
            )
            row = cur.fetchone()
            return self._row_to_job(row) if row else None

    async def get_latest_fusion_job(self, incident_id: str) -> FusionJob | None:
        """Retrieve the latest fusion job record for an incident."""
        return await asyncio.to_thread(self._sync_get_latest_job, incident_id)

    def _sync_get_latest_job(self, incident_id: str) -> FusionJob | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM fusion_jobs
                WHERE incident_id = ?
                ORDER BY id DESC LIMIT 1;
                """,
                (incident_id,),
            )
            row = cur.fetchone()
            return self._row_to_job(row) if row else None

    async def has_pending_or_leased_jobs(self) -> bool:
        """Check if any fusion jobs are pending or actively leased."""
        return await asyncio.to_thread(self._sync_has_pending_or_leased_jobs)

    def _sync_has_pending_or_leased_jobs(self) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                "SELECT 1 FROM fusion_jobs WHERE status IN ('pending', 'leased') LIMIT 1;"
            )
            return cur.fetchone() is not None

    async def get_job_counts(self) -> dict[str, int]:
        """Return counts of fusion jobs grouped by status."""
        return await asyncio.to_thread(self._sync_get_job_counts)

    def _sync_get_job_counts(self) -> dict[str, int]:
        counts = {
            "pending": 0,
            "leased": 0,
            "complete": 0,
            "failed": 0,
            "skipped": 0,
            "superseded": 0,
        }
        with self._lock:
            if not self._conn:
                return counts
            cur = self._conn.cursor()
            cur.execute("SELECT status, COUNT(*) as count FROM fusion_jobs GROUP BY status;")
            for r in cur.fetchall():
                st = r["status"]
                if st in counts:
                    counts[st] = int(r["count"])
            return counts

    def _row_to_job(self, row: dict[str, Any]) -> FusionJob:
        return FusionJob(
            id=row["id"],
            incident_id=row["incident_id"],
            input_token=row["input_token"],
            profile_digest=row["profile_digest"],
            generation=row["generation"],
            annotation_generation=row["annotation_generation"],
            status=row["status"],
            attempts=row["attempts"],
            leased_by=row["leased_by"],
            leased_until=ensure_utc(row["leased_until"]) if row["leased_until"] else None,
            next_attempt_at=ensure_utc(row["next_attempt_at"]) if row["next_attempt_at"] else None,
            last_error=row["last_error"],
            is_manual=bool(row["is_manual"]),
            created_at=ensure_utc(row["created_at"]),
            updated_at=ensure_utc(row["updated_at"]),
            profile_payload=row.get("profile_payload"),
        )
