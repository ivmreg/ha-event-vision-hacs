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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sqlite3
import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

from ..const import (
    BASE_BACKOFF_SECONDS,
    CAS_CLEANUP_MARGIN_SECONDS,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_PROVIDER_TIMEOUT,
)
from .models import (
    AnalysisJob,
    canonical_profile_json,
    derive_queue_lease_seconds,
    ensure_utc,
    format_iso,
    sanitize_error_code,
    utc_now,
)

_LOGGER = logging.getLogger(__name__)


def _dict_factory(cursor: sqlite3.Cursor, row: tuple[Any, ...]) -> dict[str, Any]:
    fields = [col[0] for col in cursor.description]
    return dict(zip(fields, row, strict=False))


def extract_payload_max_attempts(payload: str | None, default: int) -> int:
    """Extract validated max_attempts from profile_payload JSON budget, defaulting for legacy rows."""
    if not payload:
        return default
    try:
        p_data = json.loads(payload)
        if isinstance(p_data, dict):
            b_data = p_data.get("budget")
            if isinstance(b_data, dict) and "max_attempts" in b_data:
                ma = int(b_data["max_attempts"])
                if ma >= 1:
                    return ma
    except Exception:
        pass
    return default


class AnalysisJobStore:
    """Independent SQLite store managing durable analysis jobs and leases."""

    def __init__(self, db_path: str | Path, max_attempts: int = DEFAULT_MAX_ATTEMPTS) -> None:
        self._db_path = Path(db_path)
        self._max_attempts = max(1, min(max_attempts, 5))
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._is_open = False

    @property
    def is_open(self) -> bool:
        """Return whether SQLite connection is active."""
        return self._is_open

    @property
    def db_path(self) -> Path:
        """Return database path."""
        return self._db_path

    async def open(self) -> None:
        """Open database connection off the event loop and initialize schema."""
        await asyncio.to_thread(self._sync_open)

    def _sync_open(self) -> None:
        with self._lock:
            if self._is_open and self._conn:
                return

            self._db_path.parent.mkdir(parents=True, exist_ok=True)
            self._conn = sqlite3.connect(
                str(self._db_path),
                timeout=15.0,
                check_same_thread=False,
            )
            self._conn.row_factory = _dict_factory
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA busy_timeout = 5000;")
            cur.execute("PRAGMA synchronous = FULL;")

            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS analysis_jobs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    media_revision INTEGER NOT NULL,
                    profile_digest TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 1,
                    annotation_generation INTEGER NOT NULL DEFAULT 1,
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
                    UNIQUE(event_id, media_revision, profile_digest)
                );
                """
            )
            # Schema migration: check for missing profile_payload column in existing databases
            cur.execute("PRAGMA table_info(analysis_jobs);")
            cols = [r["name"] for r in cur.fetchall()]
            if cols and "profile_payload" not in cols:
                backup_path = self._db_path.with_name(f"{self._db_path.name}.pre_payload_backup")
                tmp_backup_path = self._db_path.with_name(f"{self._db_path.name}.pre_payload_backup.tmp")

                def _row_scalar(r: Any) -> Any:
                    if isinstance(r, dict):
                        return next(iter(r.values()))
                    return r[0]

                try:
                    if tmp_backup_path.exists():
                        tmp_backup_path.unlink()
                    bck_conn = sqlite3.connect(str(tmp_backup_path))
                    try:
                        self._conn.backup(bck_conn)
                    finally:
                        bck_conn.close()

                    # Validate the newly created backup
                    val_conn = sqlite3.connect(str(tmp_backup_path))
                    try:
                        chk = val_conn.execute("PRAGMA integrity_check;").fetchone()
                        if not chk or _row_scalar(chk) != "ok":
                            raise RuntimeError(f"Integrity check failed on backup: {chk}")
                        cur.execute("SELECT count(*) FROM analysis_jobs;")
                        src_count = _row_scalar(cur.fetchone())
                        bck_count = _row_scalar(
                            val_conn.execute("SELECT count(*) FROM analysis_jobs;").fetchone()
                        )
                        if src_count != bck_count:
                            raise RuntimeError(
                                f"Row count mismatch on backup: source={src_count}, backup={bck_count}"
                            )
                    finally:
                        val_conn.close()

                    os.replace(tmp_backup_path, backup_path)
                except Exception as err:
                    if tmp_backup_path.exists():
                        try:
                            tmp_backup_path.unlink()
                        except Exception:
                            pass
                    _LOGGER.error(
                        "Pre-upgrade database backup failed for %s: %s",
                        self._db_path,
                        sanitize_error_code(err),
                    )
                    raise RuntimeError(f"Pre-upgrade database backup failed: {err}") from err

                cur.execute("ALTER TABLE analysis_jobs ADD COLUMN profile_payload TEXT;")

            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_jobs_pending
                ON analysis_jobs (status, next_attempt_at, attempts);
                """
            )
            self._conn.commit()
            self._is_open = True

            # Recover any in-flight or expired leases from previous crashes/restarts
            self._sync_recover_expired_leases(force_all=True)

    async def close(self) -> None:
        """Close SQLite connection cleanly."""
        await asyncio.to_thread(self._sync_close)

    def _sync_close(self) -> None:
        with self._lock:
            if self._conn:
                try:
                    self._conn.close()
                except Exception as err:
                    _LOGGER.warning("Error closing analysis db: %s", err)
                finally:
                    self._conn = None
                    self._is_open = False

    async def enqueue_job(
        self,
        event_id: str,
        media_revision: int,
        profile_digest: str,
        is_manual: bool = False,
        annotation_generation: int | None = None,
        profile_payload: str | None = None,
    ) -> tuple[AnalysisJob, bool]:
        """Durable enqueue with atomic generation bump on manual retry.

        Returns (job, enqueued_boolean).
        """
        return await asyncio.to_thread(
            self._sync_enqueue_job,
            event_id,
            media_revision,
            profile_digest,
            is_manual,
            annotation_generation,
            profile_payload,
        )

    def _sync_enqueue_job(
        self,
        event_id: str,
        media_revision: int,
        profile_digest: str,
        is_manual: bool,
        annotation_generation: int | None,
        profile_payload: str | dict[str, Any] | None = None,
    ) -> tuple[AnalysisJob, bool]:
        if isinstance(profile_payload, dict):
            profile_payload = json.dumps(profile_payload)
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()

            cur.execute("PRAGMA synchronous = FULL;")

            cur.execute("BEGIN IMMEDIATE;")
            try:
                # First, supersede any older revisions for this event if new media revision arrives
                cur.execute(
                    """
                    UPDATE analysis_jobs
                    SET status = 'superseded', updated_at = ?
                    WHERE event_id = ? AND media_revision < ? AND status IN ('pending', 'leased');
                    """,
                    (now_str, event_id, media_revision),
                )

                cur.execute(
                    """
                    SELECT * FROM analysis_jobs
                    WHERE event_id = ? AND media_revision = ? AND profile_digest = ?;
                    """,
                    (event_id, media_revision, profile_digest),
                )
                existing = cur.fetchone()

                if existing:
                    if is_manual:
                        # Manual retry atomically supersedes generation and resets status
                        new_gen = existing["generation"] + 1
                        new_ann_gen = annotation_generation or (existing["annotation_generation"] + 1)
                        cur.execute(
                            """
                            UPDATE analysis_jobs
                            SET generation = ?, annotation_generation = ?, status = 'pending', attempts = 0,
                                leased_by = NULL, leased_until = NULL, next_attempt_at = NULL,
                                last_error = NULL, is_manual = 1, profile_payload = COALESCE(?, profile_payload),
                                updated_at = ?
                            WHERE id = ?;
                            """,
                            (new_gen, new_ann_gen, profile_payload, now_str, existing["id"]),
                        )
                        cur.execute("SELECT * FROM analysis_jobs WHERE id = ?;", (existing["id"],))
                        updated_row = cur.fetchone()
                        cur.execute("COMMIT;")
                        return self._row_to_job(updated_row), True

                    # Automatic enqueue rules
                    st = existing["status"]
                    if st in ("complete", "skipped"):
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False

                    if st in ("failed", "superseded"):
                        # Do not auto-retry failed current profile infinitely
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False

                    if st in ("pending", "leased"):
                        cur.execute("COMMIT;")
                        return self._row_to_job(existing), False
                else:
                    # Check backward-compatibility: was there an existing job under an older profile digest?
                    cur.execute(
                        """
                        SELECT * FROM analysis_jobs
                        WHERE event_id = ? AND media_revision = ?
                        ORDER BY id DESC LIMIT 1;
                        """,
                        (event_id, media_revision),
                    )
                    prior = cur.fetchone()
                    if prior:
                        if not is_manual:
                            # Automatic scan must never re-run completed jobs or duplicate pending/leased jobs
                            if prior["status"] in ("complete", "pending", "leased", "skipped"):
                                cur.execute("COMMIT;")
                                return self._row_to_job(prior), False
                        else:
                            # Explicit manual reanalysis supersedes older profile digest job
                            cur.execute(
                                """
                                UPDATE analysis_jobs
                                SET status = 'superseded', updated_at = ?
                                WHERE id = ?;
                                """,
                                (now_str, prior["id"]),
                            )

                # Insert new job
                initial_ann_gen = annotation_generation or 1
                cur.execute(
                    """
                    INSERT INTO analysis_jobs (
                        event_id, media_revision, profile_digest, generation,
                        annotation_generation, status, attempts, is_manual,
                        profile_payload, created_at, updated_at
                    ) VALUES (?, ?, ?, 1, ?, 'pending', 0, ?, ?, ?, ?);
                    """,
                    (
                        event_id,
                        media_revision,
                        profile_digest,
                        initial_ann_gen,
                        1 if is_manual else 0,
                        profile_payload,
                        now_str,
                        now_str,
                    ),
                )
                new_id = cur.lastrowid
                cur.execute("SELECT * FROM analysis_jobs WHERE id = ?;", (new_id,))
                inserted_row = cur.fetchone()
                cur.execute("COMMIT;")
                return self._row_to_job(inserted_row), True
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def lease_job(
        self,
        worker_id: str,
        lease_seconds: float = DEFAULT_JOB_DEADLINE,
    ) -> AnalysisJob | None:
        """Claim next pending or expired job under lease."""
        return await asyncio.to_thread(self._sync_lease_job, worker_id, lease_seconds)

    def _sync_lease_job(
        self,
        worker_id: str,
        lease_seconds: float,
    ) -> AnalysisJob | None:
        lease_seconds = max(10.0, lease_seconds)
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        lease_until_str = format_iso(now_dt + timedelta(seconds=lease_seconds)) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # Find eligible pending or expired lease job
                cur.execute(
                    """
                    SELECT * FROM analysis_jobs
                    WHERE (
                        status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                        AND attempts < COALESCE(CAST(CASE WHEN json_valid(profile_payload) = 1 THEN json_extract(profile_payload, '$.budget.max_attempts') ELSE NULL END AS INTEGER), ?)
                    ) OR (
                        status = 'leased' AND leased_until < ?
                        AND attempts < COALESCE(CAST(CASE WHEN json_valid(profile_payload) = 1 THEN json_extract(profile_payload, '$.budget.max_attempts') ELSE NULL END AS INTEGER), ?)
                    )
                    ORDER BY is_manual DESC, id ASC
                    LIMIT 1;
                    """,
                    (now_str, self._max_attempts, now_str, self._max_attempts),
                )
                row = cur.fetchone()
                if not row:
                    cur.execute("COMMIT;")
                    return None

                effective_lease = lease_seconds
                payload_raw = row["profile_payload"] if "profile_payload" in row else None
                if payload_raw:
                    try:
                        p_data = json.loads(payload_raw)
                        b_data = p_data.get("budget") if isinstance(p_data, dict) else None
                        if isinstance(b_data, dict) and "job_deadline" in b_data:
                            j_deadline = float(b_data["job_deadline"])
                            effective_lease = max(effective_lease, derive_queue_lease_seconds(j_deadline))
                        elif isinstance(p_data, dict) and "max_frames" in p_data:
                            m_frames = int(p_data["max_frames"])
                            derived_dead = (
                                (m_frames * DEFAULT_EXTRACTION_TIMEOUT)
                                + DEFAULT_PROVIDER_TIMEOUT
                                + CAS_CLEANUP_MARGIN_SECONDS
                            )
                            effective_lease = max(effective_lease, derive_queue_lease_seconds(derived_dead))
                    except Exception:
                        pass

                lease_until_str = format_iso(now_dt + timedelta(seconds=effective_lease)) or ""

                cur.execute(
                    """
                    UPDATE analysis_jobs
                    SET status = 'leased', leased_by = ?, leased_until = ?, updated_at = ?
                    WHERE id = ? AND generation = ?;
                    """,
                    (worker_id, lease_until_str, now_str, row["id"], row["generation"]),
                )
                if cur.rowcount != 1:
                    cur.execute("ROLLBACK;")
                    return None

                cur.execute("SELECT * FROM analysis_jobs WHERE id = ?;", (row["id"],))
                leased_row = cur.fetchone()
                cur.execute("COMMIT;")
                return self._row_to_job(leased_row)
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def complete_job(self, job_id: int, generation: int) -> bool:
        """Mark leased job complete, guarding against stale generations."""
        return await asyncio.to_thread(self._sync_complete_job, job_id, generation)

    def _sync_complete_job(self, job_id: int, generation: int) -> bool:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE analysis_jobs
                SET status = 'complete', leased_by = NULL, leased_until = NULL, updated_at = ?
                WHERE id = ? AND generation = ?;
                """,
                (now_str, job_id, generation),
            )
            self._conn.commit()
            return cur.rowcount > 0

    async def fail_job(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool,
        backoff_seconds: float = BASE_BACKOFF_SECONDS,
    ) -> bool:
        """Record job attempt failure with exponential backoff and retry bounds."""
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
                cur.execute("SELECT attempts, profile_payload FROM analysis_jobs WHERE id = ? AND generation = ?;", (job_id, generation))
                row = cur.fetchone()
                if not row:
                    cur.execute("ROLLBACK;")
                    return False

                job_max_attempts = extract_payload_max_attempts(row["profile_payload"], self._max_attempts)
                new_attempts = row["attempts"] + 1

                if retryable and new_attempts < job_max_attempts:
                    backoff = max(1.0, backoff_seconds)
                    next_attempt_str = format_iso(now_dt + timedelta(seconds=backoff))
                    cur.execute(
                        """
                        UPDATE analysis_jobs
                        SET status = 'pending', attempts = ?, next_attempt_at = ?,
                            last_error = ?, leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ? AND generation = ?;
                        """,
                        (new_attempts, next_attempt_str, clean_code, now_str, job_id, generation),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE analysis_jobs
                        SET status = 'failed', attempts = ?, next_attempt_at = NULL,
                            last_error = ?, leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ? AND generation = ?;
                        """,
                        (new_attempts, clean_code, now_str, job_id, generation),
                    )

                cur.execute("COMMIT;")
                return True
            except Exception:
                cur.execute("ROLLBACK;")
                raise

    async def skip_job(self, job_id: int, generation: int, reason: str) -> bool:
        """Mark job skipped (e.g. unavailable media)."""
        return await asyncio.to_thread(self._sync_skip_job, job_id, generation, reason)

    async def defer_job(self, job_id: int, generation: int) -> None:
        """Release an unavailable recording-store job without spending an AI attempt."""
        def defer() -> None:
            with self._lock:
                assert self._conn is not None
                self._conn.execute(
                    "UPDATE analysis_jobs SET status='pending', leased_by=NULL, leased_until=NULL "
                    "WHERE id=? AND generation=? AND status='leased'", (job_id, generation)
                )
                self._conn.commit()
        await asyncio.to_thread(defer)

    def _sync_skip_job(self, job_id: int, generation: int, reason: str) -> bool:
        now_str = format_iso(utc_now()) or ""
        clean_reason = sanitize_error_code(reason)
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE analysis_jobs
                SET status = 'skipped', last_error = ?, leased_by = NULL, leased_until = NULL, updated_at = ?
                WHERE id = ? AND generation = ?;
                """,
                (clean_reason, now_str, job_id, generation),
            )
            self._conn.commit()
            return cur.rowcount > 0

    async def update_job_annotation_generation(
        self, job_id: int, generation: int, annotation_generation: int
    ) -> bool:
        """Update annotation generation tied to an active job."""
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
                UPDATE analysis_jobs
                SET annotation_generation = ?, updated_at = ?
                WHERE id = ? AND generation = ?;
                """,
                (annotation_generation, now_str, job_id, generation),
            )
            self._conn.commit()
            return cur.rowcount > 0

    async def update_job_payload(self, job_id: int, payload: str | dict[str, Any]) -> bool:
        """Update profile payload for an existing job (used for legacy job migration)."""
        return await asyncio.to_thread(self._sync_update_job_payload, job_id, payload)

    def _sync_update_job_payload(self, job_id: int, payload: str | dict[str, Any]) -> bool:
        if isinstance(payload, dict):
            payload = canonical_profile_json(payload)
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            try:
                cur.execute(
                    """
                    UPDATE analysis_jobs
                    SET profile_payload = ?, updated_at = ?
                    WHERE id = ?;
                    """,
                    (payload, now_str, job_id),
                )
                self._conn.commit()
                return cur.rowcount > 0
            except Exception:
                self._conn.rollback()
                raise

    async def supersede_older_revisions(self, event_id: str, current_media_revision: int) -> int:
        """Mark older jobs for event_id as superseded."""
        return await asyncio.to_thread(self._sync_supersede_older_revisions, event_id, current_media_revision)

    async def supersede_job(self, job_id: int, generation: int) -> None:
        """Retire only the rejected attempt, preserving newer jobs for the same media."""
        def supersede() -> None:
            with self._lock:
                assert self._conn is not None
                self._conn.execute(
                    "UPDATE analysis_jobs SET status='superseded', leased_by=NULL, leased_until=NULL "
                    "WHERE id=? AND generation=? AND status IN ('pending','leased')",
                    (job_id, generation),
                )
                self._conn.commit()
        await asyncio.to_thread(supersede)

    def _sync_supersede_older_revisions(self, event_id: str, current_media_revision: int) -> int:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                UPDATE analysis_jobs
                SET status = 'superseded', updated_at = ?
                WHERE event_id = ? AND media_revision < ? AND status IN ('pending', 'leased');
                """,
                (now_str, event_id, current_media_revision),
            )
            self._conn.commit()
            return cur.rowcount

    async def get_job(
        self, event_id: str, media_revision: int, profile_digest: str
    ) -> AnalysisJob | None:
        """Retrieve a specific job record."""
        return await asyncio.to_thread(self._sync_get_job, event_id, media_revision, profile_digest)

    def _sync_get_job(
        self, event_id: str, media_revision: int, profile_digest: str
    ) -> AnalysisJob | None:
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM analysis_jobs
                WHERE event_id = ? AND media_revision = ? AND profile_digest = ?;
                """,
                (event_id, media_revision, profile_digest),
            )
            row = cur.fetchone()
            return self._row_to_job(row) if row else None

    async def get_latest_job_for_event(
        self, event_id: str, media_revision: int
    ) -> AnalysisJob | None:
        """Retrieve the latest job record for an event and media revision across all profiles."""
        return await asyncio.to_thread(self._sync_get_latest_job_for_event, event_id, media_revision)

    def _sync_get_latest_job_for_event(
        self, event_id: str, media_revision: int
    ) -> AnalysisJob | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT * FROM analysis_jobs
                WHERE event_id = ? AND media_revision = ?
                ORDER BY id DESC LIMIT 1;
                """,
                (event_id, media_revision),
            )
            row = cur.fetchone()
            return self._row_to_job(row) if row else None


    async def is_job_current(self, job_id: int, generation: int) -> bool:
        """Check if job exists, matches generation, and is still in leased status."""
        return await asyncio.to_thread(self._sync_is_job_current, job_id, generation)

    def _sync_is_job_current(self, job_id: int, generation: int) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                "SELECT id FROM analysis_jobs WHERE id = ? AND generation = ? AND status = 'leased';",
                (job_id, generation),
            )
            return cur.fetchone() is not None

    async def recover_expired_leases(self, force_all: bool = False) -> int:
        """Reset or fail expired worker leases (or all leased jobs on startup)."""
        return await asyncio.to_thread(self._sync_recover_expired_leases, force_all)

    def _sync_recover_expired_leases(self, force_all: bool = False) -> int:
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            if not self._conn:
                return 0
            cur = self._conn.cursor()
            if force_all:
                cur.execute(
                    """
                    SELECT id, attempts, profile_payload FROM analysis_jobs
                    WHERE status = 'leased';
                    """
                )
            else:
                cur.execute(
                    """
                    SELECT id, attempts, profile_payload FROM analysis_jobs
                    WHERE status = 'leased' AND leased_until < ?;
                    """,
                    (now_str,),
                )
            expired = cur.fetchall()
            recovered = 0
            for row in expired:
                job_max = extract_payload_max_attempts(row["profile_payload"], self._max_attempts)
                if row["attempts"] >= job_max:
                    cur.execute(
                        """
                        UPDATE analysis_jobs
                        SET status = 'failed', last_error = 'LEASE_TIMEOUT',
                            leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ?;
                        """,
                        (now_str, row["id"]),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE analysis_jobs
                        SET status = 'pending', leased_by = NULL, leased_until = NULL, updated_at = ?
                        WHERE id = ?;
                        """,
                        (now_str, row["id"]),
                    )
                recovered += 1
            self._conn.commit()
            return recovered

    async def get_stats(self) -> dict[str, int]:
        """Return non-sensitive queue metrics for diagnostics and sensors."""
        return await asyncio.to_thread(self._sync_get_stats)

    async def has_pending_or_leased_jobs(self) -> bool:
        """Return whether event-analysis work is pending or currently leased."""
        return await asyncio.to_thread(self._sync_has_pending_or_leased_jobs)

    def _sync_has_pending_or_leased_jobs(self) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                "SELECT 1 FROM analysis_jobs WHERE status IN ('pending', 'leased') LIMIT 1;"
            )
            return cur.fetchone() is not None

    def _sync_get_stats(self) -> dict[str, int]:
        with self._lock:
            if not self._conn:
                return {"pending": 0, "leased": 0, "complete": 0, "failed": 0, "skipped": 0}
            cur = self._conn.cursor()
            cur.execute("SELECT status, COUNT(*) as cnt FROM analysis_jobs GROUP BY status;")
            rows = cur.fetchall()
            stats = {"pending": 0, "leased": 0, "complete": 0, "failed": 0, "skipped": 0}
            for r in rows:
                if r["status"] in stats:
                    stats[r["status"]] = r["cnt"]
            return stats

    async def migrate_legacy_pending_jobs(
        self, profile_digest: str, profile_payload: str
    ) -> int:
        """Migrate legacy pending or leased jobs without profile payload to current profile key.

        Leaves historical completed, skipped, or failed archive results untouched.
        """
        return await asyncio.to_thread(
            self._sync_migrate_legacy_pending_jobs, profile_digest, profile_payload
        )

    def _sync_migrate_legacy_pending_jobs(
        self, profile_digest: str, profile_payload: str | dict[str, Any]
    ) -> int:
        if isinstance(profile_payload, dict):
            profile_payload = canonical_profile_json(profile_payload)
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            if not self._conn:
                return 0
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # Find distinct event_id and media_revision pairs with active legacy jobs
                cur.execute(
                    """
                    SELECT DISTINCT event_id, media_revision
                    FROM analysis_jobs
                    WHERE profile_payload IS NULL AND status IN ('pending', 'leased');
                    """
                )
                groups = cur.fetchall()
                migrated_count = 0

                for grp in groups:
                    e_id = grp["event_id"]
                    m_rev = grp["media_revision"]

                    # Check if a non-legacy (or archived) job under target profile_digest already exists
                    cur.execute(
                        """
                        SELECT id, status, is_manual, profile_payload FROM analysis_jobs
                        WHERE event_id = ? AND media_revision = ? AND profile_digest = ?;
                        """,
                        (e_id, m_rev, profile_digest),
                    )
                    target_row = cur.fetchone()
                    target_existing = None
                    if target_row:
                        if target_row["profile_payload"] is not None or target_row["status"] not in (
                            "pending",
                            "leased",
                        ):
                            target_existing = target_row

                    # Fetch active legacy candidates sorted by:
                    # 1. Matching target profile_digest (if any)
                    # 2. Manual priority (is_manual DESC)
                    # 3. Newest id (id DESC)
                    cur.execute(
                        """
                        SELECT id, status, is_manual, profile_digest FROM analysis_jobs
                        WHERE event_id = ? AND media_revision = ? AND profile_payload IS NULL
                              AND status IN ('pending', 'leased')
                        ORDER BY (profile_digest = ?) DESC, is_manual DESC, id DESC;
                        """,
                        (e_id, m_rev, profile_digest),
                    )
                    candidates = cur.fetchall()
                    if not candidates:
                        continue

                    if target_existing:
                        # Target profile already exists.
                        # Only promote priority if target is active (pending or leased).
                        # Completed, failed, skipped, or superseded target archive contents remain unchanged.
                        if target_existing["status"] in ("pending", "leased"):
                            if any(c["is_manual"] for c in candidates) and not target_existing["is_manual"]:
                                cur.execute(
                                    "UPDATE analysis_jobs SET is_manual = 1, updated_at = ? WHERE id = ?;",
                                    (now_str, target_existing["id"]),
                                )
                        for c in candidates:
                            cur.execute(
                                """
                                UPDATE analysis_jobs
                                SET status = 'superseded', last_error = 'SUPERSEDED_BY_CURRENT_PROFILE',
                                    leased_by = NULL, leased_until = NULL, updated_at = ?
                                WHERE id = ?;
                                """,
                                (now_str, c["id"]),
                            )
                    else:
                        # No external job exists under target profile: elect the active winner
                        # Inherit manual priority if any competing legacy candidate was manual.
                        winner = candidates[0]
                        winner_is_manual = max(c["is_manual"] for c in candidates)
                        cur.execute(
                            """
                            UPDATE analysis_jobs
                            SET profile_digest = ?, profile_payload = ?, is_manual = ?, updated_at = ?
                            WHERE id = ?;
                            """,
                            (profile_digest, profile_payload, winner_is_manual, now_str, winner["id"]),
                        )
                        migrated_count += 1

                        for other in candidates[1:]:
                            cur.execute(
                                """
                                UPDATE analysis_jobs
                                SET status = 'superseded', last_error = 'SUPERSEDED_BY_COALESCED_PROFILE',
                                    leased_by = NULL, leased_until = NULL, updated_at = ?
                                WHERE id = ?;
                                """,
                                (now_str, other["id"]),
                            )

                self._conn.commit()
                return migrated_count
            except Exception:
                self._conn.rollback()
                raise

    async def should_auto_enqueue(self, event_id: str, media_revision: int) -> bool:
        """Check whether an event revision is eligible for automatic enqueueing.

        Returns False if any job (complete, skipped, failed, pending, leased) already exists.
        Ensures upgrades do not trigger archive re-runs and prevents duplicate work.
        """
        return await asyncio.to_thread(self._sync_should_auto_enqueue, event_id, media_revision)

    def _sync_should_auto_enqueue(self, event_id: str, media_revision: int) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                """
                SELECT status FROM analysis_jobs
                WHERE event_id = ? AND media_revision = ?
                ORDER BY id DESC LIMIT 1;
                """,
                (event_id, media_revision),
            )
            row = cur.fetchone()
            if row is not None:
                # Any existing job for this media revision disqualifies automatic re-enqueue
                return False
            return True

    def _row_to_job(self, row: dict[str, Any]) -> AnalysisJob:
        return AnalysisJob(
            id=row["id"],
            event_id=row["event_id"],
            media_revision=row["media_revision"],
            profile_digest=row["profile_digest"],
            generation=row["generation"],
            annotation_generation=row.get("annotation_generation", row["generation"]),
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
