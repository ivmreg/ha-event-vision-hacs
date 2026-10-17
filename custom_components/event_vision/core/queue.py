"""Durable common SQLite queue engine for Event Vision analysis and fusion jobs.

Pure Python (zero Home Assistant dependencies).
Provides atomic transactions, manual-first priority scheduling, deadline-derived leases,
per-claim fencing tokens, temporal lease expiration enforcement, exponential backoff,
CAS state transitions, generation bumping on manual retry, attempt preservation on
dependency deferrals, and crash recovery with attempts preservation.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import sqlite3
import threading
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

from ..const import (
    BASE_BACKOFF_SECONDS,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
)
from .models import (
    derive_queue_lease_seconds,
    format_iso,
    sanitize_error_code,
    utc_now,
)

_LOGGER = logging.getLogger(__name__)

MAX_BACKOFF_SECONDS = 300.0
DEFAULT_RETRY_DELAY_SECONDS = 5.0

SAFE_IDENTIFIER_PATTERN = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def validate_identifier(name: str, desc: str = "identifier") -> str:
    """Validate that SQL table/column identifier consists only of safe ASCII alphanumeric characters."""
    if not isinstance(name, str) or not SAFE_IDENTIFIER_PATTERN.match(name):
        raise ValueError(f"Invalid SQL {desc}: '{name}'")
    return name


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


@dataclass(frozen=True, slots=True)
class QueueSpec:
    """Immutable declarative specification for a durable SQLite queue."""

    table_name: str
    target_column: str
    revision_column: str
    revision_type: str = "TEXT"
    default_annotation_generation: int | None = None
    has_manual_priority: bool = True
    order_by_sql: str = "is_manual DESC, id ASC"
    extra_index_cols: tuple[str, ...] = ()
    recover_on_lease: bool = False
    increment_attempts_on_lease_expiry: bool = False
    min_lease_seconds: float = 1.0

    def __post_init__(self) -> None:
        validate_identifier(self.table_name, "table_name")
        validate_identifier(self.target_column, "target_column")
        validate_identifier(self.revision_column, "revision_column")
        if self.revision_type not in ("TEXT", "INTEGER"):
            raise ValueError(f"Invalid revision_type: '{self.revision_type}'")
        if isinstance(self.extra_index_cols, (list, tuple)):
            object.__setattr__(self, "extra_index_cols", tuple(self.extra_index_cols))
        for col in self.extra_index_cols:
            validate_identifier(col, "extra_index_col")


class DurableQueueEngine:
    """True common SQLite persistent queue engine for durable jobs with per-claim fencing."""

    def __init__(
        self,
        db_path: str | Path,
        spec: QueueSpec | None = None,
        *,
        table_name: str | None = None,
        target_column: str | None = None,
        revision_column: str | None = None,
        revision_type: str = "TEXT",
        default_max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        base_backoff_seconds: float = BASE_BACKOFF_SECONDS,
        retry_delay_seconds: float = DEFAULT_RETRY_DELAY_SECONDS,
        extra_index_cols: list[str] | tuple[str, ...] | None = None,
        recover_on_lease: bool = False,
        increment_attempts_on_lease_expiry: bool = False,
        min_lease_seconds: float = 1.0,
    ) -> None:
        self._db_path = Path(db_path)
        if spec is not None:
            self._spec = spec
        else:
            if not table_name or not target_column or not revision_column:
                raise ValueError("Must provide either QueueSpec or table_name, target_column, and revision_column")
            order_by = (
                "is_manual DESC, attempts ASC, id ASC"
                if recover_on_lease
                else "is_manual DESC, id ASC"
            )
            self._spec = QueueSpec(
                table_name=table_name,
                target_column=target_column,
                revision_column=revision_column,
                revision_type=revision_type,
                default_annotation_generation=None,
                has_manual_priority=True,
                order_by_sql=order_by,
                extra_index_cols=tuple(extra_index_cols or ()),
                recover_on_lease=recover_on_lease,
                increment_attempts_on_lease_expiry=increment_attempts_on_lease_expiry,
                min_lease_seconds=min_lease_seconds,
            )
        self._default_max_attempts = max(1, min(default_max_attempts, 5))
        self._base_backoff_seconds = max(1.0, float(base_backoff_seconds))
        self._retry_delay_seconds = max(1.0, float(retry_delay_seconds))
        self._lock = threading.RLock()
        self._conn: sqlite3.Connection | None = None
        self._is_open = False

    @property
    def spec(self) -> QueueSpec:
        """Return immutable queue policy specification."""
        return self._spec

    @property
    def is_open(self) -> bool:
        """Return whether SQLite connection is active."""
        return self._is_open

    @property
    def db_path(self) -> Path:
        """Return database path."""
        return self._db_path

    @property
    def table_name(self) -> str:
        """Return table name."""
        return self._spec.table_name

    @property
    def lock(self) -> threading.RLock:
        """Return engine lock for atomic compound transactions."""
        return self._lock

    @property
    def conn(self) -> sqlite3.Connection | None:
        """Return active connection under lock."""
        return self._conn

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
                timeout=30.0,
                check_same_thread=False,
            )
            self._conn.row_factory = _dict_factory
            cur = self._conn.cursor()
            cur.execute("PRAGMA journal_mode = WAL;")
            cur.execute("PRAGMA busy_timeout = 30000;")
            cur.execute("PRAGMA synchronous = FULL;")

            ann_gen_default = (
                f"DEFAULT {self._spec.default_annotation_generation}"
                if self._spec.default_annotation_generation is not None
                else ""
            )
            create_sql = f"""
                CREATE TABLE IF NOT EXISTS {self._spec.table_name} (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    {self._spec.target_column} TEXT NOT NULL,
                    {self._spec.revision_column} {self._spec.revision_type} NOT NULL,
                    profile_digest TEXT NOT NULL,
                    generation INTEGER NOT NULL DEFAULT 1,
                    annotation_generation INTEGER {ann_gen_default},
                    status TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    leased_by TEXT,
                    leased_until TEXT,
                    claim_token TEXT,
                    next_attempt_at TEXT,
                    last_error TEXT,
                    is_manual INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    profile_payload TEXT,
                    UNIQUE({self._spec.target_column}, {self._spec.revision_column}, profile_digest)
                );
            """
            cur.execute(create_sql)

            cur.execute(f"PRAGMA table_info({self._spec.table_name});")
            cols = [r["name"] for r in cur.fetchall()]
            if cols and "profile_payload" not in cols:
                self._sync_migrate_profile_payload(cur)
            if cols and "claim_token" not in cols:
                cur.execute(f"ALTER TABLE {self._spec.table_name} ADD COLUMN claim_token TEXT;")

            cur.execute(
                f"""
                CREATE INDEX IF NOT EXISTS idx_{self._spec.table_name}_pending
                ON {self._spec.table_name} (status, next_attempt_at, attempts);
                """
            )

            if self._spec.extra_index_cols:
                idx_cols_str = ", ".join(self._spec.extra_index_cols)
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{self._spec.table_name}_{self._spec.extra_index_cols[0]}
                    ON {self._spec.table_name} ({idx_cols_str});
                    """
                )

            self._conn.commit()
            self._is_open = True
            self._sync_recover_expired_leases(force_all=True)

    def _sync_migrate_profile_payload(self, cur: sqlite3.Cursor) -> None:
        """Migrate legacy table by adding profile_payload with pre-upgrade backup."""
        assert self._conn is not None
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

            val_conn = sqlite3.connect(str(tmp_backup_path))
            try:
                chk = val_conn.execute("PRAGMA integrity_check;").fetchone()
                if not chk or _row_scalar(chk) != "ok":
                    raise RuntimeError(f"Integrity check failed on backup: {chk}")
                cur.execute(f"SELECT count(*) FROM {self._spec.table_name};")
                src_count = _row_scalar(cur.fetchone())
                bck_count = _row_scalar(
                    val_conn.execute(f"SELECT count(*) FROM {self._spec.table_name};").fetchone()
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

        cur.execute(f"ALTER TABLE {self._spec.table_name} ADD COLUMN profile_payload TEXT;")

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
                self._conn = None
            self._is_open = False

    async def enqueue(
        self,
        target_id: str,
        revision: Any,
        profile_digest: str,
        is_manual: bool = False,
        annotation_generation: int | None = None,
        profile_payload: str | dict[str, Any] | None = None,
        supersede_older_check: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically enqueue a job or retry an existing job with generation bumping."""
        return await asyncio.to_thread(
            self._sync_enqueue,
            target_id,
            revision,
            profile_digest,
            is_manual,
            annotation_generation,
            profile_payload,
            supersede_older_check,
        )

    def _sync_enqueue(
        self,
        target_id: str,
        revision: Any,
        profile_digest: str,
        is_manual: bool,
        annotation_generation: int | None,
        profile_payload: str | dict[str, Any] | None,
        supersede_older_check: bool = True,
    ) -> tuple[dict[str, Any], bool]:
        if isinstance(profile_payload, dict):
            profile_payload = json.dumps(profile_payload, sort_keys=True, separators=(",", ":"))
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # 1. Check existing exact match: (target_id, revision, profile_digest)
                cur.execute(
                    f"""
                    SELECT * FROM {self._spec.table_name}
                    WHERE {self._spec.target_column} = ? AND {self._spec.revision_column} = ? AND profile_digest = ?;
                    """,
                    (target_id, revision, profile_digest),
                )
                existing = cur.fetchone()

                if existing:
                    status = existing["status"]
                    if is_manual:
                        # Manual request bumps generation and resets attempts/errors
                        new_gen = existing["generation"] + 1
                        new_ann_gen = (
                            annotation_generation
                            if annotation_generation is not None
                            else (existing.get("annotation_generation") or new_gen)
                        )
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET generation = ?, status = 'pending', attempts = 0, is_manual = 1,
                                next_attempt_at = NULL, last_error = NULL, leased_by = NULL,
                                leased_until = NULL, claim_token = NULL, profile_payload = ?,
                                annotation_generation = ?, updated_at = ?
                            WHERE id = ?;
                            """,
                            (new_gen, profile_payload, new_ann_gen, now_str, existing["id"]),
                        )
                        cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (existing["id"],))
                        row = cur.fetchone()
                        cur.execute("COMMIT;")
                        return row, True

                    if status in ("complete", "skipped"):
                        cur.execute("COMMIT;")
                        return existing, False

                    if status in ("pending", "leased"):
                        cur.execute("COMMIT;")
                        return existing, False

                    if status == "failed":
                        # Automatic retry of failed job
                        payload = existing.get("profile_payload") or profile_payload
                        effective_max_attempts = extract_payload_max_attempts(
                            payload, self._default_max_attempts
                        )
                        if existing["attempts"] >= effective_max_attempts:
                            cur.execute("COMMIT;")
                            return existing, False

                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'pending', next_attempt_at = ?, leased_by = NULL, leased_until = NULL,
                                claim_token = NULL, updated_at = ?
                            WHERE id = ?;
                            """,
                            (now_str, now_str, existing["id"]),
                        )
                        cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (existing["id"],))
                        row = cur.fetchone()
                        cur.execute("COMMIT;")
                        return row, True

                    if status == "superseded":
                        # Resurrect superseded job
                        new_gen = existing["generation"] + 1
                        new_ann_gen = (
                            annotation_generation
                            if annotation_generation is not None
                            else (existing.get("annotation_generation") or new_gen)
                        )
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET generation = ?, status = 'pending', attempts = 0, next_attempt_at = NULL,
                                last_error = NULL, leased_by = NULL, leased_until = NULL, claim_token = NULL,
                                profile_payload = ?, annotation_generation = ?, updated_at = ?
                            WHERE id = ?;
                            """,
                            (new_gen, profile_payload, new_ann_gen, now_str, existing["id"]),
                        )
                        cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (existing["id"],))
                        row = cur.fetchone()
                        cur.execute("COMMIT;")
                        return row, True

                # 2. Check for prior active jobs for the same target with different revision/digest
                if supersede_older_check:
                    cur.execute(
                        f"""
                        SELECT * FROM {self._spec.table_name}
                        WHERE {self._spec.target_column} = ? AND status IN ('pending', 'leased');
                        """,
                        (target_id,),
                    )
                    prior_jobs = cur.fetchall()
                    for prior in prior_jobs:
                        prior_rev = prior[self._spec.revision_column]
                        if self._spec.revision_type == "INTEGER":
                            try:
                                is_older_rev = int(prior_rev) < int(revision)
                            except (ValueError, TypeError):
                                is_older_rev = prior_rev != revision
                        else:
                            is_older_rev = prior_rev != revision

                        if is_older_rev:
                            cur.execute(
                                f"""
                                UPDATE {self._spec.table_name}
                                SET status = 'superseded', updated_at = ?
                                WHERE id = ?;
                                """,
                                (now_str, prior["id"]),
                            )
                        elif prior["profile_digest"] != profile_digest:
                            if not is_manual:
                                if prior["status"] in ("pending", "leased"):
                                    cur.execute("COMMIT;")
                                    return prior, False
                            else:
                                cur.execute(
                                    f"""
                                    UPDATE {self._spec.table_name}
                                    SET status = 'superseded', updated_at = ?
                                    WHERE id = ?;
                                    """,
                                    (now_str, prior["id"]),
                                )

                # 3. Insert new job
                initial_ann_gen = (
                    annotation_generation
                    if annotation_generation is not None
                    else self._spec.default_annotation_generation
                )
                cur.execute(
                    f"""
                    INSERT INTO {self._spec.table_name} (
                        {self._spec.target_column}, {self._spec.revision_column}, profile_digest, generation,
                        annotation_generation, status, attempts, is_manual,
                        profile_payload, created_at, updated_at
                    ) VALUES (?, ?, ?, 1, ?, 'pending', 0, ?, ?, ?, ?);
                    """,
                    (
                        target_id,
                        revision,
                        profile_digest,
                        initial_ann_gen,
                        1 if is_manual else 0,
                        profile_payload,
                        now_str,
                        now_str,
                    ),
                )
                new_id = cur.lastrowid
                cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (new_id,))
                row = cur.fetchone()
                cur.execute("COMMIT;")
                return row, True
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def lease(
        self,
        worker_id: str,
        lease_seconds: float = DEFAULT_JOB_DEADLINE,
    ) -> dict[str, Any] | None:
        """Lease the highest priority eligible pending job atomically with a unique claim token."""
        return await asyncio.to_thread(self._sync_lease, worker_id, lease_seconds)

    def _sync_lease(
        self,
        worker_id: str,
        lease_seconds: float,
    ) -> dict[str, Any] | None:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                # Recover expired leases first if configured
                if self._spec.recover_on_lease:
                    self._sync_recover_expired_leases(force_all=False, commit=False)

                cur.execute(
                    f"""
                    SELECT * FROM {self._spec.table_name}
                    WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ?)
                    ORDER BY {self._spec.order_by_sql}
                    LIMIT 1;
                    """,
                    (now_str,),
                )
                job = cur.fetchone()
                if not job:
                    cur.execute("COMMIT;")
                    return None

                # Check max attempts ceiling before granting lease
                payload = job.get("profile_payload")
                effective_max_attempts = extract_payload_max_attempts(payload, self._default_max_attempts)
                if job["attempts"] >= effective_max_attempts:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'failed', last_error = 'MAX_ATTEMPTS_EXCEEDED',
                            leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                        WHERE id = ?;
                        """,
                        (now_str, job["id"]),
                    )
                    cur.execute("COMMIT;")
                    return None

                effective_lease_seconds = max(self._spec.min_lease_seconds, float(lease_seconds))
                if payload:
                    try:
                        p_data = json.loads(payload)
                        if isinstance(p_data, dict):
                            b_data = p_data.get("budget")
                            if isinstance(b_data, dict) and "job_deadline" in b_data:
                                jd = float(b_data["job_deadline"])
                                if jd > 0:
                                    effective_lease_seconds = max(
                                        effective_lease_seconds, derive_queue_lease_seconds(jd)
                                    )
                    except Exception:
                        pass

                expires_dt = now_dt + timedelta(seconds=effective_lease_seconds)
                expires_str = format_iso(expires_dt) or ""
                claim_token = secrets.token_hex(16)

                cur.execute(
                    f"""
                    UPDATE {self._spec.table_name}
                    SET status = 'leased', leased_by = ?, leased_until = ?, claim_token = ?, updated_at = ?
                    WHERE id = ? AND status = 'pending';
                    """,
                    (worker_id, expires_str, claim_token, now_str, job["id"]),
                )

                if cur.rowcount == 0:
                    cur.execute("COMMIT;")
                    return None

                cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (job["id"],))
                leased_job = cur.fetchone()
                cur.execute("COMMIT;")
                return leased_job
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def complete(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark job as complete with transactional CAS and per-claim fencing verification."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(self._sync_complete, job_id, generation, token)

    def _sync_complete(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'complete', leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (now_str, job_id, generation, token, now_str),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'complete', leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (now_str, job_id, generation, now_str),
                    )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def fail(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool = True,
        backoff_seconds: float = BASE_BACKOFF_SECONDS,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Fail job with CAS verification, per-claim fencing, and exponential retry backoff."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(
            self._sync_fail, job_id, generation, error_code, retryable, backoff_seconds, token
        )

    def _sync_fail(
        self,
        job_id: int,
        generation: int,
        error_code: str,
        retryable: bool,
        backoff_seconds: float,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        SELECT attempts, profile_payload FROM {self._spec.table_name}
                        WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (job_id, generation, token, now_str),
                    )
                else:
                    cur.execute(
                        f"""
                        SELECT attempts, profile_payload FROM {self._spec.table_name}
                        WHERE id = ? AND generation = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (job_id, generation, now_str),
                    )
                job = cur.fetchone()
                if not job:
                    cur.execute("ROLLBACK;")
                    return False

                payload = job.get("profile_payload")
                effective_max_attempts = extract_payload_max_attempts(payload, self._default_max_attempts)
                new_attempts = job["attempts"] + 1

                if not retryable or new_attempts >= effective_max_attempts:
                    if token is not None:
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'failed', attempts = ?, last_error = ?, leased_by = NULL, leased_until = NULL,
                                claim_token = NULL, next_attempt_at = NULL, updated_at = ?
                            WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased';
                            """,
                            (new_attempts, error_code, now_str, job_id, generation, token),
                        )
                    else:
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'failed', attempts = ?, last_error = ?, leased_by = NULL, leased_until = NULL,
                                claim_token = NULL, next_attempt_at = NULL, updated_at = ?
                            WHERE id = ? AND generation = ? AND status = 'leased';
                            """,
                            (new_attempts, error_code, now_str, job_id, generation),
                        )
                else:
                    backoff = min(
                        MAX_BACKOFF_SECONDS,
                        max(1.0, float(backoff_seconds)),
                    )
                    next_attempt_dt = now_dt + timedelta(seconds=backoff)
                    next_attempt_str = format_iso(next_attempt_dt) or ""
                    if token is not None:
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'pending', attempts = ?, last_error = ?, leased_by = NULL, leased_until = NULL,
                                claim_token = NULL, next_attempt_at = ?, updated_at = ?
                            WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased';
                            """,
                            (new_attempts, error_code, next_attempt_str, now_str, job_id, generation, token),
                        )
                    else:
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'pending', attempts = ?, last_error = ?, leased_by = NULL, leased_until = NULL,
                                claim_token = NULL, next_attempt_at = ?, updated_at = ?
                            WHERE id = ? AND generation = ? AND status = 'leased';
                            """,
                            (new_attempts, error_code, next_attempt_str, now_str, job_id, generation),
                        )

                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def defer(
        self,
        job_id: int,
        generation: int,
        delay_seconds: float | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Defer job to pending without burning an attempt, gated by claim token and lease validity."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(self._sync_defer, job_id, generation, delay_seconds, token)

    def _sync_defer(
        self,
        job_id: int,
        generation: int,
        delay_seconds: float | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        delay = float(delay_seconds) if delay_seconds is not None else self._retry_delay_seconds
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""
        next_attempt_str = format_iso(now_dt + timedelta(seconds=delay)) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'pending', next_attempt_at = ?, leased_by = NULL, leased_until = NULL,
                            claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (next_attempt_str, now_str, job_id, generation, token, now_str),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'pending', next_attempt_at = ?, leased_by = NULL, leased_until = NULL,
                            claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (next_attempt_str, now_str, job_id, generation, now_str),
                    )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def skip(
        self,
        job_id: int,
        generation: int,
        reason: str = "SKIPPED",
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark job as skipped with CAS generation and claim token verification."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(self._sync_skip, job_id, generation, reason, token)

    def _sync_skip(
        self,
        job_id: int,
        generation: int,
        reason: str,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'skipped', last_error = ?, leased_by = NULL, leased_until = NULL,
                            claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased'
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (reason, now_str, job_id, generation, token, now_str),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'skipped', last_error = ?, leased_by = NULL, leased_until = NULL,
                            claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND status IN ('pending', 'leased')
                          AND (leased_until IS NULL OR leased_until > ?);
                        """,
                        (reason, now_str, job_id, generation, now_str),
                    )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def supersede(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Mark job as superseded with CAS generation and claim token guard."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(self._sync_supersede, job_id, generation, token)

    def _sync_supersede(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'superseded', leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ? AND claim_token = ?;
                        """,
                        (now_str, job_id, generation, token),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET status = 'superseded', leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                        WHERE id = ? AND generation = ?;
                        """,
                        (now_str, job_id, generation),
                    )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def recover_expired_leases(self, force_all: bool = False, commit: bool = True) -> int:
        """Recover expired or in-flight leases, resetting claim tokens and preserving attempts."""
        return await asyncio.to_thread(self._sync_recover_expired_leases, force_all, commit)

    def _sync_recover_expired_leases(self, force_all: bool = False, commit: bool = True) -> int:
        now_dt = utc_now()
        now_str = format_iso(now_dt) or ""

        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            if commit:
                cur.execute("PRAGMA synchronous = FULL;")
                cur.execute("BEGIN IMMEDIATE;")
            try:
                if force_all:
                    cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE status = 'leased';")
                else:
                    cur.execute(
                        f"""
                        SELECT * FROM {self._spec.table_name}
                        WHERE status = 'leased' AND (leased_until IS NULL OR leased_until <= ?);
                        """,
                        (now_str,),
                    )
                expired = cur.fetchall()
                recovered_count = 0

                for row in expired:
                    if not force_all and self._spec.increment_attempts_on_lease_expiry:
                        new_attempts = row["attempts"] + 1
                        payload = row.get("profile_payload")
                        effective_max_attempts = extract_payload_max_attempts(
                            payload, self._default_max_attempts
                        )
                        if new_attempts >= effective_max_attempts:
                            cur.execute(
                                f"""
                                UPDATE {self._spec.table_name}
                                SET status = 'failed', attempts = ?, last_error = 'LEASE_TIMEOUT',
                                    leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                                WHERE id = ?;
                                """,
                                (new_attempts, now_str, row["id"]),
                            )
                            recovered_count += 1
                            continue
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'pending', attempts = ?, last_error = 'LEASE_TIMEOUT',
                                leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                            WHERE id = ?;
                            """,
                            (new_attempts, now_str, row["id"]),
                        )
                    else:
                        cur.execute(
                            f"""
                            UPDATE {self._spec.table_name}
                            SET status = 'pending', leased_by = NULL, leased_until = NULL, claim_token = NULL, updated_at = ?
                            WHERE id = ?;
                            """,
                            (now_str, row["id"]),
                        )
                    recovered_count += 1

                if commit:
                    cur.execute("COMMIT;")
                return recovered_count
            except Exception as err:
                if commit:
                    try:
                        cur.execute("ROLLBACK;")
                    except Exception:
                        pass
                raise err

    async def is_current(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Verify job currency under current generation, lease validity, and claim token."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(self._sync_is_current, job_id, generation, token)

    def _sync_is_current(
        self,
        job_id: int,
        generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            if token is not None:
                cur.execute(
                    f"""
                    SELECT id FROM {self._spec.table_name}
                    WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased'
                      AND (leased_until IS NULL OR leased_until > ?);
                    """,
                    (job_id, generation, token, now_str),
                )
            else:
                cur.execute(
                    f"""
                    SELECT id FROM {self._spec.table_name}
                    WHERE id = ? AND generation = ? AND status = 'leased'
                      AND (leased_until IS NULL OR leased_until > ?);
                    """,
                    (job_id, generation, now_str),
                )
            return cur.fetchone() is not None

    async def get_job(self, job_id: int) -> dict[str, Any] | None:
        """Fetch raw job row by id."""
        return await asyncio.to_thread(self._sync_get_job, job_id)

    def _sync_get_job(self, job_id: int) -> dict[str, Any] | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(f"SELECT * FROM {self._spec.table_name} WHERE id = ?;", (job_id,))
            return cur.fetchone()

    async def get_latest_for_target(self, target_id: str) -> dict[str, Any] | None:
        """Fetch latest raw job row for a given target entity or incident."""
        return await asyncio.to_thread(self._sync_get_latest_for_target, target_id)

    def _sync_get_latest_for_target(self, target_id: str) -> dict[str, Any] | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(
                f"SELECT * FROM {self._spec.table_name} WHERE {self._spec.target_column} = ? ORDER BY id DESC LIMIT 1;",
                (target_id,),
            )
            return cur.fetchone()

    async def get_job_by_inputs(
        self,
        target_id: str,
        revision: Any,
        profile_digest: str,
    ) -> dict[str, Any] | None:
        """Fetch raw job matching compound key."""
        return await asyncio.to_thread(
            self._sync_get_job_by_inputs, target_id, revision, profile_digest
        )

    def _sync_get_job_by_inputs(
        self,
        target_id: str,
        revision: Any,
        profile_digest: str,
    ) -> dict[str, Any] | None:
        with self._lock:
            if not self._conn:
                return None
            cur = self._conn.cursor()
            cur.execute(
                f"""
                SELECT * FROM {self._spec.table_name}
                WHERE {self._spec.target_column} = ? AND {self._spec.revision_column} = ? AND profile_digest = ?;
                """,
                (target_id, revision, profile_digest),
            )
            return cur.fetchone()

    async def get_stats(self) -> dict[str, int]:
        """Return counts of jobs by status."""
        return await asyncio.to_thread(self._sync_get_stats)

    def _sync_get_stats(self) -> dict[str, int]:
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
            cur.execute(
                f"SELECT status, count(*) AS cnt FROM {self._spec.table_name} GROUP BY status;"
            )
            rows = cur.fetchall()
            for r in rows:
                counts[r["status"]] = r["cnt"]
            return counts

    async def has_pending_or_leased(self) -> bool:
        """Return True if there are any jobs currently pending or leased."""
        return await asyncio.to_thread(self._sync_has_pending_or_leased)

    def _sync_has_pending_or_leased(self) -> bool:
        with self._lock:
            if not self._conn:
                return False
            cur = self._conn.cursor()
            cur.execute(
                f"SELECT 1 FROM {self._spec.table_name} WHERE status IN ('pending', 'leased') LIMIT 1;"
            )
            return cur.fetchone() is not None

    async def update_annotation_generation(
        self,
        job_id: int,
        generation: int,
        annotation_generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Update annotation generation with CAS generation and claim token check."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(
            self._sync_update_annotation_generation,
            job_id,
            generation,
            annotation_generation,
            token,
        )

    def _sync_update_annotation_generation(
        self,
        job_id: int,
        generation: int,
        annotation_generation: int,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                if token is not None:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET annotation_generation = ?, updated_at = ?
                        WHERE id = ? AND generation = ? AND claim_token = ? AND status = 'leased';
                        """,
                        (annotation_generation, now_str, job_id, generation, token),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE {self._spec.table_name}
                        SET annotation_generation = ?, updated_at = ?
                        WHERE id = ? AND generation = ? AND status = 'leased';
                        """,
                        (annotation_generation, now_str, job_id, generation),
                    )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def update_status_cas(
        self,
        job_id: int,
        expected_status: str,
        new_status: str,
        generation: int | None = None,
        last_error: str | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        """Update job status with CAS verification against expected_status, generation, and claim_token."""
        token = lease_token if lease_token is not None else claim_token
        return await asyncio.to_thread(
            self._sync_update_status_cas,
            job_id,
            expected_status,
            new_status,
            generation,
            last_error,
            token,
        )

    def _sync_update_status_cas(
        self,
        job_id: int,
        expected_status: str,
        new_status: str,
        generation: int | None = None,
        last_error: str | None = None,
        claim_token: str | None = None,
        lease_token: str | None = None,
    ) -> bool:
        token = lease_token if lease_token is not None else claim_token
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute("PRAGMA synchronous = FULL;")
            cur.execute("BEGIN IMMEDIATE;")
            try:
                where_clauses = ["id = ?", "status = ?"]
                params: list[Any] = [job_id, expected_status]

                if generation is not None:
                    where_clauses.append("generation = ?")
                    params.append(generation)

                if token is not None:
                    where_clauses.append("claim_token = ?")
                    params.append(token)

                where_sql = " AND ".join(where_clauses)
                cur.execute(
                    f"""
                    UPDATE {self._spec.table_name}
                    SET status = ?, last_error = ?, leased_by = NULL, leased_until = NULL,
                        claim_token = NULL, updated_at = ?
                    WHERE {where_sql};
                    """,
                    (new_status, last_error, now_str, *params),
                )
                success = cur.rowcount > 0
                cur.execute("COMMIT;")
                return success
            except Exception as err:
                try:
                    cur.execute("ROLLBACK;")
                except Exception:
                    pass
                raise err

    async def update_payload(self, job_id: int, payload: str | dict[str, Any]) -> bool:
        """Update profile payload JSON for an existing job."""
        return await asyncio.to_thread(self._sync_update_payload, job_id, payload)

    def _sync_update_payload(self, job_id: int, payload: str | dict[str, Any]) -> bool:
        payload_str = (
            json.dumps(payload, sort_keys=True, separators=(",", ":"))
            if isinstance(payload, dict)
            else payload
        )
        now_str = format_iso(utc_now()) or ""
        with self._lock:
            assert self._conn is not None
            cur = self._conn.cursor()
            cur.execute(
                f"UPDATE {self._spec.table_name} SET profile_payload = ?, updated_at = ? WHERE id = ?;",
                (payload_str, now_str, job_id),
            )
            self._conn.commit()
            return cur.rowcount > 0
