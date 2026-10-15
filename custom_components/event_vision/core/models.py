"""Pure Python data models, Wire DTOs, and error types for Event Vision.

Strict boundary: Zero Home Assistant imports, zero camera/sensor dependencies.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from ..const import (
    MAX_OBJECT_LEN,
    MAX_OBJECTS,
    MAX_SUMMARY_LEN,
    VALID_STATUSES,
)


def utc_now() -> datetime:
    """Return current UTC datetime."""
    return datetime.now(UTC)


def ensure_utc(dt: datetime | str) -> datetime:
    """Ensure datetime has UTC timezone."""
    if isinstance(dt, str):
        dt_val = datetime.fromisoformat(dt)
    else:
        dt_val = dt
    if dt_val.tzinfo is None:
        return dt_val.replace(tzinfo=UTC)
    return dt_val.astimezone(UTC)


def format_iso(dt: datetime | None) -> str | None:
    """Format datetime as UTC ISO-8601 string."""
    if dt is None:
        return None
    utc_dt = ensure_utc(dt)
    return utc_dt.isoformat()


def compute_profile_digest(model: str, instructions: str | None = None) -> str:
    """Compute deterministic cryptographic digest of model and instructions."""
    raw = f"{model.strip()}:{(instructions or '').strip()}".encode()
    return hashlib.sha256(raw).hexdigest()[:16]


def sanitize_error_code(err: Any) -> str:
    """Produce safe, non-sensitive error code without leaking local paths or keys."""
    if err is None:
        return ""
    err_str = str(err)
    err_upper = err_str.upper()

    if "RATE_LIMIT" in err_upper or "429" in err_upper:
        return "RATE_LIMIT"
    if "401" in err_upper or "AUTH" in err_upper or "UNAUTHORIZED" in err_upper:
        return "HTTP_401"
    if "403" in err_upper or "FORBIDDEN" in err_upper:
        return "HTTP_403"
    if "400" in err_upper:
        return "HTTP_400"
    if "500" in err_upper:
        return "HTTP_500"
    if "502" in err_upper:
        return "HTTP_502"
    if "503" in err_upper:
        return "HTTP_503"
    if "504" in err_upper:
        return "HTTP_504"
    if "TIMEOUT" in err_upper:
        return "TIMEOUT"
    if "NETWORK" in err_upper or "CONNECTION" in err_upper:
        return "NETWORK_ERROR"
    if "MEDIA_UNAVAILABLE" in err_upper:
        return "MEDIA_UNAVAILABLE"
    if "INVALID_IMAGE" in err_upper:
        return "INVALID_IMAGE"
    if "FFMPEG_NOT_FOUND" in err_upper:
        return "FFMPEG_NOT_FOUND"
    if "FRAME_EXTRACTION" in err_upper:
        return "FRAME_EXTRACTION_FAILED"
    if "MALFORMED" in err_upper:
        return "MALFORMED_RESPONSE"
    if "EMPTY" in err_upper:
        return "EMPTY_RESPONSE"
    if "SCHEMA" in err_upper:
        return "SCHEMA_ERROR"
    if "STALE" in err_upper:
        return "STALE_REVISION"
    if "LEASE_TIMEOUT" in err_upper:
        return "LEASE_TIMEOUT"

    # Block path leakage
    if ":" in err_str or "\\" in err_str or "/" in err_str or len(err_str) > 32:
        return "INTERNAL_ERROR"

    clean = "".join(c for c in err_str if c.isalnum() or c in ("_", "-")).upper()
    return clean[:32] if clean else "INTERNAL_ERROR"


# -----------------------------------------------------------------------------
# Core Exceptions
# -----------------------------------------------------------------------------


class AnalysisError(Exception):
    """Base exception for analysis errors."""

    def __init__(self, code: str, message: str, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


class GeminiKeyError(AnalysisError):
    """Missing or invalid Gemini API key."""


class GeminiSchemaError(AnalysisError):
    """Model response failed schema validation."""


class FrameExtractionError(AnalysisError):
    """Error during frame extraction."""


# -----------------------------------------------------------------------------
# Result and Annotation Data Structures
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class AnalysisResult:
    """Structured result parsed from provider response."""

    summary: str
    activity: bool
    objects: list[str] = field(default_factory=list)

    def __post_init__(self) -> None:
        if not isinstance(self.summary, str):
            raise TypeError("summary must be a string")
        if not isinstance(self.activity, bool):
            raise TypeError("activity must be a boolean")
        if not isinstance(self.objects, list):
            raise TypeError("objects must be a list of strings")


@dataclass(frozen=True)
class AnalysisAnnotation:
    """Exact wire representation stored in event_recording under namespace 'event_vision'.

    Wire format:
    {
      "status": "pending"|"processing"|"complete"|"failed"|"skipped",
      "summary"?: str,
      "activity"?: bool,
      "objects"?: list[str],
      "error_code"?: str,
      "media_revision": int,
      "generation": int,
      "model"?: str,
      "updated_at"?: str (ISO-8601)
    }
    """

    status: str
    media_revision: int
    generation: int
    summary: str | None = None
    activity: bool | None = None
    objects: list[str] | None = None
    error_code: str | None = None
    model: str | None = None
    updated_at: str | None = None

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {self.status}")
        if not isinstance(self.media_revision, int) or self.media_revision < 1:
            raise ValueError("media_revision must be positive integer")
        if not isinstance(self.generation, int) or self.generation < 1:
            raise ValueError("generation must be positive integer")

    def to_dict(self) -> dict[str, Any]:
        """Convert to strict wire dictionary with non-None optional fields."""
        payload: dict[str, Any] = {
            "status": self.status,
            "media_revision": self.media_revision,
            "generation": self.generation,
        }
        if self.summary is not None:
            payload["summary"] = self.summary[:MAX_SUMMARY_LEN]
        if self.activity is not None:
            payload["activity"] = self.activity
        if self.objects is not None:
            payload["objects"] = [o[:MAX_OBJECT_LEN] for o in self.objects[:MAX_OBJECTS]]
        if self.error_code is not None:
            payload["error_code"] = sanitize_error_code(self.error_code)
        if self.model is not None:
            payload["model"] = self.model
        if self.updated_at is not None:
            payload["updated_at"] = self.updated_at
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AnalysisAnnotation:
        """Construct from raw dictionary."""
        return cls(
            status=data["status"],
            media_revision=data["media_revision"],
            generation=data["generation"],
            summary=data.get("summary"),
            activity=data.get("activity"),
            objects=data.get("objects"),
            error_code=data.get("error_code"),
            model=data.get("model"),
            updated_at=data.get("updated_at"),
        )


@dataclass(frozen=True)
class AnalysisJob:
    """Internal durable analysis job tracking record."""

    id: int | None
    event_id: str
    media_revision: int
    profile_digest: str
    generation: int = 1
    annotation_generation: int = 1
    status: str = "pending"
    attempts: int = 0
    leased_by: str | None = None
    leased_until: datetime | None = None
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    is_manual: bool = False
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)


@dataclass(frozen=True)
class ExtractedFrame:
    """An extracted image frame ready for provider transport."""

    data: bytes
    mime_type: str = "image/jpeg"
