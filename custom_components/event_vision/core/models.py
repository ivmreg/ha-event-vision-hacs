"""Pure Python data models, Wire DTOs, and error types for Event Vision.

Strict boundary: Zero Home Assistant imports, zero camera/sensor dependencies.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from custom_components.event_recording.core.models import (
    compute_incident_input_token as compute_incident_input_token,
)

from ..const import (
    CAS_CLEANUP_MARGIN_SECONDS,
    DEFAULT_CONCURRENCY,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_FRAME_CACHE_MB,
    DEFAULT_FRAME_CACHE_TTL,
    DEFAULT_FUSION_JOB_DEADLINE,
    DEFAULT_FUSION_MAX_FRAMES,
    DEFAULT_FUSION_PROMPT,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_MODEL,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    GENERIC_MEDIA_PROMPT,
    MAX_ATTEMPTS_BOUND,
    MAX_CONCURRENCY,
    MAX_EXTRACTION_TIMEOUT,
    MAX_FRAME_CACHE_MB,
    MAX_FRAME_CACHE_TTL,
    MAX_FRAMES_BUDGET,
    MAX_FUSION_JOB_DEADLINE,
    MAX_FUSION_MAX_FRAMES,
    MAX_IMAGE_SOURCE_BYTES,
    MAX_JOB_DEADLINE,
    MAX_OBJECT_LEN,
    MAX_OBJECTS,
    MAX_PROVIDER_TIMEOUT,
    MAX_SUMMARY_LEN,
    MIN_ATTEMPTS,
    MIN_CONCURRENCY,
    MIN_EXTRACTION_TIMEOUT,
    MIN_FRAME_CACHE_MB,
    MIN_FRAME_CACHE_TTL,
    MIN_FRAMES,
    MIN_FUSION_JOB_DEADLINE,
    MIN_FUSION_MAX_FRAMES,
    MIN_JOB_DEADLINE,
    MIN_PROVIDER_TIMEOUT,
    VALID_SAMPLING_POLICIES,
    VALID_STATUSES,
)

try:
    from custom_components.event_recording.core.validation import (
        FieldSpec,
        IdSpec,
        NumberSpec,
        StringSpec,
        ValidationError,
        validate_bounded_string,
        validate_number,
        validate_safe_id,
    )
except ImportError:
    try:
        from ...event_recording.core.validation import (  # noqa: F401
            FieldSpec,
            IdSpec,
            NumberSpec,
            StringSpec,
            ValidationError,
            validate_bounded_string,
            validate_number,
            validate_safe_id,
        )
    except ImportError:

        class ValidationError(ValueError):  # type: ignore[no-redef]
            pass

        @dataclass(frozen=True, slots=True)
        class FieldSpec:  # type: ignore[no-redef]
            name: str
            allow_none: bool = False
            default: Any = None

            def validate(self, val: Any) -> Any:
                raise NotImplementedError

        @dataclass(frozen=True, slots=True)
        class NumberSpec(FieldSpec):  # type: ignore[no-redef]
            min_val: float | int | None = None
            max_val: float | int | None = None
            is_integer: bool = False
            integer: bool = False
            allow_zero: bool = True
            default: float | int | None = None

            def validate(self, val: Any) -> float | int | None:
                if val is None:
                    if self.allow_none:
                        return self.default
                    raise ValidationError(f"{self.name} cannot be None")
                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    raise ValidationError(f"{self.name} must be a number, got {type(val).__name__}")
                try:
                    num_float = float(val)
                except OverflowError as err:
                    raise ValidationError(f"{self.name} too large") from err
                if not math.isfinite(num_float):
                    raise ValidationError(f"{self.name} must be finite")
                if self.is_integer or self.integer:
                    if isinstance(val, float) and not val.is_integer():
                        raise ValidationError(f"{self.name} must be an integer")
                    result = int(val)
                else:
                    result = float(val) if isinstance(val, int) else val
                if not self.allow_zero and result == 0:
                    raise ValidationError(f"{self.name} cannot be zero")
                if self.min_val is not None and result < self.min_val:
                    raise ValidationError(f"{self.name} must be >= {self.min_val}")
                if self.max_val is not None and result > self.max_val:
                    raise ValidationError(f"{self.name} must be <= {self.max_val}")
                return result


# Canonical shared declarative specifications for Event Vision
SPEC_CONCURRENCY = NumberSpec(
    name="concurrency",
    min_val=MIN_CONCURRENCY,
    max_val=MAX_CONCURRENCY,
    is_integer=True,
    default=DEFAULT_CONCURRENCY,
)
SPEC_MAX_FRAMES = NumberSpec(
    name="max_frames",
    min_val=MIN_FRAMES,
    max_val=MAX_FRAMES_BUDGET,
    is_integer=True,
    default=DEFAULT_MAX_FRAMES,
)
SPEC_JOB_DEADLINE = NumberSpec(
    name="job_deadline",
    min_val=MIN_JOB_DEADLINE,
    max_val=MAX_JOB_DEADLINE,
    is_integer=False,
    default=DEFAULT_JOB_DEADLINE,
)
SPEC_PROVIDER_TIMEOUT = NumberSpec(
    name="provider_timeout",
    min_val=MIN_PROVIDER_TIMEOUT,
    max_val=MAX_PROVIDER_TIMEOUT,
    is_integer=False,
    default=DEFAULT_PROVIDER_TIMEOUT,
)
SPEC_EXTRACTION_TIMEOUT = NumberSpec(
    name="extraction_timeout",
    min_val=MIN_EXTRACTION_TIMEOUT,
    max_val=MAX_EXTRACTION_TIMEOUT,
    is_integer=False,
    default=DEFAULT_EXTRACTION_TIMEOUT,
)
SPEC_MAX_ATTEMPTS = NumberSpec(
    name="max_attempts",
    min_val=MIN_ATTEMPTS,
    max_val=MAX_ATTEMPTS_BOUND,
    is_integer=True,
    default=DEFAULT_MAX_ATTEMPTS,
)
SPEC_FUSION_JOB_DEADLINE = NumberSpec(
    name="fusion_job_deadline",
    min_val=MIN_FUSION_JOB_DEADLINE,
    max_val=MAX_FUSION_JOB_DEADLINE,
    is_integer=False,
    default=DEFAULT_FUSION_JOB_DEADLINE,
)
SPEC_FUSION_MAX_FRAMES = NumberSpec(
    name="fusion_max_frames",
    min_val=MIN_FUSION_MAX_FRAMES,
    max_val=MAX_FUSION_MAX_FRAMES,
    is_integer=True,
    default=DEFAULT_FUSION_MAX_FRAMES,
)
SPEC_FRAME_CACHE_MB = NumberSpec(
    name="frame_cache_mb",
    min_val=MIN_FRAME_CACHE_MB,
    max_val=MAX_FRAME_CACHE_MB,
    is_integer=True,
    default=DEFAULT_FRAME_CACHE_MB,
)
SPEC_FRAME_CACHE_TTL = NumberSpec(
    name="frame_cache_ttl",
    min_val=MIN_FRAME_CACHE_TTL,
    max_val=MAX_FRAME_CACHE_TTL,
    is_integer=False,
    default=DEFAULT_FRAME_CACHE_TTL,
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


PROFILE_POLICY_VERSION = 1


def build_profile_payload(
    model: str,
    instructions: str | None = None,
    max_frames: int = DEFAULT_MAX_FRAMES,
    sampling_policy: str = DEFAULT_SAMPLING_POLICY,
    base_prompt: str = GENERIC_MEDIA_PROMPT,
    policy_version: int = PROFILE_POLICY_VERSION,
) -> dict[str, Any]:
    """Build canonical dictionary of effective semantic configuration."""
    return {
        "base_prompt": (base_prompt or GENERIC_MEDIA_PROMPT).strip(),
        "instructions": (instructions or "").strip(),
        "max_frames": int(max_frames),
        "model": model.strip(),
        "policy_version": int(policy_version),
        "sampling_policy": (sampling_policy or DEFAULT_SAMPLING_POLICY).strip(),
    }


def canonical_profile_json(payload: dict[str, Any]) -> str:
    """Serialize profile dictionary to canonical sorted compact JSON."""
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def compute_profile_digest(
    model: str,
    instructions: str | None = None,
    max_frames: int = DEFAULT_MAX_FRAMES,
    sampling_policy: str = DEFAULT_SAMPLING_POLICY,
    base_prompt: str = GENERIC_MEDIA_PROMPT,
    policy_version: int = PROFILE_POLICY_VERSION,
) -> str:
    """Compute deterministic cryptographic digest from canonical sorted JSON."""
    payload = build_profile_payload(
        model=model,
        instructions=instructions,
        max_frames=max_frames,
        sampling_policy=sampling_policy,
        base_prompt=base_prompt,
        policy_version=policy_version,
    )
    raw = canonical_profile_json(payload).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def build_job_payload(
    model: str,
    instructions: str | None = None,
    max_frames: int = DEFAULT_MAX_FRAMES,
    sampling_policy: str = DEFAULT_SAMPLING_POLICY,
    base_prompt: str = GENERIC_MEDIA_PROMPT,
    policy_version: int = PROFILE_POLICY_VERSION,
    job_deadline: float = DEFAULT_JOB_DEADLINE,
    provider_timeout: float = DEFAULT_PROVIDER_TIMEOUT,
    extraction_timeout: float = DEFAULT_EXTRACTION_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> dict[str, Any]:
    """Build complete durable job execution snapshot including operational budget."""
    semantic = build_profile_payload(
        model=model,
        instructions=instructions,
        max_frames=max_frames,
        sampling_policy=sampling_policy,
        base_prompt=base_prompt,
        policy_version=policy_version,
    )
    return {
        **semantic,
        "budget": {
            "job_deadline": float(job_deadline),
            "provider_timeout": float(provider_timeout),
            "extraction_timeout": float(extraction_timeout),
            "max_attempts": int(max_attempts),
        },
    }


def validate_profile_payload(
    payload_str: str | None,
    expected_digest: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate durable profile payload and return verified (semantic_profile, budget).

    Raises AnalysisError("INVALID_PROFILE", ...) on corrupt or mismatched snapshots.
    Never silently falls back to arbitrary current settings on invalid payload.
    """
    if not payload_str or not isinstance(payload_str, str):
        raise AnalysisError(
            "INVALID_PROFILE", "Missing or invalid profile payload string", retryable=False
        )

    try:
        data = json.loads(payload_str)
    except Exception as err:
        raise AnalysisError(
            "INVALID_PROFILE", f"Corrupt profile payload JSON: {err}", retryable=False
        ) from err

    if not isinstance(data, dict):
        raise AnalysisError(
            "INVALID_PROFILE", "Profile payload must be a JSON object", retryable=False
        )

    # 1. Policy version check: must be integer == 1 (non-bool)
    p_ver = data.get("policy_version")
    if isinstance(p_ver, bool) or not isinstance(p_ver, int) or p_ver != PROFILE_POLICY_VERSION:
        raise AnalysisError(
            "INVALID_PROFILE", f"Unsupported policy_version in snapshot: {p_ver}", retryable=False
        )

    # 2. Semantic fields validation
    model = data.get("model")
    if not isinstance(model, str) or not model.strip():
        raise AnalysisError(
            "INVALID_PROFILE", "Invalid or missing model in snapshot", retryable=False
        )
    model = model.strip()

    try:
        max_frames = int(SPEC_MAX_FRAMES.validate(data.get("max_frames")))
    except (ValidationError, ValueError, TypeError) as err:
        raise AnalysisError(
            "INVALID_PROFILE", f"Invalid max_frames in snapshot: {err}", retryable=False
        ) from err

    sampling_policy = data.get("sampling_policy")
    if not isinstance(sampling_policy, str) or sampling_policy not in VALID_SAMPLING_POLICIES:
        raise AnalysisError(
            "INVALID_PROFILE",
            f"Invalid sampling_policy in snapshot: {sampling_policy}",
            retryable=False,
        )

    base_prompt = data.get("base_prompt")
    if base_prompt is not None and not isinstance(base_prompt, str):
        raise AnalysisError("INVALID_PROFILE", "base_prompt must be a string", retryable=False)
    base_prompt_str = (base_prompt or GENERIC_MEDIA_PROMPT).strip()

    instructions = data.get("instructions")
    if instructions is not None and not isinstance(instructions, str):
        raise AnalysisError(
            "INVALID_PROFILE", "instructions must be a string or None", retryable=False
        )
    instructions_str = instructions.strip() if instructions else ""

    # Reconstruct canonical semantic dictionary
    semantic_dict = {
        "base_prompt": base_prompt_str,
        "instructions": instructions_str,
        "max_frames": max_frames,
        "model": model,
        "policy_version": p_ver,
        "sampling_policy": sampling_policy,
    }

    # 3. Digest verification if requested
    if expected_digest:
        raw_canonical = canonical_profile_json(semantic_dict).encode("utf-8")
        computed_digest = hashlib.sha256(raw_canonical).hexdigest()[:16]
        if computed_digest != expected_digest:
            raise AnalysisError(
                "INVALID_PROFILE",
                f"Profile payload digest mismatch (expected {expected_digest}, computed {computed_digest})",
                retryable=False,
            )

    # 4. Operational budget extraction and validation
    budget_raw = data.get("budget")
    if isinstance(budget_raw, dict):
        b_deadline = budget_raw.get("job_deadline")
        b_provider = budget_raw.get("provider_timeout")
        b_extraction = budget_raw.get("extraction_timeout")
        b_attempts = budget_raw.get("max_attempts", DEFAULT_MAX_ATTEMPTS)

        ok, err = validate_budget_bounds(
            job_deadline=b_deadline,
            provider_timeout=b_provider,
            extraction_timeout=b_extraction,
            max_frames=max_frames,
            max_attempts=b_attempts,
        )
        if not ok:
            raise AnalysisError(
                "INVALID_PROFILE", f"Invalid budget bounds in snapshot: {err}", retryable=False
            )

        budget_dict = {
            "job_deadline": float(b_deadline),
            "provider_timeout": float(b_provider),
            "extraction_timeout": float(b_extraction),
            "max_attempts": int(b_attempts),
        }
    else:
        # Legacy job without persisted budget snapshot: derive safe coupled bounds for max_frames
        b_ext = DEFAULT_EXTRACTION_TIMEOUT
        b_prov = DEFAULT_PROVIDER_TIMEOUT
        b_cas = CAS_CLEANUP_MARGIN_SECONDS
        b_dead = max(DEFAULT_JOB_DEADLINE, (max_frames * b_ext) + b_prov + b_cas)
        budget_dict = {
            "job_deadline": float(b_dead),
            "provider_timeout": float(b_prov),
            "extraction_timeout": float(b_ext),
            "max_attempts": DEFAULT_MAX_ATTEMPTS,
        }

    return semantic_dict, budget_dict


def derive_reservation_ttl(
    job_deadline: float, cas_margin: float = CAS_CLEANUP_MARGIN_SECONDS
) -> float:
    """Derive annotation reservation TTL ensuring job execution + CAS margin fits."""
    return float(job_deadline + cas_margin)


def derive_lease_pin_seconds(
    job_deadline: float, cas_margin: float = CAS_CLEANUP_MARGIN_SECONDS
) -> float:
    """Derive media lease pin duration guaranteeing retention does not delete media during execution."""
    return max(300.0, float(job_deadline + cas_margin + 60.0))


def derive_queue_lease_seconds(
    job_deadline: float, cas_margin: float = CAS_CLEANUP_MARGIN_SECONDS
) -> float:
    """Derive queue lease duration guaranteeing the lease cannot expire while CAS completion is still running."""
    return float(job_deadline + cas_margin + 30.0)


def _validate_spec_for_budget(
    spec: NumberSpec, val: Any, unit: str = ""
) -> tuple[bool, float | int | None, str | None]:
    """Validate a budget field against its canonical NumberSpec."""
    try:
        res = spec.validate(val)
        return True, res, None
    except (ValidationError, ValueError, TypeError) as err:
        msg = str(err)
        if (
            ("must be >=" in msg or "must be <=" in msg)
            and spec.min_val is not None
            and spec.max_val is not None
        ):
            min_str = (
                f"{int(spec.min_val)}{unit}"
                if float(spec.min_val).is_integer()
                else f"{spec.min_val}{unit}"
            )
            max_str = (
                f"{int(spec.max_val)}{unit}"
                if float(spec.max_val).is_integer()
                else f"{spec.max_val}{unit}"
            )
            return False, None, f"{spec.name} must be between {min_str} and {max_str} ({msg})"
        return False, None, msg


def validate_budget_bounds(
    job_deadline: Any,
    provider_timeout: Any,
    extraction_timeout: Any,
    max_frames: Any,
    max_attempts: Any = 3,
    cas_margin: float = CAS_CLEANUP_MARGIN_SECONDS,
) -> tuple[bool, str | None]:
    """Ensure selected budget can fit worst-case sequential extraction + provider timeout + CAS margin."""
    checked: dict[str, Any] = {}
    for spec, raw_val, unit in (
        (SPEC_JOB_DEADLINE, job_deadline, "s"),
        (SPEC_PROVIDER_TIMEOUT, provider_timeout, "s"),
        (SPEC_EXTRACTION_TIMEOUT, extraction_timeout, "s"),
        (SPEC_MAX_FRAMES, max_frames, ""),
        (SPEC_MAX_ATTEMPTS, max_attempts, ""),
    ):
        ok, clean_val, err = _validate_spec_for_budget(spec, raw_val, unit=unit)
        if not ok:
            return False, err
        checked[spec.name] = clean_val

    deadline_float = float(checked["job_deadline"])
    provider_float = float(checked["provider_timeout"])
    extraction_float = float(checked["extraction_timeout"])
    max_frames_int = int(checked["max_frames"])

    # Sequential worst-case bound: max_frames * extraction_timeout + provider_timeout + cas_margin
    min_required = (max_frames_int * extraction_float) + provider_float + cas_margin
    if deadline_float < min_required:
        return False, (
            f"Job deadline ({deadline_float:.1f}s) is insufficient for selected budget "
            f"(sequential extraction for {max_frames_int} frames at {extraction_float:.1f}s each = "
            f"{max_frames_int * extraction_float:.1f}s + provider {provider_float:.1f}s + "
            f"CAS margin {cas_margin:.1f}s requires at least {min_required:.1f}s)"
        )
    return True, None


def validate_fusion_budget_bounds(
    fusion_job_deadline: Any,
    fusion_max_frames: Any,
    provider_timeout: Any = DEFAULT_PROVIDER_TIMEOUT,
    extraction_timeout: Any = DEFAULT_EXTRACTION_TIMEOUT,
    max_attempts: Any = DEFAULT_MAX_ATTEMPTS,
    cas_margin: float = CAS_CLEANUP_MARGIN_SECONDS,
) -> tuple[bool, str | None]:
    """Ensure selected fusion budget can fit multi-camera extraction + provider timeout + CAS margin."""
    checked: dict[str, Any] = {}
    for spec, raw_val, unit in (
        (SPEC_FUSION_JOB_DEADLINE, fusion_job_deadline, "s"),
        (SPEC_FUSION_MAX_FRAMES, fusion_max_frames, ""),
        (SPEC_PROVIDER_TIMEOUT, provider_timeout, "s"),
        (SPEC_EXTRACTION_TIMEOUT, extraction_timeout, "s"),
        (SPEC_MAX_ATTEMPTS, max_attempts, ""),
    ):
        ok, clean_val, err = _validate_spec_for_budget(spec, raw_val, unit=unit)
        if not ok:
            return False, err
        checked[spec.name] = clean_val

    deadline_float = float(checked["fusion_job_deadline"])
    fusion_frames_int = int(checked["fusion_max_frames"])
    provider_float = float(checked["provider_timeout"])
    extraction_float = float(checked["extraction_timeout"])

    # Worst-case: extracting up to fusion_frames_int frames across views + provider call + CAS margin
    min_required = (fusion_frames_int * extraction_float) + provider_float + cas_margin
    if deadline_float < min_required:
        return False, (
            f"Fusion job deadline ({deadline_float:.1f}s) is insufficient for selected budget "
            f"(sequential extraction for {fusion_frames_int} frames at {extraction_float:.1f}s each = "
            f"{fusion_frames_int * extraction_float:.1f}s + provider {provider_float:.1f}s + "
            f"CAS margin {cas_margin:.1f}s requires at least {min_required:.1f}s)"
        )
    return True, None


def validate_input_bytes(size_bytes: Any) -> tuple[bool, str | None]:
    """Validate that input media bytes are within static bounds (10MB)."""
    if isinstance(size_bytes, bool) or not isinstance(size_bytes, (int, float)):
        return False, "Input bytes must be a numeric integer"
    if not math.isfinite(size_bytes):
        return False, "Input bytes must be finite"
    size_int = int(size_bytes)
    if size_int < 0:
        return False, f"Input bytes cannot be negative: {size_int}"
    if size_int > MAX_IMAGE_SOURCE_BYTES:
        return False, (
            f"Input size ({size_int} bytes) exceeds maximum allowable limit of "
            f"{MAX_IMAGE_SOURCE_BYTES} bytes ({MAX_IMAGE_SOURCE_BYTES // (1024 * 1024)} MB)"
        )
    return True, None


def is_rolling_model_alias(model: str) -> bool:
    """Return True if model name appears to be a rolling alias (e.g. contains '-latest')."""
    m = model.lower().strip()
    return "-latest" in m or m.endswith("-preview") or m == "gemini-pro"


def model_alias_info(model: str) -> str:
    """Return factual classification of model alias vs fixed checkpoint."""
    m = model.lower().strip()
    if is_rolling_model_alias(m):
        return "Rolling model alias (resolves dynamically to latest provider checkpoint)"
    if re.search(r"-\d{3,4}$", m) or re.search(r"-\d{8}$", m) or re.search(r"-v\d+", m):
        return "Fixed model version checkpoint"
    return "Custom or unverified model identifier (versioning policy unknown)"


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

    def __init__(
        self,
        code: str,
        message: str,
        retryable: bool = False,
        retry_delay: float | None = None,
        *,
        timeout_source: str | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable
        self.retry_delay = retry_delay
        self.timeout_source = timeout_source


class GeminiKeyError(AnalysisError):
    """Missing or invalid Gemini API key."""


class GeminiSchemaError(AnalysisError):
    """Model response failed schema validation."""


class FrameExtractionError(AnalysisError):
    """Error during frame extraction."""


# -----------------------------------------------------------------------------
# Result and Annotation Data Structures
# -----------------------------------------------------------------------------


def _validate_coverage_int(val: Any, name: str) -> int:
    """Validate that a coverage count is a genuine non-boolean, non-negative bounded integer."""
    if isinstance(val, bool) or not isinstance(val, int):
        raise ValueError(f"{name} must be an integer, got {type(val).__name__}")
    if val < 0 or val > 10000:
        raise ValueError(f"{name} must be between 0 and 10000, got {val}")
    return val


@dataclass(frozen=True)
class ObservationCoverage:
    """Structured observation coverage metadata for completed event analyses.

    Explicitly records bounded sampling plan fulfillment ('complete' means all planned
    frames and planned clips in the bounded sampling plan were extracted; 'partial' means
    some planned frames or clips failed extraction). Never equates sampling-plan fulfillment
    to exhaustive event or continuous-video capture completeness; unselected_clips records
    available clips deliberately excluded by the frame budget.
    """

    planned_frames: int
    extracted_frames: int
    planned_clips: int
    sampled_clips: int
    missing_clips: int
    observation_state: str  # "complete" | "partial" (sampling plan fulfillment)
    is_partial: bool | None = None
    available_clips: int | None = None
    unselected_clips: int | None = None
    source_capture_state: str | None = None
    source_time_uncertainty_seconds: float | None = None

    def __post_init__(self) -> None:
        _validate_coverage_int(self.planned_frames, "planned_frames")
        _validate_coverage_int(self.extracted_frames, "extracted_frames")
        _validate_coverage_int(self.planned_clips, "planned_clips")
        _validate_coverage_int(self.sampled_clips, "sampled_clips")
        _validate_coverage_int(self.missing_clips, "missing_clips")

        if self.extracted_frames > self.planned_frames:
            raise ValueError("extracted_frames cannot exceed planned_frames")
        if self.sampled_clips > self.planned_clips:
            raise ValueError("sampled_clips cannot exceed planned_clips")
        if self.missing_clips != (self.planned_clips - self.sampled_clips):
            raise ValueError("missing_clips must equal planned_clips - sampled_clips")

        if self.available_clips is None and self.unselected_clips is None:
            object.__setattr__(self, "available_clips", self.planned_clips)
            object.__setattr__(self, "unselected_clips", 0)
        elif self.available_clips is not None and self.unselected_clips is None:
            _validate_coverage_int(self.available_clips, "available_clips")
            if self.available_clips < self.planned_clips:
                raise ValueError("available_clips cannot be less than planned_clips")
            object.__setattr__(self, "unselected_clips", self.available_clips - self.planned_clips)
        elif self.available_clips is None and self.unselected_clips is not None:
            _validate_coverage_int(self.unselected_clips, "unselected_clips")
            derived_available = self.planned_clips + self.unselected_clips
            _validate_coverage_int(derived_available, "available_clips")
            object.__setattr__(self, "available_clips", derived_available)
        else:
            assert self.available_clips is not None and self.unselected_clips is not None
            _validate_coverage_int(self.available_clips, "available_clips")
            _validate_coverage_int(self.unselected_clips, "unselected_clips")
            if self.available_clips < self.planned_clips:
                raise ValueError("available_clips cannot be less than planned_clips")
            if self.unselected_clips != (self.available_clips - self.planned_clips):
                raise ValueError("unselected_clips must equal available_clips - planned_clips")

        if not isinstance(self.observation_state, str) or self.observation_state not in (
            "complete",
            "partial",
        ):
            raise ValueError(f"Invalid observation_state: {self.observation_state}")

        if self.observation_state == "complete" and (
            self.extracted_frames < self.planned_frames or self.missing_clips > 0
        ):
            raise ValueError(
                "observation_state cannot be 'complete' when planned frames or clips are missing"
            )

        expected_partial = self.observation_state == "partial"
        if self.is_partial is None:
            object.__setattr__(self, "is_partial", expected_partial)
        else:
            if not isinstance(self.is_partial, bool):
                raise ValueError("is_partial must be a boolean")
            if self.is_partial != expected_partial:
                raise ValueError(
                    f"is_partial ({self.is_partial}) is inconsistent with observation_state ('{self.observation_state}')"
                )

        if self.source_capture_state is not None:
            if not isinstance(self.source_capture_state, str):
                raise ValueError("source_capture_state must be a string")
            clean_cs = self.source_capture_state.strip()
            if (
                not clean_cs
                or len(clean_cs) > 64
                or any(c in clean_cs for c in ("/", "\\", ":"))
                or not re.match(r"^[a-zA-Z0-9_-]+$", clean_cs)
            ):
                raise ValueError("Invalid source_capture_state")
            object.__setattr__(self, "source_capture_state", clean_cs)

        if self.source_time_uncertainty_seconds is not None:
            if isinstance(self.source_time_uncertainty_seconds, bool) or not isinstance(
                self.source_time_uncertainty_seconds, (int, float)
            ):
                raise ValueError(
                    "source_time_uncertainty_seconds must be a finite non-negative number"
                )
            u_val = float(self.source_time_uncertainty_seconds)
            if math.isnan(u_val) or not math.isfinite(u_val) or u_val < 0.0 or u_val > 86400.0:
                raise ValueError(
                    "source_time_uncertainty_seconds must be a finite non-negative number <= 86400"
                )
            object.__setattr__(self, "source_time_uncertainty_seconds", u_val)

    def to_dict(self) -> dict[str, Any]:
        """Convert to strict dictionary with bounded types."""
        d: dict[str, Any] = {
            "available_clips": int(
                self.available_clips if self.available_clips is not None else self.planned_clips
            ),
            "planned_frames": int(self.planned_frames),
            "extracted_frames": int(self.extracted_frames),
            "planned_clips": int(self.planned_clips),
            "sampled_clips": int(self.sampled_clips),
            "missing_clips": int(self.missing_clips),
            "unselected_clips": int(
                self.unselected_clips if self.unselected_clips is not None else 0
            ),
            "observation_state": self.observation_state,
            "is_partial": bool(self.is_partial),
        }
        if self.source_capture_state is not None:
            d["source_capture_state"] = self.source_capture_state
        if self.source_time_uncertainty_seconds is not None:
            d["source_time_uncertainty_seconds"] = float(self.source_time_uncertainty_seconds)
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> ObservationCoverage:
        """Construct and strictly validate from dictionary."""
        if not isinstance(data, dict):
            raise ValueError("coverage data must be a dictionary")
        required_keys = (
            "planned_frames",
            "extracted_frames",
            "planned_clips",
            "sampled_clips",
            "missing_clips",
            "observation_state",
        )
        for k in required_keys:
            if k not in data:
                raise ValueError(f"Missing required coverage field: {k}")

        return cls(
            planned_frames=data["planned_frames"],
            extracted_frames=data["extracted_frames"],
            planned_clips=data["planned_clips"],
            sampled_clips=data["sampled_clips"],
            missing_clips=data["missing_clips"],
            observation_state=data["observation_state"],
            is_partial=data.get("is_partial"),
            available_clips=data.get("available_clips"),
            unselected_clips=data.get("unselected_clips"),
            source_capture_state=data.get("source_capture_state"),
            source_time_uncertainty_seconds=data.get("source_time_uncertainty_seconds"),
        )


@dataclass(frozen=True)
class AnalysisResult:
    """Structured result parsed from provider response."""

    summary: str
    activity: bool
    objects: list[str] = field(default_factory=list)
    model_version: str | None = None

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
      "model_version"?: str,
      "updated_at"?: str (ISO-8601),
      "frames"?: list[dict[str, Any]],
      "coverage"?: dict[str, Any]
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
    model_version: str | None = None
    updated_at: str | None = None
    frames: list[dict[str, Any]] | None = None
    coverage: ObservationCoverage | dict[str, Any] | None = None

    def __post_init__(self) -> None:
        if self.status not in VALID_STATUSES:
            raise ValueError(f"Invalid status: {self.status}")
        if (
            isinstance(self.media_revision, bool)
            or not isinstance(self.media_revision, int)
            or self.media_revision < 1
        ):
            raise ValueError("media_revision must be positive integer")
        if (
            isinstance(self.generation, bool)
            or not isinstance(self.generation, int)
            or self.generation < 1
        ):
            raise ValueError("generation must be positive integer")
        if self.coverage is not None:
            if isinstance(self.coverage, dict):
                object.__setattr__(self, "coverage", ObservationCoverage.from_dict(self.coverage))
            elif not isinstance(self.coverage, ObservationCoverage):
                raise ValueError("coverage must be an ObservationCoverage instance or valid dict")

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
        if self.model_version is not None:
            payload["model_version"] = self.model_version
        if self.updated_at is not None:
            payload["updated_at"] = self.updated_at
        if self.frames is not None:
            payload["frames"] = self.frames
        if self.coverage is not None:
            assert isinstance(self.coverage, ObservationCoverage)
            payload["coverage"] = self.coverage.to_dict()
        return payload

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> AnalysisAnnotation:
        """Construct from raw dictionary."""
        raw_cov = data.get("coverage")
        cov_obj: ObservationCoverage | None = None
        if raw_cov is not None:
            if isinstance(raw_cov, ObservationCoverage):
                cov_obj = raw_cov
            elif isinstance(raw_cov, dict):
                cov_obj = ObservationCoverage.from_dict(raw_cov)
            else:
                raise ValueError("Invalid coverage field in annotation")

        return cls(
            status=data["status"],
            media_revision=data["media_revision"],
            generation=data["generation"],
            summary=data.get("summary"),
            activity=data.get("activity"),
            objects=data.get("objects"),
            error_code=data.get("error_code"),
            model=data.get("model"),
            model_version=data.get("model_version"),
            updated_at=data.get("updated_at"),
            frames=data.get("frames"),
            coverage=cov_obj,
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
    profile_payload: str | None = None
    claim_token: str | None = None

    @property
    def lease_token(self) -> str | None:
        """Alias for claim_token providing backward and forward compatibility."""
        return self.claim_token


@dataclass(frozen=True)
class ExtractedFrame:
    """An extracted image frame ready for provider transport and provenance tracking."""

    data: bytes
    mime_type: str = "image/jpeg"
    media_id: str | None = None
    offset_seconds: float = 0.0
    timestamp_quality: str = "unknown"  # "exact" | "estimated" | "relative_fallback" | "unknown"
    offset_quality: str = "exact"  # "exact" | "estimated" | "relative_fallback" | "unknown"


@dataclass(frozen=True)
class Incident:
    """Core neutral frozen incident model matching the cross-component contract."""

    incident_id: str
    anchor_at: datetime
    membership_revision: int
    span_start: datetime | None = None
    span_end: datetime | None = None
    annotations: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class IncidentInputs:
    """Immutable snapshot of incident context and member recorded events."""

    incident: Incident
    events: tuple[Any, ...]  # tuple of RecordedEvent
    input_token: str


@dataclass(frozen=True)
class FusionJob:
    """Durable fusion job tracking record keyed by (incident_id, input_token, profile_digest)."""

    id: int | None
    incident_id: str
    input_token: str
    profile_digest: str
    generation: int = 1
    annotation_generation: int | None = None
    status: str = "pending"
    attempts: int = 0
    leased_by: str | None = None
    leased_until: datetime | None = None
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    is_manual: bool = False
    created_at: datetime = field(default_factory=utc_now)
    updated_at: datetime = field(default_factory=utc_now)
    profile_payload: str | None = None
    claim_token: str | None = None

    @property
    def lease_token(self) -> str | None:
        """Alias for claim_token providing backward and forward compatibility."""
        return self.claim_token


@dataclass(frozen=True)
class FusionViewFrame:
    """Frame extracted from a member camera view for multi-camera visual fusion."""

    frame_ref: str  # e.g., "view1_frame0"
    data: bytes
    mime_type: str = "image/jpeg"
    timestamp_ms: int = 0
    time_quality: str = "relative"  # "known_utc" | "relative" | "unknown"
    offset_seconds: float = 0.0


@dataclass(frozen=True)
class FusionViewInput:
    """Anonymized member view with frames and sanitized per-view observations."""

    view_index: int  # 1-based index (1, 2, ...)
    view_label: str  # e.g., "View 1", "View 2"
    time_quality: str  # "known_utc" | "relative" | "unknown"
    observations: str | None = None  # Generic summary from per-camera analysis
    frames: list[FusionViewFrame] = field(default_factory=list)


@dataclass(frozen=True)
class PossibleCrossViewMatch:
    """Cross-view correspondence hypothesis with explicit uncertainty and evidence refs."""

    description: str
    evidence_refs: list[str]
    uncertainty: str = "medium"  # "low" | "medium" | "high"
    same_subject_confidence: str = "uncertain"  # "unlikely" | "possible" | "uncertain"


@dataclass(frozen=True)
class FusionResult:
    """Validated result of visual multi-camera fusion analysis."""

    narrative: str
    activity: bool
    possible_cross_view_matches: list[dict[str, Any]]
    model_version: str


@dataclass(frozen=True)
class FusionAnnotation:
    """Wire representation stored in event_recording incident annotations."""

    status: str
    input_token: str
    generation: int
    profile_digest: str
    narrative: str | None = None
    activity: bool | None = None
    possible_cross_view_matches: list[dict[str, Any]] | None = None
    error_code: str | None = None
    skipped_reason: str | None = None
    model: str | None = None
    model_version: str | None = None
    updated_at: str | None = None
    member_events: list[str] | None = None
    view_frames: list[dict[str, Any]] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to strict wire dictionary with non-None optional fields."""
        d: dict[str, Any] = {
            "status": self.status,
            "input_token": self.input_token,
            "generation": self.generation,
            "profile_digest": self.profile_digest,
        }
        if self.narrative is not None:
            d["narrative"] = self.narrative[: MAX_SUMMARY_LEN * 2]
        if self.activity is not None:
            d["activity"] = self.activity
        if self.possible_cross_view_matches is not None:
            d["possible_cross_view_matches"] = self.possible_cross_view_matches[:20]
        if self.error_code is not None:
            d["error_code"] = sanitize_error_code(self.error_code)
        if self.skipped_reason is not None:
            d["skipped_reason"] = sanitize_error_code(self.skipped_reason)
        if self.model is not None:
            d["model"] = self.model
        if self.model_version is not None:
            d["model_version"] = self.model_version
        if self.updated_at is not None:
            d["updated_at"] = self.updated_at
        if self.member_events is not None:
            d["member_events"] = self.member_events
        if self.view_frames is not None:
            d["view_frames"] = self.view_frames
        return d

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> FusionAnnotation:
        """Construct from raw dictionary."""
        return cls(
            status=data["status"],
            input_token=data["input_token"],
            generation=data["generation"],
            profile_digest=data["profile_digest"],
            narrative=data.get("narrative"),
            activity=data.get("activity"),
            possible_cross_view_matches=data.get("possible_cross_view_matches"),
            error_code=data.get("error_code"),
            skipped_reason=data.get("skipped_reason"),
            model=data.get("model"),
            model_version=data.get("model_version"),
            updated_at=data.get("updated_at"),
            member_events=data.get("member_events"),
            view_frames=data.get("view_frames"),
        )


def compute_fusion_profile_digest(
    model: str = DEFAULT_MODEL,
    fusion_prompt: str = DEFAULT_FUSION_PROMPT,
    instructions: str | None = None,
    max_frames: int = DEFAULT_FUSION_MAX_FRAMES,
    sampling_policy: str = DEFAULT_SAMPLING_POLICY,
    version: int = PROFILE_POLICY_VERSION,
) -> str:
    """Compute deterministic cryptographic digest from canonical sorted JSON for fusion."""
    payload = {
        "fusion_prompt": (fusion_prompt or DEFAULT_FUSION_PROMPT).strip(),
        "instructions": (instructions or "").strip(),
        "max_frames": int(max_frames),
        "model": model.strip(),
        "policy_version": int(version),
        "sampling_policy": (sampling_policy or DEFAULT_SAMPLING_POLICY).strip(),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()[:16]


def build_fusion_job_payload(
    model: str = DEFAULT_MODEL,
    fusion_prompt: str = DEFAULT_FUSION_PROMPT,
    instructions: str | None = None,
    max_frames: int = DEFAULT_FUSION_MAX_FRAMES,
    sampling_policy: str = DEFAULT_SAMPLING_POLICY,
    job_deadline: float = DEFAULT_FUSION_JOB_DEADLINE,
    provider_timeout: float = DEFAULT_PROVIDER_TIMEOUT,
    extraction_timeout: float = DEFAULT_EXTRACTION_TIMEOUT,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    version: int = PROFILE_POLICY_VERSION,
) -> dict[str, Any]:
    """Build complete durable fusion job execution snapshot including operational budget."""
    return {
        "fusion_prompt": (fusion_prompt or DEFAULT_FUSION_PROMPT).strip(),
        "instructions": (instructions or "").strip(),
        "max_frames": int(max_frames),
        "model": model.strip(),
        "policy_version": int(version),
        "sampling_policy": (sampling_policy or DEFAULT_SAMPLING_POLICY).strip(),
        "budget": {
            "job_deadline": float(job_deadline),
            "provider_timeout": float(provider_timeout),
            "extraction_timeout": float(extraction_timeout),
            "max_attempts": int(max_attempts),
        },
    }


def validate_fusion_profile_payload(
    payload_str: str,
    expected_digest: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Validate snapshot against strict bounds and verify profile digest consistency."""
    if not payload_str or not isinstance(payload_str, str):
        raise AnalysisError(
            "INVALID_PROFILE", "Missing or non-string fusion payload", retryable=False
        )

    try:
        data = json.loads(payload_str)
    except Exception as err:
        raise AnalysisError(
            "INVALID_PROFILE", f"Malformed fusion profile JSON: {err}", retryable=False
        ) from err

    if not isinstance(data, dict):
        raise AnalysisError(
            "INVALID_PROFILE", "Fusion profile payload must be a JSON object", retryable=False
        )

    model_raw = data.get("model", "")
    if not isinstance(model_raw, str):
        raise AnalysisError("INVALID_PROFILE", "Invalid model name", retryable=False)
    model = model_raw.strip()
    if not model or len(model) > 100:
        raise AnalysisError("INVALID_PROFILE", f"Invalid model name: '{model}'", retryable=False)

    try:
        max_frames = int(
            SPEC_FUSION_MAX_FRAMES.validate(data.get("max_frames", DEFAULT_FUSION_MAX_FRAMES))
        )
    except (ValidationError, ValueError, TypeError) as err:
        raise AnalysisError(
            "INVALID_PROFILE", f"Invalid max_frames: {err}", retryable=False
        ) from err

    sampling_policy_raw = data.get("sampling_policy", DEFAULT_SAMPLING_POLICY)
    if not isinstance(sampling_policy_raw, str):
        raise AnalysisError(
            "INVALID_PROFILE", f"Invalid sampling_policy: {sampling_policy_raw}", retryable=False
        )
    sampling_policy = sampling_policy_raw.strip()
    if sampling_policy not in VALID_SAMPLING_POLICIES:
        raise AnalysisError(
            "INVALID_PROFILE", f"Invalid sampling_policy: {sampling_policy}", retryable=False
        )

    policy_version = data.get("policy_version", PROFILE_POLICY_VERSION)
    if (
        isinstance(policy_version, bool)
        or not isinstance(policy_version, int)
        or policy_version != PROFILE_POLICY_VERSION
    ):
        raise AnalysisError(
            "INVALID_PROFILE", f"Unsupported policy_version: {policy_version}", retryable=False
        )
    fusion_prompt_raw = data.get("fusion_prompt", DEFAULT_FUSION_PROMPT)
    instructions_raw = data.get("instructions", "")
    if not isinstance(fusion_prompt_raw, str) or not isinstance(instructions_raw, str):
        raise AnalysisError(
            "INVALID_PROFILE", "Fusion prompt and instructions must be strings", retryable=False
        )
    fusion_prompt = fusion_prompt_raw.strip()
    instructions = instructions_raw.strip()

    semantic_dict = {
        "fusion_prompt": fusion_prompt,
        "instructions": instructions,
        "max_frames": max_frames,
        "model": model,
        "policy_version": policy_version,
        "sampling_policy": sampling_policy,
    }

    if expected_digest:
        raw_canonical = json.dumps(semantic_dict, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        computed_digest = hashlib.sha256(raw_canonical).hexdigest()[:16]
        if computed_digest != expected_digest:
            raise AnalysisError(
                "INVALID_PROFILE",
                f"Fusion digest mismatch (expected {expected_digest}, computed {computed_digest})",
                retryable=False,
            )

    budget_raw = data.get("budget", {})
    if not isinstance(budget_raw, dict):
        raise AnalysisError(
            "INVALID_PROFILE", "budget field must be a JSON object", retryable=False
        )

    job_deadline_raw = budget_raw.get("job_deadline", DEFAULT_FUSION_JOB_DEADLINE)
    provider_timeout_raw = budget_raw.get("provider_timeout", DEFAULT_PROVIDER_TIMEOUT)
    extraction_timeout_raw = budget_raw.get("extraction_timeout", DEFAULT_EXTRACTION_TIMEOUT)
    max_attempts_raw = budget_raw.get("max_attempts", DEFAULT_MAX_ATTEMPTS)
    valid_budget, budget_error = validate_fusion_budget_bounds(
        fusion_job_deadline=job_deadline_raw,
        fusion_max_frames=max_frames,
        provider_timeout=provider_timeout_raw,
        extraction_timeout=extraction_timeout_raw,
        max_attempts=max_attempts_raw,
    )
    if not valid_budget:
        raise AnalysisError(
            "INVALID_PROFILE", f"Invalid fusion budget in snapshot: {budget_error}", retryable=False
        )
    job_deadline = float(job_deadline_raw)
    provider_timeout = float(provider_timeout_raw)
    extraction_timeout = float(extraction_timeout_raw)
    max_attempts = int(max_attempts_raw)

    budget_dict = {
        "job_deadline": job_deadline,
        "provider_timeout": provider_timeout,
        "extraction_timeout": extraction_timeout,
        "max_attempts": max_attempts,
    }

    return semantic_dict, budget_dict


def is_user_authorized_for_event(user: Any, event: Any) -> bool:
    """Check whether a user has read access to an event based on ACLs, access entities, and owner.

    Stand-alone implementation with zero mandatory runtime import of event_recording or Home Assistant.
    """
    if not user or not getattr(user, "is_active", True):
        return False
    if getattr(user, "is_admin", False):
        return True

    # 1. Access entities: user must have 'read' permissions for all declared access entities
    access_ents = getattr(event, "access_entities", None)
    if access_ents:
        permissions = getattr(user, "permissions", None)
        if not permissions:
            return False
        return all(permissions.check_entity(ent, "read") for ent in access_ents)

    # 2. Entity resource_key: if resource_key is an entity ID, check read permission
    res_key = getattr(event, "resource_key", None)
    if res_key and isinstance(res_key, str) and "." in res_key:
        permissions = getattr(user, "permissions", None)
        if not permissions:
            return False
        if permissions.check_entity(res_key, "read"):
            return True

    # 3. Owner journal fallback: if user is the owner, allowed
    owner_id = getattr(event, "owner_user_id", None)
    if owner_id and owner_id == getattr(user, "id", None):
        return True

    return False
