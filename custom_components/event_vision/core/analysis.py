"""Direct Gemini REST API adapter, owned frame extractor, and analysis worker."""

from __future__ import annotations

from .extractor import FrameExtractor, validate_image_bytes
from .gemini import GEMINI_HOST, GeminiClient
from .models import (
    AnalysisAnnotation,
    AnalysisError,
    AnalysisJob,
    AnalysisResult,
    ExtractedFrame,
    FrameExtractionError,
    GeminiKeyError,
    GeminiSchemaError,
    compute_profile_digest,
    derive_lease_pin_seconds,
    derive_reservation_ttl,
    is_rolling_model_alias,
    validate_budget_bounds,
)
from .sampling import PlannedAnchor, plan_event_sampling
from .store import AnalysisJobStore
from .worker import AnalysisWorker, VisionCoordinator

__all__ = [
    "GEMINI_HOST",
    "AnalysisAnnotation",
    "AnalysisError",
    "AnalysisJob",
    "AnalysisJobStore",
    "AnalysisResult",
    "AnalysisWorker",
    "ExtractedFrame",
    "FrameExtractionError",
    "FrameExtractor",
    "GeminiClient",
    "GeminiKeyError",
    "GeminiSchemaError",
    "PlannedAnchor",
    "VisionCoordinator",
    "compute_profile_digest",
    "derive_lease_pin_seconds",
    "derive_reservation_ttl",
    "is_rolling_model_alias",
    "plan_event_sampling",
    "validate_budget_bounds",
    "validate_image_bytes",
]
