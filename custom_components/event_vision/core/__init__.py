"""Pure Python core for Event Vision.

Strict boundary: Zero Home Assistant imports, zero camera/sensor dependencies.
"""

from __future__ import annotations

from .extractor import FrameExtractor, validate_image_bytes
from .gemini import GeminiClient
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
    ensure_utc,
    format_iso,
    sanitize_error_code,
    utc_now,
)
from .store import AnalysisJobStore
from .worker import AnalysisWorker, VisionCoordinator

__all__ = [
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
    "VisionCoordinator",
    "compute_profile_digest",
    "ensure_utc",
    "format_iso",
    "sanitize_error_code",
    "utc_now",
    "validate_image_bytes",
]
