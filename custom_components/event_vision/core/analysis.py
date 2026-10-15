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
)
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
    "VisionCoordinator",
    "validate_image_bytes",
]
