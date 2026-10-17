"""Constants and configuration parameters for Event Vision.

Event Vision is an optional generic media consumer of event_recording.
"""

from __future__ import annotations

DOMAIN = "event_vision"
VERSION = "0.3.0"
RECORDING_DOMAIN = "event_recording"
ANNOTATION_NAMESPACE = "event_vision"
CONSUMER_ID = "event_vision"

# Config entry keys
CONF_GEMINI_API_KEY = "gemini_api_key"
CONF_MODEL = "model"
CONF_CONCURRENCY = "concurrency"
CONF_AUTO_ANALYZE = "auto_analyze"
CONF_PROMPT_INSTRUCTIONS = "prompt_instructions"
CONF_MAX_FRAMES = "max_frames"
CONF_SAMPLING_POLICY = "sampling_policy"
CONF_JOB_DEADLINE = "job_deadline"
CONF_PROVIDER_TIMEOUT = "provider_timeout"
CONF_EXTRACTION_TIMEOUT = "extraction_timeout"
CONF_MAX_ATTEMPTS = "max_attempts"

# Default configuration values
DEFAULT_MODEL = "gemini-flash-lite-latest"
DEFAULT_CONCURRENCY = 1
DEFAULT_AUTO_ANALYZE = True
DEFAULT_PROMPT_INSTRUCTIONS = ""

# Sampling policies & budget
DEFAULT_MAX_FRAMES = 6
MAX_FRAMES = DEFAULT_MAX_FRAMES  # Backward-compatible alias
MIN_FRAMES = 3
MAX_FRAMES_BUDGET = 16

SAMPLING_POLICY_SPARSE_ANCHORS = "sparse_anchors"
SAMPLING_POLICY_TRIGGER_RELEVANT = "trigger_relevant"
DEFAULT_SAMPLING_POLICY = SAMPLING_POLICY_SPARSE_ANCHORS

VALID_SAMPLING_POLICIES = frozenset(
    {
        SAMPLING_POLICY_SPARSE_ANCHORS,
        SAMPLING_POLICY_TRIGGER_RELEVANT,
    }
)

# Coupled Advanced settings: sensible defaults 170 / 60 / 10 / 3
DEFAULT_JOB_DEADLINE = 170.0
MAX_PROCESS_DEADLINE = DEFAULT_JOB_DEADLINE  # Backward-compatible alias
MIN_JOB_DEADLINE = 60.0
MAX_JOB_DEADLINE = 600.0

DEFAULT_PROVIDER_TIMEOUT = 60.0
MIN_PROVIDER_TIMEOUT = 10.0
MAX_PROVIDER_TIMEOUT = 180.0

DEFAULT_EXTRACTION_TIMEOUT = 10.0
MIN_EXTRACTION_TIMEOUT = 2.0
MAX_EXTRACTION_TIMEOUT = 30.0

DEFAULT_MAX_ATTEMPTS = 3
MAX_ATTEMPTS = DEFAULT_MAX_ATTEMPTS  # Backward-compatible alias
MIN_ATTEMPTS = 1
MAX_ATTEMPTS_BOUND = 5

CAS_CLEANUP_MARGIN_SECONDS = 10.0
LEASE_PIN_SECONDS = 300.0  # Default floor; derived at runtime from job deadline

# Concurrency & sizing bounds
MIN_CONCURRENCY = 1
MAX_CONCURRENCY = 4
MAX_PROMPT_INSTRUCTIONS_LEN = 1000
MAX_SUMMARY_LEN = 1000
MAX_OBJECTS = 20
MAX_OBJECT_LEN = 50
BASE_BACKOFF_SECONDS = 1.0
MAX_IMAGE_SOURCE_BYTES = 10 * 1024 * 1024  # 10 MB limit on raw media files
MAX_IMAGE_DIMENSION = 4096  # Max width / height bound

# Wire annotation statuses
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_COMPLETE = "complete"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

VALID_STATUSES = frozenset(
    {
        STATUS_PENDING,
        STATUS_PROCESSING,
        STATUS_COMPLETE,
        STATUS_FAILED,
        STATUS_SKIPPED,
    }
)

# Base Prompt
GENERIC_MEDIA_PROMPT = (
    "Describe the visual activity observed in the provided media frames. "
    "Provide a factual, concise summary of visible motion, subjects, or physical state changes. "
    "Do not speculate on unobserved facts. Output strictly valid JSON."
)

# Incident Visual Fusion settings & bounds
CONF_ENABLE_FUSION = "enable_fusion"
DEFAULT_ENABLE_FUSION = False

CONF_FUSION_MAX_FRAMES = "fusion_max_frames"
DEFAULT_FUSION_MAX_FRAMES = 6
MIN_FUSION_MAX_FRAMES = 2
MAX_FUSION_MAX_FRAMES = 16

CONF_FUSION_PROMPT_INSTRUCTIONS = "fusion_prompt_instructions"
DEFAULT_FUSION_PROMPT_INSTRUCTIONS = ""
MAX_FUSION_PROMPT_INSTRUCTIONS_LEN = 1000

CONF_FUSION_JOB_DEADLINE = "fusion_job_deadline"
DEFAULT_FUSION_JOB_DEADLINE = 180.0
MIN_FUSION_JOB_DEADLINE = 60.0
MAX_FUSION_JOB_DEADLINE = 600.0

DEFAULT_FUSION_PROMPT = (
    "Analyze visual activity across multiple distinct camera views of the same incident. "
    "Synthesize a unified, coherent narrative describing what occurred across all views. "
    "Identify visible activity, subjects, and potential cross-view correspondences. "
    "Do NOT assume personal identity or claim subjects are the same person solely from temporal proximity or co-occurrence. "
    "Report possible matches with explicit uncertainty and evidence references to specific frame IDs. "
    "Output strictly valid JSON conforming to the requested schema."
)

# Extracted Frame Cache settings & bounds
CONF_FRAME_CACHE_MB = "frame_cache_mb"
DEFAULT_FRAME_CACHE_MB = 64
MIN_FRAME_CACHE_MB = 0
MAX_FRAME_CACHE_MB = 512

CONF_FRAME_CACHE_TTL = "frame_cache_ttl"
DEFAULT_FRAME_CACHE_TTL = 300.0
MIN_FRAME_CACHE_TTL = 10.0
MAX_FRAME_CACHE_TTL = 86400.0

# Service names
SERVICE_ANALYZE = "analyze"
SERVICE_ANALYZE_INCIDENT = "analyze_incident"
