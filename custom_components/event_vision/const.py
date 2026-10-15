"""Constants and configuration parameters for Event Vision.

Event Vision is an optional generic media consumer of event_recording.
"""

from __future__ import annotations

DOMAIN = "event_vision"
RECORDING_DOMAIN = "event_recording"
ANNOTATION_NAMESPACE = "event_vision"
CONSUMER_ID = "event_vision"

# Config entry keys
CONF_GEMINI_API_KEY = "gemini_api_key"
CONF_MODEL = "model"
CONF_CONCURRENCY = "concurrency"
CONF_AUTO_ANALYZE = "auto_analyze"
CONF_PROMPT_INSTRUCTIONS = "prompt_instructions"

# Default configuration values
DEFAULT_MODEL = "gemini-flash-lite-latest"
DEFAULT_CONCURRENCY = 1
DEFAULT_AUTO_ANALYZE = True
DEFAULT_PROMPT_INSTRUCTIONS = ""

# Numeric and size bounds
MIN_CONCURRENCY = 1
MAX_CONCURRENCY = 4
MAX_PROMPT_INSTRUCTIONS_LEN = 1000
MAX_SUMMARY_LEN = 1000
MAX_OBJECTS = 20
MAX_OBJECT_LEN = 50
MAX_FRAMES = 6
LEASE_PIN_SECONDS = 300.0
MAX_PROCESS_DEADLINE = 170.0  # Bounded to <=170s for 10s cleanup/CAS margin before 180s reservation TTL
MAX_ATTEMPTS = 3
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

# Service names
SERVICE_ANALYZE = "analyze"
