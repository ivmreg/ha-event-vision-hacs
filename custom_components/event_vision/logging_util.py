"""Diagnostic logging configuration utility for event_vision.

Pure Python stdlib, zero Home Assistant imports.
"""

from __future__ import annotations

import logging

_LOGGER = logging.getLogger(__name__)


def configure_integration_logging(enabled: bool) -> None:
    """Configure dynamic integration logging level for event_vision."""
    target_logger = logging.getLogger("custom_components.event_vision")
    if enabled:
        target_logger.setLevel(logging.DEBUG)
        _LOGGER.debug("Diagnostic DEBUG logging enabled for Event Vision")
    else:
        target_logger.setLevel(logging.NOTSET)
