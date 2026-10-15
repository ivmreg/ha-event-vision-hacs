"""Diagnostics support for Event Vision.

Strict privacy:
- Redacts Gemini API keys and secrets.
- Exposes only generic queue metrics.
- Excludes camera entity names, event IDs, source IDs, and local filesystem paths.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import CONF_GEMINI_API_KEY, DOMAIN
from .core.store import AnalysisJobStore

TO_REDACT = {
    CONF_GEMINI_API_KEY,
    "api_key",
    "password",
    "secret",
    "token",
}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: ConfigEntry
) -> dict[str, Any]:
    """Return sanitized diagnostics for an Event Vision entry."""
    entry_info = hass.data.get(DOMAIN, {}).get("entries", {}).get(entry.entry_id, {})
    job_store: AnalysisJobStore | None = entry_info.get("job_store")

    queue_stats: dict[str, Any] = {}
    if job_store and job_store.is_open:
        queue_stats = await job_store.get_stats()

    redacted_data = async_redact_data(dict(entry.data), TO_REDACT)
    redacted_options = async_redact_data(dict(entry.options), TO_REDACT)

    return {
        "entry_data": redacted_data,
        "entry_options": redacted_options,
        "queue_stats": queue_stats,
    }
