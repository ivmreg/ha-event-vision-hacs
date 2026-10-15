"""Event Vision Home Assistant Integration.

Optional generic AI analyzer for event_recording.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path
from typing import Any

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import EVENT_HOMEASSISTANT_STOP, Platform
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_time_interval

from .const import (
    CONF_AUTO_ANALYZE,
    CONF_CONCURRENCY,
    CONF_GEMINI_API_KEY,
    CONF_MODEL,
    CONF_PROMPT_INSTRUCTIONS,
    DEFAULT_AUTO_ANALYZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_INSTRUCTIONS,
    DOMAIN,
    SERVICE_ANALYZE,
)
from .core.extractor import FrameExtractor
from .core.gemini import GeminiClient
from .core.store import AnalysisJobStore
from .core.worker import AnalysisWorker, VisionCoordinator
from .services import async_register_services

_LOGGER = logging.getLogger(__name__)

PLATFORMS: list[Platform] = [Platform.SENSOR]


def _get_recording_api(hass: HomeAssistant) -> Any | None:
    """Retrieve RecordingAPI facade from event_recording integration."""
    try:
        from custom_components.event_recording.api import get_api

        return get_api(hass)
    except ImportError:
        return None


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Event Vision from a config entry."""
    rec_api = _get_recording_api(hass)
    if rec_api is None:
        raise ConfigEntryNotReady("event_recording integration or API facade is not ready")

    config_data = {**entry.data, **entry.options}
    api_key = str(config_data.get(CONF_GEMINI_API_KEY, "")).strip()
    model = str(config_data.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL
    concurrency = int(config_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY))
    raw_auto_analyze = bool(config_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE))
    prompt_instructions = str(config_data.get(CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS)).strip()

    # If API key is missing, do not autoqueue/start provider jobs and mark consumer unavailable
    has_valid_key = bool(api_key and api_key != "placeholder")
    if not has_valid_key:
        raise ConfigEntryNotReady("Configure an API key for the optional Event Vision analyzer")
    auto_analyze = raw_auto_analyze if has_valid_key else False

    # Dedicated SQLite persistent job store
    db_path = Path(hass.config.path(".storage", "event_vision", entry.entry_id, "analysis.db"))
    job_store = AnalysisJobStore(db_path)
    await job_store.open()

    gemini_client = GeminiClient(api_key=api_key if has_valid_key else "", model=model)
    frame_extractor = FrameExtractor(ffmpeg_bin=getattr(rec_api, "ffmpeg_binary", None))

    worker = AnalysisWorker(
        job_store=job_store,
        gemini_client=gemini_client,
        frame_extractor=frame_extractor,
        recording_api=rec_api,
        worker_id=f"worker-{entry.entry_id[:8]}",
        concurrency=concurrency,
        prompt_instructions=prompt_instructions if prompt_instructions else None,
    )

    coordinator = VisionCoordinator(
        job_store=job_store,
        worker=worker,
        recording_api=rec_api,
        auto_analyze=auto_analyze,
        model=model,
        prompt_instructions=prompt_instructions if prompt_instructions else None,
    )

    unsub_periodic = None
    try:
        # Bind recording API facade and setup subscriptions cleanly through coordinator
        coordinator.bind_recording_api(rec_api, available=has_valid_key)

        # Also rebind in manual-only mode after the recording integration reloads.
        async def on_periodic_catchup(_now: Any) -> None:
            current = _get_recording_api(hass)
            if current is None or current is not coordinator.rec_api or coordinator._unsub_consumer is None:
                await worker.stop()
                coordinator.unbind_recording_api()
                if current is None:
                    return
                coordinator.bind_recording_api(current, available=True)
                await job_store.recover_expired_leases(force_all=True)
                worker.start()
            if auto_analyze:
                await coordinator.async_scan_unprocessed_media()

        unsub_periodic = async_track_time_interval(hass, on_periodic_catchup, timedelta(seconds=30))

        # Start coordinator (worker loop and initial startup catchup only if auto_analyze/valid key)
        if has_valid_key:
            await coordinator.async_startup()

        hass.data.setdefault(DOMAIN, {})
        hass.data[DOMAIN].setdefault("entries", {})
        hass.data[DOMAIN]["entries"][entry.entry_id] = {
            "job_store": job_store,
            "gemini_client": gemini_client,
            "worker": worker,
            "coordinator": coordinator,
            "unsub_periodic": unsub_periodic,
        }

        async def on_stop(_event: Any) -> None:
            if unsub_periodic:
                unsub_periodic()
            await coordinator.async_shutdown()
            await gemini_client.close()

        entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, on_stop))

        # Register services
        async_register_services(hass)

        # Register update listener for configuration reloads
        entry.async_on_unload(entry.add_update_listener(async_reload_entry))

        # Set up platforms (Sensor)
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    except Exception:
        # Cleanup on partial setup failure
        if unsub_periodic:
            unsub_periodic()
        await coordinator.async_shutdown()
        await gemini_client.close()
        await job_store.close()
        raise

    return True


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options or config entry update."""
    await hass.config_entries.async_reload(entry.entry_id)


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload an Event Vision config entry cleanly."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if not unload_ok:
        return False

    entry_data = hass.data.get(DOMAIN, {}).get("entries", {}).pop(entry.entry_id, None)
    if entry_data:
        unsub_periodic = entry_data.get("unsub_periodic")
        if unsub_periodic:
            unsub_periodic()

        coordinator: VisionCoordinator = entry_data.get("coordinator")
        if coordinator:
            await coordinator.async_shutdown()

        gemini_client: GeminiClient = entry_data.get("gemini_client")
        if gemini_client:
            await gemini_client.close()

    entries = hass.data.get(DOMAIN, {}).get("entries", {})
    if not entries:
        hass.services.async_remove(DOMAIN, SERVICE_ANALYZE)
        hass.data.pop(DOMAIN, None)

    return True
