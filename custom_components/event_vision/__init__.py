"""Event Vision Home Assistant Integration.

Optional generic AI analyzer for event_recording.
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
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
    CONF_ENABLE_DEBUG_LOGGING,
    CONF_ENABLE_FUSION,
    CONF_EXTRACTION_TIMEOUT,
    CONF_FRAME_CACHE_MB,
    CONF_FRAME_CACHE_TTL,
    CONF_FUSION_JOB_DEADLINE,
    CONF_FUSION_MAX_FRAMES,
    CONF_FUSION_PROMPT_INSTRUCTIONS,
    CONF_GEMINI_API_KEY,
    CONF_JOB_DEADLINE,
    CONF_MAX_ATTEMPTS,
    CONF_MAX_FRAMES,
    CONF_MODEL,
    CONF_PROMPT_INSTRUCTIONS,
    CONF_PROVIDER_TIMEOUT,
    CONF_SAMPLING_POLICY,
    DEFAULT_AUTO_ANALYZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_ENABLE_DEBUG_LOGGING,
    DEFAULT_ENABLE_FUSION,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_FRAME_CACHE_MB,
    DEFAULT_FRAME_CACHE_TTL,
    DEFAULT_FUSION_JOB_DEADLINE,
    DEFAULT_FUSION_MAX_FRAMES,
    DEFAULT_FUSION_PROMPT_INSTRUCTIONS,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_INSTRUCTIONS,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    DOMAIN,
    MAX_FUSION_PROMPT_INSTRUCTIONS_LEN,
    MAX_PROMPT_INSTRUCTIONS_LEN,
    SERVICE_ANALYZE,
    SERVICE_ANALYZE_INCIDENT,
    VALID_SAMPLING_POLICIES,
)
from .core.cache import FrameCache
from .core.coordinator import VisionCoordinator
from .core.extractor import FrameExtractor
from .core.fusion_store import FusionJobStore
from .core.gemini import GeminiClient
from .core.models import (
    SPEC_CONCURRENCY,
    SPEC_EXTRACTION_TIMEOUT,
    SPEC_FRAME_CACHE_MB,
    SPEC_FRAME_CACHE_TTL,
    SPEC_FUSION_JOB_DEADLINE,
    SPEC_FUSION_MAX_FRAMES,
    SPEC_JOB_DEADLINE,
    SPEC_MAX_ATTEMPTS,
    SPEC_MAX_FRAMES,
    SPEC_PROVIDER_TIMEOUT,
    ValidationError,
    validate_budget_bounds,
    validate_fusion_budget_bounds,
)
from .core.store import AnalysisJobStore
from .core.worker import AnalysisWorker
from .logging_util import configure_integration_logging
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
    enable_debug = bool(config_data.get(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING))
    configure_integration_logging(enable_debug)

    api_key = str(config_data.get(CONF_GEMINI_API_KEY, "")).strip()
    model = str(config_data.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL
    raw_auto_analyze = bool(config_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE))
    prompt_instructions = str(
        config_data.get(CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS)
    ).strip()
    if len(prompt_instructions) > MAX_PROMPT_INSTRUCTIONS_LEN:
        raise ConfigEntryNotReady(
            f"Prompt instructions length exceeds {MAX_PROMPT_INSTRUCTIONS_LEN} characters"
        )

    # Validate configuration parameters against declarative NumberSpec definitions
    try:
        concurrency = int(
            SPEC_CONCURRENCY.validate(config_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY))
        )
        max_frames = int(
            SPEC_MAX_FRAMES.validate(config_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))
        )
        job_deadline = float(
            SPEC_JOB_DEADLINE.validate(config_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE))
        )
        provider_timeout = float(
            SPEC_PROVIDER_TIMEOUT.validate(
                config_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT)
            )
        )
        extraction_timeout = float(
            SPEC_EXTRACTION_TIMEOUT.validate(
                config_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT)
            )
        )
        max_attempts = int(
            SPEC_MAX_ATTEMPTS.validate(config_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS))
        )
        fusion_max_frames = int(
            SPEC_FUSION_MAX_FRAMES.validate(
                config_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES)
            )
        )
        fusion_job_deadline = float(
            SPEC_FUSION_JOB_DEADLINE.validate(
                config_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE)
            )
        )
        cache_mb = int(
            SPEC_FRAME_CACHE_MB.validate(
                config_data.get(CONF_FRAME_CACHE_MB, DEFAULT_FRAME_CACHE_MB)
            )
        )
        cache_ttl = float(
            SPEC_FRAME_CACHE_TTL.validate(
                config_data.get(CONF_FRAME_CACHE_TTL, DEFAULT_FRAME_CACHE_TTL)
            )
        )
    except (ValidationError, ValueError, TypeError) as err:
        raise ConfigEntryNotReady(f"Invalid configuration parameter: {err}") from err

    # Validate sampling policy
    sampling_policy = str(config_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY)).strip()
    if sampling_policy not in VALID_SAMPLING_POLICIES:
        raise ConfigEntryNotReady(f"Invalid sampling policy '{sampling_policy}'")

    # Validate coupled budget bounds
    valid_budget, err_msg = validate_budget_bounds(
        job_deadline=job_deadline,
        provider_timeout=provider_timeout,
        extraction_timeout=extraction_timeout,
        max_frames=max_frames,
        max_attempts=max_attempts,
    )
    if not valid_budget:
        raise ConfigEntryNotReady(f"Invalid execution budget bounds: {err_msg}")

    # If API key is missing, do not autoqueue/start provider jobs and mark consumer unavailable
    has_valid_key = bool(api_key and api_key != "placeholder")
    if not has_valid_key:
        raise ConfigEntryNotReady("Configure an API key for the optional Event Vision analyzer")
    auto_analyze = raw_auto_analyze if has_valid_key else False

    enable_fusion = bool(config_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION))
    fusion_instructions = str(
        config_data.get(CONF_FUSION_PROMPT_INSTRUCTIONS, DEFAULT_FUSION_PROMPT_INSTRUCTIONS)
    ).strip()
    if len(fusion_instructions) > MAX_FUSION_PROMPT_INSTRUCTIONS_LEN:
        raise ConfigEntryNotReady(
            f"Fusion instructions length exceeds {MAX_FUSION_PROMPT_INSTRUCTIONS_LEN} characters"
        )

    valid_fusion_budget, fusion_err_msg = validate_fusion_budget_bounds(
        fusion_job_deadline=fusion_job_deadline,
        fusion_max_frames=fusion_max_frames,
        provider_timeout=provider_timeout,
        extraction_timeout=extraction_timeout,
    )
    if not valid_fusion_budget:
        raise ConfigEntryNotReady(f"Invalid fusion execution budget bounds: {fusion_err_msg}")

    # Dedicated SQLite persistent job stores
    db_path = Path(hass.config.path(".storage", "event_vision", entry.entry_id, "analysis.db"))
    job_store = AnalysisJobStore(db_path, max_attempts=max_attempts)

    fusion_db_path = Path(
        hass.config.path(".storage", "event_vision", entry.entry_id, "fusion_jobs.sqlite")
    )
    fusion_job_store = (
        FusionJobStore(fusion_db_path, max_attempts=max_attempts) if enable_fusion else None
    )

    frame_cache = FrameCache(max_bytes=cache_mb * 1024 * 1024, ttl_seconds=cache_ttl)

    gemini_client = GeminiClient(api_key=api_key if has_valid_key else "", model=model)
    frame_extractor = FrameExtractor(
        ffmpeg_bin=getattr(rec_api, "ffmpeg_binary", None), cache=frame_cache
    )

    analysis_options = dict(
        model=model,
        prompt_instructions=prompt_instructions or None,
        max_frames=max_frames,
        sampling_policy=sampling_policy,
        job_deadline=job_deadline,
        provider_timeout=provider_timeout,
        extraction_timeout=extraction_timeout,
        max_attempts=max_attempts,
        fusion_job_store=fusion_job_store,
        enable_fusion=enable_fusion,
        fusion_max_frames=fusion_max_frames,
        fusion_prompt_instructions=fusion_instructions or None,
        fusion_job_deadline=fusion_job_deadline,
    )
    worker = AnalysisWorker(
        job_store=job_store,
        gemini_client=gemini_client,
        frame_extractor=frame_extractor,
        recording_api=rec_api,
        worker_id=f"worker-{entry.entry_id[:8]}",
        concurrency=concurrency,
        cache=frame_cache,
        **analysis_options,
    )
    coordinator = VisionCoordinator(
        job_store=job_store,
        worker=worker,
        recording_api=rec_api,
        auto_analyze=auto_analyze,
        **analysis_options,
    )

    # Register cleanup before the first resource is acquired. AsyncExitStack closes
    # every owner even if a later cleanup fails, and aclose is idempotent.
    cleanup = AsyncExitStack()
    cleanup.push_async_callback(job_store.close)
    if fusion_job_store:
        cleanup.push_async_callback(fusion_job_store.close)
    cleanup.push_async_callback(frame_cache.close)
    cleanup.push_async_callback(gemini_client.close)
    cleanup.push_async_callback(coordinator.async_shutdown)
    unsub_periodic = None
    last_compaction = 0.0
    try:
        await job_store.open()
        if fusion_job_store:
            await fusion_job_store.open()
        # Bind recording API facade and setup subscriptions cleanly through coordinator
        coordinator.bind_recording_api(rec_api, available=has_valid_key)

        # Also rebind in manual-only mode after the recording integration reloads.
        async def on_periodic_catchup(_now: Any) -> None:
            nonlocal last_compaction
            current = _get_recording_api(hass)
            if (
                current is None
                or current is not coordinator.rec_api
                or coordinator._unsub_consumer is None
            ):
                await worker.stop()
                coordinator.unbind_recording_api()
                if current is None:
                    return
                coordinator.bind_recording_api(current, available=True)
                await job_store.recover_expired_leases(force_all=True)
                if fusion_job_store:
                    await fusion_job_store.recover_expired_leases(force_all=True)
                worker.start()
            if auto_analyze:
                await coordinator.async_scan_unprocessed_media()
            now = asyncio.get_running_loop().time()
            if now - last_compaction >= 3600:
                await job_store.compact()
                if fusion_job_store:
                    await fusion_job_store.compact()
                last_compaction = now

        unsub_periodic = async_track_time_interval(hass, on_periodic_catchup, timedelta(seconds=30))

        # Start coordinator (worker loop and initial startup catchup only if auto_analyze/valid key)
        if has_valid_key:
            await coordinator.async_startup()

        hass.data.setdefault(DOMAIN, {})
        hass.data[DOMAIN].setdefault("entries", {})
        hass.data[DOMAIN]["entries"][entry.entry_id] = {
            "job_store": job_store,
            "fusion_job_store": fusion_job_store,
            "gemini_client": gemini_client,
            "worker": worker,
            "coordinator": coordinator,
            "frame_cache": frame_cache,
            "unsub_periodic": unsub_periodic,
            "cleanup": cleanup,
        }

        async def on_stop(_event: Any) -> None:
            if unsub_periodic:
                unsub_periodic()
            await cleanup.aclose()

        entry.async_on_unload(hass.bus.async_listen_once(EVENT_HOMEASSISTANT_STOP, on_stop))

        # Register services
        async_register_services(hass)

        # Register update listener for configuration reloads
        entry.async_on_unload(entry.add_update_listener(async_reload_entry))

        # Set up platforms (Sensor)
        await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    except BaseException:
        # Cleanup on partial setup failure
        if unsub_periodic:
            unsub_periodic()
        await cleanup.aclose()
        hass.data.get(DOMAIN, {}).get("entries", {}).pop(entry.entry_id, None)
        raise

    return True


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Handle options or config entry update."""
    config_data = {**entry.data, **entry.options}
    enable_debug = bool(config_data.get(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING))
    configure_integration_logging(enable_debug)
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

        await entry_data["cleanup"].aclose()

    entries = hass.data.get(DOMAIN, {}).get("entries", {})
    if not entries:
        hass.services.async_remove(DOMAIN, SERVICE_ANALYZE)
        hass.services.async_remove(DOMAIN, SERVICE_ANALYZE_INCIDENT)
        hass.data.pop(DOMAIN, None)

    return True
