"""Diagnostics support for Event Vision.

Strict privacy:
- Redacts Gemini API keys and secrets.
- Exposes only generic queue metrics, read-only prompt compositions, and coupled budget info.
- Excludes camera entity names, event IDs, source IDs, and local filesystem paths.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant

from .const import (
    CONF_ENABLE_FUSION,
    CONF_EXTRACTION_TIMEOUT,
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
    DEFAULT_ENABLE_FUSION,
    DEFAULT_EXTRACTION_TIMEOUT,
    DEFAULT_FUSION_JOB_DEADLINE,
    DEFAULT_FUSION_MAX_FRAMES,
    DEFAULT_FUSION_PROMPT,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_MODEL,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    DOMAIN,
    GENERIC_MEDIA_PROMPT,
    MAX_PROMPT_INSTRUCTIONS_LEN,
)
from .core.fusion_store import FusionJobStore
from .core.models import (
    derive_lease_pin_seconds,
    derive_reservation_ttl,
    is_rolling_model_alias,
)
from .core.store import AnalysisJobStore
from .core.worker import VisionCoordinator

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
    fusion_job_store: FusionJobStore | None = entry_info.get("fusion_job_store")
    coordinator: VisionCoordinator | None = entry_info.get("coordinator")

    queue_stats: dict[str, Any] = {}
    if job_store and job_store.is_open:
        queue_stats = await job_store.get_stats()

    fusion_queue_stats: dict[str, Any] = {}
    if fusion_job_store and fusion_job_store.is_open:
        fusion_queue_stats = await fusion_job_store.get_job_counts()

    pre_payload_backup_exists = False
    if job_store and getattr(job_store, "_db_path", None):
        backup_path = job_store._db_path.with_name(f"{job_store._db_path.name}.pre_payload_backup")
        pre_payload_backup_exists = await hass.async_add_executor_job(backup_path.exists)

    current_data = {**entry.data, **entry.options}
    model = str(current_data.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL
    instructions = str(current_data.get(CONF_PROMPT_INSTRUCTIONS, "")).strip()[:MAX_PROMPT_INSTRUCTIONS_LEN]

    max_frames = int(current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))
    sampling_policy = str(current_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY)).strip()
    job_deadline = float(current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE))
    provider_timeout = float(current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT))
    extraction_timeout = float(current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT))
    max_attempts = int(current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS))

    enable_fusion = bool(current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION))
    fusion_max_frames = int(current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES))
    fusion_instructions = str(current_data.get(CONF_FUSION_PROMPT_INSTRUCTIONS, "")).strip()[:MAX_PROMPT_INSTRUCTIONS_LEN]
    fusion_deadline = float(current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE))

    effective_prompt = GENERIC_MEDIA_PROMPT
    if instructions:
        effective_prompt += f"\nAdditional Instructions: {instructions}"

    effective_fusion_prompt = DEFAULT_FUSION_PROMPT
    if fusion_instructions:
        effective_fusion_prompt += f"\nAdditional Instructions: {fusion_instructions}"

    redacted_data = async_redact_data(dict(entry.data), TO_REDACT)
    redacted_options = async_redact_data(dict(entry.options), TO_REDACT)

    profile_digest = coordinator.profile_digest if coordinator else None
    fusion_profile_digest = coordinator.fusion_profile_digest if coordinator else None

    return {
        "entry_data": redacted_data,
        "entry_options": redacted_options,
        "pre_payload_backup_exists": pre_payload_backup_exists,
        "queue_stats": queue_stats,
        "fusion_queue_stats": fusion_queue_stats,
        "profile_digest": profile_digest,
        "fusion_profile_digest": fusion_profile_digest,
        "provider_info": {
            "supported_provider": "Google Gemini REST (generativelanguage.googleapis.com)",
            "model": model,
            "is_rolling_alias": is_rolling_model_alias(model),
        },
        "prompt_composition": {
            "base_prompt": GENERIC_MEDIA_PROMPT,
            "additional_instructions": instructions,
            "effective_prompt": effective_prompt,
        },
        "coupled_budgets": {
            "max_frames": max_frames,
            "sampling_policy": sampling_policy,
            "job_deadline_seconds": job_deadline,
            "provider_timeout_seconds": provider_timeout,
            "extraction_timeout_seconds": extraction_timeout,
            "max_attempts": max_attempts,
            "derived_reservation_ttl_seconds": derive_reservation_ttl(job_deadline),
            "derived_lease_pin_seconds": derive_lease_pin_seconds(job_deadline),
        },
        "fusion_info": {
            "enabled": enable_fusion,
            "max_frames": fusion_max_frames,
            "job_deadline_seconds": fusion_deadline,
            "derived_reservation_ttl_seconds": derive_reservation_ttl(fusion_deadline),
            "derived_lease_pin_seconds": derive_lease_pin_seconds(fusion_deadline),
            "prompt_composition": {
                "base_prompt": DEFAULT_FUSION_PROMPT,
                "additional_instructions": fusion_instructions,
                "effective_prompt": effective_fusion_prompt,
            },
        },
    }
