"""Optional generic queue statistics sensor for Event Vision.

Strict boundary: NO camera entity attributes, NO raw event payloads, NO private context.
Exposes only aggregated job counts to prevent database bloat.
"""

from __future__ import annotations

from typing import Any

from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .core.store import AnalysisJobStore


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up Event Vision queue sensor from a config entry."""
    entry_info = hass.data[DOMAIN]["entries"][entry.entry_id]
    job_store: AnalysisJobStore = entry_info["job_store"]

    device_info = DeviceInfo(
        identifiers={(DOMAIN, entry.entry_id)},
        name="Event Vision Analyzer",
        manufacturer="Event Vision",
        model="Vision Engine",
    )

    sensor = EventVisionQueueSensor(job_store, entry.entry_id, device_info)
    async_add_entities([sensor], update_before_add=True)


class EventVisionQueueSensor(SensorEntity):
    """Sensor reporting pending analysis queue count and aggregated status metrics."""

    _attr_has_entity_name = True
    _attr_name = "Queue Pending"
    _attr_icon = "mdi:eye-check-outline"

    def __init__(
        self,
        job_store: AnalysisJobStore,
        entry_id: str,
        device_info: DeviceInfo,
    ) -> None:
        self._job_store = job_store
        self._attr_unique_id = f"{entry_id}_queue_pending"
        self._attr_device_info = device_info
        self._attr_native_value: int | None = 0
        self._attr_extra_state_attributes: dict[str, Any] = {}

    async def async_update(self) -> None:
        """Fetch latest queue counts from AnalysisJobStore."""
        if not self._job_store or not self._job_store.is_open:
            return

        stats = await self._job_store.get_stats()
        self._attr_native_value = stats.get("pending", 0)
        self._attr_extra_state_attributes = {
            "pending": stats.get("pending", 0),
            "leased": stats.get("leased", 0),
            "complete": stats.get("complete", 0),
            "failed": stats.get("failed", 0),
            "skipped": stats.get("skipped", 0),
        }
