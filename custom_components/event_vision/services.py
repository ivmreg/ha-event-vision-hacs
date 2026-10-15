"""Home Assistant service actions for Event Vision."""

from __future__ import annotations

import logging
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol
from homeassistant.core import (
    HomeAssistant,
    ServiceCall,
    ServiceResponse,
    SupportsResponse,
    callback,
)
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN, SERVICE_ANALYZE

_LOGGER = logging.getLogger(__name__)

SCHEMA_ANALYZE = vol.Schema(
    {
        vol.Required("event_id"): cv.string,
    }
)


@callback
def async_register_services(hass: HomeAssistant) -> None:
    """Register Event Vision services."""

    async def handle_analyze(call: ServiceCall) -> ServiceResponse:
        """Handle manual analyze or retry service action with admin verification."""
        # 1. Verify admin privilege if invoked with user context
        if call.context.user_id:
            user = await hass.auth.async_get_user(call.context.user_id)
            if not user or not user.is_admin:
                raise Unauthorized(context=call.context)

        event_id = call.data["event_id"].strip()
        if not event_id:
            raise HomeAssistantError("event_id cannot be empty")

        domain_data = hass.data.get(DOMAIN, {})
        entries = domain_data.get("entries", {})

        coordinator = None
        for entry_info in entries.values():
            coord = entry_info.get("coordinator")
            if coord:
                coordinator = coord
                break

        if not coordinator:
            raise HomeAssistantError("No active Event Vision analyzer instance found")

        success = await coordinator.async_manual_analyze(event_id)
        if not success:
            _LOGGER.warning("Manual analysis rejected for event %s (event not found or missing)", event_id)

        response: dict[str, Any] = {
            "event_id": event_id,
            "success": success,
            "status": "enqueued" if success else "rejected",
        }
        return response

    hass.services.async_register(
        DOMAIN,
        SERVICE_ANALYZE,
        handle_analyze,
        schema=SCHEMA_ANALYZE,
        supports_response=SupportsResponse.OPTIONAL,
    )
