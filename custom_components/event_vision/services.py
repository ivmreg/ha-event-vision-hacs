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
)
from homeassistant.exceptions import HomeAssistantError, Unauthorized
from homeassistant.helpers import config_validation as cv

from .const import DOMAIN, SERVICE_ANALYZE, SERVICE_ANALYZE_INCIDENT
from .core.models import is_user_authorized_for_event

_LOGGER = logging.getLogger(__name__)

SCHEMA_ANALYZE = vol.Schema(
    {
        vol.Required("event_id"): cv.string,
    }
)

SCHEMA_ANALYZE_INCIDENT = vol.Schema(
    {
        vol.Required("incident_id"): cv.string,
    }
)


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

        _LOGGER.debug(
            "Service %s.%s called: event_id=%s, user_id=%s",
            DOMAIN,
            SERVICE_ANALYZE,
            event_id,
            call.context.user_id,
        )

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

    async def handle_analyze_incident(call: ServiceCall) -> ServiceResponse:
        """Handle manual analyze or retry service action for an incident with member permission verification."""
        incident_id = call.data["incident_id"].strip()
        if not incident_id:
            raise HomeAssistantError("incident_id cannot be empty")

        _LOGGER.debug(
            "Service %s.%s called: incident_id=%s, user_id=%s",
            DOMAIN,
            SERVICE_ANALYZE_INCIDENT,
            incident_id,
            call.context.user_id,
        )

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

        # 1. Verify user permissions if invoked with user context
        if call.context.user_id:
            user = await hass.auth.async_get_user(call.context.user_id)
            if not user or not getattr(user, "is_active", True):
                raise Unauthorized(context=call.context)

            # Non-admin users must be authorized for ALL member events
            if not getattr(user, "is_admin", False):
                rec_api = getattr(coordinator, "rec_api", None)
                if not rec_api:
                    raise Unauthorized(context=call.context)

                inputs = await rec_api.get_incident_inputs(incident_id, "event_vision")
                if not inputs or not inputs.events:
                    raise Unauthorized(context=call.context)

                for ev in inputs.events:
                    if not is_user_authorized_for_event(user, ev):
                        raise Unauthorized(context=call.context)

        success = await coordinator.async_manual_analyze_incident(incident_id)
        if not success:
            _LOGGER.warning(
                "Manual analysis rejected for incident %s (incident not found, fusion disabled, or missing)",
                incident_id,
            )

        response: dict[str, Any] = {
            "incident_id": incident_id,
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
    hass.services.async_register(
        DOMAIN,
        SERVICE_ANALYZE_INCIDENT,
        handle_analyze_incident,
        schema=SCHEMA_ANALYZE_INCIDENT,
        supports_response=SupportsResponse.OPTIONAL,
    )
