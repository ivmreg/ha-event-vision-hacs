"""Config flow for Event Vision."""

from __future__ import annotations

from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.helpers import selector

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
    MAX_CONCURRENCY,
    MAX_PROMPT_INSTRUCTIONS_LEN,
    MIN_CONCURRENCY,
)


class EventVisionConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Event Vision analyzer."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial setup step."""
        # Single analyzer instance enforcement
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors: dict[str, str] = {}

        if user_input is not None:
            api_key = str(user_input.get(CONF_GEMINI_API_KEY, "")).strip()
            if not api_key:
                errors[CONF_GEMINI_API_KEY] = "invalid_api_key"

            concurrency = user_input.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY)
            try:
                concurrency = int(concurrency)
                if not (MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY):
                    errors[CONF_CONCURRENCY] = "invalid_concurrency"
            except (ValueError, TypeError):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"

            instructions = str(user_input.get(CONF_PROMPT_INSTRUCTIONS, "")).strip()
            if len(instructions) > MAX_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_PROMPT_INSTRUCTIONS] = "instructions_too_long"

            if not errors:
                return self.async_create_entry(
                    title="Event Vision Analyzer",
                    data={
                        CONF_GEMINI_API_KEY: api_key,
                        CONF_MODEL: str(user_input.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL,
                        CONF_CONCURRENCY: concurrency,
                        CONF_AUTO_ANALYZE: bool(user_input.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)),
                        CONF_PROMPT_INSTRUCTIONS: instructions,
                    },
                )

        schema = vol.Schema(
            {
                vol.Required(CONF_GEMINI_API_KEY): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(CONF_MODEL, default=DEFAULT_MODEL): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Optional(CONF_CONCURRENCY, default=DEFAULT_CONCURRENCY): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_CONCURRENCY,
                        max=MAX_CONCURRENCY,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(CONF_AUTO_ANALYZE, default=DEFAULT_AUTO_ANALYZE): selector.BooleanSelector(),
                vol.Optional(CONF_PROMPT_INSTRUCTIONS, default=DEFAULT_PROMPT_INSTRUCTIONS): selector.TextSelector(
                    selector.TextSelectorConfig(multiline=True)
                ),
            }
        )

        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Return options flow handler."""
        return EventVisionOptionsFlow(config_entry)


class EventVisionOptionsFlow(config_entries.OptionsFlow):
    """Handle options flow for Event Vision."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage options."""
        errors: dict[str, str] = {}
        current_data = {**self._config_entry.data, **self._config_entry.options}

        if user_input is not None:
            api_key = str(user_input.get(CONF_GEMINI_API_KEY, "")).strip()
            # If user left API key empty in options, retain current key
            if not api_key:
                api_key = current_data.get(CONF_GEMINI_API_KEY, "")

            concurrency = user_input.get(CONF_CONCURRENCY, current_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY))
            try:
                concurrency = int(concurrency)
                if not (MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY):
                    errors[CONF_CONCURRENCY] = "invalid_concurrency"
            except (ValueError, TypeError):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"

            instructions = str(user_input.get(CONF_PROMPT_INSTRUCTIONS, "")).strip()
            if len(instructions) > MAX_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_PROMPT_INSTRUCTIONS] = "instructions_too_long"

            if not errors:
                return self.async_create_entry(
                    title="",
                    data={
                        CONF_GEMINI_API_KEY: api_key,
                        CONF_MODEL: str(user_input.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL,
                        CONF_CONCURRENCY: concurrency,
                        CONF_AUTO_ANALYZE: bool(user_input.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)),
                        CONF_PROMPT_INSTRUCTIONS: instructions,
                    },
                )

        schema = vol.Schema(
            {
                vol.Optional(CONF_GEMINI_API_KEY, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(CONF_MODEL, default=current_data.get(CONF_MODEL, DEFAULT_MODEL)): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Optional(
                    CONF_CONCURRENCY,
                    default=current_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_CONCURRENCY,
                        max=MAX_CONCURRENCY,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(
                    CONF_AUTO_ANALYZE,
                    default=current_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE),
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_PROMPT_INSTRUCTIONS,
                    default=current_data.get(CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS),
                ): selector.TextSelector(selector.TextSelectorConfig(multiline=True)),
            }
        )

        return self.async_show_form(step_id="init", data_schema=schema, errors=errors)
