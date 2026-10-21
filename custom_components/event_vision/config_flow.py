"""Config flow for Event Vision."""

from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

try:
    import probatio as vol
except ImportError:
    import voluptuous as vol
from homeassistant import config_entries, data_entry_flow
from homeassistant.core import callback
from homeassistant.helpers import selector

from .const import (
    CAS_CLEANUP_MARGIN_SECONDS,
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
    SAMPLING_POLICY_SPARSE_ANCHORS,
    SAMPLING_POLICY_TRIGGER_RELEVANT,
    VALID_SAMPLING_POLICIES,
)
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
from .validation import number_spec_selector

# Native Home Assistant form section ids. Sections are presentation only; all
# values are stored as flat option keys, never nested under these ids.
SECTION_FUSION = "fusion"
SECTION_ADVANCED = "advanced"

_VISION_SECTION_IDS = frozenset({SECTION_FUSION, SECTION_ADVANCED})

# Map submitted fields to the section that renders them so a validation error
# can re-open the section holding the invalid field.
_VISION_FIELD_SECTIONS = {
    CONF_ENABLE_FUSION: SECTION_FUSION,
    CONF_FUSION_MAX_FRAMES: SECTION_FUSION,
    CONF_FUSION_PROMPT_INSTRUCTIONS: SECTION_FUSION,
    CONF_FUSION_JOB_DEADLINE: SECTION_FUSION,
    CONF_JOB_DEADLINE: SECTION_ADVANCED,
    CONF_PROVIDER_TIMEOUT: SECTION_ADVANCED,
    CONF_EXTRACTION_TIMEOUT: SECTION_ADVANCED,
    CONF_MAX_ATTEMPTS: SECTION_ADVANCED,
    CONF_FRAME_CACHE_MB: SECTION_ADVANCED,
    CONF_FRAME_CACHE_TTL: SECTION_ADVANCED,
    CONF_ENABLE_DEBUG_LOGGING: SECTION_ADVANCED,
}


def _flatten_sections(
    user_input: Mapping[str, Any], section_ids: frozenset[str]
) -> dict[str, Any]:
    """Flatten one level of native form sections into plain option keys."""
    flat: dict[str, Any] = {}
    for key, value in user_input.items():
        if key in section_ids and isinstance(value, Mapping):
            flat.update(value)
        else:
            flat[key] = value
    return flat


def _form_number(value: Any, default: Any) -> Any:
    """Return a form default without discarding an attempted invalid value."""
    if value is None or value == "" or isinstance(value, bool):
        return default
    if isinstance(value, float) and not math.isfinite(value):
        return default
    return value


def _sections_with_errors(
    errors: Mapping[str, str], field_sections: Mapping[str, str]
) -> set[str]:
    """Return the sections that should start expanded for the given errors."""
    return {field_sections[field] for field in errors if field in field_sections}


class EventVisionConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Event Vision analyzer."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Handle the initial concise setup step."""
        # Single analyzer instance enforcement
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors: dict[str, str] = {}

        if user_input is not None:
            api_key = str(user_input.get(CONF_GEMINI_API_KEY, "")).strip()
            if not api_key:
                errors[CONF_GEMINI_API_KEY] = "invalid_api_key"

            try:
                concurrency = int(
                    SPEC_CONCURRENCY.validate(
                        user_input.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY)
                    )
                )
            except (ValidationError, ValueError, TypeError):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"
                concurrency = DEFAULT_CONCURRENCY

            model = str(user_input.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL

            if not errors:
                return self.async_create_entry(
                    title="Event Vision Analyzer",
                    data={
                        CONF_GEMINI_API_KEY: api_key,
                        CONF_MODEL: model,
                        CONF_CONCURRENCY: concurrency,
                        CONF_AUTO_ANALYZE: bool(
                            user_input.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)
                        ),
                        # Sensible default values for sampling and coupled advanced parameters
                        CONF_PROMPT_INSTRUCTIONS: DEFAULT_PROMPT_INSTRUCTIONS,
                        CONF_MAX_FRAMES: DEFAULT_MAX_FRAMES,
                        CONF_SAMPLING_POLICY: DEFAULT_SAMPLING_POLICY,
                        CONF_JOB_DEADLINE: DEFAULT_JOB_DEADLINE,
                        CONF_PROVIDER_TIMEOUT: DEFAULT_PROVIDER_TIMEOUT,
                        CONF_EXTRACTION_TIMEOUT: DEFAULT_EXTRACTION_TIMEOUT,
                        CONF_MAX_ATTEMPTS: DEFAULT_MAX_ATTEMPTS,
                        # Incident fusion defaults
                        CONF_ENABLE_FUSION: DEFAULT_ENABLE_FUSION,
                        CONF_FUSION_MAX_FRAMES: DEFAULT_FUSION_MAX_FRAMES,
                        CONF_FUSION_PROMPT_INSTRUCTIONS: DEFAULT_FUSION_PROMPT_INSTRUCTIONS,
                        CONF_FUSION_JOB_DEADLINE: DEFAULT_FUSION_JOB_DEADLINE,
                        # Diagnostic logging
                        CONF_ENABLE_DEBUG_LOGGING: DEFAULT_ENABLE_DEBUG_LOGGING,
                    },
                )

        # Re-render the form with attempted values; the API key is never echoed.
        displayed = dict(user_input) if user_input is not None else {}
        schema = vol.Schema(
            {
                vol.Required(CONF_GEMINI_API_KEY): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(
                    CONF_MODEL, default=str(displayed.get(CONF_MODEL, DEFAULT_MODEL))
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Optional(
                    CONF_CONCURRENCY,
                    default=_form_number(
                        displayed.get(CONF_CONCURRENCY), DEFAULT_CONCURRENCY
                    ),
                ): number_spec_selector(SPEC_CONCURRENCY),
                vol.Optional(
                    CONF_AUTO_ANALYZE,
                    default=bool(
                        displayed.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)
                    ),
                ): selector.BooleanSelector(),
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
    """Handle the single-form options flow for Event Vision."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    def _get_current_data(self) -> dict[str, Any]:
        """Return combined configuration and options data."""
        return {**self._config_entry.data, **self._config_entry.options}

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Show and atomically save every option in one form."""
        current_data = self._get_current_data()
        errors: dict[str, str] = {}
        submitted: dict[str, Any] = {}

        if user_input is not None:
            submitted = _flatten_sections(user_input, _VISION_SECTION_IDS)
            values: dict[str, Any] = {}

            new_api_key = str(submitted.get(CONF_GEMINI_API_KEY, "")).strip()
            values[CONF_GEMINI_API_KEY] = new_api_key or str(
                current_data.get(CONF_GEMINI_API_KEY, "")
            )

            model = str(
                submitted.get(CONF_MODEL, current_data.get(CONF_MODEL, DEFAULT_MODEL))
            ).strip()
            if not model:
                errors[CONF_MODEL] = "invalid_model"
            values[CONF_MODEL] = model

            try:
                values[CONF_CONCURRENCY] = int(
                    SPEC_CONCURRENCY.validate(
                        submitted.get(
                            CONF_CONCURRENCY,
                            current_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY),
                        )
                    )
                )
            except (ValidationError, ValueError, TypeError):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"

            values[CONF_AUTO_ANALYZE] = bool(
                submitted.get(
                    CONF_AUTO_ANALYZE,
                    current_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE),
                )
            )

            try:
                max_frames = int(
                    SPEC_MAX_FRAMES.validate(
                        submitted.get(
                            CONF_MAX_FRAMES,
                            current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES),
                        )
                    )
                )
                values[CONF_MAX_FRAMES] = max_frames
            except (ValidationError, ValueError, TypeError):
                errors[CONF_MAX_FRAMES] = "invalid_max_frames"
                max_frames = int(current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))

            policy = str(
                submitted.get(
                    CONF_SAMPLING_POLICY,
                    current_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY),
                )
            ).strip()
            if policy not in VALID_SAMPLING_POLICIES:
                errors[CONF_SAMPLING_POLICY] = "invalid_sampling_policy"
            values[CONF_SAMPLING_POLICY] = policy

            instructions = str(
                submitted.get(
                    CONF_PROMPT_INSTRUCTIONS,
                    current_data.get(CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS),
                )
            ).strip()
            if len(instructions) > MAX_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_PROMPT_INSTRUCTIONS] = "instructions_too_long"
            values[CONF_PROMPT_INSTRUCTIONS] = instructions

            enable_fusion = bool(
                submitted.get(
                    CONF_ENABLE_FUSION,
                    current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION),
                )
            )
            values[CONF_ENABLE_FUSION] = enable_fusion

            try:
                fusion_max_frames = int(
                    SPEC_FUSION_MAX_FRAMES.validate(
                        submitted.get(
                            CONF_FUSION_MAX_FRAMES,
                            current_data.get(
                                CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES
                            ),
                        )
                    )
                )
                values[CONF_FUSION_MAX_FRAMES] = fusion_max_frames
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FUSION_MAX_FRAMES] = "invalid_fusion_max_frames"

            fusion_instructions = str(
                submitted.get(
                    CONF_FUSION_PROMPT_INSTRUCTIONS,
                    current_data.get(
                        CONF_FUSION_PROMPT_INSTRUCTIONS,
                        DEFAULT_FUSION_PROMPT_INSTRUCTIONS,
                    ),
                )
            ).strip()
            if len(fusion_instructions) > MAX_FUSION_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_FUSION_PROMPT_INSTRUCTIONS] = "instructions_too_long"
            values[CONF_FUSION_PROMPT_INSTRUCTIONS] = fusion_instructions

            try:
                fusion_deadline = float(
                    SPEC_FUSION_JOB_DEADLINE.validate(
                        submitted.get(
                            CONF_FUSION_JOB_DEADLINE,
                            current_data.get(
                                CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE
                            ),
                        )
                    )
                )
                values[CONF_FUSION_JOB_DEADLINE] = fusion_deadline
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FUSION_JOB_DEADLINE] = "invalid_fusion_deadline"

            for field, spec, err_key in (
                (CONF_JOB_DEADLINE, SPEC_JOB_DEADLINE, "invalid_job_deadline"),
                (CONF_PROVIDER_TIMEOUT, SPEC_PROVIDER_TIMEOUT, "invalid_provider_timeout"),
                (
                    CONF_EXTRACTION_TIMEOUT,
                    SPEC_EXTRACTION_TIMEOUT,
                    "invalid_extraction_timeout",
                ),
            ):
                try:
                    values[field] = float(
                        spec.validate(
                            submitted.get(
                                field,
                                current_data.get(field, spec.default),
                            )
                        )
                    )
                except (ValidationError, ValueError, TypeError):
                    errors[field] = err_key

            for field, spec, err_key in (
                (CONF_MAX_ATTEMPTS, SPEC_MAX_ATTEMPTS, "invalid_max_attempts"),
                (CONF_FRAME_CACHE_MB, SPEC_FRAME_CACHE_MB, "invalid_frame_cache_mb"),
            ):
                try:
                    values[field] = int(
                        spec.validate(
                            submitted.get(
                                field,
                                current_data.get(field, spec.default),
                            )
                        )
                    )
                except (ValidationError, ValueError, TypeError):
                    errors[field] = err_key

            try:
                values[CONF_FRAME_CACHE_TTL] = float(
                    SPEC_FRAME_CACHE_TTL.validate(
                        submitted.get(
                            CONF_FRAME_CACHE_TTL,
                            current_data.get(
                                CONF_FRAME_CACHE_TTL, DEFAULT_FRAME_CACHE_TTL
                            ),
                        )
                    )
                )
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FRAME_CACHE_TTL] = "invalid_frame_cache_ttl"

            values[CONF_ENABLE_DEBUG_LOGGING] = bool(
                submitted.get(
                    CONF_ENABLE_DEBUG_LOGGING,
                    current_data.get(
                        CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING
                    ),
                )
            )

            # Coupled budgets use the newly submitted values, not the stored ones.
            if not errors:
                valid_budget, _ = validate_budget_bounds(
                    job_deadline=values[CONF_JOB_DEADLINE],
                    provider_timeout=values[CONF_PROVIDER_TIMEOUT],
                    extraction_timeout=values[CONF_EXTRACTION_TIMEOUT],
                    max_frames=max_frames,
                    max_attempts=values[CONF_MAX_ATTEMPTS],
                )
                if not valid_budget:
                    errors[CONF_JOB_DEADLINE] = "budget_deadline_too_short"
                elif enable_fusion:
                    valid_fusion_budget, _ = validate_fusion_budget_bounds(
                        fusion_job_deadline=values[CONF_FUSION_JOB_DEADLINE],
                        fusion_max_frames=values[CONF_FUSION_MAX_FRAMES],
                        provider_timeout=values[CONF_PROVIDER_TIMEOUT],
                        extraction_timeout=values[CONF_EXTRACTION_TIMEOUT],
                    )
                    if not valid_fusion_budget:
                        errors[CONF_FUSION_JOB_DEADLINE] = "budget_deadline_too_short"

            if not errors:
                return self.async_create_entry(
                    title="", data={**current_data, **values}
                )

        displayed = {**current_data, **submitted}
        # Never echo a stored or attempted secret back into the form.
        displayed[CONF_GEMINI_API_KEY] = ""
        schema = self._options_schema(
            displayed, _sections_with_errors(errors, _VISION_FIELD_SECTIONS)
        )

        return self.async_show_form(
            step_id="init",
            data_schema=schema,
            errors=errors,
            description_placeholders=self._summary_placeholders(),
        )

    @staticmethod
    def _options_schema(displayed: Mapping[str, Any], expanded: set[str]) -> Any:
        """Build the single options form with collapsed fusion/advanced groups."""
        fusion = vol.Schema(
            {
                vol.Optional(
                    CONF_ENABLE_FUSION,
                    default=bool(
                        displayed.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION)
                    ),
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_FUSION_MAX_FRAMES,
                    default=_form_number(
                        displayed.get(CONF_FUSION_MAX_FRAMES),
                        DEFAULT_FUSION_MAX_FRAMES,
                    ),
                ): number_spec_selector(SPEC_FUSION_MAX_FRAMES),
                vol.Optional(
                    CONF_FUSION_PROMPT_INSTRUCTIONS,
                    default=str(
                        displayed.get(
                            CONF_FUSION_PROMPT_INSTRUCTIONS,
                            DEFAULT_FUSION_PROMPT_INSTRUCTIONS,
                        )
                    ),
                ): selector.TextSelector(selector.TextSelectorConfig(multiline=True)),
                vol.Optional(
                    CONF_FUSION_JOB_DEADLINE,
                    default=_form_number(
                        displayed.get(CONF_FUSION_JOB_DEADLINE),
                        DEFAULT_FUSION_JOB_DEADLINE,
                    ),
                ): number_spec_selector(
                    SPEC_FUSION_JOB_DEADLINE, step=10, unit_of_measurement="s"
                ),
            }
        )
        advanced = vol.Schema(
            {
                vol.Optional(
                    CONF_JOB_DEADLINE,
                    default=_form_number(
                        displayed.get(CONF_JOB_DEADLINE), DEFAULT_JOB_DEADLINE
                    ),
                ): number_spec_selector(
                    SPEC_JOB_DEADLINE, step=5, unit_of_measurement="s"
                ),
                vol.Optional(
                    CONF_PROVIDER_TIMEOUT,
                    default=_form_number(
                        displayed.get(CONF_PROVIDER_TIMEOUT), DEFAULT_PROVIDER_TIMEOUT
                    ),
                ): number_spec_selector(
                    SPEC_PROVIDER_TIMEOUT, step=5, unit_of_measurement="s"
                ),
                vol.Optional(
                    CONF_EXTRACTION_TIMEOUT,
                    default=_form_number(
                        displayed.get(CONF_EXTRACTION_TIMEOUT),
                        DEFAULT_EXTRACTION_TIMEOUT,
                    ),
                ): number_spec_selector(
                    SPEC_EXTRACTION_TIMEOUT, step=1, unit_of_measurement="s"
                ),
                vol.Optional(
                    CONF_MAX_ATTEMPTS,
                    default=_form_number(
                        displayed.get(CONF_MAX_ATTEMPTS), DEFAULT_MAX_ATTEMPTS
                    ),
                ): number_spec_selector(SPEC_MAX_ATTEMPTS),
                vol.Optional(
                    CONF_FRAME_CACHE_MB,
                    default=_form_number(
                        displayed.get(CONF_FRAME_CACHE_MB), DEFAULT_FRAME_CACHE_MB
                    ),
                ): number_spec_selector(
                    SPEC_FRAME_CACHE_MB, step=8, unit_of_measurement="MB"
                ),
                vol.Optional(
                    CONF_FRAME_CACHE_TTL,
                    default=_form_number(
                        displayed.get(CONF_FRAME_CACHE_TTL), DEFAULT_FRAME_CACHE_TTL
                    ),
                ): number_spec_selector(
                    SPEC_FRAME_CACHE_TTL, step=30, unit_of_measurement="s"
                ),
                vol.Optional(
                    CONF_ENABLE_DEBUG_LOGGING,
                    default=bool(
                        displayed.get(
                            CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING
                        )
                    ),
                ): selector.BooleanSelector(),
            }
        )

        return vol.Schema(
            {
                vol.Optional(CONF_GEMINI_API_KEY, default=""): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(
                    CONF_MODEL, default=str(displayed.get(CONF_MODEL, DEFAULT_MODEL))
                ): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Optional(
                    CONF_CONCURRENCY,
                    default=_form_number(
                        displayed.get(CONF_CONCURRENCY), DEFAULT_CONCURRENCY
                    ),
                ): number_spec_selector(SPEC_CONCURRENCY),
                vol.Optional(
                    CONF_AUTO_ANALYZE,
                    default=bool(displayed.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)),
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_MAX_FRAMES,
                    default=_form_number(
                        displayed.get(CONF_MAX_FRAMES), DEFAULT_MAX_FRAMES
                    ),
                ): number_spec_selector(SPEC_MAX_FRAMES),
                vol.Optional(
                    CONF_SAMPLING_POLICY,
                    default=str(
                        displayed.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY)
                    ),
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(
                                value=SAMPLING_POLICY_SPARSE_ANCHORS,
                                label="Sparse Anchors (Chronological)",
                            ),
                            selector.SelectOptionDict(
                                value=SAMPLING_POLICY_TRIGGER_RELEVANT,
                                label="Trigger Relevant (Trigger Focused)",
                            ),
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
                vol.Optional(
                    CONF_PROMPT_INSTRUCTIONS,
                    default=str(
                        displayed.get(
                            CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS
                        )
                    ),
                ): selector.TextSelector(selector.TextSelectorConfig(multiline=True)),
                vol.Required(SECTION_FUSION): data_entry_flow.section(
                    fusion, {"collapsed": SECTION_FUSION not in expanded}
                ),
                vol.Required(SECTION_ADVANCED): data_entry_flow.section(
                    advanced, {"collapsed": SECTION_ADVANCED not in expanded}
                ),
            }
        )

    def _summary_placeholders(self) -> dict[str, str]:
        """Return a concise, secret-free summary for the form description."""
        current_data = self._get_current_data()
        model = str(current_data.get(CONF_MODEL, DEFAULT_MODEL)).strip()
        max_frames = int(current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))
        policy = str(current_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY))
        fusion_enabled = bool(
            current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION)
        )
        fusion_state = (
            "enabled ({frames} frames, {deadline:.1f}s deadline)".format(
                frames=int(
                    current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES)
                ),
                deadline=float(
                    current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE)
                ),
            )
            if fusion_enabled
            else "disabled"
        )
        min_deadline = (
            max_frames
            * float(current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT))
            + float(current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT))
            + CAS_CLEANUP_MARGIN_SECONDS
        )
        return {
            "model_name": model,
            "max_frames": str(max_frames),
            "sampling_policy": policy,
            "fusion_state": fusion_state,
            "min_deadline": f"{min_deadline:.1f}",
        }
