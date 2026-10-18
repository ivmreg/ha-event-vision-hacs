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
    DEFAULT_FUSION_PROMPT,
    DEFAULT_FUSION_PROMPT_INSTRUCTIONS,
    DEFAULT_JOB_DEADLINE,
    DEFAULT_MAX_ATTEMPTS,
    DEFAULT_MAX_FRAMES,
    DEFAULT_MODEL,
    DEFAULT_PROMPT_INSTRUCTIONS,
    DEFAULT_PROVIDER_TIMEOUT,
    DEFAULT_SAMPLING_POLICY,
    DOMAIN,
    GENERIC_MEDIA_PROMPT,
    MAX_ATTEMPTS_BOUND,
    MAX_CONCURRENCY,
    MAX_EXTRACTION_TIMEOUT,
    MAX_FRAME_CACHE_MB,
    MAX_FRAME_CACHE_TTL,
    MAX_FRAMES_BUDGET,
    MAX_FUSION_JOB_DEADLINE,
    MAX_FUSION_MAX_FRAMES,
    MAX_FUSION_PROMPT_INSTRUCTIONS_LEN,
    MAX_JOB_DEADLINE,
    MAX_PROMPT_INSTRUCTIONS_LEN,
    MAX_PROVIDER_TIMEOUT,
    MIN_ATTEMPTS,
    MIN_CONCURRENCY,
    MIN_EXTRACTION_TIMEOUT,
    MIN_FRAME_CACHE_MB,
    MIN_FRAME_CACHE_TTL,
    MIN_FRAMES,
    MIN_FUSION_JOB_DEADLINE,
    MIN_FUSION_MAX_FRAMES,
    MIN_JOB_DEADLINE,
    MIN_PROVIDER_TIMEOUT,
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
    model_alias_info,
    validate_budget_bounds,
    validate_fusion_budget_bounds,
)
from .validation import number_spec_selector


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
                    SPEC_CONCURRENCY.validate(user_input.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY))
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
                        CONF_AUTO_ANALYZE: bool(user_input.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE)),
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

        schema = vol.Schema(
            {
                vol.Required(CONF_GEMINI_API_KEY): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.PASSWORD)
                ),
                vol.Optional(CONF_MODEL, default=DEFAULT_MODEL): selector.TextSelector(
                    selector.TextSelectorConfig(type=selector.TextSelectorType.TEXT)
                ),
                vol.Optional(CONF_CONCURRENCY, default=DEFAULT_CONCURRENCY): (
                    number_spec_selector(SPEC_CONCURRENCY)
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_CONCURRENCY,
                            max=MAX_CONCURRENCY,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(CONF_AUTO_ANALYZE, default=DEFAULT_AUTO_ANALYZE): selector.BooleanSelector(),
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
    """Handle structured options flow for Event Vision."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    def _get_current_data(self) -> dict[str, Any]:
        """Return combined configuration and options data."""
        return {**self._config_entry.data, **self._config_entry.options}

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Show options sub-menu."""
        return self.async_show_menu(
            step_id="init",
            menu_options=["general", "sampling", "fusion", "advanced", "effective"],
        )

    async def async_step_general(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage general options: Model, Concurrency, Auto-Analyze, and API Key."""
        errors: dict[str, str] = {}
        current_data = self._get_current_data()

        if user_input is not None:
            new_api_key = str(user_input.get(CONF_GEMINI_API_KEY, "")).strip()
            # If user left API key empty, preserve existing key
            api_key = new_api_key if new_api_key else current_data.get(CONF_GEMINI_API_KEY, "")

            model = str(user_input.get(CONF_MODEL, current_data.get(CONF_MODEL, DEFAULT_MODEL))).strip()
            if not model:
                errors[CONF_MODEL] = "invalid_model"

            concurrency_raw = user_input.get(CONF_CONCURRENCY, current_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY))
            try:
                concurrency = int(SPEC_CONCURRENCY.validate(concurrency_raw))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"
                concurrency = current_data.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY)

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_GEMINI_API_KEY: api_key,
                    CONF_MODEL: model,
                    CONF_CONCURRENCY: concurrency,
                    CONF_AUTO_ANALYZE: bool(user_input.get(CONF_AUTO_ANALYZE, current_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE))),
                    CONF_ENABLE_DEBUG_LOGGING: bool(
                        user_input.get(
                            CONF_ENABLE_DEBUG_LOGGING,
                            current_data.get(CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING),
                        )
                    ),
                }
                return self.async_create_entry(title="", data=updated_options)

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
                ): (
                    number_spec_selector(SPEC_CONCURRENCY)
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_CONCURRENCY,
                            max=MAX_CONCURRENCY,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_AUTO_ANALYZE,
                    default=current_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE),
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_ENABLE_DEBUG_LOGGING,
                    default=current_data.get(
                        CONF_ENABLE_DEBUG_LOGGING, DEFAULT_ENABLE_DEBUG_LOGGING
                    ),
                ): selector.BooleanSelector(),
            }
        )

        return self.async_show_form(step_id="general", data_schema=schema, errors=errors)

    async def async_step_sampling(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage sampling options: frame budget, policy, and prompt instructions."""
        errors: dict[str, str] = {}
        current_data = self._get_current_data()

        if user_input is not None:
            max_frames_raw = user_input.get(CONF_MAX_FRAMES, current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))
            try:
                max_frames = int(SPEC_MAX_FRAMES.validate(max_frames_raw))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_MAX_FRAMES] = "invalid_max_frames"
                max_frames = current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES)

            policy = str(user_input.get(CONF_SAMPLING_POLICY, current_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY))).strip()
            if policy not in VALID_SAMPLING_POLICIES:
                errors[CONF_SAMPLING_POLICY] = "invalid_sampling_policy"

            instructions = str(user_input.get(CONF_PROMPT_INSTRUCTIONS, "")).strip()
            if len(instructions) > MAX_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_PROMPT_INSTRUCTIONS] = "instructions_too_long"

            # Coupled budget check with current advanced settings
            valid_budget, _ = validate_budget_bounds(
                job_deadline=current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE),
                provider_timeout=current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT),
                extraction_timeout=current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT),
                max_frames=max_frames,
                max_attempts=current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS),
            )
            if not valid_budget:
                errors[CONF_MAX_FRAMES] = "budget_deadline_too_short"

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_MAX_FRAMES: max_frames,
                    CONF_SAMPLING_POLICY: policy,
                    CONF_PROMPT_INSTRUCTIONS: instructions,
                }
                return self.async_create_entry(title="", data=updated_options)

        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_MAX_FRAMES,
                    default=current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES),
                ): (
                    number_spec_selector(SPEC_MAX_FRAMES)
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_FRAMES,
                            max=MAX_FRAMES_BUDGET,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_SAMPLING_POLICY,
                    default=current_data.get(CONF_SAMPLING_POLICY, DEFAULT_SAMPLING_POLICY),
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
                    default=current_data.get(CONF_PROMPT_INSTRUCTIONS, DEFAULT_PROMPT_INSTRUCTIONS),
                ): selector.TextSelector(selector.TextSelectorConfig(multiline=True)),
            }
        )

        return self.async_show_form(step_id="sampling", data_schema=schema, errors=errors)

    async def async_step_fusion(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage multi-camera visual fusion options."""
        errors: dict[str, str] = {}
        current_data = self._get_current_data()

        if user_input is not None:
            enable_fusion = bool(user_input.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION))

            raw_max_frames = user_input.get(
                CONF_FUSION_MAX_FRAMES,
                current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES),
            )
            try:
                fusion_max_frames = int(SPEC_FUSION_MAX_FRAMES.validate(raw_max_frames))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FUSION_MAX_FRAMES] = "invalid_fusion_max_frames"
                fusion_max_frames = current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES)

            instructions = str(user_input.get(CONF_FUSION_PROMPT_INSTRUCTIONS, "")).strip()
            if len(instructions) > MAX_FUSION_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_FUSION_PROMPT_INSTRUCTIONS] = "instructions_too_long"

            raw_deadline = user_input.get(
                CONF_FUSION_JOB_DEADLINE,
                current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE),
            )
            try:
                fusion_deadline = float(SPEC_FUSION_JOB_DEADLINE.validate(raw_deadline))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FUSION_JOB_DEADLINE] = "invalid_fusion_deadline"
                fusion_deadline = current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE)

            if not errors:
                valid_fbudget, _ = validate_fusion_budget_bounds(
                    fusion_job_deadline=fusion_deadline,
                    fusion_max_frames=fusion_max_frames,
                    provider_timeout=current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT),
                    extraction_timeout=current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT),
                )
                if not valid_fbudget:
                    errors[CONF_FUSION_JOB_DEADLINE] = "budget_deadline_too_short"

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_ENABLE_FUSION: enable_fusion,
                    CONF_FUSION_MAX_FRAMES: fusion_max_frames,
                    CONF_FUSION_PROMPT_INSTRUCTIONS: instructions,
                    CONF_FUSION_JOB_DEADLINE: fusion_deadline,
                }
                return self.async_create_entry(title="", data=updated_options)

        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_ENABLE_FUSION,
                    default=current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION),
                ): selector.BooleanSelector(),
                vol.Optional(
                    CONF_FUSION_MAX_FRAMES,
                    default=current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES),
                ): (
                    number_spec_selector(SPEC_FUSION_MAX_FRAMES)
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_FUSION_MAX_FRAMES,
                            max=MAX_FUSION_MAX_FRAMES,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_FUSION_PROMPT_INSTRUCTIONS,
                    default=current_data.get(
                        CONF_FUSION_PROMPT_INSTRUCTIONS, DEFAULT_FUSION_PROMPT_INSTRUCTIONS
                    ),
                ): selector.TextSelector(selector.TextSelectorConfig(multiline=True)),
                vol.Optional(
                    CONF_FUSION_JOB_DEADLINE,
                    default=current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE),
                ): (
                    number_spec_selector(SPEC_FUSION_JOB_DEADLINE, step=10, unit_of_measurement="s")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_FUSION_JOB_DEADLINE,
                            max=MAX_FUSION_JOB_DEADLINE,
                            step=10,
                            unit_of_measurement="s",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
            }
        )

        return self.async_show_form(step_id="fusion", data_schema=schema, errors=errors)

    async def async_step_advanced(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Manage coupled execution timeouts and attempt bounds."""
        errors: dict[str, str] = {}
        current_data = self._get_current_data()

        if user_input is not None:
            raw_deadline = user_input.get(CONF_JOB_DEADLINE, current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE))
            raw_provider = user_input.get(CONF_PROVIDER_TIMEOUT, current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT))
            raw_extraction = user_input.get(CONF_EXTRACTION_TIMEOUT, current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT))
            raw_attempts = user_input.get(CONF_MAX_ATTEMPTS, current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS))
            raw_cache_mb = user_input.get(CONF_FRAME_CACHE_MB, current_data.get(CONF_FRAME_CACHE_MB, DEFAULT_FRAME_CACHE_MB))
            raw_cache_ttl = user_input.get(CONF_FRAME_CACHE_TTL, current_data.get(CONF_FRAME_CACHE_TTL, DEFAULT_FRAME_CACHE_TTL))
            current_max_frames = current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES)

            deadline = current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE)
            provider_timeout = current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT)
            extraction_timeout = current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT)
            attempts = current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS)
            cache_mb = current_data.get(CONF_FRAME_CACHE_MB, DEFAULT_FRAME_CACHE_MB)
            cache_ttl = current_data.get(CONF_FRAME_CACHE_TTL, DEFAULT_FRAME_CACHE_TTL)

            try:
                deadline = float(SPEC_JOB_DEADLINE.validate(raw_deadline))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_JOB_DEADLINE] = "invalid_job_deadline"

            try:
                provider_timeout = float(SPEC_PROVIDER_TIMEOUT.validate(raw_provider))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_PROVIDER_TIMEOUT] = "invalid_provider_timeout"

            try:
                extraction_timeout = float(SPEC_EXTRACTION_TIMEOUT.validate(raw_extraction))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_EXTRACTION_TIMEOUT] = "invalid_extraction_timeout"

            try:
                attempts = int(SPEC_MAX_ATTEMPTS.validate(raw_attempts))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_MAX_ATTEMPTS] = "invalid_max_attempts"

            try:
                cache_mb = int(SPEC_FRAME_CACHE_MB.validate(raw_cache_mb))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FRAME_CACHE_MB] = "invalid_frame_cache_mb"

            try:
                cache_ttl = float(SPEC_FRAME_CACHE_TTL.validate(raw_cache_ttl))
            except (ValidationError, ValueError, TypeError):
                errors[CONF_FRAME_CACHE_TTL] = "invalid_frame_cache_ttl"

            # Check coupled budget only if individual bounds are valid
            if not errors:
                valid_budget, _ = validate_budget_bounds(
                    job_deadline=deadline,
                    provider_timeout=provider_timeout,
                    extraction_timeout=extraction_timeout,
                    max_frames=int(current_max_frames),
                    max_attempts=int(attempts),
                )
                if not valid_budget:
                    errors[CONF_JOB_DEADLINE] = "budget_deadline_too_short"

                if current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION):
                    valid_fbudget, _ = validate_fusion_budget_bounds(
                        fusion_job_deadline=current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE),
                        fusion_max_frames=current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES),
                        provider_timeout=provider_timeout,
                        extraction_timeout=extraction_timeout,
                    )
                    if not valid_fbudget:
                        errors[CONF_JOB_DEADLINE] = "budget_deadline_too_short"

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_JOB_DEADLINE: deadline,
                    CONF_PROVIDER_TIMEOUT: provider_timeout,
                    CONF_EXTRACTION_TIMEOUT: extraction_timeout,
                    CONF_MAX_ATTEMPTS: attempts,
                    CONF_FRAME_CACHE_MB: cache_mb,
                    CONF_FRAME_CACHE_TTL: cache_ttl,
                }
                return self.async_create_entry(title="", data=updated_options)

        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_JOB_DEADLINE,
                    default=current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE),
                ): (
                    number_spec_selector(SPEC_JOB_DEADLINE, step=5, unit_of_measurement="s")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_JOB_DEADLINE,
                            max=MAX_JOB_DEADLINE,
                            step=5,
                            unit_of_measurement="s",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_PROVIDER_TIMEOUT,
                    default=current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT),
                ): (
                    number_spec_selector(SPEC_PROVIDER_TIMEOUT, step=5, unit_of_measurement="s")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_PROVIDER_TIMEOUT,
                            max=MAX_PROVIDER_TIMEOUT,
                            step=5,
                            unit_of_measurement="s",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_EXTRACTION_TIMEOUT,
                    default=current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT),
                ): (
                    number_spec_selector(SPEC_EXTRACTION_TIMEOUT, step=1, unit_of_measurement="s")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_EXTRACTION_TIMEOUT,
                            max=MAX_EXTRACTION_TIMEOUT,
                            step=1,
                            unit_of_measurement="s",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_MAX_ATTEMPTS,
                    default=current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS),
                ): (
                    number_spec_selector(SPEC_MAX_ATTEMPTS)
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_ATTEMPTS,
                            max=MAX_ATTEMPTS_BOUND,
                            step=1,
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_FRAME_CACHE_MB,
                    default=current_data.get(CONF_FRAME_CACHE_MB, DEFAULT_FRAME_CACHE_MB),
                ): (
                    number_spec_selector(SPEC_FRAME_CACHE_MB, step=8, unit_of_measurement="MB")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_FRAME_CACHE_MB,
                            max=MAX_FRAME_CACHE_MB,
                            step=8,
                            unit_of_measurement="MB",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
                vol.Optional(
                    CONF_FRAME_CACHE_TTL,
                    default=current_data.get(CONF_FRAME_CACHE_TTL, DEFAULT_FRAME_CACHE_TTL),
                ): (
                    number_spec_selector(SPEC_FRAME_CACHE_TTL, step=30, unit_of_measurement="s")
                    if number_spec_selector is not None
                    else selector.NumberSelector(
                        selector.NumberSelectorConfig(
                            min=MIN_FRAME_CACHE_TTL,
                            max=MAX_FRAME_CACHE_TTL,
                            step=30,
                            unit_of_measurement="s",
                            mode=selector.NumberSelectorMode.BOX,
                        )
                    )
                ),
            }
        )

        return self.async_show_form(step_id="advanced", data_schema=schema, errors=errors)

    async def async_step_effective(
        self, user_input: dict[str, Any] | None = None
    ) -> config_entries.ConfigFlowResult:
        """Show read-only effective configuration, prompt compositions, and budget limits."""
        if user_input is not None:
            return self.async_create_entry(title="", data=self._get_current_data())

        current_data = self._get_current_data()
        model = str(current_data.get(CONF_MODEL, DEFAULT_MODEL)).strip()
        custom_instructions = str(current_data.get(CONF_PROMPT_INSTRUCTIONS, "")).strip()
        max_frames = int(current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES))
        ext_timeout = float(current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT))
        prov_timeout = float(current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT))
        deadline = float(current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE))

        # Effective prompt composition
        effective_prompt = GENERIC_MEDIA_PROMPT
        if custom_instructions:
            effective_prompt += f"\nAdditional Instructions: {custom_instructions}"

        alias_info = model_alias_info(model)

        min_req = (max_frames * ext_timeout) + prov_timeout + 10.0
        budget_summary = (
            f"Configured: {deadline:.1f}s | Minimum Required: {min_req:.1f}s "
            f"({max_frames} frames * {ext_timeout:.1f}s + {prov_timeout:.1f}s prov + 10.0s CAS)"
        )

        # Fusion effective summary
        fusion_enabled = bool(current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION))
        fusion_frames = int(current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES))
        fusion_inst = str(current_data.get(CONF_FUSION_PROMPT_INSTRUCTIONS, "")).strip()
        fusion_dead = float(current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE))
        effective_fusion_prompt = DEFAULT_FUSION_PROMPT
        if fusion_inst:
            effective_fusion_prompt += f"\nAdditional Instructions: {fusion_inst}"

        fusion_summary = (
            f"Enabled: {fusion_enabled} | Frame Budget: {fusion_frames} | Deadline: {fusion_dead:.1f}s\n"
            f"Effective Fusion Prompt:\n{effective_fusion_prompt}"
        )

        placeholders = {
            "model_name": model,
            "model_alias_fact": alias_info,
            "base_prompt": GENERIC_MEDIA_PROMPT,
            "custom_instructions": custom_instructions or "(None configured)",
            "effective_prompt": effective_prompt,
            "fusion_summary": fusion_summary,
            "input_limits": "10 MB per file, max 4096px dimension",
            "budget_requirements": budget_summary,
            "policy_scope": (
                "Settings apply to future events and explicit manual reanalysis only. "
                "Historical completed and skipped records remain untouched."
            ),
        }

        return self.async_show_form(
            step_id="effective",
            data_schema=vol.Schema({}),
            description_placeholders=placeholders,
        )
