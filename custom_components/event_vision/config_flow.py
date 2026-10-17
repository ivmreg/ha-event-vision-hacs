"""Config flow for Event Vision."""

from __future__ import annotations

import math
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
    DEFAULT_AUTO_ANALYZE,
    DEFAULT_CONCURRENCY,
    DEFAULT_ENABLE_FUSION,
    DEFAULT_EXTRACTION_TIMEOUT,
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
    MIN_FRAMES,
    MIN_FUSION_JOB_DEADLINE,
    MIN_FUSION_MAX_FRAMES,
    MIN_JOB_DEADLINE,
    MIN_PROVIDER_TIMEOUT,
    SAMPLING_POLICY_SPARSE_ANCHORS,
    SAMPLING_POLICY_TRIGGER_RELEVANT,
    VALID_SAMPLING_POLICIES,
)
from .core.models import model_alias_info, validate_budget_bounds, validate_fusion_budget_bounds


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

            concurrency_raw = user_input.get(CONF_CONCURRENCY, DEFAULT_CONCURRENCY)
            if (
                isinstance(concurrency_raw, bool)
                or not isinstance(concurrency_raw, (int, float))
                or not math.isfinite(concurrency_raw)
                or (isinstance(concurrency_raw, float) and not concurrency_raw.is_integer())
            ):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"
            else:
                concurrency = int(concurrency_raw)
                if not (MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY):
                    errors[CONF_CONCURRENCY] = "invalid_concurrency"

            model = str(user_input.get(CONF_MODEL, DEFAULT_MODEL)).strip() or DEFAULT_MODEL

            if not errors:
                return self.async_create_entry(
                    title="Event Vision Analyzer",
                    data={
                        CONF_GEMINI_API_KEY: api_key,
                        CONF_MODEL: model,
                        CONF_CONCURRENCY: int(concurrency_raw),
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
            if (
                isinstance(concurrency_raw, bool)
                or not isinstance(concurrency_raw, (int, float))
                or not math.isfinite(concurrency_raw)
                or (isinstance(concurrency_raw, float) and not concurrency_raw.is_integer())
            ):
                errors[CONF_CONCURRENCY] = "invalid_concurrency"
            else:
                concurrency = int(concurrency_raw)
                if not (MIN_CONCURRENCY <= concurrency <= MAX_CONCURRENCY):
                    errors[CONF_CONCURRENCY] = "invalid_concurrency"

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_GEMINI_API_KEY: api_key,
                    CONF_MODEL: model,
                    CONF_CONCURRENCY: int(concurrency_raw),
                    CONF_AUTO_ANALYZE: bool(user_input.get(CONF_AUTO_ANALYZE, current_data.get(CONF_AUTO_ANALYZE, DEFAULT_AUTO_ANALYZE))),
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
            if (
                isinstance(max_frames_raw, bool)
                or not isinstance(max_frames_raw, (int, float))
                or not math.isfinite(max_frames_raw)
                or (isinstance(max_frames_raw, float) and not max_frames_raw.is_integer())
            ):
                errors[CONF_MAX_FRAMES] = "invalid_max_frames"
                max_frames = DEFAULT_MAX_FRAMES
            else:
                max_frames = int(max_frames_raw)
                if not (MIN_FRAMES <= max_frames <= MAX_FRAMES_BUDGET):
                    errors[CONF_MAX_FRAMES] = "invalid_max_frames"

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
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_FRAMES,
                        max=MAX_FRAMES_BUDGET,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
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
            if (
                isinstance(raw_max_frames, bool)
                or not isinstance(raw_max_frames, (int, float))
                or not math.isfinite(raw_max_frames)
                or (isinstance(raw_max_frames, float) and not raw_max_frames.is_integer())
                or not (MIN_FUSION_MAX_FRAMES <= int(raw_max_frames) <= MAX_FUSION_MAX_FRAMES)
            ):
                errors[CONF_FUSION_MAX_FRAMES] = "invalid_fusion_max_frames"
                fusion_max_frames = DEFAULT_FUSION_MAX_FRAMES
            else:
                fusion_max_frames = int(raw_max_frames)

            instructions = str(user_input.get(CONF_FUSION_PROMPT_INSTRUCTIONS, "")).strip()
            if len(instructions) > MAX_FUSION_PROMPT_INSTRUCTIONS_LEN:
                errors[CONF_FUSION_PROMPT_INSTRUCTIONS] = "instructions_too_long"

            raw_deadline = user_input.get(
                CONF_FUSION_JOB_DEADLINE,
                current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE),
            )
            if (
                isinstance(raw_deadline, bool)
                or not isinstance(raw_deadline, (int, float))
                or not math.isfinite(raw_deadline)
                or not (MIN_FUSION_JOB_DEADLINE <= float(raw_deadline) <= MAX_FUSION_JOB_DEADLINE)
            ):
                errors[CONF_FUSION_JOB_DEADLINE] = "invalid_fusion_deadline"
                fusion_deadline = DEFAULT_FUSION_JOB_DEADLINE
            else:
                fusion_deadline = float(raw_deadline)

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
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_FUSION_MAX_FRAMES,
                        max=MAX_FUSION_MAX_FRAMES,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
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
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_FUSION_JOB_DEADLINE,
                        max=MAX_FUSION_JOB_DEADLINE,
                        step=10,
                        unit_of_measurement="s",
                        mode=selector.NumberSelectorMode.BOX,
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
            current_max_frames = current_data.get(CONF_MAX_FRAMES, DEFAULT_MAX_FRAMES)

            # Validate raw inputs before coercion
            if (
                isinstance(raw_deadline, bool)
                or not isinstance(raw_deadline, (int, float))
                or not math.isfinite(raw_deadline)
                or not (MIN_JOB_DEADLINE <= float(raw_deadline) <= MAX_JOB_DEADLINE)
            ):
                errors[CONF_JOB_DEADLINE] = "invalid_job_deadline"

            if (
                isinstance(raw_provider, bool)
                or not isinstance(raw_provider, (int, float))
                or not math.isfinite(raw_provider)
                or not (MIN_PROVIDER_TIMEOUT <= float(raw_provider) <= MAX_PROVIDER_TIMEOUT)
            ):
                errors[CONF_PROVIDER_TIMEOUT] = "invalid_provider_timeout"

            if (
                isinstance(raw_extraction, bool)
                or not isinstance(raw_extraction, (int, float))
                or not math.isfinite(raw_extraction)
                or not (MIN_EXTRACTION_TIMEOUT <= float(raw_extraction) <= MAX_EXTRACTION_TIMEOUT)
            ):
                errors[CONF_EXTRACTION_TIMEOUT] = "invalid_extraction_timeout"

            if (
                isinstance(raw_attempts, bool)
                or not isinstance(raw_attempts, (int, float))
                or not math.isfinite(raw_attempts)
                or (isinstance(raw_attempts, float) and not raw_attempts.is_integer())
                or not (MIN_ATTEMPTS <= int(raw_attempts) <= MAX_ATTEMPTS_BOUND)
            ):
                errors[CONF_MAX_ATTEMPTS] = "invalid_max_attempts"

            # Check coupled budget only if individual bounds are valid
            if not errors:
                valid_budget, _ = validate_budget_bounds(
                    job_deadline=float(raw_deadline),
                    provider_timeout=float(raw_provider),
                    extraction_timeout=float(raw_extraction),
                    max_frames=int(current_max_frames),
                    max_attempts=int(raw_attempts),
                )
                if not valid_budget:
                    errors[CONF_JOB_DEADLINE] = "budget_deadline_too_short"

                if current_data.get(CONF_ENABLE_FUSION, DEFAULT_ENABLE_FUSION):
                    valid_fbudget, _ = validate_fusion_budget_bounds(
                        fusion_job_deadline=current_data.get(CONF_FUSION_JOB_DEADLINE, DEFAULT_FUSION_JOB_DEADLINE),
                        fusion_max_frames=current_data.get(CONF_FUSION_MAX_FRAMES, DEFAULT_FUSION_MAX_FRAMES),
                        provider_timeout=float(raw_provider),
                        extraction_timeout=float(raw_extraction),
                    )
                    if not valid_fbudget:
                        errors[CONF_JOB_DEADLINE] = "budget_deadline_too_short"

            if not errors:
                updated_options = {
                    **current_data,
                    CONF_JOB_DEADLINE: float(raw_deadline),
                    CONF_PROVIDER_TIMEOUT: float(raw_provider),
                    CONF_EXTRACTION_TIMEOUT: float(raw_extraction),
                    CONF_MAX_ATTEMPTS: int(raw_attempts),
                }
                return self.async_create_entry(title="", data=updated_options)

        schema = vol.Schema(
            {
                vol.Optional(
                    CONF_JOB_DEADLINE,
                    default=current_data.get(CONF_JOB_DEADLINE, DEFAULT_JOB_DEADLINE),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_JOB_DEADLINE,
                        max=MAX_JOB_DEADLINE,
                        step=5,
                        unit_of_measurement="s",
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(
                    CONF_PROVIDER_TIMEOUT,
                    default=current_data.get(CONF_PROVIDER_TIMEOUT, DEFAULT_PROVIDER_TIMEOUT),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_PROVIDER_TIMEOUT,
                        max=MAX_PROVIDER_TIMEOUT,
                        step=5,
                        unit_of_measurement="s",
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(
                    CONF_EXTRACTION_TIMEOUT,
                    default=current_data.get(CONF_EXTRACTION_TIMEOUT, DEFAULT_EXTRACTION_TIMEOUT),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_EXTRACTION_TIMEOUT,
                        max=MAX_EXTRACTION_TIMEOUT,
                        step=1,
                        unit_of_measurement="s",
                        mode=selector.NumberSelectorMode.BOX,
                    )
                ),
                vol.Optional(
                    CONF_MAX_ATTEMPTS,
                    default=current_data.get(CONF_MAX_ATTEMPTS, DEFAULT_MAX_ATTEMPTS),
                ): selector.NumberSelector(
                    selector.NumberSelectorConfig(
                        min=MIN_ATTEMPTS,
                        max=MAX_ATTEMPTS_BOUND,
                        step=1,
                        mode=selector.NumberSelectorMode.BOX,
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
