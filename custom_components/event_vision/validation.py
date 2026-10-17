"""Home Assistant and Voluptuous validation integration for Event Vision.

Bridges declarative pure-Python FieldSpec/NumberSpec definitions to Voluptuous
schemas and Home Assistant UI selectors.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

try:
    import probatio as vol
except ImportError:
    try:
        import voluptuous as vol
    except ImportError:
        vol = None  # type: ignore[assignment]

try:
    from custom_components.event_recording.validation import (
        SpecNumberSelector,
        float_spec_validator,
        id_spec_validator,
        integer_spec_validator,
        number_spec_selector,
        spec_validator,
        string_spec_validator,
    )
except ImportError:
    # Stand-alone fallback
    from .core.models import (
        FieldSpec,
        IdSpec,
        NumberSpec,
        StringSpec,
        ValidationError,
    )

    def spec_validator(spec: FieldSpec) -> Callable[[Any], Any]:
        """Produce a Voluptuous-compatible validator callable from a FieldSpec."""

        def _validate(val: Any) -> Any:
            try:
                return spec.validate(val)
            except (ValidationError, ValueError) as err:
                if vol is not None:
                    raise vol.Invalid(str(err)) from err
                raise

        return _validate

    def integer_spec_validator(
        name: str,
        min_val: int | None = None,
        max_val: int | None = None,
        *,
        allow_none: bool = False,
        default: int | None = None,
    ) -> Callable[[Any], int | None]:
        spec = NumberSpec(
            name=name,
            min_val=min_val,
            max_val=max_val,
            is_integer=True,
            allow_none=allow_none,
            default=default,
        )
        return spec_validator(spec)

    def float_spec_validator(
        name: str,
        min_val: float | None = None,
        max_val: float | None = None,
        *,
        allow_none: bool = False,
        default: float | None = None,
    ) -> Callable[[Any], float | None]:
        spec = NumberSpec(
            name=name,
            min_val=min_val,
            max_val=max_val,
            is_integer=False,
            allow_none=allow_none,
            default=default,
        )
        return spec_validator(spec)

    def string_spec_validator(
        name: str,
        min_len: int = 1,
        max_len: int = 255,
        *,
        allow_none: bool = False,
        default: str | None = None,
    ) -> Callable[[Any], str | None]:
        spec = StringSpec(
            name=name,
            min_len=min_len,
            max_len=max_len,
            allow_none=allow_none,
            default=default,
        )
        return spec_validator(spec)

    def id_spec_validator(
        name: str = "id",
        max_len: int = 64,
    ) -> Callable[[Any], str]:
        spec = IdSpec(name=name, max_len=max_len)
        return spec_validator(spec)

    try:
        from homeassistant.helpers import selector
    except ImportError:
        selector = None  # type: ignore[assignment]

    if selector is not None:

        class SpecNumberSelector(selector.NumberSelector):
            """Native number selector backed by a declarative NumberSpec."""

            def __init__(
                self,
                spec: NumberSpec,
                *,
                step: float | int | None = None,
                mode: Any = None,
                unit_of_measurement: str | None = None,
            ) -> None:
                self.spec = spec
                calc_step = step
                if calc_step is None:
                    calc_step = 1 if (spec.is_integer or spec.integer) else 0.1
                min_v = float(spec.min_val) if spec.min_val is not None else None
                max_v = float(spec.max_val) if spec.max_val is not None else None
                effective_mode = mode or getattr(selector.NumberSelectorMode, "BOX", "box")
                cfg_kwargs: dict[str, Any] = {
                    "step": calc_step,
                    "mode": effective_mode,
                }
                if min_v is not None:
                    cfg_kwargs["min"] = min_v
                if max_v is not None:
                    cfg_kwargs["max"] = max_v
                if unit_of_measurement is not None:
                    cfg_kwargs["unit_of_measurement"] = unit_of_measurement
                config = selector.NumberSelectorConfig(**cfg_kwargs)
                super().__init__(config)

            def __call__(self, data: Any) -> float | int | None:
                import math

                if isinstance(data, bool):
                    if vol is not None:
                        raise vol.Invalid(f"{self.spec.name} must not be a boolean")
                    raise ValidationError(f"{self.spec.name} must not be a boolean")
                try:
                    res = super().__call__(data)
                except OverflowError as err:
                    if vol is not None:
                        raise vol.Invalid(f"{self.spec.name} value is too large") from err
                    raise ValidationError(f"{self.spec.name} value is too large") from err
                if isinstance(res, float) and math.isinf(res):
                    if vol is not None:
                        raise vol.Invalid(f"{self.spec.name} must be finite")
                    raise ValidationError(f"{self.spec.name} must be finite")
                if (
                    (self.spec.is_integer or self.spec.integer)
                    and isinstance(res, float)
                    and math.isfinite(res)
                    and res.is_integer()
                ):
                    return int(res)
                return res

        def number_spec_selector(
            spec: NumberSpec,
            *,
            step: float | int | None = None,
            mode: Any = None,
            unit_of_measurement: str | None = None,
        ) -> Any:
            return SpecNumberSelector(
                spec, step=step, mode=mode, unit_of_measurement=unit_of_measurement
            )

    else:
        SpecNumberSelector = None  # type: ignore[assignment, misc]
        number_spec_selector = None  # type: ignore[assignment]


__all__ = [
    "SpecNumberSelector",
    "float_spec_validator",
    "id_spec_validator",
    "integer_spec_validator",
    "number_spec_selector",
    "spec_validator",
    "string_spec_validator",
]
