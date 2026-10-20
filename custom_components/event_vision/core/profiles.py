"""Immutable validated execution snapshots, shared by deadline and job handlers."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from functools import lru_cache
from types import MappingProxyType
from typing import Any

from .models import validate_fusion_profile_payload, validate_profile_payload


@dataclass(frozen=True, slots=True)
class ExecutionProfile:
    semantic: Mapping[str, Any]
    budget: Mapping[str, Any]

    @property
    def deadline(self) -> float:
        return self.budget["job_deadline"]


@lru_cache(maxsize=64)
def load_profile(payload: str, digest: str, fusion: bool = False) -> ExecutionProfile:
    """Decode/validate a durable snapshot once, never use mutable current options."""
    validate = validate_fusion_profile_payload if fusion else validate_profile_payload
    semantic, budget = validate(payload, expected_digest=digest)
    return ExecutionProfile(MappingProxyType(semantic), MappingProxyType(budget))
