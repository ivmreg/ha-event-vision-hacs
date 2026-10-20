"""Direct Gemini REST API adapter for event_vision.

Features:
- Pure Python (zero Home Assistant dependencies)
- Direct Gemini REST generateContent (v1beta) using x-goog-api-key header
- Host validation (strictly generativelanguage.googleapis.com)
- API key secrecy (never logged, never in URL, never in exceptions)
- Structured JSON schema enforcement with bounded types and lengths
- Retryable status classification (429, 5xx, timeouts) vs permanent (400, 401, 403, schema errors)
- Generic prompt describing media only; zero source payload or private context
"""

from __future__ import annotations

import base64
import email.utils
import json
import logging
import math
import re
import time
import urllib.parse
from datetime import UTC
from typing import Any

import aiohttp

from ..const import (
    DEFAULT_FUSION_PROMPT,
    DEFAULT_MODEL,
    GENERIC_MEDIA_PROMPT,
    MAX_FUSION_PROMPT_INSTRUCTIONS_LEN,
    MAX_OBJECT_LEN,
    MAX_OBJECTS,
    MAX_PROMPT_INSTRUCTIONS_LEN,
    MAX_RETRY_AFTER_SECONDS,
    MAX_SUMMARY_LEN,
)
from .models import (
    AnalysisError,
    AnalysisResult,
    ExtractedFrame,
    FusionResult,
    FusionViewInput,
    GeminiKeyError,
    GeminiSchemaError,
    utc_now,
)

_LOGGER = logging.getLogger(__name__)


def parse_retry_after(
    header_val: str | None, max_delay: float = MAX_RETRY_AFTER_SECONDS
) -> float | None:
    """Parse HTTP Retry-After header (seconds or HTTP-date) into bounded numeric delay in seconds.

    Validates max_delay is a finite non-negative number capped by MAX_RETRY_AFTER_SECONDS.
    Returns None if header is absent, empty, or unparseable.
    Never logs headers or response bodies.
    Guarantees result is finite, non-negative, and <= effective max_delay.
    """
    if isinstance(max_delay, bool) or not isinstance(max_delay, (int, float)):
        raise ValueError("max_delay must be a finite non-negative number")
    max_cap = float(max_delay)
    if math.isnan(max_cap) or not math.isfinite(max_cap) or max_cap < 0.0:
        raise ValueError("max_delay must be a finite non-negative number")
    effective_max = min(max_cap, float(MAX_RETRY_AFTER_SECONDS))

    if header_val is None or not isinstance(header_val, str):
        return None
    val = header_val.strip().strip('"')
    if not val:
        return None

    # 1. Try integer/float seconds
    try:
        delay = float(val)
        if math.isnan(delay) or not math.isfinite(delay):
            return None
        if delay < 0.0:
            return 0.0
        parsed = min(delay, effective_max)
        _LOGGER.debug("Parsed numeric Retry-After delay: %.2fs (capped to %.2fs)", delay, parsed)
        return parsed
    except (ValueError, OverflowError):
        pass

    # 2. Try HTTP-date format (RFC 7231 / RFC 9110)
    try:
        dt = email.utils.parsedate_to_datetime(val)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            now = utc_now()
            diff = (dt - now).total_seconds()
            if not math.isfinite(diff) or math.isnan(diff):
                return None
            if diff < 0.0:
                return 0.0
            parsed = min(diff, effective_max)
            _LOGGER.debug(
                "Parsed HTTP-date Retry-After delay: %.2fs (capped to %.2fs)", diff, parsed
            )
            return parsed
    except Exception:
        pass

    return None


GEMINI_HOST = "generativelanguage.googleapis.com"

# Regex patterns to detect disallowed private data in provider requests
_PRIVATE_URL_PATTERN = re.compile(r"https?://|ftp://", re.IGNORECASE)
_PRIVATE_PATH_PATTERN = re.compile(
    r"(?:[a-zA-Z]:[\\/]|/(?:config|var|tmp|home|etc|media)/)", re.IGNORECASE
)
_PRIVATE_ENTITY_PATTERN = re.compile(
    r"\b(?:camera|event_recording|event_vision|sensor|switch|binary_sensor)\.[a-z0-9_]+\b",
    re.IGNORECASE,
)


def verify_privacy_clean(text: str) -> None:
    """Verify that no private URLs, local file paths, or HA entity IDs leak into provider requests."""
    if _PRIVATE_URL_PATTERN.search(text):
        raise AnalysisError(
            "PRIVACY_LEAK_PREVENTED", "Disallowed URL found in provider request", retryable=False
        )
    if _PRIVATE_PATH_PATTERN.search(text):
        raise AnalysisError(
            "PRIVACY_LEAK_PREVENTED",
            "Disallowed local file path found in provider request",
            retryable=False,
        )
    if _PRIVATE_ENTITY_PATTERN.search(text):
        raise AnalysisError(
            "PRIVACY_LEAK_PREVENTED",
            "Disallowed HA entity identifier found in provider request",
            retryable=False,
        )


class GeminiClient:
    """Direct Google Gemini REST v1beta generateContent client."""

    def __init__(
        self,
        api_key: str,
        model: str = DEFAULT_MODEL,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        if not api_key or not api_key.strip():
            raise GeminiKeyError("INVALID_KEY", "Gemini API key is required and cannot be empty.")
        self._api_key = api_key.strip()
        self._model = model.strip() or DEFAULT_MODEL
        self._session = session
        self._own_session = session is None

    @property
    def api_key(self) -> str:
        """Return configured API key."""
        return self._api_key

    @property
    def model(self) -> str:
        """Return configured model identifier."""
        return self._model

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
        return self._session

    async def close(self) -> None:
        """Close client session only if owned."""
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    def _build_url(self, model: str | None = None) -> str:
        """Construct sanitized endpoint URL strictly on generativelanguage.googleapis.com."""
        target_model = model.strip() if model and model.strip() else self._model
        encoded_model = urllib.parse.quote(target_model, safe="")
        url = f"https://{GEMINI_HOST}/v1beta/models/{encoded_model}:generateContent"
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc != GEMINI_HOST:
            raise AnalysisError(
                "SECURITY_ERROR", "Invalid host in Gemini URL construction.", retryable=False
            )
        return url

    async def generate_analysis(
        self,
        frames: list[ExtractedFrame],
        prompt_instructions: str | None = None,
        timeout_seconds: float = 60.0,
        model: str | None = None,
        base_prompt: str | None = None,
    ) -> AnalysisResult:
        """Call Gemini REST generateContent with media frames and structured JSON output."""
        if not frames:
            raise AnalysisError("NO_FRAMES", "No frames provided for analysis.", retryable=False)

        # Build prompt: generic media description or overridden base prompt, optionally augmented with user instructions
        effective_base = (base_prompt or GENERIC_MEDIA_PROMPT).strip()
        if prompt_instructions and prompt_instructions.strip():
            clean_instructions = prompt_instructions.strip()[:MAX_PROMPT_INSTRUCTIONS_LEN]
            prompt_text = f"{effective_base}\nAdditional Instructions: {clean_instructions}"
        else:
            prompt_text = effective_base

        parts: list[dict[str, Any]] = [{"text": prompt_text}]
        for frame in frames:
            b64_data = base64.b64encode(frame.data).decode("ascii")
            parts.append(
                {
                    "inline_data": {
                        "mime_type": frame.mime_type,
                        "data": b64_data,
                    }
                }
            )

        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "OBJECT",
                    "properties": {
                        "summary": {"type": "STRING"},
                        "activity": {"type": "BOOLEAN"},
                        "objects": {
                            "type": "ARRAY",
                            "items": {"type": "STRING"},
                        },
                    },
                    "required": ["summary", "activity"],
                },
            },
        }

        raw_json = await self._request(payload, timeout_seconds, model)
        return self._parse_and_validate_response(raw_json)

    async def _request(
        self, payload: dict[str, Any], timeout_seconds: float, model: str | None
    ) -> dict[str, Any]:
        """Shared authenticated transport, error classification, and usage logging."""
        url = self._build_url(model=model)
        session = await self._get_session()
        headers = {"Content-Type": "application/json", "x-goog-api-key": self._api_key}
        _LOGGER.debug(
            "Dispatching Gemini generateContent request: model=%s, timeout=%.1fs",
            model or self._model,
            timeout_seconds,
        )
        start_time = time.monotonic()
        try:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as resp:
                latency = time.monotonic() - start_time
                status = resp.status
                if status == 200:
                    raw_json = await resp.json()
                    usage = raw_json.get("usageMetadata", {}) if isinstance(raw_json, dict) else {}
                    _LOGGER.debug(
                        "Gemini response 200 in %.2fs: prompt_tokens=%s, candidates_tokens=%s, total_tokens=%s",
                        latency,
                        usage.get("promptTokenCount"),
                        usage.get("candidatesTokenCount"),
                        usage.get("totalTokenCount"),
                    )
                    return raw_json

                retry_after_raw = resp.headers.get("Retry-After")
                retry_delay = parse_retry_after(retry_after_raw)
                _LOGGER.debug(
                    "Gemini error response status=%d in %.2fs (retry_delay=%s)",
                    status,
                    latency,
                    retry_delay,
                )

                if status == 429:
                    raise AnalysisError(
                        "RATE_LIMIT",
                        "Gemini 429 rate limit exceeded",
                        retryable=True,
                        retry_delay=retry_delay,
                    )

                if status in (500, 502, 503, 504):
                    raise AnalysisError(
                        f"HTTP_{status}",
                        f"Gemini server error {status}",
                        retryable=True,
                        retry_delay=retry_delay,
                    )

                if status in (400, 401, 403):
                    raise AnalysisError(
                        f"HTTP_{status}",
                        f"Gemini client authentication error {status}",
                        retryable=False,
                    )

                raise AnalysisError(
                    f"HTTP_{status}",
                    f"Unexpected status from Gemini: {status}",
                    retryable=False,
                )

        except TimeoutError as err:
            raise AnalysisError(
                "TIMEOUT", "Gemini request timed out", retryable=True, timeout_source="provider"
            ) from err
        except aiohttp.ClientError as err:
            raise AnalysisError(
                "NETWORK_ERROR", "Network error connecting to Gemini", retryable=True
            ) from err

    def _parse_and_validate_response(self, raw_resp: dict[str, Any]) -> AnalysisResult:
        """Validate structure and fields of the Gemini JSON response."""
        try:
            model_version = raw_resp.get("modelVersion")
            candidates = raw_resp.get("candidates", [])
            if not candidates:
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "No candidates in Gemini response", retryable=False
                )

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if not parts:
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "No parts in candidate content", retryable=False
                )

            text_str = parts[0].get("text", "")
            if not text_str or not text_str.strip():
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "Empty text in candidate parts", retryable=False
                )

            data = json.loads(text_str)
            if not isinstance(data, dict):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE", "Model response is not a JSON object", retryable=False
                )

            summary = data.get("summary")
            activity = data.get("activity")
            raw_objects = data.get("objects", [])

            if not isinstance(summary, str) or not (1 <= len(summary.strip()) <= MAX_SUMMARY_LEN):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE",
                    f"Summary must be string 1..{MAX_SUMMARY_LEN} chars",
                    retryable=False,
                )

            if not isinstance(activity, bool):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE", "activity must be a boolean", retryable=False
                )

            objects: list[str] = []
            if isinstance(raw_objects, list):
                for obj in raw_objects[:MAX_OBJECTS]:
                    if isinstance(obj, str) and obj.strip():
                        objects.append(obj.strip()[:MAX_OBJECT_LEN])

            return AnalysisResult(
                summary=summary.strip(),
                activity=activity,
                objects=objects,
                model_version=str(model_version).strip() if model_version else None,
            )
        except json.JSONDecodeError as err:
            raise GeminiSchemaError(
                "MALFORMED_RESPONSE", f"Malformed JSON from model: {err}", retryable=False
            ) from err
        except GeminiSchemaError:
            raise
        except Exception as err:
            raise GeminiSchemaError(
                "SCHEMA_ERROR", f"Error validating schema: {err}", retryable=False
            ) from err

    async def generate_fusion_analysis(
        self,
        views: list[FusionViewInput],
        prompt_instructions: str | None = None,
        timeout_seconds: float = 120.0,
        model: str | None = None,
        fusion_prompt: str | None = None,
    ) -> FusionResult:
        """Call Gemini REST generateContent with multi-camera frames and return structured FusionResult."""
        if not views or len(views) < 2:
            raise AnalysisError(
                "INSUFFICIENT_VIEWS",
                "At least two distinct camera views are required for visual fusion.",
                retryable=False,
            )

        valid_refs: set[str] = set()
        total_frames = 0
        for view in views:
            if not view.frames:
                raise AnalysisError(
                    "NO_FRAMES_IN_VIEW",
                    f"View '{view.view_label}' has no extracted frames.",
                    retryable=False,
                )
            valid_refs.add(view.view_label)
            for frame in view.frames:
                valid_refs.add(frame.frame_ref)
                total_frames += 1

        if total_frames == 0:
            raise AnalysisError(
                "NO_FRAMES",
                "No frames provided across views for fusion analysis.",
                retryable=False,
            )

        effective_base = (fusion_prompt or DEFAULT_FUSION_PROMPT).strip()
        verify_privacy_clean(effective_base)

        prompt_lines = [effective_base]
        if prompt_instructions and prompt_instructions.strip():
            clean_instructions = prompt_instructions.strip()[:MAX_FUSION_PROMPT_INSTRUCTIONS_LEN]
            verify_privacy_clean(clean_instructions)
            prompt_lines.append(f"Additional Instructions: {clean_instructions}")

        prompt_lines.append("\nCamera Views Included:")
        for view in views:
            t_qual = view.time_quality or "unknown"
            obs_str = ""
            if view.observations and view.observations.strip():
                clean_obs = view.observations.strip()
                verify_privacy_clean(clean_obs)
                obs_str = f" | Prior observations: {clean_obs}"
            prompt_lines.append(f"- {view.view_label} (Time quality: {t_qual}){obs_str}")

        prompt_text = "\n".join(prompt_lines)
        parts: list[dict[str, Any]] = [{"text": prompt_text}]

        for view in views:
            parts.append({"text": f"=== Start frames for {view.view_label} ==="})
            for frame in view.frames:
                parts.append(
                    {
                        "text": (
                            f"Frame Reference: {frame.frame_ref} "
                            f"(offset: {frame.offset_seconds:.1f}s, quality: {frame.time_quality})"
                        )
                    }
                )
                b64_data = base64.b64encode(frame.data).decode("ascii")
                parts.append(
                    {
                        "inline_data": {
                            "mime_type": frame.mime_type,
                            "data": b64_data,
                        }
                    }
                )
            parts.append({"text": f"=== End frames for {view.view_label} ==="})

        payload = {
            "contents": [{"role": "user", "parts": parts}],
            "generationConfig": {
                "response_mime_type": "application/json",
                "response_schema": {
                    "type": "OBJECT",
                    "properties": {
                        "narrative": {"type": "STRING"},
                        "activity": {"type": "BOOLEAN"},
                        "possible_cross_view_matches": {
                            "type": "ARRAY",
                            "items": {
                                "type": "OBJECT",
                                "properties": {
                                    "description": {"type": "STRING"},
                                    "evidence_refs": {
                                        "type": "ARRAY",
                                        "items": {"type": "STRING"},
                                    },
                                    "uncertainty": {"type": "STRING"},
                                    "same_subject_confidence": {"type": "STRING"},
                                },
                                "required": [
                                    "description",
                                    "evidence_refs",
                                    "uncertainty",
                                    "same_subject_confidence",
                                ],
                            },
                        },
                    },
                    "required": ["narrative", "activity", "possible_cross_view_matches"],
                },
            },
        }

        raw_json = await self._request(payload, timeout_seconds, model)
        return self._parse_and_validate_fusion_response(raw_json, valid_refs=valid_refs)

    def _parse_and_validate_fusion_response(
        self,
        raw_resp: dict[str, Any],
        valid_refs: set[str],
    ) -> FusionResult:
        """Validate structure and fields of the Gemini multi-camera fusion JSON response."""
        try:
            model_version = raw_resp.get("modelVersion") or self._model
            candidates = raw_resp.get("candidates", [])
            if not candidates:
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "No candidates in Gemini response", retryable=False
                )

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if not parts:
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "No parts in candidate content", retryable=False
                )

            text_str = parts[0].get("text", "")
            if not text_str or not text_str.strip():
                raise GeminiSchemaError(
                    "EMPTY_RESPONSE", "Empty text in candidate parts", retryable=False
                )

            data = json.loads(text_str)
            if not isinstance(data, dict):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE", "Model response is not a JSON object", retryable=False
                )

            narrative = data.get("narrative")
            activity = data.get("activity")
            raw_matches = data.get("possible_cross_view_matches", [])

            if not isinstance(narrative, str) or not (
                1 <= len(narrative.strip()) <= MAX_SUMMARY_LEN * 2
            ):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE",
                    f"Narrative must be string 1..{MAX_SUMMARY_LEN * 2} chars",
                    retryable=False,
                )

            if not isinstance(activity, bool):
                raise GeminiSchemaError(
                    "MALFORMED_RESPONSE", "activity must be a boolean", retryable=False
                )

            matches: list[dict[str, Any]] = []
            if isinstance(raw_matches, list):
                for match in raw_matches[:20]:
                    if not isinstance(match, dict):
                        continue
                    desc = str(match.get("description", "")).strip()[:500]
                    if not desc:
                        continue
                    raw_refs = match.get("evidence_refs", [])
                    if not isinstance(raw_refs, list):
                        raw_refs = []

                    evidence_refs: list[str] = []
                    for r in raw_refs:
                        ref_str = str(r).strip()
                        if not ref_str:
                            continue
                        if ref_str not in valid_refs:
                            raise GeminiSchemaError(
                                "INVALID_FUSION_REFERENCES",
                                f"Model returned reference '{ref_str}' which was not provided in request frames/views",
                                retryable=False,
                            )
                        evidence_refs.append(ref_str)

                    uncertainty = str(match.get("uncertainty", "medium")).lower().strip()
                    if uncertainty not in ("low", "medium", "high"):
                        uncertainty = "medium"

                    confidence = (
                        str(match.get("same_subject_confidence", "uncertain")).lower().strip()
                    )
                    if confidence not in ("unlikely", "possible", "uncertain"):
                        confidence = "uncertain"

                    matches.append(
                        {
                            "description": desc,
                            "evidence_refs": evidence_refs,
                            "uncertainty": uncertainty,
                            "same_subject_confidence": confidence,
                        }
                    )

            return FusionResult(
                narrative=narrative.strip(),
                activity=activity,
                possible_cross_view_matches=matches,
                model_version=str(model_version).strip(),
            )
        except json.JSONDecodeError as err:
            raise GeminiSchemaError(
                "MALFORMED_RESPONSE", f"Malformed JSON from model: {err}", retryable=False
            ) from err
        except (GeminiSchemaError, AnalysisError):
            raise
        except Exception as err:
            raise GeminiSchemaError(
                "SCHEMA_ERROR", f"Error validating fusion schema: {err}", retryable=False
            ) from err
