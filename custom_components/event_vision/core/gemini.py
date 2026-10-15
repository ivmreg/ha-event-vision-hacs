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
import json
import logging
import urllib.parse
from typing import Any

import aiohttp

from ..const import (
    DEFAULT_MODEL,
    MAX_OBJECT_LEN,
    MAX_OBJECTS,
    MAX_PROMPT_INSTRUCTIONS_LEN,
    MAX_SUMMARY_LEN,
)
from .models import (
    AnalysisError,
    AnalysisResult,
    ExtractedFrame,
    GeminiKeyError,
    GeminiSchemaError,
)

_LOGGER = logging.getLogger(__name__)

GEMINI_HOST = "generativelanguage.googleapis.com"

GENERIC_MEDIA_PROMPT = (
    "Describe the visual activity observed in the provided media frames. "
    "Provide a factual, concise summary of visible motion, subjects, or physical state changes. "
    "Do not speculate on unobserved facts. Output strictly valid JSON."
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

    def _build_url(self) -> str:
        """Construct sanitized endpoint URL strictly on generativelanguage.googleapis.com."""
        encoded_model = urllib.parse.quote(self._model, safe="")
        url = f"https://{GEMINI_HOST}/v1beta/models/{encoded_model}:generateContent"
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc != GEMINI_HOST:
            raise AnalysisError("SECURITY_ERROR", "Invalid host in Gemini URL construction.", retryable=False)
        return url

    async def generate_analysis(
        self,
        frames: list[ExtractedFrame],
        prompt_instructions: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> AnalysisResult:
        """Call Gemini REST generateContent with media frames and structured JSON output."""
        if not frames:
            raise AnalysisError("NO_FRAMES", "No frames provided for analysis.", retryable=False)

        url = self._build_url()
        session = await self._get_session()

        # Build prompt: generic media description, optionally augmented with user instructions
        prompt_text = GENERIC_MEDIA_PROMPT
        if prompt_instructions and prompt_instructions.strip():
            clean_instructions = prompt_instructions.strip()[:MAX_PROMPT_INSTRUCTIONS_LEN]
            prompt_text += f"\nAdditional Instructions: {clean_instructions}"

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

        # Header strictly carries key, NEVER URL parameter
        headers = {
            "Content-Type": "application/json",
            "x-goog-api-key": self._api_key,
        }

        try:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout_seconds),
            ) as resp:
                status = resp.status
                if status == 200:
                    raw_json = await resp.json()
                    return self._parse_and_validate_response(raw_json)

                if status == 429:
                    raise AnalysisError("RATE_LIMIT", "Gemini 429 rate limit exceeded", retryable=True)

                if status in (500, 502, 503, 504):
                    raise AnalysisError(f"HTTP_{status}", f"Gemini server error {status}", retryable=True)

                if status in (400, 401, 403):
                    raise AnalysisError(f"HTTP_{status}", f"Gemini client authentication error {status}", retryable=False)

                raise AnalysisError(f"HTTP_{status}", f"Unexpected status from Gemini: {status}", retryable=False)

        except TimeoutError as err:
            raise AnalysisError("TIMEOUT", "Gemini request timed out", retryable=True) from err
        except aiohttp.ClientError as err:
            raise AnalysisError("NETWORK_ERROR", "Network error connecting to Gemini", retryable=True) from err

    def _parse_and_validate_response(self, raw_resp: dict[str, Any]) -> AnalysisResult:
        """Validate structure and fields of the Gemini JSON response."""
        try:
            candidates = raw_resp.get("candidates", [])
            if not candidates:
                raise GeminiSchemaError("EMPTY_RESPONSE", "No candidates in Gemini response", retryable=False)

            content = candidates[0].get("content", {})
            parts = content.get("parts", [])
            if not parts:
                raise GeminiSchemaError("EMPTY_RESPONSE", "No parts in candidate content", retryable=False)

            text_str = parts[0].get("text", "")
            if not text_str or not text_str.strip():
                raise GeminiSchemaError("EMPTY_RESPONSE", "Empty text in candidate parts", retryable=False)

            data = json.loads(text_str)
            if not isinstance(data, dict):
                raise GeminiSchemaError("MALFORMED_RESPONSE", "Model response is not a JSON object", retryable=False)

            summary = data.get("summary")
            activity = data.get("activity")
            raw_objects = data.get("objects", [])

            if not isinstance(summary, str) or not (1 <= len(summary.strip()) <= MAX_SUMMARY_LEN):
                raise GeminiSchemaError("MALFORMED_RESPONSE", f"Summary must be string 1..{MAX_SUMMARY_LEN} chars", retryable=False)

            if not isinstance(activity, bool):
                raise GeminiSchemaError("MALFORMED_RESPONSE", "activity must be a boolean", retryable=False)

            objects: list[str] = []
            if isinstance(raw_objects, list):
                for obj in raw_objects[:MAX_OBJECTS]:
                    if isinstance(obj, str) and obj.strip():
                        objects.append(obj.strip()[:MAX_OBJECT_LEN])

            return AnalysisResult(
                summary=summary.strip(),
                activity=activity,
                objects=objects,
            )
        except json.JSONDecodeError as err:
            raise GeminiSchemaError("MALFORMED_RESPONSE", f"Malformed JSON from model: {err}", retryable=False) from err
        except GeminiSchemaError:
            raise
        except Exception as err:
            raise GeminiSchemaError("SCHEMA_ERROR", f"Error validating schema: {err}", retryable=False) from err
