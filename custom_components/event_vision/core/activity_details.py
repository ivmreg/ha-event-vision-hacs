"""Pure optional activity-details parsing and normalization (contract v1).

Provider output is untrusted. Every field is validated independently so a bad
optional field can never invalidate a valid core summary/activity response.
All media identifiers, offsets, and timing quality in the persisted shape are
resolved from frames and media already held locally.
"""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

ACTIVITY_DETAILS_VERSION = 1
MAX_ACTIVITY_TITLE_LEN = 80
MAX_MOMENT_LABEL_LEN = 80
MAX_MOMENTS = 4
MAX_MEDIA_ID_LEN = 128

VALID_ACTIVITY_TYPES = frozenset(
    {"delivery", "person", "car", "animal", "child", "other", "unknown"}
)
VALID_DURATION_BANDS = frozenset({"short", "medium", "long", "unknown"})
_TIMING_QUALITIES = frozenset({"exact", "estimated"})
_ALLOWED_CONTINUITIES = frozenset(
    {"verified_segments", "contiguous", "continuous", "provider_verified"}
)
#: Capture states whose media can still carry a trustworthy intra-clip window.
#: ``partial`` only means the requested window was not fully covered; the
#: duration evidence itself is checked separately (observation, continuity, gaps).
_TRUSTED_CAPTURE_STATES = frozenset({"ready", "partial"})
#: Coverage gaps below this are segment/frame-boundary noise. The recorder
#: tolerates sub-second shortfalls itself and the card treats under 2s as timing
#: noise (frontend/src/coverage.js), so a healthy recording must not lose its
#: duration estimate over one.
MAX_TOLERATED_COVERAGE_GAP_SECONDS = 2.0
_UNKNOWN_DURATION: dict[str, Any] = {"band": "unknown", "estimated": False}

ACTIVITY_DETAILS_PROMPT = """Optionally add activity_details for visible activity: a factual
short title (80 characters maximum), one type from delivery/person/car/animal/child/other/unknown,
and up to four moments with a short label and a supplied frame_ref such as F2.
Use delivery or child only when clearly supported; otherwise person or unknown. Do not identify people.
Only describe visible facts. Never invent arrivals, departures, activity endings, times or media IDs.
For one unambiguous episode you may return window with single_episode=true and frame references
first_active, last_active and after_inactive, only when all three observations exist in the same
media group. Add before_inactive only when a supplied frame shows the scene before the activity
began; omit it when the activity is already in progress in the first frame. Omit window if
boundaries, continuity, or episode count are uncertain.
Media groups are separate recordings; their concatenation does not imply chronological continuity.
Do not estimate seconds or substitute recording length. Omit unsupported fields. activity=false
must not include positive activity_details. Existing summary and activity remain required."""


def activity_details_schema() -> dict[str, Any]:
    """Bound optional output in the existing provider request; no additional call."""
    # String lengths are validated locally; provider decoding does not enforce them.
    text = {"type": "STRING"}
    return {
        "type": "OBJECT",
        "properties": {
            "title": dict(text),
            "type": {"type": "STRING", "enum": sorted(VALID_ACTIVITY_TYPES)},
            "window": {
                "type": "OBJECT",
                "properties": {
                    "single_episode": {"type": "BOOLEAN"},
                    **{
                        name: {"type": "STRING"}
                        for name in (
                            "before_inactive",
                            "first_active",
                            "last_active",
                            "after_inactive",
                        )
                    },
                },
            },
            "moments": {
                "type": "ARRAY",
                "maxItems": MAX_MOMENTS,
                "items": {
                    "type": "OBJECT",
                    "properties": {"label": dict(text), "frame_ref": {"type": "STRING"}},
                },
            },
        },
    }


@dataclass(frozen=True)
class ActivityWindowRefs:
    """Locally resolvable frame labels for one candidate activity window."""

    first_active: str
    last_active: str
    after_inactive: str
    before_inactive: str | None = None


@dataclass(frozen=True)
class ActivityMomentRef:
    """A provider-selected moment expressed only as a frame label."""

    label: str
    frame_ref: str


@dataclass(frozen=True)
class ActivityDetailsRaw:
    """Forgivingly parsed provider metadata before local evidence resolution."""

    title: str | None = None
    type: str | None = None
    window: ActivityWindowRefs | None = None
    moments: tuple[ActivityMomentRef, ...] = ()


def _clean_text(value: Any, max_len: int) -> str | None:
    if not isinstance(value, str):
        return None
    clean = value.strip()
    if not clean or len(clean) > max_len:
        return None
    return clean


def _clean_ref(value: Any, valid_refs: set[str] | None) -> str | None:
    if not isinstance(value, str):
        return None
    clean = value.strip()
    if not clean or valid_refs is None or clean not in valid_refs:
        return None
    return clean


def _finite_float(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(result):
        return None
    return result


def parse_activity_details(
    raw: Any, valid_refs: set[str] | None = None
) -> ActivityDetailsRaw | None:
    """Forgivingly parse optional provider metadata without raising.

    Invalid fields and references are dropped independently. Returns None when
    nothing usable remains or the version is unsupported.
    """
    try:
        if not isinstance(raw, Mapping):
            return None
        version = raw.get("version", ACTIVITY_DETAILS_VERSION)
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != ACTIVITY_DETAILS_VERSION
        ):
            return None

        title = _clean_text(raw.get("title"), MAX_ACTIVITY_TITLE_LEN)

        raw_type = raw.get("type")
        type_value = raw_type.strip().lower() if isinstance(raw_type, str) else None
        if type_value not in VALID_ACTIVITY_TYPES:
            type_value = None

        window: ActivityWindowRefs | None = None
        raw_window = raw.get("window")
        if isinstance(raw_window, Mapping) and raw_window.get("single_episode") is True:
            before = _clean_ref(raw_window.get("before_inactive"), valid_refs)
            first = _clean_ref(raw_window.get("first_active"), valid_refs)
            last = _clean_ref(raw_window.get("last_active"), valid_refs)
            after = _clean_ref(raw_window.get("after_inactive"), valid_refs)
            # before_inactive is optional: a recording that starts with the
            # activity already in progress has no inactive frame before it.
            if all((first, last, after)):
                window = ActivityWindowRefs(
                    first_active=first,
                    last_active=last,
                    after_inactive=after,
                    before_inactive=before,
                )

        moments: list[ActivityMomentRef] = []
        raw_moments = raw.get("moments")
        if isinstance(raw_moments, list):
            seen: set[tuple[str, str]] = set()
            for item in raw_moments:
                if len(moments) >= MAX_MOMENTS:
                    break
                if not isinstance(item, Mapping):
                    continue
                label = _clean_text(item.get("label"), MAX_MOMENT_LABEL_LEN)
                ref = _clean_ref(item.get("frame_ref"), valid_refs)
                if label is None or ref is None:
                    continue
                key = (label, ref)
                if key in seen:
                    continue
                seen.add(key)
                moments.append(ActivityMomentRef(label=label, frame_ref=ref))

        if title is None and type_value is None and window is None and not moments:
            return None
        return ActivityDetailsRaw(
            title=title,
            type=type_value,
            window=window,
            moments=tuple(moments),
        )
    except Exception:
        return None


def _sanitize_persisted_duration(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        return dict(_UNKNOWN_DURATION)
    band = raw.get("band")
    if not isinstance(band, str) or band not in VALID_DURATION_BANDS:
        return dict(_UNKNOWN_DURATION)
    if band == "unknown":
        return dict(_UNKNOWN_DURATION)
    if raw.get("estimated") is not True:
        return dict(_UNKNOWN_DURATION)

    evidence = raw.get("evidence")
    if not isinstance(evidence, Mapping):
        return dict(_UNKNOWN_DURATION)
    media_id = _clean_text(evidence.get("media_id"), MAX_MEDIA_ID_LEN)
    offsets = [
        _finite_float(evidence.get(key))
        for key in (
            "before_inactive_offset",
            "first_active_offset",
            "last_active_offset",
            "after_inactive_offset",
        )
    ]
    before, first, last, after = offsets
    if media_id is None or any(value is None for value in offsets):
        return dict(_UNKNOWN_DURATION)
    assert before is not None and first is not None and last is not None and after is not None
    # A persisted 0.0 before-offset means "activity already in progress when the
    # footage starts", so it may coincide with the first active offset.
    if not (0.0 <= before <= first <= last < after):
        return dict(_UNKNOWN_DURATION)
    lower, upper = last - first, after - before
    expected_band = (
        "short"
        if upper < 30
        else "medium"
        if lower >= 30 and upper <= 60
        else "long"
        if lower > 60
        else "unknown"
    )
    if band != expected_band:
        return dict(_UNKNOWN_DURATION)
    return {
        "band": band,
        "estimated": True,
        "evidence": {
            "media_id": media_id,
            "before_inactive_offset": before,
            "first_active_offset": first,
            "last_active_offset": last,
            "after_inactive_offset": after,
        },
    }


def _sanitize_persisted_moment(raw: Any) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    label = _clean_text(raw.get("label"), MAX_MOMENT_LABEL_LEN)
    media_id = _clean_text(raw.get("media_id"), MAX_MEDIA_ID_LEN)
    offset = _finite_float(raw.get("offset_seconds"))
    quality = raw.get("timestamp_quality")
    if (
        label is None
        or media_id is None
        or offset is None
        or offset < 0.0
        or not isinstance(quality, str)
        or quality not in _TIMING_QUALITIES
    ):
        return None
    return {
        "label": label,
        "media_id": media_id,
        "offset_seconds": offset,
        "timestamp_quality": quality,
    }


def sanitize_activity_details(raw: Any) -> dict[str, Any] | None:
    """Sanitize a persisted/public activity_details extension without raising."""
    try:
        if not isinstance(raw, Mapping):
            return None
        version = raw.get("version")
        if (
            isinstance(version, bool)
            or not isinstance(version, int)
            or version != ACTIVITY_DETAILS_VERSION
        ):
            return None

        result: dict[str, Any] = {"version": ACTIVITY_DETAILS_VERSION}
        title = _clean_text(raw.get("title"), MAX_ACTIVITY_TITLE_LEN)
        if title is not None:
            result["title"] = title
        raw_type = raw.get("type")
        type_value = raw_type.strip().lower() if isinstance(raw_type, str) else None
        if type_value in VALID_ACTIVITY_TYPES:
            result["type"] = type_value

        result["duration"] = _sanitize_persisted_duration(raw.get("duration"))

        moments: list[dict[str, Any]] = []
        raw_moments = raw.get("moments")
        if isinstance(raw_moments, list):
            seen: set[tuple[str, str, float]] = set()
            for item in raw_moments:
                if len(moments) >= MAX_MOMENTS:
                    break
                clean = _sanitize_persisted_moment(item)
                if clean is None:
                    continue
                key = (
                    clean["label"],
                    clean["media_id"],
                    round(clean["offset_seconds"], 6),
                )
                if key in seen:
                    continue
                seen.add(key)
                moments.append(clean)
        if moments:
            result["moments"] = moments
        return result
    except Exception:
        return None


def _attr(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _is_video_media(media: Any) -> bool:
    kind = _attr(media, "kind")
    mime = _attr(media, "mime_type")
    if kind != "video":
        return False
    if isinstance(mime, str) and mime.lower() in ("image/jpeg", "image/png", "image/jpg"):
        return False
    return True


def _frame_offset(frame: Any, duration: float | None) -> float | None:
    offset = _finite_float(_attr(frame, "offset_seconds"))
    if offset is None or offset < 0.0:
        return None
    if duration is not None and offset > duration:
        return None
    return offset


def _coverage_supports_duration(coverage: Any) -> bool:
    if coverage is None:
        return False
    return (
        _attr(coverage, "observation_state") == "complete"
        and _attr(coverage, "extracted_frames") == _attr(coverage, "planned_frames")
        and _attr(coverage, "missing_clips") == 0
        and _attr(coverage, "unselected_clips") == 0
    )


def _duration_band(
    window: ActivityWindowRefs,
    frame_by_ref: dict[str, Any],
    media_by_id: dict[str, Any],
    coverage: Any,
    capture_state: str | None,
) -> dict[str, Any]:
    """Derive a duration band from resolved frame offsets.

    ``before_inactive`` is optional. When the recording starts with the activity
    already in progress there is no inactive frame to prove when it began, so the
    first footage frame (offset 0) is used as the earliest bound. The band then
    only claims what the footage actually shows: the activity had ended by
    ``after_inactive`` and the observed episode is bounded by the clip start.
    """
    unknown = dict(_UNKNOWN_DURATION)
    if not _coverage_supports_duration(coverage) or capture_state not in _TRUSTED_CAPTURE_STATES:
        return unknown

    first = frame_by_ref.get(window.first_active)
    last = frame_by_ref.get(window.last_active)
    after = frame_by_ref.get(window.after_inactive)
    before = (
        frame_by_ref.get(window.before_inactive)
        if window.before_inactive is not None
        else None
    )
    if first is None or last is None or after is None:
        return unknown
    if window.before_inactive is not None and before is None:
        return unknown

    frames = [frame for frame in (before, first, last, after) if frame is not None]
    media_ids = {_attr(frame, "media_id") for frame in frames}
    if None in media_ids or len(media_ids) != 1:
        return unknown
    media_id = next(iter(media_ids))
    assert isinstance(media_id, str)
    media = media_by_id.get(media_id)
    if media is None or not _attr(media, "available", True) or not _is_video_media(media):
        return unknown
    continuity = _attr(media, "continuity")
    if continuity not in _ALLOWED_CONTINUITIES:
        return unknown

    duration = _finite_float(_attr(media, "duration"))
    if duration is None or duration <= 0.0:
        return unknown

    offsets: dict[str, float] = {}
    for name, frame in (
        ("before", before),
        ("first", first),
        ("last", last),
        ("after", after),
    ):
        if frame is None:
            continue
        if _attr(frame, "timestamp_quality") not in _TIMING_QUALITIES:
            return unknown
        if _attr(frame, "offset_quality") not in _TIMING_QUALITIES:
            return unknown
        offset = _frame_offset(frame, duration)
        if offset is None:
            return unknown
        offsets[name] = offset

    before_offset = offsets.get("before", 0.0)
    first_offset = offsets["first"]
    last_offset = offsets["last"]
    after_offset = offsets["after"]
    if "before" in offsets:
        if not (before_offset < first_offset <= last_offset < after_offset):
            return unknown
    elif not (0.0 <= first_offset <= last_offset < after_offset):
        return unknown

    lower = last_offset - first_offset
    upper = after_offset - before_offset
    if upper < 30.0:
        band = "short"
    elif lower >= 30.0 and upper <= 60.0:
        band = "medium"
    elif lower > 60.0:
        band = "long"
    else:
        return unknown

    return {
        "band": band,
        "estimated": True,
        "evidence": {
            "media_id": media_id,
            "before_inactive_offset": before_offset,
            "first_active_offset": first_offset,
            "last_active_offset": last_offset,
            "after_inactive_offset": after_offset,
        },
    }


def _resolve_moments(
    raw_moments: Sequence[ActivityMomentRef],
    frame_by_ref: dict[str, Any],
    media_by_id: dict[str, Any],
    media_order: dict[str, int],
) -> list[dict[str, Any]]:
    moments: list[dict[str, Any]] = []
    seen: set[tuple[str, str, float]] = set()
    for raw in raw_moments:
        if len(moments) >= MAX_MOMENTS:
            break
        frame = frame_by_ref.get(raw.frame_ref)
        if frame is None:
            continue
        media_id = _attr(frame, "media_id")
        if not isinstance(media_id, str):
            continue
        media = media_by_id.get(media_id)
        if media is None or not _attr(media, "available", True) or not _is_video_media(media):
            continue
        duration = _finite_float(_attr(media, "duration"))
        if duration is None or duration <= 0.0:
            continue
        offset = _frame_offset(frame, duration)
        if offset is None:
            continue
        if (
            _attr(frame, "timestamp_quality") not in _TIMING_QUALITIES
            or _attr(frame, "offset_quality") not in _TIMING_QUALITIES
        ):
            continue
        quality = (
            "exact"
            if _attr(frame, "timestamp_quality") == "exact"
            and _attr(frame, "offset_quality") == "exact"
            else "estimated"
        )
        key = (raw.label, media_id, round(offset, 6))
        if key in seen:
            continue
        seen.add(key)
        moments.append(
            {
                "label": raw.label,
                "media_id": media_id,
                "offset_seconds": offset,
                "timestamp_quality": quality,
            }
        )

    # Stable local-media order then local offset; never infer cross-media alignment.
    moments.sort(
        key=lambda m: (
            media_order.get(m["media_id"], len(media_order)),
            m["offset_seconds"],
            m["label"],
        )
    )
    return moments[:MAX_MOMENTS]


def build_activity_details(
    raw: ActivityDetailsRaw | None,
    frames: Sequence[Any],
    media_items: Sequence[Any],
    coverage: Any,
    *,
    capture_state: str | None = None,
    capture_error: str | None = None,
    event_coverage: Any = None,
    version: int = 0,
) -> dict[str, Any] | None:
    """Resolve optional provider metadata to the persisted v1 contract shape.

    Returns None when enrichment is disabled/legacy or nothing usable remains.
    Never raises into the core analysis path.
    """
    try:
        if version != ACTIVITY_DETAILS_VERSION or raw is None:
            return None
        frame_by_ref = {f"F{index + 1}": frame for index, frame in enumerate(frames)}
        media_by_id: dict[str, Any] = {}
        media_order: dict[str, int] = {}
        for index, media in enumerate(media_items):
            media_id = _attr(media, "id")
            if isinstance(media_id, str) and media_id not in media_by_id:
                media_by_id[media_id] = media
                media_order[media_id] = index

        result: dict[str, Any] = {"version": ACTIVITY_DETAILS_VERSION}
        if raw.title is not None:
            result["title"] = raw.title
        if raw.type is not None:
            result["type"] = raw.type

        gap_free = True
        if isinstance(event_coverage, Mapping):
            for key in ("gaps", "gap_seconds"):
                if key not in event_coverage:
                    continue
                gap = _finite_float(event_coverage.get(key))
                # A missing measurement stays unknown; only a real gap of two
                # seconds or more disqualifies the window.
                if gap is None or gap >= MAX_TOLERATED_COVERAGE_GAP_SECONDS:
                    gap_free = False
                    break
        if raw.window is not None and not capture_error and gap_free:
            result["duration"] = _duration_band(
                raw.window, frame_by_ref, media_by_id, coverage, capture_state
            )
        else:
            result["duration"] = dict(_UNKNOWN_DURATION)

        moments = _resolve_moments(raw.moments, frame_by_ref, media_by_id, media_order)
        if moments:
            result["moments"] = moments

        return result
    except Exception:
        return None
