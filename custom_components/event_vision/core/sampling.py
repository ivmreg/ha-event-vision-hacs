"""Sampling planner and anchor calculation for Event Vision.

Implements bounded configurable sampling policies:
- sparse_anchors: chronological evenly-spaced anchors across event media.
- trigger_relevant: focuses anchors around the source trigger time, with bounded
  relative coverage fallback for unknown UTC timing.

Preserves strict privacy:
- Returns per-frame media_id, offset_seconds, timestamp_quality, and offset_quality provenance.
- Zero entity names, raw event payloads, or signed URLs.
- Pure Python (zero Home Assistant dependencies).
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from ..const import (
    DEFAULT_MAX_FRAMES,
    DEFAULT_SAMPLING_POLICY,
    MAX_FRAMES_BUDGET,
    MIN_FRAMES,
    SAMPLING_POLICY_SPARSE_ANCHORS,
    SAMPLING_POLICY_TRIGGER_RELEVANT,
    VALID_SAMPLING_POLICIES,
)

#: Clock origins whose wall-clock timestamps may anchor a media item. The
#: recorder's producer-specific sources are host/provider observations, while
#: camera PTS and unknown origins stay untrusted.
_TRUSTED_CLOCK_SOURCES = frozenset(
    {"ntp", "ptp", "wall_clock", "ha_stream", "go2rtc", "frigate_nvr"}
)


@dataclass(frozen=True)
class PlannedAnchor:
    """Calculated sample anchor for an individual media item."""

    media_id: str
    offset_seconds: float
    timestamp_quality: str = "unknown"  # "exact" | "estimated" | "relative_fallback" | "unknown"
    offset_quality: str = "exact"  # "exact" | "estimated" | "relative_fallback" | "unknown"


def _ensure_utc(dt: datetime | str) -> datetime:
    """Ensure datetime has UTC timezone."""
    if isinstance(dt, str):
        dt_val = datetime.fromisoformat(dt)
    else:
        dt_val = dt
    if dt_val.tzinfo is None:
        return dt_val.replace(tzinfo=UTC)
    return dt_val.astimezone(UTC)


def _sanitize_duration(raw_dur: Any) -> tuple[float, bool]:
    """Sanitize duration value against non-finite, negative, or missing inputs."""
    if (
        raw_dur is not None
        and not isinstance(raw_dur, bool)
        and isinstance(raw_dur, (int, float))
        and math.isfinite(raw_dur)
        and raw_dur > 0.0
    ):
        return float(raw_dur), True
    return 1.0, False


def _qualify_media_clock(
    item: Any,
    event_start_at: datetime | None = None,
) -> tuple[str, datetime | None]:
    """Derive trusted capture start datetime and clock quality.

    Enforces that logical event.start_at + relative PTS is NOT actual capture UTC.
    Derives trigger offset only from qualified media.capture_start plus declared
    clock quality and uncertainty, or qualified explicit offset with known origin.
    - Declared time_quality "unknown"/"estimated"/"device_clock" is never
      upgraded to "exact"; recorder "host_recorded"/"provider_reported" means
      the producer observed the wall-clock interval, not that the device clock
      is calibrated.
    - None uncertainty is never treated as exact.
    - Camera relative PTS (clock_source == "camera_pts" or None) is never UTC.
    """
    capture_start = getattr(item, "capture_start", None)
    clock_source = getattr(item, "clock_source", None)
    uncertainty = getattr(item, "time_uncertainty_seconds", None)
    raw_quality = getattr(item, "time_quality", None)

    declared_quality: str | None = None
    if raw_quality is not None and not isinstance(raw_quality, bool):
        q_str = str(raw_quality).strip().lower()
        if q_str in ("exact", "host_recorded", "provider_reported"):
            declared_quality = "exact"
        elif q_str in ("estimated", "device_clock"):
            declared_quality = "estimated"
        else:
            declared_quality = "unknown"

    # Declared unknown quality fails closed immediately
    if declared_quality == "unknown":
        return "unknown", None

    # Validate uncertainty
    uncertainty_val: float | None = None
    if (
        uncertainty is not None
        and not isinstance(uncertainty, bool)
        and isinstance(uncertainty, (int, float))
        and math.isfinite(uncertainty)
    ):
        uncertainty_val = float(uncertainty)

    # Relative PTS never represents UTC
    if clock_source in ("camera_pts", "pts") or not clock_source:
        return "unknown", None

    # 1. Direct qualified capture_start
    if capture_start is not None and isinstance(capture_start, (datetime, str)):
        try:
            dt_start = _ensure_utc(capture_start)
            # Must declare a trusted clock source
            if clock_source in _TRUSTED_CLOCK_SOURCES:
                # Declared estimated quality must NEVER be upgraded to exact
                if declared_quality == "estimated":
                    return "estimated", dt_start
                # None uncertainty is never treated as exact
                if uncertainty_val is not None and 0.0 <= uncertainty_val < 1.0:
                    return "exact", dt_start
                return "estimated", dt_start
            return "unknown", None
        except (ValueError, TypeError, OverflowError):
            pass

    # 2. Qualified explicit offset with known trusted origin
    start_offset = getattr(item, "start_offset_seconds", None)
    if (
        start_offset is not None
        and not isinstance(start_offset, bool)
        and isinstance(start_offset, (int, float))
        and math.isfinite(start_offset)
        and event_start_at is not None
        and clock_source in _TRUSTED_CLOCK_SOURCES
    ):
        try:
            dt_origin = _ensure_utc(event_start_at) + timedelta(seconds=float(start_offset))
            if declared_quality == "estimated":
                return "estimated", dt_origin
            if uncertainty_val is not None and 0.0 <= uncertainty_val < 1.0:
                return "exact", dt_origin
            return "estimated", dt_origin
        except (ValueError, TypeError, OverflowError):
            pass

    # Uncalibrated, camera_pts relative PTS, or unknown timing origin
    return "unknown", None


def _make_strictly_monotonic_anchors(
    raw_points: list[float],
    duration: float,
    budget: int,
) -> list[float]:
    """Ensure offsets are within [0.0, duration], strictly sorted ascending, and deduplicated.

    For tiny durations below rounded precision, yields fewer unique anchors rather than
    duplicate out-of-bounds or clamped timestamps. Budget is the maximum frame count,
    not a mandatory exact count for unrepresentable clips.
    """
    if budget <= 0 or duration <= 0.0:
        return []

    precision = 4 if duration < 0.1 else 3

    if budget == 1:
        pt = raw_points[0] if raw_points else duration / 2.0
        clamped_pt = round(max(0.0, min(duration, pt)), precision)
        return [clamped_pt]

    # Proportional candidate generation
    if duration <= 0.1 or len(raw_points) < budget:
        step = duration / (budget + 1)
        raw_candidates = [(i + 1) * step for i in range(budget)]
    else:
        raw_candidates = sorted(max(0.0, min(duration, p)) for p in raw_points)

    # Deduplicate and enforce strict monotonicity within [0.0, duration]
    unique_offsets: list[float] = []
    max_dur = round(duration, precision)

    for p in raw_candidates:
        rounded_p = round(max(0.0, min(duration, p)), precision)
        if not unique_offsets:
            if 0.0 <= rounded_p <= max_dur:
                unique_offsets.append(rounded_p)
        else:
            prev = unique_offsets[-1]
            if rounded_p > prev and rounded_p <= max_dur:
                unique_offsets.append(rounded_p)

    return unique_offsets[:budget]


def plan_event_sampling(
    media_items: list[Any],
    policy: str = DEFAULT_SAMPLING_POLICY,
    max_frames: int = DEFAULT_MAX_FRAMES,
    event_start_at: datetime | None = None,
    source_events: tuple[Any, ...] | list[Any] = (),
) -> list[PlannedAnchor]:
    """Plan frame extraction points across event media respecting budget and policy bounds."""
    if not media_items:
        return []

    # Enforce budget bounds [3, 16]
    budget = max(MIN_FRAMES, min(max_frames, MAX_FRAMES_BUDGET))

    # Validate policy
    active_policy = policy if policy in VALID_SAMPLING_POLICIES else SAMPLING_POLICY_SPARSE_ANCHORS

    # Filter to available media
    available = [m for m in media_items if getattr(m, "available", True)]
    if not available:
        return []

    num_media = len(available)

    # Multi-clip distribution:
    # Selects media items covering the event timeline (first, last, and intermediate clips).
    # Multi-camera view allocation will be handled by fusion in the next evolution stage.
    if num_media <= budget:
        selected_media = available
    else:
        anchor_indices = [
            round(i * (num_media - 1) / (budget - 1))
            for i in range(budget)
        ]
        seen_indices: set[int] = set()
        selected_indices: list[int] = []
        for idx in anchor_indices:
            if idx not in seen_indices:
                seen_indices.add(idx)
                selected_indices.append(idx)
        selected_media = [available[i] for i in selected_indices]

    # Distribute frame budget across selected media items:
    # Images receive 1 frame each; remaining budget is allocated across video items
    images = [
        m for m in selected_media
        if getattr(m, "kind", "video") == "image"
        or getattr(m, "mime_type", "") in ("image/jpeg", "image/png", "image/jpg")
    ]
    videos = [m for m in selected_media if m not in images]

    num_images = len(images)
    num_videos = len(videos)
    video_budget_total = max(num_videos, budget - num_images) if num_videos > 0 else 0

    video_budgets: dict[str, int] = {}
    if num_videos > 0:
        base_v = video_budget_total // num_videos
        rem_v = video_budget_total % num_videos
        for i, v in enumerate(videos):
            extra = 1 if i < rem_v else 0
            video_budgets[getattr(v, "id", str(i))] = max(1, base_v + extra)

    media_budgets: list[int] = []
    for item in selected_media:
        m_id = getattr(item, "id", "")
        if item in images:
            media_budgets.append(1)
        else:
            media_budgets.append(video_budgets.get(m_id, 1))

    planned_anchors: list[PlannedAnchor] = []

    for item, item_budget in zip(selected_media, media_budgets, strict=False):
        m_id = getattr(item, "id", "")
        m_kind = getattr(item, "kind", "video")
        m_mime = getattr(item, "mime_type", "")

        is_image = (
            m_kind == "image"
            or m_mime in ("image/jpeg", "image/png", "image/jpg")
        )

        clock_quality, _ = _qualify_media_clock(item, event_start_at=event_start_at)

        if is_image:
            # Images yield exactly one frame at 0.0s
            planned_anchors.append(
                PlannedAnchor(
                    media_id=m_id,
                    offset_seconds=0.0,
                    timestamp_quality=clock_quality if clock_quality != "unknown" else "unknown",
                    offset_quality="exact",
                )
            )
            continue

        # Video media item
        raw_dur = getattr(item, "duration", None)
        dur, dur_known = _sanitize_duration(raw_dur)

        if active_policy == SAMPLING_POLICY_TRIGGER_RELEVANT:
            anchors = _plan_trigger_relevant_video(
                item=item,
                duration=dur,
                duration_known=dur_known,
                budget=item_budget,
                event_start_at=event_start_at,
                source_events=source_events,
            )
        else:
            anchors = _plan_sparse_anchors_video(
                item=item,
                duration=dur,
                duration_known=dur_known,
                budget=item_budget,
                event_start_at=event_start_at,
            )

        planned_anchors.extend(anchors)

    # Trim strictly to budget in case of any edge rounding
    return planned_anchors[:budget]


def _plan_sparse_anchors_video(
    item: Any,
    duration: float,
    duration_known: bool,
    budget: int,
    event_start_at: datetime | None = None,
) -> list[PlannedAnchor]:
    """Calculate chronological evenly-spaced anchors across a video clip."""
    item_id = getattr(item, "id", "")
    offset_q = "exact" if duration_known else "estimated"
    clock_quality, _ = _qualify_media_clock(item, event_start_at=event_start_at)

    if budget <= 1:
        offset = round(min(0.5, duration * 0.1), 3)
        return [
            PlannedAnchor(
                media_id=item_id,
                offset_seconds=offset,
                timestamp_quality=clock_quality,
                offset_quality=offset_q,
            )
        ]

    step = duration / (budget + 1)
    raw_points = [(i + 1) * step for i in range(budget)]
    clean_offsets = _make_strictly_monotonic_anchors(raw_points, duration, budget)
    return [
        PlannedAnchor(
            media_id=item_id,
            offset_seconds=off,
            timestamp_quality=clock_quality,
            offset_quality=offset_q,
        )
        for off in clean_offsets
    ]


def _plan_trigger_relevant_video(
    item: Any,
    duration: float,
    duration_known: bool,
    budget: int,
    event_start_at: datetime | None,
    source_events: tuple[Any, ...] | list[Any],
) -> list[PlannedAnchor]:
    """Calculate anchors focused on trigger time with bounded relative coverage fallback."""
    item_id = getattr(item, "id", "")
    offset_q = "exact" if duration_known else "estimated"
    clock_quality, capture_start = _qualify_media_clock(item, event_start_at=event_start_at)

    trigger_offset_in_clip: float | None = None
    ts_quality = "unknown"

    # Identify relevant trigger in source_events
    if capture_start is not None and source_events:
        for s in source_events:
            occ = getattr(s, "occurred_at", None)
            if occ is not None and isinstance(occ, (datetime, str)):
                try:
                    occ_utc = _ensure_utc(occ)
                    sec_from_capture_start = (occ_utc - capture_start).total_seconds()
                    if 0.0 <= sec_from_capture_start <= duration:
                        trigger_offset_in_clip = sec_from_capture_start
                        ts_quality = clock_quality
                        break
                except (ValueError, TypeError, OverflowError):
                    continue

    # Unknown trigger origin fallback evenly covers clip rather than guessing 25% lookback
    if trigger_offset_in_clip is None:
        ts_quality = "relative_fallback"
        step = duration / (budget + 1)
        precision = 4 if duration < 0.1 else 3
        return [
            PlannedAnchor(
                media_id=item_id,
                offset_seconds=round((i + 1) * step, precision),
                timestamp_quality=ts_quality,
                offset_quality=offset_q,
            )
            for i in range(budget)
        ]

    # Relevant trigger found inside clip bounds
    t_focal = max(0.0, min(trigger_offset_in_clip, duration))

    if budget <= 1:
        return [
            PlannedAnchor(
                media_id=item_id,
                offset_seconds=round(t_focal, 3),
                timestamp_quality=ts_quality,
                offset_quality=offset_q,
            )
        ]

    if budget == 2:
        offsets = _make_strictly_monotonic_anchors([0.0, t_focal], duration, 2)
        return [
            PlannedAnchor(
                media_id=item_id,
                offset_seconds=o,
                timestamp_quality=ts_quality,
                offset_quality=offset_q,
            )
            for o in offsets
        ]

    # For budget >= 3:
    # 1. Early context anchor
    # 2. Late context anchor
    # 3. Intermediate anchors clustered tightly around trigger
    early_bound = max(0.0, min(duration * 0.05, t_focal / 2.0))
    late_bound = min(duration, max(duration * 0.95, (t_focal + duration) / 2.0))

    num_focal = budget - 2
    focal_points: list[float] = []

    span_before = min(1.0, duration * 0.2, max(0.0, t_focal - early_bound))
    span_after = min(2.0, duration * 0.2, max(0.0, late_bound - t_focal))

    if num_focal == 1:
        focal_points.append(t_focal)
    elif num_focal == 2:
        focal_points.append(t_focal)
        focal_points.append(min(late_bound, t_focal + (span_after * 0.5)))
    else:
        left_count = (num_focal - 1) // 2
        right_count = (num_focal - 1) - left_count
        for i in range(left_count, 0, -1):
            focal_points.append(t_focal - (span_before * (i / (left_count + 1))))
        focal_points.append(t_focal)
        for i in range(1, right_count + 1):
            focal_points.append(t_focal + (span_after * (i / (right_count + 1))))

    raw_candidates = [early_bound, *focal_points, late_bound]
    final_offsets = _make_strictly_monotonic_anchors(raw_candidates, duration, budget)

    return [
        PlannedAnchor(
            media_id=item_id,
            offset_seconds=offset,
            timestamp_quality=ts_quality,
            offset_quality=offset_q,
        )
        for offset in final_offsets
    ]
