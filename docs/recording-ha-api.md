# Event Recording Integration & API Reference

The `event_recording` integration is a pure, neutral, high-performance event journaling and media management subsystem for Home Assistant. It functions with **zero AI dependencies, zero API keys, and zero required camera entities** during configuration.

---

## 1. Architectural Principles

1. **Trigger Neutrality:** Home Assistant native automations own all triggers (motion, contact, numeric threshold, MQTT, RFID tags, webhooks, or manual). Triggers are acknowledged immediately upon durable SQLite journal insertion or intent admission.
2. **Decoupled Facade:** External consumer integrations (such as `event_vision` for AI analysis) interact with `event_recording` solely via pure Python public facade (`custom_components.event_recording.api.RecordingAPI`) or frozen WebSocket commands. The recording database contains **zero analyzer-private jobs, tables, or API keys**.
3. **Stream Quarantine & Resource Serialization:** Capture tasks serialize native `camera.record` operations per resource key. If tasks are cancelled or timed out, stream resources enter quarantine until the underlying runner completely settles, preventing dual-recorder camera pipeline corruption across integration reloads.
4. **Fail-Closed Security & Range Streaming:** Access control is strictly enforced on access entity IDs and event owners. Media streaming operates over HTTP with Range header support (HTTP 200/206) via short-lived signed tokens (`async_sign_path`) outside public `/config/www`. All filesystem access occurs off-loop.
5. **Pre-Chunk Capacity Safeguards:** Storage budgets and available disk space are verified prior to starting each capture chunk under a shared capacity lock. Rejected captures record visible `capacity_error` source events without false ready states.

---

## 2. Configuration Flow & Options

- **Single Instance:** Enforced via `single_instance_allowed = True`. Only one instance of `event_recording` exists per Home Assistant instance.
- **Config Flow Parameters:**
  - `media_root`: Filesystem path for owned recordings (default: `event_recording_media`). Rejects any path located inside public `/config/www` (`www_forbidden`) or exposed via configured `media_dirs` (`media_dir_exposed`).
  - `retention_size_mb`: Soft maximum storage budget in MB (default: `2048`).
  - `retention_days`: Maximum age before media deletion (default: `7`).
  - `chunk_seconds`: Recording chunk duration in seconds (default: `10`, maximum `15`).
- **Options Flow:** Allows non-disruptive reconfiguration of retention size, retention age, and chunk duration.
- **Camera Recorder Adapter Prerequisite (`allowlist_external_dirs`):**
  - Recording with the optional native `camera` adapter invokes `camera.record`, which requires that the target storage directory is explicitly configured in Home Assistant's `allowlist_external_dirs` in `configuration.yaml` (e.g. `/config/event_recording_media`).
  - The adapter preflights this check via `hass.config.is_allowed_path` and fails closed with `MEDIA_ROOT_NOT_ALLOWED` if unlisted.
  - Pure journal logging (`log_event`), external asset attachment (`attach_media`), and imported media work without camera allowlisting.
  - Allowed import roots automatically include configured `hass.config.media_dirs.values()`, owned store root, and standard media directories. Imported media is copied into owned private storage without deleting producer assets.

---

## 3. Public Python API Facade (`custom_components.event_recording.api`)

Consumer integrations and extensions interact through `RecordingAPI`:

```python
from custom_components.event_recording.api import get_api

api = get_api(hass)
if api:
    # 1. Pure core read/write & CAS annotations
    store = api.store

    # 2. Retrieve event
    event = await api.get_event("ev_12345")

    # 3. Resolve verified absolute filesystem path for media (off-loop verification)
    path = await api.resolve_media("ev_12345", "media_67890")

    # 4. Subscribe to post-commit event revision updates
    unsub = api.subscribe(lambda event_id, revision: print(f"Updated {event_id} -> {revision}"))

    # 5. Register consumer capabilities and retry delegate
    unsub_consumer = api.register_consumer(
        consumer_id="event_vision",
        retry_callback=async_retry_handler,
        available=True,
    )

    # 6. Optional ffmpeg binary discovery
    ffmpeg_bin = api.ffmpeg_binary
```

### Methods & Properties
- `api.store -> JournalStore`: Direct pure Python SQLite journal store supporting CAS annotations, leases, and retention enforcement.
- `async api.resolve_media(event_id: str, media_id: str) -> Path`: Confirms asset association, checks availability, verifies path containment within `media_root` off-loop, and confirms file exists on disk.
- `api.subscribe(callback: Callable[[str, int], None]) -> Callable[[], None]`: Notifies on post-commit global mutation revisions.
- `api.register_consumer(consumer_id: str, retry_callback: Callable[[str], Awaitable[bool]], available: bool = True) -> Callable[[], None]`: Neutral registry for external consumers. Calling unsubscribe only removes the exact matching instance.
- `api.ffmpeg_binary -> str | None`: Detects HA ffmpeg manager binary (`get_ffmpeg_manager(hass).binary`) or system ffmpeg without blocking setup if unavailable.

---

## 4. Home Assistant Services

All services support `SupportsResponse.OPTIONAL`. When invoked with `response_variable` in automations, they return typed status dictionaries immediately.

### `event_recording.log_event`
Direct journal insertion for non-camera or arbitrary sensors.
```yaml
action: event_recording.log_event
data:
  source_id: sensor.server_temperature
  kind: threshold_exceeded
  occurred_at: "2026-10-15T06:00:00Z" # Optional, defaults to now
  idempotency_key: "{{ context.id }}" # Optional, defaults to context.id
  label: "High Temperature Warning"
  payload:
    celsius: 38.5
  access_entities:
    - sensor.server_temperature
response_variable: log_result
# Returns: {"event_id": "ev_...", "inserted": true}
```

### `event_recording.record`
Queues a durable recording intent. Positive acceptance ACK occurs immediately upon SQLite intent write; camera capture proceeds asynchronously.
```yaml
action: event_recording.record
data:
  source_id: binary_sensor.front_door_contact
  kind: contact_open
  resource_key: camera.front_door # Must match canonical camera_entity if adapter=camera
  operation: start_or_extend     # 'start_or_extend' or 'end_after'
  window_seconds: 15             # 1 to 300
  lookback_seconds: 5            # 0 to 60 (used on initial capture chunk)
  hold: true                     # Keep active until explicit end_after or max 300s
  recorder_config:
    adapter: camera
    camera_entity: camera.front_door
  idempotency_key: "{{ context.id }}"
response_variable: record_result
# Returns: {"event_id": "ev_...", "inserted": true, "intent_id": "intent_..."}
```

### `event_recording.attach_media`
Atomically validates and attaches an external video or image file to an event. Restricted to **Admin and System Automation callers** to prevent arbitrary filesystem attachment by untrusted human users.
```yaml
action: event_recording.attach_media
data:
  event_id: "ev_12345"
  media_locator: "/config/event_recording_media/.staging/clip1.mp4"
  kind: video
  finalize: true
response_variable: attach_result
# Returns: {"event_id": "ev_...", "media_id": "media_..."}
```

### `event_recording.finalize`
Concludes recording and marks an event ready for consumer processing.
```yaml
action: event_recording.finalize
data:
  event_id: "ev_12345"
response_variable: finalize_result
# Returns: {"event_id": "ev_...", "capture_state": "ready"}
```

---

## 5. Frozen WebSocket API

### `event_recording/events`
List events with keyset pagination and server-side security filtering. Cursor strictly tracks the last consumed record.
```json
// Request
{
  "id": 1,
  "type": "event_recording/events",
  "source_id": "binary_sensor.front_door_contact",
  "kind": "contact_open",
  "limit": 20,
  "before_cursor": "..."
}

// Response
{
  "id": 1,
  "type": "result",
  "success": true,
  "result": {
    "events": [
      {
        "event_id": "ev_abc123",
        "source_id": "binary_sensor.front_door_contact",
        "kind": "contact_open",
        "label": "Front Door Open",
        "start_at": "2026-10-15T06:00:00+00:00",
        "end_at": "2026-10-15T06:00:30+00:00",
        "capture_state": "ready",
        "capture_error": null,
        "media_revision": 2,
        "revision": 3,
        "source_events": [
          {
            "type": "contact_open",
            "occurred_at": "2026-10-15T06:00:00+00:00",
            "source_id": "binary_sensor.front_door_contact",
            "label": "Front Door Open"
          }
        ],
        "media": [
          {
            "id": "media_xyz789",
            "kind": "video",
            "mime_type": "video/mp4",
            "duration": 30.0,
            "available": true
          }
        ],
        "coverage": {},
        "annotations": {}
      }
    ],
    "next_cursor": "2026-10-15T06:00:00+00:00|ev_abc123",
    "capabilities": {
      "consumers": [
        {
          "id": "event_vision",
          "available": true,
          "can_retry": true
        }
      ]
    }
  }
}
```

### `event_recording/event`
Get a single event by ID.
```json
// Request: {"id": 2, "type": "event_recording/event", "event_id": "ev_abc123"}
// Response: {"id": 2, "type": "result", "success": true, "result": {"event": {...}}}
```

### `event_recording/subscribe`
Subscribe to post-commit event revision notifications. Revalidates live user status and event permissions on each event dispatch.
```json
// Request: {"id": 3, "type": "event_recording/subscribe"}
// Push Event: {"id": 3, "type": "event", "event": {"event_id": "ev_abc123", "revision": 4}}
```

### `event_recording/media`
Obtain signed, time-limited URL for authenticated media playback.
```json
// Request: {"id": 4, "type": "event_recording/media", "event_id": "ev_abc123", "media_id": "media_xyz789", "expires_in": 300}
// Response:
{
  "id": 4,
  "type": "result",
  "success": true,
  "result": {
    "url": "/api/event_recording/media/ev_abc123/media_xyz789?authSig=...",
    "expires_at": "2026-10-15T06:05:00+00:00",
    "mime_type": "video/mp4"
  }
}
```

### `event_recording/process`
Trigger external consumer retry for an event. Requires admin privileges.
```json
// Request: {"id": 5, "type": "event_recording/process", "event_id": "ev_abc123", "consumer": "event_vision", "retry": true}
// Response: {"id": 5, "type": "result", "success": true, "result": {"accepted": true}}
```

---

## 6. Security Model

1. **Authorization Matrix:**
   - **WebSocket / Media HTTP:** Requires active, non-system authenticated user. Inactive or revoked users are rejected with HTTP 401.
   - **Access Entities:** When an event defines `access_entities`, the user must possess `read` permissions for **all** specified entities. If `access_entities` is empty, fallback to `owner_user_id == user.id` applies. Admins possess global access.
   - **Service Calls:** HA system automations (`context.user_id is None`) are fully authorized. Human UI callers must have `control` permission for relevant entities. Attaching filesystem media requires admin privileges.
   - **Camera Protection:** Recording with a camera adapter automatically adds the canonical camera entity to the event's `access_entities`, ensuring camera recordings are never exposed based on sensor-only permissions.
2. **Server-Side Pagination Filtering:** Keyset cursor accurately points to the last scanned record consumed, avoiding skips. Exhausted scan budgets return a continuation cursor when more events exist.
3. **HTTP Range Streaming:** Implemented via `homeassistant.components.http.HomeAssistantView` and `aiohttp.web.FileResponse`. Serves HTTP 200/206 with `Accept-Ranges: bytes` and `Cache-Control: private, no-store`.
4. **Signed Paths:** Token dispatch uses `homeassistant.components.http.auth.async_sign_path` bound to `ActiveConnection.refresh_token_id`. Path tokens expire within $\le 300$ seconds.
