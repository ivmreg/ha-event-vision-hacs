# Installation & Setup Guide

This guide walks through installing and configuring `event_recording` (standalone core) and `event_vision` (optional AI consumer) on Home Assistant 2026.10.1.

---

## 1. Overview & Installation Method

HACS requires one integration per repository. Add `ivmreg/ha-event-recording`
and, optionally, `ivmreg/ha-event-vision-hacs` as custom repositories with type
**Integration**. Download Event Recording first, then Event Vision, and restart
Home Assistant. Continue with the configuration steps below, skipping the manual
copy steps. The recording integration includes the timeline card.

The development repository contains both components and is not itself a HACS
repository. Distribution repositories must be public for standard HACS; private
staging mirrors cannot be installed. See [HACS packaging](hacs.md) for details.
Manual copy into `custom_components/` remains supported.

> [!IMPORTANT]
> **Version Dependency & HACS Upgrade Order:**
> `event_vision 0.5.4` declares `event_recording` as a dependency for shared declarative validation modules, so Recording must be installed before Vision.
> Only when both components need an upgrade in the same HACS pass, update **Event Recording** before **Event Vision**, then restart Home Assistant once.
> A Recording-only update such as the 0.7.13 activity-filter fix needs no Event Vision reinstall and no analyzer changes; existing Event Vision installations keep working unchanged.

| Component | Role | Required Dependencies | Cloud / API Key |
|---|---|---|---|
| **`event_recording` (0.7.14)** | Standalone event journal, media store, streaming, card | None (works standalone) | None (100% local) |
| **`event_vision` (0.5.4)** | Optional AI incident analyzer | `event_recording` (install first) | Google Gemini API key |

---

## 2. Step 1: Install `event_recording` (Standalone Core)

1. Copy the `custom_components/event_recording` directory into your Home Assistant configuration directory:
   ```
   <ha_config_dir>/custom_components/event_recording/
   ```
2. Restart Home Assistant to allow the integration to be discovered.
3. In Home Assistant, navigate to **Settings -> Devices & Services -> Add Integration**.
4. Search for and select **Event Recording**.
5. Configure the integration options:
   - **Media Root:** Private subdirectory of the Home Assistant configuration directory (default: `event_recording_media`).
   - **Retention Size Budget:** Maximum storage quota in megabytes allocated to recordings before oldest unpinned media is unlinked (default: `2048` MB).
   - **Retention Age Budget:** Maximum age in days before media files become eligible for pruning (default: `7` days).
   - **Chunk Duration:** Base capture segment duration in seconds (default: `10`, maximum `15` seconds).
6. Click **Submit**.

> [!NOTE]
> `event_recording` requires zero API keys and zero camera entities during setup. It is ready for logging and media ingestion immediately.

### Configure camera profiles later

After setup, open **Settings -> Devices & Services -> Event Recording -> Configure**. Configure opens a single settings page directly; there is no menu and no separate camera wizard:

- **Settings page** — storage retention, go2rtc connection, queue/advanced, and correlation values sit together with the camera profile list and are saved in one atomic step. The media root is fixed at setup and shown read-only.
- **Camera profile list** — one row per camera, using the list's native **Add**, **Edit**, and **Delete** controls. A row edits `source_id`, `label`, `window_seconds` (1–300), `lookback_seconds` (0–60), `chunk_seconds` (1–15), and `max_window_seconds` (1–300; at least `window_seconds`), plus the provider and correlation fields. For a new camera, blank `source_id` or `label` values are inferred from the entity and the row starts from the current global timing defaults.
- **One Save** — a single Save writes the global settings and every camera row together. Removing a camera profile, or changing a row's camera entity, is applied only after the explicit removal confirmation shown on the same page.

Camera rows are identified by their camera entity. Changing a row's camera entity replaces that profile: the new entity starts fresh and does not inherit the previous camera's saved Frigate token or extra stored settings. Leave the token blank to keep the token saved for that same camera, or tick the clear box to remove it.

Camera credentials and connection settings remain with the camera's existing Home Assistant integration.

Profiles are optional and stored under `entry.options.camera_profiles`, keyed by camera entity ID. Adding or editing a profile sets defaults for future recording sessions; it does not interrupt or reconfigure an active episode. Removing a profile removes only its settings and leaves recordings intact. The `camera_entity` shortcut on `event_recording.record` requires a configured profile. Existing automations that pass `resource_key` and `recorder_config` remain supported.

For example, a profile can supply source and timing defaults while an automation overrides only the window and lookback for one call:

```yaml
action: event_recording.record
data:
  camera_entity: camera.example_entry
  kind: motion
  operation: start_or_extend
  source_id: entryway       # Optional per-call override
  window_seconds: 20        # Optional; otherwise use the profile
  lookback_seconds: 5       # Optional; otherwise use the profile
```

The shortcut uses the selected camera as the canonical `resource_key`; `kind` and `operation` are still required. Omit per-call profile fields to use the saved values. Lookback depends on the camera's stream buffering or preload settings in Home Assistant; a configured lookback does not guarantee pre-trigger footage.

---

## Optional native camera recording

Keep the owned media root outside `www` and outside every configured `media_dirs` directory so other media handlers cannot bypass event permissions. The default is `/config/event_recording_media` on Home Assistant OS.

For the native camera adapter, add only that private directory to the existing `homeassistant` configuration (merge with your current settings):

```yaml
homeassistant:
  allowlist_external_dirs:
    - /config/event_recording_media
```

Validate configuration and restart after this manual change. Use your actual configuration path on other installations. The integration does not change the global allowlist. Event-only journaling and external media imports do not need this camera permission. FFmpeg and ffprobe must be available for validated video imports/capture; Home Assistant OS normally provides them.

## 3. Step 2: Register the Lovelace Card Resource

`event_recording` bundles a companion Lovelace card for viewing event timelines, playing Range-streamed video clips, and inspecting annotations.

1. In Home Assistant, navigate to **Settings -> Dashboards -> Resources** (or menu dots in Lovelace UI -> Manage resources).
2. Click **Add Resource**.
3. Fill in the resource details:
   - **URL:** `/event_recording/event-card.js?v=0.7.14`
   - **Resource Type:** `JavaScript Module`
4. Click **Create**.
5. You can now add the card to any Lovelace view:
   ```yaml
   type: custom:event-recording-card
   page_size: 25
   ```

> [!TIP]
> Keep the `?v=` suffix in sync with the installed integration version (currently `0.7.14`). Changing it after a HACS upgrade makes browsers fetch the new module instead of a cached one.

---

## 4. Step 3: Install `event_vision` (Optional AI Consumer)

If you wish to enable multimodal AI event analysis via direct Google Gemini:

1. Copy the `custom_components/event_vision` directory into your Home Assistant directory:
   ```
   <ha_config_dir>/custom_components/event_vision/
   ```
2. Restart Home Assistant.
3. In Home Assistant, navigate to **Settings -> Devices & Services -> Add Integration**.
4. Search for and select **Event Vision**.
5. Provide your configuration:
   - **Gemini API Key:** Your Google AI Studio API key.
   - **Gemini Model:** Multimodal model ID (default: `gemini-flash-lite-latest`).
6. Click **Submit**.

To change settings later, open **Settings -> Devices & Services -> Event Vision -> Configure**. One form edits the model, concurrency, auto-analyze, sampling, fusion, and advanced timing values together and saves them atomically. The API key field is masked; leave it blank to keep the saved key. The form description summarizes the saved settings and the minimum job deadline implied by the current timeouts.

> [!NOTE]
> **Secret Storage Notice:** Home Assistant persists configuration entries on disk in `.storage/core.config_entries` as plaintext JSON. Keys are redacted in diagnostics and logging, but are not encrypted at rest by the Home Assistant platform.

---

## 5. Step 4: Configure Home Assistant Automations

All event triggers are defined via native Home Assistant automations. Use `mode: queued` with `max: 100` to prevent drop-off during rapid successive triggers.

### 5.1 Generic Event Logging (Without Camera)
Log sensor transitions or system events directly into the durable journal:

```yaml
automation:
  - alias: "Server Room High Temperature"
    mode: queued
    max: 100
    trigger:
      - platform: numeric_state
        entity_id: sensor.server_room_temperature
        above: 32.0
    action:
      - action: event_recording.log_event
        data:
          source_id: sensor.server_room_temperature
          kind: high_temperature
          label: "Server Room Above 32C"
          payload:
            current: "{{ states('sensor.server_room_temperature') }}"
            alert_level: warning
          access_entities:
            - sensor.server_room_temperature
```

### 5.2 Trigger Recording with a Camera Profile
Capture video from a configured camera profile. Its source and timing defaults apply unless the action supplies an override:

```yaml
automation:
  - alias: "Front Door Motion Recording"
    mode: queued
    max: 100
    trigger:
      - platform: state
        entity_id: binary_sensor.example_motion
        to: "on"
    action:
      - action: event_recording.record
        data:
          camera_entity: camera.example_entry
          kind: motion
          operation: start_or_extend
          source_id: entryway       # Optional override of profile source
          window_seconds: 15        # Optional override of profile default
          lookback_seconds: 5       # Optional override of profile default
          access_entities:
            - camera.example_entry
            - binary_sensor.example_motion
```

### 5.3 Reference Blueprints & Templates
- Door contact recording blueprint: [`blueprints/automation/event_recording/door_recording.yaml`](../blueprints/automation/event_recording/door_recording.yaml).
- Event journal automation templates:
  - [`examples/automations/ha_event_journal.yaml`](../examples/automations/ha_event_journal.yaml)
  - [`examples/automations/mqtt_journal.yaml`](../examples/automations/mqtt_journal.yaml)
  - [`examples/automations/numeric_state_journal.yaml`](../examples/automations/numeric_state_journal.yaml)

### 5.4 Camera Prerequisites
- **Lookback Buffering:** Lookback pre-roll depends on stream buffering or preload settings in Home Assistant. Cold or non-preloaded streams may not supply pre-trigger footage; setting a lookback duration does not guarantee it.
- **Single Recorder Rule:** Only one recording task should manage a given `resource_key` at a time. Do not configure competing automations or external recording integrations on the same camera stream.
