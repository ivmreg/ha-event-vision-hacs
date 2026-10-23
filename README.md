# Event Vision

Optional direct Gemini analysis of finalized Event Recording events. Its durable queue is separate from recording. Cloud failures and analysis retries do not block event intake or capture. This integration has no LLMVision dependency.

Requires Home Assistant **2026.10.1** or later. Version **0.5.5**.

## HACS custom repository

In HACS > Custom repositories, add the following repository with type **Integration**:

`ivmreg/ha-event-vision-hacs`

HACS requires public GitHub repositories; a private staging mirror is not installable.

## Setup

1. Install and configure [Event Recording](https://github.com/ivmreg/ha-event-recording) first.
2. Download **Event Vision** from HACS, then restart Home Assistant.
3. Add **Event Vision** under Settings > Devices & services.
4. Enter a Google Gemini API key in the Home Assistant configuration form and choose a model.
5. Analysis runs on finalized events and appears in the Event Recording timeline card.

Removing or disabling this optional analyzer leaves the event journal and recording pipeline available.

See [the installation guide](docs/install.md) for configuration and automation examples.

## Known Limitations

Concrete operational limitations and architectural boundaries are documented in `custom_components/event_vision/KNOWN_ISSUES.md` and visible in the installed integration Options screen.

This distribution is generated from the source identified in `SOURCE.json`. It contains one integration only; local recordings, API keys, and Home Assistant configuration are not part of the package.
