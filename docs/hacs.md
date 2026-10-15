# HACS distributions

HACS manages only one integration per repository. This development repository
contains both integrations, so do not add it directly to HACS. Generate two
independent distribution repositories instead:

| Repository | Integration | Includes |
| --- | --- | --- |
| `ivmreg/ha-event-recording` | `event_recording` | Journal, optional capture, media API, timeline card |
| `ivmreg/ha-event-vision-hacs` | `event_vision` | Optional direct Gemini analyzer |

Install Event Recording first. Event Vision declares its dependency on
`event_recording`; Home Assistant does not download the dependency automatically.
The card ships inside Event Recording and needs no third HACS repository.

## Build and verify

```powershell
python -m pytest tests/packaging -q
python tools/build_hacs.py --output dist/hacs
```

The output contains one clean repository tree and one reproducible archive per
integration, with `hacs.json`, component manifest, license, installation guide,
and `SOURCE.json` file hashes. Only tracked component files and the selected
documentation are exported. Git history, credentials, deployment configuration,
and runtime media are excluded. The output directory must be fresh.

Publish each tree to its corresponding repository and create a GitHub release
whose tag matches the component manifest version. HACS installs the component
directory from that release; these source archives do not use `zip_release`.
Never manually edit generated integration files in a distribution repository.
Make source changes here, run CI, and regenerate both distributions as needed.

## Repository visibility

[HACS supports public GitHub repositories](https://www.hacs.dev/docs/publish/start/).
A private staging repository is reviewable but cannot be installed through
standard HACS. Making the two generated distribution repositories public is a
separate publication decision; it does not require publishing this development
repository or any Home Assistant configuration. The exporter and CI never publish
code or change visibility themselves.

## Install and roll back

Add each distribution under HACS > Custom repositories with type **Integration**,
then download Event Recording and optionally Event Vision. Restart Home Assistant
and configure each integration through Settings > Devices & services. Follow
[the installation guide](install.md) for the private camera media allowlist and
dashboard resource registration.

HACS can select an earlier published release for rollback. Media, the event
journal, and analysis databases are outside the integration directories and are
not overwritten by a HACS download. Keep a scoped configuration backup before
changing live automation definitions. A rollback to code with a different database
schema requires checking compatibility first.
