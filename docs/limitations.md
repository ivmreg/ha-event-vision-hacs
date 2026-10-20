# Operational limits

This is the maintained limitations reference for both integrations. Component manifests
identify installed versions; release notes describe historical changes.

- **Coverage:** Native HLS and go2rtc buffers need time to warm. Cold/reconnected
  streams can lack lookback. Keyframes and chunk handoffs can leave gaps; duration
  does not prove UTC-aligned coverage. Snapshot mode produces still images.
- **Ownership:** The registry serializes this integration's writers. Unrelated
  consumers can contend for the same physical stream. Unsettled writers remain
  quarantined and charged until actual completion.
- **Storage:** Retention uses bounded batches and a soft target. Active captures
  and bounded pins postpone deletion. Jobs waiting for analysis are not pinned
  indefinitely and may lose their input to expiry. Failed deletion claims remain
  unavailable and charged until retry succeeds; new pins fail during deletion.
- **History:** Media expiry preserves factual metadata. Operational compaction
  preserves deduplication fences and does not delete source history. Long-lived
  factual archives still grow and need an installation-specific backup/export
  policy. No automatic source-history deletion is configured.
- **Analysis:** Timeouts and retries can cause additional provider charges. Claim
  fences protect local commits, not provider billing. RAM caches disappear on
  process restart. Cache reuse does not improve uncertain source clocks.
- **Fusion:** Multi-camera analysis is opt-in and can issue extra requests.
  Correspondences are uncertain hypotheses, not identity proof. Grouped incident
  presentation does not fully replace the per-event card.
- **Credentials:** Home Assistant stores config-entry secrets as plaintext in its
  configuration storage. Keep private media outside `www` and public media roots.
  Logs and diagnostics redact credentials, private paths, and raw provider payloads.
- **Migration:** Schema upgrades create verified database backups. Changing the
  media root reloads the integration; automatic archive relocation is not provided.
  Automated tests use synthetic media and isolated HA instances, not physical
  devices or paid provider requests.
