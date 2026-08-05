# Project V Watchtower Connectors

Phase Eighteen introduces a declarative connector catalog for future data-source updates.

A connector describes routing, credentials, refresh and cache policy, and the normalized Project V record type. It does **not** contain arbitrary executable JavaScript. This allows ordinary endpoint or field-mapping updates to be reviewed and shipped separately from the desktop executable later.

## Routing modes

- `local-sidecar` — fetched and normalized by the trusted local Watchtower service.
- `direct-provider` — uses the provider's official endpoint and the user's provider credential.
- `project-v-gateway` — reserved for a future self-hosted Project V Core or Synology deployment.
- `optional-upstream` — inherited compatibility fallback, currently World Monitor.

## Validation

```powershell
npm run connectors:validate
```

Future connector packs should be signed, versioned, permission-scoped, and rollback-capable before remote installation is enabled.
