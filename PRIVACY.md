# Privacy Notes

## Summary

Project V Watchtower is designed as a local-first workstation, but it is not an offline-only application and does not make a blanket zero-telemetry or anonymity guarantee.

Privacy depends on:

- Which panels are enabled
- Which providers are configured
- Which websites are opened
- Which API keys are supplied
- Whether Ollama or another AI endpoint is used
- Which plugins are installed
- Which files and sources the analyst imports
- Windows, WebView2, network, and provider behavior

## Data stored locally

Depending on use, local application storage may include:

- Workspace layouts
- Panel visibility and geometry
- Interface preferences
- Alert rules
- Watchlists
- Event timelines
- Research Library records and excerpts
- Case Desk investigations
- Data Desk workbooks and snapshots
- Map Operations records
- AI conversation history
- Analysis Room missions and outputs
- Plugin manifests and plugin-local data
- Recent source and OSINT history
- Local caches and logs

Some records use localStorage; larger records may use IndexedDB or runtime-specific storage.

## Credentials

Supported desktop credentials are intended to use the operating-system credential vault rather than readable project files.

Do not assume every provider's browser sign-in or third-party webview storage is controlled by Project V. Remote services manage their own cookies, sessions, and privacy practices.

## External requests

Live or connected features may send requests to:

- Configured data providers
- Public feeds
- Map and imagery providers
- Local sidecar services
- Ollama or another configured AI endpoint
- Public-source search services
- Communications websites
- User-opened URLs
- Optional inherited compatibility routes

Search terms, URLs, IP addresses, account sessions, provider identifiers, and request metadata may be visible to those services.

## AI data

When using local Ollama, prompts and supplied context are sent to the configured local Ollama endpoint.

When using any non-local model or remote gateway, prompts and selected context may leave the computer. Review the endpoint before sending private documents, evidence, locations, or personal data.

Multi-agent roles can reuse the same underlying model. They are not independent privacy boundaries.

## Backups and exports

Project V backups may contain substantial personal or investigative data.

Protected backups use password-based encryption, but their protection depends on:

- Passphrase strength
- Secure storage
- Correct implementation
- An uncompromised computer

API keys and selected machine secrets are intended to be excluded from backups. Inspect exports before sharing them.

## Network controls

Network modes can reduce new frontend connections. They do not guarantee total network isolation of:

- Existing media streams
- Remote webviews
- External browsers
- Other Windows processes
- Provider applications
- Malware
- Operating-system services

Use operating-system firewall rules or a physically disconnected network when a stronger boundary is required.

## Deletion

Deleting an item inside the application may not erase:

- Backups
- Exported files
- Windows restore points
- Provider-side records
- Browser or WebView caches
- Operating-system credential entries
- Logs created outside the application
- Copies held by third-party services

For sensitive decommissioning, export what is needed, remove credentials, uninstall the program, review the application profile and WebView2 data, remove backups, and follow Windows secure-deletion practices appropriate to the storage device.

## No anonymity guarantee

Project V Watchtower is not Tor Browser, a VPN, an anonymity network, or a hardened operating system.

Opening a public-source service may reveal the user's network address and normal browser or WebView characteristics to that service.

## Children and regulated data

The software is not specifically designed for children, healthcare records, legal privilege, classified data, or regulated identity data. Users are responsible for determining whether their use complies with applicable law, policy, contract, and provider terms.

## Changes

Privacy behavior can change as providers, dependencies, and Project V features change. Review these notes with each release.
