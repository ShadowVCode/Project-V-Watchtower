# Project V Watchtower

> **Experimental pre-alpha software.** Project V Watchtower has not undergone an independent security audit. It does not guarantee anonymity, immunity from malware, protection from a compromised operating system, or accuracy of external data or AI-generated analysis. The current Windows installers are not Authenticode-signed and may appear as an unknown publisher.

Project V Watchtower is a Windows desktop situational-awareness and research command center built with Tauri, TypeScript, Vite, MapLibre, and optional local AI through Ollama.

It combines live information panels, operational maps, local research tools, case management, configurable workspaces, restricted web handoffs, alerting, and source-aware AI analysis in one desktop interface.

## Upstream foundation

Project V Watchtower is an independently maintained and substantially modified
downstream project based on World Monitor.

Project V Watchtower is not an official World Monitor release and is not
endorsed or maintained by the upstream World Monitor developers.

## Download

Use the **Releases** page rather than downloading binaries from the repository itself.

### Recommended for most users

`Project-V-Watchtower-1.0.0-Windows-x64-Setup.exe`

### Managed or administrative deployment

`Project-V-Watchtower-1.0.0-Windows-x64.msi`

Verify the downloaded file against `SHA256SUMS.txt` before installation.

## Current release

**Project V Watchtower 1.0.0**

This release provides an unsigned Windows NSIS installer and MSI package. The automatic updater is not enabled in this local unsigned build. Future updater-enabled releases require a permanent Tauri updater signing identity and HTTPS release endpoints.

## Core capabilities

- Customizable multi-workspace command deck
- Live world map and situational-awareness panels
- News, natural events, weather, economic, infrastructure, maritime, aviation, and other configured feeds
- Research Library for documents, excerpts, notes, and source material
- Case Desk for investigations and evidence organization
- Data Desk for CSV and limited XLSX workflows
- Map Operations for markers, areas, routes, measurements, GeoJSON, and geofenced alerts
- Optional local Command Assistant using Ollama
- Local multi-agent Analysis Room with collector, verifier, timeline, contradiction, geospatial, and briefing roles
- OSINT Desk for launching approved public-source searches
- Camera Wall, Communications Wall, Launch Deck, and restricted Source Browser
- Sandboxed local plugin modules with declared permissions
- Project Lock, backup and recovery tools, network modes, diagnostics, and safe mode
- Optional voice commands and spoken alerts

See [the white paper](WHITEPAPER.md) and [architecture overview](docs/ARCHITECTURE.md) for the design in more detail.

## Local-first does not mean offline-only

Project V Watchtower stores many workspaces, cases, research records, preferences, and local AI conversations on the computer. However, live panels and public-source tools may connect to external providers. Some features require provider API keys, internet access, or third-party web services.

Use **Security → Network** to restrict new frontend connections when needed. Do not assume the application produces zero network traffic without reviewing the enabled panels, provider settings, webviews, and runtime behavior.

## Optional local AI

Ollama is optional and is installed separately. Project V Watchtower does not require Ollama merely to open the command deck.

When local AI is enabled:

1. Install Ollama on Windows.
2. Download a model appropriate for the computer.
3. Open Project V Watchtower's AI or API settings.
4. Select the local Ollama endpoint and model.
5. Test the connection before running Analysis Room missions.

AI output is not evidence. Agreement between multiple agent roles is not independent corroboration when the roles use the same underlying model.

## Installation

Read [INSTALLATION.md](INSTALLATION.md).

Minimum practical expectations:

- Windows 10 or Windows 11, 64-bit
- Microsoft WebView2 Runtime
- Sufficient memory and disk space for enabled panels and optional local models
- Internet access for live external feeds
- Ollama only for optional local AI features

## Security and privacy

Read:

- [SECURITY.md](SECURITY.md)
- [PRIVACY.md](PRIVACY.md)
- [Threat model](docs/THREAT_MODEL.md)
- [Data and AI behavior](docs/DATA_AND_AI.md)

## License and upstream notice

Project V Watchtower is distributed under **AGPL-3.0-only** according to the project metadata and retains applicable upstream notices and attribution.

This project is a modified fork and must not be represented as the official World Monitor service or an official product of any upstream project, provider, platform, or government agency.

The attached `Project-V-Watchtower-1.0.0-Source.zip` is the corresponding application source for this binary release. GitHub's automatically generated “Source code” archives refer only to the documentation repository used for this release page; use the specifically named Project V source archive for the application source.

See [UPSTREAM_AND_LICENSE.md](UPSTREAM_AND_LICENSE.md).

## Status

This is an experimental first public release line. Expect incomplete integrations, provider changes, UI defects, large bundle sizes, unsigned-installer warnings, and features that depend on separately configured services.

## Support

Use GitHub Issues for reproducible bugs and feature requests. Do not post API keys, private research, personal data, sensitive locations, exploit details, or private logs.

For suspected security vulnerabilities, follow [SECURITY.md](SECURITY.md).
