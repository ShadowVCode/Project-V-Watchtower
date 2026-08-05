# Project V Watchtower  
## A Local-First Situational-Awareness and Research Workstation

**Version:** 1.0  
**Release line:** Project V Watchtower 1.0.0  
**Status:** Experimental pre-alpha

## Executive summary

Project V Watchtower is a browser-technology-based Windows desktop application designed to bring situational awareness, source collection, operational organization, and optional local AI into a single analyst-controlled workspace.

Its objective is not to create an all-knowing intelligence system. Its objective is to reduce fragmentation: maps in one window, articles in another, notes in a third, spreadsheets elsewhere, local AI in a terminal, and alerts scattered across unrelated services.

Watchtower provides a command deck where those activities can be organized without treating AI output as verified fact and without granting every embedded tool the same level of trust.

## Problem statement

Modern public-source research has three recurring problems:

1. **Information fragmentation**  
   Events, maps, feeds, documents, cases, media, and notes live in separate systems.

2. **Context loss**  
   A source may be read, summarized, and forgotten without preserving the original excerpt, timestamp, location, or relationship to an investigation.

3. **Automation without trust boundaries**  
   Dashboards often combine remote content, credentials, extensions, and AI while failing to make clear which component can access what.

Project V Watchtower addresses these problems through a modular command deck, local records, separated workspaces, restricted webviews, permission-gated plugins, and explicit AI limitations.

## Design principles

### Analyst control

The user chooses which modules are visible, which sources are configured, when live refresh occurs, what material enters a case, and whether AI receives workspace context.

### Local-first records

Workspaces, cases, research items, map records, alert rules, and many preferences remain in the local application profile unless deliberately exported, backed up, or sent to a configured service.

### Evidence before synthesis

AI roles operate on supplied Watchtower context, documents, cases, and sources. Their output is analysis—not independent evidence.

### Separation of trust

Core Project V windows, imported plugins, public websites, communications services, local applications, and external browsers are not treated as equivalent.

### Honest limitations

Project Lock is not full-disk encryption. Network controls do not neutralize a compromised operating system. A local model can hallucinate. Public-source providers can be wrong, delayed, unavailable, or manipulated.

## System overview

Project V Watchtower uses a layered architecture:

1. **Desktop shell**  
   Tauri provides the Windows application container and native integrations.

2. **Command interface**  
   TypeScript, Vite, HTML, and CSS provide the multi-workspace command deck and dedicated tool windows.

3. **Map and visualization layer**  
   MapLibre and Deck.gl render maps, operational records, and supported geospatial layers.

4. **Local sidecar and provider routing**  
   The application can use a local sidecar, direct provider credentials, optional local services, and inherited compatibility routes.

5. **Operational storage**  
   Browser-profile storage and IndexedDB retain local application records. Desktop secrets use operating-system-backed credential storage where implemented.

6. **Optional local AI**  
   Ollama supplies local language-model inference. The Command Assistant and Analysis Room use selected Watchtower context rather than unrestricted hidden browsing.

7. **Security and recovery controls**  
   Project Lock, network modes, plugin permissions, backups, safe mode, diagnostics, and trusted-window controls reduce accidental exposure and improve recovery.

## Command deck and workspaces

The command deck is built around movable, resizable modules and separate workspaces. Analysts can create task-specific arrangements without duplicating the entire application.

Typical workspaces include:

- Watchtower
- Live operations
- Intelligence
- Research
- Assistant
- Custom desks

Map size, panel visibility, panel geometry, minimized state, and hidden state can be stored per workspace.

## Research and operational workflow

A typical workflow is:

1. A live panel, map marker, article, camera, or public source produces a signal.
2. The analyst saves a source, marker, timeline entry, alert, or evidence item.
3. Related material is organized in the Research Library or Case Desk.
4. Map Operations adds routes, areas, measurements, and geofence rules.
5. The Command Assistant or Analysis Room receives only the context selected by the analyst.
6. The final assessment is reviewed against the original sources.
7. Records are exported or backed up deliberately.

This flow attempts to preserve provenance instead of turning every observation directly into a model-generated conclusion.

## Local multi-agent research

Analysis Room divides a mission among specialist roles:

- Collector
- Verifier
- Timeline Analyst
- Contradiction Analyst
- Geospatial Analyst
- Briefing Officer

These roles may use the same local model. Their agreement is therefore not independent corroboration. The division is useful for structured review, not for manufacturing certainty.

The evidence remains the supplied source material. Agent output should be treated as a set of inspectable working analyses.

## Plugin model

Imported Project V plugin modules run in sandboxed frames and request declared permissions such as:

- Namespaced local storage
- Approved network origins
- Notifications
- Clipboard access
- Limited workspace context

They do not automatically receive unrestricted Node.js, Tauri, local-file, API-key, or parent-page access.

Permission controls reduce exposure but do not turn unreviewed third-party code into trusted code.

## Restricted external content

Public websites and communications services can open in restricted or untrusted webviews or in the user's external browser. Remote pages are not intended to receive trusted Watchtower IPC capabilities.

Embedded-provider behavior can change. Authentication may fail, providers may block embedded sign-in, and a user may need to open the service externally.

## Security controls

Project V Watchtower includes:

- Numeric Project Lock verifier
- Inactivity and focus-related lock options
- Operating-system credential storage for supported desktop secrets
- Configuration and full backups
- Optional AES-256-GCM protected backup archives
- PBKDF2-SHA256 key derivation
- Safe Mode
- Last-known-good restoration
- Network modes
- Blocked-request audit records
- Plugin permission revocation
- Restricted webview and launcher boundaries

These controls are defense-in-depth measures. They are not a substitute for Windows account security, BitLocker, endpoint protection, software updates, network controls, or safe handling of sensitive data.

## Network modes

The security layer provides modes conceptually equivalent to:

- Normal
- Restricted
- Local AI only
- Emergency disconnect

The guard primarily controls new frontend fetch and WebSocket activity. Existing external windows, media streams, operating-system processes, or traffic outside the guarded path may require separate closure or controls.

## Data and privacy model

Project V Watchtower is local-first, but connected features may transmit:

- Search terms
- Requested feed parameters
- Provider API requests
- URLs opened in external services
- Material explicitly sent to a configured AI endpoint
- Authentication data entered directly into third-party websites

Users should inspect enabled providers, configured endpoints, and the network policy before handling sensitive work.

## Distribution model

The first public release provides:

- Windows NSIS installer
- Windows MSI installer
- SHA-256 checksums
- Corresponding source archive
- Documentation and release notes

The current installers are unsigned. Windows may show an unknown-publisher or SmartScreen warning.

A future automatic updater requires:

- Permanent updater signing keys
- Embedded public key
- HTTPS update manifest
- Signed updater artifacts
- Secure protection of the private key

Windows Authenticode signing is separate from Tauri updater signing.

## Current limitations

- Experimental pre-alpha status
- No independent security audit
- No guarantee of anonymity
- No protection against a compromised operating system
- Unsigned Windows installers
- External data quality varies by provider
- Provider APIs, terms, authentication, and availability can change
- Some inherited panels may depend on upstream compatibility services
- Local AI quality depends on model, hardware, prompt, and source context
- Large frontend bundles remain a performance optimization target
- Portable and mobile release paths are not part of this specific unsigned installer release
- Automatic updates are not enabled in the current local build

## Non-goals

Project V Watchtower is not:

- A government intelligence system
- A guaranteed real-time source of truth
- A replacement for professional emergency services
- A malware sandbox
- A secure operating system
- A full anonymity system
- A substitute for independent source verification
- An authorization to access private systems or accounts
- A tool for bypassing authentication or provider controls

## Roadmap direction

Future work may include:

- Independent security review
- Authenticode signing
- Signed updater releases
- Portable distribution
- More complete source provenance
- Performance and bundle-size optimization
- Additional reviewed connectors and module packs
- Optional self-hosted gateway services
- Mobile companion features
- Tested application-identifier migration
- Expanded accessibility and voice controls

## Conclusion

Project V Watchtower is intended to be a controlled workspace, not an oracle.

Its value comes from bringing maps, sources, cases, documents, alerts, and optional local AI into one inspectable workflow while preserving distinctions between observation, evidence, analysis, and conclusion.
