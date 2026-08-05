# Architecture Overview

## Desktop runtime

Project V Watchtower is packaged as a Tauri desktop application. The UI is built with TypeScript and Vite and rendered through Windows WebView2.

Native Tauri commands provide selected desktop capabilities such as trusted windows, credential access, file operations, and approved application launching.

## Interface layer

The main application uses a multi-page and multi-window structure:

- Main command deck
- Settings
- Research and case windows
- Data Desk
- Map Desk
- Assistant
- Analysis Room
- OSINT Desk
- Camera and communications workspaces
- Launch Desk

Trusted Project V windows share selected local records and lock state.

## Mapping

MapLibre provides the map engine. Deck.gl supports advanced data visualization. Operational records include markers, circles, rectangles, polygons, routes, measurements, and geofenced alert rules.

## Data routing

A request may use:

1. Configured Project V gateway, when built and configured
2. Local Watchtower sidecar
3. Direct provider credentials
4. Optional inherited upstream compatibility route

Availability depends on release configuration and provider settings.

## Storage

### localStorage

Used for smaller configuration and workspace records.

### IndexedDB

Used for larger local records such as research, cases, and workbooks.

### Operating-system credential vault

Used for supported desktop secrets.

### Exported backups

Configuration and full archives are user-created files. Protected archives use password-derived encryption.

## AI architecture

The local AI path uses a configured Ollama endpoint.

Command Assistant can receive selected workspace and research context.

Analysis Room runs specialist roles sequentially and keeps their outputs separate before a Briefing Officer synthesis.

## Plugin architecture

Plugins are declarative modules rendered in sandboxed frames. The host supplies a limited bridge based on declared and granted permissions.

Plugins do not become trusted native Tauri modules merely by being installed.

## Remote content

Remote pages open in untrusted or restricted webviews or in an external browser. They are intentionally separated from trusted Project V IPC.

## Build and packaging

The production path is:

TypeScript type check  
→ Vite bundle  
→ Rust release build  
→ Tauri packaging  
→ NSIS and MSI

The current unsigned 1.0.0 build does not enable the production updater feature.
