# Project V Watchtower 1.0.0

## Initial Windows release

Project V Watchtower 1.0.0 establishes the first Project V-branded Windows release line.

## Release assets

- `Project-V-Watchtower-1.0.0-Windows-x64-Setup.exe` — NSIS installer recommended for most users
- `Project-V-Watchtower-1.0.0-Windows-x64.msi` — MSI package for managed or administrative deployment
- `Project-V-Watchtower-1.0.0-Source.zip` — corresponding application source
- `SHA256SUMS.txt` — release asset checksums

## Highlights

- Customizable command deck and workspaces
- Live map and configured situational-awareness feeds
- Research Library
- Case Desk
- Data Desk
- Map Operations and geofenced alerts
- Command Assistant with optional local Ollama
- Multi-agent Analysis Room
- Camera Wall
- Communications Wall
- Restricted Source Browser
- OSINT Desk
- Launch Deck
- Plugin foundation with sandboxing and permissions
- Project Lock
- Backup, recovery, safe mode, and diagnostics
- Network control modes
- Voice-command and spoken-alert foundations
- Project V branding and 1.0.0 release identity

## Build validation

The Windows production build completed successfully using:

- TypeScript type checking
- Vite production bundling
- Rust optimized release compilation
- Tauri Windows packaging
- NSIS bundle generation
- MSI bundle generation

## Important warnings

- Experimental pre-alpha
- No independent security audit
- Installers are not Authenticode-signed
- Windows may display an unknown-publisher or SmartScreen warning
- Automatic updater is not enabled in this unsigned local build
- External feeds and providers may be unavailable or require API keys
- AI-generated analysis may be incorrect
- Some frontend chunks remain large and are a future optimization target

## Upgrade and compatibility note

The first Project V release retains the existing application identifier to avoid silently abandoning compatible local storage, IndexedDB, credentials, cases, research records, and workspace state from the development line.

A future identifier migration should be performed only with a tested data-migration process.

## License

AGPL-3.0-only, with applicable upstream notices and corresponding source provided as a release asset.
