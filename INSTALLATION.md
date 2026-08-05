# Installation Guide

## Choose the correct file

### NSIS installer — recommended

`Project-V-Watchtower-1.0.0-Windows-x64-Setup.exe`

Use this edition for most personal Windows installations.

### MSI installer

`Project-V-Watchtower-1.0.0-Windows-x64.msi`

Use this edition for administrative, managed, or MSI-preferred deployment.

Do not install both editions at the same time unless you are deliberately testing installer behavior.

## Requirements

- Windows 10 or Windows 11, 64-bit
- Microsoft WebView2 Runtime
- Adequate disk space for the program, caches, local records, and optional AI models
- Internet access for live external feeds
- Ollama only when using optional local AI

## Verify the download

The release includes `SHA256SUMS.txt`.

From PowerShell:

```powershell
Get-FileHash ".\Project-V-Watchtower-1.0.0-Windows-x64-Setup.exe" -Algorithm SHA256
```

Compare the result to the matching line in `SHA256SUMS.txt`.

A matching checksum confirms that the file is byte-for-byte identical to the release asset. It does not prove that the publisher's build environment was uncompromised.

## Windows warning

The 1.0.0 installers are not Authenticode-signed. Windows may show:

- Unknown publisher
- Windows protected your PC
- SmartScreen warning

Only continue after verifying that:

1. The file came from the official Project V Watchtower GitHub Release.
2. The SHA-256 checksum matches.
3. The release page and repository have not been unexpectedly replaced or redirected.

## Install

1. Close older Project V Watchtower or World Monitor test builds.
2. Run the selected installer.
3. Review the requested install location and scope.
4. Complete installation.
5. Launch Project V Watchtower.
6. Confirm that the title and interface identify the program as Project V Watchtower.

## First-run checklist

- Open the main command deck.
- Review **Security** settings.
- Review enabled panels and data providers.
- Check **API Keys / Data Sources**.
- Set the preferred workspace.
- Test the world map and primary panels.
- Open the Research Library, Case Desk, Data Desk, Map Desk, OSINT Desk, Assistant, and Analysis Room as applicable.
- Create a configuration backup before extensive customization.

## Optional Ollama setup

Ollama is installed separately and does not belong inside the Project V folder.

After installing Ollama:

1. Pull a model appropriate for the computer.
2. Start Ollama.
3. Open Project V Watchtower's local AI settings.
4. Confirm the endpoint, normally a localhost address.
5. Select a model.
6. Run the connection test.
7. Begin with a small Command Assistant prompt before a multi-agent mission.

Local models may require substantial memory and storage.

## Optional provider credentials

Some live feeds require provider API keys. Add only keys obtained directly from the relevant provider.

Never place public-release secrets in:

- `.env`
- `.env.local`
- GitHub commits
- issue attachments
- screenshots
- release ZIP files

Use the application's supported desktop credential storage.

## Updating

The unsigned 1.0.0 build does not include the production signed automatic updater.

To install a later unsigned release:

1. Export or create a Project V backup.
2. Close the application.
3. Download and verify the new installer.
4. Install the new version.
5. Confirm that workspaces and local records remain available.
6. Restore from backup only if necessary.

## Uninstalling and local data

Uninstalling the program may not remove all WebView2 profile data, local caches, operating-system credentials, or exported files.

Before uninstalling:

- Export needed cases and research.
- Create a backup.
- Remove provider credentials through the application.
- Record any local Ollama model or endpoint settings needed later.

## Troubleshooting

### Application does not open

- Restart Windows.
- Confirm WebView2 Runtime is installed.
- Try Project V Safe Mode if available.
- Check whether endpoint security quarantined the executable.
- Re-download and verify the installer.
- Report reproducible errors without secrets.

### Live panels are empty

- Confirm internet access.
- Review Network mode.
- Check provider credentials.
- Review provider rate limits and service status.
- Test the local sidecar and configured endpoints.
- Remember that some panels can depend on optional inherited compatibility routing.

### Local AI does not connect

- Confirm Ollama is running.
- Confirm the configured endpoint.
- Confirm the selected model exists.
- Test with a smaller model.
- Check Local AI Only or Restricted network behavior.
- Review local firewall rules.

### Separate windows open behind a lock screen

Confirm the main command deck is unlocked, close the affected child window once, and reopen it.
