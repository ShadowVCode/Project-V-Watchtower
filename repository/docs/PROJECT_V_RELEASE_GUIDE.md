# Project V Watchtower Release Guide

This guide manufactures the Windows installer, MSI, portable archive, source package, update manifests, signatures, and checksums for Project V Watchtower.

## 1. Release machine

Use a trusted Windows build computer with:

- Node.js and npm
- Rust stable with the MSVC toolchain
- Microsoft C++ Build Tools
- WebView2 build prerequisites
- Git, recommended
- An Authenticode certificate when publisher signing is available

Install project dependencies:

```powershell
npm install
```

Run a development smoke test first:

```powershell
npm run desktop:dev
```

## 2. Set the release version

Set the desired semantic version in `package.json`, then synchronize the desktop files:

```powershell
npm run version:sync
npm run version:check
```

This synchronizes:

- `package.json`
- `src-tauri/tauri.conf.json`
- `src-tauri/Cargo.toml`

## 3. Generate the updater signing key

Run once for the release identity:

```powershell
npm run release:keygen
```

By default, the private key is created outside the repository under:

```text
%USERPROFILE%\.project-v\signing\
```

Back up the private key securely. Losing it prevents future installations from trusting updates signed by a replacement key. Exposing it would allow someone else to sign malicious updates.

## 4. Host locations

Choose HTTPS locations for:

- Tauri updater manifest: `latest.json`
- Project V release manifest: `project-v-release.json`
- Versioned downloadable artifacts
- Release notes or release page

Example layout:

```text
https://updates.example.org/watchtower/latest.json
https://updates.example.org/watchtower/project-v-release.json
https://updates.example.org/watchtower/releases/1.0.0/...
```

These files may be hosted through GitHub Releases plus a static manifest location, a normal HTTPS server, or a future Project V Core. Do not use plain HTTP for a public release.

## 5. Configure the release environment

```powershell
$env:PROJECT_V_UPDATER_PUBLIC_KEY = Get-Content "$HOME\.project-v\signing\watchtower-updater.key.pub" -Raw
$env:TAURI_SIGNING_PRIVATE_KEY = "$HOME\.project-v\signing\watchtower-updater.key"
$env:TAURI_SIGNING_PRIVATE_KEY_PASSWORD = "YOUR_PRIVATE_KEY_PASSWORD"

$env:PROJECT_V_UPDATE_ENDPOINT = "https://updates.example.org/watchtower/latest.json"
$env:PROJECT_V_RELEASE_MANIFEST_URL = "https://updates.example.org/watchtower/project-v-release.json"
$env:PROJECT_V_RELEASE_BASE_URL = "https://updates.example.org/watchtower/releases/1.0.0"
$env:PROJECT_V_RELEASE_PAGE_URL = "https://updates.example.org/watchtower/releases/1.0.0"
```

Never save private signing values in `.env`, source control, screenshots, issue reports, or a public CI log.

## 6. Build all Windows editions

```powershell
npm run release:windows
```

To reuse an already installed `node_modules` directory:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\release\build-windows-release.ps1 -SkipInstall
```

The build performs:

1. Reproducible `npm ci` dependency installation unless skipped
2. Version synchronization
3. Signed updater configuration generation
4. Tauri NSIS and MSI build
5. Portable folder assembly
6. Portable ZIP creation
7. Corresponding source ZIP creation
8. Manifest generation
9. SHA-256 checksum generation
10. Integrity verification

## 7. Verify the output

```powershell
npm run release:verify
```

Review `release\SHA256SUMS.txt` and test the artifacts on a clean Windows virtual machine or separate Windows account.

Minimum test matrix:

### NSIS

- Current-user install
- Machine install when applicable
- First launch
- Existing-data upgrade
- Uninstall behavior

### MSI

- Install and uninstall
- Managed deployment behavior
- Existing-data upgrade

### Portable

- Extract to a writable folder
- Launch without installation
- Create data and verify `ProjectVData`
- Move the folder and relaunch
- Confirm API credentials remain protected by the Windows credential vault
- Replace program files with a newer portable build while retaining data

### Update

- Host `latest.json`, installer, and `.sig`
- Launch an older installed build
- Confirm the update notification
- Download and install
- Confirm restart and version change
- Confirm cases, research, workspaces, plugins, alerts, and settings survive

## 8. Authenticode signing

Updater signatures and Windows publisher signatures solve different problems.

Tauri updater signatures verify the package inside Watchtower. Authenticode signatures identify the publisher to Windows, support timestamping, and can reduce unknown-publisher warnings.

Configure Windows signing according to the certificate provider being used. Keep certificate credentials outside the repository and CI logs.

## 9. Publish in this order

1. Upload installers, portable ZIP, source ZIP, license, release notes, and signatures.
2. Verify every public download checksum.
3. Upload `project-v-release.json` for portable update discovery.
4. Upload `latest.json` **last** so installed clients do not see an update before its artifact is available.
5. Test the public URLs from a clean machine.

## 10. Rollback

Keep at least the previous stable release online:

```text
1.0.0
1.0.1
```

Do not overwrite old versioned artifacts. If a release is bad, remove or replace the current `latest.json` pointer, publish a corrected patch version, and preserve pre-update backup behavior.

## 11. Update channels

The release script accepts a channel:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts\release\build-windows-release.ps1 -Channel stable
```

Planned channels:

- `stable`
- `preview`
- `development`

Use separate manifest URLs for each channel before exposing channel selection in the user interface.

## 12. Connector changes

Validate built-in connector metadata:

```powershell
npm run connectors:validate
```

For the initial release, connector logic remains bundled with the trusted application or local sidecar. Do not distribute remotely executable parser scripts. Future connector packs should be declarative, signed, versioned, permission-scoped, and rollback-capable.

## 13. AGPL and attribution checklist

Before public release:

- Include `LICENSE`
- Publish the matching source archive
- Preserve required notices and attribution
- Document material changes
- Review third-party provider terms
- Avoid implying official World Monitor endorsement
- Avoid distributing private keys, `.env` files, user databases, or API credentials

## 14. Release output

A successful release directory contains installable, portable, source, manifest, signature, checksum, notes, and license artifacts. Filenames may differ slightly according to the Tauri bundler version, and the manifest script discovers the actual generated bundle names.

## Development versus updater-enabled builds

The updater plugin is intentionally disabled for `npm run desktop:dev` and ordinary unsigned local builds. This avoids requiring a production updater endpoint or signing public key during development.

The `npm run release:windows` workflow enables the Cargo feature `updater` and merges `src-tauri/tauri.release.conf.json`. Do not add placeholder signing keys to the normal `tauri.conf.json`.
