# Threat Model

## Protected assets

- Provider API keys
- Local research and case records
- Map and timeline records
- Workspace configuration
- Backup archives
- AI prompts and outputs
- User-selected local files
- Trusted native command surface
- Release signing keys

## Adversaries considered

- Malicious or compromised public websites
- Unreviewed plugins
- Poisoned or misleading external data
- Malicious imported configuration
- Accidental user disclosure
- Network observers
- Stolen release assets
- Dependency vulnerabilities
- Weak local passphrases

## Trust assumptions

Project V Watchtower assumes:

- Windows and the user account are not already compromised.
- WebView2 and Tauri enforce their security boundaries correctly.
- The release was downloaded from the intended repository.
- The user verifies checksums.
- Release maintainers protect signing keys and build systems.
- Provider credentials are obtained and stored responsibly.

## In-scope defenses

- Restricted IPC
- Plugin sandboxing
- Permission-gated plugin bridge
- Untrusted webview separation
- Credential-vault use
- Local backups and recovery
- Application lock
- Network request modes
- Source and release checksums
- Explicit AI uncertainty warnings
- Safe application-launch restrictions

## Out of scope

- Kernel compromise
- Administrator-level malware
- Keyloggers and screen capture
- Firmware compromise
- Physical attacks on an unlocked computer
- Provider-side account takeover
- Nation-state endpoint compromise
- Guaranteed anonymous browsing
- Guaranteed data correctness
- Guaranteed model correctness
- Complete prevention of user-authorized data sharing

## Key risks

### Remote content risk

A remote page can track, deceive, or exploit browser vulnerabilities. Keep remote content separated and open externally when embedded behavior is unreliable.

### Plugin risk

Sandboxing reduces capability but cannot make malicious content trustworthy. Review plugin source and requested origins.

### AI risk

Models can hallucinate, follow malicious instructions embedded in sources, or present speculation confidently. Preserve original sources and require analyst review.

### Release risk

Unsigned installers are easier to impersonate. Checksums reduce accidental or transit corruption but are not equivalent to a trusted code-signing identity.

### Backup risk

Backups aggregate sensitive data. Protected archives depend on a strong passphrase and safe endpoint.

### Data-source risk

Public feeds can be incomplete, delayed, manipulated, duplicated, or misinterpreted.

## Security objectives for future releases

- Authenticode signing
- Permanent updater signing identity
- Independent audit
- Stronger release provenance
- Automated secret scanning
- Dependency review
- More extensive adversarial testing
- Better redaction and export controls
