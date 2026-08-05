# Support

## Bug reports

Open a GitHub Issue and include:

- Project V Watchtower version
- Windows version
- Installer type: NSIS or MSI
- What you expected
- What happened
- Exact reproducible steps
- Whether the issue occurs after restart
- Relevant error text with secrets removed
- Whether Ollama or external provider keys are involved

## Do not upload

- API keys
- `.env` files
- passwords
- private research
- personal data
- unredacted case files
- sensitive locations
- updater private keys
- Authenticode certificates
- full diagnostic archives without inspection

## Provider problems

A third-party provider can change authentication, rate limits, response formats, or embedded-browser policies without a Project V update.

Before reporting a Project V bug:

1. Check whether the provider works in a normal browser.
2. Review the configured API key.
3. Review network mode.
4. Confirm system time and internet access.
5. Capture only non-sensitive error details.

## Local AI problems

Include:

- Ollama version
- Model name
- Approximate available memory
- Whether the endpoint connection test passes
- Whether a small prompt succeeds
- Whether the problem occurs in Command Assistant, Analysis Room, or both

Do not attach private prompt content unless it is necessary and safely redacted.

## Security issues

Follow [SECURITY.md](SECURITY.md). Do not disclose exploit details in a public issue.
