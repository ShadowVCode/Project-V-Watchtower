# Publishing the GitHub Release

## Recommended structure

The normal Git repository contains documentation, issue templates, and the license.

The compiled `.exe`, `.msi`, source archive, and checksums are attached to a GitHub Release. They are not committed into ordinary Git history.

## Why use GitHub Releases?

GitHub Releases are designed to package software, release notes, and binary assets. GitHub CLI can create a release and upload assets in one command.

## Step 1 — Extract this kit

Place the extracted kit inside the Project V source folder.

Example:

```text
E:\PROJECTS\worldmonitor-2.5.23\
  Project-V-Watchtower-GitHub-Release-Kit-1.0.0\
```

## Step 2 — Prepare output

From the Project V source folder:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File `
  ".\Project-V-Watchtower-GitHub-Release-Kit-1.0.0\scripts\Prepare-ReleaseAssets.ps1" `
  -ProjectRoot "E:\PROJECTS\worldmonitor-2.5.23"
```

The script creates:

```text
github-release-output\
  repository\
  release-assets\
```

Review both folders before publishing.

## Step 3 — Install GitHub CLI

```powershell
winget install --id GitHub.cli
```

Close and reopen PowerShell if `gh` is not immediately recognized.

## Step 4 — Authenticate

```powershell
gh auth login
```

Choose GitHub.com and follow the browser authentication flow.

## Step 5 — Create repository and draft release

Replace the repository owner:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File `
  ".\Project-V-Watchtower-GitHub-Release-Kit-1.0.0\scripts\Publish-GitHubRelease.ps1" `
  -ProjectRoot "E:\PROJECTS\worldmonitor-2.5.23" `
  -Repository "YOUR-GITHUB-USERNAME/Project-V-Watchtower"
```

By default, the script:

1. Creates a new public repository.
2. Commits and pushes the documentation.
3. Creates tag `v1.0.0`.
4. Creates a draft GitHub Release.
5. Uploads the NSIS installer, MSI installer, source archive, and checksums.

## Step 6 — Review the draft

On GitHub:

- Confirm the repository name and description.
- Confirm README formatting.
- Confirm the release is still marked Draft.
- Confirm all four release assets are attached.
- Confirm installer filenames.
- Confirm `SHA256SUMS.txt`.
- Confirm the corresponding source archive.
- Confirm no secrets or private files appear.
- Confirm the warning and unsigned-installer status.
- Publish only after testing the downloaded assets on a clean Windows account or test machine.

## Existing repository

The provided publisher intentionally refuses to overwrite an existing repository. This protects against pushing into the wrong project.

If the target repository already exists, publish the prepared `repository` folder manually or adapt the script only after checking the existing branch and remote.

## GitHub automatic source links

GitHub automatically displays “Source code (zip)” and “Source code (tar.gz)” for the tagged documentation repository.

Those files are not the application source in this documentation-focused release structure.

The release notes and README explicitly direct users to:

`Project-V-Watchtower-1.0.0-Source.zip`
