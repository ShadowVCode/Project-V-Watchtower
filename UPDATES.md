# Project V Watchtower — Development Updates

> **Current status:** `v1.1.0-1` · **PRE-ALPHA**  
> Windows production candidate built successfully. Linux, Synology Core, and Android Companion are planned next.

---

## 🚀 Current Release

### Project V Watchtower `v1.1.0-1`

The current Windows production candidate has been built and packaged.

| Platform | Package | Status |
|---|---|---|
| Windows x64 | NSIS `.exe` installer | ✅ Built |
| Windows x64 | MSI `.msi` installer | ✅ Built |
| Windows x64 | Portable package | ✅ Built |
| Linux x64 | AppImage | 🔜 Next |
| Linux x64 | Debian `.deb` | 🔜 Next |
| Synology | Watchtower Core | 🧭 Planned |
| Android | Mobile Companion | 🧭 Planned |

---

# ✅ Recent Additions

## 🗺️ Map 2.0

A redesigned operational map system with expanded situational-awareness capabilities.

- Updated command-map workflow
- Operational layer controls
- Better integration with Watchtower modules
- Weather and flight-operation handoffs
- Map-based analyst actions

---

## ✈️ Air Operations

A dedicated Air Operations workspace is now available.

### Included capabilities

- Live ADS-B aircraft tracking
- Military, PIA, and LADD aircraft feeds
- Aircraft selection and detailed intelligence
- Flight watchlists
- Aircraft trails
- Locally observed flight history
- Source and altitude filtering
- Aircraft-database filtering
- Independent aircraft intelligence panel
- Independent live contact list
- Full-screen Air Operations command view

---

## 🌦️ Weather Operations

A dedicated Weather Operations workspace has been added.

### Included capabilities

- Live weather radar
- Radar timeline and playback
- Current weather conditions
- 12-hour forecast
- 7-day forecast
- Watched locations
- Local weather history
- Analyst notes
- Copyable weather briefs
- Surface-wind information

---

## ⚠️ Severe Weather Intelligence

Weather Operations now includes active severe-weather analysis.

### Included capabilities

- National Weather Service active alerts
- Tornado and severe-weather warnings
- Alert polygons
- Severity filtering
- Official warning descriptions
- Official instructions
- Storm-motion visualization when available
- Watch-area support
- Weather alert briefs

---

## 🌀 Tropical Operations

A dedicated Tropical Operations desk has been added using NOAA / NHC data.

### Included capabilities

- Active tropical cyclone tracking
- Hurricane intelligence
- Tropical depression intelligence
- Forecast tracks
- Forecast cones
- Forecast points
- Wind-field information
- Tropical development outlooks
- Storm notes
- Operational briefs

---

## 📹 Camera Wall Improvements

Camera source management has been expanded.

### Improvements

- Built-in camera sources can be edited
- Replacement stream URLs can be saved
- Core sources can be reset
- Core cameras can be removed from the active catalog
- Removed core cameras can be restored
- Custom camera sources can be added
- Custom camera sources can be deleted
- Camera configuration remains locally persistent

---

# 🪟 Windows Production

Windows is currently the reference platform for Project V Watchtower.

The current production candidate includes:

- NSIS installer
- MSI installer
- Portable Windows package
- Updated Project V Watchtower packaging icon
- Optimized Tauri production build

### Current Windows milestone

**Project V Watchtower `v1.1.0-1` — Production Candidate**

The Windows release will remain the reference build while the Linux version is prepared.

---

# 🛣️ Development Roadmap

## 1. 🐧 Linux Desktop Release — NEXT

The next major target is a native Linux edition.

Initial testing will focus on:

**Zorin OS / Ubuntu-based Linux**

### Planned Linux packages

- AppImage
- Debian `.deb`

### Goals

- Preserve the full Watchtower desktop experience
- Adapt Windows-specific features where required
- Validate Air Operations
- Validate Weather Operations
- Validate Tropical Operations
- Validate Camera Wall
- Validate Project Lock
- Validate Assistant, Case Desk, and Analysis Room
- Establish a Linux production build pipeline

---

## 2. 🗄️ Watchtower Core — Synology NAS

A lightweight always-on Watchtower Core is planned for compatible Synology systems.

The goal is to allow Watchtower services to continue operating even when the primary Windows or Linux computer is powered off.

### Planned capabilities

- Always-on alert monitoring
- Weather data collection
- Tropical-system monitoring
- Aircraft/watchlist data caching
- Shared operational state
- Event history
- Shared cases and notes
- Private remote access
- Watchtower API
- Browser-accessible Watchtower interface
- Synchronization with desktop and mobile clients

### Intended architecture

```text
                    WATCHTOWER CORE
                    Synology NAS
                      24 / 7
                         │
              ┌──────────┼──────────┐
              │          │          │
           Windows     Linux     Android
          Watchtower  Watchtower Companion
```

---

## 3. 📱 Android Mobile Companion

The Android version will focus on field monitoring and remote access rather than reproducing every desktop-only feature.

### Planned capabilities

- Live alerts
- Weather Operations
- Severe-weather warnings
- Tropical systems
- Aircraft tracking
- Watched aircraft
- Watched locations
- Maps
- Operational briefs
- Case and analyst notes
- Connection to Watchtower Core

---

# 🔗 Future Project V Integration

Project V applications are intended to remain independently usable while gaining controlled interoperability.

## 🧠 Project V AI / Phoenix

Planned integration:

- Search live Watchtower operational data
- Analyze aircraft activity
- Analyze weather threats
- Summarize changing situations
- Compare events across modules
- Natural-language Watchtower queries
- Cross-module operational briefs

Example future queries:

```text
Tell me about CMP809.

Which watched aircraft are approaching severe weather?

Summarize the current tornado warnings.

What changed with this tropical system in the last six hours?

Which watched locations currently have active warnings?
```

---

## 🌐 Project V Browser

Planned integration:

- Open Watchtower intelligence in the hardened browser
- Send researched sources back to Watchtower
- Attach browser research to cases
- Add research findings to timelines
- Controlled source-to-evidence handoff

---

## 💬 ShadowChat

Planned integration:

- Share Watchtower alerts
- Share coordinates
- Share operational briefs
- Share case excerpts
- Share selected intelligence through secure conversations

---

## 🔐 Shadow Gallery / Vault

Planned integration:

- Archive exported cases
- Store reports
- Store screenshots
- Store evidence packages
- Store Watchtower exports
- Secure long-term operational records

---

# 🔭 Longer-Term Goals

- Cross-module event correlation
- Air + weather correlation
- Automated operational briefs
- Improved historical tracking
- Additional data-source integrations
- Expanded Case Desk workflows
- Expanded evidence workflows
- Signed application releases
- Signed automatic updates
- Automated regression testing
- Linux release pipeline
- Android release pipeline
- Watchtower Core synchronization
- Continued performance optimization
- Continued interface refinement

---

# 🧩 Project V Architecture

Project V Watchtower is intended to remain a modular situational-awareness platform rather than becoming one giant application.

| Project | Primary Role |
|---|---|
| **Watchtower** | Situational awareness, operations, maps, alerts |
| **Phoenix / Project V AI** | Local intelligence and analysis |
| **Project V Browser** | Hardened private research |
| **ShadowChat** | Secure communications |
| **Shadow Gallery / Vault** | Secure file and evidence storage |
| **Watchtower Core** | Always-on shared infrastructure |

The long-term goal is controlled integration between these systems while preserving independent operation.

---

## ⚠️ Pre-Alpha Notice

> **Project V Watchtower remains experimental pre-alpha software.**
>
> Features, interfaces, data sources, platform support, and integration plans may change as development continues.
