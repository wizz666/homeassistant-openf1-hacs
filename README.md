# OpenF1 🏎️

**Live Formula 1 data in Home Assistant — real-time race data, standings, track status, and weather.**

> *"Safety Car deployed on lap 34. Verstappen P1, Hamilton P3. Track temp 48°C. Japanese GP in 7 days."*

No API key required. Data fetched directly from F1 Live Timing.

---

## Features

- ✅ Live session status — active race, qualifying, practice
- ✅ Live standings — top 20 positions with driver names, teams, and tyre compounds
- ✅ Fastest lap — time, driver, team, lap number
- ✅ Track status — Green / Safety Car / Red Flag / VSC with latest messages
- ✅ Track weather — temperature, rain, wind
- ✅ Next race countdown — name, circuit, country, days until event
- ✅ Season championship standings — drivers and constructors
- ✅ Gap intervals — live gaps between all cars
- ✅ Optional: dedicated sensor for your favorite driver
- ✅ Works during off-season (shows last session + next race)
- ✅ Adaptive polling — 15 s during live sessions, configured interval otherwise

---

## Installation

### HACS (recommended)
1. Add this repository in HACS → Custom repositories
2. Install **OpenF1**
3. Restart Home Assistant
4. Go to **Settings → Devices & Services → Add Integration** → search **OpenF1**

### Manual
Copy `custom_components/openf1/` to your `/config/custom_components/` directory and restart.

---

## Configuration

| Setting | Description | Default |
|---|---|---|
| **Update interval** | How often to poll (seconds) | 60 |
| **Favorite driver** | Driver number for dedicated sensor (e.g. `44`) | — |

---

## Sensors

| Sensor | State | Key attributes |
|---|---|---|
| `sensor.openf1_current_session` | "Race \| Monza" | circuit, country, is_active, date_start, cdn_locked |
| `sensor.openf1_standings` | P1 driver abbreviation | standings (all 20), p1/p2/p3 |
| `sensor.openf1_fastest_lap` | "1:23.456" | driver_name, team, lap_number |
| `sensor.openf1_track_status` | "Safety Car" | message, category, recent_messages |
| `sensor.openf1_track_temperature` | 48.2 °C | air_temperature_c, rainfall_mm, wind_speed_ms |
| `sensor.openf1_next_race` | "Australian GP (in 12d)" | country, circuit, date, days_until |
| `sensor.openf1_intervals` | "+1.234s" | intervals (all drivers), is_active |
| `sensor.openf1_championship` | "VER 100pts" | drivers, constructors, season, round |
| `sensor.openf1_driver_44` | "P3" | name, team, position *(if favorite driver set)* |

---

## Lovelace examples

### Race weekend card

```yaml
type: markdown
content: >
  ## 🏎️ F1 Live
  **{{ states('sensor.openf1_current_session') }}**
  {% if is_state_attr('sensor.openf1_current_session', 'is_active', true) %}🟢 Live{% endif %}

  🏆 {{ state_attr('sensor.openf1_standings', 'top3') }}

  ⚡ Fastest: {{ states('sensor.openf1_fastest_lap') }}
  — {{ state_attr('sensor.openf1_fastest_lap', 'driver_name') }}

  🚦 {{ states('sensor.openf1_track_status') }}
  | 🌡️ {{ states('sensor.openf1_track_temperature') }} °C

  📅 Next: {{ states('sensor.openf1_next_race') }}
```

### Full standings card

```yaml
type: markdown
content: >
  ## 🏆 Standings
  {% for s in state_attr('sensor.openf1_standings', 'standings') %}
  **P{{ s.position }}** {{ s.name }} ({{ s.team }})
  {% endfor %}
```

---

## Automation examples

### Notify when red flag

```yaml
alias: F1 Red Flag Alert
trigger:
  - platform: state
    entity_id: sensor.openf1_track_status
    to: "Red"
action:
  - service: notify.mobile_app
    data:
      title: "🚨 Red Flag"
      message: "{{ state_attr('sensor.openf1_track_status', 'message') }}"
```

### Notify when session goes live

```yaml
alias: F1 Session Started
trigger:
  - platform: state
    entity_id: sensor.openf1_current_session
    attribute: is_active
    to: true
action:
  - service: notify.mobile_app
    data:
      title: "🏎️ F1 Live"
      message: "{{ states('sensor.openf1_current_session') }} has started!"
```

---

## Action

### `openf1.refresh`

Immediately fetches new data.

```yaml
service: openf1.refresh
```

---

## Troubleshooting

**Sensors show "No data" or "No session"**
- Expected during off-season — shows the latest finished session
- `sensor.openf1_next_race` will still show the next upcoming event

**Standings empty between races**
- Position data is only available when a session has started
- Check `sensor.openf1_current_session` `is_active` attribute

**Session shows "Race | Live – data locked"**
- F1 restricts live timing access during broadcasts
- The integration will serve the last known standings data until the session ends
- After the session, full data becomes available again

**Data feels stale**
- Default poll interval is 60 seconds — lower it in settings for race weekends
- During active sessions the integration automatically polls every 15 seconds
- Call `openf1.refresh` to force an immediate update

**"F1LT" errors in logs**
- Check that Home Assistant can reach `livetiming.formula1.com`
- Championship data requires access to `api.jolpi.ca`

---

## Data source

Primary data from **F1 Live Timing** (`livetiming.formula1.com`) — the same source the official F1 website uses. Championship standings from **Jolpica** (open Ergast mirror). No API keys required.

---

## License

MIT License — see [LICENSE](LICENSE)
