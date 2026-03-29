"""OpenF1 sensors."""
from __future__ import annotations

from homeassistant.components.sensor import SensorEntity, SensorDeviceClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTemperature
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import CONF_FAVORITE_DRIVER, DEFAULT_FAVORITE_DRIVER, DOMAIN
from .coordinator import OpenF1Coordinator


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: OpenF1Coordinator = hass.data[DOMAIN][entry.entry_id]

    entities: list[SensorEntity] = [
        OpenF1SessionSensor(coordinator, entry),
        OpenF1StandingsSensor(coordinator, entry),
        OpenF1FastestLapSensor(coordinator, entry),
        OpenF1TrackStatusSensor(coordinator, entry),
        OpenF1WeatherSensor(coordinator, entry),
        OpenF1NextEventSensor(coordinator, entry),
        OpenF1IntervalsSensor(coordinator, entry),
        OpenF1ChampionshipSensor(coordinator, entry),
    ]

    fav = (
        entry.options.get(CONF_FAVORITE_DRIVER)
        or entry.data.get(CONF_FAVORITE_DRIVER, DEFAULT_FAVORITE_DRIVER)
    )
    if fav and str(fav).strip().isdigit():
        entities.append(OpenF1FavoriteDriverSensor(coordinator, entry, int(fav.strip())))

    async_add_entities(entities)


# ── Base ──────────────────────────────────────────────────────────────────────

class _OpenF1SensorBase(SensorEntity):
    _attr_has_entity_name = True
    _attr_should_poll = False

    def __init__(self, coordinator: OpenF1Coordinator, entry: ConfigEntry) -> None:
        self._coordinator = coordinator
        self._entry = entry

    async def async_added_to_hass(self) -> None:
        self._coordinator.async_add_listener(self.async_write_ha_state)

    @property
    def device_info(self) -> dict:
        return {
            "identifiers": {(DOMAIN, self._entry.entry_id)},
            "name": "OpenF1",
            "manufacturer": "OpenF1",
            "model": "Formula 1 Live Data",
            "entry_type": "service",
            "configuration_url": "https://openf1.org",
        }

    @property
    def _data(self) -> dict:
        return self._coordinator.data or {}


# ── Current session ───────────────────────────────────────────────────────────

class OpenF1SessionSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:flag-checkered"

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_session"
        self._attr_name = "Current Session"

    @property
    def native_value(self) -> str:
        s = self._data.get("session", {})
        if s.get("cdn_locked"):
            # cdn_locked but api_restricted=False means we have openf1.org live data
            if not self._data.get("api_restricted"):
                return f"{s.get('session_type', 'Race')} | Live"
            return f"{s.get('session_type', 'Race')} | Live – data locked"
        name = s.get("session_name", "")
        circuit = s.get("circuit", "")
        if name and circuit:
            return f"{name} | {circuit}"
        return name or "No session"

    @property
    def extra_state_attributes(self) -> dict:
        s = self._data.get("session", {})
        return {
            "session_type": s.get("session_type"),
            "circuit": s.get("circuit"),
            "country": s.get("country"),
            "is_active": s.get("is_active", False),
            "cdn_locked": s.get("cdn_locked", False),
            "date_start": s.get("date_start"),
            "date_end": s.get("date_end"),
            "year": s.get("year"),
            "session_key": s.get("session_key"),
        }


# ── Standings ─────────────────────────────────────────────────────────────────

class OpenF1StandingsSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:podium"

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_standings"
        self._attr_name = "Standings"

    @property
    def native_value(self) -> str:
        standings = self._data.get("standings", [])
        if not standings:
            return "No data"
        p1 = standings[0]
        return p1.get("abbreviation") or p1.get("name", "Unknown")

    @property
    def extra_state_attributes(self) -> dict:
        standings = self._data.get("standings", [])
        top3 = [
            f"P{s['position']} {s.get('abbreviation', s.get('name', '?'))}"
            for s in standings[:3]
            if s.get("position")
        ]
        return {
            "p1": standings[0] if standings else None,
            "p2": standings[1] if len(standings) > 1 else None,
            "p3": standings[2] if len(standings) > 2 else None,
            "top3": " | ".join(top3),
            "standings": standings,
            "total_drivers": len(standings),
        }


# ── Fastest lap ───────────────────────────────────────────────────────────────

class OpenF1FastestLapSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:timer-outline"

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_fastest_lap"
        self._attr_name = "Fastest Lap"

    @property
    def native_value(self) -> str | None:
        fl = self._data.get("fastest_lap")
        if not fl:
            return None
        return fl.get("time_str")

    @property
    def extra_state_attributes(self) -> dict:
        fl = self._data.get("fastest_lap") or {}
        return {
            "driver_name": fl.get("driver_name"),
            "driver_abbreviation": fl.get("driver_abbr"),
            "driver_number": fl.get("driver_number"),
            "team": fl.get("team"),
            "lap_number": fl.get("lap_number"),
            "duration_seconds": fl.get("duration_s"),
        }


# ── Track status ──────────────────────────────────────────────────────────────

class OpenF1TrackStatusSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:flag"

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_track_status"
        self._attr_name = "Track Status"

    @property
    def native_value(self) -> str:
        return self._data.get("track_status", {}).get("flag", "Unknown")

    @property
    def icon(self) -> str:
        flag = self._data.get("track_status", {}).get("raw_flag", "")
        icons = {
            "RED": "mdi:flag",
            "SAFETY_CAR": "mdi:car-emergency",
            "VIRTUAL_SAFETY_CAR": "mdi:car-speed-limiter",
            "YELLOW": "mdi:flag",
            "DOUBLE_YELLOW": "mdi:flag",
            "CHEQUERED": "mdi:flag-checkered",
            "GREEN": "mdi:flag",
        }
        return icons.get(flag, "mdi:flag")

    @property
    def extra_state_attributes(self) -> dict:
        ts = self._data.get("track_status", {})
        return {
            "message": ts.get("message"),
            "category": ts.get("category"),
            "lap": ts.get("lap"),
            "recent_messages": ts.get("recent_messages", []),
        }


# ── Weather ───────────────────────────────────────────────────────────────────

class OpenF1WeatherSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:thermometer"
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_native_unit_of_measurement = UnitOfTemperature.CELSIUS

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_weather"
        self._attr_name = "Track Temperature"

    @property
    def native_value(self) -> float | None:
        w = self._data.get("weather", {})
        v = w.get("track_temp")
        try:
            return round(float(v), 1) if v is not None else None
        except (TypeError, ValueError):
            return None

    @property
    def extra_state_attributes(self) -> dict:
        w = self._data.get("weather", {})
        return {
            "air_temperature_c": w.get("air_temp"),
            "humidity_pct": w.get("humidity"),
            "rainfall_mm": w.get("rainfall"),
            "wind_speed_ms": w.get("wind_speed"),
            "wind_direction_deg": w.get("wind_direction"),
        }


# ── Next event ────────────────────────────────────────────────────────────────

class OpenF1NextEventSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:calendar-star"

    def __init__(self, coordinator, entry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_next_event"
        self._attr_name = "Next Race"

    @property
    def native_value(self) -> str:
        ev = self._data.get("next_event", {})
        name = ev.get("name", "")
        days = ev.get("days_until")
        if name and days is not None:
            if days == 0:
                return f"{name} (today)"
            if days == 1:
                return f"{name} (tomorrow)"
            return f"{name} (in {days}d)"
        return name or "Unknown"

    @property
    def extra_state_attributes(self) -> dict:
        ev = self._data.get("next_event", {})
        return {
            "official_name": ev.get("official_name"),
            "country": ev.get("country"),
            "circuit": ev.get("circuit"),
            "date": ev.get("date"),
            "days_until": ev.get("days_until"),
            "year": ev.get("year"),
        }


# ── Favorite driver ───────────────────────────────────────────────────────────

class OpenF1FavoriteDriverSensor(_OpenF1SensorBase):
    _attr_icon = "mdi:racing-helmet"

    def __init__(self, coordinator, entry, driver_number: int) -> None:
        super().__init__(coordinator, entry)
        self._driver_number = driver_number
        self._attr_unique_id = f"{entry.entry_id}_driver_{driver_number}"
        self._attr_name = f"Driver #{driver_number}"

    @property
    def native_value(self) -> str:
        standings = self._data.get("standings", [])
        for s in standings:
            if s.get("driver_number") == self._driver_number:
                pos = s.get("position")
                return f"P{pos}" if pos else "–"
        return "Not in session"

    @property
    def extra_state_attributes(self) -> dict:
        drivers = self._data.get("drivers", {})
        standings = self._data.get("standings", [])
        drv = drivers.get(self._driver_number, {})

        position = None
        for s in standings:
            if s.get("driver_number") == self._driver_number:
                position = s.get("position")
                break

        return {
            "driver_number": self._driver_number,
            "name": drv.get("name"),
            "abbreviation": drv.get("abbreviation"),
            "team": drv.get("team"),
            "team_color": drv.get("team_color"),
            "position": position,
        }


# ── Championship ──────────────────────────────────────────────────────────────

class OpenF1ChampionshipSensor(_OpenF1SensorBase):
    """Current season driver + constructor championship standings."""

    _attr_icon = "mdi:trophy"

    def __init__(self, coordinator: OpenF1Coordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_championship"
        self._attr_name = "Championship"

    @property
    def native_value(self) -> str:
        drivers = self._data.get("championship", {}).get("drivers", [])
        if not drivers:
            return "No data"
        p1 = drivers[0]
        return f"{p1.get('abbreviation', p1.get('name', '?'))} {int(p1.get('points', 0))}pts"

    @property
    def extra_state_attributes(self) -> dict:
        champ = self._data.get("championship", {})
        return {
            "season": champ.get("season"),
            "round": champ.get("round"),
            "drivers": champ.get("drivers", []),
            "constructors": champ.get("constructors", []),
        }


# ── Intervals ──────────────────────────────────────────────────────────────────

class OpenF1IntervalsSensor(_OpenF1SensorBase):
    """Gap to leader for each driver during a live session."""

    _attr_icon = "mdi:timer-outline"

    def __init__(self, coordinator: OpenF1Coordinator, entry: ConfigEntry) -> None:
        super().__init__(coordinator, entry)
        self._attr_unique_id = f"{entry.entry_id}_intervals"
        self._attr_name = "Intervals"

    @property
    def native_value(self) -> str:
        intervals = self._data.get("intervals", [])
        if not intervals:
            return "No data"
        # Show gap of P2 to leader (the most-cited number during a race)
        if len(intervals) > 1:
            p2_gap = intervals[1].get("gap_to_leader")
            if p2_gap is not None:
                try:
                    return f"+{float(p2_gap):.3f}s"
                except (TypeError, ValueError):
                    return str(p2_gap)
        return "0.000s"

    @property
    def extra_state_attributes(self) -> dict:
        intervals = self._data.get("intervals", [])
        return {
            "intervals": intervals,
            "total_drivers": len(intervals),
            "is_active": self._data.get("session", {}).get("is_active", False),
        }
