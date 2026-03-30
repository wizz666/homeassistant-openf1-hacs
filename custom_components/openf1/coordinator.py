"""F1 Live Timing coordinator — fetches live F1 data from livetiming.formula1.com."""
from __future__ import annotations

import asyncio
import json
import logging
import ssl
import threading
from datetime import datetime, timedelta, timezone

import aiohttp

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator

from .const import JOLPICA_BASE, OPENF1_BASE, CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL, DOMAIN

_LOGGER = logging.getLogger(__name__)

F1LT_BASE = "https://livetiming.formula1.com/static"
_UA = {"User-Agent": "Mozilla/5.0 (compatible; HomeAssistant-F1/2.0)"}

# Serve cached data without heavy re-fetch this long after session ends
_LIVE_WINDOW = timedelta(hours=2)
# Cache Index.json — short during/near sessions, longer off-season
_INDEX_TTL_LIVE = timedelta(minutes=2)
_INDEX_TTL_IDLE = timedelta(minutes=10)
# Poll intervals
_POLL_LIVE = timedelta(seconds=15)    # during active session
_POLL_SOON = timedelta(seconds=30)    # session starts within 1 hour
_POLL_IDLE = None                     # use user-configured interval

# F1LT track status code → human label / raw flag key
_STATUS_LABEL = {
    "1": "Green", "2": "Yellow", "3": "Yellow",
    "4": "Safety Car", "5": "Red",
    "6": "Virtual Safety Car", "7": "VSC Ending",
}
_STATUS_RAW = {
    "1": "GREEN", "2": "YELLOW", "3": "DOUBLE_YELLOW",
    "4": "SAFETY_CAR", "5": "RED",
    "6": "VIRTUAL_SAFETY_CAR", "7": "VSC_ENDING",
}

_NOTIFY_FLAGS = {"RED", "SAFETY_CAR", "VIRTUAL_SAFETY_CAR", "VSC_ENDING", "GREEN", "CHEQUERED"}
_FLAG_EMOJI = {
    "RED": "🔴", "SAFETY_CAR": "🚗", "VIRTUAL_SAFETY_CAR": "🟡",
    "VSC_ENDING": "🟡", "GREEN": "🟢", "CHEQUERED": "🏁",
}


# ── MQTT live data collector ───────────────────────────────────────────────────

class MQTTLiveCollector:
    """Background MQTT subscriber for openf1.org push data.

    Connects to mqtt.openf1.org:8883 (TLS) with public credentials openf1/openf1.
    Stores the latest payload per driver for each topic in a thread-safe dict.
    """

    BROKER = "mqtt.openf1.org"
    PORT = 8883
    USER = "openf1"
    PASSWORD = "openf1"
    TOPICS = [
        "v1/position", "v1/intervals", "v1/race_control",
        "v1/weather", "v1/drivers", "v1/stints", "v1/laps",
    ]

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._positions: dict[int, dict] = {}
        self._intervals: dict[int, dict] = {}
        self._weather: dict = {}
        self._rc_messages: list[dict] = []
        self._drivers: dict[int, dict] = {}
        self._stints: dict[int, dict] = {}
        self._laps: dict[int, dict] = {}
        self._client = None
        self._running = False
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        """Start the MQTT background thread (idempotent)."""
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True, name="openf1-mqtt")
        self._thread.start()
        _LOGGER.debug("[MQTT] Background thread started")

    def stop(self) -> None:
        """Stop the MQTT client and background thread."""
        self._running = False
        if self._client:
            try:
                self._client.disconnect()
                self._client.loop_stop()
            except Exception:
                pass
        _LOGGER.debug("[MQTT] Stopped")

    def clear_session(self) -> None:
        """Clear per-session data when a new session begins."""
        with self._lock:
            self._positions.clear()
            self._intervals.clear()
            self._weather.clear()
            self._rc_messages.clear()
            self._drivers.clear()
            self._stints.clear()
            self._laps.clear()

    def has_data(self) -> bool:
        """Return True if position data has been received."""
        with self._lock:
            return bool(self._positions)

    def snapshot(self) -> dict:
        """Return a thread-safe deep-enough copy of the current data."""
        with self._lock:
            return {
                "positions": dict(self._positions),
                "intervals": dict(self._intervals),
                "weather": dict(self._weather),
                "rc_messages": list(self._rc_messages),
                "drivers": dict(self._drivers),
                "stints": dict(self._stints),
                "laps": dict(self._laps),
            }

    # ── Background thread ─────────────────────────────────────────────────────

    def _run(self) -> None:
        try:
            import paho.mqtt.client as mqtt
        except ImportError:
            _LOGGER.error("[MQTT] paho-mqtt not installed — live push disabled")
            return

        # Support both paho v1 and v2
        try:
            cb_ver = mqtt.CallbackAPIVersion.VERSION2
        except AttributeError:
            cb_ver = None

        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                _LOGGER.debug("[MQTT] Connected to openf1.org — subscribing")
                for topic in self.TOPICS:
                    client.subscribe(topic, qos=0)
            else:
                _LOGGER.warning("[MQTT] Connect failed rc=%s", rc)

        def on_message(client, userdata, msg):
            try:
                payload = json.loads(msg.payload.decode())
                self._handle_message(msg.topic, payload)
            except Exception as exc:
                _LOGGER.debug("[MQTT] Parse error on %s: %s", msg.topic, exc)

        def on_disconnect(client, userdata, rc, properties=None, reason_code=None):
            if self._running:
                _LOGGER.debug("[MQTT] Disconnected rc=%s — reconnecting", rc)

        if cb_ver is not None:
            client = mqtt.Client(callback_api_version=cb_ver, client_id="ha-openf1")
        else:
            client = mqtt.Client(client_id="ha-openf1")

        client.username_pw_set(self.USER, self.PASSWORD)
        client.tls_set(cert_reqs=ssl.CERT_REQUIRED)
        client.on_connect = on_connect
        client.on_message = on_message
        client.on_disconnect = on_disconnect
        self._client = client

        while self._running:
            try:
                _LOGGER.debug("[MQTT] Connecting to %s:%s", self.BROKER, self.PORT)
                client.connect(self.BROKER, self.PORT, keepalive=60)
                client.loop_forever(retry_first_connection=True)
            except Exception as exc:
                _LOGGER.warning("[MQTT] Error: %s — retry in 30s", exc)
                if self._running:
                    threading.Event().wait(30)

    def _handle_message(self, topic: str, payload: dict) -> None:
        if not isinstance(payload, dict):
            return
        with self._lock:
            if topic == "v1/position":
                num = payload.get("driver_number")
                if num is not None:
                    ex = self._positions.get(num)
                    if ex is None or payload.get("date", "") >= ex.get("date", ""):
                        self._positions[num] = payload

            elif topic == "v1/intervals":
                num = payload.get("driver_number")
                if num is not None:
                    ex = self._intervals.get(num)
                    if ex is None or payload.get("date", "") >= ex.get("date", ""):
                        self._intervals[num] = payload

            elif topic == "v1/weather":
                if payload.get("date", "") >= self._weather.get("date", ""):
                    self._weather = payload

            elif topic == "v1/race_control":
                self._rc_messages.append(payload)
                if len(self._rc_messages) > 100:
                    self._rc_messages = self._rc_messages[-100:]

            elif topic == "v1/drivers":
                num = payload.get("driver_number")
                if num is not None:
                    self._drivers[num] = payload

            elif topic == "v1/stints":
                num = payload.get("driver_number")
                if num is not None:
                    ex = self._stints.get(num)
                    if ex is None or payload.get("stint_number", 0) >= ex.get("stint_number", 0):
                        self._stints[num] = payload

            elif topic == "v1/laps":
                num = payload.get("driver_number")
                if num is not None:
                    ex = self._laps.get(num)
                    if ex is None or payload.get("lap_number", 0) >= ex.get("lap_number", 0):
                        self._laps[num] = payload


class OpenF1Coordinator(DataUpdateCoordinator):
    """Fetches and caches F1 data from livetiming.formula1.com."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry) -> None:
        self._cached_data: dict | None = None

        self._session_key: int | None = None          # F1LT session Key
        self._last_rc_date: str = ""                  # ISO UTC of last processed RC message
        self._championship_cache: dict = {}
        self._championship_fetched: datetime | None = None
        self._index_cache: dict | None = None         # Index.json content
        self._index_fetched: datetime | None = None   # when Index was last fetched
        self._mqtt = MQTTLiveCollector()              # MQTT push collector

        self._base_interval = int(
            entry.options.get(CONF_POLL_INTERVAL)
            or entry.data.get(CONF_POLL_INTERVAL, DEFAULT_POLL_INTERVAL)
        )
        super().__init__(
            hass, _LOGGER, name=DOMAIN,
            update_interval=timedelta(seconds=self._base_interval),
        )
        self.entry = entry

    def _reset_session_state(self) -> None:
        self._last_rc_date = ""
        self._mqtt.clear_session()

    # ── Public update entry point ─────────────────────────────────────────────

    async def _async_update_data(self) -> dict:
        try:
            data = await self._fetch_all()
            data["is_stale"] = False
            data["api_restricted"] = False
            data["last_updated"] = datetime.now(timezone.utc).isoformat()
            self._cached_data = data
            self._adapt_poll_interval(data)
            return data
        except Exception as exc:
            _LOGGER.warning("[F1LT] Fetch failed, serving cached data: %s", exc)
            if self._cached_data:
                stale = dict(self._cached_data)
                stale["is_stale"] = True
                return stale
            return self._empty_skeleton()

    def _adapt_poll_interval(self, data: dict) -> None:
        """Shorten poll interval during live sessions, restore it afterwards."""
        is_active = data.get("session", {}).get("is_active", False)
        next_event = data.get("next_event", {})
        days_until = next_event.get("days_until")

        if is_active:
            new_interval = _POLL_LIVE
        elif days_until == 0:
            # Race/sprint day — sessions likely imminent
            new_interval = _POLL_SOON
        else:
            new_interval = timedelta(seconds=self._base_interval)

        if self.update_interval != new_interval:
            _LOGGER.debug("[F1LT] Adapting poll interval to %s", new_interval)
            self.update_interval = new_interval

    @staticmethod
    def _empty_skeleton() -> dict:
        return {
            "session": {}, "drivers": {}, "standings": [],
            "fastest_lap": None,
            "track_status": {
                "flag": "No connection", "raw_flag": "", "message": "",
                "category": "", "lap": None, "recent_messages": [],
            },
            "weather": {}, "next_event": {},
            "stints": {}, "intervals": [],
            "championship": {"drivers": [], "constructors": [], "season": None, "round": None},
            "is_stale": True, "api_restricted": False, "last_updated": None,
        }

    # ── HTTP helpers ──────────────────────────────────────────────────────────

    async def _get_lt(
        self, http: aiohttp.ClientSession, path: str
    ) -> dict | list | None:
        """GET a file from F1 Live Timing static CDN. Returns parsed JSON or None."""
        url = f"{F1LT_BASE}/{path}"
        try:
            async with http.get(
                url, headers=_UA, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    _LOGGER.debug("[F1LT] %s → HTTP %s", path, resp.status)
                    return None
                raw = await resp.read()
                # Files are UTF-8 with BOM (\ufeff) — strip it
                text = raw.decode("utf-8-sig")
                return __import__("json").loads(text)
        except asyncio.TimeoutError:
            _LOGGER.debug("[F1LT] Timeout: %s", path)
            return None
        except Exception as exc:
            _LOGGER.debug("[F1LT] Error fetching %s: %s", path, exc)
            return None

    async def _get_jolpica(
        self, http: aiohttp.ClientSession, path: str
    ) -> dict | None:
        """GET from Jolpica/Ergast. Returns parsed JSON or None."""
        url = f"{JOLPICA_BASE}{path}"
        try:
            async with http.get(
                url, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    return None
                return await resp.json(content_type=None)
        except Exception as exc:
            _LOGGER.debug("[F1LT] Jolpica error %s: %s", path, exc)
            return None

    async def _get_openf1(
        self, http: aiohttp.ClientSession, path: str
    ) -> list | None:
        """GET from openf1.org REST API. Returns parsed JSON list or None.
        Returns None for error/auth responses (which come back as dicts).
        """
        url = f"{OPENF1_BASE}/{path}"
        try:
            async with http.get(
                url, headers=_UA, timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status not in (200, 401, 403):
                    _LOGGER.debug("[OpenF1] %s → HTTP %s", path, resp.status)
                    return None
                data = await resp.json(content_type=None)
                # Error responses come back as dicts ({"detail": "..."})
                if isinstance(data, dict):
                    _LOGGER.debug("[OpenF1] %s → blocked: %s", path, data.get("detail", "")[:80])
                    return None
                return data if isinstance(data, list) else None
        except Exception as exc:
            _LOGGER.debug("[OpenF1] Error %s: %s", path, exc)
            return None

    # ── Index / session discovery ─────────────────────────────────────────────

    async def _get_index(self, http: aiohttp.ClientSession, year: int, live: bool = False) -> dict | None:
        """Fetch (and cache) the F1LT Index.json for the given year."""
        now = datetime.now(timezone.utc)
        ttl = _INDEX_TTL_LIVE if live else _INDEX_TTL_IDLE
        if (
            self._index_cache is not None
            and self._index_fetched is not None
            and (now - self._index_fetched) < ttl
        ):
            return self._index_cache

        idx = await self._get_lt(http, f"{year}/Index.json")
        if idx and isinstance(idx, dict):
            self._index_cache = idx
            self._index_fetched = now
        return idx

    def _parse_session_time(self, date_str: str, gmt_offset: str) -> datetime | None:
        """Parse F1LT local datetime + GMT offset → UTC datetime."""
        try:
            h, m, s = (int(x) for x in gmt_offset.split(":"))
            offset = timedelta(hours=h, minutes=m, seconds=s)
            local = datetime.fromisoformat(date_str)
            return (local - offset).replace(tzinfo=timezone.utc)
        except Exception:
            return None

    def _find_session(
        self, index: dict, now_utc: datetime
    ) -> tuple[dict | None, dict | None, datetime | None, datetime | None]:
        """Return (session_info, meeting_info, start_utc, end_utc) for best match.

        Priority: active session > most recently completed session.
        """
        best_recent: tuple | None = None
        best_recent_end: datetime | None = None

        for meeting in index.get("Meetings", []):
            for sess in meeting.get("Sessions", []):
                path = sess.get("Path")
                if not path:
                    continue
                gmt = sess.get("GmtOffset", "00:00:00")
                start = self._parse_session_time(sess.get("StartDate", ""), gmt)
                end   = self._parse_session_time(sess.get("EndDate", ""), gmt)
                if not start or not end:
                    continue

                if start <= now_utc <= end:
                    return sess, meeting, start, end

                if end < now_utc:
                    if best_recent_end is None or end > best_recent_end:
                        best_recent = (sess, meeting, start, end)
                        best_recent_end = end

        if best_recent:
            return best_recent
        return None, None, None, None

    def _find_next_race(self, index: dict, now_utc: datetime) -> dict:
        """Find next Race session in Index and return next_event dict."""
        upcoming: list[tuple[datetime, dict, dict]] = []
        for meeting in index.get("Meetings", []):
            for sess in meeting.get("Sessions", []):
                if sess.get("Type") not in ("Race", "Sprint"):
                    continue
                gmt = sess.get("GmtOffset", "00:00:00")
                start = self._parse_session_time(sess.get("StartDate", ""), gmt)
                if start and start > now_utc:
                    upcoming.append((start, sess, meeting))

        if not upcoming:
            return {}

        upcoming.sort(key=lambda x: x[0])
        start, sess, meeting = upcoming[0]
        days = max(0, (start.date() - now_utc.date()).days)

        return {
            "name": meeting.get("Name", ""),
            "official_name": meeting.get("OfficialName", meeting.get("Name", "")),
            "country": meeting.get("Location", ""),
            "circuit": "",  # not in Index; populated below if available
            "date": start.strftime("%Y-%m-%dT%H:%M:%S"),
            "days_until": days,
            "year": start.year,
        }

    # ── Main fetch ────────────────────────────────────────────────────────────

    async def _fetch_all(self) -> dict:
        now_utc = datetime.now(timezone.utc)
        year = now_utc.year

        async with aiohttp.ClientSession() as http:

            # ── 1. Discover current session from Index ────────────────────────
            index = await self._get_index(http, year, live=self._session_key is not None)
            if not index:
                _LOGGER.warning("[F1LT] Could not fetch Index.json for %s", year)
                data = self._empty_skeleton()
                data["is_stale"] = True
                return data

            sess_info, meeting_info, start_utc, end_utc = self._find_session(index, now_utc)

            if not sess_info:
                # No session found at all — return next event only
                data = self._empty_skeleton()
                data["next_event"] = self._find_next_race(index, now_utc)
                return data

            path = sess_info.get("Path")  # None during live Race (F1LT CDN locked)
            session_key = sess_info.get("Key")
            is_active = start_utc <= now_utc <= end_utc

            # F1LT CDN returns 403 / Path=None during live broadcast — flag it
            cdn_locked = is_active and not path

            # ── 2. Reset state if session has changed ────────────────────────
            if session_key != self._session_key:
                _LOGGER.debug("[F1LT] Session changed → %s, resetting state", session_key)
                self._session_key = session_key
                self._reset_session_state()

            # ── 3. Map session info ──────────────────────────────────────────
            sess_type = sess_info.get("Type", "")
            sess_name = sess_info.get("Name", "")
            session_display = f"{sess_type} {sess_info.get('Number', '')}".strip() if sess_type not in ("Race", "Qualifying") else sess_type

            data: dict = {
                "session": {
                    "session_key": session_key,
                    "session_name": sess_name,
                    "session_type": sess_type,
                    "circuit": "",
                    "country": meeting_info.get("Location", "") if meeting_info else "",
                    "date_start": start_utc.isoformat(),
                    "date_end": end_utc.isoformat(),
                    "is_active": is_active,
                    "year": start_utc.year,
                },
                "drivers": {},
                "standings": [],
                "fastest_lap": None,
                "track_status": {
                    "flag": "Unknown", "raw_flag": "", "message": "",
                    "category": "", "lap": None, "recent_messages": [],
                },
                "weather": {},
                "next_event": {},
                "stints": {},
                "intervals": [],
            }

            # ── 4. CDN locked during live broadcast — use MQTT or openf1.org ──
            if cdn_locked:
                if self._mqtt.has_data():
                    _LOGGER.debug("[F1LT] CDN locked — using MQTT live data")
                    snap = self._mqtt.snapshot()
                    live = self._build_from_mqtt(
                        snap, sess_info, meeting_info, start_utc, end_utc, index, now_utc
                    )
                else:
                    _LOGGER.debug("[F1LT] CDN locked — falling back to openf1.org REST")
                    live = await self._fetch_openf1_live(
                        http, sess_info, meeting_info, start_utc, end_utc, index, now_utc
                    )
                live["championship"] = await self._fetch_championship(http, now_utc)
                return live

            # ── 5. Off-season guard: use cache if session ended > 2h ago ─────
            # Only valid if cached data is from the SAME session — otherwise fetch fresh.
            cached_session_key = (self._cached_data or {}).get("session", {}).get("session_key")
            session_is_old = (
                not is_active
                and end_utc is not None
                and now_utc - end_utc > _LIVE_WINDOW
            )

            if (
                session_is_old
                and self._cached_data
                and self._cached_data.get("standings")
                and cached_session_key == session_key
            ):
                cached = dict(self._cached_data)
                cached["next_event"] = self._find_next_race(index, now_utc)
                cached["championship"] = await self._fetch_championship(http, now_utc)
                _LOGGER.debug("[F1LT] Off-season — serving cached data")
                return cached

            # ── 6. Fetch all session endpoints in parallel ───────────────────
            (
                driver_raw,
                timing_raw,
                timing_app_raw,
                track_raw,
                weather_raw,
                rc_raw,
            ) = await asyncio.gather(
                self._get_lt(http, f"{path}DriverList.json"),
                self._get_lt(http, f"{path}TimingData.json"),
                self._get_lt(http, f"{path}TimingAppData.json"),
                self._get_lt(http, f"{path}TrackStatus.json"),
                self._get_lt(http, f"{path}WeatherData.json"),
                self._get_lt(http, f"{path}RaceControlMessages.json"),
            )

            # ── 6. Drivers ───────────────────────────────────────────────────
            drivers: dict[int, dict] = {}
            if isinstance(driver_raw, dict):
                for num_str, info in driver_raw.items():
                    try:
                        num = int(num_str)
                    except (ValueError, TypeError):
                        continue
                    color = info.get("TeamColour", "FFFFFF")
                    drivers[num] = {
                        "name": info.get("FullName", f"#{num}"),
                        "abbreviation": info.get("Tla", f"#{num}"),
                        "team": info.get("TeamName", ""),
                        "team_color": f"#{color}" if not color.startswith("#") else color,
                    }
            data["drivers"] = drivers

            # ── 7. Tyre/stint data ──────────────────────────────────────────
            # TimingAppData.Lines[num].Stints = list of stint dicts
            stints_by_num: dict[int, dict] = {}
            if isinstance(timing_app_raw, dict):
                for num_str, line in timing_app_raw.get("Lines", {}).items():
                    try:
                        num = int(num_str)
                    except (ValueError, TypeError):
                        continue
                    stints = line.get("Stints", [])
                    if isinstance(stints, list) and stints:
                        last = stints[-1]
                        stints_by_num[num] = {
                            "compound": last.get("Compound", ""),
                            "new": last.get("New", "true").lower() == "true" if isinstance(last.get("New"), str) else bool(last.get("New")),
                            "total_laps": last.get("TotalLaps", 0),
                        }

            # ── 8. Standings from TimingData ─────────────────────────────────
            intervals_list: list[dict] = []
            if isinstance(timing_raw, dict):
                lines = timing_raw.get("Lines", {})
                entries = []
                for num_str, line in lines.items():
                    try:
                        num = int(num_str)
                    except (ValueError, TypeError):
                        continue
                    pos = line.get("Line")  # integer position
                    if pos is None:
                        pos = line.get("Position")
                    try:
                        pos = int(pos)
                    except (ValueError, TypeError):
                        pos = 99

                    best_lap = ""
                    bl = line.get("BestLapTime")
                    if isinstance(bl, dict):
                        best_lap = bl.get("Value", "")

                    gap_raw = line.get("TimeDiffToFastest", "")

                    drv = drivers.get(num, {})
                    stint = stints_by_num.get(num, {})

                    entries.append({
                        "position": pos,
                        "driver_number": num,
                        "name": drv.get("name", f"#{num}"),
                        "abbreviation": drv.get("abbreviation", f"#{num}"),
                        "team": drv.get("team", ""),
                        "team_color": drv.get("team_color", "#ffffff"),
                        "best_lap": best_lap,
                        "gap_raw": gap_raw,
                        "in_pit": line.get("InPit", False),
                        "pit_out": line.get("PitOut", False),
                        "num_laps": line.get("NumberOfLaps", 0),
                        "compound": stint.get("compound", ""),
                        "tyre_new": stint.get("new", True),
                        "tyre_laps": stint.get("total_laps", 0),
                    })

                entries.sort(key=lambda x: x["position"])

                # Build standings list (matches sensor expectations)
                standings = []
                for e in entries:
                    standings.append({
                        "position": e["position"],
                        "driver_number": e["driver_number"],
                        "name": e["name"],
                        "abbreviation": e["abbreviation"],
                        "team": e["team"],
                        "team_color": e["team_color"],
                        "compound": e["compound"],
                        "best_lap": e["best_lap"],
                        "gap": e["gap_raw"],
                        "in_pit": e["in_pit"],
                        "num_laps": e["num_laps"],
                    })
                data["standings"] = standings[:20]

                # Build intervals list (matches sensor expectations)
                for e in entries:
                    gap_str = e["gap_raw"]
                    gap_val: float | None = None
                    if gap_str and gap_str not in ("", "LEADER"):
                        try:
                            gap_val = float(gap_str.lstrip("+"))
                        except ValueError:
                            pass
                    intervals_list.append({
                        "position": e["position"],
                        "driver_number": e["driver_number"],
                        "abbreviation": e["abbreviation"],
                        "gap_to_leader": gap_val,
                        "gap_str": gap_str,
                        "in_pit": e["in_pit"],
                    })
                data["intervals"] = intervals_list

                # Fastest lap across all drivers
                best_entry = None
                best_seconds: float = 9999.0
                for e in entries:
                    t = e.get("best_lap", "")
                    if not t:
                        continue
                    secs = self._lap_str_to_seconds(t)
                    if secs and secs < best_seconds:
                        best_seconds = secs
                        best_entry = e

                if best_entry:
                    data["fastest_lap"] = {
                        "time_str": best_entry["best_lap"],
                        "duration_s": best_seconds,
                        "driver_number": best_entry["driver_number"],
                        "driver_name": best_entry["name"],
                        "driver_abbr": best_entry["abbreviation"],
                        "team": best_entry["team"],
                        "lap_number": None,
                    }

            # ── 9. Track status ──────────────────────────────────────────────
            if isinstance(track_raw, dict):
                status_code = track_raw.get("Status", "")
                message = track_raw.get("Message", "")
                flag_label = _STATUS_LABEL.get(str(status_code), message or "Unknown")
                raw_flag = _STATUS_RAW.get(str(status_code), "")
                data["track_status"] = {
                    "flag": flag_label,
                    "raw_flag": raw_flag,
                    "message": message,
                    "category": "",
                    "lap": None,
                    "recent_messages": [],
                }

            # ── 10. Race control messages ────────────────────────────────────
            rc_messages: list[dict] = []
            if isinstance(rc_raw, dict):
                msgs_raw = rc_raw.get("Messages", {})
                # Messages can be dict (keyed 0,1,2...) or list
                if isinstance(msgs_raw, dict):
                    msgs_raw = list(msgs_raw.values())
                for m in msgs_raw:
                    rc_messages.append({
                        "date": m.get("Utc", ""),
                        "category": m.get("Category", ""),
                        "message": m.get("Message", ""),
                        "flag": m.get("Flag", ""),
                        "lap_number": m.get("Lap"),
                    })
                rc_messages.sort(key=lambda x: x["date"])

                if rc_messages:
                    recent_texts = [m["message"] for m in rc_messages[-5:] if m["message"]]
                    data["track_status"]["recent_messages"] = recent_texts

            # ── 11. Weather ──────────────────────────────────────────────────
            if isinstance(weather_raw, dict):
                data["weather"] = {
                    "track_temp": self._to_float(weather_raw.get("TrackTemp")),
                    "air_temp": self._to_float(weather_raw.get("AirTemp")),
                    "humidity": self._to_float(weather_raw.get("Humidity")),
                    "rainfall": self._to_float(weather_raw.get("Rainfall", "0")),
                    "wind_speed": self._to_float(weather_raw.get("WindSpeed")),
                    "wind_direction": self._to_float(weather_raw.get("WindDirection")),
                }

            # ── 12. Next event from Index ────────────────────────────────────
            data["next_event"] = self._find_next_race(index, now_utc)

            # ── 13. Race control notifications ───────────────────────────────
            if is_active and rc_messages:
                await self._notify_race_control(rc_messages)

            # ── 14. Championship (Jolpica, hourly) ───────────────────────────
            data["championship"] = await self._fetch_championship(http, now_utc)

            return data

    # ── OpenF1.org live data (CDN fallback) ───────────────────────────────────

    async def _fetch_openf1_live(
        self,
        http: aiohttp.ClientSession,
        sess_info: dict,
        meeting_info: dict | None,
        start_utc: datetime,
        end_utc: datetime,
        index: dict,
        now_utc: datetime,
    ) -> dict:
        """Fetch real-time data from openf1.org when F1LT CDN is locked."""
        (
            drivers_raw, positions_raw, intervals_raw,
            rc_raw, weather_raw, stints_raw,
        ) = await asyncio.gather(
            self._get_openf1(http, "drivers?session_key=latest"),
            self._get_openf1(http, "position?session_key=latest"),
            self._get_openf1(http, "intervals?session_key=latest"),
            self._get_openf1(http, "race_control?session_key=latest"),
            self._get_openf1(http, "weather?session_key=latest"),
            self._get_openf1(http, "stints?session_key=latest"),
        )

        sess_type = sess_info.get("Type", "")
        sess_name = sess_info.get("Name", "")

        # If openf1.org returned nothing (it also blocks during live sessions
        # without an API key), fall back to last cached data.
        if not drivers_raw and not positions_raw and not intervals_raw:
            _LOGGER.debug("[OpenF1] Live blocked on all endpoints — serving cached data")
            base = dict(self._cached_data) if self._cached_data else self._empty_skeleton()
            base["session"] = {
                **base.get("session", {}),
                "session_key": sess_info.get("Key"),
                "session_name": sess_name,
                "session_type": sess_type,
                "country": meeting_info.get("Location", "") if meeting_info else "",
                "date_start": start_utc.isoformat(),
                "date_end": end_utc.isoformat(),
                "is_active": True,
                "cdn_locked": True,
            }
            base["next_event"] = self._find_next_race(index, now_utc)
            base["api_restricted"] = True
            base["is_stale"] = True
            return base

        data: dict = {
            "session": {
                "session_key": sess_info.get("Key"),
                "session_name": sess_name,
                "session_type": sess_type,
                "circuit": "",
                "country": meeting_info.get("Location", "") if meeting_info else "",
                "date_start": start_utc.isoformat(),
                "date_end": end_utc.isoformat(),
                "is_active": True,
                "cdn_locked": True,
            },
            "drivers": {},
            "standings": [],
            "fastest_lap": None,
            "track_status": {
                "flag": "Unknown", "raw_flag": "", "message": "",
                "category": "", "lap": None, "recent_messages": [],
            },
            "weather": {},
            "next_event": self._find_next_race(index, now_utc),
            "stints": {},
            "intervals": [],
            "api_restricted": False,
            "is_stale": False,
        }

        # ── Drivers ──────────────────────────────────────────────────────────
        drivers: dict[int, dict] = {}
        if isinstance(drivers_raw, list):
            for d in drivers_raw:
                num = d.get("driver_number")
                if num is None:
                    continue
                color = d.get("team_colour", "FFFFFF") or "FFFFFF"
                drivers[num] = {
                    "name": d.get("full_name", f"#{num}"),
                    "abbreviation": d.get("name_acronym", f"#{num}"),
                    "team": d.get("team_name", ""),
                    "team_color": f"#{color}" if not color.startswith("#") else color,
                }
        data["drivers"] = drivers

        # ── Stints (current tyre per driver) ─────────────────────────────────
        latest_stint: dict[int, dict] = {}
        if isinstance(stints_raw, list):
            for s in stints_raw:
                num = s.get("driver_number")
                if num is None:
                    continue
                ex = latest_stint.get(num)
                if ex is None or s.get("stint_number", 0) > ex.get("stint_number", 0):
                    latest_stint[num] = s

        # ── Latest position per driver ────────────────────────────────────────
        latest_pos: dict[int, dict] = {}
        if isinstance(positions_raw, list):
            for p in positions_raw:
                num = p.get("driver_number")
                if num is None:
                    continue
                ex = latest_pos.get(num)
                if ex is None or p.get("date", "") > ex.get("date", ""):
                    latest_pos[num] = p

        # ── Latest interval per driver ────────────────────────────────────────
        latest_iv: dict[int, dict] = {}
        if isinstance(intervals_raw, list):
            for iv in intervals_raw:
                num = iv.get("driver_number")
                if num is None:
                    continue
                ex = latest_iv.get(num)
                if ex is None or iv.get("date", "") > ex.get("date", ""):
                    latest_iv[num] = iv

        # ── Build standings + intervals ───────────────────────────────────────
        entries = []
        for num, pos_data in latest_pos.items():
            drv = drivers.get(num, {})
            stint = latest_stint.get(num, {})
            iv = latest_iv.get(num, {})
            gap_val = iv.get("gap_to_leader")
            if isinstance(gap_val, (int, float)) and gap_val > 0:
                gap_str = f"+{gap_val:.3f}"
            else:
                gap_str = "LEADER"
            entries.append({
                "position": pos_data.get("position", 99),
                "driver_number": num,
                "name": drv.get("name", f"#{num}"),
                "abbreviation": drv.get("abbreviation", f"#{num}"),
                "team": drv.get("team", ""),
                "team_color": drv.get("team_color", "#ffffff"),
                "compound": (stint.get("compound") or "").upper(),
                "best_lap": "",
                "gap": gap_str,
                "gap_to_leader": gap_val,
                "in_pit": False,
                "num_laps": stint.get("lap_end") or 0,
            })
        entries.sort(key=lambda x: x["position"])

        data["standings"] = [{
            "position": e["position"],
            "driver_number": e["driver_number"],
            "name": e["name"],
            "abbreviation": e["abbreviation"],
            "team": e["team"],
            "team_color": e["team_color"],
            "compound": e["compound"],
            "best_lap": e["best_lap"],
            "gap": e["gap"],
            "in_pit": e["in_pit"],
            "num_laps": e["num_laps"],
        } for e in entries[:20]]

        data["intervals"] = [{
            "position": e["position"],
            "driver_number": e["driver_number"],
            "abbreviation": e["abbreviation"],
            "gap_to_leader": e["gap_to_leader"],
            "gap_str": e["gap"],
            "in_pit": e["in_pit"],
        } for e in entries]

        # ── Race control + track status ───────────────────────────────────────
        if isinstance(rc_raw, list) and rc_raw:
            rc_sorted = sorted(rc_raw, key=lambda x: x.get("date", ""))
            recent_texts = [m.get("message", "") for m in rc_sorted[-5:] if m.get("message")]

            # Find last known flag state
            for msg in reversed(rc_sorted):
                raw_flag = (msg.get("flag") or "").upper().replace(" ", "_")
                if raw_flag:
                    flag_label = raw_flag.replace("_", " ").title()
                    data["track_status"] = {
                        "flag": flag_label,
                        "raw_flag": raw_flag,
                        "message": msg.get("message", ""),
                        "category": msg.get("category", ""),
                        "lap": msg.get("lap_number"),
                        "recent_messages": recent_texts,
                    }
                    break
            else:
                data["track_status"]["recent_messages"] = recent_texts

            # Fire notifications for important flag changes
            rc_for_notify = [{
                "date": m.get("date", ""),
                "category": m.get("category", ""),
                "message": m.get("message", ""),
                "flag": (m.get("flag") or "").upper().replace(" ", "_"),
                "lap_number": m.get("lap_number"),
            } for m in rc_sorted]
            await self._notify_race_control(rc_for_notify)

        # ── Weather (latest entry) ────────────────────────────────────────────
        if isinstance(weather_raw, list) and weather_raw:
            w = sorted(weather_raw, key=lambda x: x.get("date", ""))[-1]
            data["weather"] = {
                "track_temp": self._to_float(w.get("track_temperature")),
                "air_temp": self._to_float(w.get("air_temperature")),
                "humidity": self._to_float(w.get("humidity")),
                "rainfall": self._to_float(w.get("rainfall")),
                "wind_speed": self._to_float(w.get("wind_speed")),
                "wind_direction": self._to_float(w.get("wind_direction")),
            }

        _LOGGER.debug(
            "[OpenF1] Live data: %d drivers, %d rc messages",
            len(drivers), len(rc_raw) if isinstance(rc_raw, list) else 0,
        )
        return data

    # ── Build data from MQTT snapshot ─────────────────────────────────────────

    def _build_from_mqtt(
        self,
        snap: dict,
        sess_info: dict,
        meeting_info: dict | None,
        start_utc: datetime,
        end_utc: datetime,
        index: dict,
        now_utc: datetime,
    ) -> dict:
        """Build coordinator data dict from MQTT snapshot (synchronous)."""
        sess_type = sess_info.get("Type", "")
        sess_name = sess_info.get("Name", "")

        data: dict = {
            "session": {
                "session_key": sess_info.get("Key"),
                "session_name": sess_name,
                "session_type": sess_type,
                "circuit": "",
                "country": meeting_info.get("Location", "") if meeting_info else "",
                "date_start": start_utc.isoformat(),
                "date_end": end_utc.isoformat(),
                "is_active": True,
                "cdn_locked": True,
            },
            "drivers": {},
            "standings": [],
            "fastest_lap": None,
            "track_status": {
                "flag": "Unknown", "raw_flag": "", "message": "",
                "category": "", "lap": None, "recent_messages": [],
            },
            "weather": {},
            "next_event": self._find_next_race(index, now_utc),
            "stints": {},
            "intervals": [],
            "api_restricted": False,
            "is_stale": False,
        }

        # ── Drivers ──────────────────────────────────────────────────────────
        drivers: dict[int, dict] = {}
        for num, d in snap["drivers"].items():
            color = d.get("team_colour", "FFFFFF") or "FFFFFF"
            drivers[num] = {
                "name": d.get("full_name", f"#{num}"),
                "abbreviation": d.get("name_acronym", f"#{num}"),
                "team": d.get("team_name", ""),
                "team_color": f"#{color}" if not color.startswith("#") else color,
            }
        # Fallback: build from position keys if driver list not yet received
        if not drivers:
            for num in snap["positions"]:
                drivers[num] = {
                    "name": f"#{num}", "abbreviation": f"#{num}",
                    "team": "", "team_color": "#ffffff",
                }
        data["drivers"] = drivers

        # ── Standings + intervals ─────────────────────────────────────────────
        entries = []
        for num, pos_data in snap["positions"].items():
            drv = drivers.get(num, {})
            stint = snap["stints"].get(num, {})
            iv = snap["intervals"].get(num, {})
            lap_data = snap["laps"].get(num, {})

            gap_val = iv.get("gap_to_leader")
            if isinstance(gap_val, (int, float)) and gap_val > 0:
                gap_str = f"+{gap_val:.3f}"
            else:
                gap_str = "LEADER"

            # Best lap from laps topic (duration in seconds)
            best_lap_str = ""
            lap_dur = lap_data.get("lap_duration")
            if lap_dur:
                try:
                    mins = int(lap_dur) // 60
                    secs = float(lap_dur) - mins * 60
                    best_lap_str = f"{mins}:{secs:06.3f}"
                except (ValueError, TypeError):
                    pass

            entries.append({
                "position": pos_data.get("position", 99),
                "driver_number": num,
                "name": drv.get("name", f"#{num}"),
                "abbreviation": drv.get("abbreviation", f"#{num}"),
                "team": drv.get("team", ""),
                "team_color": drv.get("team_color", "#ffffff"),
                "compound": (stint.get("compound") or "").upper(),
                "best_lap": best_lap_str,
                "gap": gap_str,
                "gap_to_leader": gap_val,
                "in_pit": False,
                "num_laps": stint.get("lap_end") or 0,
            })
        entries.sort(key=lambda x: x["position"])

        data["standings"] = [{
            "position": e["position"],
            "driver_number": e["driver_number"],
            "name": e["name"],
            "abbreviation": e["abbreviation"],
            "team": e["team"],
            "team_color": e["team_color"],
            "compound": e["compound"],
            "best_lap": e["best_lap"],
            "gap": e["gap"],
            "in_pit": e["in_pit"],
            "num_laps": e["num_laps"],
        } for e in entries[:20]]

        data["intervals"] = [{
            "position": e["position"],
            "driver_number": e["driver_number"],
            "abbreviation": e["abbreviation"],
            "gap_to_leader": e["gap_to_leader"],
            "gap_str": e["gap"],
            "in_pit": e["in_pit"],
        } for e in entries]

        # ── Fastest lap ───────────────────────────────────────────────────────
        best_entry = None
        best_seconds: float = 9999.0
        for e in entries:
            t = e.get("best_lap", "")
            if not t:
                continue
            secs = self._lap_str_to_seconds(t)
            if secs and secs < best_seconds:
                best_seconds = secs
                best_entry = e
        if best_entry:
            data["fastest_lap"] = {
                "time_str": best_entry["best_lap"],
                "duration_s": best_seconds,
                "driver_number": best_entry["driver_number"],
                "driver_name": best_entry["name"],
                "driver_abbr": best_entry["abbreviation"],
                "team": best_entry["team"],
                "lap_number": None,
            }

        # ── Race control + track status ───────────────────────────────────────
        rc_messages = snap["rc_messages"]
        if rc_messages:
            rc_sorted = sorted(rc_messages, key=lambda x: x.get("date", ""))
            recent_texts = [m.get("message", "") for m in rc_sorted[-5:] if m.get("message")]
            for msg in reversed(rc_sorted):
                raw_flag = (msg.get("flag") or "").upper().replace(" ", "_")
                if raw_flag:
                    flag_label = raw_flag.replace("_", " ").title()
                    data["track_status"] = {
                        "flag": flag_label,
                        "raw_flag": raw_flag,
                        "message": msg.get("message", ""),
                        "category": msg.get("category", ""),
                        "lap": msg.get("lap_number"),
                        "recent_messages": recent_texts,
                    }
                    break
            else:
                data["track_status"]["recent_messages"] = recent_texts

        # ── Weather ───────────────────────────────────────────────────────────
        w = snap["weather"]
        if w:
            data["weather"] = {
                "track_temp": self._to_float(w.get("track_temperature")),
                "air_temp": self._to_float(w.get("air_temperature")),
                "humidity": self._to_float(w.get("humidity")),
                "rainfall": self._to_float(w.get("rainfall")),
                "wind_speed": self._to_float(w.get("wind_speed")),
                "wind_direction": self._to_float(w.get("wind_direction")),
            }

        _LOGGER.debug(
            "[MQTT] Snapshot: %d positions, %d drivers, %d rc",
            len(snap["positions"]), len(snap["drivers"]), len(snap["rc_messages"]),
        )
        return data

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _lap_str_to_seconds(t: str) -> float | None:
        """Convert '1:23.456' → 83.456 seconds."""
        try:
            if ":" in t:
                mins, rest = t.split(":", 1)
                return int(mins) * 60 + float(rest)
            return float(t)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _to_float(v) -> float | None:
        try:
            return float(v) if v is not None else None
        except (ValueError, TypeError):
            return None

    # ── Race control notifications ─────────────────────────────────────────────

    async def _notify_race_control(self, rc_messages: list[dict]) -> None:
        """Fire persistent_notification for important race control messages."""
        if not rc_messages:
            return

        if not self._last_rc_date:
            if rc_messages:
                self._last_rc_date = rc_messages[-1].get("date", "")
            return

        new = [m for m in rc_messages if m.get("date", "") > self._last_rc_date]
        if not new:
            return

        self._last_rc_date = new[-1].get("date", "")

        for msg in new:
            flag = (msg.get("flag") or "").upper().replace(" ", "_")
            category = (msg.get("category") or "").upper()
            is_important = (
                flag in _NOTIFY_FLAGS
                or category in ("SAFETYCAR", "FLAG", "RED FLAG")
            )
            if not is_important:
                continue

            emoji = _FLAG_EMOJI.get(flag, "🏎")
            lap = msg.get("lap_number")
            lap_str = f" · Lap {lap}" if lap else ""
            label = flag.replace("_", " ").title() if flag else category.title()
            text = msg.get("message") or label

            await self.hass.services.async_call(
                "persistent_notification", "create",
                {
                    "title": f"{emoji} F1: {label}{lap_str}",
                    "message": text,
                    "notification_id": f"openf1_rc_{flag.lower() or 'event'}",
                },
                blocking=False,
            )

    # ── Championship standings (Jolpica/Ergast) ────────────────────────────────

    async def _fetch_championship(
        self, http: aiohttp.ClientSession, now_utc: datetime
    ) -> dict:
        """Fetch driver + constructor standings from Jolpica (refreshed hourly)."""
        if (
            self._championship_cache
            and self._championship_fetched
            and (now_utc - self._championship_fetched) < timedelta(hours=1)
        ):
            return self._championship_cache

        year = now_utc.year
        empty = {"drivers": [], "constructors": [], "season": year, "round": None}

        drv_raw, con_raw = await asyncio.gather(
            self._get_jolpica(http, f"/{year}/driverStandings.json"),
            self._get_jolpica(http, f"/{year}/constructorStandings.json"),
        )

        driver_standings = []
        round_num = None

        if drv_raw:
            try:
                lists = drv_raw["MRData"]["StandingsTable"]["StandingsLists"]
                if lists:
                    sl = lists[0]
                    round_num = sl.get("round")
                    for entry in sl.get("DriverStandings", []):
                        drv = entry.get("Driver", {})
                        cons = entry.get("Constructors", [{}])
                        team = cons[0].get("name", "") if cons else ""
                        driver_standings.append({
                            "position": int(entry.get("position", 0)),
                            "points": float(entry.get("points", 0)),
                            "wins": int(entry.get("wins", 0)),
                            "abbreviation": drv.get("code", ""),
                            "name": f"{drv.get('givenName', '')} {drv.get('familyName', '')}".strip(),
                            "team": team,
                        })
            except (KeyError, TypeError, ValueError) as exc:
                _LOGGER.debug("[F1LT] Jolpica driver parse error: %s", exc)

        constructor_standings = []
        if con_raw:
            try:
                lists = con_raw["MRData"]["StandingsTable"]["StandingsLists"]
                if lists:
                    for entry in lists[0].get("ConstructorStandings", []):
                        con = entry.get("Constructor", {})
                        constructor_standings.append({
                            "position": int(entry.get("position", 0)),
                            "points": float(entry.get("points", 0)),
                            "wins": int(entry.get("wins", 0)),
                            "name": con.get("name", ""),
                        })
            except (KeyError, TypeError, ValueError) as exc:
                _LOGGER.debug("[F1LT] Jolpica constructor parse error: %s", exc)

        if not driver_standings:
            return self._championship_cache or empty

        self._championship_cache = {
            "drivers": driver_standings,
            "constructors": constructor_standings,
            "season": year,
            "round": round_num,
        }
        self._championship_fetched = now_utc
        return self._championship_cache
