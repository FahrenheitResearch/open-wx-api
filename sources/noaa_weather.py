"""NOAA Weather Observations — NWS API, NDBC Buoys, IEM Deep Integration.

Three data sources are integrated here:

1. NWS Observation API (National Weather Service)
   Surface weather observations from ~3,000 ASOS/AWOS airport stations
   nationwide.  The api.weather.gov REST API returns JSON-LD / GeoJSON and
   requires only a User-Agent header (no auth key).
   Base URL: https://api.weather.gov/

2. NDBC (National Data Buoy Center)
   ~900 active ocean buoys and coastal C-MAN stations.  Data is fetched from
   NDBC's pipe-delimited text files; the realtime2 directory is the most
   comprehensive data source (2-minute or hourly updates depending on station).
   Base URL: https://www.ndbc.noaa.gov/

3. IEM (Iowa Environmental Mesonet) — deep integration
   The most comprehensive free weather data aggregator in the US.  This module
   extends the basic IEM coverage in mesonets.py with the full GeoJSON station
   network endpoint, the ASOS CSV historical archive, the JSON observation
   history endpoint, and the daily-summary API.
   Base URL: https://mesonet.agron.iastate.edu/

All APIs are free with no authentication required.
"""
from __future__ import annotations

import asyncio
import io
import csv
import logging
import re
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from fastapi import APIRouter, HTTPException, Query, Path

import cache
import config
from models import Reading, Station, StationDetail

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Source registry metadata
# ---------------------------------------------------------------------------
SOURCE_NAME = "NOAA Weather & Buoys"
SOURCE_DESCRIPTION = (
    "NWS observation stations (ASOS/AWOS), NDBC marine buoys/C-MAN stations, "
    "and Iowa Environmental Mesonet (IEM) deep API integration for comprehensive "
    "US surface weather observations."
)
SOURCE_META = {
    "url": "https://api.weather.gov/",
    "auth_required": False,
    "update_frequency": "5 min (ASOS), 10 min (buoys), varies (IEM)",
    "coverage": "United States + coastal waters",
    "params": [
        "temperature", "wind", "pressure", "precipitation",
        "humidity", "wave_height", "sea_temperature",
    ],
    "category": "weather",
}

router = APIRouter(prefix="/api/noaa_weather", tags=["NOAA Weather & Buoys"])

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NWS_BASE = "https://api.weather.gov"
NDBC_BASE = "https://www.ndbc.noaa.gov"
IEM_BASE = "https://mesonet.agron.iastate.edu"

# NWS returns SI values with unitCode strings such as
# "wmoUnit:degC", "wmoUnit:m_s-1", "wmoUnit:Pa".
# This map converts them to display-friendly units with optional scale factors.
# Format: unitCode_suffix -> (display_unit, multiplier)
_NWS_UNIT_MAP: dict[str, tuple[str, float]] = {
    "degC":          ("°C",    1.0),
    "degree_(angle)":("deg",   1.0),
    "degree_C":      ("°C",    1.0),
    "m":             ("m",     1.0),
    "m_s-1":         ("m/s",   1.0),
    "km_h-1":        ("km/h",  1.0),
    "Pa":            ("hPa",   0.01),   # Pascals -> hectopascals
    "percent":       ("%",     1.0),
    "m3_m-3":        ("%",   100.0),    # volumetric fraction -> percent
    "km":            ("km",    1.0),
    "mm":            ("mm",    1.0),
}

# NWS property keys -> (canonical parameter name, display name)
_NWS_OBS_PROPS: dict[str, tuple[str, str]] = {
    "temperature":              ("temperature",          "Temperature"),
    "dewpoint":                 ("dewpoint",             "Dewpoint"),
    "windDirection":            ("wind_direction",       "Wind Direction"),
    "windSpeed":                ("wind_speed",           "Wind Speed"),
    "windGust":                 ("wind_gust",            "Wind Gust"),
    "barometricPressure":       ("barometric_pressure",  "Barometric Pressure"),
    "seaLevelPressure":         ("pressure_msl",         "Sea-Level Pressure"),
    "visibility":               ("visibility",           "Visibility"),
    "maxTemperatureLast24Hours":("temperature_max_24h",  "Max Temp (24h)"),
    "minTemperatureLast24Hours":("temperature_min_24h",  "Min Temp (24h)"),
    "precipitationLastHour":    ("precipitation_1h",     "Precipitation (1h)"),
    "precipitationLast3Hours":  ("precipitation_3h",     "Precipitation (3h)"),
    "precipitationLast6Hours":  ("precipitation_6h",     "Precipitation (6h)"),
    "relativeHumidity":         ("relative_humidity",    "Relative Humidity"),
    "windChill":                ("wind_chill",           "Wind Chill"),
    "heatIndex":                ("heat_index",           "Heat Index"),
}

# NDBC standard meteorological column positions (realtime2 format).
# Header row defines names; this map translates them to canonical params.
_NDBC_COL_MAP: dict[str, tuple[str, str]] = {
    "WDIR":   ("wind_direction",         "deg"),
    "WSPD":   ("wind_speed",             "m/s"),
    "GST":    ("wind_gust",              "m/s"),
    "WVHT":   ("wave_height",            "m"),
    "DPD":    ("dominant_wave_period",   "s"),
    "APD":    ("average_wave_period",    "s"),
    "MWD":    ("wave_direction",         "deg"),
    "PRES":   ("air_pressure",           "hPa"),
    "ATMP":   ("air_temperature",        "°C"),
    "WTMP":   ("sea_surface_temperature","°C"),
    "DEWP":   ("dewpoint",               "°C"),
    "VIS":    ("visibility",             "km"),
    "PTDY":   ("pressure_tendency",      "hPa"),
    "TIDE":   ("tide",                   "ft"),
}

# IEM variable -> (canonical parameter name, unit)
# Extended beyond what mesonets.py covers to include ASOS-specific fields.
_IEM_VAR_MAP: dict[str, tuple[str, str]] = {
    "tmpf":              ("temperature",         "°F"),
    "tmpc":              ("temperature",         "°C"),
    "dwpf":              ("dewpoint",            "°F"),
    "dwpc":              ("dewpoint",            "°C"),
    "relh":              ("relative_humidity",   "%"),
    "sknt":              ("wind_speed",          "kt"),
    "drct":              ("wind_direction",      "deg"),
    "gust":              ("wind_gust",           "kt"),
    "p01i":              ("precipitation_1h",    "in"),
    "p01m":              ("precipitation_1h",    "mm"),
    "alti":              ("altimeter",           "inHg"),
    "mslp":              ("pressure_msl",        "mb"),
    "vsby":              ("visibility",          "mi"),
    "feel":              ("apparent_temperature","°F"),
    "peak_wind_gust":    ("peak_wind_gust",      "kt"),
    "peak_wind_drct":    ("peak_wind_direction", "deg"),
    "snowdepth":         ("snow_depth",          "in"),
    "max_tmpf_6hr":      ("temperature_max_6h",  "°F"),
    "min_tmpf_6hr":      ("temperature_min_6h",  "°F"),
    "max_tmpf_24hr":     ("temperature_max_24h", "°F"),
    "min_tmpf_24hr":     ("temperature_min_24h", "°F"),
    "pday":              ("precipitation_day",   "in"),
    "pmonth":            ("precipitation_month", "in"),
    "c1tmpf":            ("soil_temperature_4in","°F"),
    "c2tmpf":            ("soil_temperature_8in","°F"),
    "c3tmpf":            ("soil_temperature_12in","°F"),
    "c4tmpf":            ("soil_temperature_24in","°F"),
}

# Well-known IEM network types used for discovery
_IEM_NETWORK_TYPES: dict[str, str] = {
    "ASOS":    "Automated Surface Observing System (FAA/NWS airport stations)",
    "AWOS":    "Automated Weather Observing System",
    "COOP":    "NWS Cooperative Observer Program",
    "DCP":     "Data Collection Platform",
    "ISUSM":   "Iowa State University Soil Moisture Network",
    "MESONET": "State Mesonet",
    "RWIS":    "Road Weather Information System",
}

_HTTP_HEADERS = {
    "User-Agent": config.USER_AGENT,
    "Accept": "application/json",
}


# ---------------------------------------------------------------------------
# HTTP client helper
# ---------------------------------------------------------------------------

def _make_client() -> httpx.AsyncClient:
    """Return a configured async HTTP client."""
    return httpx.AsyncClient(
        headers=_HTTP_HEADERS,
        timeout=config.HTTP_TIMEOUT,
        follow_redirects=True,
    )


# ---------------------------------------------------------------------------
# NWS unit conversion helpers
# ---------------------------------------------------------------------------

def _nws_value_and_unit(prop_block: dict) -> tuple[Optional[float], str]:
    """Extract a numeric value and display unit from an NWS quantity block.

    NWS wraps each measured property as::

        {"value": 21.7, "unitCode": "wmoUnit:degC", "qualityControl": "C"}

    The unitCode prefix is either "wmoUnit:" or "nwsUnit:" followed by a
    UCUM-style unit abbreviation.  We strip the prefix and look up a human-
    friendly display unit together with an optional scale factor.
    """
    raw = prop_block.get("value")
    if raw is None:
        return None, ""

    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None, ""

    unit_code: str = prop_block.get("unitCode", "")
    # Strip namespace prefix: "wmoUnit:degC" -> "degC"
    suffix = unit_code.split(":")[-1] if ":" in unit_code else unit_code

    display_unit, multiplier = _NWS_UNIT_MAP.get(suffix, (suffix, 1.0))
    return value * multiplier, display_unit


def _nws_qc(prop_block: dict) -> Optional[str]:
    """Return the NWS quality-control flag string, or None."""
    qc = prop_block.get("qualityControl")
    return str(qc) if qc else None


# ---------------------------------------------------------------------------
# NWS station / observation parsers
# ---------------------------------------------------------------------------

def _nws_feature_to_station(feature: dict) -> Optional[Station]:
    """Convert a NWS GeoJSON station feature to a Station model.

    Features look like::

        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.6, 45.5]},
            "properties": {
                "stationIdentifier": "KPDX",
                "name": "Portland International Airport",
                "elevation": {"value": 9.1, "unitCode": "wmoUnit:m"},
                "timeZone": "America/Los_Angeles",
                "stateAbbreviation": "OR",
                ...
            }
        }

    NOTE: last_update is NOT populated here because the NWS
    ``/stations?state=XX`` endpoint returns only station metadata — no
    observation timestamps.  To add last_update for the ~25K NWS stations
    would require per-station calls to ``/stations/{id}/observations/latest``
    which is infeasible at scale.  The unified catalog's live overlay
    already covers most of these stations via the IEM ASOS currents
    endpoint (``/api/mesonets/iem/current?network={state}_ASOS``), which
    provides observation timestamps in bulk.
    """
    props = feature.get("properties", {})
    geom = feature.get("geometry") or {}
    coords = geom.get("coordinates", [])

    sid = props.get("stationIdentifier", "")
    if not sid:
        return None

    try:
        lon = float(coords[0]) if len(coords) >= 2 else 0.0
        lat = float(coords[1]) if len(coords) >= 2 else 0.0
    except (TypeError, ValueError, IndexError):
        lat, lon = 0.0, 0.0

    elev_block = props.get("elevation") or {}
    elev_raw = elev_block.get("value")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    return Station(
        id=sid,
        name=props.get("name", sid),
        lat=lat,
        lon=lon,
        source="noaa_weather",
        network="NWS",
        params=list(_NWS_OBS_PROPS.keys()),   # all obs properties potentially available
        elevation=elevation,
        url=f"https://www.weather.gov/wrh/timeseries?site={sid}",
        extra={
            "state": props.get("stateAbbreviation", ""),
            "timezone": props.get("timeZone", ""),
            "county": props.get("county", ""),
            "forecast_zone": props.get("forecastZone", ""),
            "nws_url": props.get("@id", ""),
        },
    )


def _nws_obs_to_readings(obs_props: dict, station_id: str) -> list[Reading]:
    """Extract Reading objects from a NWS observation properties block.

    The NWS observation endpoint returns each meteorological quantity as a
    nested dict with ``value``, ``unitCode``, and ``qualityControl`` fields.
    Missing values have ``"value": null``.
    """
    ts_str: str = obs_props.get("timestamp", "")
    try:
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = datetime.now(timezone.utc)

    readings: list[Reading] = []
    for api_key, (param_name, _display_name) in _NWS_OBS_PROPS.items():
        prop_block = obs_props.get(api_key)
        if not isinstance(prop_block, dict):
            continue
        value, unit = _nws_value_and_unit(prop_block)
        if value is None:
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality=_nws_qc(prop_block),
                source="noaa_weather",
            )
        )
    return readings


# ---------------------------------------------------------------------------
# NDBC parsers
# ---------------------------------------------------------------------------

_NDBC_LOCATION_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*([NS])\s+(\d+(?:\.\d+)?)\s*([EW])")

_NDBC_MISSING = {"MM", "99.0", "999", "9999", "99999", "999.0", "9999.0", "99999.0"}


def _ndbc_station_table_to_stations(text: str) -> list[Station]:
    """Parse NDBC station_table.txt (pipe-delimited) into Station objects.

    File format (first few lines)::

        #station_id|type|location|lat|lon|site_elev|air_temp_elev|anem_elev|
        #           |    |        |   |   |         |             |         |
        41001|3m Discus|BERMUDA|34.700|-72.734|0.5|3.8|5.0|...
        ...

    Comment lines start with '#'.  The first comment line is the header.

    NOTE: last_update is NOT populated here because station_table.txt is a
    pure metadata file with no observation timestamps.  To add last_update
    for ~900 NDBC stations would require per-station fetches of realtime2
    data files (``/data/realtime2/{sid}.txt``) — each ~100 KB.  The
    single-station detail endpoint (``ndbc_get_station``) already does this
    and correctly sets last_update from the latest observation row.
    """
    stations: list[Station] = []
    header: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("#"):
            # The first '#' line contains the column names
            if not header:
                header = [h.strip().lstrip("#").lower() for h in line.split("|")]
            continue

        cols = [c.strip() for c in line.split("|")]
        if len(cols) < 5:
            continue

        # Map by position against header when available, else positional
        def _col(name: str, fallback_idx: int) -> str:
            if name in header:
                idx = header.index(name)
                return cols[idx] if idx < len(cols) else ""
            return cols[fallback_idx] if fallback_idx < len(cols) else ""

        sid = _col("station_id", 0)
        if not sid:
            continue

        location = _col("location", 2)

        lat: Optional[float] = None
        lon: Optional[float] = None
        if "lat" in header and "lon" in header:
            # Legacy layout: numeric lat/lon columns
            try:
                lat = float(_col("lat", 3))
                lon = float(_col("lon", 4))
            except (ValueError, TypeError):
                lat = lon = None
        else:
            # Current layout: "43.098 N 87.850 W (...)" in the LOCATION column
            m = _NDBC_LOCATION_RE.match(location)
            if m:
                lat = float(m.group(1)) * (-1 if m.group(2) == "S" else 1)
                lon = float(m.group(3)) * (-1 if m.group(4) == "W" else 1)
        if lat is None or lon is None:
            continue

        elev_raw = _col("site_elev", 5) if "site_elev" in header else ""
        try:
            elevation = float(elev_raw) if elev_raw else None
        except ValueError:
            elevation = None

        buoy_type = _col("ttype", 2) if "ttype" in header else _col("type", 1)
        station_name = _col("name", -1) if "name" in header else ""
        name = station_name or (location.title() if location else sid)

        stations.append(
            Station(
                id=sid.lower(),
                name=name,
                lat=lat,
                lon=lon,
                source="noaa_weather",
                network="NDBC",
                params=[
                    "wind_speed", "wind_direction", "wind_gust",
                    "wave_height", "dominant_wave_period",
                    "sea_surface_temperature", "air_temperature",
                    "air_pressure",
                ],
                elevation=elevation,
                url=f"https://www.ndbc.noaa.gov/station_page.php?station={sid.lower()}",
                extra={
                    "buoy_type": buoy_type,
                    "location_description": location,
                },
            )
        )
    return stations


def _ndbc_parse_realtime(text: str, station_id: str) -> list[Reading]:
    """Parse NDBC realtime2 text data into Reading objects.

    The realtime2 format has two header lines followed by data rows::

        #YY  MM DD hh mm WDIR WSPD GST  WVHT   DPD   APD MWD   PRES  ATMP  WTMP  DEWP  VIS PTDY  TIDE
        #yr  mo dy hr mn degT m/s  m/s     m   sec   sec degT   hPa  degC  degC  degC  nmi  hPa    ft
        2024 01 15 12 00 230  8.5  10.2   1.8   9.0   7.1 220 1015.3  14.2  16.8  11.0  MM   MM   MM

    Missing values are represented as "MM" (or a fill value like 99.00, 999).
    """
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 3:
        return []

    # First two lines are headers (both start with '#')
    col_names = lines[0].lstrip("#").split()
    # lines[1] is unit hints — skip it

    readings: list[Reading] = []

    for raw_line in lines[2:]:
        if raw_line.startswith("#"):
            continue
        parts = raw_line.split()
        if len(parts) < len(col_names):
            continue

        row = dict(zip(col_names, parts))

        # Build timestamp from YY MM DD hh mm columns
        try:
            year  = int(row.get("YY",  row.get("#YY", "0")))
            month = int(row.get("MM",  "0"))
            day   = int(row.get("DD",  "0"))
            hour  = int(row.get("hh",  "0"))
            minute= int(row.get("mm",  "0"))
            # NDBC uses 2-digit years only in older files; realtime2 is 4-digit
            if year < 100:
                year += 2000
            ts = datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except (ValueError, KeyError):
            continue

        for col, (param, unit) in _NDBC_COL_MAP.items():
            raw = row.get(col)
            if raw is None or raw in _NDBC_MISSING:
                continue
            try:
                value = float(raw)
            except ValueError:
                continue
            # Secondary fill-value check (e.g. 99.0 for wave height means missing)
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param,
                    value=value,
                    unit=unit,
                    quality=None,
                    source="noaa_weather",
                )
            )

    return readings


def _ndbc_latest_obs_text_to_readings(text: str, station_id: str) -> list[Reading]:
    """Parse the NDBC latest_obs/{station}.txt file.

    This file is a concise single-observation text block::

        Station 41001
        ...
        Date: 01/15/2024 12:00 UTC
        Wind: SW (210°) at 8.5 m/s gusting to 10.2 m/s
        ...

    Because the format is verbose prose it is more reliable to use the
    realtime2 endpoint instead.  We delegate there after downloading the
    realtime file.  This function is kept as a thin fallback that parses
    key: value lines.
    """
    # Not the primary path — delegate to realtime2 parser above
    return _ndbc_parse_realtime(text, station_id)


# ---------------------------------------------------------------------------
# IEM helpers
# ---------------------------------------------------------------------------

def _iem_station_from_geojson(feature: dict, network: str) -> Optional[Station]:
    """Build a Station from an IEM GeoJSON network feature.

    NOTE: last_update is NOT populated here because the IEM GeoJSON
    network endpoint (``/geojson/network/{network}.geojson``) returns only
    station metadata (name, coordinates, archive_begin/end).  To add
    last_update would require a separate call to the IEM currents endpoint
    (``/api/1/currents.json?network={network}``).  The unified catalog's
    ``/maps/current`` live overlay already does this via
    ``_collect_iem_map_providers``, so these stations get timestamps
    through that path instead.
    """
    props = feature.get("properties") or {}
    geom  = feature.get("geometry") or {}
    coords = geom.get("coordinates", [])

    sid = props.get("sid") or props.get("id") or props.get("station_id") or ""
    if not sid:
        return None

    try:
        lon = float(coords[0]) if len(coords) >= 2 else 0.0
        lat = float(coords[1]) if len(coords) >= 2 else 0.0
    except (TypeError, ValueError, IndexError):
        lat, lon = 0.0, 0.0

    elev_raw = props.get("elevation") or props.get("elev")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    return Station(
        id=sid,
        name=props.get("sname") or props.get("name") or sid,
        lat=lat,
        lon=lon,
        source="noaa_weather",
        network=network,
        params=[v[0] for v in _IEM_VAR_MAP.values()],
        elevation=elevation,
        url=f"https://mesonet.agron.iastate.edu/sites/site.php?station={sid}&network={network}",
        extra={
            "network": network,
            "county": props.get("county", ""),
            "state": props.get("state", ""),
            "archive_begin": props.get("archive_begin", ""),
        },
    )


def _iem_readings_from_obs(obs: dict, station_id: str) -> list[Reading]:
    """Extract Reading objects from a single IEM observation dict."""
    valid_str = obs.get("utc_valid") or obs.get("valid") or ""
    try:
        ts = datetime.fromisoformat(valid_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = datetime.now(timezone.utc)

    readings: list[Reading] = []
    for iem_key, (param_name, unit) in _IEM_VAR_MAP.items():
        raw = obs.get(iem_key)
        if raw is None or raw == "M":
            continue
        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue
        readings.append(
            Reading(
                station_id=station_id,
                timestamp=ts,
                parameter=param_name,
                value=value,
                unit=unit,
                quality=None,
                source="noaa_weather",
            )
        )
    return readings


def _iem_readings_from_asos_csv(csv_text: str, station_id: str) -> list[Reading]:
    """Parse IEM ASOS CSV archive response into Reading objects.

    The CSV columns produced by the ASOS request endpoint include::

        station,valid,tmpf,dwpf,relh,drct,sknt,p01i,alti,mslp,vsby,gust,...

    IEM uses "M" for missing values and "T" for trace precipitation.
    """
    readings: list[Reading] = []

    # IEM prepends a few comment lines beginning with '#' before the CSV header
    clean_lines: list[str] = []
    for line in csv_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#") or not stripped:
            continue
        clean_lines.append(stripped)

    if not clean_lines:
        return readings

    reader = csv.DictReader(io.StringIO("\n".join(clean_lines)))
    for row in reader:
        valid_str = row.get("valid", "")
        try:
            ts = datetime.strptime(valid_str, "%Y-%m-%d %H:%M").replace(
                tzinfo=timezone.utc
            )
        except ValueError:
            try:
                ts = datetime.fromisoformat(valid_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError):
                continue

        for iem_key, (param_name, unit) in _IEM_VAR_MAP.items():
            raw = row.get(iem_key)
            if raw is None or raw in ("M", "T", ""):
                continue
            try:
                value = float(raw)
            except (TypeError, ValueError):
                continue
            readings.append(
                Reading(
                    station_id=station_id,
                    timestamp=ts,
                    parameter=param_name,
                    value=value,
                    unit=unit,
                    quality=None,
                    source="noaa_weather",
                )
            )
    return readings


# ---------------------------------------------------------------------------
# NWS endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/nws/stations",
    summary="List NWS observation stations by state",
)
async def nws_list_stations(
    state: str = Query(
        ...,
        description="Two-letter US state code (e.g. 'OR', 'TX')",
        min_length=2,
        max_length=2,
    ),
):
    """Return all NWS ASOS/AWOS observation stations in a given state.

    Results are sourced from the api.weather.gov /stations endpoint filtered by
    state.  Each station entry includes location metadata and the set of
    meteorological parameters it reports.

    The station list is cached for one hour to avoid hammering the NWS API.
    """
    state_upper = state.upper()
    cache_key = f"noaa_weather:nws:stations:{state_upper}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NWS_BASE}/stations"
    params_q: dict = {"state": state_upper}

    try:
        async with _make_client() as client:
            resp = await client.get(url, params=params_q)
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NWS API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NWS API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching NWS stations for state %s", state_upper)
        raise HTTPException(status_code=500, detail=str(exc))

    features = data.get("features") or data.get("observationStations", [])
    stations: list[Station] = []
    for feat in features:
        try:
            station = _nws_feature_to_station(feat)
            if station:
                stations.append(station)
        except Exception:
            logger.debug("Failed to parse NWS station feature", exc_info=True)

    result = {
        "source": "noaa_weather",
        "network": "NWS",
        "state": state_upper,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/nws/stations/{station_id}",
    summary="NWS station latest observation",
)
async def nws_get_station(
    station_id: str = Path(
        description="NWS station identifier, e.g. 'KPDX' for Portland Intl Airport"
    ),
):
    """Return station metadata and the latest observation for a NWS station.

    The latest observation is fetched from the /stations/{id}/observations/latest
    endpoint and normalised into the shared Reading model.  NWS values are
    reported in SI units (°C, m/s, Pa) and are converted to more familiar
    display units (°C retained, m/s retained, Pa -> hPa).
    """
    sid = station_id.upper()
    cache_key = f"noaa_weather:nws:station:{sid}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    station_url = f"{NWS_BASE}/stations/{sid}"
    obs_url     = f"{NWS_BASE}/stations/{sid}/observations/latest"

    try:
        async with _make_client() as client:
            station_resp = await client.get(station_url)
            station_resp.raise_for_status()
            station_data = station_resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"NWS station '{sid}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NWS API error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NWS API timeout")
    except Exception as exc:
        logger.exception("Error fetching NWS station metadata for %s", sid)
        raise HTTPException(status_code=500, detail=str(exc))

    station = _nws_feature_to_station(station_data)
    if not station:
        raise HTTPException(
            status_code=404,
            detail=f"NWS station '{sid}' returned unparseable metadata",
        )

    # Fetch latest observation
    readings: list[Reading] = []
    raw_obs: dict = {}
    try:
        async with _make_client() as client:
            obs_resp = await client.get(obs_url)
            obs_resp.raise_for_status()
            obs_data = obs_resp.json()
        obs_props = obs_data.get("properties", {})
        raw_obs = obs_props
        readings = _nws_obs_to_readings(obs_props, sid)
    except httpx.HTTPStatusError as exc:
        # 404 on latest obs is common for inactive stations; surface as warning
        if exc.response.status_code == 404:
            logger.warning("No latest observation available for NWS station %s", sid)
        else:
            logger.warning(
                "NWS observations error for %s: %s", sid, exc.response.status_code
            )
    except Exception:
        logger.debug("Failed to fetch NWS latest obs for %s", sid, exc_info=True)

    detail = StationDetail(station=station, readings=readings)
    result = {
        **detail.model_dump(),
        "raw_text_description": raw_obs.get("textDescription"),
        "icon": raw_obs.get("icon"),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/nws/stations/{station_id}/history",
    summary="NWS station observation history",
)
async def nws_station_history(
    station_id: str = Path(
        description="NWS station identifier, e.g. 'KPDX'"
    ),
    hours: int = Query(
        24,
        ge=1,
        le=168,
        description="Hours of history to return (1–168, default 24)",
    ),
):
    """Return recent time-series observations for a NWS station.

    Observations are fetched from the NWS /stations/{id}/observations endpoint
    using ISO 8601 start/end timestamps.  The NWS API returns up to a few
    hundred observations per request (typically one per hour).

    Readings are returned chronologically (oldest first).
    """
    sid = station_id.upper()
    cache_key = f"noaa_weather:nws:history:{sid}:{hours}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=hours)
    obs_url = f"{NWS_BASE}/stations/{sid}/observations"
    params_q = {
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end":   now.strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    try:
        async with _make_client() as client:
            resp = await client.get(obs_url, params=params_q)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"NWS station '{sid}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NWS API error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NWS API timeout")
    except Exception as exc:
        logger.exception("Error fetching NWS history for %s", sid)
        raise HTTPException(status_code=500, detail=str(exc))

    features = data.get("features", [])
    all_readings: list[Reading] = []
    for feat in features:
        props = feat.get("properties", {})
        try:
            all_readings.extend(_nws_obs_to_readings(props, sid))
        except Exception:
            logger.debug("Failed to parse NWS observation", exc_info=True)

    all_readings.sort(key=lambda r: r.timestamp)

    result = {
        "source": "noaa_weather",
        "network": "NWS",
        "station_id": sid,
        "hours": hours,
        "observation_count": len(features),
        "reading_count": len(all_readings),
        "readings": [r.model_dump() for r in all_readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# NWS bulk timestamp endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/nws/timestamps",
    summary="Bulk latest-observation timestamps for NWS stations in a state",
)
async def nws_bulk_timestamps(
    state: str = Query(
        ...,
        description="Two-letter US state code (e.g. 'OR', 'TX')",
        min_length=2,
        max_length=2,
    ),
):
    """Fetch the latest observation timestamp for every NWS station in a state.

    This calls ``/stations/{id}/observations/latest`` in parallel for each
    station returned by the NWS station-list endpoint.  Concurrency is
    limited with a semaphore to avoid overwhelming the NWS API.

    Stations that return 404 or time out are included with ``last_update=None``.
    The result is cached for 5 minutes (standard cache TTL).
    """
    state_upper = state.upper()
    cache_key = f"noaa_weather:nws:timestamps:{state_upper}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    # 1. Get the station list for this state (reuses existing endpoint + its cache)
    station_list = await nws_list_stations(state=state_upper)
    raw_stations: list[dict] = station_list.get("stations", [])
    if not raw_stations:
        return {
            "source": "noaa_weather",
            "network": "NWS",
            "state": state_upper,
            "count": 0,
            "fetched": 0,
            "stations": [],
        }

    # 2. Fetch latest observation timestamp for each station in parallel
    sem = asyncio.Semaphore(30)

    async def _fetch_ts(client: httpx.AsyncClient, stn: dict) -> dict:
        sid = stn.get("id", "")
        out = {
            "id": sid,
            "name": stn.get("name", sid),
            "lat": stn.get("lat", 0.0),
            "lon": stn.get("lon", 0.0),
            "source": "noaa_weather",
            "last_update": None,
            "params": stn.get("params", []),
        }
        if not sid:
            return out
        async with sem:
            try:
                resp = await client.get(
                    f"{NWS_BASE}/stations/{sid}/observations/latest",
                    timeout=8.0,
                )
                if resp.status_code == 200:
                    props = resp.json().get("properties", {})
                    ts_str = props.get("timestamp")
                    if ts_str:
                        out["last_update"] = ts_str
                # 404 / other status codes -> last_update stays None
            except Exception:
                pass  # timeout / network error -> skip
        return out

    async with _make_client() as client:
        results = await asyncio.gather(
            *[_fetch_ts(client, s) for s in raw_stations]
        )

    stamped = [r for r in results if r.get("last_update")]

    result = {
        "source": "noaa_weather",
        "network": "NWS",
        "state": state_upper,
        "count": len(results),
        "fetched": len(stamped),
        "stations": results,
    }
    # Long TTL so pre-warmed results survive the full catalog rebuild
    await cache.set(cache_key, result, ttl=3600)
    return result


# ---------------------------------------------------------------------------
# NDBC endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/ndbc/stations",
    summary="List active NDBC buoy and C-MAN stations",
)
async def ndbc_list_stations():
    """Return all active NDBC marine monitoring stations.

    The station list is parsed from the NDBC station_table.txt pipe-delimited
    metadata file, which is updated daily.  The result includes buoy type,
    geographic location, and the standard set of parameters NDBC stations
    report (wind, waves, sea temperature, pressure).

    The station list is cached for one hour.
    """
    cache_key = "noaa_weather:ndbc:stations"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NDBC_BASE}/data/stations/station_table.txt"
    try:
        async with _make_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NDBC station table request timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NDBC station table fetch error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching NDBC station table")
        raise HTTPException(status_code=500, detail=str(exc))

    stations = _ndbc_station_table_to_stations(text)
    if not stations:
        logger.warning(
            "NDBC station_table.txt returned 0 stations; "
            "first 200 chars: %s", text[:200]
        )

    result = {
        "source": "noaa_weather",
        "network": "NDBC",
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/ndbc/stations/{station_id}",
    summary="NDBC buoy latest observation",
)
async def ndbc_get_station(
    station_id: str = Path(
        description="NDBC station ID, e.g. '46029' (Columbia River Bar) or 'BVRO3' (C-MAN)"
    ),
):
    """Return the latest observation from an NDBC buoy or C-MAN station.

    Data is fetched from the NDBC realtime2 directory which contains the most
    recent ~45 days of observations at full resolution (typically 10-minute or
    hourly intervals).  Only the single most recent observation row for each
    parameter is returned.

    Station IDs are case-insensitive.  Common buoy IDs are 5-digit numeric
    strings (e.g. '46029'); C-MAN station IDs are alphanumeric (e.g. 'BVRO3').
    """
    sid = station_id.lower()
    cache_key = f"noaa_weather:ndbc:station:{sid}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{NDBC_BASE}/data/realtime2/{sid}.txt"
    try:
        async with _make_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            text = resp.text
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"NDBC station '{sid}' not found or has no realtime data",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"NDBC realtime fetch error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="NDBC realtime request timed out")
    except Exception as exc:
        logger.exception("Error fetching NDBC realtime data for %s", sid)
        raise HTTPException(status_code=500, detail=str(exc))

    all_readings = _ndbc_parse_realtime(text, sid)
    if not all_readings:
        raise HTTPException(
            status_code=422,
            detail=f"NDBC realtime data for '{sid}' could not be parsed",
        )

    # Keep only the latest reading per parameter
    latest: dict[str, Reading] = {}
    for r in all_readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    latest_readings = sorted(latest.values(), key=lambda r: r.timestamp, reverse=True)
    latest_ts = latest_readings[0].timestamp if latest_readings else None

    # Build a minimal Station from what we know
    station = Station(
        id=sid,
        name=sid.upper(),
        lat=0.0,
        lon=0.0,
        source="noaa_weather",
        network="NDBC",
        params=[r.parameter for r in latest_readings],
        url=f"https://www.ndbc.noaa.gov/station_page.php?station={sid}",
        last_update=latest_ts,
        extra={"data_source": "realtime2"},
    )

    detail = StationDetail(station=station, readings=latest_readings)
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


# ---------------------------------------------------------------------------
# IEM endpoints
# ---------------------------------------------------------------------------

@router.get(
    "/iem/networks",
    summary="List IEM network codes and types",
)
async def iem_networks(
    state: Optional[str] = Query(
        None,
        description=(
            "Filter by 2-letter state code (e.g. 'OR').  Returns common network "
            "IDs for that state.  Omit for the full IEM network catalogue."
        ),
    ),
):
    """Return IEM network identifiers.

    When ``state`` is provided the endpoint returns the standard network IDs
    for that state (ASOS, AWOS, COOP, RWIS).  Without ``state`` it fetches
    the full IEM network catalogue from the API.

    The ``id`` field from each result can be passed directly to the
    ``network`` parameter of the other IEM endpoints.
    """
    if state:
        state_upper = state.upper()
        networks_out = []
        for net_suffix, description in _IEM_NETWORK_TYPES.items():
            net_id = f"{state_upper}_{net_suffix}"
            networks_out.append(
                {
                    "id": net_id,
                    "name": f"{state_upper} {net_suffix}",
                    "description": description,
                    "state": state_upper,
                    "stations_url": (
                        f"/api/noaa_weather/iem/stations?network={net_id}"
                    ),
                    "current_url": (
                        f"/api/noaa_weather/iem/stations/{net_id}/current"
                        f"?network={net_id}"
                    ),
                }
            )
        return {
            "source": "noaa_weather",
            "state": state_upper,
            "networks": networks_out,
            "count": len(networks_out),
            "note": "Pass the 'id' value as the 'network' parameter to other IEM endpoints.",
        }

    # Full catalogue from IEM
    cache_key = "noaa_weather:iem:networks:all"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        async with _make_client() as client:
            resp = await client.get(f"{IEM_BASE}/api/1/networks.json")
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching IEM network list")
        raise HTTPException(status_code=500, detail=str(exc))

    result = {
        "source": "noaa_weather",
        "data": data,
        "note": (
            "Pass a network 'id' to GET /api/noaa_weather/iem/stations?network=<id>"
        ),
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/iem/stations",
    summary="Stations in an IEM network (GeoJSON)",
)
async def iem_list_stations(
    network: str = Query(
        ...,
        description=(
            "IEM network code, e.g. 'OR_ASOS', 'IA_COOP', 'OK_MESONET'.  "
            "Use GET /api/noaa_weather/iem/networks to discover valid codes."
        ),
    ),
):
    """Return all stations in an IEM network using the GeoJSON network endpoint.

    This endpoint queries IEM's /geojson/network/{network}.geojson which
    returns a full GeoJSON FeatureCollection with station metadata including
    name, coordinates, elevation, county, state, and archive period.

    Results are cached for one hour since station lists change infrequently.
    """
    cache_key = f"noaa_weather:iem:stations:{network}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    url = f"{IEM_BASE}/geojson/network/{network}.geojson"
    try:
        async with _make_client() as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(
                status_code=404,
                detail=f"IEM network '{network}' not found",
            )
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except Exception as exc:
        logger.exception("Error fetching IEM stations for network %s", network)
        raise HTTPException(status_code=500, detail=str(exc))

    features = data.get("features", [])
    stations: list[Station] = []
    for feat in features:
        try:
            station = _iem_station_from_geojson(feat, network)
            if station:
                stations.append(station)
        except Exception:
            logger.debug("Failed to parse IEM GeoJSON feature", exc_info=True)

    result = {
        "source": "noaa_weather",
        "network": network,
        "count": len(stations),
        "stations": [s.model_dump() for s in stations],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL_LONG)
    return result


@router.get(
    "/iem/stations/{station_id}/current",
    summary="IEM station current conditions",
)
async def iem_station_current(
    station_id: str = Path(
        description="IEM station identifier, e.g. 'KPDX', 'KOKC', 'KLAX'"
    ),
    network: str = Query(
        ...,
        description="IEM network the station belongs to, e.g. 'OR_ASOS'",
    ),
):
    """Return the most recent observation for a single IEM station.

    Uses the IEM currents JSON endpoint filtered to a specific station.
    The ``network`` parameter is required because IEM station IDs are not
    globally unique — the same 4-letter ICAO ID may appear in multiple networks.

    Returns station metadata and the latest reading for each available parameter.
    """
    sid = station_id.upper()
    cache_key = f"noaa_weather:iem:current:{network}:{sid}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    try:
        async with _make_client() as client:
            resp = await client.get(
                f"{IEM_BASE}/api/1/currents.json",
                params={"network": network, "station": sid},
            )
            resp.raise_for_status()
            data = resp.json()
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM API timeout")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM API error: {exc}",
        )
    except Exception as exc:
        logger.exception(
            "Error fetching IEM current conditions for %s / %s", sid, network
        )
        raise HTTPException(status_code=500, detail=str(exc))

    raw_obs_list = data.get("data", [])
    if not raw_obs_list:
        raise HTTPException(
            status_code=404,
            detail=(
                f"No current observations found for station '{sid}' "
                f"in network '{network}'"
            ),
        )

    obs = raw_obs_list[0]
    readings = _iem_readings_from_obs(obs, sid)

    # Build station from obs metadata
    valid_str = obs.get("utc_valid") or obs.get("valid")
    last_update: Optional[datetime] = None
    if valid_str:
        try:
            last_update = datetime.fromisoformat(valid_str.replace("Z", "+00:00"))
        except ValueError:
            pass

    elev_raw = obs.get("elevation") or obs.get("elev")
    try:
        elevation = float(elev_raw) if elev_raw is not None else None
    except (TypeError, ValueError):
        elevation = None

    station = Station(
        id=sid,
        name=obs.get("name") or obs.get("sname") or sid,
        lat=float(obs.get("lat") or obs.get("latitude") or 0.0),
        lon=float(obs.get("lon") or obs.get("longitude") or 0.0),
        source="noaa_weather",
        network=network,
        params=[r.parameter for r in readings],
        elevation=elevation,
        url=f"https://mesonet.agron.iastate.edu/sites/site.php?station={sid}&network={network}",
        last_update=last_update,
        extra={
            "network": network,
            "county": obs.get("county", ""),
            "state": obs.get("state", ""),
        },
    )

    # Keep only the latest reading per parameter (should already be singular here)
    latest: dict[str, Reading] = {}
    for r in readings:
        if r.parameter not in latest or r.timestamp > latest[r.parameter].timestamp:
            latest[r.parameter] = r

    detail = StationDetail(station=station, readings=list(latest.values()))
    result = detail.model_dump()
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


@router.get(
    "/iem/stations/{station_id}/history",
    summary="IEM station observation history",
)
async def iem_station_history(
    station_id: str = Path(
        description="IEM station identifier, e.g. 'KPDX'"
    ),
    network: str = Query(
        ...,
        description="IEM network the station belongs to, e.g. 'OR_ASOS'",
    ),
    hours: int = Query(
        24,
        ge=1,
        le=720,
        description="Hours of history to return (1–720, default 24)",
    ),
):
    """Return observation history for a single IEM station.

    History is fetched from the IEM ASOS CSV archive (``/cgi-bin/request/asos.py``)
    which provides the highest-resolution data available — typically one
    observation per 5 minutes for ASOS stations.

    For non-ASOS networks the endpoint falls back to the IEM JSON observation-
    history endpoint (``/api/1/obhistory.json``) which returns daily data.

    Readings are returned chronologically (oldest first).
    """
    sid = station_id.upper()
    cache_key = f"noaa_weather:iem:history:{network}:{sid}:{hours}"
    cached = await cache.get(cache_key)
    if cached is not None:
        return cached

    now = datetime.now(tz=timezone.utc)
    start = now - timedelta(hours=hours)

    # Prefer the high-resolution ASOS CSV path for ASOS/AWOS networks
    use_asos_csv = any(
        tok in network.upper() for tok in ("ASOS", "AWOS")
    )

    all_readings: list[Reading] = []

    if use_asos_csv:
        all_readings = await _fetch_iem_asos_csv(sid, network, start, now)
    else:
        all_readings = await _fetch_iem_obhistory(sid, network, start, now)

    all_readings.sort(key=lambda r: r.timestamp)

    result = {
        "source": "noaa_weather",
        "network": network,
        "station_id": sid,
        "hours": hours,
        "reading_count": len(all_readings),
        "readings": [r.model_dump() for r in all_readings],
    }
    await cache.set(cache_key, result, ttl=config.CACHE_TTL)
    return result


async def _fetch_iem_asos_csv(
    station_id: str,
    network: str,
    start: datetime,
    end: datetime,
) -> list[Reading]:
    """Fetch and parse IEM ASOS CSV archive data for a time window.

    Queries the ``/cgi-bin/request/asos.py`` endpoint which accepts datetime
    components and returns a comma-delimited file.
    """
    params_q = {
        "station": station_id,
        "data": "all",
        "year1":  str(start.year),
        "month1": str(start.month),
        "day1":   str(start.day),
        "hour1":  str(start.hour),
        "minute1":"0",
        "year2":  str(end.year),
        "month2": str(end.month),
        "day2":   str(end.day),
        "hour2":  str(end.hour),
        "minute2":"59",
        "tz":     "UTC",
        "format": "comma",
        "latlon": "no",
        "direct": "no",
        "report_type": "3",   # hourly + special obs
    }

    try:
        async with _make_client() as client:
            resp = await client.get(
                f"{IEM_BASE}/cgi-bin/request/asos.py",
                params=params_q,
            )
            resp.raise_for_status()
            csv_text = resp.text
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="IEM ASOS archive request timed out")
    except httpx.HTTPStatusError as exc:
        raise HTTPException(
            status_code=exc.response.status_code,
            detail=f"IEM ASOS archive error: {exc}",
        )
    except Exception as exc:
        logger.exception("Error fetching IEM ASOS CSV for %s", station_id)
        raise HTTPException(status_code=500, detail=str(exc))

    return _iem_readings_from_asos_csv(csv_text, station_id)


async def _fetch_iem_obhistory(
    station_id: str,
    network: str,
    start: datetime,
    end: datetime,
) -> list[Reading]:
    """Fetch and parse IEM JSON observation history for a non-ASOS station.

    Uses the ``/api/1/obhistory.json`` endpoint which returns a day's worth
    of observations.  For multi-day requests we iterate over dates.
    """
    all_readings: list[Reading] = []
    current_date = start.date()
    end_date = end.date()

    async with _make_client() as client:
        while current_date <= end_date:
            date_str = current_date.isoformat()
            try:
                resp = await client.get(
                    f"{IEM_BASE}/api/1/obhistory.json",
                    params={
                        "station": station_id,
                        "network": network,
                        "date": date_str,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                rows = data.get("data", [])
                for row in rows:
                    all_readings.extend(_iem_readings_from_obs(row, station_id))
            except httpx.HTTPStatusError as exc:
                # Non-fatal: some dates may have no data
                logger.debug(
                    "IEM obhistory returned %s for %s on %s",
                    exc.response.status_code, station_id, date_str,
                )
            except Exception:
                logger.debug(
                    "IEM obhistory error for %s on %s",
                    station_id, date_str, exc_info=True,
                )

            current_date = current_date + timedelta(days=1)

    return all_readings


# ---------------------------------------------------------------------------
# Overview endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/overview",
    summary="Summary of all NOAA Weather & Buoy data sources",
)
async def overview():
    """Return a summary of the three data sources integrated in this module.

    Includes endpoint listings, coverage notes, and parameter lists for NWS,
    NDBC, and IEM.
    """
    sources = [
        {
            "id": "nws",
            "name": "National Weather Service Observation API",
            "description": (
                "~3,000 ASOS/AWOS airport stations nationwide.  "
                "JSON-LD/GeoJSON format, SI units (°C, m/s, Pa).  "
                "No authentication required; User-Agent header mandatory."
            ),
            "base_url": NWS_BASE,
            "auth_required": False,
            "update_frequency": "5 min (ASOS)",
            "coverage": "United States",
            "params": list(_NWS_OBS_PROPS.keys()),
            "endpoints": [
                "GET /api/noaa_weather/nws/stations?state=OR",
                "GET /api/noaa_weather/nws/stations/{station_id}",
                "GET /api/noaa_weather/nws/stations/{station_id}/history?hours=24",
            ],
        },
        {
            "id": "ndbc",
            "name": "National Data Buoy Center",
            "description": (
                "~900 active ocean buoys and coastal C-MAN stations.  "
                "Pipe-delimited text format parsed from NDBC realtime2 directory.  "
                "Key marine parameters: wind, waves, sea temperature."
            ),
            "base_url": NDBC_BASE,
            "auth_required": False,
            "update_frequency": "10 min (moored buoys), hourly (C-MAN)",
            "coverage": "US coastal waters and open ocean",
            "params": list(_NDBC_COL_MAP.keys()),
            "endpoints": [
                "GET /api/noaa_weather/ndbc/stations",
                "GET /api/noaa_weather/ndbc/stations/{station_id}",
            ],
        },
        {
            "id": "iem",
            "name": "Iowa Environmental Mesonet (IEM) — Deep Integration",
            "description": (
                "The most comprehensive free weather data aggregator in the US.  "
                "100+ networks, thousands of stations across all states.  "
                "GeoJSON station lists, real-time currents, ASOS CSV archive "
                "(5-min resolution), and daily summaries."
            ),
            "base_url": IEM_BASE,
            "auth_required": False,
            "update_frequency": "5 min (ASOS), hourly (COOP), varies",
            "coverage": "United States (all states)",
            "network_types": _IEM_NETWORK_TYPES,
            "params": [v[0] for v in _IEM_VAR_MAP.values()],
            "endpoints": [
                "GET /api/noaa_weather/iem/networks?state=OR",
                "GET /api/noaa_weather/iem/stations?network=OR_ASOS",
                "GET /api/noaa_weather/iem/stations/{station_id}/current?network=OR_ASOS",
                "GET /api/noaa_weather/iem/stations/{station_id}/history?network=OR_ASOS&hours=24",
            ],
        },
    ]

    return {
        "module": "noaa_weather",
        "source_name": SOURCE_NAME,
        "description": SOURCE_DESCRIPTION,
        "source_count": len(sources),
        "sources": sources,
        "nws_unit_conversions": {
            k: {"display_unit": v[0], "multiplier": v[1]}
            for k, v in _NWS_UNIT_MAP.items()
        },
        "ndbc_column_map": {
            k: {"parameter": v[0], "unit": v[1]}
            for k, v in _NDBC_COL_MAP.items()
        },
        "iem_variable_map": {
            k: {"parameter": v[0], "unit": v[1]}
            for k, v in _IEM_VAR_MAP.items()
        },
    }
