"""Unified station catalog across all configured station endpoints.

This module is the first normalization layer for a Synoptic-style API:
- Collects station lists from many source endpoints
- Normalizes core station fields
- De-duplicates likely same physical stations
- Keeps duplicate members as backup providers
"""
from __future__ import annotations

import asyncio
import json
import math
import re
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import APIRouter, HTTPException, Query

import cache
import config
from models import SOURCE_QUALITY_TIERS

router = APIRouter(prefix="/api/unified", tags=["unified"])

SOURCE_NAME = "Unified Station Catalog"
SOURCE_DESCRIPTION = "Normalized + deduplicated station index with backup providers."
SOURCE_META = {
    "category": "meta",
    "coverage": "All configured station endpoints",
    "auth_required": False,
    "update_frequency": "on-demand, cached",
    "params": ["station metadata"],
}

_ENDPOINT_LIST_PATH = Path(__file__).resolve().parent.parent / "count_stations.py"
_DEFAULT_CONCURRENCY = 20
_ID_SANITIZE_RE = re.compile(r"[^a-z0-9]+")
_SOURCE_PRIORITY = {
    # Lower is better.
    "usgs": 10,
    "noaa_water": 15,
    "noaa_weather": 20,
    "faa_aviation": 22,
    "noaa_arcgis": 23,
    "synoptic": 25,
    "snotel": 30,
    "raws": 35,
    "raws_dcp": 36,
    "oklahoma_mesonet": 40,
}

# Endpoints to omit from unified build due duplication or known env requirements.
_UNIFIED_EXCLUDE_ENDPOINTS = {
    # Duplicate Florida FAWN path; keep regional/fawn source of truth.
    "/api/eastcoast/fawn/stations",
}

# Parameterized endpoints that require sweeping a dimension.
_ALL_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "PR", "VI", "GU", "AS", "DC",
]
_USACE_OFFICES = [
    "LRB", "LRC", "LRD", "LRE", "LRH", "LRL", "LRN", "LRP", "MVD", "MVK",
    "MVM", "MVN", "MVP", "MVR", "MVS", "NAB", "NAD", "NAE", "NAN", "NAO",
    "NAP", "NWD", "NWK", "NWO", "NWP", "NWS", "NWW", "POD", "POA", "SAD",
    "SAJ", "SAM", "SAS", "SAW", "SPA", "SPD", "SPK", "SPL", "SPN", "SWD",
    "SWF", "SWG", "SWL", "SWT", "TAD",
]

_MAP_STATES = [
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
]
_IEM_MAP_NETWORK_SUFFIXES = ["ASOS", "RWIS"]
_IEM_DISCOVERED_SUFFIXES_DEFAULT = ["ASOS", "RWIS", "DCP", "COOP", "COCORAHS"]

# Canonical normalized unit target per canonical parameter.
_CANONICAL_UNITS: dict[str, str] = {
    "temperature": "C",
    "dewpoint": "C",
    "apparent_temperature": "C",
    "wet_bulb_temperature": "C",
    "soil_temperature": "C",
    "road_temperature": "C",
    "sea_surface_temperature": "C",
    "water_temperature": "C",
    "wind_speed": "m/s",
    "wind_gust": "m/s",
    "wind_direction": "deg",
    "relative_humidity": "%",
    "pressure": "hPa",
    "pressure_msl": "hPa",
    "altimeter": "hPa",
    "visibility": "km",
    "temperature_max": "C",
    "temperature_min": "C",
    "temperature_max_24h": "C",
    "temperature_min_24h": "C",
    "temperature_avg": "C",
    "precipitation": "mm",
    "precipitation_1h": "mm",
    "precipitation_3h": "mm",
    "precipitation_6h": "mm",
    "precipitation_day": "mm",
    "snow_depth": "mm",
    "snow_water_equivalent": "mm",
    "wave_height": "m",
    "dominant_wave_period": "s",
    "average_wave_period": "s",
    "streamflow": "cfs",
    "stream_stage": "ft",
    "water_level": "ft",
    "salinity": "ppt",
    "soil_moisture": "%",
    "battery_voltage": "V",
    "road_condition": "code",
    "subsurface_temperature": "C",
    "solar_radiation": "W/m2",
}

# Alias map from common raw names to canonical parameter names.
_PARAM_ALIAS_TO_CANONICAL: dict[str, str] = {
    "air_temperature": "temperature",
    "temp": "temperature",
    "temperature": "temperature",
    "temperature_air": "temperature",
    "apparent_temperature": "apparent_temperature",
    "heat_index": "apparent_temperature",
    "wind_chill": "apparent_temperature",
    "dewpoint": "dewpoint",
    "dew_point": "dewpoint",
    "dew_point_temperature": "dewpoint",
    "relative_humidity": "relative_humidity",
    "humidity": "relative_humidity",
    "rh": "relative_humidity",
    "wind_speed": "wind_speed",
    "wind": "wind_speed",
    "wind_gust": "wind_gust",
    "gust": "wind_gust",
    "wind_direction": "wind_direction",
    "wind_dir": "wind_direction",
    "pressure": "pressure",
    "barometric_pressure": "pressure",
    "air_pressure": "pressure",
    "pressure_msl": "pressure_msl",
    "sea_level_pressure": "pressure_msl",
    "altimeter": "altimeter",
    "visibility": "visibility",
    "precip": "precipitation",
    "precipitation": "precipitation",
    "rainfall": "precipitation",
    "precipitation_1h": "precipitation_1h",
    "precipitation_3h": "precipitation_3h",
    "precipitation_6h": "precipitation_6h",
    "precipitation_day": "precipitation_day",
    "snow_depth": "snow_depth",
    "swe": "snow_water_equivalent",
    "wave_height": "wave_height",
    "streamflow": "streamflow",
    "discharge": "streamflow",
    "stream_stage": "stream_stage",
    "gage_height": "stream_stage",
    "water_level": "water_level",
    "road_temperature": "road_temperature",
    "soil_temperature": "soil_temperature",
    "solar_radiation": "solar_radiation",
    "fuel_moisture": "fuel_moisture",
    "air_quality_index": "air_quality_index",
    "pm25": "pm25",
    "pm10": "pm10",
    "ozone": "ozone",
    "barometric_pressure": "pressure",
    "barometricpressure": "pressure",
    "sea_level_pressure": "pressure_msl",
    "sealevelpressure": "pressure_msl",
    "relativehumidity": "relative_humidity",
    "windspeed": "wind_speed",
    "winddirection": "wind_direction",
    "windgust": "wind_gust",
    "wind_chill": "apparent_temperature",
    "windchill": "apparent_temperature",
    "heatindex": "apparent_temperature",
    "max_temperature_last24_hours": "temperature_max_24h",
    "maxtemperaturelast24hours": "temperature_max_24h",
    "min_temperature_last24_hours": "temperature_min_24h",
    "mintemperaturelast24hours": "temperature_min_24h",
    "precipitation_last_hour": "precipitation_1h",
    "precipitationlasthour": "precipitation_1h",
    "precipitation_last3_hours": "precipitation_3h",
    "precipitationlast3hours": "precipitation_3h",
    "precipitation_last6_hours": "precipitation_6h",
    "precipitationlast6hours": "precipitation_6h",
    "river_stage": "stream_stage",
    "river_discharge": "streamflow",
    "gage_height": "stream_stage",
    "temperature_max": "temperature_max",
    "temperature_min": "temperature_min",
    "tavg": "temperature_avg",
    "tmax": "temperature_max",
    "tmin": "temperature_min",
    "wteq": "snow_water_equivalent",
    "snwd": "snow_depth",
    "rhum": "relative_humidity",
    "srad": "solar_radiation",
    "wdirv": "wind_direction",
    "wspdv": "wind_speed",
    "wspdx": "wind_gust",
    "sms": "soil_moisture",
    "sto": "soil_temperature",
    "sal": "salinity",
    "batt": "battery_voltage",
    "road_condition": "road_condition",
    "pavement_temperature": "road_temperature",
    "subsurface_temperature": "subsurface_temperature",
    "dominant_wave_period": "dominant_wave_period",
    "average_wave_period": "average_wave_period",
}


def _client() -> httpx.AsyncClient:
    # Use longer timeout for catalog builds — some endpoints (NWS timestamp
    # sweeps) make hundreds of sub-calls and need more than 30s.
    return httpx.AsyncClient(
        timeout=max(config.HTTP_TIMEOUT, 120),
        headers={"User-Agent": config.USER_AGENT, "Accept": "application/json"},
    )


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two (lat, lon) pairs."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


# Map source names to their internal readings endpoint patterns.
# {source} -> (url_template, response_parser_hint)
# url_template uses {station_id}, {hours}, {param} placeholders.
_SOURCE_READINGS_ENDPOINTS: dict[str, str] = {
    "usgs": "/api/usgs/stations/{station_id}/readings?hours={hours}",
    "noaa_water": "/api/noaa_water/coops/stations/{station_id}/readings?hours={hours}",
    "noaa_weather": "/api/noaa_weather/nws/stations/{station_id}",
    "synoptic": "/api/synoptic/stations/{station_id}/timeseries?hours={hours}",
    "snotel": "/api/noaa_weather/snotel/stations/{station_id}",
    "iem": "/api/mesonets/iem/current?network={network}",
}


def _parse_endpoints_from_counter() -> list[str]:
    if not _ENDPOINT_LIST_PATH.exists():
        return []
    text = _ENDPOINT_LIST_PATH.read_text(encoding="utf-8", errors="replace")
    m = re.search(r"ENDPOINTS\s*=\s*\[(.*?)\]\s*\n", text, flags=re.S)
    if not m:
        return []
    return re.findall(r'"(/api/[^"]+)"', m.group(1))


def _endpoint_inventory(include_param_sweeps: bool = True) -> list[str]:
    endpoints = _parse_endpoints_from_counter()
    dynamic_endpoints: list[str] = list(endpoints)
    if include_param_sweeps:
        dynamic_endpoints += [f"/api/noaa_weather/nws/stations?state={s}" for s in _ALL_STATES]
        dynamic_endpoints += [f"/api/noaa_weather/nws/timestamps?state={s}" for s in _ALL_STATES]
        dynamic_endpoints += [f"/api/usgs/stations?state={s}" for s in _ALL_STATES]
        dynamic_endpoints += [f"/api/climate/acis/stations?state={s}" for s in _ALL_STATES]
        dynamic_endpoints += [f"/api/federal_water/usace/locations?office={o}" for o in _USACE_OFFICES]
        dynamic_endpoints.append("/api/noaa_water/nwps/gauges")
        dynamic_endpoints.append("/api/noaa_weather/ndbc/stations")

        # ── Observation / current-data endpoints (provide last_update timestamps) ──
        # IEM current obs — returns stations with timestamps
        for suffix in ("ASOS", "AWOS", "DCP", "COOP", "RWIS"):
            dynamic_endpoints += [f"/api/mesonets/iem/current?network={s}_{suffix}" for s in _ALL_STATES]
        # FAA METAR — bulk current observations with timestamps (~2,500+ stations)
        # Uses /metar/all/stations (flat station list) instead of /metar?state=
        # (StationDetail format) so _normalize_station can parse directly.
        dynamic_endpoints.append("/api/faa_weather/metar/all/stations")
        # NOAA ArcGIS surface obs — ~4,380 METAR stations with timestamps direct from NWS
        dynamic_endpoints.append("/api/noaa_arcgis/surface_obs")
        # Infrastructure / mesonets with current data
        dynamic_endpoints.append("/api/infrastructure/texmesonet/current")
        dynamic_endpoints.append("/api/coagmet/latest")
        dynamic_endpoints.append("/api/mt_mesonet/stations")
        dynamic_endpoints.append("/api/sdge_weather/current")
        # International current obs
        dynamic_endpoints.append("/api/hong_kong/current")
        dynamic_endpoints.append("/api/austria/current")
        dynamic_endpoints.append("/api/poland/synop")
        dynamic_endpoints.append("/api/poland/hydro")
        dynamic_endpoints.append("/api/iceland/current")
        dynamic_endpoints.append("/api/baltic/lithuania/current")
        dynamic_endpoints.append("/api/baltic/estonia/current")
        dynamic_endpoints.append("/api/argentina/current")
        dynamic_endpoints.append("/api/asia_east/taiwan/current")
        dynamic_endpoints.append("/api/asia_east/korea/current")
        dynamic_endpoints.append("/api/europe_west/france/current")
        dynamic_endpoints.append("/api/europe_west/spain/current")
        dynamic_endpoints.append("/api/europe_west/switzerland/current")
        # Fire weather
        dynamic_endpoints.append("/api/fire_weather/raws/current")
        # DOT RWIS (ArcGIS — already have timestamps)
        dynamic_endpoints.append("/api/dot_rwis/iowa/atmospheric")
        dynamic_endpoints.append("/api/dot_rwis/maryland")
        dynamic_endpoints.append("/api/dot_rwis/washington")
        # ArcGIS real-time RWIS (Illinois, Georgia — have ObservationTime timestamps)
        dynamic_endpoints.append("/api/rt_arcgis/idot_rwis")
        dynamic_endpoints.append("/api/rt_arcgis/gdot_rwis")
        # CODOT CoTrip RWIS (has ws_lastupdated timestamps)
        dynamic_endpoints.append("/api/infrastructure/codot/rwis")
        # USDA SCAN current observations (IEM — has timestamps)
        dynamic_endpoints.append("/api/regional/scan/current")
        # LA County DPW rainfall (live scrape — has timestamps)
        dynamic_endpoints.append("/api/california/lacounty/stations")
        # OC PWD hydrology (ArcGIS — has timestamps)
        dynamic_endpoints.append("/api/california/orange/stations")
        # Valley Water ALERT (station list has no timestamps; per-station
        # readings would need individual fetches — already ingested via counter)
        # CDEC current observations (parallel CSV fetch — has timestamps)
        dynamic_endpoints.append("/api/california/cdec/current")

    out: list[str] = []
    seen: set[str] = set()
    for ep in dynamic_endpoints:
        if ep in _UNIFIED_EXCLUDE_ENDPOINTS:
            continue
        if ep.startswith("/api/regional/cimis/stations") and not config.CIMIS_APP_KEY:
            continue
        if ep not in seen:
            seen.add(ep)
            out.append(ep)
    return out


def _extract_station_array(payload: Any) -> list[dict]:
    raw: list[dict] = []
    if isinstance(payload, list):
        raw = [x for x in payload if isinstance(x, dict)]
    elif isinstance(payload, dict):
        for key in ("stations", "gauges", "locations", "reservoirs", "features", "data", "results", "items"):
            val = payload.get(key)
            if isinstance(val, list):
                raw = [x for x in val if isinstance(x, dict)]
                break

    # Unwrap StationDetail-style records: {station: {...}, readings: [...]}
    # Pull the station sub-dict out so _normalize_station can find id/lat/lon.
    unwrapped: list[dict] = []
    for item in raw:
        inner = item.get("station")
        if isinstance(inner, dict) and ("id" in inner or "station_id" in inner):
            unwrapped.append(inner)
        else:
            unwrapped.append(item)
    return unwrapped


def _float_or_none(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _norm_text(v: str) -> str:
    return _ID_SANITIZE_RE.sub("", v.lower()).strip()


def _station_id_core(v: Any) -> str:
    s = str(v or "").strip().upper()
    s = re.sub(r"[^A-Z0-9]", "", s)
    # Normalize common ICAO-style prefixes for dedupe purposes.
    if len(s) == 4 and s.startswith("K"):
        return s[1:]
    return s


def _slug_param(v: str) -> str:
    s = str(v or "").strip()
    # Preserve camelCase boundaries before lowercasing.
    s = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", s)
    s = s.lower()
    s = s.replace("%", "pct")
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _normalize_param_name(raw: str) -> tuple[str, str]:
    slug = _slug_param(raw)
    if not slug:
        return "", "empty"
    canonical = _PARAM_ALIAS_TO_CANONICAL.get(slug)
    if canonical:
        return canonical, "alias"
    # Fallback normalization so every variable is still normalized to stable key.
    return slug, "fallback"


def _normalize_param_list(params: list[str]) -> dict[str, Any]:
    out: list[str] = []
    details: list[dict[str, str]] = []
    seen: set[str] = set()
    mapped_alias = 0
    mapped_fallback = 0
    for p in params:
        canonical, method = _normalize_param_name(str(p))
        if not canonical:
            continue
        if canonical not in seen:
            seen.add(canonical)
            out.append(canonical)
        details.append({"raw": str(p), "normalized": canonical, "method": method})
        if method == "alias":
            mapped_alias += 1
        elif method == "fallback":
            mapped_fallback += 1
    return {
        "params_normalized": sorted(out),
        "params_normalization_details": details,
        "params_alias_mapped_count": mapped_alias,
        "params_fallback_mapped_count": mapped_fallback,
    }


def _infer_default_params(source: str, endpoint: str) -> list[str]:
    s = (source or "").lower()
    ep = (endpoint or "").lower()
    if any(k in ep for k in ["/usgs/", "hydrometric", "coops", "nwps", "water", "reclamation", "usace"]):
        return ["water_level", "streamflow", "stream_stage", "water_temperature"]
    if any(k in ep for k in ["air_quality", "purpleair", "ambient", "tempest"]):
        return ["pm25", "pm10", "air_quality_index"]
    if any(k in ep for k in ["ndbc", "buoy", "marine"]):
        return ["wind_speed", "wind_direction", "wave_height", "sea_surface_temperature", "pressure"]
    if any(k in ep for k in ["snotel", "snow", "scan"]):
        return ["temperature", "precipitation", "snow_depth", "snow_water_equivalent", "soil_moisture"]
    if any(k in ep for k in ["rwis", "mesonet", "nws", "asos", "awos", "weather", "raws"]):
        return ["temperature", "dewpoint", "relative_humidity", "wind_speed", "wind_direction", "pressure", "precipitation"]
    if s in {"usgs", "noaa_water", "federal_water"}:
        return ["water_level", "streamflow", "stream_stage"]
    if s in {"noaa_weather", "mesonets", "regional", "synoptic"}:
        return ["temperature", "dewpoint", "relative_humidity", "wind_speed", "wind_direction", "pressure"]
    # Explicitly normalize metadata-only feeds to a stable key.
    return ["station_metadata"]


def _pick_primary(members: list[dict]) -> dict:
    def freshness_rank(last_update: Any) -> int:
        if not last_update:
            return 1
        try:
            # Accept Z suffix and offset-aware ISO forms.
            iso = str(last_update).replace("Z", "+00:00")
            _ = datetime.fromisoformat(iso)
            return 0
        except Exception:  # noqa: BLE001
            return 1

    def rank_key(m: dict) -> tuple:
        tier = m.get("quality_tier") or SOURCE_QUALITY_TIERS.get(m.get("source", ""), 9)
        source_priority = _SOURCE_PRIORITY.get(m.get("source", ""), 999)
        fresh = freshness_rank(m.get("last_update"))
        # More observed normalized params is better for primary selection.
        param_score = -len(m.get("params_normalized", []) or m.get("params", []))
        return (tier, source_priority, fresh, param_score, m.get("source", "zzz"), m.get("id", "zzz"))

    return sorted(members, key=rank_key)[0]


def _normalize_station(st: dict, endpoint: str) -> Optional[dict]:
    sid = (
        st.get("id")
        or st.get("station_id")
        or st.get("site_no")
        or st.get("loc_id")
        or st.get("stationTriplet")
        or st.get("station")
    )
    if sid is None:
        return None
    sid = str(sid).strip()
    if not sid:
        return None

    lat = _float_or_none(st.get("lat"))
    if lat is None:
        lat = _float_or_none(st.get("latitude"))
    lon = _float_or_none(st.get("lon"))
    if lon is None:
        lon = _float_or_none(st.get("longitude"))

    # Keep station even without coordinates; dedupe key will include id/source fallback.
    name = (
        st.get("name")
        or st.get("station_name")
        or st.get("title")
        or st.get("display_name")
        or sid
    )
    source = st.get("source") or endpoint.split("/")[2]
    network = st.get("network") or ""
    params = st.get("params")
    if not isinstance(params, list):
        params = []
    if not params:
        params = _infer_default_params(str(source), endpoint)
    params_raw = [str(p) for p in params]
    params_norm_meta = _normalize_param_list(params_raw)
    last_update = st.get("last_update") or st.get("updated") or st.get("timestamp")
    tier = st.get("quality_tier")
    if tier is None:
        tier = SOURCE_QUALITY_TIERS.get(source)

    return {
        "id": sid,
        "name": str(name),
        "lat": lat,
        "lon": lon,
        "source": str(source),
        "network": str(network),
        "params": params_raw,
        "params_normalized": params_norm_meta["params_normalized"],
        "params_normalization_details": params_norm_meta["params_normalization_details"],
        "params_alias_mapped_count": params_norm_meta["params_alias_mapped_count"],
        "params_fallback_mapped_count": params_norm_meta["params_fallback_mapped_count"],
        "elevation": _float_or_none(st.get("elevation")),
        "url": st.get("url"),
        "last_update": last_update,
        "quality_tier": tier,
        "endpoint": endpoint,
    }


def _dedupe_key(st: dict) -> str:
    idk = _station_id_core(st.get("id"))
    # Strongest key: geospatial+id core.
    if st.get("lat") is not None and st.get("lon") is not None and idk:
        latk = round(float(st["lat"]), 2)
        lonk = round(float(st["lon"]), 2)
        return f"geoid:{latk}:{lonk}:id:{idk}"
    # Strong key: geospatial+name.
    if st.get("lat") is not None and st.get("lon") is not None:
        latk = round(float(st["lat"]), 3)
        lonk = round(float(st["lon"]), 3)
        namek = _norm_text(st.get("name", ""))
        if namek:
            return f"geo:{latk}:{lonk}:name:{namek}"
    # Mid-strength key: id+name.
    if idk:
        namek = _norm_text(st.get("name", ""))
        if namek:
            return f"idname:{idk}:{namek}"
    # Fallback: source+id only (no cross-source merge).
    return f"id:{st.get('source','')}:{_norm_text(st.get('id',''))}"


async def _fetch_endpoint(client: httpx.AsyncClient, endpoint: str) -> tuple[str, Optional[Any], Optional[str]]:
    base_url = f"http://127.0.0.1:{config.PORT}{endpoint}"
    sep = "&" if "?" in endpoint else "?"
    candidates = [base_url]
    if "limit=" not in endpoint:
        candidates.insert(0, f"{base_url}{sep}limit=5000")

    last_err = ""
    for url in candidates:
        for attempt in range(3):
            try:
                r = await client.get(url)
                r.raise_for_status()
                return endpoint, r.json(), None
            except httpx.HTTPStatusError as exc:
                last_err = str(exc)
                # Retry without limit if we got client-side validation failures.
                if exc.response.status_code in (400, 401, 403, 404, 422):
                    break
                if exc.response.status_code >= 500 and attempt < 2:
                    await asyncio.sleep(0.4 * (attempt + 1))
                    continue
                break
            except Exception as exc:  # noqa: BLE001
                last_err = str(exc)
                if attempt < 2:
                    await asyncio.sleep(0.4 * (attempt + 1))
                    continue
                break
    return endpoint, None, last_err


async def _collect_raw_stations(include_param_sweeps: bool = True) -> tuple[list[dict], list[dict]]:
    dedup_eps = _endpoint_inventory(include_param_sweeps=include_param_sweeps)
    if not dedup_eps:
        raise HTTPException(status_code=500, detail="Could not parse ENDPOINTS from count_stations.py")

    # De-duplicate endpoint URLs.
    seen: set[str] = set()
    dedup_eps = [ep for ep in dedup_eps if not (ep in seen or seen.add(ep))]

    sem = asyncio.Semaphore(_DEFAULT_CONCURRENCY)
    errors: list[dict] = []
    stations: list[dict] = []

    async with _client() as client:
        async def run(ep: str) -> None:
            async with sem:
                endpoint, payload, err = await _fetch_endpoint(client, ep)
                if err:
                    ep_cache_key = f"unified:endpoint:v1:{endpoint}"
                    cached_ep = await cache.get(ep_cache_key)
                    if isinstance(cached_ep, dict) and isinstance(cached_ep.get("stations"), list):
                        stations.extend([x for x in cached_ep["stations"] if isinstance(x, dict)])
                        errors.append({"endpoint": endpoint, "error": err, "used_cached": True})
                        return
                    errors.append({"endpoint": endpoint, "error": err, "used_cached": False})
                    return
                endpoint_stations: list[dict] = []
                for raw in _extract_station_array(payload):
                    norm = _normalize_station(raw, endpoint)
                    if norm is not None:
                        stations.append(norm)
                        endpoint_stations.append(norm)
                # Save per-endpoint normalized payload for resilience on transient failures.
                await cache.set(
                    f"unified:endpoint:v1:{endpoint}",
                    {"saved_at": datetime.now(UTC).isoformat(), "stations": endpoint_stations},
                    ttl=config.CACHE_TTL_LONG,
                )

        await asyncio.gather(*(run(ep) for ep in dedup_eps))

    return stations, errors


def _build_unified(stations: list[dict]) -> dict:
    groups: dict[str, list[dict]] = {}
    for st in stations:
        key = _dedupe_key(st)
        groups.setdefault(key, []).append(st)

    unified: list[dict] = []
    dupes = 0
    for key, members in groups.items():
        primary = _pick_primary(members)
        backups = [m for m in members if m is not primary]
        dupes += len(backups)
        # Carry forward last_update from ANY member if primary lacks one
        if not primary.get("last_update"):
            for m in members:
                if m.get("last_update"):
                    primary["last_update"] = m["last_update"]
                    break
        params_union_raw = sorted({p for m in members for p in m.get("params", [])})
        params_union_norm = sorted({p for m in members for p in m.get("params_normalized", [])})
        source_set = sorted({m.get("source", "") for m in members})
        alias_mapped = sum(int(m.get("params_alias_mapped_count", 0)) for m in members)
        fallback_mapped = sum(int(m.get("params_fallback_mapped_count", 0)) for m in members)
        param_units = {p: _CANONICAL_UNITS.get(p, "") for p in params_union_norm}
        canonical_id = f"u_{_norm_text(primary.get('name','') or primary.get('id',''))}_{abs(hash(key)) % 10_000_000}"
        unified.append(
            {
                "canonical_id": canonical_id,
                "name": primary.get("name"),
                "lat": primary.get("lat"),
                "lon": primary.get("lon"),
                "primary": primary,
                "backups": backups,
                "provider_count": len(members),
                "provider_sources": source_set,
                "params": params_union_raw,
                "params_normalized": params_union_norm,
                "param_units_normalized": param_units,
                "params_alias_mapped_count": alias_mapped,
                "params_fallback_mapped_count": fallback_mapped,
                "quality_tier": primary.get("quality_tier"),
            }
        )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "raw_station_count": len(stations),
        "unique_station_count": len(unified),
        "duplicates_collapsed": dupes,
        "stations": unified,
    }


def _apply_bbox(stations: list[dict], bbox: str) -> list[dict]:
    try:
        west, south, east, north = [float(x.strip()) for x in bbox.split(",")]
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"bbox must be west,south,east,north: {exc}") from exc

    out: list[dict] = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lon")
        if lat is None or lon is None:
            continue
        if south <= lat <= north and west <= lon <= east:
            out.append(st)
    return out


def _clean_unit(unit: Any) -> str:
    if unit is None:
        return ""
    return str(unit).replace("Â", "").strip().lower()


def _to_c(value: float, unit: str) -> Optional[float]:
    u = _clean_unit(unit)
    if u in {"c", "°c", "degc"}:
        return value
    if u in {"f", "°f", "degf"}:
        return (value - 32.0) * (5.0 / 9.0)
    if u in {"k", "kelvin"}:
        return value - 273.15
    return None


def _to_mps(value: float, unit: str) -> Optional[float]:
    u = _clean_unit(unit)
    if u in {"m/s", "ms-1", "mps"}:
        return value
    if u in {"kt", "kts", "knot", "knots"}:
        return value * 0.514444
    if u in {"km/h", "kph"}:
        return value / 3.6
    if u in {"mph"}:
        return value * 0.44704
    return None


def _to_hpa(value: float, unit: str) -> Optional[float]:
    u = _clean_unit(unit)
    if u in {"hpa", "mb", "mbar"}:
        return value
    if u in {"pa"}:
        return value / 100.0
    if u in {"inhg"}:
        return value * 33.8638866667
    return None


def _parse_iso_dt(value: Any) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:  # noqa: BLE001
        return None


def _dt_iso(value: Optional[datetime]) -> Optional[str]:
    if value is None:
        return None
    return value.astimezone(UTC).isoformat()


def _is_fresh(last_update: Optional[datetime], stale_minutes: int) -> bool:
    if last_update is None:
        return False
    age_sec = (datetime.now(UTC) - last_update.astimezone(UTC)).total_seconds()
    return age_sec <= stale_minutes * 60


def _extract_reading_value(parameter: str, value: float, unit: str) -> tuple[Optional[str], Optional[float]]:
    p = str(parameter or "").strip().lower()
    if p == "temperature":
        return "temperature_c", _to_c(value, unit)
    if p == "dewpoint":
        return "dewpoint_c", _to_c(value, unit)
    if p == "wind_speed":
        return "wind_speed_mps", _to_mps(value, unit)
    if p == "wind_gust":
        return "wind_gust_mps", _to_mps(value, unit)
    if p == "wind_direction":
        return "wind_direction_deg", value
    if p == "relative_humidity":
        return "relative_humidity_pct", value
    if p in {"pressure_msl", "barometric_pressure", "pressure", "altimeter"}:
        return "pressure_hpa", _to_hpa(value, unit)
    return None, None


def _provider_rank(provider: dict) -> tuple:
    fresh_rank = 0 if provider.get("is_fresh") else 1
    tier = provider.get("quality_tier") or SOURCE_QUALITY_TIERS.get(provider.get("source", ""), 9)
    source_priority = _SOURCE_PRIORITY.get(provider.get("source", ""), 999)
    core_count = sum(
        1 for k in ("temperature_c", "wind_speed_mps", "wind_direction_deg", "relative_humidity_pct", "pressure_hpa")
        if provider.get(k) is not None
    )
    return (fresh_rank, tier, source_priority, -core_count, provider.get("provider_id", "zzz"))


def _qc_for_provider(provider: dict, stale_minutes: int) -> dict:
    flags: list[str] = []
    score = 100

    if not provider.get("is_fresh"):
        flags.append("stale")
        score -= 40

    t = provider.get("temperature_c")
    if t is not None and (t < -90 or t > 60):
        flags.append("temperature_out_of_range")
        score -= 20

    rh = provider.get("relative_humidity_pct")
    if rh is not None and (rh < 0 or rh > 100):
        flags.append("humidity_out_of_range")
        score -= 20

    ws = provider.get("wind_speed_mps")
    if ws is not None and (ws < 0 or ws > 120):
        flags.append("wind_speed_out_of_range")
        score -= 20

    wd = provider.get("wind_direction_deg")
    if wd is not None and (wd < 0 or wd > 360):
        flags.append("wind_direction_out_of_range")
        score -= 10

    p = provider.get("pressure_hpa")
    if p is not None and (p < 800 or p > 1100):
        flags.append("pressure_out_of_range")
        score -= 20

    if score >= 85:
        level = "good"
    elif score >= 60:
        level = "fair"
    else:
        level = "poor"
    return {"score": max(0, score), "level": level, "flags": flags}


def _dedupe_current_key(provider: dict) -> str:
    idk = _station_id_core(provider.get("station_id"))
    if provider.get("lat") is not None and provider.get("lon") is not None and idk:
        latk = round(float(provider["lat"]), 2)
        lonk = round(float(provider["lon"]), 2)
        return f"geoid:{latk}:{lonk}:id:{idk}"
    if provider.get("lat") is not None and provider.get("lon") is not None:
        latk = round(float(provider["lat"]), 3)
        lonk = round(float(provider["lon"]), 3)
        namek = _norm_text(provider.get("name", ""))
        if namek:
            return f"geo:{latk}:{lonk}:name:{namek}"
    return f"id:{provider.get('source','')}:{_norm_text(provider.get('provider_id',''))}"


def _apply_bbox_current(stations: list[dict], bbox: str) -> list[dict]:
    try:
        west, south, east, north = [float(x.strip()) for x in bbox.split(",")]
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=400, detail=f"bbox must be west,south,east,north: {exc}") from exc

    out: list[dict] = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lon")
        if lat is None or lon is None:
            continue
        if south <= lat <= north and west <= lon <= east:
            out.append(st)
    return out


async def _collect_iem_map_providers(
    states: list[str],
    include_asos: bool,
    include_rwis: bool,
    include_discovered: bool,
    discovered_suffixes_csv: str,
) -> tuple[list[dict], list[dict], int]:
    suffixes = []
    if include_asos:
        suffixes.append("ASOS")
    if include_rwis:
        suffixes.append("RWIS")

    endpoints = [f"/api/mesonets/iem/current?network={state}_{suffix}" for state in states for suffix in suffixes]
    if include_discovered:
        discovered = await _discover_iem_network_ids(states=states, suffixes_csv=discovered_suffixes_csv)
        endpoints.extend([f"/api/mesonets/iem/current?network={nid}" for nid in discovered])
    # Deduplicate endpoint list while preserving order.
    seen: set[str] = set()
    endpoints = [ep for ep in endpoints if not (ep in seen or seen.add(ep))]
    if not endpoints:
        return [], [], 0
    return await _collect_iem_map_providers_by_networks(endpoints)


async def _discover_iem_network_ids(states: list[str], suffixes_csv: str) -> list[str]:
    state_set = {s.upper() for s in states}
    allowed_suffixes = {s.strip().upper() for s in suffixes_csv.split(",") if s.strip()}
    if not allowed_suffixes:
        return []

    payload = None
    try:
        async with _client() as client:
            r = await client.get(f"http://127.0.0.1:{config.PORT}/api/mesonets/iem/networks")
            r.raise_for_status()
            payload = r.json()
    except Exception:
        return []

    items = (((payload or {}).get("data") or {}).get("data") or [])
    out: set[str] = set()
    for item in items:
        if not isinstance(item, dict):
            continue
        nid = str(item.get("id", "")).strip().upper()
        if not nid:
            continue
        # Keep US-style state networks only (avoid global AE__ASOS style ids).
        m = re.match(r"^([A-Z]{2})_([A-Z0-9_]+)$", nid)
        if not m:
            continue
        st, suffix = m.group(1), m.group(2)
        if st not in state_set:
            continue
        if suffix in allowed_suffixes:
            out.add(nid)
    return sorted(out)


async def _collect_iem_map_providers_by_networks(network_endpoints: list[str]) -> tuple[list[dict], list[dict], int]:
    endpoints = network_endpoints
    if not endpoints:
        return [], [], 0
    sem = asyncio.Semaphore(_DEFAULT_CONCURRENCY)
    providers: list[dict] = []
    errors: list[dict] = []

    async with _client() as client:
        async def run(endpoint: str) -> None:
            async with sem:
                _, payload, err = await _fetch_endpoint(client, endpoint)
                if err:
                    errors.append({"endpoint": endpoint, "error": err})
                    return
                if not isinstance(payload, dict):
                    errors.append({"endpoint": endpoint, "error": "non-dict payload"})
                    return

                stations = payload.get("stations") or []
                readings = payload.get("readings") or []
                if not isinstance(stations, list) or not isinstance(readings, list):
                    errors.append({"endpoint": endpoint, "error": "missing stations/readings arrays"})
                    return

                station_by_id: dict[str, dict] = {}
                for st in stations:
                    if not isinstance(st, dict):
                        continue
                    sid = str(st.get("id", "")).strip()
                    if sid:
                        station_by_id[sid] = st

                obs_by_station: dict[str, dict] = {}
                for rd in readings:
                    if not isinstance(rd, dict):
                        continue
                    sid = str(rd.get("station_id", "")).strip()
                    if not sid:
                        continue
                    param = rd.get("parameter")
                    val = rd.get("value")
                    unit = rd.get("unit")
                    ts = _parse_iso_dt(rd.get("timestamp"))
                    try:
                        fval = float(val)
                    except (TypeError, ValueError):
                        continue
                    target_key, norm_val = _extract_reading_value(str(param or ""), fval, str(unit or ""))
                    if target_key is None or norm_val is None:
                        continue

                    entry = obs_by_station.setdefault(sid, {"last_update": None, "vals": {}, "ts_by_key": {}})
                    key_ts = entry["ts_by_key"].get(target_key)
                    if key_ts is None or (ts is not None and ts > key_ts):
                        entry["vals"][target_key] = norm_val
                        entry["ts_by_key"][target_key] = ts
                    if ts is not None and (entry["last_update"] is None or ts > entry["last_update"]):
                        entry["last_update"] = ts

                for sid, st in station_by_id.items():
                    vals = obs_by_station.get(sid, {"vals": {}, "last_update": None})
                    last_update = vals.get("last_update") or _parse_iso_dt(st.get("last_update"))
                    src = str(st.get("source") or "iem")
                    network = str(st.get("network") or "")
                    provider = {
                        "provider_id": f"{src}:{network}:{sid}",
                        "station_id": sid,
                        "name": st.get("name") or sid,
                        "lat": _float_or_none(st.get("lat")),
                        "lon": _float_or_none(st.get("lon")),
                        "elevation": _float_or_none(st.get("elevation")),
                        "source": src,
                        "network": network,
                        "state": (st.get("extra") or {}).get("state"),
                        "quality_tier": st.get("quality_tier") or SOURCE_QUALITY_TIERS.get(src),
                        "last_update": _dt_iso(last_update),
                        "is_fresh": False,
                    }
                    provider.update(vals.get("vals", {}))
                    providers.append(provider)

        await asyncio.gather(*(run(ep) for ep in endpoints))

    return providers, errors, len(endpoints)


def _build_current_map_payload(
    providers: list[dict],
    stale_minutes: int,
    include_backups: bool,
    errors: list[dict],
    requested_endpoints: int,
) -> dict:
    dedupe_groups: dict[str, list[dict]] = {}
    for p in providers:
        dt = _parse_iso_dt(p.get("last_update"))
        p["is_fresh"] = _is_fresh(dt, stale_minutes)
        p["qc"] = _qc_for_provider(p, stale_minutes)
        dedupe_groups.setdefault(_dedupe_current_key(p), []).append(p)

    stations: list[dict] = []
    for members in dedupe_groups.values():
        primary = sorted(members, key=_provider_rank)[0]
        backups = [m for m in members if m is not primary]

        station = {
            "canonical_id": f"m_{_norm_text(primary.get('name','') or primary.get('station_id',''))}_{abs(hash(_dedupe_current_key(primary))) % 10_000_000}",
            "name": primary.get("name"),
            "lat": primary.get("lat"),
            "lon": primary.get("lon"),
            "elevation": primary.get("elevation"),
            "source": primary.get("source"),
            "network": primary.get("network"),
            "state": primary.get("state"),
            "last_update": primary.get("last_update"),
            "is_fresh": primary.get("is_fresh"),
            "temperature_c": primary.get("temperature_c"),
            "dewpoint_c": primary.get("dewpoint_c"),
            "wind_speed_mps": primary.get("wind_speed_mps"),
            "wind_gust_mps": primary.get("wind_gust_mps"),
            "wind_direction_deg": primary.get("wind_direction_deg"),
            "relative_humidity_pct": primary.get("relative_humidity_pct"),
            "pressure_hpa": primary.get("pressure_hpa"),
            "qc": primary.get("qc", {}),
            "provider_count": len(members),
            "provider_sources": sorted({m.get("source", "") for m in members}),
            "primary": primary,
            "backups": backups if include_backups else [],
        }
        stations.append(station)

    stations.sort(key=lambda x: (not bool(x.get("is_fresh")), x.get("name") or ""))
    with_temp = sum(1 for s in stations if s.get("temperature_c") is not None)
    with_wind = sum(1 for s in stations if s.get("wind_speed_mps") is not None)
    with_temp_and_wind = sum(1 for s in stations if s.get("temperature_c") is not None and s.get("wind_speed_mps") is not None)
    fresh_count = sum(1 for s in stations if s.get("is_fresh"))
    qc_good = sum(1 for s in stations if (s.get("qc") or {}).get("level") == "good")
    qc_fair = sum(1 for s in stations if (s.get("qc") or {}).get("level") == "fair")
    qc_poor = sum(1 for s in stations if (s.get("qc") or {}).get("level") == "poor")
    freshest = None
    for s in stations:
        dt = _parse_iso_dt(s.get("last_update"))
        if dt and (freshest is None or dt > freshest):
            freshest = dt

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "stale_minutes": stale_minutes,
        "requested_endpoint_count": requested_endpoints,
        "error_count": len(errors),
        "errors": errors,
        "raw_provider_count": len(providers),
        "unique_station_count": len(stations),
        "duplicates_collapsed": max(0, len(providers) - len(stations)),
        "fresh_station_count": fresh_count,
        "qc_good_station_count": qc_good,
        "qc_fair_station_count": qc_fair,
        "qc_poor_station_count": qc_poor,
        "stations_with_temperature": with_temp,
        "stations_with_wind": with_wind,
        "stations_with_temp_and_wind": with_temp_and_wind,
        "freshest_observation_time": _dt_iso(freshest),
        "stations": stations,
    }


def _to_geojson(stations: list[dict]) -> dict:
    features = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lon")
        if lat is None or lon is None:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "canonical_id": st.get("canonical_id"),
                    "name": st.get("name"),
                    "source": st.get("source"),
                    "network": st.get("network"),
                    "state": st.get("state"),
                    "last_update": st.get("last_update"),
                    "is_fresh": st.get("is_fresh"),
                    "provider_count": st.get("provider_count"),
                    "temperature_c": st.get("temperature_c"),
                    "dewpoint_c": st.get("dewpoint_c"),
                    "wind_speed_mps": st.get("wind_speed_mps"),
                    "wind_gust_mps": st.get("wind_gust_mps"),
                    "wind_direction_deg": st.get("wind_direction_deg"),
                    "relative_humidity_pct": st.get("relative_humidity_pct"),
                    "pressure_hpa": st.get("pressure_hpa"),
                    "qc": st.get("qc", {}),
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _infer_domain(params_normalized: list[str], source: str) -> str:
    p = set(params_normalized or [])
    s = (source or "").lower()
    if p & {"water_level", "streamflow", "stream_stage", "storage"} or s in {"usgs", "noaa_water", "federal_water"}:
        return "water"
    if p & {"pm25", "pm10", "ozone", "air_quality_index"}:
        return "air_quality"
    if p & {"wave_height", "sea_surface_temperature", "dominant_wave_period"}:
        return "marine"
    if p & {"snow_depth", "snow_water_equivalent", "soil_moisture"}:
        return "cryosphere_soil"
    if p & {"temperature", "dewpoint", "wind_speed", "pressure", "relative_humidity"}:
        return "weather"
    return "metadata"


@router.get(
    "/stations",
    summary="Unified, normalized, deduplicated station catalog",
)
async def unified_stations(
    refresh: bool = Query(False, description="Force rebuild instead of cache hit"),
    include_backups: bool = Query(True, description="Include backup providers for duplicates"),
    include_param_sweeps: bool = Query(True, description="Include all-state/all-office sweeps for major federated APIs"),
    bbox: Optional[str] = Query(None, description="west,south,east,north"),
    limit: int = Query(20000, ge=1, le=250000),
):
    cache_key = f"unified:stations:v1:{include_param_sweeps}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            cached_stations = cached.get("stations", [])
            if cached_stations and "params_normalized" not in cached_stations[0]:
                cached = None
            else:
                payload = cached
                stations = payload.get("stations", [])
                if bbox:
                    stations = _apply_bbox(stations, bbox)
                stations = stations[:limit]
                if not include_backups:
                    stations = [{**s, "backups": []} for s in stations]
                return {
                    **payload,
                    "from_cache": True,
                    "returned": len(stations),
                    "stations": stations,
                }

    raw_stations, errors = await _collect_raw_stations(include_param_sweeps=include_param_sweeps)
    payload = _build_unified(raw_stations)
    payload["errors"] = errors
    payload["error_count"] = len(errors)
    await cache.set(cache_key, payload, ttl=config.CACHE_TTL_LONG)

    stations = payload["stations"]
    if bbox:
        stations = _apply_bbox(stations, bbox)
    stations = stations[:limit]
    if not include_backups:
        stations = [{**s, "backups": []} for s in stations]

    return {
        **payload,
        "from_cache": False,
        "returned": len(stations),
        "stations": stations,
    }


@router.get("/schema", summary="Normalized station schema and dedupe policy")
async def unified_schema():
    return {
        "canonical_fields": [
            "canonical_id",
            "name",
            "lat",
            "lon",
            "primary",
            "backups",
            "provider_count",
            "provider_sources",
            "params",
            "params_normalized",
            "param_units_normalized",
            "quality_tier",
        ],
        "dedupe_rule": "geo(3 decimals)+normalized_name; fallback source+id",
        "backup_policy": "all duplicate members kept in backups[]; primary picked by quality_tier then freshness hint",
        "param_normalization": {
            "strategy": "alias map first, fallback to stable slug",
            "canonical_unit_defaults": _CANONICAL_UNITS,
        },
    }


@router.get(
    "/variables",
    summary="Canonical variable registry and unit normalization targets",
)
async def unified_variables():
    aliases_by_canonical: dict[str, list[str]] = {}
    for alias, canonical in _PARAM_ALIAS_TO_CANONICAL.items():
        aliases_by_canonical.setdefault(canonical, []).append(alias)
    for canonical in aliases_by_canonical:
        aliases_by_canonical[canonical] = sorted(aliases_by_canonical[canonical])

    variables = []
    all_canon = sorted(set(list(_CANONICAL_UNITS.keys()) + list(aliases_by_canonical.keys())))
    for canonical in all_canon:
        variables.append(
            {
                "name": canonical,
                "normalized_unit": _CANONICAL_UNITS.get(canonical, ""),
                "aliases": aliases_by_canonical.get(canonical, []),
            }
        )
    return {"count": len(variables), "variables": variables}


@router.get(
    "/normalization/status",
    summary="Normalization coverage and readiness audit across unified catalog",
)
async def unified_normalization_status(
    refresh: bool = Query(False, description="Force rebuild of unified station cache first"),
    include_param_sweeps: bool = Query(True, description="Use all-state/all-office sweeps"),
):
    if refresh:
        raw_stations, errors = await _collect_raw_stations(include_param_sweeps=include_param_sweeps)
        payload = _build_unified(raw_stations)
        payload["errors"] = errors
        payload["error_count"] = len(errors)
        await cache.set(f"unified:stations:v1:{include_param_sweeps}", payload, ttl=config.CACHE_TTL_LONG)
    else:
        payload = await cache.get(f"unified:stations:v1:{include_param_sweeps}")
        if payload is None:
            raw_stations, errors = await _collect_raw_stations(include_param_sweeps=include_param_sweeps)
            payload = _build_unified(raw_stations)
            payload["errors"] = errors
            payload["error_count"] = len(errors)
            await cache.set(f"unified:stations:v1:{include_param_sweeps}", payload, ttl=config.CACHE_TTL_LONG)

    stations = payload.get("stations", [])
    total = len(stations)
    with_coords = sum(1 for s in stations if s.get("lat") is not None and s.get("lon") is not None)
    with_any_params = sum(1 for s in stations if s.get("params"))
    with_norm_params = sum(1 for s in stations if s.get("params_normalized"))

    raw_param_counter: Counter[str] = Counter()
    norm_param_counter: Counter[str] = Counter()
    fallback_counter: Counter[str] = Counter()

    for st in stations:
        raw_param_counter.update(st.get("params", []))
        norm_param_counter.update(st.get("params_normalized", []))
        # Approximate fallback exposure at station-level from detailed mapping in providers.
        for provider in [st.get("primary")] + (st.get("backups", []) or []):
            if not isinstance(provider, dict):
                continue
            for d in provider.get("params_normalization_details", []) or []:
                if d.get("method") == "fallback":
                    fallback_counter.update([d.get("normalized", "")])

    normalized_coverage_pct = round((with_norm_params / total) * 100.0, 2) if total else 0.0
    coord_coverage_pct = round((with_coords / total) * 100.0, 2) if total else 0.0

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "station_counts": {
            "raw_station_count": payload.get("raw_station_count", 0),
            "unique_station_count": total,
            "duplicates_collapsed": payload.get("duplicates_collapsed", 0),
        },
        "coverage": {
            "with_coordinates": with_coords,
            "with_coordinates_pct": coord_coverage_pct,
            "with_any_params": with_any_params,
            "with_normalized_params": with_norm_params,
            "with_normalized_params_pct": normalized_coverage_pct,
        },
        "top_raw_params": raw_param_counter.most_common(40),
        "top_normalized_params": norm_param_counter.most_common(40),
        "top_fallback_normalized_params": fallback_counter.most_common(40),
        "canonical_variable_count": len(set(_CANONICAL_UNITS.keys()) | set(_PARAM_ALIAS_TO_CANONICAL.values())),
        "source_endpoint_errors": payload.get("errors", []),
        "error_count": payload.get("error_count", 0),
        "ready": {
            "all_stations_have_normalized_param_keys": with_norm_params == total,
            "all_stations_have_coordinates": with_coords == total,
            "dedupe_enabled": True,
            "backup_providers_preserved": True,
        },
    }


@router.get(
    "/maps/catalog",
    summary="All-source normalized station catalog as map-ready GeoJSON",
)
async def unified_maps_catalog(
    refresh: bool = Query(False, description="Force rebuild instead of cache hit"),
    include_param_sweeps: bool = Query(True, description="Use all-state/all-office sweeps"),
    bbox: Optional[str] = Query(None, description="west,south,east,north"),
    domain: Optional[str] = Query(None, description="Filter domain: weather,water,air_quality,marine,cryosphere_soil,metadata"),
    limit: int = Query(250000, ge=1, le=500000),
):
    cache_key = f"unified:stations:v1:{include_param_sweeps}"
    payload = None
    if not refresh:
        payload = await cache.get(cache_key)
    if payload is None:
        raw_stations, errors = await _collect_raw_stations(include_param_sweeps=include_param_sweeps)
        payload = _build_unified(raw_stations)
        payload["errors"] = errors
        payload["error_count"] = len(errors)
        await cache.set(cache_key, payload, ttl=config.CACHE_TTL_LONG)

    stations = payload.get("stations", [])
    if bbox:
        stations = _apply_bbox(stations, bbox)

    features = []
    for st in stations:
        lat = st.get("lat")
        lon = st.get("lon")
        if lat is None or lon is None:
            continue
        params_norm = st.get("params_normalized", [])
        dom = _infer_domain(params_norm, (st.get("primary") or {}).get("source", ""))
        if domain and dom != domain:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": {
                    "canonical_id": st.get("canonical_id"),
                    "name": st.get("name"),
                    "domain": dom,
                    "quality_tier": st.get("quality_tier"),
                    "provider_count": st.get("provider_count"),
                    "provider_sources": st.get("provider_sources"),
                    "params_normalized": params_norm,
                    "param_units_normalized": st.get("param_units_normalized", {}),
                },
            }
        )
        if len(features) >= limit:
            break

    return {
        "meta": {
            "generated_at": datetime.now(UTC).isoformat(),
            "source_unique_station_count": payload.get("unique_station_count"),
            "returned": len(features),
            "domain_filter": domain,
            "include_param_sweeps": include_param_sweeps,
            "error_count": payload.get("error_count", 0),
        },
        "data": {"type": "FeatureCollection", "features": features},
    }


@router.get(
    "/readings/current",
    summary="Normalized current reading stream across unified current providers",
)
async def unified_readings_current(
    refresh: bool = Query(False),
    stale_minutes: int = Query(180, ge=10, le=1440),
    limit: int = Query(500000, ge=1, le=2000000),
):
    current = await unified_maps_current(
        refresh=refresh,
        states=None,
        include_iem_asos=True,
        include_iem_rwis=True,
        include_iem_discovered_networks=True,
        iem_discovered_suffixes=",".join(_IEM_DISCOVERED_SUFFIXES_DEFAULT),
        stale_minutes=stale_minutes,
        bbox=None,
        include_backups=False,
        format="array",
        limit=500000,
    )
    readings: list[dict] = []
    for st in current.get("stations", []):
        sid = st.get("canonical_id")
        ts = st.get("last_update")
        source = st.get("source")
        qc = st.get("qc", {})
        values = {
            "temperature": ("temperature_c", "C"),
            "dewpoint": ("dewpoint_c", "C"),
            "wind_speed": ("wind_speed_mps", "m/s"),
            "wind_gust": ("wind_gust_mps", "m/s"),
            "wind_direction": ("wind_direction_deg", "deg"),
            "relative_humidity": ("relative_humidity_pct", "%"),
            "pressure": ("pressure_hpa", "hPa"),
        }
        for param, (key, unit) in values.items():
            val = st.get(key)
            if val is None:
                continue
            readings.append(
                {
                    "station_id": sid,
                    "timestamp": ts,
                    "parameter": param,
                    "value": val,
                    "unit": unit,
                    "source": source,
                    "qc": qc,
                    "is_fresh": st.get("is_fresh", False),
                }
            )
            if len(readings) >= limit:
                break
        if len(readings) >= limit:
            break

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "stale_minutes": stale_minutes,
        "station_count": current.get("unique_station_count", 0),
        "reading_count": len(readings),
        "readings": readings,
        "error_count": current.get("error_count", 0),
    }


@router.get(
    "/freshness/summary",
    summary="Reasonably updated station counts at a freshness threshold",
)
async def unified_freshness_summary(
    minutes: int = Query(60, ge=5, le=1440, description="Freshness threshold in minutes"),
    refresh: bool = Query(False, description="Force recompute of current providers"),
):
    current = await unified_maps_current(
        refresh=refresh,
        states=None,
        include_iem_asos=True,
        include_iem_rwis=True,
        include_iem_discovered_networks=True,
        iem_discovered_suffixes=",".join(_IEM_DISCOVERED_SUFFIXES_DEFAULT),
        stale_minutes=minutes,
        bbox=None,
        include_backups=False,
        format="array",
        limit=500000,
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "freshness_minutes": minutes,
        "reasonably_updated_station_count": current.get("fresh_station_count", 0),
        "unique_station_count_in_current_feed": current.get("unique_station_count", 0),
        "stations_with_temperature": current.get("stations_with_temperature", 0),
        "stations_with_wind": current.get("stations_with_wind", 0),
        "qc_good_station_count": current.get("qc_good_station_count", 0),
        "qc_fair_station_count": current.get("qc_fair_station_count", 0),
        "qc_poor_station_count": current.get("qc_poor_station_count", 0),
        "error_count": current.get("error_count", 0),
    }


@router.get(
    "/reliability/status",
    summary="Unified ingestion reliability summary",
)
async def unified_reliability_status(
    include_param_sweeps: bool = Query(True),
    refresh: bool = Query(False),
):
    status = await unified_normalization_status(
        refresh=refresh,
        include_param_sweeps=include_param_sweeps,
    )
    errs = status.get("source_endpoint_errors", [])
    total_err = len(errs)
    used_cached = sum(1 for e in errs if e.get("used_cached"))
    hard_fail = total_err - used_cached
    endpoint_total = len(_endpoint_inventory(include_param_sweeps=include_param_sweeps))
    endpoint_success = max(0, endpoint_total - hard_fail)
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "include_param_sweeps": include_param_sweeps,
        "endpoint_total_attempted": endpoint_total,
        "endpoint_success_count": endpoint_success,
        "endpoint_error_count": total_err,
        "endpoint_errors_served_from_cache": used_cached,
        "endpoint_hard_failures": hard_fail,
        "reliability_score_pct": round((endpoint_success / max(1, endpoint_total)) * 100.0, 2),
        "errors": errs,
    }


@router.get(
    "/maps/current",
    summary="Real-time normalized station observations for map layers",
)
async def unified_maps_current(
    refresh: bool = Query(False, description="Force rebuild instead of cache hit"),
    states: Optional[str] = Query(None, description="Comma-separated 2-letter states. Default: all 50 states + DC"),
    include_iem_asos: bool = Query(True, description="Include IEM state ASOS networks"),
    include_iem_rwis: bool = Query(True, description="Include IEM state RWIS networks"),
    include_iem_discovered_networks: bool = Query(
        True,
        description="Discover additional IEM state networks by suffix and include in current sweep",
    ),
    iem_discovered_suffixes: str = Query(
        ",".join(_IEM_DISCOVERED_SUFFIXES_DEFAULT),
        description="Comma-separated suffix allowlist for discovered IEM networks (e.g. ASOS,RWIS,DCP,COOP)",
    ),
    stale_minutes: int = Query(180, ge=10, le=1440, description="Freshness window in minutes"),
    bbox: Optional[str] = Query(None, description="west,south,east,north"),
    include_backups: bool = Query(False, description="Include backup providers per canonical station"),
    format: str = Query("geojson", pattern="^(geojson|array)$"),
    limit: int = Query(200000, ge=1, le=500000),
):
    state_list = _MAP_STATES
    if states:
        parsed = [s.strip().upper() for s in states.split(",") if s.strip()]
        if not parsed:
            raise HTTPException(status_code=400, detail="states parsed empty")
        bad = [s for s in parsed if len(s) != 2]
        if bad:
            raise HTTPException(status_code=400, detail=f"invalid state codes: {bad}")
        state_list = parsed

    cache_key = (
        "unified:maps:current:v1:"
        f"{','.join(state_list)}:{include_iem_asos}:{include_iem_rwis}:{include_iem_discovered_networks}:"
        f"{iem_discovered_suffixes}:{stale_minutes}:{include_backups}"
    )
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            stations = cached.get("stations", [])
            if bbox:
                stations = _apply_bbox_current(stations, bbox)
            stations = stations[:limit]
            if format == "geojson":
                return {
                    "meta": {**cached, "returned": len(stations), "from_cache": True, "stations": None},
                    "data": _to_geojson(stations),
                }
            return {**cached, "returned": len(stations), "from_cache": True, "stations": stations}

    providers: list[dict] = []
    all_errors: list[dict] = []
    requested_count = 0

    iem_providers, iem_errors, iem_requested = await _collect_iem_map_providers(
        states=state_list,
        include_asos=include_iem_asos,
        include_rwis=include_iem_rwis,
        include_discovered=include_iem_discovered_networks,
        discovered_suffixes_csv=iem_discovered_suffixes,
    )
    providers.extend(iem_providers)
    all_errors.extend(iem_errors)
    requested_count += iem_requested

    payload = _build_current_map_payload(
        providers=providers,
        stale_minutes=stale_minutes,
        include_backups=include_backups,
        errors=all_errors,
        requested_endpoints=requested_count,
    )
    payload["states"] = state_list
    networks_included = [
        f"*_{sfx}" for sfx in _IEM_MAP_NETWORK_SUFFIXES
        if (sfx == "ASOS" and include_iem_asos) or (sfx == "RWIS" and include_iem_rwis)
    ]
    if include_iem_discovered_networks:
        discovered_suffixes = sorted({s.strip().upper() for s in iem_discovered_suffixes.split(",") if s.strip()})
        networks_included.extend([f"*_{sfx}" for sfx in discovered_suffixes])
    payload["networks_included"] = sorted(set(networks_included))
    await cache.set(cache_key, payload, ttl=config.CACHE_TTL)

    stations = payload["stations"]
    if bbox:
        stations = _apply_bbox_current(stations, bbox)
    stations = stations[:limit]

    if format == "geojson":
        return {
            "meta": {**payload, "returned": len(stations), "from_cache": False, "stations": None},
            "data": _to_geojson(stations),
        }
    return {**payload, "returned": len(stations), "from_cache": False, "stations": stations}


@router.get(
    "/maps/proof",
    summary="Proof metrics for Synoptic-style station + map readiness",
)
async def unified_maps_proof(
    refresh: bool = Query(False, description="Force live recompute for map metrics"),
    stale_minutes: int = Query(180, ge=10, le=1440),
):
    station_cache = await cache.get("unified:stations:v1:True")
    if station_cache is None:
        # Build with param sweeps for broadest station catalog proof.
        raw_stations, errors = await _collect_raw_stations(include_param_sweeps=True)
        station_cache = _build_unified(raw_stations)
        station_cache["errors"] = errors
        station_cache["error_count"] = len(errors)
        await cache.set("unified:stations:v1:True", station_cache, ttl=config.CACHE_TTL_LONG)

    current = await unified_maps_current(
        refresh=refresh,
        states=None,
        include_iem_asos=True,
        include_iem_rwis=True,
        include_iem_discovered_networks=True,
        iem_discovered_suffixes=",".join(_IEM_DISCOVERED_SUFFIXES_DEFAULT),
        stale_minutes=stale_minutes,
        bbox=None,
        include_backups=False,
        format="array",
        limit=500000,
    )

    unified_stations = station_cache.get("stations", [])
    norm_station_count = sum(1 for s in unified_stations if s.get("params_normalized"))

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "station_catalog": {
            "raw_station_count": station_cache.get("raw_station_count"),
            "unique_station_count": station_cache.get("unique_station_count"),
            "duplicates_collapsed": station_cache.get("duplicates_collapsed"),
            "stations_with_normalized_params": norm_station_count,
            "error_count": station_cache.get("error_count", 0),
        },
        "map_current_coverage": {
            "requested_endpoint_count": current.get("requested_endpoint_count"),
            "raw_provider_count": current.get("raw_provider_count"),
            "unique_station_count": current.get("unique_station_count"),
            "fresh_station_count": current.get("fresh_station_count"),
            "stations_with_temperature": current.get("stations_with_temperature"),
            "stations_with_wind": current.get("stations_with_wind"),
            "stations_with_temp_and_wind": current.get("stations_with_temp_and_wind"),
            "freshest_observation_time": current.get("freshest_observation_time"),
            "error_count": current.get("error_count"),
        },
        "readiness": {
            "temp_map_ready": (current.get("stations_with_temperature") or 0) > 1000,
            "wind_map_ready": (current.get("stations_with_wind") or 0) > 1000,
            "temp_wind_joint_ready": (current.get("stations_with_temp_and_wind") or 0) > 1000,
            "dedupe_enabled": True,
            "backup_providers_preserved": True,
            "normalized_units_enabled": True,
            "all_catalog_stations_have_normalized_param_keys": norm_station_count == (station_cache.get("unique_station_count") or 0),
        },
    }


# ---------------------------------------------------------------------------
# NEW: Primary query interface endpoints
# ---------------------------------------------------------------------------


async def _get_or_build_catalog(refresh: bool = False, include_param_sweeps: bool = True) -> dict:
    """Return the unified station catalog, building it if needed."""
    cache_key = f"unified:stations:v1:{include_param_sweeps}"
    if not refresh:
        cached = await cache.get(cache_key)
        if cached is not None:
            return cached
    raw_stations, errors = await _collect_raw_stations(include_param_sweeps=include_param_sweeps)
    payload = _build_unified(raw_stations)
    payload["errors"] = errors
    payload["error_count"] = len(errors)
    await cache.set(cache_key, payload, ttl=config.CACHE_TTL_LONG)
    return payload


def _stations_within_radius(
    stations: list[dict],
    lat: float,
    lon: float,
    radius_km: float,
) -> list[dict]:
    """Filter stations within radius_km of (lat, lon), annotate with distance_km, sort by distance."""
    results: list[dict] = []
    for st in stations:
        slat = st.get("lat")
        slon = st.get("lon")
        if slat is None or slon is None:
            continue
        dist = _haversine_km(lat, lon, slat, slon)
        if dist <= radius_km:
            results.append({**st, "distance_km": round(dist, 2)})
    results.sort(key=lambda s: s["distance_km"])
    return results


def _filter_by_params(stations: list[dict], params: list[str]) -> list[dict]:
    """Keep only stations whose params_normalized intersects the requested params."""
    if not params:
        return stations
    # Normalize the requested params through the alias map.
    wanted: set[str] = set()
    for p in params:
        canonical, _ = _normalize_param_name(p.strip())
        if canonical:
            wanted.add(canonical)
        else:
            wanted.add(p.strip().lower())
    return [
        st for st in stations
        if wanted & set(st.get("params_normalized", []))
    ]


@router.get(
    "/search",
    summary="Geographic station search across all sources with dedup",
)
async def unified_search(
    lat: float = Query(..., ge=-90, le=90, description="Latitude of search center"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude of search center"),
    radius_km: float = Query(50.0, ge=0.1, le=500, description="Search radius in km"),
    params: Optional[str] = Query(None, description="Comma-separated parameter filter (e.g. temperature,precipitation)"),
    domain: Optional[str] = Query(None, description="Domain filter: weather,water,air_quality,marine,cryosphere_soil"),
    quality_tier_max: int = Query(5, ge=1, le=5, description="Maximum quality tier to include (1=best)"),
    include_backups: bool = Query(True, description="Include backup provider details"),
    limit: int = Query(100, ge=1, le=5000, description="Max stations to return"),
    refresh: bool = Query(False, description="Force rebuild of unified catalog"),
):
    """Search for stations near a geographic point across ALL source modules.

    Returns deduplicated stations sorted by distance, with backup sources listed.
    This is the primary query interface for the unified station catalog.
    """
    catalog = await _get_or_build_catalog(refresh=refresh)
    stations = catalog.get("stations", [])

    # Geographic filter.
    nearby = _stations_within_radius(stations, lat, lon, radius_km)

    # Parameter filter.
    if params:
        param_list = [p.strip() for p in params.split(",") if p.strip()]
        nearby = _filter_by_params(nearby, param_list)

    # Domain filter.
    if domain:
        nearby = [
            st for st in nearby
            if _infer_domain(
                st.get("params_normalized", []),
                (st.get("primary") or {}).get("source", ""),
            ) == domain
        ]

    # Quality tier filter.
    if quality_tier_max < 5:
        nearby = [st for st in nearby if (st.get("quality_tier") or 5) <= quality_tier_max]

    # Trim.
    nearby = nearby[:limit]

    # Strip backups if not requested.
    if not include_backups:
        nearby = [{**st, "backups": []} for st in nearby]

    # Annotate with domain.
    for st in nearby:
        st["domain"] = _infer_domain(
            st.get("params_normalized", []),
            (st.get("primary") or {}).get("source", ""),
        )

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "query": {
            "lat": lat,
            "lon": lon,
            "radius_km": radius_km,
            "params": params,
            "domain": domain,
            "quality_tier_max": quality_tier_max,
        },
        "catalog_station_count": catalog.get("unique_station_count", 0),
        "returned": len(nearby),
        "stations": nearby,
    }


async def _fetch_readings_for_station(
    client: httpx.AsyncClient,
    station_id: str,
    source: str,
    network: str,
    hours: int,
    param: Optional[str],
) -> Optional[dict]:
    """Attempt to fetch readings from one source for one station.

    Returns a dict with readings list or None on failure.
    """
    template = _SOURCE_READINGS_ENDPOINTS.get(source)
    if source == "noaa_weather" and (network or "").upper() == "NDBC":
        template = "/api/noaa_weather/ndbc/stations/{station_id}"
    if not template:
        # Fallback: try the generic pattern /api/{source}/stations/{station_id}/readings
        template = f"/api/{source}/stations/{{station_id}}/readings?hours={{hours}}"

    url_path = template.format(
        station_id=station_id,
        hours=hours,
        network=network or "",
        param=param or "",
    )
    base_url = f"http://127.0.0.1:{config.PORT}{url_path}"

    try:
        r = await client.get(base_url)
        r.raise_for_status()
        payload = r.json()

        # Extract readings from various response shapes.
        readings = []
        if isinstance(payload, dict):
            readings = (
                payload.get("readings")
                or payload.get("data")
                or payload.get("observations")
                or []
            )
            # Some endpoints nest readings inside station detail.
            if not readings and "station" in payload:
                readings = payload.get("readings", [])
            # synoptic timeseries nests under "readings"
            if not readings and "count" in payload:
                readings = payload.get("readings", [])
        if not isinstance(readings, list):
            readings = []

        if not readings:
            return None

        return {
            "source": source,
            "station_id": station_id,
            "endpoint": url_path,
            "reading_count": len(readings),
            "readings": readings,
        }
    except Exception:  # noqa: BLE001
        return None


@router.get(
    "/readings",
    summary="Unified readings with source fallback",
)
async def unified_readings(
    lat: float = Query(..., ge=-90, le=90, description="Latitude"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude"),
    radius_km: float = Query(20.0, ge=0.1, le=200, description="Search radius in km"),
    param: Optional[str] = Query(None, description="Parameter filter (e.g. precipitation, temperature)"),
    hours: int = Query(24, ge=1, le=720, description="Hours of history to request"),
    max_stations: int = Query(10, ge=1, le=50, description="Max stations to return readings for"),
    refresh: bool = Query(False, description="Force rebuild of station catalog"),
):
    """Find stations near a point and fetch readings with automatic source fallback.

    For each deduplicated station, the highest-tier source is tried first.
    If that fails, backup sources are tried in priority order.
    Returns readings with full source attribution.
    """
    catalog = await _get_or_build_catalog(refresh=refresh)
    stations = catalog.get("stations", [])

    # Geographic + param filter.
    nearby = _stations_within_radius(stations, lat, lon, radius_km)
    if param:
        param_list = [p.strip() for p in param.split(",") if p.strip()]
        nearby = _filter_by_params(nearby, param_list)

    nearby = nearby[:max_stations]
    if not nearby:
        return {
            "generated_at": datetime.now(UTC).isoformat(),
            "query": {"lat": lat, "lon": lon, "radius_km": radius_km, "param": param, "hours": hours},
            "station_count": 0,
            "stations": [],
        }

    # For each station, try primary then backup sources.
    results: list[dict] = []

    async with _client() as client:
        async def fetch_with_fallback(station: dict) -> dict:
            primary = station.get("primary", {})
            backups = station.get("backups", [])
            # Order: primary first, then backups sorted by _SOURCE_PRIORITY.
            candidates = [primary] + sorted(
                backups,
                key=lambda m: (
                    m.get("quality_tier") or SOURCE_QUALITY_TIERS.get(m.get("source", ""), 9),
                    _SOURCE_PRIORITY.get(m.get("source", ""), 999),
                ),
            )

            for candidate in candidates:
                src = candidate.get("source", "")
                sid = candidate.get("id", "")
                net = candidate.get("network", "")
                if not sid:
                    continue
                result = await _fetch_readings_for_station(client, sid, src, net, hours, param)
                if result is not None:
                    return {
                        "canonical_id": station.get("canonical_id"),
                        "name": station.get("name"),
                        "lat": station.get("lat"),
                        "lon": station.get("lon"),
                        "distance_km": station.get("distance_km"),
                        "quality_tier": candidate.get("quality_tier"),
                        "source_used": src,
                        "station_id_used": sid,
                        "sources_available": station.get("provider_sources", []),
                        "fallback_used": candidate is not primary,
                        "reading_count": result["reading_count"],
                        "readings": result["readings"],
                    }

            # All sources failed.
            return {
                "canonical_id": station.get("canonical_id"),
                "name": station.get("name"),
                "lat": station.get("lat"),
                "lon": station.get("lon"),
                "distance_km": station.get("distance_km"),
                "quality_tier": station.get("quality_tier"),
                "source_used": None,
                "station_id_used": None,
                "sources_available": station.get("provider_sources", []),
                "fallback_used": False,
                "reading_count": 0,
                "readings": [],
                "error": "all sources failed",
            }

        results = await asyncio.gather(*(fetch_with_fallback(st) for st in nearby))

    # Sort: stations with readings first, then by distance.
    results = sorted(results, key=lambda r: (r["reading_count"] == 0, r.get("distance_km", 999)))

    total_readings = sum(r["reading_count"] for r in results)
    stations_with_data = sum(1 for r in results if r["reading_count"] > 0)
    fallback_count = sum(1 for r in results if r.get("fallback_used"))

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "query": {"lat": lat, "lon": lon, "radius_km": radius_km, "param": param, "hours": hours},
        "station_count": len(results),
        "stations_with_data": stations_with_data,
        "total_readings": total_readings,
        "fallback_used_count": fallback_count,
        "stations": list(results),
    }


@router.get(
    "/status",
    summary="Source health dashboard — status of all source modules",
)
async def unified_status(
    refresh: bool = Query(False, description="Bypass 5-minute status cache"),
):
    """Query each source module with a lightweight probe request.

    Returns per-source health status (ok, slow, error, unreachable) with response times.
    Results are cached for 5 minutes.
    """
    status_cache_key = "unified:status:v1"
    if not refresh:
        cached = await cache.get(status_cache_key)
        if cached is not None:
            return {**cached, "from_cache": True}

    # Build probe list: one lightweight endpoint per source module.
    # These are cheap metadata/station-list endpoints — NOT full data fetches.
    _PROBE_ENDPOINTS: dict[str, str] = {
        "usgs": "/api/usgs/stations?state=OR&limit=1",
        "noaa_water": "/api/noaa_water/coops/stations?limit=1",
        "noaa_weather": "/api/noaa_weather/nws/stations?state=OR&limit=1",
        "synoptic": "/api/synoptic/stations/nearby?lat=45.5&lon=-122.6&radius=1&limit=1",
        "snotel": "/api/noaa_weather/snotel/stations?state=OR&limit=1",
        "climate": "/api/climate/acis/stations?state=OR&limit=1",
        "mesonets": "/api/mesonets/iem/networks",
        "federal_water": "/api/federal_water/usace/locations?office=NWP&limit=1",
        "aquarius": "/api/aquarius/instances",
        "erddap": "/api/erddap/datasets?limit=1",
        "regional": "/api/regional/stations?limit=1",
        "california": "/api/california/cdec/stations?limit=1",
        "eastcoast": "/api/eastcoast/stations?limit=1",
        "infrastructure": "/api/infrastructure/stations?limit=1",
        "international": "/api/international/stations?limit=1",
        "specialty": "/api/specialty/stations?limit=1",
        "citizen": "/api/citizen/stations?limit=1",
    }

    sem = asyncio.Semaphore(20)
    source_statuses: list[dict] = []

    async with _client() as client:
        async def probe(source_name: str, endpoint: str) -> dict:
            async with sem:
                url = f"http://127.0.0.1:{config.PORT}{endpoint}"
                t0 = time.monotonic()
                try:
                    r = await client.get(url)
                    elapsed_ms = round((time.monotonic() - t0) * 1000)
                    if r.status_code < 400:
                        status = "slow" if elapsed_ms > 5000 else "ok"
                    elif r.status_code == 404:
                        status = "not_found"
                    else:
                        status = "error"
                    return {
                        "source": source_name,
                        "endpoint": endpoint,
                        "status": status,
                        "http_status": r.status_code,
                        "response_time_ms": elapsed_ms,
                        "quality_tier": SOURCE_QUALITY_TIERS.get(source_name),
                    }
                except httpx.TimeoutException:
                    elapsed_ms = round((time.monotonic() - t0) * 1000)
                    return {
                        "source": source_name,
                        "endpoint": endpoint,
                        "status": "timeout",
                        "http_status": None,
                        "response_time_ms": elapsed_ms,
                        "quality_tier": SOURCE_QUALITY_TIERS.get(source_name),
                    }
                except Exception as exc:  # noqa: BLE001
                    elapsed_ms = round((time.monotonic() - t0) * 1000)
                    return {
                        "source": source_name,
                        "endpoint": endpoint,
                        "status": "unreachable",
                        "http_status": None,
                        "response_time_ms": elapsed_ms,
                        "error": str(exc)[:200],
                        "quality_tier": SOURCE_QUALITY_TIERS.get(source_name),
                    }

        source_statuses = list(await asyncio.gather(
            *(probe(name, ep) for name, ep in _PROBE_ENDPOINTS.items())
        ))

    # Sort: ok first, then slow, then errors.
    status_order = {"ok": 0, "slow": 1, "not_found": 2, "error": 3, "timeout": 4, "unreachable": 5}
    source_statuses.sort(key=lambda s: (status_order.get(s["status"], 9), s["source"]))

    ok_count = sum(1 for s in source_statuses if s["status"] == "ok")
    slow_count = sum(1 for s in source_statuses if s["status"] == "slow")
    error_count = sum(1 for s in source_statuses if s["status"] in ("error", "timeout", "unreachable"))
    avg_ms = round(sum(s["response_time_ms"] for s in source_statuses) / max(1, len(source_statuses)))

    payload = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_count": len(source_statuses),
        "ok": ok_count,
        "slow": slow_count,
        "error": error_count,
        "not_probed": len(source_statuses) - ok_count - slow_count - error_count,
        "avg_response_time_ms": avg_ms,
        "sources": source_statuses,
    }
    await cache.set(status_cache_key, payload, ttl=300)  # 5-minute cache
    return {**payload, "from_cache": False}
