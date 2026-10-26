"""v2 readings endpoint — cross-source readings with source fallback and unit normalization.

GET /api/v2/readings
  - Finds stations near (lat, lon) from the unified catalog
  - Fetches time-series readings with automatic source fallback
  - Normalizes units to canonical targets (C, m/s, hPa, mm, …)
  - Optionally filters by parameter name or v2 category
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, Optional

from fastapi import APIRouter, Query

import cache
import config
from models import SOURCE_QUALITY_TIERS
from sources.unified import (
    _CANONICAL_UNITS,
    _PARAM_ALIAS_TO_CANONICAL,
    _SOURCE_PRIORITY,
    _SOURCE_READINGS_ENDPOINTS,
    _client,
    _fetch_readings_for_station,
    _filter_by_params,
    _get_or_build_catalog,
    _haversine_km,
    _normalize_param_name,
    _stations_within_radius,
    _to_c,
    _to_hpa,
    _to_mps,
)
from sources.v2.registry import CATEGORIES, _SOURCE_TO_CATEGORY

router = APIRouter(tags=["v2-readings"])

# ---------------------------------------------------------------------------
# Unit normalization
# ---------------------------------------------------------------------------

def _clean_unit(unit: Any) -> str:
    if unit is None:
        return ""
    return str(unit).replace("\u00c2", "").strip().lower()


def _to_mm(value: float, unit: str) -> Optional[float]:
    u = _clean_unit(unit)
    if u in {"mm", "millimeters"}:
        return value
    if u in {"in", "inches"}:
        return value * 25.4
    if u in {"cm", "centimeters"}:
        return value * 10.0
    if u in {"m", "meters"}:
        return value * 1000.0
    return None


def _to_km(value: float, unit: str) -> Optional[float]:
    u = _clean_unit(unit)
    if u in {"km", "kilometers"}:
        return value
    if u in {"m", "meters"}:
        return value / 1000.0
    if u in {"mi", "miles", "statute miles"}:
        return value * 1.60934
    if u in {"nm", "nautical miles"}:
        return value * 1.852
    return None


def _normalize_reading_value(
    parameter: str, value: float, unit: str
) -> tuple[Optional[float], str]:
    """Normalize a reading value to its canonical unit.

    Returns (normalized_value, canonical_unit).
    If normalization isn't possible, returns (original_value, original_unit).
    """
    canonical_param, _ = _normalize_param_name(parameter)
    if not canonical_param:
        canonical_param = parameter

    target_unit = _CANONICAL_UNITS.get(canonical_param, "")
    if not target_unit:
        return value, _clean_unit(unit) or unit

    # Temperature family → C
    if target_unit == "C":
        converted = _to_c(value, unit)
        if converted is not None:
            return round(converted, 2), "C"

    # Wind speed family → m/s
    elif target_unit == "m/s":
        converted = _to_mps(value, unit)
        if converted is not None:
            return round(converted, 2), "m/s"

    # Pressure family → hPa
    elif target_unit == "hPa":
        converted = _to_hpa(value, unit)
        if converted is not None:
            return round(converted, 2), "hPa"

    # Precipitation / snow / SWE → mm
    elif target_unit == "mm":
        converted = _to_mm(value, unit)
        if converted is not None:
            return round(converted, 2), "mm"

    # Visibility → km
    elif target_unit == "km":
        converted = _to_km(value, unit)
        if converted is not None:
            return round(converted, 2), "km"

    # Passthrough for units that don't need conversion (%, deg, ft, cfs, …)
    elif target_unit in {"%", "deg", "ft", "cfs", "ppt", "V", "s", "m", "W/m2", "code"}:
        return value, target_unit

    # Fallback: return original
    return value, _clean_unit(unit) or unit


# ---------------------------------------------------------------------------
# Category filtering helpers
# ---------------------------------------------------------------------------

def _sources_for_category(category: str) -> set[str]:
    """Return the set of source names belonging to a v2 category."""
    cat_meta = CATEGORIES.get(category)
    if cat_meta is None:
        return set()
    return set(cat_meta["sources"])


def _station_matches_category(station: dict, category: str) -> bool:
    """Check if any provider (primary or backup) belongs to the given category."""
    allowed = _sources_for_category(category)
    if not allowed:
        return False
    # NDBC buoys are served by noaa_weather but belong to the ocean category.
    if category == "ocean" and any(
        (m.get("network") or "").upper() == "NDBC"
        for m in [station.get("primary", {}), *station.get("backups", [])]
    ):
        return True
    primary = station.get("primary", {})
    if primary.get("source", "") in allowed:
        return True
    for backup in station.get("backups", []):
        if backup.get("source", "") in allowed:
            return True
    return False


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get(
    "/readings",
    summary="Cross-source readings with source fallback and unit normalization",
)
async def v2_readings(
    lat: float = Query(..., ge=-90, le=90, description="Latitude of search center"),
    lon: float = Query(..., ge=-180, le=180, description="Longitude of search center"),
    radius_km: float = Query(20.0, ge=0.1, le=500, description="Search radius in km"),
    param: Optional[str] = Query(
        None,
        description="Specific parameter to filter (temperature, precipitation, wind_speed, …)",
    ),
    category: Optional[str] = Query(
        None,
        description="Filter stations by v2 category (weather, water, air_quality, fire, ocean, climate, …)",
    ),
    hours: int = Query(24, ge=1, le=720, description="Lookback window in hours"),
    max_stations: int = Query(5, ge=1, le=50, description="Max stations to return"),
    normalize_units: bool = Query(
        True,
        description="Normalize all readings to canonical units (C, m/s, hPa, mm, …)",
    ),
    refresh: bool = Query(False, description="Force rebuild of station catalog"),
):
    """Find nearby stations and fetch time-series readings with automatic source fallback.

    For each deduplicated station the highest-quality source is tried first.
    If that source fails, backup sources are tried in priority order until one
    succeeds.  Readings are optionally normalized to canonical SI-style units.
    """
    # ------------------------------------------------------------------
    # 1. Build query descriptor (echoed back in the response)
    # ------------------------------------------------------------------
    query_desc: dict[str, Any] = {
        "lat": lat,
        "lon": lon,
        "radius_km": radius_km,
    }
    if param:
        query_desc["param"] = param
    if category:
        query_desc["category"] = category
    query_desc["hours"] = hours
    query_desc["max_stations"] = max_stations
    query_desc["normalize_units"] = normalize_units

    # ------------------------------------------------------------------
    # 2. Get unified catalog and find nearby stations
    # ------------------------------------------------------------------
    catalog = await _get_or_build_catalog(refresh=refresh)
    all_stations = catalog.get("stations", [])

    nearby = _stations_within_radius(all_stations, lat, lon, radius_km)

    # Parameter filter
    if param:
        param_list = [p.strip() for p in param.split(",") if p.strip()]
        nearby = _filter_by_params(nearby, param_list)

    # Category filter
    if category:
        nearby = [st for st in nearby if _station_matches_category(st, category)]

    nearby = nearby[:max_stations]

    if not nearby:
        return {
            "query": query_desc,
            "stations": [],
        }

    # ------------------------------------------------------------------
    # 3. Fetch readings with source fallback
    # ------------------------------------------------------------------
    results: list[dict] = []

    async with _client() as client:

        async def _fetch_with_fallback(station: dict) -> Optional[dict]:
            primary = station.get("primary", {})
            backups = station.get("backups", [])

            # Order candidates: primary first, then backups by quality tier
            candidates = [primary] + sorted(
                backups,
                key=lambda m: (
                    m.get("quality_tier") or SOURCE_QUALITY_TIERS.get(m.get("source", ""), 9),
                    _SOURCE_PRIORITY.get(m.get("source", ""), 999),
                ),
            )

            # If category filter is active, prefer candidates from that category
            if category:
                allowed = _sources_for_category(category)
                in_cat = [c for c in candidates if c.get("source", "") in allowed]
                out_cat = [c for c in candidates if c.get("source", "") not in allowed]
                candidates = in_cat + out_cat

            for candidate in candidates:
                src = candidate.get("source", "")
                sid = candidate.get("id", "")
                net = candidate.get("network", "")
                if not sid:
                    continue

                result = await _fetch_readings_for_station(
                    client, sid, src, net, hours, param,
                )
                if result is not None and result.get("readings"):
                    raw_readings = result["readings"]

                    # Normalize each reading
                    normalized: list[dict] = []
                    for rd in raw_readings:
                        if not isinstance(rd, dict):
                            continue

                        ts = rd.get("timestamp")
                        raw_param = rd.get("parameter", "")
                        raw_value = rd.get("value")
                        raw_unit = rd.get("unit", "")

                        # Resolve canonical parameter name
                        canonical_param, _ = _normalize_param_name(raw_param)
                        if not canonical_param:
                            canonical_param = raw_param

                        # Parse value
                        try:
                            fval = float(raw_value)
                        except (TypeError, ValueError):
                            continue

                        # Unit normalization
                        if normalize_units:
                            norm_val, norm_unit = _normalize_reading_value(
                                raw_param, fval, raw_unit,
                            )
                        else:
                            norm_val = fval
                            norm_unit = raw_unit

                        normalized.append({
                            "timestamp": ts,
                            "parameter": canonical_param,
                            "value": round(norm_val, 4) if norm_val is not None else None,
                            "unit": norm_unit,
                            "source": src,
                        })

                    if not normalized:
                        continue

                    return {
                        "id": station.get("canonical_id", sid),
                        "name": station.get("name"),
                        "source": src,
                        "distance_km": station.get("distance_km"),
                        "reading_count": len(normalized),
                        "readings": normalized,
                    }

            # All candidates failed
            return None

        raw_results = await asyncio.gather(
            *(_fetch_with_fallback(st) for st in nearby)
        )

    # Filter out stations where all sources failed
    results = [r for r in raw_results if r is not None]

    # Sort by distance
    results.sort(key=lambda r: r.get("distance_km", 999))

    return {
        "query": query_desc,
        "stations": results,
    }
