"""
SQL helpers for towers table (Supabase/Postgres). Used when DATABASE_URL is set
to avoid loading the full table into RAM at startup.
"""

from __future__ import annotations

import math
from typing import Any, Iterator

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from models.tower import TOWER_PROPERTY_KEYS


def _serialize_value(val: Any) -> Any:
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return None
    if isinstance(val, (np.integer, np.floating)):
        return val.item()
    if isinstance(val, float) and val.is_integer():
        return int(val)
    return val


def _sql_row_to_props(row: dict[str, Any], keys: list[str]) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for key in keys:
        if key not in row or row[key] is None:
            d[key] = "—"
            continue
        v = row[key]
        if isinstance(v, str) and (not v.strip() or v.lower() == "nan"):
            d[key] = "—"
            continue
        d[key] = _serialize_value(v)
    return d


def tower_id_lookup_candidates(tower_id: str) -> list[str]:
    """
    OSM-style ids are often stored as 'node/12345'. URLs may pass only the numeric
    part, or the DB may store only one form — try sensible alternates.
    """
    t = (tower_id or "").strip()
    if not t:
        return []
    candidates = [t]
    if "/" in t:
        suffix = t.split("/", 1)[1].strip()
        if suffix:
            candidates.append(suffix)
    elif t.isdigit():
        candidates.append(f"node/{t}")
    seen: set[str] = set()
    out: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            out.append(c)
    return out


# Columns selected for API properties (tower_id exposed as id)
_GEOJSON_SELECT = """
SELECT
  tower_id AS id,
  longitude,
  latitude,
  voltage,
  volt_class,
  owner,
  status,
  structure,
  exposure_score,
  concern_color,
  max_wind_speed,
  max_snow_depth,
  min_temp,
  avg_temp,
  nearest_storm_type,
  max_storm_damage_usd,
  total_storm_damage,
  storm_event_count,
  storm_deaths,
  storm_injuries,
  worst_storm_state,
  doe_event_count,
  doe_max_customers,
  doe_max_mw_loss,
  doe_dominant_type,
  doe_worst_utility,
  doe_nerc_region,
  doe_location_source
FROM towers
"""


def _build_where(
    *,
    color: str | None,
    state: str | None,
    owner: str | None,
    min_score: float | None,
    max_score: float | None,
    min_lon: float | None,
    min_lat: float | None,
    max_lon: float | None,
    max_lat: float | None,
) -> tuple[str, dict[str, Any]]:
    clauses: list[str] = ["1=1"]
    params: dict[str, Any] = {}
    if color:
        clauses.append("concern_color = :color")
        params["color"] = color
    if state and state.strip():
        clauses.append("worst_storm_state = :state")
        params["state"] = state.strip()
    if owner and owner.strip():
        clauses.append("LOWER(owner) LIKE LOWER(:owner_like)")
        params["owner_like"] = f"%{owner.strip()}%"
    if min_score is not None:
        clauses.append("exposure_score >= :min_score")
        params["min_score"] = min_score
    if max_score is not None:
        clauses.append("exposure_score <= :max_score")
        params["max_score"] = max_score
    if (
        min_lon is not None
        and min_lat is not None
        and max_lon is not None
        and max_lat is not None
    ):
        clauses.append("longitude >= :min_lon")
        clauses.append("longitude <= :max_lon")
        clauses.append("latitude >= :min_lat")
        clauses.append("latitude <= :max_lat")
        params["min_lon"] = min_lon
        params["max_lon"] = max_lon
        params["min_lat"] = min_lat
        params["max_lat"] = max_lat
    return " AND ".join(clauses), params


CHUNK_SIZE = 5000


def fetch_stats_sql(engine: Engine) -> dict[str, Any]:
    sql = text(
        """
        SELECT
          COUNT(*)::bigint AS total,
          SUM(CASE WHEN concern_color = '#E24B4A' THEN 1 ELSE 0 END)::bigint AS red,
          SUM(CASE WHEN concern_color = '#EF9F27' THEN 1 ELSE 0 END)::bigint AS amber,
          SUM(CASE WHEN concern_color = 'green' THEN 1 ELSE 0 END)::bigint AS green,
          AVG(exposure_score)::float AS avg_score
        FROM towers
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().one()
    total = int(row["total"] or 0)
    avg_score = round(float(row["avg_score"] or 0), 4) if total else 0.0

    top_sql = text(
        """
        SELECT owner, COUNT(*)::bigint AS cnt
        FROM towers
        WHERE COALESCE(TRIM(owner), '') <> ''
          AND UPPER(TRIM(owner)) <> 'NOT AVAILABLE'
        GROUP BY owner
        ORDER BY cnt DESC
        LIMIT 10
        """
    )
    with engine.connect() as conn:
        tops = conn.execute(top_sql).mappings().all()
    top_owners = [{"name": str(r["owner"]), "count": int(r["cnt"])} for r in tops]

    return {
        "total": total,
        "red": int(row["red"] or 0),
        "amber": int(row["amber"] or 0),
        "green": int(row["green"] or 0),
        "avg_score": avg_score,
        "top_owners": top_owners,
    }


def iter_geojson_features_sql(
    engine: Engine,
    *,
    color: str | None,
    state: str | None,
    owner: str | None,
    min_score: float | None,
    max_score: float | None,
    property_keys: list[str],
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
    limit: int | None = None,
) -> Iterator[dict[str, Any]]:
    where_sql, params = _build_where(
        color=color,
        state=state,
        owner=owner,
        min_score=min_score,
        max_score=max_score,
        min_lon=min_lon,
        min_lat=min_lat,
        max_lon=max_lon,
        max_lat=max_lat,
    )
    if limit is not None:
        sql = text(f"{_GEOJSON_SELECT.strip()} WHERE {where_sql} LIMIT :limit")
        params["limit"] = int(limit)
    else:
        sql = text(f"{_GEOJSON_SELECT.strip()} WHERE {where_sql}")
    with engine.connect() as conn:
        for chunk in pd.read_sql(sql, conn, params=params, chunksize=CHUNK_SIZE):
            for rec in chunk.to_dict("records"):
                lon = float(rec["longitude"])
                lat = float(rec["latitude"])
                props = _sql_row_to_props(rec, property_keys)
                yield {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": props,
                }


def fetch_filter_options_sql(engine: Engine) -> dict[str, Any]:
    st_sql = text(
        """
        SELECT DISTINCT worst_storm_state
        FROM towers
        WHERE worst_storm_state IS NOT NULL
          AND TRIM(worst_storm_state) <> ''
          AND LOWER(TRIM(worst_storm_state)) NOT IN ('nan', 'none', '—')
        ORDER BY 1
        """
    )
    nerc_sql = text(
        """
        SELECT DISTINCT doe_nerc_region
        FROM towers
        WHERE doe_nerc_region IS NOT NULL
          AND TRIM(doe_nerc_region) <> ''
          AND LOWER(TRIM(doe_nerc_region)) <> 'nan'
        ORDER BY 1
        """
    )
    with engine.connect() as conn:
        states = [str(r[0]) for r in conn.execute(st_sql).fetchall()]
        nerc = [str(r[0]) for r in conn.execute(nerc_sql).fetchall()]
    return {
        "states": states,
        "colors": ["#E24B4A", "#EF9F27", "green"],
        "nerc_regions": nerc,
    }


def stream_csv_sql(
    engine: Engine,
    *,
    color: str | None,
    state: str | None,
    owner: str | None,
    min_score: float | None,
    max_score: float | None,
) -> Iterator[bytes]:
    where_sql, params = _build_where(
        color=color,
        state=state,
        owner=owner,
        min_score=min_score,
        max_score=max_score,
        min_lon=None,
        min_lat=None,
        max_lon=None,
        max_lat=None,
    )
    sql = text(f"SELECT * FROM towers WHERE {where_sql}")
    first = True
    with engine.connect() as conn:
        for chunk in pd.read_sql(sql, conn, params=params, chunksize=CHUNK_SIZE):
            yield chunk.to_csv(index=False, header=first, na_rep="").encode("utf-8")
            first = False


def fetch_doe_match_meta_sql(
    engine: Engine,
    *,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
) -> tuple[int, int, int]:
    where_clauses: list[str] = []
    params: dict[str, Any] = {}
    if (
        min_lon is not None
        and min_lat is not None
        and max_lon is not None
        and max_lat is not None
    ):
        where_clauses.append("longitude >= :min_lon")
        where_clauses.append("longitude <= :max_lon")
        where_clauses.append("latitude >= :min_lat")
        where_clauses.append("latitude <= :max_lat")
        params.update(
            {
                "min_lon": min_lon,
                "max_lon": max_lon,
                "min_lat": min_lat,
                "max_lat": max_lat,
            }
        )
    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    sql = text(
        f"""
        SELECT
          COUNT(*)::bigint AS total,
          SUM(CASE WHEN COALESCE(doe_event_count, 0) > 0 THEN 1 ELSE 0 END)::bigint AS matched
        FROM towers{where_sql}
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql, params).mappings().one()
    total = int(row["total"] or 0)
    matched = int(row["matched"] or 0)
    unmatched = total - matched
    return total, matched, unmatched


def iter_doe_match_features_sql(
    engine: Engine,
    *,
    limit: int | None = None,
    min_lon: float | None = None,
    min_lat: float | None = None,
    max_lon: float | None = None,
    max_lat: float | None = None,
) -> Iterator[dict[str, Any]]:
    select_sql = """
        SELECT
          tower_id AS id,
          longitude,
          latitude,
          owner,
          volt_class,
          COALESCE(doe_event_count, 0)::double precision AS doe_event_count,
          COALESCE(doe_max_customers, 0)::double precision AS doe_max_customers,
          COALESCE(doe_max_mw_loss, 0)::double precision AS doe_max_mw_loss,
          doe_dominant_type,
          doe_nerc_region,
          CASE WHEN COALESCE(doe_event_count, 0) > 0 THEN TRUE ELSE FALSE END AS doe_matched,
          CASE WHEN COALESCE(doe_event_count, 0) > 0 THEN '#E24B4A' ELSE '#1D9E75' END AS color
        FROM towers
    """
    where_clauses: list[str] = []
    params: dict[str, Any] = {}
    if (
        min_lon is not None
        and min_lat is not None
        and max_lon is not None
        and max_lat is not None
    ):
        where_clauses.append("longitude >= :min_lon")
        where_clauses.append("longitude <= :max_lon")
        where_clauses.append("latitude >= :min_lat")
        where_clauses.append("latitude <= :max_lat")
        params.update(
            {
                "min_lon": min_lon,
                "max_lon": max_lon,
                "min_lat": min_lat,
                "max_lat": max_lat,
            }
        )

    where_sql = f" WHERE {' AND '.join(where_clauses)}" if where_clauses else ""
    limit_sql = " LIMIT :limit" if limit is not None else ""
    if limit is not None:
        params["limit"] = int(limit)

    sql = text(select_sql + where_sql + limit_sql)
    with engine.connect() as conn:
        for chunk in pd.read_sql(sql, conn, params=params if params else None, chunksize=CHUNK_SIZE):
            for rec in chunk.to_dict("records"):
                ecn = float(rec.get("doe_event_count") or 0)
                lon = float(rec["longitude"])
                lat = float(rec["latitude"])
                props = {
                    "id": _serialize_value(rec.get("id")),
                    "owner": _doe_str(rec.get("owner")),
                    "volt_class": _doe_str(rec.get("volt_class")),
                    "doe_matched": bool(rec.get("doe_matched")),
                    "doe_event_count": _doe_num(ecn),
                    "doe_max_customers": _doe_num(rec.get("doe_max_customers")),
                    "doe_max_mw_loss": _doe_num(rec.get("doe_max_mw_loss")),
                    "doe_dominant_type": _doe_str(rec.get("doe_dominant_type")),
                    "doe_nerc_region": _doe_str(rec.get("doe_nerc_region")),
                    "color": rec.get("color") or "#1D9E75",
                }
                yield {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [lon, lat]},
                    "properties": props,
                }


def _doe_str(val: Any) -> str:
    if val is None:
        return "—"
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return "—"
    return s


def _doe_num(val: Any) -> float | int:
    if val is None:
        return 0
    try:
        x = float(val)
    except (TypeError, ValueError):
        return 0
    if math.isnan(x) or math.isinf(x):
        return 0
    if abs(x - round(x)) < 1e-9:
        return int(round(x))
    return x


def fetch_tower_by_id_sql(engine: Engine, tower_id: str) -> dict[str, Any] | None:
    """Resolve tower_id against OSM-style ids and plain numeric ids."""
    raw = (tower_id or "").strip()
    if not raw:
        return None

    cands = tower_id_lookup_candidates(raw)
    clauses: list[str] = []
    params: dict[str, Any] = {}
    for i, c in enumerate(cands):
        key = f"c{i}"
        clauses.append(f"TRIM(tower_id::text) = TRIM(:{key})")
        params[key] = c

    # Match backend-only numeric ids when the client sends "node/<digits>"
    if "/" in raw:
        suf = raw.split("/", 1)[1].strip()
        if suf:
            clauses.append(
                "("
                "(POSITION('/' IN TRIM(tower_id::text)) = 0 AND TRIM(tower_id::text) = :id_suffix)"
                " OR "
                "(POSITION('/' IN TRIM(tower_id::text)) > 0 AND split_part(TRIM(tower_id::text), '/', 2) = :id_suffix)"
                ")"
            )
            params["id_suffix"] = suf

    where_sql = " OR ".join(f"({c})" for c in clauses)
    sql = text(f"{_GEOJSON_SELECT.strip()} WHERE {where_sql} LIMIT 1")
    with engine.connect() as conn:
        row = conn.execute(sql, params).mappings().first()
    if not row:
        return None
    return _sql_row_to_props(dict(row), TOWER_PROPERTY_KEYS)
