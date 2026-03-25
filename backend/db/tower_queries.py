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
) -> Iterator[dict[str, Any]]:
    where_sql, params = _build_where(
        color=color, state=state, owner=owner, min_score=min_score, max_score=max_score
    )
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
        color=color, state=state, owner=owner, min_score=min_score, max_score=max_score
    )
    sql = text(f"SELECT * FROM towers WHERE {where_sql}")
    first = True
    with engine.connect() as conn:
        for chunk in pd.read_sql(sql, conn, params=params, chunksize=CHUNK_SIZE):
            yield chunk.to_csv(index=False, header=first, na_rep="").encode("utf-8")
            first = False


def fetch_doe_match_meta_sql(engine: Engine) -> tuple[int, int, int]:
    sql = text(
        """
        SELECT
          COUNT(*)::bigint AS total,
          SUM(CASE WHEN COALESCE(doe_event_count, 0) > 0 THEN 1 ELSE 0 END)::bigint AS matched
        FROM towers
        """
    )
    with engine.connect() as conn:
        row = conn.execute(sql).mappings().one()
    total = int(row["total"] or 0)
    matched = int(row["matched"] or 0)
    unmatched = total - matched
    return total, matched, unmatched


def iter_doe_match_features_sql(engine: Engine) -> Iterator[dict[str, Any]]:
    sql = text(
        """
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
    )
    with engine.connect() as conn:
        for chunk in pd.read_sql(sql, conn, chunksize=CHUNK_SIZE):
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
    sql = text(
        f"{_GEOJSON_SELECT.strip()} WHERE tower_id = :tid LIMIT 1"
    )
    with engine.connect() as conn:
        row = conn.execute(sql, {"tid": tower_id}).mappings().first()
    if not row:
        return None
    return _sql_row_to_props(dict(row), TOWER_PROPERTY_KEYS)
