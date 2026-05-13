from __future__ import annotations

import math
import os
import urllib.error
import urllib.request
from typing import Any
from urllib.parse import urlencode, unquote

import numpy as np
import pandas as pd
from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response, StreamingResponse

from db import tower_queries as tq
from db.database import get_df, get_engine, uses_database
from models.tower import TOWER_PROPERTY_KEYS

router = APIRouter(prefix="/towers", tags=["towers"])


def _serialize_value(val: Any) -> Any:
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return None
    if isinstance(val, (np.integer, np.floating)):
        return val.item()
    if isinstance(val, float) and val.is_integer():
        return int(val)
    return val


def _row_to_props(row: pd.Series) -> dict[str, Any]:
    d: dict[str, Any] = {}
    for key in TOWER_PROPERTY_KEYS:
        if key not in row.index:
            d[key] = "—"
            continue
        d[key] = _serialize_value(row[key])
    return d


def _doe_match_num(row: pd.Series, key: str) -> float | int:
    if key not in row.index:
        return 0
    v = pd.to_numeric(row[key], errors="coerce")
    if pd.isna(v):
        return 0
    x = float(v)
    if math.isnan(x) or math.isinf(x):
        return 0
    if abs(x - round(x)) < 1e-9:
        return int(round(x))
    return x


def _apply_tower_filters(
    df: pd.DataFrame,
    *,
    color: str | None = None,
    state: str | None = None,
    owner: str | None = None,
    min_score: float | None = None,
    max_score: float | None = None,
) -> pd.DataFrame:
    if color:
        df = df[df["concern_color"].astype(str) == color]
    if state and state.strip() and "worst_storm_state" in df.columns:
        s = state.strip()
        df = df[df["worst_storm_state"].astype(str) == s]
    if owner and owner.strip():
        needle = owner.strip().lower()
        df = df[df["owner"].astype(str).str.lower().str.contains(needle, na=False)]
    if min_score is not None:
        df = df[df["exposure_score"] >= min_score]
    if max_score is not None:
        df = df[df["exposure_score"] <= max_score]
    return df


def _doe_match_str(row: pd.Series, key: str) -> str:
    if key not in row.index:
        return "—"
    val = row[key]
    if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
        return "—"
    s = str(val).strip()
    if not s or s.lower() == "nan":
        return "—"
    return s


@router.get("/stats")
def get_stats() -> dict[str, Any]:
    if uses_database():
        return tq.fetch_stats_sql(get_engine())
    df = get_df()
    total = len(df)
    red = int((df["concern_color"] == "#E24B4A").sum())
    amber = int((df["concern_color"] == "#EF9F27").sum())
    green = int((df["concern_color"] == "green").sum())
    avg_score = round(float(df["exposure_score"].mean()), 4) if total else 0.0

    owners = (
        df[df["owner"].astype(str) != "NOT AVAILABLE"]["owner"]
        .astype(str)
        .value_counts()
        .head(10)
    )
    top_owners = [{"name": str(name), "count": int(count)} for name, count in owners.items()]

    return {
        "total": total,
        "red": red,
        "amber": amber,
        "green": green,
        "avg_score": avg_score,
        "top_owners": top_owners,
    }


@router.get("/geojson")
def get_geojson(
    color: str | None = Query(None),
    state: str | None = Query(None),
    owner: str | None = Query(None),
    min_score: float | None = Query(None),
    max_score: float | None = Query(None),
    limit: int = Query(5000, ge=1, le=50000),
    min_lon: float | None = Query(None),
    min_lat: float | None = Query(None),
    max_lon: float | None = Query(None),
    max_lat: float | None = Query(None),
) -> dict[str, Any]:
    if uses_database():
        eng = get_engine()
        features = list(
            tq.iter_geojson_features_sql(
                eng,
                color=color,
                state=state,
                owner=owner,
                min_score=min_score,
                max_score=max_score,
                property_keys=TOWER_PROPERTY_KEYS,
                limit=limit,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        )
        return {
            "type": "FeatureCollection",
            "features": features,
            "meta": {"count": len(features), "limit": limit},
        }

    df = _apply_tower_filters(
        get_df().copy(),
        color=color,
        state=state,
        owner=owner,
        min_score=min_score,
        max_score=max_score,
    )
    if (
        min_lon is not None
        and min_lat is not None
        and max_lon is not None
        and max_lat is not None
    ):
        df = df[
            (df["longitude"] >= min_lon)
            & (df["longitude"] <= max_lon)
            & (df["latitude"] >= min_lat)
            & (df["latitude"] <= max_lat)
        ]
    df = df.head(limit)

    features = []
    for _, row in df.iterrows():
        lon = float(row["longitude"])
        lat = float(row["latitude"])
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": _row_to_props(row),
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {"count": len(features), "limit": limit},
    }


@router.get("/filters/options")
def get_filter_options() -> dict[str, Any]:
    if uses_database():
        return tq.fetch_filter_options_sql(get_engine())
    df = get_df()
    states: list[str] = []
    if "worst_storm_state" in df.columns:
        states = sorted(
            {
                str(x)
                for x in df["worst_storm_state"].unique()
                if str(x) and str(x) != "—" and str(x).lower() not in ("nan", "none")
            }
        )
    nerc = sorted(
        {
            str(x)
            for x in df["doe_nerc_region"].unique()
            if str(x) and str(x) != "—" and str(x).lower() != "nan"
        }
    )
    return {
        "states": states,
        "colors": ["#E24B4A", "#EF9F27", "green"],
        "nerc_regions": nerc,
    }


@router.get("/export/csv", response_model=None)
def export_towers_csv(
    color: str | None = Query(None),
    state: str | None = Query(None),
    owner: str | None = Query(None),
    min_score: float | None = Query(None),
    max_score: float | None = Query(None),
) -> Response:
    if uses_database():
        return StreamingResponse(
            tq.stream_csv_sql(
                get_engine(),
                color=color,
                state=state,
                owner=owner,
                min_score=min_score,
                max_score=max_score,
            ),
            media_type="text/csv; charset=utf-8",
            headers={
                "Content-Disposition": 'attachment; filename="towers_export.csv"',
            },
        )

    df = _apply_tower_filters(
        get_df().copy(),
        color=color,
        state=state,
        owner=owner,
        min_score=min_score,
        max_score=max_score,
    )
    csv_bytes = df.to_csv(index=False, na_rep="").encode("utf-8")
    return Response(
        content=csv_bytes,
        media_type="text/csv; charset=utf-8",
        headers={
            "Content-Disposition": 'attachment; filename="towers_export.csv"',
        },
    )


@router.get("/geojson/doe-match")
def get_geojson_doe_match(
    limit: int = Query(5000, ge=1, le=50000),
    min_lon: float | None = Query(None),
    min_lat: float | None = Query(None),
    max_lon: float | None = Query(None),
    max_lat: float | None = Query(None),
) -> dict[str, Any]:
    if uses_database():
        eng = get_engine()
        # Keep meta as GLOBAL dataset totals (independent of viewport bbox),
        # while features are still limited by bbox/limit for memory safety.
        total, matched, unmatched = tq.fetch_doe_match_meta_sql(eng)
        features = list(
            tq.iter_doe_match_features_sql(
                eng,
                limit=limit,
                min_lon=min_lon,
                min_lat=min_lat,
                max_lon=max_lon,
                max_lat=max_lat,
            )
        )
        return {
            "type": "FeatureCollection",
            "features": features,
            "meta": {
                "total": total,
                "matched": matched,
                "unmatched": unmatched,
                "limit": limit,
            },
        }

    df = get_df()
    ec = pd.to_numeric(df["doe_event_count"], errors="coerce").fillna(0)
    matched = int((ec > 0).sum())
    total = len(df)
    unmatched = total - matched
    if (
        min_lon is not None
        and min_lat is not None
        and max_lon is not None
        and max_lat is not None
    ):
        df = df[
            (df["longitude"] >= min_lon)
            & (df["longitude"] <= max_lon)
            & (df["latitude"] >= min_lat)
            & (df["latitude"] <= max_lat)
        ]
    df = df.head(limit)

    features: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        ecn = _doe_match_num(row, "doe_event_count")
        doe_matched = float(ecn) > 0
        color = "#E24B4A" if doe_matched else "#1D9E75"

        lon = float(row["longitude"])
        lat = float(row["latitude"])

        props = {
            "id": _serialize_value(row["id"]) if "id" in row.index else None,
            "owner": _doe_match_str(row, "owner"),
            "volt_class": _doe_match_str(row, "volt_class"),
            "doe_matched": doe_matched,
            "doe_event_count": ecn,
            "doe_max_customers": _doe_match_num(row, "doe_max_customers"),
            "doe_max_mw_loss": _doe_match_num(row, "doe_max_mw_loss"),
            "doe_dominant_type": _doe_match_str(row, "doe_dominant_type"),
            "doe_nerc_region": _doe_match_str(row, "doe_nerc_region"),
            "color": color,
        }

        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [lon, lat]},
                "properties": props,
            }
        )

    return {
        "type": "FeatureCollection",
        "features": features,
        "meta": {
            "total": total,
            "matched": matched,
            "unmatched": unmatched,
        },
    }


@router.get("/image")
def get_tower_satellite_image_query(
    tower_id: str = Query(..., description="Tower id (e.g. node/33349175). Prefer this over path form."),
    zoom: int = Query(18, ge=14, le=20),
) -> Response:
    return _tower_satellite_image_response(tower_id, zoom)


@router.get("/image/{tower_id:path}")
def get_tower_satellite_image_path(
    tower_id: str,
    zoom: int = Query(18, ge=14, le=20),
) -> Response:
    return _tower_satellite_image_response(tower_id, zoom)


def _safe_coord_float(val: Any) -> float | None:
    if val is None:
        return None
    if isinstance(val, str):
        s = val.strip()
        if not s or s == "—":
            return None
    try:
        x = float(val)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(x):
        return None
    return x


def _tower_satellite_image_response(raw_tower_id: str, zoom: int) -> Response:
    """
    Fetch the Static Maps image on the server and return bytes to the client.

    Redirecting the browser straight to Google often breaks <img> loading (Referer
    restrictions on the API key, redirect handling, or JSON/HTML error bodies).
    Same-origin image bytes avoid those issues.
    """
    api_key = os.getenv("GOOGLE_MAPS_KEY")
    if not api_key or not str(api_key).strip():
        raise HTTPException(status_code=500, detail="GOOGLE_MAPS_KEY not configured")

    tower_id = unquote((raw_tower_id or "").strip())

    try:
        tower_props = get_tower(tower_id)
    except HTTPException:
        raise
    lat_val = tower_props.get("latitude")
    lon_val = tower_props.get("longitude")
    lat = _safe_coord_float(lat_val)
    lon = _safe_coord_float(lon_val)

    if lat is None or lon is None:
        raise HTTPException(status_code=404, detail="Tower not found")

    params = {
        "center": f"{lat},{lon}",
        "zoom": str(zoom),
        "size": "560x320",
        "maptype": "satellite",
        "markers": f"color:red|{lat},{lon}",
        "key": api_key.strip(),
    }
    url = "https://maps.googleapis.com/maps/api/staticmap?" + urlencode(params)

    headers: dict[str, str] = {"User-Agent": "steel-tower-risk-dashboard/1.0"}
    referer = os.getenv("GOOGLE_STATIC_MAPS_REFERER", "http://localhost:5173/").strip()
    if referer:
        headers["Referer"] = referer

    try:
        req = urllib.request.Request(url, headers=headers, method="GET")
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            media_type = resp.headers.get("Content-Type", "image/png")
    except urllib.error.HTTPError as e:
        snippet = e.read().decode("utf-8", errors="replace")[:400]
        raise HTTPException(
            status_code=502,
            detail=f"Google Static Maps returned {e.code}. {snippet}",
        ) from e
    except urllib.error.URLError as e:
        raise HTTPException(status_code=502, detail=f"Could not reach Google Static Maps: {e.reason}") from e

    return Response(
        content=body,
        media_type=media_type.split(";")[0].strip() or "image/png",
        headers={"Cache-Control": "public, max-age=3600"},
    )


@router.get("/{tower_id:path}")
def get_tower(tower_id: str) -> dict[str, Any]:
    tower_id = unquote((tower_id or "").strip())
    if uses_database():
        row = tq.fetch_tower_by_id_sql(get_engine(), tower_id)
        if row is None:
            raise HTTPException(status_code=404, detail="Tower not found")
        return row

    df = get_df()
    tid_clean = (tower_id or "").strip()
    ids = df["id"].astype(str).str.strip()
    for tid in tq.tower_id_lookup_candidates(tid_clean):
        match = df[ids == tid]
        if not match.empty:
            return _row_to_props(match.iloc[0])
    if "/" in tid_clean:
        suf = tid_clean.split("/", 1)[1].strip()
        if suf:
            tail = ids.str.split("/").str[-1]
            loose = df[(ids == suf) | (tail == suf)]
            if not loose.empty:
                return _row_to_props(loose.iloc[0])
    raise HTTPException(status_code=404, detail="Tower not found")
