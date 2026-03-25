import os
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_df: Optional[pd.DataFrame] = None
_engine: Optional[Engine] = None

NUMERIC_COLS = {
    "latitude",
    "longitude",
    "voltage",
    "exposure_score",
    "max_wind_speed",
    "max_snow_depth",
    "min_temp",
    "avg_temp",
    "max_storm_damage_usd",
    "total_storm_damage",
    "storm_event_count",
    "storm_deaths",
    "storm_injuries",
    "doe_event_count",
    "doe_max_customers",
    "doe_max_mw_loss",
}


def _normalize_dataframe(raw: pd.DataFrame) -> pd.DataFrame:
    """Match legacy CSV normalization: numeric coercion + display strings."""
    df = raw.copy()
    for col in df.columns:
        if col in NUMERIC_COLS or pd.api.types.is_numeric_dtype(df[col]):
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
        else:
            s = df[col].fillna("—").astype(str)
            df[col] = s.replace({"nan": "—", "NaT": "—", "None": "—", "": "—"})
    return df


def _load_from_csv(path: str) -> pd.DataFrame:
    raw = pd.read_csv(path)
    rename_map = {
        "@id": "id",
        "VOLTAGE": "voltage",
        "VOLT_CLASS": "volt_class",
        "OWNER": "owner",
        "STATUS": "status",
    }
    raw = raw.rename(columns=rename_map)
    return _normalize_dataframe(raw)


def uses_database() -> bool:
    """True when DATABASE_URL is used: queries run in Postgres, no full-table RAM load."""
    return _engine is not None


def load_data() -> None:
    """
    DATABASE_URL set: create a small SQLAlchemy pool only (chunked queries per request).
    Else: load a CSV from DATA_PATH (optional local dev only; no default filename).
    """
    global _df, _engine
    db_url = os.getenv("DATABASE_URL", "").strip()
    if db_url:
        _engine = create_engine(
            db_url,
            pool_pre_ping=True,
            pool_size=2,
            max_overflow=2,
            pool_recycle=300,
        )
        _df = None
        with _engine.connect() as conn:
            n = conn.execute(text("SELECT COUNT(*) FROM towers")).scalar()
        print(
            f"Database mode: {int(n):,} towers in `towers` (no full-table load; chunked SQL reads)"
        )
        return

    _engine = None
    path_raw = os.getenv("DATA_PATH", "").strip()
    if not path_raw:
        raise RuntimeError(
            "No data source configured: set DATABASE_URL (Supabase/Postgres), "
            "or set DATA_PATH to an existing scored CSV for offline/local use."
        )
    path = str(Path(path_raw).expanduser().resolve())
    if not Path(path).is_file():
        raise RuntimeError(
            f"DATA_PATH is not a file: {path}. "
            "Set DATABASE_URL, or point DATA_PATH at your CSV."
        )
    _df = _load_from_csv(path)
    print(f"CSV mode: loaded {len(_df)} tower rows from {path} (in-memory DataFrame)")


def get_engine() -> Engine:
    if _engine is None:
        raise RuntimeError("Database engine not initialized; set DATABASE_URL and call load_data()")
    return _engine


def get_df() -> pd.DataFrame:
    """In-memory towers table (CSV mode only)."""
    if _df is None:
        raise RuntimeError("Dataframe not loaded; call load_data() first (CSV mode)")
    return _df
