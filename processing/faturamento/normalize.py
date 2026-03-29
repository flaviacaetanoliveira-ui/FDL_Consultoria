"""Normalização de SKU e valores numéricos (BR)."""
from __future__ import annotations

import pandas as pd


def normalize_sku_key(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def to_numeric_br(series: pd.Series) -> pd.Series:
    s = series
    if s.dtype == object or str(s.dtype).startswith("string"):
        t = s.fillna("").astype(str).str.strip()
        t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        t = t.str.replace(r"[^0-9\.-]", "", regex=True)
        return pd.to_numeric(t, errors="coerce")
    return pd.to_numeric(s, errors="coerce")
