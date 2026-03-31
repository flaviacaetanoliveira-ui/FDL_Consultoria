"""Normalização de SKU e valores numéricos (BR / misto com ponto decimal)."""
from __future__ import annotations

import re

import numpy as np
import pandas as pd


def normalize_sku_key(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _parse_number_scalar(raw: object) -> float:
    """Interpreta valores com vírgula BR (1.234,56) ou ponto decimal (79.95)."""
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return float("nan")
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return float("nan")
    s = s.replace("\u00a0", " ").replace(" ", "").strip()
    neg = s.startswith("-")
    if neg:
        s = s[1:].strip()
    s = re.sub(r"[^\d,\.\-]", "", s)
    if not s or s in (".", ",", "-"):
        return float("nan")
    last_c = s.rfind(",")
    last_d = s.rfind(".")
    if last_c != -1 and last_d != -1:
        if last_c > last_d:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif last_c != -1:
        s = s.replace(".", "").replace(",", ".")
    else:
        nd = s.count(".")
        if nd == 1:
            i = s.index(".")
            tail = s[i + 1 :]
            if len(tail) <= 2 and tail.isdigit():
                pass
            else:
                s = s.replace(".", "")
        elif nd > 1:
            s = s.replace(".", "")
    try:
        v = float(s)
    except ValueError:
        return float("nan")
    return -v if neg else v


def to_numeric_br(series: pd.Series) -> pd.Series:
    if series.dtype == object or str(series.dtype).startswith("string"):
        return series.map(_parse_number_scalar)
    return pd.to_numeric(series, errors="coerce")
