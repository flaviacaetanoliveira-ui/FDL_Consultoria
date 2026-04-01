"""Normalização de SKU e valores numéricos (BR / misto com ponto decimal)."""
from __future__ import annotations

import re

import numpy as np
import pandas as pd


def normalize_sku_join_key_scalar(raw: object) -> str:
    """
    Chave canónica para join pedidos ↔ custo e auditoria.

    1. texto; 2. trim; 3. remover sufixo ``.0`` típico de export Excel/float;
    4. remover zeros à esquerda em cadeias só numéricas (``03160`` → ``3160``).
    Identificadores alfanuméricos (ex.: ``SKU-A``) mantêm-se após trim / ``.0``.
    """
    if raw is None:
        return ""
    try:
        if isinstance(raw, float) and np.isnan(raw):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(raw, str) and raw.strip().lower() in ("nan", "none", "nat", "<na>"):
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    if s.lower() in ("nan", "none", "nat", "<na>"):
        return ""
    # Excel: "3160.0", "03160.0"
    if re.fullmatch(r"-?\d+\.0", s):
        s = s[:-2]
    if not s:
        return ""
    # Apenas dígitos (com sinal opcional): zeros à esquerda
    if re.fullmatch(r"-?\d+", s):
        neg = s.startswith("-")
        body = s[1:] if neg else s
        body = body.lstrip("0") or "0"
        return f"-{body}" if neg else body
    return s


def normalize_sku_key(series: pd.Series) -> pd.Series:
    """Série de chaves SKU para join e flags (mesma regra que :func:`normalize_sku_join_key_scalar`)."""
    return series.map(normalize_sku_join_key_scalar)


def normalize_pedido_join_key_scalar(raw: object) -> str:
    """
    Chave canónica para vínculo pedidos ↔ notas (número do pedido / multiloja).

    Trim, remove sufixo ``.0`` de float Excel, preserva letras e dígitos (ex. ``MLB123``).
    """
    if raw is None:
        return ""
    try:
        if isinstance(raw, float) and np.isnan(raw):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat", "<na>"):
        return ""
    if re.fullmatch(r"-?\d+\.0", s):
        s = s[:-2]
    return s.strip()


def normalize_pedido_join_key(series: pd.Series) -> pd.Series:
    return series.map(normalize_pedido_join_key_scalar)


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
