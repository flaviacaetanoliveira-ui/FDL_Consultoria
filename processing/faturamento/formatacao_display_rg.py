"""Formatação pt-BR para exibição em camadas de processamento RG (sem Streamlit)."""

from __future__ import annotations

import math

import pandas as pd


def fmt_brl_ptbr_celula(x: object) -> str:
    """Moeda pt-BR (R$ 1.234,56) — espelha a semântica de ``app_operacional._fmt_brl_ptbr_celula``."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except TypeError:
        pass
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x).strip()
    if math.isnan(v):
        return ""
    neg = v < 0
    v = abs(v)
    cents = int(round(v * 100 + 1e-9))
    inteiro, cent = divmod(cents, 100)
    int_str = f"{inteiro:,}".replace(",", ".")
    corpo = f"{int_str},{cent:02d}"
    if neg:
        return f"R$ -{corpo}"
    return f"R$ {corpo}"


def fmt_pct_um_decimal(v: float) -> str:
    """Percentual com uma casa decimal e vírgula decimal (ex.: ``12,3%``)."""
    return f"{float(v):.1f}".replace(".", ",") + "%"
