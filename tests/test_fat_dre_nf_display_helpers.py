"""Helpers de exibição NF-first / fiscal (sem subir Streamlit)."""
from __future__ import annotations

import pandas as pd


def _df_get_series_column(df: pd.DataFrame, col: str) -> pd.Series:
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0].copy()
    return obj.copy()


def _series_nf_emissao_pt_br(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce", dayfirst=False)
    fmt = ts.dt.strftime("%d/%m/%Y")
    return fmt.where(ts.notna(), "—")


def test_emissao_pt_br_accepts_iso_strings() -> None:
    s = pd.Series(["2026-03-15", None, "invalid"])
    out = _series_nf_emissao_pt_br(s)
    assert out.iloc[0] == "15/03/2026"
    assert out.iloc[1] == "—"
    assert out.iloc[2] == "—"


def test_get_series_column_first_when_duplicated() -> None:
    df = pd.DataFrame([[1, 2]], columns=["a", "a"])
    assert isinstance(df["a"], pd.DataFrame)
    got = _df_get_series_column(df, "a")
    assert list(got) == [1]
