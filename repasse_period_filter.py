"""
Recorte por período no painel de repasse: pagamento ou, se vazio, data de emissão da NF.
Módulo isolado para testes sem carregar Streamlit.
"""
from __future__ import annotations

from datetime import date

import pandas as pd


def _first_series(df: pd.DataFrame, col: str) -> pd.Series:
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def _pick_col_by_tokens(columns: list[str], tokens: list[str]) -> str:
    import unicodedata

    for c in columns:
        n = unicodedata.normalize("NFKD", str(c)).encode("ascii", "ignore").decode().lower()
        if all(t in n for t in tokens):
            return c
    return ""


def _resolve_col_data_emissao(columns: list[str]) -> str:
    if "Data de emissão" in columns:
        return "Data de emissão"
    return _pick_col_by_tokens(columns, ["data", "emiss"])


def _parse_data_emissao_final(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    s = s.mask(s.str.lower().isin({"none", "nan", "nat", "<na>", "null"}), "")
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def _parse_data_pagamento_final(series: pd.Series) -> pd.Series:
    from zoneinfo import ZoneInfo

    _BR_TZ = ZoneInfo("America/Sao_Paulo")
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    s = s.mask(s.str.lower().isin({"none", "nan", "nat", "<na>", "null"}), "")
    t = pd.to_datetime(s, errors="coerce", format="mixed", utc=True)
    try:
        t = t.dt.tz_convert(_BR_TZ).dt.tz_localize(None)
    except Exception:  # noqa: BLE001
        t = pd.to_datetime(s, errors="coerce", format="mixed")
    return t


def repasse_mascara_periodo_pagamento_ou_emissao(
    tabela: pd.DataFrame,
    data_pag_ini: date,
    data_pag_fim: date,
) -> pd.Series:
    """
    Recorte por período: data de pagamento no intervalo, ou — se pagamento vazio —
    data de emissão da NF no intervalo (ex.: Shopee pendente de liberação).
    """
    _dp = _parse_data_pagamento_final(_first_series(tabela, "Data de pagamento"))
    _dd_p = _dp.dt.normalize()
    col_de = _resolve_col_data_emissao(list(tabela.columns))
    if col_de and col_de in tabela.columns:
        _de = _parse_data_emissao_final(_first_series(tabela, col_de))
    else:
        _de = pd.Series(pd.NaT, index=tabela.index)
    _dd_e = _de.dt.normalize()
    _ini_ts = pd.Timestamp(data_pag_ini)
    _fim_ts = pd.Timestamp(data_pag_fim) + pd.Timedelta(days=1)
    m_pay = _dp.notna() & (_dd_p >= _ini_ts) & (_dd_p < _fim_ts)
    m_emit = _dp.isna() & _de.notna() & (_dd_e >= _ini_ts) & (_dd_e < _fim_ts)
    has_pay = _dp.notna().any()
    has_emit = _de.notna().any()
    if not has_pay and not has_emit:
        return pd.Series(True, index=tabela.index)
    return m_pay | m_emit
