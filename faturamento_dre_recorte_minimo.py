"""
Recorte mínimo (Etapa 1) — Faturamento & DRE: empresa, plataforma, período de venda.

Sem filtro fiscal, sem situação de pedido (todo o materializado entra salvo os três eixos).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd

from faturamento_dre_recorte import (
    _BR_TZ,
    _fdl_fr_etiquetas_empresa_recorte,
    _fdl_fr_filtrar_por_etiquetas_empresa,
    _fdl_fr_mask_venda_no_periodo,
    _fdl_fr_safe_streamlit_date,
    _fdl_fr_series_datetime_bounds_dates,
)


def _min_cal_limits(d_min: date, d_max: date) -> tuple[date, date]:
    today = datetime.now(_BR_TZ).date()
    cal_max = max(d_max, today)
    cal_min = min(d_min, today - timedelta(days=3 * 365))
    return cal_min, cal_max


@dataclass(frozen=True)
class FaturamentoRecorteMinState:
    empresas: tuple[str, ...]
    plataformas: tuple[str, ...]
    data_venda_ini: object | None
    data_venda_fim: object | None


def faturamento_recorte_min_state_from_session(ss: Mapping[str, Any]) -> FaturamentoRecorteMinState:
    def _tup(key: str) -> tuple[str, ...]:
        raw = ss.get(key)
        if not isinstance(raw, list):
            return ()
        return tuple(str(x) for x in raw if str(x).strip())

    return FaturamentoRecorteMinState(
        empresas=_tup("fdl_fat_min_emp"),
        plataformas=_tup("fdl_fat_min_plat"),
        data_venda_ini=ss.get("fdl_fat_min_d_ini"),
        data_venda_fim=ss.get("fdl_fat_min_d_fim"),
    )


def apply_recorte_minimo(
    df_raw: pd.DataFrame,
    state: FaturamentoRecorteMinState,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Ordem: empresa → plataforma → período (**Data** venda).

    Limites de data para ``_safe_streamlit_date`` vêm do ``df_raw`` completo (antes dos filtros),
    para o calendário não encolher só porque se filtrou empresa.
    """
    warn: list[str] = []
    if df_raw.empty:
        return df_raw.copy(), ()

    has_data = "Data" in df_raw.columns
    if has_data:
        d_min, d_max, ok_dates = _fdl_fr_series_datetime_bounds_dates(df_raw["Data"])
    else:
        d_min = d_max = datetime.now(_BR_TZ).date()
        ok_dates = False

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(df_raw)
    sel_emp = list(state.empresas)
    if emp_opts and sel_emp:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, sel_emp)

    sel_plat = list(state.plataformas)
    if sel_plat and "Nome da plataforma" in sliced.columns:
        sliced = sliced[sliced["Nome da plataforma"].isin(sel_plat)].copy()

    if ok_dates and not sliced.empty:
        d_ini = _fdl_fr_safe_streamlit_date(state.data_venda_ini, d_min)
        d_fim = _fdl_fr_safe_streamlit_date(state.data_venda_fim, d_max)
        if d_fim < d_ini:
            warn.append("A data final da **venda** não pode ser anterior à inicial.")
            d_fim = d_ini
        m_d = _fdl_fr_mask_venda_no_periodo(sliced["Data"], d_ini, d_fim)
        sliced = sliced.loc[m_d].copy()

    return sliced, tuple(warn)
