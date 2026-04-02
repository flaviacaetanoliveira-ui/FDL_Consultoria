"""
Recorte mínimo (Etapa 1) — Faturamento & DRE: empresa, plataforma, período de venda.

O KPI fiscal **Vl. Nota Fiscal** (painel mínimo) usa período de **emissão** e agregação por NF;
o ``apply_recorte_minimo`` continua só no eixo comercial (venda).
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
    _fdl_fr_faturamento_series_bool_mask,
    _fdl_fr_mask_nf_emissao_no_periodo,
    _fdl_fr_mask_venda_no_periodo,
    _fdl_fr_safe_streamlit_date,
    _fdl_fr_series_datetime_bounds_dates,
    _fdl_fr_ts_nf_emissao_para_dia_civil,
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
    nf_emissao_ini: object | None = None
    nf_emissao_fim: object | None = None


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
        nf_emissao_ini=ss.get("fdl_fat_min_nf_d_ini"),
        nf_emissao_fim=ss.get("fdl_fat_min_nf_d_fim"),
    )


def faturamento_min_series_nf_emissao_bounds_dates(df_raw: pd.DataFrame) -> tuple[date, date, bool]:
    """Retorna (mín, máx, ok) dos dias civis de ``Nota_Data_Emissao`` (ISO / ``dayfirst=False``)."""
    if df_raw.empty or "Nota_Data_Emissao" not in df_raw.columns:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    ts = _fdl_fr_ts_nf_emissao_para_dia_civil(df_raw["Nota_Data_Emissao"])
    t = ts[ts.notna()]
    if t.empty:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    return t.min().date(), t.max().date(), True


def _nf_fiscal_situacao_invalida(series: pd.Series) -> pd.Series:
    ss = series.fillna("").astype(str).str.strip().str.lower()
    return (
        ss.str.contains("cancel", na=False)
        | ss.str.contains("deneg", na=False)
        | ss.str.contains("inutil", na=False)
    )


@dataclass(frozen=True)
class FatMinComercialConferenciaStats:
    """Recorte comercial (``df_recorte``): venda = Qtd × Preço de lista."""

    valor_venda: float
    linhas_pedido: int
    pedidos_multiloja_distintos: int


@dataclass(frozen=True)
class FatMinFiscalConferenciaStats:
    """Eixo fiscal: emissão + NF válida, uma vez por NF (``Nota_Valor_Liquido_Total``)."""

    n_nf_distintas: int
    valor_nota_fiscal: float


def compute_comercial_conferencia_stats(df_recorte: pd.DataFrame) -> FatMinComercialConferenciaStats:
    if df_recorte.empty:
        return FatMinComercialConferenciaStats(0.0, 0, 0)
    qcol, pl_col = "Quantidade", "Preço de lista"
    if qcol not in df_recorte.columns or pl_col not in df_recorte.columns:
        return FatMinComercialConferenciaStats(0.0, int(len(df_recorte)), 0)
    qtd = pd.to_numeric(df_recorte[qcol], errors="coerce").fillna(0.0)
    pl = pd.to_numeric(df_recorte[pl_col], errors="coerce").fillna(0.0)
    valor_venda = float((qtd * pl).sum())
    n_lin = int(len(df_recorte))
    ml_col = "Número do pedido multiloja"
    if ml_col not in df_recorte.columns:
        return FatMinComercialConferenciaStats(valor_venda, n_lin, 0)
    ml = df_recorte[ml_col].fillna("").astype(str).str.strip()
    n_ml = int(ml[ml.ne("")].nunique())
    return FatMinComercialConferenciaStats(valor_venda, n_lin, n_ml)


def compute_fiscal_nf_conferencia_stats(
    df_raw: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
) -> FatMinFiscalConferenciaStats:
    """
    NFs distintas e soma de ``Nota_Valor_Liquido_Total`` (uma vez por NF) com ``Nota_Data_Emissao`` no intervalo,
    após filtro **Empresa**; sem plataforma / sem ``Data`` venda. Exclui cancelada / denegada / inutilizada.
    """
    if df_raw.empty or nf_d_fim < nf_d_ini:
        return FatMinFiscalConferenciaStats(0, 0.0)
    need = {"Nota_Data_Emissao", "Nota_Valor_Liquido_Total", "Nota_Numero_Normalizado"}
    if not need.issubset(df_raw.columns):
        return FatMinFiscalConferenciaStats(0, 0.0)

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(sliced)
    if emp_opts and empresas_sel:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, list(empresas_sel))
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    nn = sliced["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    mask_nf = nn.ne("")
    if "faturamento_nota_vinculada" in sliced.columns:
        mask_nf = mask_nf | _fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])
    sliced = sliced.loc[mask_nf].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    if "Nota_Situacao" in sliced.columns:
        sliced = sliced.loc[~_nf_fiscal_situacao_invalida(sliced["Nota_Situacao"])].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    m_period = _fdl_fr_mask_nf_emissao_no_periodo(sliced["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    sliced = sliced.loc[m_period].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    gb_keys: list[str] = []
    if "org_id" in sliced.columns:
        gb_keys.append("org_id")
    gb_keys.append("Nota_Numero_Normalizado")

    total = 0.0
    n_gr = 0
    for _, gr in sliced.groupby(gb_keys, sort=False):
        n_gr += 1
        vals = pd.to_numeric(gr["Nota_Valor_Liquido_Total"], errors="coerce").dropna()
        total += float(vals.iloc[0]) if not vals.empty else 0.0
    return FatMinFiscalConferenciaStats(n_gr, total)


def compute_vl_nota_fiscal_fiscal_kpi(
    df_raw: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
) -> float:
    """
    Soma do valor líquido **por nota** (``Nota_Valor_Liquido_Total`` uma vez por NF),
    com ``Nota_Data_Emissao`` no intervalo, após filtro **Empresa** (sem plataforma / sem ``Data`` venda).
    Exclui situações cancelada / denegada / inutilizada (mesmo critério textual do pipeline de notas).
    """
    return compute_fiscal_nf_conferencia_stats(
        df_raw, empresas_sel=empresas_sel, nf_d_ini=nf_d_ini, nf_d_fim=nf_d_fim
    ).valor_nota_fiscal


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
