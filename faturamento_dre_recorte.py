"""
Recorte do módulo Faturamento & DRE (camada de app).

Fase 1: estado explícito + ``apply_recorte_modulo`` espelham a lógica que vivia
só em ``_render_faturamento_dre_recorte_global`` (sem alterar regras de cálculo).

As funções ``_fdl_fr_*`` são cópias alinhadas a ``app_operacional`` (sem importar
o módulo do app, para evitar efeitos colaterais Streamlit em testes e import circular).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Mapping
from zoneinfo import ZoneInfo

import pandas as pd

# -----------------------------------------------------------------------------
# Espelho de helpers de ``app_operacional`` (manter sincronizado manualmente).
# -----------------------------------------------------------------------------

_BR_TZ = ZoneInfo("America/Sao_Paulo")


def _fdl_fr_safe_streamlit_date(value: object, fallback: date) -> date:
    if value is None:
        return fallback
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return fallback


def _fdl_fr_series_datetime_bounds_dates(
    series: pd.Series, *, dayfirst: bool = True
) -> tuple[date, date, bool]:
    ts = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    t = ts[ts.notna()]
    if t.empty:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    return t.min().date(), t.max().date(), True


def _fdl_fr_faturamento_series_bool_mask(series: pd.Series) -> pd.Series:
    s = series
    if isinstance(s.dtype, pd.BooleanDtype):
        return s.fillna(False).astype(bool)
    if s.dtype == bool or pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).ne(0)
    x = s.astype(str).str.strip().str.casefold()
    return x.eq("true") | x.eq("1") | x.eq("yes") | x.eq("sim")


def _fdl_fr_ts_nf_emissao_para_dia_civil(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if ts.empty:
        return ts
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    return ts.dt.normalize()


def _fdl_fr_mask_nf_emissao_no_periodo(s: pd.Series, d_ini: date, d_fim: date) -> pd.Series:
    ts = _fdl_fr_ts_nf_emissao_para_dia_civil(s)
    if ts.empty:
        return pd.Series(False, index=ts.index)
    dcal = ts.dt.date
    ok = pd.notna(ts)
    ge = pd.Series(dcal, index=ts.index) >= d_ini
    le = pd.Series(dcal, index=ts.index) <= d_fim
    return ok & ge & le


def _fdl_fr_ts_pedido_para_dia_civil(s: pd.Series) -> pd.Series:
    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if ts.empty:
        return ts
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    return ts.dt.normalize()


def _fdl_fr_mask_venda_no_periodo(s: pd.Series, d_ini: date, d_fim: date) -> pd.Series:
    ts = _fdl_fr_ts_pedido_para_dia_civil(s)
    if ts.empty:
        return pd.Series(False, index=ts.index)
    dcal = ts.dt.date
    ok = pd.notna(ts)
    ge = pd.Series(dcal, index=ts.index) >= d_ini
    le = pd.Series(dcal, index=ts.index) <= d_fim
    return ok & ge & le


def _fdl_fr_etiquetas_empresa_recorte(df: pd.DataFrame) -> list[str]:
    if df.empty:
        return []
    if "empresa" in df.columns:
        u = sorted({str(x).strip() for x in df["empresa"].dropna().unique() if str(x).strip()})
        if u:
            return u
    if "org_id" in df.columns:
        return sorted({str(x).strip() for x in df["org_id"].dropna().unique() if str(x).strip()})
    return []


def _fdl_fr_filtrar_por_etiquetas_empresa(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    if df.empty or not labels:
        return df
    if "empresa" in df.columns:
        em = df["empresa"].fillna("").astype(str).str.strip()
        return df.loc[em.isin(labels)].copy()
    if "org_id" in df.columns:
        oid = df["org_id"].astype(str).str.strip()
        return df.loc[oid.isin(labels)].copy()
    return df


# -----------------------------------------------------------------------------
# Estado e aplicação do recorte
# -----------------------------------------------------------------------------


@dataclass(frozen=True)
class FaturamentoRecorteState:
    """Valores de filtro do recorte global (espelho das chaves ``fdl_fat_dre_*`` na sessão)."""

    empresas: tuple[str, ...]
    situacoes_pedido: tuple[str, ...]
    plataformas: tuple[str, ...]
    data_venda_ini: object | None
    data_venda_fim: object | None
    presenca_nf: str
    nf_emissao_filtrar: bool
    nf_emissao_ini: object | None
    nf_emissao_fim: object | None
    situacoes_nf: tuple[str, ...]


@dataclass(frozen=True)
class FaturamentoRecorteApplyResult:
    """Resultado de ``apply_recorte_modulo`` + avisos para a UI (``st.warning`` / admin)."""

    df: pd.DataFrame
    warnings: tuple[str, ...] = ()
    admin_messages: tuple[str, ...] = ()


def faturamento_recorte_state_from_session(ss: Mapping[str, Any]) -> FaturamentoRecorteState:
    """Constrói estado a partir de ``st.session_state`` (ou qualquer mapping compatível)."""

    def _tuple_strs(key: str) -> tuple[str, ...]:
        raw = ss.get(key)
        if not isinstance(raw, list):
            return ()
        return tuple(str(x) for x in raw if str(x).strip())

    pres = str(ss.get("fdl_fat_dre_presenca_nf") or "Todos").strip()
    return FaturamentoRecorteState(
        empresas=_tuple_strs("fdl_fat_dre_emp"),
        situacoes_pedido=_tuple_strs("fdl_fat_dre_sit"),
        plataformas=_tuple_strs("fdl_fat_dre_plat"),
        data_venda_ini=ss.get("fdl_fat_dre_d_ini"),
        data_venda_fim=ss.get("fdl_fat_dre_d_fim"),
        presenca_nf=pres,
        nf_emissao_filtrar=bool(ss.get("fdl_fat_dre_nf_emi_use")),
        nf_emissao_ini=ss.get("fdl_fat_dre_nf_emi_ini"),
        nf_emissao_fim=ss.get("fdl_fat_dre_nf_emi_fim"),
        situacoes_nf=_tuple_strs("fdl_fat_dre_nf_sit"),
    )


def apply_recorte_modulo(
    df_raw: pd.DataFrame,
    state: FaturamentoRecorteState,
) -> FaturamentoRecorteApplyResult:
    """
    Aplica o recorte global (comercial + fiscal) sobre ``df_raw``.

    Replica a sequência e as condições que, antes da Fase 1, estavam inline em
    ``app_operacional._render_faturamento_dre_recorte_global`` (após os widgets).

    Com ``use_modulo_recorte=True`` no app, o painel **não** reaplica período por
    **Data** — o recorte temporal fica só nesta função.
    """
    warn: list[str] = []
    admin_msg: list[str] = []

    if df_raw.empty:
        return FaturamentoRecorteApplyResult(df=df_raw.copy())

    out = df_raw
    has_data = "Data" in out.columns
    if has_data:
        d_min, d_max, ok_dates = _fdl_fr_series_datetime_bounds_dates(out["Data"])
    else:
        d_min = d_max = datetime.now(_BR_TZ).date()
        ok_dates = False

    has_nf_emi = "Nota_Data_Emissao" in out.columns
    if has_nf_emi:
        nf_d_min, nf_d_max, nf_ok_dates = _fdl_fr_series_datetime_bounds_dates(
            out["Nota_Data_Emissao"], dayfirst=False
        )
    else:
        nf_d_min = nf_d_max = datetime.now(_BR_TZ).date()
        nf_ok_dates = False

    emp_opts = _fdl_fr_etiquetas_empresa_recorte(out)

    sel_emp_g = list(state.empresas)
    sel_sit_g = list(state.situacoes_pedido)
    sel_plat_g = list(state.plataformas)

    sliced = out.copy()
    if emp_opts and sel_emp_g:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, sel_emp_g)
    if sel_sit_g:
        sliced = sliced[sliced["Situação"].isin(sel_sit_g)].copy()
    if ok_dates:
        d_ini_g = _fdl_fr_safe_streamlit_date(state.data_venda_ini, d_min)
        d_fim_g = _fdl_fr_safe_streamlit_date(state.data_venda_fim, d_max)
        if d_fim_g < d_ini_g:
            warn.append("A data final da **venda** não pode ser anterior à inicial.")
            d_fim_g = d_ini_g
        m_d = _fdl_fr_mask_venda_no_periodo(sliced["Data"], d_ini_g, d_fim_g)
        sliced = sliced.loc[m_d].copy()
    if sel_plat_g and "Nome da plataforma" in sliced.columns:
        sliced = sliced[sliced["Nome da plataforma"].isin(sel_plat_g)].copy()

    _pres = str(state.presenca_nf or "Todos").strip()
    if _pres == "Com NF vinculada" and "faturamento_nota_vinculada" in sliced.columns:
        sliced = sliced.loc[_fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])].copy()
    elif _pres == "Sem NF vinculada" and "faturamento_nota_vinculada" in sliced.columns:
        sliced = sliced.loc[~_fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])].copy()

    _emi_active = state.nf_emissao_filtrar
    if _emi_active:
        if "faturamento_nota_vinculada" in sliced.columns:
            sliced = sliced.loc[_fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])].copy()
        elif "Nota_Numero_Normalizado" in sliced.columns:
            nn = sliced["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
            sliced = sliced.loc[nn.ne("")].copy()
        else:
            sliced = sliced.iloc[0:0].copy()
        if not has_nf_emi or not nf_ok_dates:
            sliced = sliced.iloc[0:0].copy()
        elif not sliced.empty:
            d_ni = _fdl_fr_safe_streamlit_date(state.nf_emissao_ini, nf_d_min)
            d_nf = _fdl_fr_safe_streamlit_date(state.nf_emissao_fim, nf_d_max)
            if d_nf < d_ni:
                warn.append("A data final da **emissão da NF** não pode ser anterior à inicial.")
                d_nf = d_ni
            m_nf = _fdl_fr_mask_nf_emissao_no_periodo(sliced["Nota_Data_Emissao"], d_ni, d_nf)
            sliced = sliced.loc[m_nf].copy()

    sel_nf_sit = list(state.situacoes_nf)
    if sel_nf_sit:
        col_nf_s = "Nota_Situacao"
        if col_nf_s in sliced.columns:
            ss = sliced[col_nf_s].fillna("").astype(str).str.strip()
            sliced = sliced.loc[ss.isin(sel_nf_sit)].copy()
        else:
            admin_msg.append(
                "Filtro **Situação da NF** não aplicado: o materializado não traz a coluna **Nota_Situacao**. "
                "Reprocesse o faturamento com notas de saída para atualizar."
            )

    return FaturamentoRecorteApplyResult(
        df=sliced,
        warnings=tuple(warn),
        admin_messages=tuple(admin_msg),
    )
