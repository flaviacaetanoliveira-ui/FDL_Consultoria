"""
Recorte mínimo (Etapa 1) — Faturamento & DRE: painel **NF-first**.

Universo: NFs válidas no **período de emissão**; comercial/custos nas **linhas de pedido** ligadas
(``build_nf_grain_dataframe``). ``apply_recorte_minimo`` (grão pedido) aceita janela de **Data** venda só por argumento opcional, não pelo state da sessão.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd

from processing.faturamento.calc import _frete_mercado_envios_vs_transportadora
from processing.faturamento.config import STATUS_CUSTO_OK

# Mesmo contrato que ``nf_panel_materializado`` (evita import circular com esse módulo).
_NF_PANEL_ALIGN_REQUIRED: frozenset[str] = frozenset(
    {
        "org_id",
        "Nota_Numero_Normalizado",
        "Nota_Data_Emissao",
        "Nota_Situacao",
        "empresa",
        "valor_faturado_nf",
        "valor_venda",
        "diferenca",
        "comissao",
        "custo_produto",
        "receita_frete_tp",
        "tarifa_custo_envio",
        "imposto",
        "despesa_fixa",
        "custo_ads_variavel",
        "custo_ads_fixo",
        "custo_ads",
        "resultado",
        "plataforma_resumo",
        "plataforma",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
        "faturamento_nota_vinculada",
        "comercial_incompleto",
    }
)


def _nf_panel_dataframe_valid_for_align(df: pd.DataFrame) -> bool:
    return not df.empty and _NF_PANEL_ALIGN_REQUIRED.issubset(df.columns)

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

# Painel NF-first: despesa fixa explícita = esta alíquota × valor da venda agregado à NF
# (Σ Quantidade × Preço de lista nas linhas ligadas à nota).
NF_FIRST_PANEL_DESPESA_FIXA_ALIQUOTA = 0.05

# Custo de mídia (ADS) por NF no painel materializado: % sobre valor_venda + valor fixo por venda com lista > 0.
NF_FIRST_PANEL_ADS_ALIQUOTA = 0.035
NF_FIRST_PANEL_ADS_FIXO_POR_VENDA = 2.0


def nf_grain_plataforma_match_key(raw: object) -> str:
    """
    Chave estável para filtrar «Nome da plataforma» / ``plataforma`` (grão NF).

    Alinha rótulos do export («MADEIRA MADEIRA», «MadeiraMadeira», «madeira madeira») para o mesmo
    critério, evitando que o multiselect deixe de bater com o materializado.
    """
    xs = str(raw).strip() if raw is not None else ""
    if not xs or xs.casefold() in {"nan", "none", "nat", "<na>"}:
        return ""
    return xs.casefold().replace(" ", "")


# ERP / canal interno no export — não é marketplace; não deve aparecer no filtro «Plataforma».
_NF_GRAIN_PLATAFORMA_UI_EXCLUDE_KEYS: frozenset[str] = frozenset({"bling"})


def nf_grain_plataforma_ui_options(series: pd.Series) -> list[str]:
    """Rótulos únicos para multiselect (exclui placeholders tipo «Bling»)."""
    raw = {str(x).strip() for x in series.dropna().unique() if str(x).strip()}
    out = [x for x in raw if nf_grain_plataforma_match_key(x) not in _NF_GRAIN_PLATAFORMA_UI_EXCLUDE_KEYS]
    return sorted(out)


def nf_grain_plataforma_label_for_ui(raw: object) -> str:
    """Texto na tabela: «Bling» no pedido = venda direta/ERP, não canal ML/Shopee."""
    xs = str(raw).strip() if raw is not None else ""
    if not xs or xs.casefold() in {"nan", "none", "nat", "<na>", "—"}:
        return "—"
    if nf_grain_plataforma_match_key(xs) == "bling":
        return "Loja direta"
    return xs


def _min_cal_limits(d_min: date, d_max: date) -> tuple[date, date]:
    today = datetime.now(_BR_TZ).date()
    cal_max = max(d_max, today)
    cal_min = min(d_min, today - timedelta(days=3 * 365))
    return cal_min, cal_max


@dataclass(frozen=True)
class FaturamentoRecorteMinState:
    """Estado do painel NF-first e filtros comuns (empresa, plataforma, situação NF). Sem eixo **Data** venda."""

    empresas: tuple[str, ...]
    plataformas: tuple[str, ...]
    situacoes_nf: tuple[str, ...] = ()


def faturamento_recorte_min_state_from_session(ss: Mapping[str, Any]) -> FaturamentoRecorteMinState:
    def _tup(key: str) -> tuple[str, ...]:
        raw = ss.get(key)
        if not isinstance(raw, list):
            return ()
        return tuple(str(x) for x in raw if str(x).strip())

    return FaturamentoRecorteMinState(
        empresas=_tup("fdl_fat_min_emp"),
        plataformas=_tup("fdl_fat_min_plat"),
        situacoes_nf=_tup("fdl_fat_min_nf_sit"),
    )


def faturamento_nf_situacao_select_options(df: pd.DataFrame) -> list[str]:
    """
    Valores distintos de ``Nota_Situacao`` para multiselect (vazio = sem filtro extra).
    Exclui rótulos vazios e situações já tratadas como inválidas pelo painel (cancel/deneg/inutil).
    """
    if df.empty or "Nota_Situacao" not in df.columns:
        return []
    s = df["Nota_Situacao"].fillna("").astype(str).str.strip()
    s = s[s.ne("") & ~_nf_fiscal_situacao_invalida(s)]
    uniq = sorted({x for x in s.tolist() if x}, key=lambda t: t.casefold())
    return uniq


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


@dataclass(frozen=True)
class FaturamentoFiscalBaseStats:
    """Topo do painel: totais sobre o conjunto base fiscal (emitidas + válidas no recorte)."""

    n_nf: int
    valor_liquido_fiscal_sum: float


def build_faturamento_fiscal_base_slice(
    df_fiscal: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
    ok_nf_dates: bool,
    situacoes_sel: tuple[str, ...] | None = None,
) -> tuple[pd.DataFrame, FaturamentoFiscalBaseStats]:
    """
    Recorte **base fiscal** alinhado ao Bling: **empresa** + **emissão** no intervalo + NF com situação válida
    (exclui cancelada / denegada / inutilizada). Uma linha por NF após agregação por chave canónica.

    Usa ``dataset_faturamento_fiscal.parquet`` (contrato fiscal). **Não** aplica plataforma, produto nem resultado.

    ``situacoes_sel``: quando não vazio, restringe às situações cujo texto (case-insensitive) está na lista.
    """
    empty_stats = FaturamentoFiscalBaseStats(0, 0.0)
    if df_fiscal.empty or not ok_nf_dates or nf_d_fim < nf_d_ini:
        return pd.DataFrame(), empty_stats

    need = {"empresa", "Nota_Data_Emissao", "Nota_Numero_Normalizado", "Valor_Liquido_NF"}
    if not need.issubset(df_fiscal.columns):
        return pd.DataFrame(), empty_stats

    out = df_fiscal.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(out)
    if emp_opts and empresas_sel:
        out = _fdl_fr_filtrar_por_etiquetas_empresa(out, list(empresas_sel))
    if out.empty:
        return pd.DataFrame(), empty_stats

    if "Nota_Situacao" in out.columns:
        out = out.loc[~_nf_fiscal_situacao_invalida(out["Nota_Situacao"])].copy()
    if out.empty:
        return pd.DataFrame(), empty_stats

    m_period = _fdl_fr_mask_nf_emissao_no_periodo(out["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    out = out.loc[m_period].copy()
    if out.empty:
        return pd.DataFrame(), empty_stats

    _sit = situacoes_sel or ()
    if _sit and any(str(x).strip() for x in _sit) and "Nota_Situacao" in out.columns:
        want = {str(x).strip().casefold() for x in _sit if str(x).strip()}
        ss = out["Nota_Situacao"].fillna("").astype(str).str.strip()
        out = out.loc[ss.str.casefold().isin(want)].copy()
        if out.empty:
            return pd.DataFrame(), empty_stats

    gb_keys: list[str] = []
    if "org_id" in out.columns:
        gb_keys.append("org_id")
    gb_keys.append("empresa")
    gb_keys.append("Nota_Numero_Normalizado")

    agg_dict: dict[str, str] = {
        "Nota_Data_Emissao": "min",
        "Valor_Liquido_NF": "sum",
    }
    if "Nota_Situacao" in out.columns:
        agg_dict["Nota_Situacao"] = "first"
    for opt in ("Frete_Nota_Export", "Valor_Total_NF", "schema_version_fiscal"):
        if opt in out.columns:
            agg_dict[opt] = "first"

    grouped = out.groupby(gb_keys, sort=False).agg(agg_dict).reset_index()
    vl = pd.to_numeric(grouped["Valor_Liquido_NF"], errors="coerce").fillna(0.0)
    n_nf = int(len(grouped))
    total = float(vl.sum())
    return grouped, FaturamentoFiscalBaseStats(n_nf=n_nf, valor_liquido_fiscal_sum=total)


def _fiscal_base_merge_keys(df: pd.DataFrame) -> list[str]:
    keys: list[str] = []
    if "org_id" in df.columns:
        keys.append("org_id")
    keys.extend(["empresa", "Nota_Numero_Normalizado"])
    return keys


@dataclass(frozen=True)
class CommercialCoverageStats:
    """Cobertura comercial sobre o conjunto base fiscal (N_base notas)."""

    n_total: int
    n_com_vinculo_pedido_nf: int
    n_sem_vinculo_ou_so_fiscal: int
    n_com_venda_lista: int
    n_sem_resultado: int
    n_com_resultado_numerico: int


def compute_commercial_coverage_stats(df_aligned: pd.DataFrame, *, eps: float = 1e-9) -> CommercialCoverageStats:
    """
    Estatísticas para UI: quantas NFs do conjunto base têm vínculo comercial, lista, resultado, etc.
    """
    if df_aligned.empty:
        return CommercialCoverageStats(0, 0, 0, 0, 0, 0)
    n = int(len(df_aligned))
    vinc = (
        df_aligned["faturamento_nota_vinculada"].fillna(False).astype(bool)
        if "faturamento_nota_vinculada" in df_aligned.columns
        else pd.Series(False, index=df_aligned.index)
    )
    vv = (
        pd.to_numeric(df_aligned["valor_venda"], errors="coerce").fillna(0.0)
        if "valor_venda" in df_aligned.columns
        else pd.Series(0.0, index=df_aligned.index)
    )
    res = (
        pd.to_numeric(df_aligned["resultado"], errors="coerce")
        if "resultado" in df_aligned.columns
        else pd.Series(dtype=float, index=df_aligned.index)
    )
    n_vinc = int(vinc.sum())
    n_lista = int((vv > eps).sum())
    n_sem_res = int(res.isna().sum())
    n_com_res = int(res.notna().sum())
    n_so_fiscal = int(((~vinc) & (vv <= eps)).sum())
    return CommercialCoverageStats(
        n_total=n,
        n_com_vinculo_pedido_nf=n_vinc,
        n_sem_vinculo_ou_so_fiscal=n_so_fiscal,
        n_com_venda_lista=n_lista,
        n_sem_resultado=n_sem_res,
        n_com_resultado_numerico=n_com_res,
    )


def build_nf_panel_aligned_to_fiscal_base(
    df_fiscal_base: pd.DataFrame,
    df_panel_empresa_emissao: pd.DataFrame,
) -> pd.DataFrame:
    """
    Uma linha por NF do **conjunto base fiscal**, com colunas do painel NF preenchidas pelo merge comercial
    quando existir; caso contrário valores neutros (lista 0, sem vínculo, resultado NaN).

    ``valor_faturado_nf`` em todas as linhas vem de ``Valor_Liquido_NF`` do fiscal base (comparável ao Bling).
    """
    if df_fiscal_base.empty:
        return pd.DataFrame()

    keys = _fiscal_base_merge_keys(df_fiscal_base)
    for k in keys:
        if k not in df_fiscal_base.columns:
            return pd.DataFrame()

    base = df_fiscal_base.copy()
    base["valor_faturado_nf"] = pd.to_numeric(base["Valor_Liquido_NF"], errors="coerce").fillna(0.0)
    keep_base = keys + ["valor_faturado_nf", "Nota_Data_Emissao", "Nota_Situacao"]
    keep_base = [c for c in keep_base if c in base.columns]
    out = base[keep_base].copy()

    if df_panel_empresa_emissao.empty or not _nf_panel_dataframe_valid_for_align(df_panel_empresa_emissao):
        return _nf_panel_fill_defaults_for_aligned(out)

    ps = df_panel_empresa_emissao.drop_duplicates(subset=keys, keep="first")
    drop_ps = [c for c in ("Valor_Liquido_NF", "valor_faturado_nf", "Nota_Data_Emissao", "Nota_Situacao") if c in ps.columns]
    ps_m = ps.drop(columns=drop_ps, errors="ignore")
    merged = out.merge(ps_m, on=keys, how="left")
    merged["valor_faturado_nf"] = pd.to_numeric(
        df_fiscal_base["Valor_Liquido_NF"], errors="coerce"
    ).fillna(0.0).to_numpy()
    return _nf_panel_fill_defaults_for_aligned(merged)


def _nf_panel_fill_defaults_for_aligned(df: pd.DataFrame) -> pd.DataFrame:
    """Garante colunas esperadas por ``compute_nf_panel_kpis`` e textos do painel."""
    m = df.copy()
    if "valor_venda" not in m.columns:
        m["valor_venda"] = 0.0
    else:
        m["valor_venda"] = pd.to_numeric(m["valor_venda"], errors="coerce").fillna(0.0)
    m["valor_faturado_nf"] = pd.to_numeric(m["valor_faturado_nf"], errors="coerce").fillna(0.0)
    for c, default in (
        ("comissao", 0.0),
        ("custo_produto", 0.0),
        ("receita_frete_tp", 0.0),
        ("custo_frete_plataforma", 0.0),
        ("repasse_frete_transportadora_propria", 0.0),
        ("tarifa_custo_envio", 0.0),
        ("imposto", 0.0),
        ("despesa_fixa", 0.0),
        ("custo_ads_variavel", 0.0),
        ("custo_ads_fixo", 0.0),
        ("custo_ads", 0.0),
    ):
        if c not in m.columns:
            m[c] = default
        else:
            m[c] = pd.to_numeric(m[c], errors="coerce").fillna(0.0)
    if "resultado" not in m.columns:
        m["resultado"] = float("nan")
    else:
        m["resultado"] = pd.to_numeric(m["resultado"], errors="coerce")
    if "diferenca" not in m.columns:
        m["diferenca"] = m["valor_venda"] - m["valor_faturado_nf"]
    else:
        d = pd.to_numeric(m["diferenca"], errors="coerce")
        m["diferenca"] = d.fillna(m["valor_venda"] - m["valor_faturado_nf"])
    for c, default in (
        ("plataforma_resumo", "—"),
        ("plataforma", "—"),
        ("pedido_resumo", "—"),
        ("produto_resumo", "—"),
    ):
        if c not in m.columns:
            m[c] = default
        else:
            m[c] = m[c].fillna("").astype(str).replace("", "—")
    if "n_linhas_pedido" not in m.columns:
        m["n_linhas_pedido"] = 0
    else:
        m["n_linhas_pedido"] = pd.to_numeric(m["n_linhas_pedido"], errors="coerce").fillna(0).astype(int)
    if "faturamento_nota_vinculada" not in m.columns:
        m["faturamento_nota_vinculada"] = False
    else:
        m["faturamento_nota_vinculada"] = m["faturamento_nota_vinculada"].fillna(False).astype(bool)
    inc_calc = (~m["faturamento_nota_vinculada"]) | m["resultado"].isna()
    if "comercial_incompleto" not in m.columns:
        m["comercial_incompleto"] = inc_calc.astype(bool)
    else:
        m["comercial_incompleto"] = (
            m["comercial_incompleto"].fillna(False).astype(bool) | inc_calc.astype(bool)
        )
    return m


def _nf_grain_groupby_keys(df: pd.DataFrame) -> list[str]:
    keys: list[str] = []
    if "org_id" in df.columns:
        keys.append("org_id")
    keys.append("Nota_Numero_Normalizado")
    return keys


def _nf_grain_tarifa_custo_envio_sum(gr: pd.DataFrame) -> float:
    """
    Tarifa de envio: soma da coluna **Custo de Frete** do relatório de pedidos.
    Sem essa coluna, usa-se **Frete_Plataforma** (legado / export já derivado) como proxy da tarifa.
    """
    if "Custo de Frete" in gr.columns:
        return float(pd.to_numeric(gr["Custo de Frete"], errors="coerce").fillna(0.0).sum())
    if "Frete_Plataforma" in gr.columns:
        return float(pd.to_numeric(gr["Frete_Plataforma"], errors="coerce").fillna(0.0).sum())
    return 0.0


def _nf_grain_custo_frete_plataforma_sum(gr: pd.DataFrame) -> float:
    """
    Custo de frete da **plataforma** (Mercado Envios / logística ME), alinhado a ``Frete_Plataforma`` no ``calc``.

    Prioriza a coluna **Frete_Plataforma** quando soma > 0; senão usa a parcela ME do split sobre **Custo de Frete**.
    """
    if "Frete_Plataforma" in gr.columns:
        fp = float(pd.to_numeric(gr["Frete_Plataforma"], errors="coerce").fillna(0.0).sum())
        if fp > 1e-12:
            return fp
    if "Custo de Frete" in gr.columns:
        cf = pd.to_numeric(gr["Custo de Frete"], errors="coerce").fillna(0.0)
        frete_me, _ = _frete_mercado_envios_vs_transportadora(gr, cf)
        return float(frete_me.sum())
    return 0.0


def _nf_grain_repasse_frete_transportadora_propria_sum(gr: pd.DataFrame) -> float:
    """Repasse / custo da **transportadora própria**: parcela TP do ``Custo de Frete`` (modalidade ≠ ME)."""
    if "Custo de Frete" not in gr.columns:
        return 0.0
    cf = pd.to_numeric(gr["Custo de Frete"], errors="coerce").fillna(0.0)
    _, frete_tp = _frete_mercado_envios_vs_transportadora(gr, cf)
    return float(frete_tp.sum())


def _nf_grain_receita_frete_tp_sum(gr: pd.DataFrame) -> float:
    """
    Receita de frete: parcela **transportadora própria** do ``Custo de Frete`` (mesma regra que ``calc``).
    Sem ``Custo de Frete``, não há split — fica 0 (imputação por gap NF×lista trata só ``receita_frete_tp``).
    """
    if "Custo de Frete" not in gr.columns:
        return 0.0
    cf = pd.to_numeric(gr["Custo de Frete"], errors="coerce").fillna(0.0)
    _, frete_tp = _frete_mercado_envios_vs_transportadora(gr, cf)
    return float(frete_tp.sum())


def _nf_frete_nota_coincide_com_gap(
    valor_faturado_nf: float,
    valor_venda: float,
    receita_frete_tp: float,
    *,
    eps: float = 1e-9,
    rel_tol: float = 0.02,
) -> bool:
    """
    True quando a receita de frete (TP / gap) explica (aprox.) o excesso do líquido da NF sobre a venda (lista).
    """
    gap = valor_faturado_nf - valor_venda
    if valor_venda <= eps or gap <= eps or receita_frete_tp <= eps:
        return False
    tol = max(eps, rel_tol * max(abs(valor_venda), abs(valor_faturado_nf), abs(gap)))
    return abs(receita_frete_tp - gap) <= tol


def _nf_grain_custo_produto_col_name(columns: list[str]) -> str:
    """Mesma prioridade que o painel linha (``Custo_Produto_Total`` V2, senão legado)."""
    if "Custo_Produto_Total" in columns:
        return "Custo_Produto_Total"
    if "Custo do Produto" in columns:
        return "Custo do Produto"
    return ""


def _nf_grain_custo_total_for_group(gr: pd.DataFrame, custo_col: str, *, eps: float = 1e-12) -> float:
    """
    Soma custo no grão NF: coluna total (V2/legado) se existir e somar > eps; senão ``Quantidade × Custo_Unitario``
    (alinhado ao pipeline de faturamento). Se a coluna total existir mas só tiver zeros/NaN, recalcula pelo unitário.
    """
    qcol, ucol = "Quantidade", "Custo_Unitario"

    def _from_unitario() -> float:
        if qcol not in gr.columns or ucol not in gr.columns:
            return 0.0
        qtd = pd.to_numeric(gr[qcol], errors="coerce").fillna(0.0)
        uni = pd.to_numeric(gr[ucol], errors="coerce").fillna(0.0)
        return float((qtd * uni).sum())

    if custo_col and custo_col in gr.columns:
        tot = float(pd.to_numeric(gr[custo_col], errors="coerce").fillna(0.0).sum())
        if tot > eps:
            return tot
        alt = _from_unitario()
        return alt if alt > eps else tot
    return _from_unitario()


def _nf_grain_venda_linha_series(
    gr: pd.DataFrame,
    *,
    has_qpl: bool,
    qcol: str,
    pl_col: str,
) -> pd.Series:
    """Valor comercial por linha no grão NF: ``Quantidade × Preço de lista`` (sem hierarquia «Valor total»)."""
    if has_qpl:
        qtd = pd.to_numeric(gr[qcol], errors="coerce").fillna(0.0)
        pl = pd.to_numeric(gr[pl_col], errors="coerce").fillna(0.0)
        return (qtd * pl).astype(float).fillna(0.0)
    return pd.Series(0.0, index=gr.index, dtype=float)


def build_nf_grain_dataframe(
    df_raw: pd.DataFrame,
    state: FaturamentoRecorteMinState,
    *,
    ok_nf_dates: bool,
    nf_d_ini: date,
    nf_d_fim: date,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Um único universo para KPIs e tabela (**só eixo temporal = emissão da NF**):

    1. Filtro **empresa**.
    2. NFs **válidas** (situação) com **emissão** em ``[nf_d_ini, nf_d_fim]`` e vínculo NF.
    3. Todas as **linhas de pedido** com chave ``(org_id, NF)`` nesse conjunto (sem filtro por **Data** de venda).
    4. Filtro opcional: **plataforma**.

    **NF sem linha de pedido no materializado:** não aparece (o dataset é grão pedido; não há nota órfã).
    **Vários pedidos por NF:** agregados na mesma linha NF (somas comerciais; texto ``pedido_resumo``).
    **Várias NFs por pedido:** várias linhas em ``df_nf`` (uma por NF).

    **Venda (lista):** por linha, ``Quantidade × Preço de lista``; soma no grupo NF.

    **Comissão / custo produto / frete:** soma por linha de ``Taxa de Comissão``; custo = ``Custo_Produto_Total``
    (ou ``Custo do Produto``) ou, se ausente/só zeros, ``Quantidade × Custo_Unitario``;
    **tarifa_custo_envio** = soma ``Custo de Frete`` (ou ``Frete_Plataforma`` se CF ausente);
    **custo_frete_plataforma** = parcela ME / coluna ``Frete_Plataforma`` (custo logística plataforma);
    **repasse_frete_transportadora_propria** = parcela TP do ``Custo de Frete`` (repasse à transportadora);
    **receita_frete_tp** (grão comercial) = parcela TP do CF ou gap NF×lista; no painel com fiscal substitui-se pela NF.

    **Resultado:** Σ ``Resultado`` das linhas (já desconta ``Frete_Plataforma`` por linha, **não** o repasse TP).
    No **painel materializado**: ``resultado += receita_frete_tp − repasse_frete_transportadora_propria`` (receita fiscal
    na NF vs repasse pedido) e o custo **ADS** (3,5% × ``valor_venda`` + fixo por NF com venda > 0).

    **Despesa fixa:** ``NF_FIRST_PANEL_DESPESA_FIXA_ALIQUOTA`` × ``valor_venda`` (por NF).

    **Receita frete (gap NF×lista):** com **uma** linha, ``receita_frete_tp`` ~0 e ``valor_faturado_nf > valor_venda``,
    imputa-se ``receita_frete_tp = valor_faturado_nf − valor_venda`` (frete só na nota; não altera ``tarifa_custo_envio``).
    """
    warn: list[str] = []
    cols_out = [
        "org_id",
        "Nota_Numero_Normalizado",
        "Nota_Data_Emissao",
        "Nota_Situacao",
        "empresa",
        "valor_faturado_nf",
        "valor_venda",
        "diferenca",
        "comissao",
        "custo_produto",
        "receita_frete_tp",
        "custo_frete_plataforma",
        "repasse_frete_transportadora_propria",
        "tarifa_custo_envio",
        "imposto",
        "despesa_fixa",
        "resultado",
        "plataforma_resumo",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
        "faturamento_nota_vinculada",
        "comercial_incompleto",
    ]
    if df_raw.empty or not ok_nf_dates or nf_d_fim < nf_d_ini:
        return pd.DataFrame(columns=cols_out), tuple(warn)
    need = {"Nota_Data_Emissao", "Nota_Valor_Liquido_Total", "Nota_Numero_Normalizado"}
    if not need.issubset(df_raw.columns):
        warn.append("Materializado sem colunas fiscais mínimas para o painel NF-first.")
        return pd.DataFrame(columns=cols_out), tuple(warn)

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(sliced)
    sel_emp = list(state.empresas)
    if emp_opts and sel_emp:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, sel_emp)
    if sliced.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    nn = sliced["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    mask_nf = nn.ne("")
    if "faturamento_nota_vinculada" in sliced.columns:
        mask_nf = mask_nf | _fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])
    sel_emit = sliced.loc[mask_nf].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    if "Nota_Situacao" in sel_emit.columns:
        sel_emit = sel_emit.loc[~_nf_fiscal_situacao_invalida(sel_emit["Nota_Situacao"])].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    m_period = _fdl_fr_mask_nf_emissao_no_periodo(sel_emit["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    sel_emit = sel_emit.loc[m_period].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    gb_keys = _nf_grain_groupby_keys(sel_emit)
    if "org_id" in sel_emit.columns:
        sel_emit = sel_emit.copy()
        sel_emit["_org_join"] = sel_emit["org_id"].fillna("").astype(str).str.strip()
        unique_nf = sel_emit.drop_duplicates(subset=["_org_join", "Nota_Numero_Normalizado"])[
            ["_org_join", "Nota_Numero_Normalizado"]
        ]
        sliced = sliced.copy()
        sliced["_org_join"] = sliced["org_id"].fillna("").astype(str).str.strip()
        df_linked = sliced.merge(
            unique_nf,
            left_on=["_org_join", "Nota_Numero_Normalizado"],
            right_on=["_org_join", "Nota_Numero_Normalizado"],
            how="inner",
            suffixes=("", "_y"),
        )
        df_linked = df_linked.drop(columns=["_org_join"], errors="ignore")
    else:
        unique_nf = sel_emit[["Nota_Numero_Normalizado"]].drop_duplicates()
        df_linked = sliced.merge(unique_nf, on="Nota_Numero_Normalizado", how="inner")

    sel_plat = list(state.plataformas)
    if sel_plat and "Nome da plataforma" in df_linked.columns:
        want = {nf_grain_plataforma_match_key(x) for x in sel_plat}
        want.discard("")
        if want:
            got = df_linked["Nome da plataforma"].map(nf_grain_plataforma_match_key)
            df_linked = df_linked.loc[got.isin(want)].copy()

    if df_linked.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    gcols = _nf_grain_groupby_keys(df_linked)
    rows: list[dict[str, object]] = []
    has_desp_fix_col = "Despesas Fixas" in df_linked.columns

    qcol, pl_col = "Quantidade", "Preço de lista"
    has_qpl = qcol in df_linked.columns and pl_col in df_linked.columns
    custo_col = _nf_grain_custo_produto_col_name(list(df_linked.columns))
    prod_col = "Descrição" if "Descrição" in df_linked.columns else ("Nome" if "Nome" in df_linked.columns else "")
    ml_col = "Número do pedido multiloja"
    ped_col = "Número do pedido"

    for key, gr in df_linked.groupby(gcols, sort=False):
        if len(gcols) == 2:
            oid_s, nfk = str(key[0]).strip(), str(key[1]).strip()
        else:
            oid_s, nfk = "", str(key).strip()
        gr = gr.copy()
        nf_vals = pd.to_numeric(gr["Nota_Valor_Liquido_Total"], errors="coerce").dropna()
        vl_nf = float(nf_vals.iloc[0]) if not nf_vals.empty else 0.0

        v_lin = _nf_grain_venda_linha_series(gr, has_qpl=has_qpl, qcol=qcol, pl_col=pl_col)
        v_venda = float(v_lin.sum())

        com = (
            float(pd.to_numeric(gr["Taxa de Comissão"], errors="coerce").fillna(0.0).sum())
            if "Taxa de Comissão" in gr.columns
            else 0.0
        )
        custo_p = _nf_grain_custo_total_for_group(gr, custo_col)
        tarifa_env = _nf_grain_tarifa_custo_envio_sum(gr)
        c_frete_plat = _nf_grain_custo_frete_plataforma_sum(gr)
        rep_tp_ped = _nf_grain_repasse_frete_transportadora_propria_sum(gr)
        rec_tp = _nf_grain_receita_frete_tp_sum(gr)
        imp = float(pd.to_numeric(gr["Imposto"], errors="coerce").fillna(0.0).sum()) if "Imposto" in gr.columns else 0.0
        desp_fix = float(NF_FIRST_PANEL_DESPESA_FIXA_ALIQUOTA * v_venda)

        res_num = (
            pd.to_numeric(gr["Resultado"], errors="coerce") if "Resultado" in gr.columns else pd.Series(dtype=float)
        )
        if "Status_Custo" in gr.columns:
            sc = gr["Status_Custo"].astype(str).str.strip()
            sc = sc.mask(sc.str.lower().isin({"nan", "none", "<na>", ""}), "")
            ok_s = sc.eq(STATUS_CUSTO_OK)
            any_bad_status = bool((~ok_s).any())
        else:
            any_bad_status = False
        any_nan_res = bool(len(res_num) and res_num.isna().any())
        comercial_incompleto = any_bad_status or any_nan_res

        if not comercial_incompleto and "Resultado" in gr.columns:
            res_raw = float(res_num.fillna(0.0).sum())
        else:
            res_raw = float("nan")

        emi = pd.to_datetime(gr["Nota_Data_Emissao"], errors="coerce", dayfirst=False)
        emi_first = emi.min()
        sit = gr["Nota_Situacao"].dropna().astype(str).str.strip()
        sit_s = str(sit.iloc[0]) if len(sit) else ""

        emp = gr["empresa"].dropna().astype(str).str.strip() if "empresa" in gr.columns else pd.Series(dtype=str)
        emp_s = str(emp.iloc[0]) if len(emp) else ""

        if "Nome da plataforma" in gr.columns:
            plat_vals = gr["Nome da plataforma"].fillna("").astype(str).str.strip()
            w_arr = v_lin.to_numpy(dtype=float)
            best = ""
            best_w = -1.0
            for p, tw in zip(plat_vals, w_arr, strict=False):
                if tw > best_w:
                    best_w = float(tw)
                    best = p
            plats_u = plat_vals[plat_vals.ne("")]
            if plats_u.nunique() > 1:
                plat_res = f"{best or '—'} (+{plats_u.nunique() - 1})" if best else f"{plats_u.nunique()} plataformas"
            else:
                plat_res = best or (str(plats_u.iloc[0]) if len(plats_u) else "—")
        else:
            plat_res = "—"

        if comercial_incompleto or (isinstance(res_raw, float) and pd.isna(res_raw)):
            res = float("nan")
        elif has_desp_fix_col:
            df_lin_sum = float(pd.to_numeric(gr["Despesas Fixas"], errors="coerce").fillna(0.0).sum())
            res = float(res_raw) + df_lin_sum - desp_fix
        else:
            res = float(res_raw)

        _eps_f = 1e-9
        gap_nf = vl_nf - v_venda
        nl_gr = int(len(gr))
        if nl_gr == 1 and rec_tp <= _eps_f and gap_nf > _eps_f and v_venda > _eps_f:
            rec_tp = float(gap_nf)

        ml_set: set[str] = set()
        ped_set: set[str] = set()
        if ml_col in gr.columns:
            for x in gr[ml_col].fillna("").astype(str).str.strip():
                if x:
                    ml_set.add(x)
        if ped_col in gr.columns:
            for x in gr[ped_col].fillna("").astype(str).str.strip():
                if x:
                    ped_set.add(x)

        if len(ml_set) <= 1 and len(ped_set) <= 1:
            ped_res = ""
            if ml_set:
                ped_res = next(iter(ml_set))
            elif ped_set:
                ped_res = next(iter(ped_set))
            else:
                ped_res = "—"
        else:
            parts: list[str] = []
            if len(ml_set) > 1:
                parts.append(f"{len(ml_set)} multiloja")
            elif ml_set:
                parts.append(next(iter(ml_set)))
            if len(ped_set) > 1:
                parts.append(f"{len(ped_set)} pedidos")
            elif ped_set and not ml_set:
                parts.append(next(iter(ped_set)))
            ped_res = " · ".join(parts) if parts else "—"

        if prod_col:
            skus = gr[prod_col].fillna("").astype(str).str.strip()
            skus = skus[skus.ne("")]
            nu = skus.nunique()
            prod_res = str(skus.iloc[0]) if nu == 1 else (f"{skus.iloc[0]} (+{nu - 1} itens)" if nu else "—")
        else:
            prod_res = "—"

        if "faturamento_nota_vinculada" in gr.columns:
            vinc = bool(_fdl_fr_faturamento_series_bool_mask(gr["faturamento_nota_vinculada"]).any())
        else:
            vinc = True

        rows.append(
            {
                "org_id": oid_s,
                "Nota_Numero_Normalizado": nfk,
                "Nota_Data_Emissao": emi_first,
                "Nota_Situacao": sit_s,
                "empresa": emp_s,
                "valor_faturado_nf": vl_nf,
                "valor_venda": v_venda,
                "diferenca": v_venda - vl_nf,
                "comissao": com,
                "custo_produto": custo_p,
                "receita_frete_tp": rec_tp,
                "custo_frete_plataforma": c_frete_plat,
                "repasse_frete_transportadora_propria": rep_tp_ped,
                "tarifa_custo_envio": tarifa_env,
                "imposto": imp,
                "despesa_fixa": desp_fix,
                "resultado": res,
                "plataforma_resumo": plat_res,
                "pedido_resumo": ped_res,
                "n_linhas_pedido": int(len(gr)),
                "produto_resumo": prod_res,
                "faturamento_nota_vinculada": vinc,
                "comercial_incompleto": bool(comercial_incompleto),
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty and out["Nota_Data_Emissao"].notna().any():
        out = out.sort_values("Nota_Data_Emissao", ascending=False, na_position="last")
    return out.reset_index(drop=True), tuple(warn)


def apply_nf_panel_frete_gap_fallback(df: pd.DataFrame, *, eps: float = 1e-9) -> pd.DataFrame:
    """
    Se ``receita_frete_tp`` está ~0, ``valor_venda`` > 0, ``valor_faturado_nf > valor_venda`` e
    ``n_linhas_pedido == 1``, define ``receita_frete_tp = valor_faturado_nf - valor_venda``.

    **Tarifa de envio** (``tarifa_custo_envio`` = coluna Custo de Frete do pedido) **não** é imputada aqui.

    Não altera se ``receita_frete_tp`` já > 0 nem se há mais do que uma linha de pedido na NF.
    """
    need = {"receita_frete_tp", "valor_faturado_nf", "valor_venda", "n_linhas_pedido"}
    if df.empty or not need.issubset(df.columns):
        return df
    out = df.copy()
    rftp = pd.to_numeric(out["receita_frete_tp"], errors="coerce").fillna(0.0)
    vf = pd.to_numeric(out["valor_faturado_nf"], errors="coerce").fillna(0.0)
    vv = pd.to_numeric(out["valor_venda"], errors="coerce").fillna(0.0)
    gap = vf - vv
    nl = pd.to_numeric(out["n_linhas_pedido"], errors="coerce").fillna(0).astype(int)
    m = (rftp <= eps) & (gap > eps) & (vv > eps) & (nl == 1)
    if m.any():
        out.loc[m, "receita_frete_tp"] = gap.loc[m].astype(float)
    return out


def apply_nf_panel_frete_repasse_e_plataforma_coerencia(df: pd.DataFrame, *, eps: float = 1e-9) -> pd.DataFrame:
    """
    Coerência frete TP × plataforma (sem alterar comissão / preço lista / vínculos):

    **A) Com ``tarifa_custo_envio`` > 0:** o split ME/TP pode mandar o CF todo para plataforma (``repasse = 0``)
    enquanto a **NF** traz frete em ``Frete_Nota_Export``. Ajusta-se ``repasse = min(receita, tarifa)`` e
    ``custo_frete_plataforma = tarifa − repasse``.

    **B) Com tarifa ~0 e ``receita_frete_tp`` > 0:** imputa ``repasse = receita`` (inclui perfil em que a receita
    coincide com o gap NF×lista). O ajuste ``+ receita − repasse`` em ``apply_nf_panel_resultado_frete_nota_lista``
    fica neutro no frete TP; a DRE mostra receita e repasse alinhados.

    **C) Rubrica plataforma:** onde tarifa ~0 e há receita TP, força ``custo_frete_plataforma = 0`` para o KPI não
    contaminar a logística da plataforma com valor que é frete na NF / TP (o agregado comercial às vezes replica
    esse montante no campo de plataforma).
    """
    need = {
        "receita_frete_tp",
        "repasse_frete_transportadora_propria",
        "custo_frete_plataforma",
        "tarifa_custo_envio",
    }
    if df.empty or not need.issubset(df.columns):
        return df
    out = df.copy()
    rec = pd.to_numeric(out["receita_frete_tp"], errors="coerce").fillna(0.0)
    rep0 = pd.to_numeric(out["repasse_frete_transportadora_propria"], errors="coerce").fillna(0.0)
    tar = pd.to_numeric(out["tarifa_custo_envio"], errors="coerce").fillna(0.0)
    inc = (
        out["comercial_incompleto"].fillna(False).astype(bool)
        if "comercial_incompleto" in out.columns
        else pd.Series(False, index=out.index)
    )
    m_tar = (rep0 <= eps) & (rec > eps) & (tar > eps) & (~inc)
    if m_tar.any():
        cap = pd.concat([rec.loc[m_tar], tar.loc[m_tar]], axis=1).min(axis=1).astype(float)
        out.loc[m_tar, "repasse_frete_transportadora_propria"] = cap
        out.loc[m_tar, "custo_frete_plataforma"] = (tar.loc[m_tar] - cap).clip(lower=0.0).astype(float)

    rep = pd.to_numeric(out["repasse_frete_transportadora_propria"], errors="coerce").fillna(0.0)
    m_zero = (rep <= eps) & (rec > eps) & (tar <= eps) & (~inc)
    if m_zero.any():
        out.loc[m_zero, "repasse_frete_transportadora_propria"] = rec.loc[m_zero].astype(float)

    tar_c = pd.to_numeric(out["tarifa_custo_envio"], errors="coerce").fillna(0.0)
    rec_c = pd.to_numeric(out["receita_frete_tp"], errors="coerce").fillna(0.0)
    m_clean = (tar_c <= eps) & (rec_c > eps) & (~inc)
    if m_clean.any():
        out.loc[m_clean, "custo_frete_plataforma"] = 0.0
    return out


def apply_nf_panel_resultado_frete_nota_lista(
    df: pd.DataFrame,
    *,
    eps: float = 1e-9,
    rel_tol: float = 0.02,
) -> pd.DataFrame:
    """
    Ajusta ``resultado`` pelo frete: **receita** (``receita_frete_tp`` = ``Frete_Nota_Export`` no merge fiscal ou
    grão/gap sem fiscal) menos **repasse** à transportadora própria (``repasse_frete_transportadora_propria``).

    O ``Resultado`` por linha de pedido já desconta ``Frete_Plataforma`` (custo da logística da plataforma / ME),
    mas **não** desconta a parcela TP do ``Custo de Frete``; por isso o repasse é abatido aqui.

    ``rel_tol`` permanece na assinatura por compatibilidade com chamadas antigas; não é usado.
    """
    _ = (eps, rel_tol)
    need = {"receita_frete_tp", "resultado"}
    if df.empty or not need.issubset(df.columns):
        return df
    out = df.copy()
    rftp = pd.to_numeric(out["receita_frete_tp"], errors="coerce").fillna(0.0)
    if "repasse_frete_transportadora_propria" in out.columns:
        rep = pd.to_numeric(out["repasse_frete_transportadora_propria"], errors="coerce").fillna(0.0)
    else:
        rep = pd.Series(0.0, index=out.index, dtype=float)
    res = pd.to_numeric(out["resultado"], errors="coerce")
    inc = (
        out["comercial_incompleto"].fillna(False).astype(bool)
        if "comercial_incompleto" in out.columns
        else pd.Series(False, index=out.index)
    )
    m_ok = (~inc) & res.notna()
    if not m_ok.any():
        return out
    out.loc[m_ok, "resultado"] = (
        res.loc[m_ok] + rftp.loc[m_ok].astype(float) - rep.loc[m_ok].astype(float)
    ).astype(float)
    return out


def apply_nf_panel_custo_ads(df: pd.DataFrame, *, eps: float = 1e-9) -> pd.DataFrame:
    """
    Custo de **ADS** por NF: ``NF_FIRST_PANEL_ADS_ALIQUOTA × valor_venda`` + fixo por venda com lista > 0.

    Grava ``custo_ads_variavel``, ``custo_ads_fixo`` e ``custo_ads`` (soma) e **subtrai** ``custo_ads`` de
    ``resultado`` (NaN permanece NaN). Sem ``valor_venda`` / ``resultado`` não altera.
    """
    need = {"valor_venda", "resultado"}
    if df.empty or not need.issubset(df.columns):
        return df
    out = df.copy()
    vv = pd.to_numeric(out["valor_venda"], errors="coerce").fillna(0.0)
    m_sale = vv > eps
    ads_var = (vv * float(NF_FIRST_PANEL_ADS_ALIQUOTA)).astype(float)
    ads_fix = pd.Series(0.0, index=out.index, dtype=float)
    ads_fix.loc[m_sale] = float(NF_FIRST_PANEL_ADS_FIXO_POR_VENDA)
    ads_tot = ads_var + ads_fix
    out["custo_ads_variavel"] = ads_var
    out["custo_ads_fixo"] = ads_fix.astype(float)
    out["custo_ads"] = ads_tot.astype(float)
    res = pd.to_numeric(out["resultado"], errors="coerce")
    out["resultado"] = res - ads_tot
    return out


def apply_nf_panel_custo_from_line_grain(
    df_nf: pd.DataFrame,
    df_line: pd.DataFrame,
    state: FaturamentoRecorteMinState,
    *,
    ok_nf_dates: bool,
    nf_d_ini: date,
    nf_d_fim: date,
    eps: float = 1e-9,
) -> pd.DataFrame:
    """
    Recalcula ``custo_produto`` a partir do dataset **linha** (mesmo recorte que ``build_nf_grain_dataframe``).
    Não é usado pelo Streamlit: o app lê só o Parquet NF materializado; mantida para testes ou scripts offline.
    """
    if df_nf.empty or df_line.empty or "custo_produto" not in df_nf.columns:
        return df_nf
    if not ok_nf_dates or nf_d_fim < nf_d_ini:
        return df_nf
    need = {"Nota_Data_Emissao", "Nota_Valor_Liquido_Total", "Nota_Numero_Normalizado"}
    if not need.issubset(df_line.columns):
        return df_nf

    g_line, _ = build_nf_grain_dataframe(
        df_line,
        state,
        ok_nf_dates=ok_nf_dates,
        nf_d_ini=nf_d_ini,
        nf_d_fim=nf_d_fim,
    )
    if g_line.empty or "custo_produto" not in g_line.columns:
        return df_nf

    out = df_nf.copy()

    def _oid(frame: pd.DataFrame) -> pd.Series:
        if "org_id" in frame.columns:
            return frame["org_id"].fillna("").astype(str).str.strip()
        return pd.Series("", index=frame.index, dtype=str)

    out["_k_o"] = _oid(out)
    out["_k_n"] = out["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    gl = g_line.copy()
    gl["_k_o"] = _oid(gl)
    gl["_k_n"] = gl["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    sub = gl[["_k_o", "_k_n", "custo_produto"]].rename(columns={"custo_produto": "_custo_grain_linha"})
    sub = sub.drop_duplicates(subset=["_k_o", "_k_n"], keep="first")
    out = out.merge(sub, on=["_k_o", "_k_n"], how="left")
    cp = pd.to_numeric(out["custo_produto"], errors="coerce").fillna(0.0)
    cl = pd.to_numeric(out["_custo_grain_linha"], errors="coerce").fillna(0.0)
    out["custo_produto"] = cl.where(cl > eps, cp).astype(float)
    return out.drop(columns=["_k_o", "_k_n", "_custo_grain_linha"])


def compute_nf_panel_kpis(df_nf: pd.DataFrame) -> dict[str, float | int]:
    """KPIs do painel NF-first = somas (e contagens) sobre ``df_nf``."""
    empty = {
        "valor_venda": 0.0,
        "valor_faturado_nf": 0.0,
        "diferenca": 0.0,
        "comissao": 0.0,
        "custo_produto": 0.0,
        "receita_frete_tp": 0.0,
        "custo_frete_plataforma": 0.0,
        "repasse_frete_transportadora_propria": 0.0,
        "tarifa_custo_envio": 0.0,
        "imposto": 0.0,
        "despesa_fixa": 0.0,
        "custo_ads_variavel": 0.0,
        "custo_ads_fixo": 0.0,
        "custo_ads": 0.0,
        "resultado": 0.0,
        "n_nf": 0,
    }
    if df_nf.empty:
        return empty
    vv = float(pd.to_numeric(df_nf["valor_venda"], errors="coerce").fillna(0.0).sum())
    vf = float(pd.to_numeric(df_nf["valor_faturado_nf"], errors="coerce").fillna(0.0).sum())
    return {
        "valor_venda": vv,
        "valor_faturado_nf": vf,
        "diferenca": vv - vf,
        "comissao": float(pd.to_numeric(df_nf["comissao"], errors="coerce").fillna(0.0).sum()),
        "custo_produto": float(
            pd.to_numeric(df_nf["custo_produto"], errors="coerce").fillna(0.0).sum()
        )
        if "custo_produto" in df_nf.columns
        else 0.0,
        "receita_frete_tp": float(
            pd.to_numeric(df_nf["receita_frete_tp"], errors="coerce").fillna(0.0).sum()
        )
        if "receita_frete_tp" in df_nf.columns
        else 0.0,
        "custo_frete_plataforma": float(
            pd.to_numeric(df_nf["custo_frete_plataforma"], errors="coerce").fillna(0.0).sum()
        )
        if "custo_frete_plataforma" in df_nf.columns
        else 0.0,
        "repasse_frete_transportadora_propria": float(
            pd.to_numeric(df_nf["repasse_frete_transportadora_propria"], errors="coerce").fillna(0.0).sum()
        )
        if "repasse_frete_transportadora_propria" in df_nf.columns
        else 0.0,
        "tarifa_custo_envio": float(
            pd.to_numeric(df_nf["tarifa_custo_envio"], errors="coerce").fillna(0.0).sum()
        )
        if "tarifa_custo_envio" in df_nf.columns
        else 0.0,
        "imposto": float(pd.to_numeric(df_nf["imposto"], errors="coerce").fillna(0.0).sum()),
        "despesa_fixa": float(pd.to_numeric(df_nf["despesa_fixa"], errors="coerce").fillna(0.0).sum()),
        "custo_ads_variavel": float(
            pd.to_numeric(df_nf["custo_ads_variavel"], errors="coerce").fillna(0.0).sum()
        )
        if "custo_ads_variavel" in df_nf.columns
        else 0.0,
        "custo_ads_fixo": float(
            pd.to_numeric(df_nf["custo_ads_fixo"], errors="coerce").fillna(0.0).sum()
        )
        if "custo_ads_fixo" in df_nf.columns
        else 0.0,
        "custo_ads": float(pd.to_numeric(df_nf["custo_ads"], errors="coerce").fillna(0.0).sum())
        if "custo_ads" in df_nf.columns
        else 0.0,
        "resultado": float(pd.to_numeric(df_nf["resultado"], errors="coerce").sum()),
        "n_nf": int(len(df_nf)),
    }


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
    *,
    data_venda_ini: object | None = None,
    data_venda_fim: object | None = None,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Recorte em **grão pedido**: empresa → plataforma → período opcional (**Data** venda).

    O painel NF-first não usa esta função; datas de venda ficam como argumentos para testes / reuso.

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
        want = {nf_grain_plataforma_match_key(x) for x in sel_plat}
        want.discard("")
        if want:
            got = sliced["Nome da plataforma"].map(nf_grain_plataforma_match_key)
            sliced = sliced.loc[got.isin(want)].copy()

    if ok_dates and not sliced.empty:
        d_ini = _fdl_fr_safe_streamlit_date(data_venda_ini, d_min)
        d_fim = _fdl_fr_safe_streamlit_date(data_venda_fim, d_max)
        if d_fim < d_ini:
            warn.append("A data final da **venda** não pode ser anterior à inicial.")
            d_fim = d_ini
        m_d = _fdl_fr_mask_venda_no_periodo(sliced["Data"], d_ini, d_fim)
        sliced = sliced.loc[m_d].copy()

    return sliced, tuple(warn)
