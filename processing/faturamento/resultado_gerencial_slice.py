"""
Camada de dados do Resultado Gerencial (grão linha), com âncora temporal em **Data** (venda).

Esta camada não altera a UI nem a Apuração Fiscal; serve de base para migração gradual dos blocos
(KPIs, DRE, Saúde, Tabela) para o recorte por data de venda.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import pandas as pd

from comercial_pedidos_analise import pedido_id_series
from faturamento_dre_recorte import _fdl_fr_filtrar_por_etiquetas_empresa
from faturamento_dre_recorte_minimo import nf_grain_plataforma_match_key
from processing.faturamento.calc import _frete_mercado_envios_vs_transportadora

# Colunas mínimas do ``dataset.parquet`` (grão linha) documentadas para o Resultado Gerencial.
REQUIRED_LINE_COLUMNS: frozenset[str] = frozenset(
    {
        "Valor total",
        "Taxa de Comissão",
        "Frete_Plataforma",
        "Custo_Produto_Total",
        "Resultado",
        "Data",
        "Nome da plataforma",
        "empresa",
        "org_id",
        "Número do pedido",
    }
)


def _validate_line_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_LINE_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "dataset.parquet (grão linha) sem colunas esperadas pelo Resultado Gerencial: "
            + ", ".join(sorted(missing))
        )


def _num_sum(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())


def _receita_linha_series(df: pd.DataFrame) -> pd.Series:
    """Receita comercial por linha: ``Vl_Venda`` quando existir; senão ``Valor total``."""
    if "Vl_Venda" in df.columns:
        return pd.to_numeric(df["Vl_Venda"], errors="coerce").fillna(0.0)
    return pd.to_numeric(df["Valor total"], errors="coerce").fillna(0.0)


def _frete_transportadora_propria_sum(df: pd.DataFrame) -> float:
    if "Frete transportadora própria" in df.columns:
        return _num_sum(df, "Frete transportadora própria")
    if "Custo de Frete" not in df.columns:
        return 0.0
    cf = pd.to_numeric(df["Custo de Frete"], errors="coerce").fillna(0.0)
    _me, ftp = _frete_mercado_envios_vs_transportadora(df, cf)
    return float(ftp.fillna(0.0).sum())


def _ads_total(df: pd.DataFrame) -> float:
    if "custo_ads" in df.columns:
        return _num_sum(df, "custo_ads")
    v = 0.0
    if "custo_ads_variavel" in df.columns:
        v += _num_sum(df, "custo_ads_variavel")
    if "custo_ads_fixo" in df.columns:
        v += _num_sum(df, "custo_ads_fixo")
    return v


@dataclass(frozen=True)
class ResultadoGerencialSliceMeta:
    """Filtros e período aplicados ao slice."""

    empresas_sel: tuple[str, ...]
    plataformas_sel: tuple[str, ...]
    data_venda_ini: date
    data_venda_fim: date


@dataclass(frozen=True)
class ResultadoGerencialSliceStats:
    """Totais vetorizados sobre o recorte (grão linha)."""

    receita_total: float
    comissao_total: float
    frete_plataforma_total: float
    frete_transportadora_propria_total: float
    cmv_total: float
    resultado_linhas_total: float
    despesa_fixa_total: float
    ads_total: float
    n_linhas: int
    n_pedidos_unicos: int


@dataclass(frozen=True)
class ResultadoGerencialSlice:
    """Recorte gerencial em grão linha + totais e chave de pedido canónica."""

    df_linha: pd.DataFrame
    pedido_ids: pd.Series  # mesmo índice que ``df_linha``; produto ``comercial_pedidos_analise.pedido_id_series``
    stats: ResultadoGerencialSliceStats
    meta: ResultadoGerencialSliceMeta


def build_resultado_gerencial_slice(
    df_linha: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_venda_ini: date,
    data_venda_fim: date,
) -> ResultadoGerencialSlice:
    """Aplica recorte gerencial (empresas, plataformas, data da venda) e devolve slice + stats."""
    _validate_line_columns(df_linha)
    df = df_linha
    if empresas_sel:
        df = _fdl_fr_filtrar_por_etiquetas_empresa(df, list(empresas_sel))
    if plataformas_sel:
        want = {nf_grain_plataforma_match_key(x) for x in plataformas_sel}
        want.discard("")
        got = df["Nome da plataforma"].map(nf_grain_plataforma_match_key)
        df = df.loc[got.isin(want)].copy()

    ts = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    dcal = ts.dt.date
    m_date = ts.notna() & (dcal >= data_venda_ini) & (dcal <= data_venda_fim)
    df = df.loc[m_date].copy()

    receita_total = float(_receita_linha_series(df).sum())
    comissao_total = _num_sum(df, "Taxa de Comissão")
    frete_plataforma_total = _num_sum(df, "Frete_Plataforma")
    frete_tp_total = _frete_transportadora_propria_sum(df)
    cmv_total = _num_sum(df, "Custo_Produto_Total")
    resultado_linhas_total = _num_sum(df, "Resultado")
    despesa_fixa_total = _num_sum(df, "Despesas Fixas")
    ads_total = _ads_total(df)

    pids = pedido_id_series(df).astype(str).str.strip()
    n_pedidos = int(pids[pids.ne("")].nunique())

    stats = ResultadoGerencialSliceStats(
        receita_total=receita_total,
        comissao_total=comissao_total,
        frete_plataforma_total=frete_plataforma_total,
        frete_transportadora_propria_total=frete_tp_total,
        cmv_total=cmv_total,
        resultado_linhas_total=resultado_linhas_total,
        despesa_fixa_total=despesa_fixa_total,
        ads_total=ads_total,
        n_linhas=int(len(df)),
        n_pedidos_unicos=n_pedidos,
    )
    meta = ResultadoGerencialSliceMeta(
        empresas_sel=empresas_sel,
        plataformas_sel=plataformas_sel,
        data_venda_ini=data_venda_ini,
        data_venda_fim=data_venda_fim,
    )
    return ResultadoGerencialSlice(df_linha=df, pedido_ids=pids.reindex(df.index), stats=stats, meta=meta)


def compute_resultado_gerencial_kpis(
    slice_: ResultadoGerencialSlice,
    *,
    fiscal_imposto_valor: float,
) -> dict[str, float | int]:
    """Calcula KPIs consolidados do Resultado Gerencial a partir do slice gerencial.

    PONTE COM APURAÇÃO FISCAL:
    O imposto desta função NÃO é recalculado — é consumido da Apuração Fiscal via parâmetro
    ``fiscal_imposto_valor``. O imposto é calculado sobre a base fiscal (data de emissão da NF),
    enquanto os demais KPIs usam data de venda.

    Isso pode gerar pequena defasagem temporal: vendas do período filtrado podem ter NFs emitidas
    em períodos adjacentes, e vice-versa. Isso é inerente ao regime de competência fiscal e deve ser
    comunicado ao usuário via tooltip na UI.

    Definições (Etapa 1 — base analítica):
    * **Valor da Venda (lista)** / receita_total: soma da receita por linha no slice (``Vl_Venda`` ou
      ``Valor total``).
    * **Total Receita (DRE):** receita_total + frete_transportadora_própria (repasse TP como receita).
    * **Total Deduções:** comissão + CMV + frete plataforma + frete TP + imposto fiscal + despesa fixa + ADS.
      (Colunas opcionais de ADS ausentes contribuem com 0.)
    * **Resultado:** receita_total − Total Deduções (imposto = valor fiscal externo).
    * **Margem:** resultado ÷ receita_total quando receita_total > 0 (fração 0–1).
    * **Ticket médio:** receita_total ÷ pedidos quando há pedidos.
    """
    s = slice_.stats
    receita = s.receita_total
    total_deducoes = (
        s.comissao_total
        + s.cmv_total
        + s.frete_plataforma_total
        + s.frete_transportadora_propria_total
        + float(fiscal_imposto_valor)
        + s.despesa_fixa_total
        + s.ads_total
    )
    resultado = receita - total_deducoes
    pedidos = s.n_pedidos_unicos
    ticket = receita / pedidos if pedidos else float("nan")
    margem = resultado / receita if receita else float("nan")
    total_receita_dre = receita + s.frete_transportadora_propria_total

    return {
        "resultado": resultado,
        "margem": margem,
        "valor_venda_lista": receita,
        "pedidos": pedidos,
        "ticket_medio": ticket,
        "total_receita_dre": total_receita_dre,
        "total_deducoes": total_deducoes,
        "fiscal_imposto_valor": float(fiscal_imposto_valor),
        "n_linhas": s.n_linhas,
        "resultado_linhas_total": s.resultado_linhas_total,
    }
