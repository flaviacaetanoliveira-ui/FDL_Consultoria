"""Cache Streamlit para agregações pesadas do Resultado Gerencial (apenas cálculo, sem UI)."""

from __future__ import annotations

import os
from datetime import date

import pandas as pd
import streamlit as st

from processing.faturamento.resultado_gerencial_slice import (
    PedidoGerencialRow,
    ResultadoGerencialSlice,
    build_resultado_gerencial_slice,
    compute_resultado_gerencial_kpis,
    compute_tabela_por_pedido,
)
from processing.faturamento.analise_plataforma import AnalisePlataforma, compute_analise_plataforma
from processing.faturamento.comparacao_temporal_kpis import ComparacaoKpisTemporal, compute_comparacao_kpis_temporal
from processing.faturamento.rg_cache_keys import PIPELINE_VERSION_ENV_NAME, dataframe_cache_token

DEFAULT_RG_PIPELINE_VERSION = "rg_slice_agg_v1"


def pipeline_version() -> str:
    return (os.environ.get(PIPELINE_VERSION_ENV_NAME) or DEFAULT_RG_PIPELINE_VERSION).strip() or DEFAULT_RG_PIPELINE_VERSION


@st.cache_data(
    ttl=3600,
    show_spinner=False,
    hash_funcs={pd.DataFrame: dataframe_cache_token},
)
def cached_rg_slice_kpis_tabela(
    df_linha: pd.DataFrame,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_venda_ini: date,
    data_venda_fim: date,
    fiscal_imposto_valor: float,
    pipeline_version: str,
    cliente_slug: str,
) -> tuple[ResultadoGerencialSlice, dict[str, float | int], list[PedidoGerencialRow]]:
    """Uma passagem: slice + KPIs + tabela por pedido (linhas completas do período)."""
    _ = cliente_slug  # faz parte da chave do ``@st.cache_data`` (tenant / slug)
    _ = pipeline_version  # faz parte da chave — invalida quando o ETL sobe versão
    sl = build_resultado_gerencial_slice(
        df_linha,
        empresas_sel=empresas_sel,
        plataformas_sel=plataformas_sel,
        data_venda_ini=data_venda_ini,
        data_venda_fim=data_venda_fim,
    )
    kp = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=float(fiscal_imposto_valor))
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=float(fiscal_imposto_valor))
    return sl, kp, tab


@st.cache_data(
    ttl=3600,
    show_spinner=False,
    hash_funcs={pd.DataFrame: dataframe_cache_token},
)
def cached_comparacao_kpis_temporal(
    df_linha: pd.DataFrame,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_venda_ini: date,
    data_venda_fim: date,
    receita_kp: float,
    resultado_kp: float,
    pipeline_version: str,
    cliente_slug: str,
) -> ComparacaoKpisTemporal:
    """Mesma família de chave que ``cached_rg_slice_kpis_tabela`` + totais KPI para invalidação coerente."""
    _ = pipeline_version  # invalidação quando o ETL sobe versão
    _ = cliente_slug
    kp = {"valor_venda_lista": float(receita_kp), "resultado": float(resultado_kp)}
    return compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df_linha,
        empresas_sel=empresas_sel,
        plataformas_sel=plataformas_sel,
        data_inicio=data_venda_ini,
        data_fim=data_venda_fim,
        kp_rg=kp,
    )


@st.cache_data(
    ttl=3600,
    show_spinner=False,
    hash_funcs={pd.DataFrame: dataframe_cache_token},
)
def cached_analise_plataforma(
    df_linha: pd.DataFrame,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_venda_ini: date,
    data_venda_fim: date,
    fiscal_imposto_valor: float,
    pipeline_version: str,
    cliente_slug: str,
) -> AnalisePlataforma:
    """Reusa ``cached_rg_slice_kpis_tabela`` (hit no cache) e agrega por plataforma."""
    _ = pipeline_version
    _ = cliente_slug
    sl, kp, tab = cached_rg_slice_kpis_tabela(
        df_linha,
        empresas_sel,
        plataformas_sel,
        data_venda_ini,
        data_venda_fim,
        fiscal_imposto_valor,
        pipeline_version,
        cliente_slug,
    )
    return compute_analise_plataforma(slice_rg=sl, pedidos_tabela=tab, kp_rg=kp)


# Re-export for app/tests
PIPELINE_VERSION_DEFAULT = DEFAULT_RG_PIPELINE_VERSION
