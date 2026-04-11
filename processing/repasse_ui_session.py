"""
PR5 — lógica de sessão da UI de repasse (Parquet canónico vs CSV legado).

Funções puras testáveis: escolha da coluna de ação e série de período **sem**
recomputação pay/emissão quando o consumo é Parquet (já materializado no pipeline).
"""

from __future__ import annotations

from collections.abc import Callable

import pandas as pd

from processing.repasse_contract import REPASSE_ACTION_COLUMN

COL_DATA_PERIODO_REPASSE = "Data período repasse"
COL_ACAO_LEGACY_UI = "Ação sugerida operacional"


def repasse_ui_acao_column(*, use_parquet: bool) -> str:
    """Parquet: coluna canónica do pipeline; legado: coluna derivada com map_acao na UI."""
    return REPASSE_ACTION_COLUMN if use_parquet else COL_ACAO_LEGACY_UI


def repasse_ui_periodo_series_parquet(
    df: pd.DataFrame,
    *,
    parse_data_periodo_repasse_column: Callable[[pd.DataFrame], pd.Series],
) -> tuple[pd.Series, str]:
    """
    Período **somente** a partir de ``Data período repasse`` (valores vazios → NaT).

    Não usa pagamento/emissão como fallback (isso fica no caminho legado).
    """
    if COL_DATA_PERIODO_REPASSE not in df.columns:
        return (
            pd.Series(pd.NaT, index=df.index),
            f"{COL_DATA_PERIODO_REPASSE} (ausente no ficheiro)",
        )
    s = parse_data_periodo_repasse_column(df)
    return s, COL_DATA_PERIODO_REPASSE


def repasse_ui_apply_pipeline_exclusao_na_ui(*, use_parquet: bool) -> bool:
    """Legado aplica ``_excluir_linhas_fora_conciliacao`` na UI; Parquet já vem filtrado do pipeline."""
    return not use_parquet


def repasse_ui_apply_filtro_somente_linhas_com_data_pagamento(*, use_parquet: bool) -> bool:
    """Legado restringe tabela/KPI a linhas com data de pagamento; Parquet: só filtros de sessão."""
    return not use_parquet
