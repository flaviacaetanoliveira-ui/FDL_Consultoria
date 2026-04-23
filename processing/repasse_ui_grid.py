"""
Guardrail de volume na grelha do módulo Conciliação de Repasse (UI).

Limita linhas enviadas ao ``st.dataframe`` para evitar timeout/ecrã em branco em orgs grandes,
sem alterar o dataset carregado nem KPIs/export (export continua a usar o recorte completo).
"""

from __future__ import annotations

import pandas as pd

# Alinhado ao limiar de export pesado (Excel/PDF) no painel repasse.
REPASSE_UI_GRID_ROW_CAP: int = 3000


def repasse_ui_grid_display_slice(
    tabela_exibir: pd.DataFrame,
    *,
    cap: int = REPASSE_UI_GRID_ROW_CAP,
) -> tuple[pd.DataFrame, int, bool]:
    """
    Devolve um subconjunto para renderização na grelha.

    Returns:
        (dataframe_para_grelha, n_linhas_recorte_total, truncado)
    """
    n = len(tabela_exibir)
    if tabela_exibir.empty or n <= cap:
        return tabela_exibir, n, False
    return tabela_exibir.head(cap).copy(), n, True


def repasse_ui_apply_grid_styler(*, grid_truncated: bool) -> bool:
    """Styler pesado só quando a grelha mostra o recorte completo (sem truncamento)."""
    return not grid_truncated
