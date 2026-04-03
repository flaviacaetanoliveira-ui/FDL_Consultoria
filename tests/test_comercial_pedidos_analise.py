"""Testes do módulo comercial_pedidos_analise (sem Streamlit)."""

from __future__ import annotations

from datetime import date

import pandas as pd

import comercial_pedidos_analise as cpa


def test_three_month_calendar_bounds_march_end() -> None:
    start, end, triple = cpa.three_month_calendar_bounds(date(2026, 3, 31))
    assert triple == ((2026, 1), (2026, 2), (2026, 3))
    assert start.year == 2026 and start.month == 1 and start.day == 1
    assert end.year == 2026 and end.month == 3 and end.day == 31


def test_pedidos_atendidos_distintos_nao_linhas() -> None:
    df = pd.DataFrame(
        {
            "Situação": ["Atendido", "Atendido", "Atendido"],
            "Número do pedido": ["P1", "P1", "P2"],
            "Código": ["A", "A", "B"],
            "Quantidade": [1.0, 2.0, 1.0],
            "Preço de lista": [10.0, 10.0, 5.0],
            "Data": ["01/01/2026", "01/01/2026", "02/01/2026"],
            "Descrição": ["x", "x", "y"],
        }
    )
    base = cpa.filter_atendidos(df)
    k = cpa.compute_kpis(base)
    assert k["pedidos_atendidos_distintos"] == 2
    assert k["quantidade_total"] == 4.0


def test_filter_trend_window_respects_period_end_month() -> None:
    df = pd.DataFrame(
        {
            "Situação": ["Atendido"] * 5,
            "Número do pedido": ["1", "2", "3", "4", "5"],
            "Código": ["S"] * 5,
            "Quantidade": [1.0] * 5,
            "Preço de lista": [1.0] * 5,
            "Data": [
                "15/01/2026",
                "15/02/2026",
                "15/03/2026",
                "15/12/2025",
                "15/04/2026",
            ],
            "Descrição": ["p"] * 5,
        }
    )
    out = cpa.filter_trend_window(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        period_end=date(2026, 3, 20),
    )
    # Dezembro 2025 e abril 2026 fora de jan–mar (M=março); permanecem jan–mar.
    assert len(out) == 3


def test_compute_trend_uses_calendar_months_not_data_max() -> None:
    """Tendência deve usar M-2,M-1,M de ``period_end``, não o último mês com dados."""
    df = pd.DataFrame(
        {
            "Situação": ["Atendido"] * 4,
            "Número do pedido": ["a", "b", "c", "d"],
            "Código": ["X"] * 4,
            "Quantidade": [10.0, 10.0, 10.0, 100.0],
            "Preço de lista": [1.0] * 4,
            "Data": ["10/01/2026", "10/02/2026", "05/03/2026", "28/03/2026"],
            "Descrição": ["x"] * 4,
        }
    )
    abc = cpa.compute_abc_valor(df)
    tbl = cpa.compute_trend_and_suggestion(df, abc, period_end=date(2026, 3, 31))
    row = tbl.loc[tbl["SKU"].astype(str).eq("X")].iloc[0]
    assert row["Qtd mês atual"] == 110.0  # março: 10+100
    assert row["Qtd mês -1"] == 10.0
    assert row["Qtd mês -2"] == 10.0
