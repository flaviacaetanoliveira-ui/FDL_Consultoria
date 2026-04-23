"""Testes do módulo comercial_pedidos_analise (sem Streamlit)."""

from __future__ import annotations

from datetime import date

import pandas as pd

import comercial_pedidos_analise as cpa


def test_three_closed_months_april_2026_example() -> None:
    """Em abril/2026, tendência = jan–mar (nunca abril em aberto)."""
    start, end, triple = cpa.three_closed_months_trend_bounds(
        date(2026, 4, 10), as_of=date(2026, 4, 15)
    )
    assert triple == ((2026, 1), (2026, 2), (2026, 3))
    assert start.month == 1 and end.month == 3 and end.day == 31


def test_period_end_march_closed_uses_march() -> None:
    start, end, triple = cpa.three_closed_months_trend_bounds(
        date(2026, 3, 31), as_of=date(2026, 4, 5)
    )
    assert triple == ((2026, 1), (2026, 2), (2026, 3))


def test_period_end_in_open_month_caps_to_previous() -> None:
    """Fim do período no mesmo mês que «hoje» → último mês da janela = mês anterior."""
    triple = cpa.three_closed_months_trend_bounds(date(2026, 3, 18), as_of=date(2026, 3, 20))[2]
    assert triple == ((2025, 12), (2026, 1), (2026, 2))


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


def test_filter_trend_window_only_closed_months() -> None:
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
        as_of=date(2026, 4, 1),
    )
    assert len(out) == 3
    assert "15/04/2026" not in out["Data"].astype(str).tolist()


def test_compute_trend_uses_three_closed_months_not_partial() -> None:
    """Última coluna = último mês fechado da janela, não o mês do fim do filtro se em aberto."""
    df = pd.DataFrame(
        {
            "Situação": ["Atendido"] * 5,
            "Número do pedido": ["a", "b", "c", "d", "e"],
            "Código": ["X"] * 5,
            "Quantidade": [10.0, 10.0, 10.0, 100.0, 999.0],
            "Preço de lista": [1.0] * 5,
            "Data": ["10/01/2026", "10/02/2026", "05/03/2026", "28/03/2026", "15/04/2026"],
            "Descrição": ["x"] * 5,
        }
    )
    abc = cpa.compute_abc_valor(df)
    tbl = cpa.compute_trend_and_suggestion(
        df, abc, period_end=date(2026, 4, 10), as_of=date(2026, 4, 15)
    )
    row = tbl.loc[tbl["SKU"].astype(str).eq("X")].iloc[0]
    assert row["Qtd mês atual"] == 110.0
    assert row["Qtd mês -1"] == 10.0
    assert row["Qtd mês -2"] == 10.0
