"""Recorte mínimo Etapa 1 — empresa, plataforma, período venda."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoRecorteMinState,
    apply_recorte_minimo,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Data": pd.to_datetime(["2025-01-10", "2025-01-20", "2025-02-05"]),
            "Nome da plataforma": ["ML", "Shopee", "ML"],
            "empresa": ["A", "A", "B"],
            "Situação": ["Atendido", "Cancelado", "Atendido"],
        }
    )


def test_empty_empresa_and_plat_is_all_rows() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, w = apply_recorte_minimo(df, st)
    assert len(out) == 3 and not w


def test_empresa_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=("B",),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1 and out.iloc[0]["empresa"] == "B"


def test_plat_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=("Shopee",),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1


def test_date_window() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=(),
        data_venda_ini=date(2025, 1, 15),
        data_venda_fim=date(2025, 2, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1


def test_all_situacoes_preserved_without_situacao_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState((), (), date(2024, 1, 1), date(2030, 1, 1))
    out, _ = apply_recorte_minimo(df, st)
    assert set(out["Situação"].astype(str)) == {"Atendido", "Cancelado"}
