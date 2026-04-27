"""Regressão: ``SITUACOES_DEVOLUCAO_VALIDAS`` inclui «Emitida DANFE» e strip de situação."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.io_notas_entrada import aplicar_filtros_devolucao


def _minimal_devolucoes_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_autorizada_passa_no_filtro() -> None:
    df = _minimal_devolucoes_df(
        [
            {"Natureza": "Entrada de Devolução", "Situação": "Autorizada"},
        ]
    )
    got = aplicar_filtros_devolucao(df)
    assert len(got) == 1


def test_emitida_danfe_passa_no_filtro_regressao_checkpoint_a() -> None:
    df = _minimal_devolucoes_df(
        [
            {"Natureza": "Entrada de Devolução", "Situação": "Emitida DANFE"},
        ]
    )
    got = aplicar_filtros_devolucao(df)
    assert len(got) == 1


def test_cancelada_excluida() -> None:
    df = _minimal_devolucoes_df(
        [
            {"Natureza": "Entrada de Devolução", "Situação": "Cancelada"},
        ]
    )
    got = aplicar_filtros_devolucao(df)
    assert len(got) == 0


def test_situacao_com_espacos_emitida_danfe_aceita() -> None:
    df = _minimal_devolucoes_df(
        [
            {"Natureza": "Entrada de Devolução", "Situação": " Emitida DANFE "},
        ]
    )
    got = aplicar_filtros_devolucao(df)
    assert len(got) == 1
