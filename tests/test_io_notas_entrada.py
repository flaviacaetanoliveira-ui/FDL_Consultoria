"""Testes de filtro de notas de entrada (devoluções fiscais)."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.io_notas_entrada import aplicar_filtros_devolucao


def test_natureza_entrada_devolucao_sem_preposicao_aceita() -> None:
    """
    Variação 'Entrada Devolução' (sem 'de') é aceita.
    Reflete grafia real usada pela Mega Fácil no Bling.
    """
    df_mock = pd.DataFrame(
        {
            "Natureza": ["Entrada Devolução", "Entrada de Devolução", "Compra de mercadorias"],
            "Situação": ["Autorizada", "Autorizada", "Autorizada"],
            "Nota_Numero_Normalizado": ["001", "002", "003"],
            "Nota_Data_Emissao": ["2026-03-01", "2026-03-01", "2026-03-01"],
            "Valor_Liquido_Devolucao": [100.0, 200.0, 500.0],
        }
    )

    df_filtrado = aplicar_filtros_devolucao(df_mock)

    assert len(df_filtrado) == 2
    assert "Entrada Devolução" in df_filtrado["Natureza"].values
    assert "Entrada de Devolução" in df_filtrado["Natureza"].values
    assert "Compra de mercadorias" not in df_filtrado["Natureza"].values


def test_natureza_variacao_com_situacao_nao_autorizada_rejeitada() -> None:
    """
    Mesmo com natureza aceita, situação diferente de 'Autorizada' continua rejeitada.
    """
    df_mock = pd.DataFrame(
        {
            "Natureza": ["Entrada Devolução", "Entrada Devolução"],
            "Situação": ["Autorizada", "Registrada"],
            "Nota_Numero_Normalizado": ["001", "002"],
            "Nota_Data_Emissao": ["2026-03-01", "2026-03-01"],
            "Valor_Liquido_Devolucao": [100.0, 200.0],
        }
    )

    df_filtrado = aplicar_filtros_devolucao(df_mock)

    assert len(df_filtrado) == 1
    assert df_filtrado["Situação"].iloc[0] == "Autorizada"
