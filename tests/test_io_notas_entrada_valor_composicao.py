"""Composição do valor de NF de entrada (Bling) para devoluções."""

from __future__ import annotations

import pandas as pd
import pytest

from processing.faturamento.io_notas_entrada import series_valor_liquido_nota_entrada_bling


def test_nf_exemplo_valor_total_mais_frete_sem_aux_restores_total() -> None:
    """NF 3892: Valor total 349,99 + Frete 58 ≈ total fiscal da nota."""
    df = pd.DataFrame(
        {
            "Número": ["3892"],
            "Valor total": ["349,99"],
            "Frete": ["58,00"],
            "Data de emissão": ["01/04/2026"],
            "Natureza": ["Devolução"],
            "Situação": ["Autorizada"],
        }
    )
    s = series_valor_liquido_nota_entrada_bling(df)
    assert len(s) == 1
    assert float(s.iloc[0]) == pytest.approx(407.99, abs=0.02)


def test_sem_colunas_auxiliares_usa_valor_total_ou_liquido() -> None:
    df = pd.DataFrame({"Valor total": ["100,50"], "x": [1]})
    s = series_valor_liquido_nota_entrada_bling(df)
    assert float(s.iloc[0]) == pytest.approx(100.50)


def test_prioriza_valor_total_liquido_quando_sem_aux() -> None:
    df = pd.DataFrame({"Valor total líquido": ["200"], "Valor total": ["150"], "x": [1]})
    s = series_valor_liquido_nota_entrada_bling(df)
    assert float(s.iloc[0]) == pytest.approx(200.0)


def test_com_aux_usa_valor_total_como_base_nao_liquido() -> None:
    df = pd.DataFrame(
        {
            "Valor total líquido": ["999"],
            "Valor total": ["100"],
            "Frete": ["10"],
        }
    )
    s = series_valor_liquido_nota_entrada_bling(df)
    assert float(s.iloc[0]) == pytest.approx(110.0)


def test_outras_despesas_e_desconto() -> None:
    df = pd.DataFrame(
        {
            "Valor total": ["100"],
            "Frete": ["5"],
            "Outras despesas": ["3"],
            "Desconto": ["8"],
        }
    )
    s = series_valor_liquido_nota_entrada_bling(df)
    assert float(s.iloc[0]) == pytest.approx(100.0)
