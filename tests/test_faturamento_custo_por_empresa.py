"""Coluna de custo na planilha wide conforme a empresa (SKU = ``Código``)."""

from __future__ import annotations

import unittest

import pandas as pd

from processing.faturamento.config import (
    CUSTO_COL_VALOR_EAP,
    CUSTO_COL_VALOR_GENERIC,
    CUSTO_COL_VALOR_MEGA,
    CUSTO_COL_VALOR_STAR_GAMA,
    CUSTO_SKU_COL,
    CUSTO_UNITARIO_COL,
    SKU_NORMALIZADO_COL,
)
from processing.faturamento.custo_por_empresa import join_custo_produto_por_empresa, resolve_custo_coluna_preco_nome
from processing.faturamento.join_custo import join_custo_produto


def _wide_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            CUSTO_SKU_COL: ["SKU1"],
            CUSTO_COL_VALOR_MEGA: ["10,00"],
            CUSTO_COL_VALOR_EAP: ["20,00"],
            CUSTO_COL_VALOR_STAR_GAMA: ["30,00"],
        }
    )


def test_resolve_coluna_mega_facil() -> None:
    assert resolve_custo_coluna_preco_nome(_wide_df(), "Mega Fácil") == CUSTO_COL_VALOR_MEGA


def test_resolve_coluna_moveis_eap() -> None:
    assert resolve_custo_coluna_preco_nome(_wide_df(), "Móveis EAP") == CUSTO_COL_VALOR_EAP


def test_resolve_coluna_mega_star_e_gama_home() -> None:
    assert resolve_custo_coluna_preco_nome(_wide_df(), "Mega Star") == CUSTO_COL_VALOR_STAR_GAMA
    assert resolve_custo_coluna_preco_nome(_wide_df(), "Gama Home") == CUSTO_COL_VALOR_STAR_GAMA


def test_join_custo_produto_escolhe_coluna_por_empresa() -> None:
    df_c = _wide_df()
    df_p = pd.DataFrame({CUSTO_SKU_COL: ["SKU1"], "Quantidade": [1.0]})
    for emp, exp in (
        ("Mega Fácil", 10.0),
        ("Móveis EAP", 20.0),
        ("Mega Star", 30.0),
        ("Gama Home", 30.0),
    ):
        out = join_custo_produto(df_p, df_c, empresa=emp)
        assert abs(float(out.iloc[0][CUSTO_UNITARIO_COL]) - exp) < 1e-6, emp


def test_join_custo_produto_wrapper_aceita_empresa_kw() -> None:
    out = join_custo_produto(
        pd.DataFrame({CUSTO_SKU_COL: ["SKU1"], "Quantidade": [1.0]}),
        _wide_df(),
        empresa="Mega Fácil",
    )
    assert abs(float(out.iloc[0][CUSTO_UNITARIO_COL]) - 10.0) < 1e-6


def test_join_custo_produto_por_empresa_alias() -> None:
    out = join_custo_produto_por_empresa(
        pd.DataFrame({CUSTO_SKU_COL: ["SKU1"], "Quantidade": [1.0]}),
        _wide_df(),
        "Móveis EAP",
    )
    assert abs(float(out.iloc[0][CUSTO_UNITARIO_COL]) - 20.0) < 1e-6


class TestJoinFallbackF(unittest.TestCase):
    def test_join_custo_fallback_prefix_f_numeric(self) -> None:
        """Pedido só numérico casa com planilha ``F`` + dígitos (Gama Home / frigideiras)."""
        df_c = pd.DataFrame(
            {
                CUSTO_SKU_COL: ["F6513", "F1642"],
                CUSTO_COL_VALOR_STAR_GAMA: ["11,00", "22,00"],
            }
        )
        df_p = pd.DataFrame({CUSTO_SKU_COL: ["6513", "1642"], "Quantidade": [1.0, 1.0]})
        out = join_custo_produto_por_empresa(df_p, df_c, "Gama Home")
        self.assertEqual(out[SKU_NORMALIZADO_COL].tolist(), ["f6513", "f1642"])
        self.assertAlmostEqual(float(out.iloc[0][CUSTO_UNITARIO_COL]), 11.0, places=6)
        self.assertAlmostEqual(float(out.iloc[1][CUSTO_UNITARIO_COL]), 22.0, places=6)


def test_fallback_na_celula_vazia_usa_valor_de_compra_generico() -> None:
    df_c = pd.DataFrame(
        {
            CUSTO_SKU_COL: ["X1"],
            CUSTO_COL_VALOR_MEGA: [""],
            CUSTO_COL_VALOR_GENERIC: ["99,50"],
        }
    )
    df_p = pd.DataFrame({CUSTO_SKU_COL: ["X1"], "Quantidade": [1.0]})
    out = join_custo_produto_por_empresa(df_p, df_c, "Mega Fácil")
    assert abs(float(out.iloc[0][CUSTO_UNITARIO_COL]) - 99.5) < 1e-6
