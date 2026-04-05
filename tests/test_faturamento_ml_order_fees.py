"""Rateio de comissão e frete duplicados por item no mesmo pedido multiloja ML."""
from __future__ import annotations

import unittest

import pandas as pd

REPO = __import__("pathlib").Path(__file__).resolve().parent.parent


class TestMlOrderFees(unittest.TestCase):
    def test_rateia_comissao_e_frete_proporcional_vl_lista(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.ml_order_fees import (
            COL_CF,
            COL_TC,
            FLAG_RATEIO,
            allocate_multiloja_order_level_fees,
        )

        df = pd.DataFrame(
            {
                "org_id": ["e", "e"],
                "empresa": ["Esquilo", "Esquilo"],
                "Número do pedido multiloja": ["2000015745230170", "2000015745230170"],
                "Quantidade": [1, 1],
                "Preço de lista": [300.0, 342.75],
                "Taxa de Comissão": [109.27, 109.27],
                "Custo de Frete": [91.15, 91.15],
            }
        )
        out, meta = allocate_multiloja_order_level_fees(df)
        self.assertEqual(meta.get("ml_fee_rateio_grupos_comissao"), 1)
        self.assertEqual(meta.get("ml_fee_rateio_grupos_frete"), 1)
        self.assertEqual(meta.get("ml_fee_rateio_linhas_tocadas"), 2)
        s_tc = float(out[COL_TC].sum())
        s_cf = float(out[COL_CF].sum())
        self.assertAlmostEqual(s_tc, 109.27, places=2)
        self.assertAlmostEqual(s_cf, 91.15, places=2)
        self.assertTrue(bool(out[FLAG_RATEIO].all()))
        # 300 / 642.75 * 109.27
        self.assertAlmostEqual(float(out[COL_TC].iloc[0]), 300.0 / 642.75 * 109.27, places=2)
        self.assertAlmostEqual(float(out[COL_TC].iloc[1]), 342.75 / 642.75 * 109.27, places=2)

    def test_uma_linha_nao_altera(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.ml_order_fees import COL_TC, FLAG_RATEIO, allocate_multiloja_order_level_fees

        df = pd.DataFrame(
            {
                "org_id": ["e"],
                "empresa": ["Esquilo"],
                "Número do pedido multiloja": ["MLB1"],
                "Quantidade": [1],
                "Preço de lista": [100.0],
                "Taxa de Comissão": [10.0],
                "Custo de Frete": [5.0],
            }
        )
        out, meta = allocate_multiloja_order_level_fees(df)
        self.assertEqual(meta.get("ml_fee_rateio_grupos_comissao"), 0)
        self.assertAlmostEqual(float(out[COL_TC].iloc[0]), 10.0)
        self.assertFalse(bool(out[FLAG_RATEIO].iloc[0]))

    def test_comissao_diferente_entre_linhas_nao_rateia_comissao(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.ml_order_fees import COL_TC, allocate_multiloja_order_level_fees

        df = pd.DataFrame(
            {
                "org_id": ["e", "e"],
                "empresa": ["Esquilo", "Esquilo"],
                "Número do pedido multiloja": ["MLB1", "MLB1"],
                "Quantidade": [1, 1],
                "Preço de lista": [50.0, 50.0],
                "Taxa de Comissão": [10.0, 20.0],
                "Custo de Frete": [8.0, 8.0],
            }
        )
        out, meta = allocate_multiloja_order_level_fees(df)
        self.assertEqual(meta.get("ml_fee_rateio_grupos_comissao"), 0)
        self.assertEqual(meta.get("ml_fee_rateio_grupos_frete"), 1)
        self.assertAlmostEqual(float(out[COL_TC].iloc[0]), 10.0)
        self.assertAlmostEqual(float(out[COL_TC].iloc[1]), 20.0)
        self.assertAlmostEqual(float(out["Custo de Frete"].sum()), 8.0, places=5)


if __name__ == "__main__":
    unittest.main()
