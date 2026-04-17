"""Dedupe da tabela de custo por chave SKU normalizada (variantes na planilha)."""

from __future__ import annotations

import unittest

import pandas as pd

from processing.faturamento.config import CUSTO_COL_VALOR_STAR_GAMA, CUSTO_SKU_COL
from processing.faturamento.io_custo import dedupe_custo_dataframe_by_normalized_sku


class TestIoCustoDedupeNormalized(unittest.TestCase):
    def test_dedupe_collapsed_variant_rows(self) -> None:
        df = pd.DataFrame(
            {
                CUSTO_SKU_COL: ["170555", "170555-1", "BELA4P1"],
                CUSTO_COL_VALOR_STAR_GAMA: ["100", "200", "50"],
            }
        )
        out, dropped = dedupe_custo_dataframe_by_normalized_sku(df)
        self.assertEqual(dropped, 1)
        self.assertEqual(len(out), 2)
        # Primeira ocorrência de 170555 conservada
        r170 = out[out[CUSTO_SKU_COL].astype(str) == "170555"]
        self.assertEqual(len(r170), 1)
        self.assertEqual(r170.iloc[0][CUSTO_COL_VALOR_STAR_GAMA], "100")
        bela = out[out[CUSTO_SKU_COL].astype(str) == "BELA4P1"]
        self.assertEqual(len(bela), 1)


if __name__ == "__main__":
    unittest.main()
