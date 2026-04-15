"""Leitura wide de Custos.xlsx com cabeçalho deslocado (linhas vazias antes da tabela)."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd

from processing.faturamento.config import (
    CUSTO_COL_VALOR_EAP,
    CUSTO_COL_VALOR_GENERIC,
    CUSTO_COL_VALOR_MEGA,
    CUSTO_COL_VALOR_STAR_GAMA,
    CUSTO_SKU_COL,
    CUSTO_UNITARIO_COL,
)
from processing.faturamento.custo_por_empresa import join_custo_produto_por_empresa
from processing.faturamento.io_custo import load_custo_xlsx


class TestIoCustoLoadWide(unittest.TestCase):
    def test_load_custo_autodetect_wide_shifted_header(self) -> None:
        rows = [
            [None] * 7,
            [
                None,
                "CÓDIGO",
                "PRODUTO",
                "VALOR DE COMPRA",
                "VALOR DE COMPRA MEGA",
                "VALOR COMPRA EAP",
                "VALOR COMPRA STAR/GAMA",
            ],
            [None, "13794", "BALCÃO", "184,48", "168,338", "190,4756", "205"],
        ]
        with tempfile.TemporaryDirectory() as td:
            tmp_path = Path(td)
            p = tmp_path / "Custos.xlsx"
            pd.DataFrame(rows).to_excel(p, sheet_name="Planilha1", header=False, index=False)

            df, meta = load_custo_xlsx(p)
            self.assertEqual(meta["custo_reader"], "header_autodetect_wide")
            self.assertIn(CUSTO_COL_VALOR_MEGA, df.columns)
            self.assertIn(CUSTO_COL_VALOR_EAP, df.columns)
            self.assertIn(CUSTO_COL_VALOR_STAR_GAMA, df.columns)
            self.assertIn(CUSTO_COL_VALOR_GENERIC, df.columns)

            row = df[df[CUSTO_SKU_COL].astype(str) == "13794"].iloc[0]
            self.assertEqual(row[CUSTO_COL_VALOR_GENERIC], "184,48")
            self.assertEqual(row[CUSTO_COL_VALOR_MEGA], "168,338")
            self.assertEqual(row[CUSTO_COL_VALOR_EAP], "190,4756")
            self.assertEqual(row[CUSTO_COL_VALOR_STAR_GAMA], "205")

            df_p = pd.DataFrame({CUSTO_SKU_COL: ["13794"], "Quantidade": [1.0]})
            for emp, exp in (
                ("Mega Fácil", 168.338),
                ("Móveis EAP", 190.4756),
                ("Mega Star", 205.0),
                ("Gama Home", 205.0),
            ):
                with self.subTest(empresa=emp):
                    out = join_custo_produto_por_empresa(df_p, df, emp)
                    self.assertAlmostEqual(float(out.iloc[0][CUSTO_UNITARIO_COL]), exp, places=6)


if __name__ == "__main__":
    unittest.main()
