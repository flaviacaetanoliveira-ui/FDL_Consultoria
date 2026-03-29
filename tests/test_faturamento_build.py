"""Testes do pipeline de faturamento (fixtures temporárias)."""
from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent


class TestFaturamentoBuild(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = REPO / "tests" / "_tmp_faturamento"
        self._tmpdir.mkdir(parents=True, exist_ok=True)
        self.pedidos_dir = self._tmpdir / "PEDIDOS"
        self.pedidos_dir.mkdir()
        shutil.copy(
            REPO / "tests" / "fixtures" / "faturamento" / "pedidos_min.csv",
            self.pedidos_dir / "pedidos_min.csv",
        )
        custo_path = self._tmpdir / "custo.xlsx"
        df_c = pd.DataFrame(
            {
                "Código": ["SKU-A", "SKU-B"],
                "PREÇO DE CUSTO com IPI": ["30,00", "15,00"],
            }
        )
        with pd.ExcelWriter(custo_path, engine="openpyxl") as xw:
            df_c.to_excel(xw, sheet_name="Planilha1", index=False)
        self.custo_path = custo_path
        self.params_path = self._tmpdir / "faturamento_params.json"
        self.params_path.write_text(
            json.dumps(
                {
                    "aliquota_imposto": 0.1,
                    "aliquota_despesas_fixas": 0.05,
                    "permite_faturamento_sem_nf": True,
                    "pedidos_dir": str(self.pedidos_dir.resolve()),
                    "custo_xlsx": str(self.custo_path.resolve()),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_build_resultado_e_flags(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.build import build_faturamento_dataset

        df, meta = build_faturamento_dataset(self.params_path)
        self.assertEqual(len(df), 2)
        self.assertTrue(bool(df.loc[0, "faturamento_com_nf"]))
        self.assertTrue(bool(df.loc[1, "faturamento_sem_nf"]))
        self.assertEqual(meta["row_count"], 2)


if __name__ == "__main__":
    unittest.main()
