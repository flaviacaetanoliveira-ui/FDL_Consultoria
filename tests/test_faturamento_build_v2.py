"""Build faturamento schema_version 2 (multi-empresa, custo compartilhado)."""
from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent


class TestFaturamentoBuildV2(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = REPO / "tests" / "_tmp_faturamento_v2"
        self._tmpdir.mkdir(parents=True, exist_ok=True)
        root = self._tmpdir / "Cliente_X"
        root.mkdir()
        (root / "Esquilo").mkdir()
        (root / "Wood").mkdir()
        shutil.copy(
            REPO / "tests" / "fixtures" / "faturamento" / "pedidos_min.csv",
            root / "Esquilo" / "a.csv",
        )
        shutil.copy(
            REPO / "tests" / "fixtures" / "faturamento" / "pedidos_min.csv",
            root / "Wood" / "b.csv",
        )
        custo_path = root / "custo.xlsx"
        df_c = pd.DataFrame(
            {
                "Código": ["SKU-A", "SKU-B"],
                "PREÇO DE CUSTO com IPI": ["30,00", "15,00"],
            }
        )
        with pd.ExcelWriter(custo_path, engine="openpyxl") as xw:
            df_c.to_excel(xw, sheet_name="Planilha1", index=False)

        self.params_path = self._tmpdir / "faturamento_params.json"
        self.params_path.write_text(
            json.dumps(
                {
                    "schema_version": 2,
                    "cliente_root": str(root.resolve()),
                    "cliente_slug": "cliente_test_faturamento_v2",
                    "custo_xlsx": "custo.xlsx",
                    "aliquota_imposto": 0.1,
                    "aliquota_despesas_fixas": 0.05,
                    "permite_faturamento_sem_nf": True,
                    "coluna_base_imposto": "Valor total",
                    "empresas": [
                        {
                            "org_id": "esquilo",
                            "empresa": "Esquilo",
                            "pedidos_dir": "Esquilo",
                        },
                        {
                            "org_id": "wood",
                            "empresa": "Wood",
                            "pedidos_dir": "Wood",
                            "permite_faturamento_sem_nf": False,
                        },
                    ],
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_build_concat_e_identidade(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.build import build_faturamento_dataset
        from processing.faturamento.params import peek_faturamento_schema_version, read_cliente_slug_v2

        self.assertEqual(peek_faturamento_schema_version(self.params_path), 2)
        self.assertEqual(read_cliente_slug_v2(self.params_path), "cliente_test_faturamento_v2")

        df, meta = build_faturamento_dataset(self.params_path)
        self.assertEqual(len(df), 4)
        self.assertEqual(meta.get("schema_version"), 2)
        self.assertEqual(set(df["org_id"].unique()), {"esquilo", "wood"})
        self.assertEqual(df["cliente_slug"].iloc[0], "cliente_test_faturamento_v2")
        esq_sem = df.loc[df["org_id"].eq("esquilo") & df["Código"].eq("SKU-B"), "faturamento_sem_nf"].iloc[0]
        self.assertTrue(bool(esq_sem))
        wood_sem = df.loc[df["org_id"].eq("wood") & df["Código"].eq("SKU-B"), "faturamento_sem_nf"].iloc[0]
        self.assertFalse(bool(wood_sem))
        self.assertTrue(meta.get("coluna_base_imposto_resolvida") == "Valor total")


if __name__ == "__main__":
    unittest.main()
