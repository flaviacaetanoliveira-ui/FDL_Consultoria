"""Build faturamento com notas, params mensais e rateio do valor líquido da NF."""
from __future__ import annotations

import json
import shutil
import unittest
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent


def _write_cenario_ok(base: Path) -> Path:
    root = base / "ok"
    root.mkdir(parents=True)
    (root / "LojaA").mkdir()
    (root / "notas_saida").mkdir()

    pd.DataFrame(
        {
            "Quantidade": [1, 1],
            "Preço de lista": ["100,00", "50,00"],
            "Valor total": ["100,00", "50,00"],
            "Custo de Frete": ["0", "0"],
            "Taxa de Comissão": ["0", "0"],
            "Situação": ["Atendido", "Atendido"],
            "Existe Nota Fiscal gerada": ["Sim", "Sim"],
            "Número da nota": ["", ""],
            "Código": ["SKU-A", "SKU-B"],
            "Nome da plataforma": ["ML", "ML"],
            "Número do pedido": ["P1", "P2"],
            "Número do pedido multiloja": ["MLB001", "MLB001"],
            "Data": ["15/01/2025", "15/01/2025"],
        }
    ).to_csv(root / "LojaA" / "ped.csv", sep=";", index=False, encoding="utf-8")

    df_c = pd.DataFrame(
        {
            "Código": ["SKU-A", "SKU-B"],
            "PREÇO DE CUSTO com IPI": ["10,00", "5,00"],
        }
    )
    with pd.ExcelWriter(root / "custo.xlsx", engine="openpyxl") as xw:
        df_c.to_excel(xw, sheet_name="Planilha1", index=False)

    pd.DataFrame(
        {
            "Número": ["NF900"],
            "Número do pedido multiloja": ["MLB001"],
            "Valor total líquido": ["300,00"],
            "Data de emissão": ["15/01/2025 10:00"],
            "Situação": ["Autorizada"],
        }
    ).to_csv(root / "notas_saida" / "n.csv", sep=";", index=False, encoding="utf-8")

    pd.DataFrame(
        {
            "org_id": ["lojaa"],
            "competencia": ["2025-01"],
            "aliquota_imposto": [0.1],
            "despesa_fixa": [0.05],
        }
    ).to_csv(root / "params_mensais.csv", sep=";", index=False, encoding="utf-8")

    params_path = base / "params_ok.json"
    params_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "cliente_root": str(root.resolve()),
                "cliente_slug": "cliente_test_regras",
                "custo_xlsx": "custo.xlsx",
                "aliquota_imposto": 0.99,
                "aliquota_despesas_fixas": 0.99,
                "permite_faturamento_sem_nf": True,
                "coluna_base_imposto": "Valor total",
                "params_mensais": "params_mensais.csv",
                "notas_saida_dir": "notas_saida",
                "empresas": [
                    {"org_id": "lojaa", "empresa": "Loja A", "pedidos_dir": "LojaA"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return params_path


def _write_cenario_vl_zero(base: Path) -> Path:
    root = base / "bad"
    root.mkdir(parents=True)
    (root / "LojaA").mkdir()
    (root / "notas_saida").mkdir()

    pd.DataFrame(
        {
            "Quantidade": [1, 1],
            "Preço de lista": ["0,00", "0,00"],
            "Valor total": ["0,00", "0,00"],
            "Custo de Frete": ["0", "0"],
            "Taxa de Comissão": ["0", "0"],
            "Situação": ["Atendido", "Atendido"],
            "Existe Nota Fiscal gerada": ["Sim", "Sim"],
            "Número da nota": ["", ""],
            "Código": ["SKU-A", "SKU-B"],
            "Nome da plataforma": ["ML", "ML"],
            "Número do pedido": ["PZ1", "PZ2"],
            "Número do pedido multiloja": ["MLBZ", "MLBZ"],
            "Data": ["15/01/2025", "15/01/2025"],
        }
    ).to_csv(root / "LojaA" / "ped.csv", sep=";", index=False, encoding="utf-8")

    df_c = pd.DataFrame(
        {
            "Código": ["SKU-A", "SKU-B"],
            "PREÇO DE CUSTO com IPI": ["1,00", "1,00"],
        }
    )
    with pd.ExcelWriter(root / "custo.xlsx", engine="openpyxl") as xw:
        df_c.to_excel(xw, sheet_name="Planilha1", index=False)

    pd.DataFrame(
        {
            "Número": ["NFZ"],
            "Número do pedido multiloja": ["MLBZ"],
            "Valor total líquido": ["10,00"],
            "Data de emissão": ["15/01/2025"],
            "Situação": ["Autorizada"],
        }
    ).to_csv(root / "notas_saida" / "n.csv", sep=";", index=False, encoding="utf-8")

    pd.DataFrame(
        {
            "org_id": ["lojaa"],
            "competencia": ["2025-01"],
            "aliquota_imposto": [0.1],
            "despesa_fixa": [0.05],
        }
    ).to_csv(root / "params_mensais.csv", sep=";", index=False, encoding="utf-8")

    params_path = base / "params_bad.json"
    params_path.write_text(
        json.dumps(
            {
                "schema_version": 2,
                "cliente_root": str(root.resolve()),
                "cliente_slug": "cliente_test_regras_bad",
                "custo_xlsx": "custo.xlsx",
                "aliquota_imposto": 0.1,
                "aliquota_despesas_fixas": 0.05,
                "permite_faturamento_sem_nf": True,
                "coluna_base_imposto": "Valor total",
                "params_mensais": "params_mensais.csv",
                "notas_saida_dir": "notas_saida",
                "empresas": [
                    {"org_id": "lojaa", "empresa": "Loja A", "pedidos_dir": "LojaA"},
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return params_path


class TestFaturamentoRegrasFechadas(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = REPO / "tests" / "_tmp_faturamento_regras"
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir.mkdir(parents=True)

    def tearDown(self) -> None:
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_rateio_e_imposto_por_linha(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.build import build_faturamento_dataset

        p = _write_cenario_ok(self._tmpdir)
        df, meta = build_faturamento_dataset(p)
        self.assertEqual(len(df), 2)
        self.assertEqual(meta.get("pipeline_revision"), "faturamento-v3")
        self.assertAlmostEqual(float(df["Nota_Valor_Liquido_Total"].iloc[0]), 300.0, places=3)
        s_rate = float(df["Nota_Valor_Liquido_Rateado"].sum())
        self.assertAlmostEqual(s_rate, 300.0, places=2)
        self.assertAlmostEqual(float(df.loc[df.index[0], "Nota_Valor_Liquido_Rateado"]), 200.0, places=2)
        self.assertAlmostEqual(float(df.loc[df.index[1], "Nota_Valor_Liquido_Rateado"]), 100.0, places=2)
        self.assertAlmostEqual(float(df["Imposto"].sum()), 30.0, places=2)
        self.assertAlmostEqual(float(df["Despesas Fixas"].sum()), 7.5, places=2)
        self.assertIn("Vl_Venda", df.columns)
        self.assertIn("Frete_Plataforma", df.columns)
        self.assertAlmostEqual(float(df["Receita_Bruta"].sum()), 150.0, places=2)

    def test_rateio_vl_venda_zero_erro(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.build import build_faturamento_dataset
        from processing.faturamento.validate import FaturamentoValidationError

        p = _write_cenario_vl_zero(self._tmpdir)
        with self.assertRaises(FaturamentoValidationError) as ctx:
            build_faturamento_dataset(p)
        self.assertIn("Vl_Venda", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
