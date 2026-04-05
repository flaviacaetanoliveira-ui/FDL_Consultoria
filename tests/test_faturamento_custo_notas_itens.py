"""Custo preenchido a partir dos itens da NF quando o SKU do pedido não está na tabela."""
from __future__ import annotations

import shutil
import unittest
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent


class TestFaturamentoCustoNotasItens(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = REPO / "tests" / "_tmp_custo_nf_itens"
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)
        self._tmpdir.mkdir(parents=True)
        self.notas_dir = self._tmpdir / "notas_saida"
        self.notas_dir.mkdir()

        # Duas linhas na mesma NF: 2×INT001 + 1×INT002 → custo 25 se unitários 10 e 5
        csv_body = (
            "Número;Valor total líquido;Número do pedido;Código;Quantidade;Frete\n"
            "42517;1000,00;PMLB1;INT001;2;0\n"
            "42517;1000,00;PMLB1;INT002;1;0\n"
        )
        (self.notas_dir / "nf.csv").write_text(csv_body, encoding="utf-8")

        self.df_custo = pd.DataFrame(
            {
                "Código": ["INT001", "INT002"],
                "PREÇO DE CUSTO com IPI": ["10,00", "5,00"],
            }
        )

    def tearDown(self) -> None:
        if self._tmpdir.exists():
            shutil.rmtree(self._tmpdir, ignore_errors=True)

    def test_compute_custo_total_por_nf(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.join_custo_notas_itens import compute_custo_total_por_nf_desde_itens_notas

        s, meta = compute_custo_total_por_nf_desde_itens_notas(
            self.notas_dir, self.df_custo, org_id="o1", empresa="Acme"
        )
        self.assertIn("42517", s.index.str.strip())
        k = [x for x in s.index if str(x).strip() == "42517"][0]
        self.assertAlmostEqual(float(s.loc[k]), 25.0, places=5)
        self.assertGreaterEqual(meta.get("custo_nf_itens_linhas_com_custo", 0), 2)

    def test_enrich_preenche_unitario_e_flag(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.config import CUSTO_UNITARIO_COL, STATUS_SKU_SEM_CORRESPONDENCIA
        from processing.faturamento.join_custo_notas_itens import FLAG_CUSTO_ORIGEM_NF, enrich_custo_from_notas_itens

        df = pd.DataFrame(
            {
                "Quantidade": [1],
                "Preço de lista": [100.0],
                "Vl_Venda": [100.0],
                "Código": ["MLB_ANUNCIO_X"],
                CUSTO_UNITARIO_COL: [np.nan],
                "Flag_SKU_Sem_Custo": [True],
                "Flag_Produto_Sem_Correspondencia_SKU": [True],
                "Status_Custo": [STATUS_SKU_SEM_CORRESPONDENCIA],
                "Nota_Numero_Normalizado": ["42517"],
                "faturamento_nota_vinculada": [True],
            }
        )
        out, meta = enrich_custo_from_notas_itens(
            df,
            self.df_custo,
            notas_dir=self.notas_dir,
            org_id="o1",
            empresa="Acme",
        )
        self.assertAlmostEqual(float(out[CUSTO_UNITARIO_COL].iloc[0]), 25.0, places=5)
        self.assertFalse(bool(out["Flag_SKU_Sem_Custo"].iloc[0]))
        self.assertTrue(bool(out[FLAG_CUSTO_ORIGEM_NF].iloc[0]))
        self.assertEqual(meta.get("custo_nf_enriquecido_linhas_pedido"), 1)

    def test_custo_ok_pedido_nao_alterado(self) -> None:
        import sys

        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.config import CUSTO_UNITARIO_COL, STATUS_CUSTO_OK
        from processing.faturamento.join_custo_notas_itens import FLAG_CUSTO_ORIGEM_NF, enrich_custo_from_notas_itens

        df = pd.DataFrame(
            {
                "Quantidade": [1],
                "Preço de lista": [100.0],
                "Vl_Venda": [100.0],
                "Código": ["INT001"],
                CUSTO_UNITARIO_COL: [10.0],
                "Flag_SKU_Sem_Custo": [False],
                "Flag_Produto_Sem_Correspondencia_SKU": [False],
                "Status_Custo": [STATUS_CUSTO_OK],
                "Nota_Numero_Normalizado": ["42517"],
                "faturamento_nota_vinculada": [True],
                FLAG_CUSTO_ORIGEM_NF: [False],
            }
        )
        out, _meta = enrich_custo_from_notas_itens(
            df,
            self.df_custo,
            notas_dir=self.notas_dir,
            org_id="o1",
            empresa="Acme",
        )
        self.assertAlmostEqual(float(out[CUSTO_UNITARIO_COL].iloc[0]), 10.0, places=5)
        self.assertFalse(bool(out[FLAG_CUSTO_ORIGEM_NF].iloc[0]))


if __name__ == "__main__":
    unittest.main()
