"""Fallback «Número» (coluna G pedidos) → NF quando mapa pedido/multiloja falha."""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent


class TestJoinNotasNumeroFallback(unittest.TestCase):
    def test_fallback_usa_numero_quando_pedido_nao_mapeia(self) -> None:
        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.join_notas import enrich_pedidos_com_notas

        tmp = REPO / "tests" / "_tmp_join_notas_fb"
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            nd = tmp / "notas"
            nd.mkdir()
            # Uma NF conhecida; pedido na nota é outro — linha de pedido usa P_ORFAO mas Número = NF
            notas = nd / "n.csv"
            notas.write_text(
                "Número;Valor total líquido;Data de emissão;Número do pedido;Número do pedido multiloja;Situação\n"
                "010500;250,50;15/03/2026;P_NOTAS;MLB999;Autorizada\n",
                encoding="utf-8",
            )
            out = pd.DataFrame(
                {
                    "Número do pedido": ["P_ORFAO"],
                    "Número do pedido multiloja": ["MLB_ORFAO"],
                    "Quantidade": ["1"],
                    "Preço de lista": ["100,00"],
                    "Custo de Frete": ["0,00"],
                    "Número": ["010500"],
                }
            )
            got, meta = enrich_pedidos_com_notas(
                out,
                notas_dir=nd,
                org_id="test_org",
                empresa="Test Co",
            )
            self.assertEqual(int(meta.get("notas_fallback_numero_coluna_linhas", 0)), 1)
            self.assertTrue(bool(got["faturamento_nota_vinculada"].iloc[0]))
            self.assertEqual(str(got["Nota_Numero_Normalizado"].iloc[0]).strip(), "010500")
            self.assertGreater(float(got["Nota_Valor_Liquido_Rateado"].iloc[0]), 0.0)
        finally:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)

    def test_fallback_ignora_traco_e_nf_desconhecida(self) -> None:
        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        from processing.faturamento.join_notas import enrich_pedidos_com_notas

        tmp = REPO / "tests" / "_tmp_join_notas_fb2"
        tmp.mkdir(parents=True, exist_ok=True)
        try:
            nd = tmp / "notas"
            nd.mkdir()
            notas = nd / "n.csv"
            notas.write_text(
                "Número;Valor total líquido;Data de emissão;Número do pedido;Número do pedido multiloja;Situação\n"
                "010500;250,50;15/03/2026;P1;MLB1;Autorizada\n",
                encoding="utf-8",
            )
            out = pd.DataFrame(
                {
                    "Número do pedido": ["PX", "PY"],
                    "Número do pedido multiloja": ["MLX", "MLY"],
                    "Quantidade": ["1", "1"],
                    "Preço de lista": ["10,00", "20,00"],
                    "Custo de Frete": ["0,00", "0,00"],
                    "Número": ["-", "999999"],
                }
            )
            got, meta = enrich_pedidos_com_notas(
                out,
                notas_dir=nd,
                org_id="test_org",
                empresa="Test Co",
            )
            self.assertEqual(int(meta.get("notas_fallback_numero_coluna_linhas", 0)), 0)
            self.assertFalse(bool(got["faturamento_nota_vinculada"].iloc[0]))
            self.assertFalse(bool(got["faturamento_nota_vinculada"].iloc[1]))
        finally:
            import shutil

            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
