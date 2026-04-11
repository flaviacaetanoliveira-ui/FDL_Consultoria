"""
PR1 Faturamento & DRE: cards/DRE/tabela devem usar o mesmo ``compute_nf_panel_kpis(df_nf_panel)``.

Invariante de dados: filtrar o painel NF (ex.: por plataforma) altera os totais da mesma forma
que seria esperado ao somar só as linhas visíveis na tabela.
"""

from __future__ import annotations

import unittest

import pandas as pd

from faturamento_dre_recorte_minimo import compute_nf_panel_kpis


def _minimal_nf_panel_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "valor_venda": [100.0, 200.0],
            "valor_faturado_nf": [95.0, 210.0],
            "diferenca": [5.0, -10.0],
            "comissao": [10.0, 20.0],
            "custo_produto": [30.0, 40.0],
            "frete": [5.0, 5.0],
            "imposto": [2.0, 4.0],
            "despesa_fixa": [5.0, 10.0],
            "custo_ads_variavel": [3.5, 7.0],
            "custo_ads_fixo": [2.0, 2.0],
            "custo_ads": [5.5, 9.0],
            "resultado": [44.5, 110.0],
            "plataforma": ["Shopee", "Mercado Livre"],
        }
    )


class TestPr1KpisRecorteUnificado(unittest.TestCase):
    def test_filtrar_plataforma_reduz_n_nf_e_somas(self) -> None:
        df = _minimal_nf_panel_df()
        full = compute_nf_panel_kpis(df)
        shopee = df[df["plataforma"].astype(str).eq("Shopee")]
        kp_s = compute_nf_panel_kpis(shopee)
        self.assertEqual(full["n_nf"], 2)
        self.assertEqual(kp_s["n_nf"], 1)
        self.assertAlmostEqual(float(kp_s["valor_venda"]), 100.0)
        self.assertAlmostEqual(float(kp_s["valor_faturado_nf"]), 95.0)
        self.assertAlmostEqual(float(kp_s["diferenca"]), 5.0)

    def test_kp_subset_igual_soma_manual_valor_venda(self) -> None:
        df = _minimal_nf_panel_df()
        one = df.iloc[[1]].copy()
        kp_one = compute_nf_panel_kpis(one)
        self.assertEqual(kp_one["n_nf"], 1)
        self.assertAlmostEqual(float(kp_one["valor_venda"]), 200.0)


if __name__ == "__main__":
    unittest.main()
