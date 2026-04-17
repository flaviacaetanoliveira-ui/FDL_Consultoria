"""Conjunto base fiscal do painel Faturamento & DRE (empresa + emissão + NFs válidas)."""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoFiscalBaseStats,
    build_faturamento_fiscal_base_slice,
)

_PARQUET_FISCAL = Path(__file__).resolve().parent.parent / (
    "data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet"
)


class TestBuildFaturamentoFiscalBaseSlice(unittest.TestCase):
    def test_vazio_sem_colunas_obrigatorias(self) -> None:
        df = pd.DataFrame({"x": [1]})
        out, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=(),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertTrue(out.empty)
        self.assertEqual(st, FaturamentoFiscalBaseStats(0, 0.0))

    def test_filtra_cancelada_e_agrega_nf(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1", "o1"],
                "empresa": ["Acme", "Acme"],
                "Nota_Numero_Normalizado": ["001", "001"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15"), pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada", "Autorizada"],
                "Valor_Liquido_NF": [100.0, 50.0],
            }
        )
        out, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertEqual(st.n_nf, 1)
        self.assertAlmostEqual(st.valor_liquido_fiscal_sum, 150.0)
        self.assertEqual(len(out), 1)

        df_bad = df.copy()
        df_bad.loc[0, "Nota_Situacao"] = "Cancelada"
        _, st2 = build_faturamento_fiscal_base_slice(
            df_bad,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertEqual(st2.n_nf, 1)
        self.assertAlmostEqual(st2.valor_liquido_fiscal_sum, 50.0)

    def test_situacoes_sel_restringe_base(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1", "o1"],
                "empresa": ["Acme", "Acme"],
                "Nota_Numero_Normalizado": ["A", "B"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15"), pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada", "Outra"],
                "Valor_Liquido_NF": [100.0, 200.0],
            }
        )
        out_all, st_all = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertEqual(st_all.n_nf, 2)
        out_f, st_f = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
            situacoes_sel=("autorizada",),
        )
        self.assertEqual(st_f.n_nf, 1)
        self.assertAlmostEqual(st_f.valor_liquido_fiscal_sum, 100.0)
        self.assertEqual(len(out_f), 1)

    def test_nao_depends_de_parametros_comerciais_inexistentes(self) -> None:
        """O slice só recebe empresa + datas; não há produto/resultado na API."""
        df = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["X"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-10")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [10.0],
            }
        )
        _, a = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("X",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        _, b = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("X",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertEqual(a, b)

    @unittest.skipUnless(_PARQUET_FISCAL.is_file(), f"sem {_PARQUET_FISCAL.name}")
    def test_gama_home_marco_2026_conferencia_bling(self) -> None:
        df = pd.read_parquet(_PARQUET_FISCAL, engine="pyarrow")
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Gama Home",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertEqual(st.n_nf, 437)
        self.assertAlmostEqual(st.valor_liquido_fiscal_sum, 82337.10, places=2)


if __name__ == "__main__":
    unittest.main()
