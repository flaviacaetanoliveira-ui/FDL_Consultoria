"""Conjunto base fiscal do painel Faturamento & DRE (empresa + emissão + NFs válidas)."""

from __future__ import annotations

import json
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoFiscalBaseStats,
    build_faturamento_fiscal_base_slice,
    enrich_faturamento_fiscal_base_stats,
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
        self.assertAlmostEqual(st2.valor_cancelado, 100.0)

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

    def test_abatimento_devolucoes_base_liquida(self) -> None:
        df_f = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [1000.0],
            }
        )
        df_d = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["D1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-20")],
                "Valor_Liquido_Devolucao": [23.0],
            }
        )
        _, st = build_faturamento_fiscal_base_slice(
            df_f,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
            df_devolucoes=df_d,
        )
        self.assertAlmostEqual(st.valor_liquido_fiscal_sum, 1000.0)
        self.assertAlmostEqual(st.total_devolvido, 23.0)
        self.assertEqual(st.nfs_devolucao, 1)
        self.assertAlmostEqual(st.base_fiscal_liquida, 977.0)

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

    def test_stats_expoe_valor_faturado_nf(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [250.0],
            }
        )
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertGreaterEqual(st.valor_faturado_nf, 0.0)
        self.assertAlmostEqual(st.valor_faturado_nf, st.valor_liquido_fiscal_sum)

    def test_stats_expoe_diferenca_lista_nf(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [100.0],
            }
        )
        df_nf = pd.DataFrame({"diferenca": [12.5, -2.0]})
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
            imposto_apurado=10.0,
            df_nf_aligned=df_nf,
        )
        self.assertAlmostEqual(st.diferenca_lista_nf, 10.5)

    def test_stats_expoe_valor_cancelado(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1", "o1"],
                "empresa": ["Acme", "Acme"],
                "Nota_Numero_Normalizado": ["X", "X"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15"), pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Cancelada", "Autorizada"],
                "Valor_Liquido_NF": [40.0, 60.0],
            }
        )
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
        )
        self.assertAlmostEqual(st.valor_cancelado, 40.0)

    def test_stats_expoe_aliquota_efetiva(self) -> None:
        df = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [1000.0],
            }
        )
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
            imposto_apurado=60.0,
            aliquota_configurada_pct=11.0,
        )
        self.assertGreater(st.base_fiscal_liquida, 0)
        esperado_pct = (st.imposto / st.base_fiscal_liquida) * 100.0
        self.assertAlmostEqual(st.aliquota_efetiva_pct, esperado_pct, places=4)

    def test_stats_aliquota_configurada_vem_de_params(self) -> None:
        params_path = Path(__file__).resolve().parent.parent / "ops/faturamento_params_cliente_2_gama_star_eap.json"
        self.assertTrue(params_path.is_file(), msg=f"falta {params_path}")
        meta = json.loads(params_path.read_text(encoding="utf-8"))
        cfg_pct = float(meta["aliquota_imposto"]) * 100.0
        df = pd.DataFrame(
            {
                "org_id": ["o1"],
                "empresa": ["Acme"],
                "Nota_Numero_Normalizado": ["N1"],
                "Nota_Data_Emissao": [pd.Timestamp("2026-03-15")],
                "Nota_Situacao": ["Autorizada"],
                "Valor_Liquido_NF": [100.0],
            }
        )
        _, st = build_faturamento_fiscal_base_slice(
            df,
            empresas_sel=("Acme",),
            nf_d_ini=date(2026, 3, 1),
            nf_d_fim=date(2026, 3, 31),
            ok_nf_dates=True,
            imposto_apurado=11.0,
            aliquota_configurada_pct=cfg_pct,
        )
        self.assertGreater(st.aliquota_configurada_pct, 0)
        self.assertAlmostEqual(st.aliquota_configurada_pct, cfg_pct)

    def test_campos_novos_com_default_zero_nao_quebram_construcao_antiga(self) -> None:
        st = FaturamentoFiscalBaseStats(3, 99.5)
        self.assertEqual(st.valor_faturado_nf, 0.0)
        self.assertEqual(st.valor_cancelado, 0.0)
        self.assertEqual(st.diferenca_lista_nf, 0.0)
        self.assertEqual(st.aliquota_efetiva_pct, 0.0)
        self.assertEqual(st.aliquota_configurada_pct, 0.0)
        self.assertEqual(st.imposto, 0.0)

    def test_enrich_preserva_campos_base(self) -> None:
        base = FaturamentoFiscalBaseStats(
            n_nf=2,
            valor_liquido_fiscal_sum=200.0,
            base_fiscal_liquida=180.0,
            valor_faturado_nf=200.0,
        )
        out = enrich_faturamento_fiscal_base_stats(
            base,
            imposto_apurado=18.0,
            df_nf_aligned=None,
            aliquota_configurada_pct=10.0,
        )
        self.assertAlmostEqual(out.imposto, 18.0)
        self.assertAlmostEqual(out.aliquota_efetiva_pct, 10.0)


if __name__ == "__main__":
    unittest.main()
