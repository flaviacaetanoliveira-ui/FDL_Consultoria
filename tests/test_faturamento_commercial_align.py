"""Alinhamento comercial ao conjunto base fiscal (N_base) — painel Faturamento & DRE mínimo."""

from __future__ import annotations

import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from faturamento_dre_recorte import _fdl_fr_mask_nf_emissao_no_periodo
from faturamento_dre_recorte_minimo import (
    CommercialCoverageStats,
    build_faturamento_fiscal_base_slice,
    build_nf_panel_aligned_to_fiscal_base,
    compute_commercial_coverage_stats,
    compute_nf_panel_kpis,
)

_PARQUET_FISCAL = Path(__file__).resolve().parent.parent / (
    "data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet"
)
_PARQUET_PANEL = Path(__file__).resolve().parent.parent / (
    "data_products/cliente_2/faturamento/current/dataset_faturamento_nf_panel.parquet"
)


def _panel_parquet_has_frete_v2_columns(path: Path) -> bool:
    """Painéis antigos não têm ``receita_frete_tp`` / ``tarifa_custo_envio`` — exige rematerialização."""
    if not path.is_file():
        return False
    try:
        import pyarrow.parquet as pq
    except ImportError:
        return False
    try:
        names = pq.read_schema(path).names
    except Exception:
        return False
    return "receita_frete_tp" in names and "tarifa_custo_envio" in names


_CLIENTE2_PARQUETS_OK = (
    _PARQUET_FISCAL.is_file()
    and _PARQUET_PANEL.is_file()
    and _panel_parquet_has_frete_v2_columns(_PARQUET_PANEL)
)


def _panel_scope_empresa_emissao_gama_mar2026(panel: pd.DataFrame) -> pd.DataFrame:
    """Espelha empresa + emissão do app, sem plataforma (casefold em empresa)."""
    emp = "Gama Home"
    em_cf = panel["empresa"].fillna("").astype(str).str.strip().str.casefold()
    p2 = panel.loc[em_cf == emp.casefold()].copy()
    d_ini, d_fim = date(2026, 3, 1), date(2026, 3, 31)
    m = _fdl_fr_mask_nf_emissao_no_periodo(p2["Nota_Data_Emissao"], d_ini, d_fim)
    return p2.loc[m].copy()


class TestBuildNfPanelAlignedToFiscalBase(unittest.TestCase):
    def test_fiscal_base_vazio_devolve_vazio(self) -> None:
        panel = pd.DataFrame()
        out = build_nf_panel_aligned_to_fiscal_base(pd.DataFrame(), panel)
        self.assertTrue(out.empty)

    @unittest.skipUnless(
        _CLIENTE2_PARQUETS_OK,
        "sem Parquets cliente_2 ou painel sem receita_frete_tp/tarifa_custo_envio (rematerializar)",
    )
    def test_gama_home_marco_2026_alinha_n_base_e_total_fiscal(self) -> None:
        fiscal = pd.read_parquet(_PARQUET_FISCAL, engine="pyarrow")
        panel = pd.read_parquet(_PARQUET_PANEL, engine="pyarrow")
        d_ini, d_fim = date(2026, 3, 1), date(2026, 3, 31)
        base, st = build_faturamento_fiscal_base_slice(
            fiscal,
            empresas_sel=("Gama Home",),
            nf_d_ini=d_ini,
            nf_d_fim=d_fim,
            ok_nf_dates=True,
        )
        scope = _panel_scope_empresa_emissao_gama_mar2026(panel)
        aligned = build_nf_panel_aligned_to_fiscal_base(base, scope)
        self.assertEqual(len(aligned), 438)
        self.assertEqual(st.n_nf, 438)
        kp = compute_nf_panel_kpis(aligned)
        self.assertAlmostEqual(float(kp["valor_faturado_nf"]), 82347.10, places=2)
        self.assertEqual(int(kp["n_nf"]), 438)

    @unittest.skipUnless(
        _CLIENTE2_PARQUETS_OK,
        "sem Parquets cliente_2 ou painel sem receita_frete_tp/tarifa_custo_envio (rematerializar)",
    )
    def test_cobertura_comercial_gama_home_marco_2026(self) -> None:
        fiscal = pd.read_parquet(_PARQUET_FISCAL, engine="pyarrow")
        panel = pd.read_parquet(_PARQUET_PANEL, engine="pyarrow")
        d_ini, d_fim = date(2026, 3, 1), date(2026, 3, 31)
        base, _ = build_faturamento_fiscal_base_slice(
            fiscal,
            empresas_sel=("Gama Home",),
            nf_d_ini=d_ini,
            nf_d_fim=d_fim,
            ok_nf_dates=True,
        )
        scope = _panel_scope_empresa_emissao_gama_mar2026(panel)
        aligned = build_nf_panel_aligned_to_fiscal_base(base, scope)
        cov = compute_commercial_coverage_stats(aligned)
        self.assertEqual(
            cov,
            CommercialCoverageStats(
                n_total=438,
                n_com_vinculo_pedido_nf=438,
                n_sem_vinculo_ou_so_fiscal=0,
                n_com_venda_lista=438,
                n_sem_resultado=86,
                n_com_resultado_numerico=352,
            ),
        )


if __name__ == "__main__":
    unittest.main()
