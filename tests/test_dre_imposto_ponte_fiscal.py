"""Regressão: imposto na DRE gerencial e métrica da Apuração Fiscal usam a mesma ponte fiscal."""

from __future__ import annotations

from faturamento_dre_recorte_minimo import (
    FaturamentoFiscalBaseStats,
    dre_imposto_para_linha_dre_gerencial,
)


def test_dre_imposto_sem_parquet_retorna_soma_comercial_do_kp() -> None:
    kp = {"imposto": 123.45}
    got = dre_imposto_para_linha_dre_gerencial(
        kp,
        fiscal_base_stats=FaturamentoFiscalBaseStats(n_nf=1, valor_liquido_fiscal_sum=1000.0, base_fiscal_liquida=800.0),
        aplicar_ponte_base_liquida=False,
    )
    assert got == 123.45


def test_dre_imposto_ponte_usa_base_fiscal_liquida_vezes_taxa_efetiva() -> None:
    """Taxa = imposto_comercial / valor_liquido_fiscal_sum; linha DRE = base_fiscal_liquida × taxa."""
    kp = {"imposto": 100.0}
    stats = FaturamentoFiscalBaseStats(
        n_nf=5,
        valor_liquido_fiscal_sum=1000.0,
        total_devolvido=200.0,
        nfs_devolucao=1,
        base_fiscal_liquida=800.0,
    )
    got = dre_imposto_para_linha_dre_gerencial(
        kp,
        fiscal_base_stats=stats,
        aplicar_ponte_base_liquida=True,
    )
    rate = 100.0 / 1000.0
    assert got == 800.0 * rate == 80.0


def test_dre_imposto_vfscal_quase_zero_nao_escala() -> None:
    kp = {"imposto": 50.0}
    stats = FaturamentoFiscalBaseStats(
        n_nf=0,
        valor_liquido_fiscal_sum=0.0,
        base_fiscal_liquida=0.0,
    )
    got = dre_imposto_para_linha_dre_gerencial(
        kp,
        fiscal_base_stats=stats,
        aplicar_ponte_base_liquida=True,
    )
    assert got == 50.0


def test_apuracao_e_dre_mesmo_kp_mesmo_stats_mesmo_imposto() -> None:
    """Com os mesmos inputs, KPI Apuração e linha DRE devem coincidir (função única)."""
    kp = {"imposto": 333.33}
    stats = FaturamentoFiscalBaseStats(
        n_nf=10,
        valor_liquido_fiscal_sum=500_000.0,
        base_fiscal_liquida=480_000.0,
    )
    d1 = dre_imposto_para_linha_dre_gerencial(
        kp,
        fiscal_base_stats=stats,
        aplicar_ponte_base_liquida=True,
    )
    d2 = dre_imposto_para_linha_dre_gerencial(
        kp,
        fiscal_base_stats=stats,
        aplicar_ponte_base_liquida=True,
    )
    assert d1 == d2
    assert abs(d1 - (480_000.0 * (333.33 / 500_000.0))) < 1e-9
