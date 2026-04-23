"""Regressão: Painel de Saúde (topo) usa a mesma fonte que ``compute_resultado_gerencial_kpis``."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.components.health_score import (
    build_health_panel_top_kpis,
    calcular_health_score,
    slice_linhas_nf_periodo,
)
from processing.faturamento.config import STATUS_CUSTO_OK
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    build_resultado_gerencial_slice,
    compute_resultado_gerencial_kpis,
)


def _row(
    *,
    data: str,
    empresa: str,
    org_id: str,
    pedido: str,
    plataforma: str,
    valor_total: float,
    comissao: float,
    frete_plat: float,
    cmv: float,
    resultado: float,
    frete_tp: float = 0.0,
    desp_fixa: float = 0.0,
    ads: float = 0.0,
) -> dict:
    r = {
        "Data": data,
        "empresa": empresa,
        "org_id": org_id,
        "Número do pedido": pedido,
        "Nome da plataforma": plataforma,
        "Valor total": valor_total,
        "Taxa de Comissão": comissao,
        "Frete_Plataforma": frete_plat,
        "Custo_Produto_Total": cmv,
        "Resultado": resultado,
        "Frete transportadora própria": frete_tp,
        "Despesas Fixas": desp_fixa,
        "custo_ads": ads,
    }
    assert REQUIRED_LINE_COLUMNS.issubset(r.keys())
    return r


def test_painel_saude_consome_mesma_fonte_de_kpis() -> None:
    """
    Garante que os KPIs de topo do Painel de Saúde batem ao centavo com compute_resultado_gerencial_kpis.
    """
    df = pd.DataFrame(
        [
            _row(
                data="15/03/2026",
                empresa="Acme",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=10_000.0,
                comissao=800.0,
                frete_plat=200.0,
                cmv=4_000.0,
                resultado=5_000.0,
                frete_tp=150.0,
                desp_fixa=300.0,
                ads=50.0,
            ),
        ]
    )
    df["SKU_Normalizado"] = "SKU-A"
    df["Quantidade"] = 100.0
    df["Status_Custo"] = STATUS_CUSTO_OK
    df["Vl_Venda"] = df["Valor total"]

    d_ini = date(2026, 3, 1)
    d_fim = date(2026, 3, 31)

    slice_rg = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=d_ini,
        data_venda_fim=d_fim,
    )
    fiscal_imposto_valor = 420.0
    kpis = compute_resultado_gerencial_kpis(slice_rg, fiscal_imposto_valor=fiscal_imposto_valor)

    painel = build_health_panel_top_kpis(kpis)
    assert abs(painel.resultado - float(kpis["resultado"])) < 0.01
    assert abs(painel.venda_base - float(kpis["valor_venda_lista"])) < 0.01
    assert abs(painel.margem - float(kpis["margem"])) < 1e-6

    sl = slice_linhas_nf_periodo(
        df,
        d_ini=d_ini,
        d_fim=d_fim,
        empresas_sel=(),
        coluna_temporal="Data",
    )
    health = calcular_health_score(
        sl,
        "o1",
        2026,
        3,
        kpis_gerenciais=kpis,
        cmv_total_gerencial=float(slice_rg.stats.cmv_total),
    )
    assert abs(health.resultado - float(kpis["resultado"])) < 0.01
    assert abs(health.receita - float(kpis["valor_venda_lista"])) < 0.01
    assert abs(health.margem_pct / 100.0 - float(kpis["margem"])) < 1e-6
    cmv = float(slice_rg.stats.cmv_total)
    receita = float(kpis["valor_venda_lista"])
    esperado_custo_pct = (cmv / receita * 100.0) if receita > 0 else 0.0
    assert health.custo_pct == pytest.approx(esperado_custo_pct, rel=1e-9)
