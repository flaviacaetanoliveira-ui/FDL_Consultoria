"""Regressão: margem operacional vs líquida (Resultado Gerencial, Opção B)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.components.health_score import compute_skus_em_risco, compute_skus_risco_duas_visoes
from app.components.tabela_pedidos_gerencial import _filtro_pedidos
from processing.faturamento.config import SKU_NORMALIZADO_COL
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    build_resultado_gerencial_slice,
    compute_resultado_gerencial_kpis,
    compute_sku_margens_para_saude,
    compute_tabela_por_pedido,
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
    sku: str = "S1",
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
        SKU_NORMALIZADO_COL: sku,
        "Quantidade": 1.0,
    }
    assert REQUIRED_LINE_COLUMNS.issubset(r.keys())
    return r


def test_soma_operacional_tabela_bate_com_kpi_operacional() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="10/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=40.0,
                frete_tp=2.0,
                desp_fixa=3.0,
                sku="A",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads_variavel"] = 1.0
    df["custo_ads_fixo"] = 2.0

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    imp = 7.5
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=imp)
    tabela = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)

    soma_op = sum(p.resultado_operacional for p in tabela)
    assert abs(soma_op - float(kpis["resultado_operacional"])) < 0.01
    soma_desp = sum(p.despesa_fixa for p in tabela)
    assert abs(soma_desp - float(kpis["total_despesa_fixa"])) < 0.01
    assert kpis["ads_sem_split_agregado"] is False


def test_soma_liquida_tabela_bate_com_kpi_liquido() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="10/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=40.0,
                frete_tp=2.0,
                desp_fixa=3.0,
                sku="A",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads"] = 5.0

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    imp = 12.34
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=imp)
    tabela = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)

    soma_res = sum(p.resultado_liquido for p in tabela)
    assert abs(soma_res - float(kpis["resultado"])) < 0.01


def test_operacional_sempre_maior_ou_igual_liquido() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="10/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=40.0,
                frete_tp=2.0,
                desp_fixa=3.0,
                sku="A",
            ),
            _row(
                data="11/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=2.0,
                cmv=15.0,
                resultado=20.0,
                desp_fixa=2.0,
                sku="B",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads_variavel"] = [1.0, 0.5]
    df["custo_ads_fixo"] = [4.0, 2.0]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tabela = compute_tabela_por_pedido(sl, fiscal_imposto_valor=30.0)
    for p in tabela:
        assert p.resultado_operacional >= p.resultado_liquido - 1e-6


def test_filtro_saudavel_mas_negativo_no_liquido() -> None:
    class P:
        def __init__(self, ro: float, rl: float) -> None:
            self.resultado_operacional = ro
            self.resultado_liquido = rl

    linhas = [P(10.0, -5.0), P(-3.0, -8.0), P(2.0, 1.0)]
    fil = _filtro_pedidos(
        linhas,  # type: ignore[arg-type]
        plats=(),
        statuses=(),
        texto="",
        faixa_resultado="saudavel_neg_liquido",
    )
    assert len(fil) == 1
    assert fil[0].resultado_operacional == 10.0


def test_retrocompatibilidade_campos_antigos() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=40.0,
                desp_fixa=4.0,
                sku="X",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads"] = 2.0

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=5.0)
    assert kpis["resultado"] == pytest.approx(float(kpis["resultado_liquido"]))
    assert kpis["margem"] == pytest.approx(float(kpis["margem_liquida"]))
    assert kpis["margem_sobre_venda"] == pytest.approx(float(kpis["margem_liquida"]))


def test_sku_buckets_cobrem_todos_liquido_negativo() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=200.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=100.0,
                frete_tp=10.0,
                desp_fixa=80.0,
                sku="SKU_NEG_LIQ",
            ),
            _row(
                data="02/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=40.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=-30.0,
                desp_fixa=5.0,
                sku="SKU_NEG_OP",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads_variavel"] = [2.0, 1.0]
    df["custo_ads_fixo"] = [5.0, 3.0]
    imp = 25.0
    margens = compute_sku_margens_para_saude(df, fiscal_imposto_valor=imp)
    neg_liq = {m.sku for m in margens if m.resultado_liquido < -1e-6}

    sp, sc, _, _ = compute_skus_risco_duas_visoes(df, fiscal_imposto_valor=imp)
    sp_s = {s.sku for s in sp}
    sc_s = {s.sku for s in sc}
    assert sp_s.isdisjoint(sc_s)
    assert sp_s | sc_s == neg_liq


def test_union_skus_legacy_negativo_resultado_coluna() -> None:
    """Lista antiga (coluna Resultado por SKU) ⊆ união das duas visões ∪ SKUs só líquido+."""
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=200.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=100.0,
                frete_tp=10.0,
                desp_fixa=80.0,
                sku="SKU_NEG_LIQ",
            ),
            _row(
                data="02/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=40.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=-30.0,
                desp_fixa=5.0,
                sku="SKU_NEG_OP",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads_variavel"] = [2.0, 1.0]
    df["custo_ads_fixo"] = [5.0, 3.0]
    imp = 25.0
    _, sku_analise_old, _ = compute_skus_em_risco(df)
    legacy_neg = set(sku_analise_old.loc[sku_analise_old["Resultado"] < 0, "SKU_Normalizado"].astype(str))

    sp, sc, _, _ = compute_skus_risco_duas_visoes(df, fiscal_imposto_valor=imp)
    novos_alvos = {s.sku for s in sp} | {s.sku for s in sc}
    assert legacy_neg.issubset(novos_alvos)


def test_empresa_sem_split_ads_degrada_graciosamente() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=30.0,
                resultado=40.0,
                desp_fixa=5.0,
                sku="Z",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    df["custo_ads"] = 7.0

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=3.0)
    assert kpis["total_ads_variavel"] == pytest.approx(0.0)
    assert kpis["total_ads_fixo"] == pytest.approx(7.0)
    assert kpis["ads_sem_split_agregado"] is True
    row = compute_tabela_por_pedido(sl, fiscal_imposto_valor=3.0)[0]
    assert row.ads_variavel == pytest.approx(0.0)
    assert row.ads_fixo == pytest.approx(7.0)
