"""Regressão: ficha por pedido (Resultado Gerencial Ciclo C)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.config import SKU_NORMALIZADO_COL
from processing.faturamento.ficha_pedido_rg import (
    compute_benchmarks_comparacao,
    compute_benchmarks_empresa,
    compute_diagnostico_automatico,
    compute_ficha_pedido,
)
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    PedidoGerencialRow,
    build_resultado_gerencial_slice,
    compute_tabela_por_pedido,
)


def _row(**kwargs: object) -> dict:
    base = {
        "Data": "01/03/2026",
        "empresa": "E1",
        "org_id": "o1",
        "Número do pedido": "P1",
        "Nome da plataforma": "Mercado Livre",
        "Valor total": 100.0,
        "Taxa de Comissão": 10.0,
        "Frete_Plataforma": 5.0,
        "Custo_Produto_Total": 30.0,
        "Resultado": 40.0,
        "Frete transportadora própria": 2.0,
        "Despesas Fixas": 5.0,
        "Quantidade": 1.0,
        "Preço de lista": 100.0,
        SKU_NORMALIZADO_COL: "SKU_A",
        "produto_resumo": "Produto teste",
    }
    base.update(kwargs)
    assert REQUIRED_LINE_COLUMNS.issubset(base.keys())
    return base


def test_composicao_ficha_bate_com_linha_da_tabela() -> None:
    df = pd.DataFrame([_row()])
    df["Vl_Venda"] = df["Valor total"]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=5.0)
    pid = tab[0].pedido_id
    rg = {"benchmarks": {"margem_operacional_saudavel": 0.99}}
    fh = compute_ficha_pedido(
        sl,
        pedido_id=pid,
        fiscal_imposto_valor=5.0,
        pedidos_contexto=tab,
        rg_config=rg,
    )
    assert fh is not None
    row = tab[0]
    assert fh.resultado_liquido == pytest.approx(row.resultado_liquido)
    assert fh.resultado_operacional == pytest.approx(row.resultado_operacional)


def test_soma_percentuais_componentes_igual_margem_liquida() -> None:
    df = pd.DataFrame([_row()])
    df["Vl_Venda"] = df["Valor total"]
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=8.0)
    fh = compute_ficha_pedido(
        sl,
        pedido_id=tab[0].pedido_id,
        fiscal_imposto_valor=8.0,
        pedidos_contexto=tab,
        rg_config={"benchmarks": {}},
    )
    assert fh is not None
    soma_neg = (
        fh.cmv_pct
        + fh.comissao_pct
        + fh.frete_plataforma_pct
        + fh.frete_tp_pct
        + fh.imposto_pct
        + fh.despesa_fixa_pct
        + fh.ads_pct
    )
    assert abs(100.0 - soma_neg - fh.margem_liquida_pct) < 0.05


def test_comparacao_exclui_proprio_pedido() -> None:
    df = pd.DataFrame(
        [
            _row(**{"Número do pedido": "PA", "Valor total": 100.0, "Taxa de Comissão": 10.0}),
            _row(**{"Data": "02/03/2026", "Número do pedido": "PB", "Valor total": 200.0, "Taxa de Comissão": 20.0}),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=0.0)
    assert len(tab) == 2
    alvo = tab[0]
    cmp_ = compute_benchmarks_comparacao(
        pedidos_contexto=tab,
        pedido_alvo=alvo,
        df_linhas=sl.df_linha,
        fiscal_imposto_valor=0.0,
    )
    # Outro pedido mesmo ML / empresa — média macro deve ser só do outro
    outros = [p for p in tab if p.pedido_id != alvo.pedido_id]
    esperado = sum(p.resultado_liquido for p in outros) / sum(p.receita for p in outros) * 100.0
    assert cmp_.margem_plataforma == pytest.approx(esperado)


def test_diagnostico_alerta_quando_cmv_acima_media() -> None:
    df = pd.DataFrame(
        [
            _row(**{"Valor total": 100.0, "Custo_Produto_Total": 70.0}),
            _row(
                **{
                    "Data": "02/03/2026",
                    "Número do pedido": "P2",
                    "Valor total": 100.0,
                    "Custo_Produto_Total": 10.0,
                }
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=0.0)
    alvo = max(tab, key=lambda p: p.cmv)
    fh = compute_ficha_pedido(
        sl,
        pedido_id=alvo.pedido_id,
        fiscal_imposto_valor=0.0,
        pedidos_contexto=tab,
        rg_config={"benchmarks": {"cmv_alert_above_empresa_pp": 3.0}},
    )
    assert fh is not None
    bench = compute_benchmarks_empresa(pedidos_contexto=tab, pedido_alvo=alvo, rg_config={})
    diag = compute_diagnostico_automatico(
        fh,
        bench,
        {"benchmarks": {"cmv_alert_above_empresa_pp": 3.0}},
        comissao_esperada_frac=None,
    )
    assert any("CMV" in d.titulo for d in diag)


def test_diagnostico_sem_cards_quando_tudo_ok() -> None:
    df = pd.DataFrame([_row()])
    df["Vl_Venda"] = df["Valor total"]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=3.0)
    fh = compute_ficha_pedido(
        sl,
        pedido_id=tab[0].pedido_id,
        fiscal_imposto_valor=3.0,
        pedidos_contexto=tab,
        rg_config={"benchmarks": {"margem_operacional_saudavel": 0.99}},
    )
    assert fh is not None
    assert any(d.tipo == "saudavel" for d in fh.diagnosticos)
    assert not any(d.tipo == "risco" for d in fh.diagnosticos)


def test_ficha_com_pedido_multi_sku() -> None:
    df = pd.DataFrame(
        [
            {
                **_row(),
                "Número do pedido": "PX",
                "Valor total": 50.0,
                "Taxa de Comissão": 5.0,
                "Frete_Plataforma": 2.0,
                "Custo_Produto_Total": 15.0,
                SKU_NORMALIZADO_COL: "S1",
            },
            {
                **_row(),
                "Data": "01/03/2026",
                "Número do pedido": "PX",
                "Valor total": 50.0,
                "Taxa de Comissão": 5.0,
                "Frete_Plataforma": 3.0,
                "Custo_Produto_Total": 15.0,
                SKU_NORMALIZADO_COL: "S2",
                "produto_resumo": "Outro",
            },
        ]
    )
    df["Vl_Venda"] = df["Valor total"]

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=1.0)
    fh = compute_ficha_pedido(
        sl,
        pedido_id=tab[0].pedido_id,
        fiscal_imposto_valor=1.0,
        pedidos_contexto=tab,
        rg_config={},
    )
    assert fh is not None
    assert len(fh.itens) == 2


def test_ficha_degrada_sem_benchmark_config() -> None:
    df = pd.DataFrame([_row()])
    df["Vl_Venda"] = df["Valor total"]
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=0.0)
    fh = compute_ficha_pedido(
        sl,
        pedido_id=tab[0].pedido_id,
        fiscal_imposto_valor=0.0,
        pedidos_contexto=tab,
        rg_config={},
    )
    assert fh is not None


def test_comparacao_usa_mesmo_recorte_da_tabela() -> None:
    """Recorte estreito (só um pedido no contexto) → benchmark plataforma None ou não inclui self."""
    df = pd.DataFrame([_row()])
    df["Vl_Venda"] = df["Valor total"]
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=1.0)
    alvo = tab[0]
    cmp_ = compute_benchmarks_comparacao(
        pedidos_contexto=[alvo],
        pedido_alvo=alvo,
        df_linhas=sl.df_linha,
        fiscal_imposto_valor=1.0,
    )
    assert cmp_.margem_plataforma is None
