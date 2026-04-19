"""Testes da agregação por plataforma (Resultado Gerencial)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd
import pytest

from processing.faturamento.analise_plataforma import compute_analise_plataforma
from processing.faturamento.resultado_gerencial_slice import (
    PedidoGerencialRow,
    ResultadoGerencialSlice,
    ResultadoGerencialSliceMeta,
    ResultadoGerencialSliceStats,
)
from processing.faturamento.rg_cache_keys import dataframe_cache_token


def _minimal_slice() -> ResultadoGerencialSlice:
    st = ResultadoGerencialSliceStats(
        receita_total=1000.0,
        comissao_total=0.0,
        frete_plataforma_total=0.0,
        frete_transportadora_propria_total=0.0,
        cmv_total=0.0,
        resultado_linhas_total=0.0,
        despesa_fixa_total=0.0,
        ads_total=0.0,
        n_linhas=1,
        n_pedidos_unicos=1,
    )
    meta = ResultadoGerencialSliceMeta(
        empresas_sel=("X",),
        plataformas_sel=(),
        data_venda_ini=__import__("datetime").date(2026, 3, 1),
        data_venda_fim=__import__("datetime").date(2026, 3, 31),
    )
    return ResultadoGerencialSlice(
        df_linha=pd.DataFrame({"Data": [datetime(2026, 3, 1)]}),
        pedido_ids=pd.Series(["1"]),
        stats=st,
        meta=meta,
    )


def _row(
    *,
    pid: str,
    plat: str,
    rec: float,
    rop: float,
    rliq: float,
) -> PedidoGerencialRow:
    return PedidoGerencialRow(
        data_venda=datetime(2026, 3, 15),
        plataforma=plat,
        empresa="Gama Home",
        pedido_id=pid,
        numero_pedido_ui=pid,
        skus=("a",),
        qtd_itens=1,
        receita=rec,
        comissao=0.0,
        frete_plataforma=0.0,
        cmv=0.0,
        frete_tp=0.0,
        imposto_rateado=0.0,
        despesa_fixa=0.0,
        ads_variavel=0.0,
        ads_fixo=0.0,
        resultado_operacional=rop,
        resultado_liquido=rliq,
        margem_operacional_pct=(rop / rec * 100) if rec else 0.0,
        margem_liquida_pct=(rliq / rec * 100) if rec else 0.0,
        resultado=rliq,
        margem_pct=(rliq / rec * 100) if rec else 0.0,
        status_nf="",
    )


def test_soma_receita_por_plataforma_bate_kpi():
    tab = [
        _row(pid="1", plat="Mercado Livre", rec=600.0, rop=100.0, rliq=80.0),
        _row(pid="2", plat="Shopee", rec=400.0, rop=50.0, rliq=40.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 120.0, "resultado_liquido": 120.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert sum(x.receita for x in a.linhas) == pytest.approx(1000.0)


def test_soma_resultado_por_plataforma_bate_kpi():
    tab = [
        _row(pid="1", plat="Mercado Livre", rec=600.0, rop=100.0, rliq=80.0),
        _row(pid="2", plat="Shopee", rec=400.0, rop=50.0, rliq=40.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 120.0, "resultado_liquido": 120.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert sum(x.resultado_liquido for x in a.linhas) == pytest.approx(120.0)


def test_soma_pct_receita_igual_a_1():
    tab = [
        _row(pid="1", plat="A", rec=600.0, rop=1.0, rliq=1.0),
        _row(pid="2", plat="B", rec=400.0, rop=1.0, rliq=1.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 2.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert sum(x.pct_da_receita for x in a.linhas) == pytest.approx(1.0, abs=0.002)


def test_margem_calculada_ponderada_nao_media():
    """Receitas diferentes: margem agregada ≠ média simples das duas taxas por pedido."""
    tab = [
        _row(pid="1", plat="ML", rec=100.0, rop=10.0, rliq=10.0),
        _row(pid="2", plat="ML", rec=300.0, rop=150.0, rliq=150.0),
    ]
    kp = {"valor_venda_lista": 400.0, "resultado": 160.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert len(a.linhas) == 1
    ln = a.linhas[0]
    taxa_p1 = 10.0 / 100.0 * 100.0
    taxa_p2 = 150.0 / 300.0 * 100.0
    media_simples_taxas = (taxa_p1 + taxa_p2) / 2.0
    ponderada = 160.0 / 400.0 * 100.0
    assert ln.margem_liquida_pct == pytest.approx(ponderada)
    assert ln.margem_liquida_pct != pytest.approx(media_simples_taxas)


def test_ordenacao_default_por_pct_receita_desc():
    tab = [
        _row(pid="1", plat="Pequeno", rec=100.0, rop=1.0, rliq=1.0),
        _row(pid="2", plat="Grande", rec=900.0, rop=1.0, rliq=1.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 2.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert a.linhas[0].plataforma == "Grande"
    assert a.linhas[1].plataforma == "Pequeno"


def test_uma_plataforma_nao_renderiza(monkeypatch: pytest.MonkeyPatch) -> None:
    import streamlit as st

    from app.components.analise_plataforma_ui import render_analise_plataforma

    calls: list[str] = []

    def _capture_markdown(*_a: object, **_k: object) -> None:
        calls.append("markdown")

    monkeypatch.setattr(st, "markdown", _capture_markdown)
    monkeypatch.setattr(st, "dataframe", lambda *_a, **_k: None)

    tab = [_row(pid="1", plat="ML", rec=1000.0, rop=100.0, rliq=100.0)]
    kp = {"valor_venda_lista": 1000.0, "resultado": 100.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert len(a.linhas) == 1
    render_analise_plataforma(a, debug_enabled=False)
    assert calls == []


def test_plataforma_mais_rentavel():
    tab = [
        _row(pid="1", plat="Alta", rec=100.0, rop=50.0, rliq=50.0),
        _row(pid="2", plat="Baixa", rec=900.0, rop=10.0, rliq=10.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 60.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert a.plataforma_mais_rentavel == "Alta"


def test_plataforma_mais_volume():
    tab = [
        _row(pid="1", plat="Vol", rec=800.0, rop=1.0, rliq=1.0),
        _row(pid="2", plat="Peq", rec=200.0, rop=1.0, rliq=1.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 2.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert a.plataforma_mais_volume == "Vol"


def test_cache_invalida_com_troca_empresa():
    df_a = pd.DataFrame([{"x": 1}])
    df_b = pd.DataFrame([{"x": 2}])
    assert dataframe_cache_token(df_a) != dataframe_cache_token(df_b)


def test_plataforma_com_margem_negativa_classificada_vermelha():
    from app.components.analise_plataforma_ui import _tier_label

    assert _tier_label(-1.0, 10.0) == "Risco"


def test_sem_pedidos_retorna_vazio():
    tab = []
    kp = {"valor_venda_lista": 0.0, "resultado": 0.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert a.linhas == ()


def test_formatacao_brl_receita_display():
    tab = [
        _row(pid="1", plat="Canal", rec=140992.82, rop=1.0, rliq=1.0),
        _row(pid="2", plat="Outro", rec=100.0, rop=1.0, rliq=1.0),
    ]
    kp = {"valor_venda_lista": 141092.82, "resultado": 2.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    grande = max(a.linhas, key=lambda x: x.receita)
    assert grande.receita_display.startswith("R$ ")
    assert "," in grande.receita_display
    assert "." in grande.receita_display


def test_plataforma_vazia_agrupada_como_nao_identificado():
    tab = [
        _row(pid="1", plat="", rec=400.0, rop=50.0, rliq=40.0),
        _row(pid="2", plat="Shopee", rec=600.0, rop=10.0, rliq=10.0),
    ]
    kp = {"valor_venda_lista": 1000.0, "resultado": 50.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    labels = [x.plataforma for x in a.linhas]
    assert "Não identificado" in labels


def test_margem_liquida_display_formato_pct():
    tab = [_row(pid="1", plat="ML", rec=100.0, rop=10.0, rliq=12.34)]
    kp = {"valor_venda_lista": 100.0, "resultado": 12.34}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert "%" in a.linhas[0].margem_liquida_display
    assert "," in a.linhas[0].margem_liquida_display


def test_linha_sem_volume_e_removida():
    """Filtro defensivo: pedidos>0 e receita>0 (não deve gerar linha fantasma)."""
    tab = [
        _row(pid="1", plat="A", rec=100.0, rop=1.0, rliq=1.0),
        _row(pid="2", plat="B", rec=200.0, rop=1.0, rliq=1.0),
    ]
    kp = {"valor_venda_lista": 300.0, "resultado": 2.0}
    a = compute_analise_plataforma(slice_rg=_minimal_slice(), pedidos_tabela=tab, kp_rg=kp)
    assert all(x.pedidos >= 1 and x.receita > 0 for x in a.linhas)
