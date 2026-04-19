"""Regressão: Curva ABC por SKU (Resultado Gerencial)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.curva_abc import SKU_VAZIO_LABEL, compute_curva_abc
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    build_resultado_gerencial_slice,
    compute_resultado_gerencial_kpis,
)
from processing.faturamento.rg_cache_keys import dataframe_cache_token


def _base_row(
    *,
    data: str,
    pedido: str,
    plataforma: str,
    valor_total: float,
    comissao: float,
    frete_plat: float,
    cmv: float,
    resultado: float,
    sku: str,
    desp_fixa: float = 0.0,
    ads: float = 0.0,
    codigo: str = "",
) -> dict:
    r = {
        "Data": data,
        "empresa": "Acme",
        "org_id": "o1",
        "Número do pedido": pedido,
        "Nome da plataforma": plataforma,
        "Valor total": valor_total,
        "Taxa de Comissão": comissao,
        "Frete_Plataforma": frete_plat,
        "Custo_Produto_Total": cmv,
        "Resultado": resultado,
        "Frete transportadora própria": 0.0,
        "Despesas Fixas": desp_fixa,
        "custo_ads": ads,
        "SKU_Normalizado": sku,
        "Código": codigo,
    }
    assert REQUIRED_LINE_COLUMNS.issubset(r.keys())
    return r


def _slice_kpi_curva(df: pd.DataFrame, *, fiscal: float = 0.0):
    d_ini = date(2026, 3, 1)
    d_fim = date(2026, 3, 31)
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=d_ini,
        data_venda_fim=d_fim,
    )
    kp = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=float(fiscal))
    curva = compute_curva_abc(slice_rg=sl, kp_rg=kp, fiscal_imposto_valor=float(fiscal))
    return sl, kp, curva


def test_soma_receita_por_sku_bate_kpi():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=600.0,
                comissao=60.0,
                frete_plat=10.0,
                cmv=200.0,
                resultado=330.0,
                sku="A1",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=400.0,
                comissao=40.0,
                frete_plat=10.0,
                cmv=150.0,
                resultado=200.0,
                sku="B2",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert sum(x.receita for x in curva.linhas) == pytest.approx(float(kp["valor_venda_lista"]), abs=0.02)


def test_soma_resultado_liquido_por_sku_bate_kpi():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=600.0,
                comissao=60.0,
                frete_plat=10.0,
                cmv=200.0,
                resultado=330.0,
                sku="A1",
                desp_fixa=50.0,
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=400.0,
                comissao=40.0,
                frete_plat=10.0,
                cmv=150.0,
                resultado=200.0,
                sku="B2",
                desp_fixa=25.0,
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert sum(x.resultado_liquido for x in curva.linhas) == pytest.approx(float(kp["resultado_liquido"]), abs=0.02)


def test_classificacao_abc_thresholds_70_90():
    """Receitas 70 / 20 / 10 → cum após cada linha 0,7 / 0,9 / 1,0."""
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=700.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=700.0,
                sku="SKU_BIG",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=200.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=200.0,
                sku="SKU_MED",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P3",
                plataforma="ML",
                valor_total=100.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=100.0,
                sku="SKU_SMALL",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert len(curva.linhas) == 3
    assert curva.linhas[0].sku == "SKU_BIG" and curva.linhas[0].classe_abc == "A"
    assert curva.linhas[1].sku == "SKU_MED" and curva.linhas[1].classe_abc == "B"
    assert curva.linhas[2].sku == "SKU_SMALL" and curva.linhas[2].classe_abc == "C"


def test_ordenacao_default_por_receita_desc():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=100.0,
                sku="PEQ",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=900.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=900.0,
                sku="GRD",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert curva.linhas[0].sku == "GRD"
    assert curva.linhas[1].sku == "PEQ"


def test_pct_acumulado_cresce_monotonicamente():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido=f"P{i}",
                plataforma="ML",
                valor_total=float(v),
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=float(v),
                sku=f"S{i}",
            )
            for i, v in enumerate([500.0, 300.0, 200.0], start=1)
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    cums = [x.pct_acumulado for x in curva.linhas]
    assert all(cums[i] <= cums[i + 1] + 1e-12 for i in range(len(cums) - 1))


def test_pct_acumulado_ultimo_igual_1():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=0.0,
                cmv=20.0,
                resultado=70.0,
                sku="X",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=0.0,
                cmv=10.0,
                resultado=35.0,
                sku="Y",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert curva.linhas[-1].pct_acumulado == pytest.approx(1.0, abs=0.002)


def test_margem_por_sku_ponderada_nao_media():
    """Mesmo SKU em duas linhas: margem SKU = sum(rl)/sum(rec), não média das duas taxas."""
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=2.0,
                cmv=20.0,
                resultado=58.0,
                sku="MERGE",
                desp_fixa=10.0,
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=300.0,
                comissao=30.0,
                frete_plat=2.0,
                cmv=60.0,
                resultado=178.0,
                sku="MERGE",
                desp_fixa=30.0,
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert len(curva.linhas) == 1
    ln = curva.linhas[0]
    rl1 = 100.0 - 10.0 - 2.0 - 20.0 - 10.0
    rl2 = 300.0 - 30.0 - 2.0 - 60.0 - 30.0
    rec_t = 400.0
    ponderada = (rl1 + rl2) / rec_t * 100.0
    media_simples = ((rl1 / 100.0 * 100.0) + (rl2 / 300.0 * 100.0)) / 2.0
    assert ln.margem_liquida_pct == pytest.approx(ponderada, rel=1e-6)
    assert ln.margem_liquida_pct != pytest.approx(media_simples, abs=0.05)


def test_classe_a_soma_proximo_70_pct():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=700.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=700.0,
                sku="A",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=200.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=200.0,
                sku="B",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P3",
                plataforma="ML",
                valor_total=100.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=100.0,
                sku="C",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    pa = sum(x.pct_da_receita for x in curva.linhas if x.classe_abc == "A")
    assert pa == pytest.approx(0.70, abs=0.002)


def test_sku_vazio_agrupado_como_nao_identificado():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=400.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=400.0,
                sku="",
                codigo="",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=600.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=600.0,
                sku="OK",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    labels = [x.sku for x in curva.linhas]
    assert SKU_VAZIO_LABEL in labels


def test_formatacao_brl_receita_display():
    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=56664.89,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=56664.89,
                sku="BIG",
            ),
            _base_row(
                data="15/03/2026",
                pedido="P2",
                plataforma="ML",
                valor_total=100.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=100.0,
                sku="SM",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    grande = max(curva.linhas, key=lambda x: x.receita)
    assert grande.receita_display.startswith("R$ ")
    assert "," in grande.receita_display


def test_sku_unico_nao_renderiza(monkeypatch: pytest.MonkeyPatch) -> None:
    import streamlit as st

    from app.components.curva_abc_ui import render_curva_abc

    calls: list[str] = []

    def _capture_markdown(*_a: object, **_k: object) -> None:
        calls.append("markdown")

    monkeypatch.setattr(st, "markdown", _capture_markdown)
    monkeypatch.setattr(st, "dataframe", lambda *_a, **_k: None)
    monkeypatch.setattr(st, "checkbox", lambda *_a, **_k: False)

    df = pd.DataFrame(
        [
            _base_row(
                data="15/03/2026",
                pedido="P1",
                plataforma="ML",
                valor_total=1000.0,
                comissao=0.0,
                frete_plat=0.0,
                cmv=0.0,
                resultado=1000.0,
                sku="ONLY",
            ),
        ]
    )
    _sl, kp, curva = _slice_kpi_curva(df)
    assert len(curva.linhas) == 1
    render_curva_abc(curva, debug_enabled=False)
    assert calls == []


def test_cache_invalida_com_filtro():
    df_a = pd.DataFrame([{"x": 1}])
    df_b = pd.DataFrame([{"x": 2}])
    assert dataframe_cache_token(df_a) != dataframe_cache_token(df_b)
