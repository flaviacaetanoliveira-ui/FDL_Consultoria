"""Regressão: tabela por pedido no Resultado Gerencial (coerência com KPIs)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.config import SKU_NORMALIZADO_COL
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    _allocate_imposto_total_centavos,
    build_resultado_gerencial_slice,
    compute_resultado_gerencial_kpis,
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
    ads: float = 0.0,
    sku: str = "SKU1",
    nota_situacao: str = "",
    quantidade: float = 1.0,
    custo_frete: float = 0.0,
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
        SKU_NORMALIZADO_COL: sku,
        "Quantidade": quantidade,
        "Custo de Frete": custo_frete,
        "Nota_Situacao": nota_situacao,
    }
    assert REQUIRED_LINE_COLUMNS.issubset(r.keys())
    return r


def test_soma_resultado_tabela_bate_com_kpis() -> None:
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
                resultado=50.0,
                frete_tp=2.0,
                desp_fixa=3.0,
                ads=0.0,
                sku="A",
            ),
            _row(
                data="11/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=200.0,
                comissao=20.0,
                frete_plat=10.0,
                cmv=60.0,
                resultado=100.0,
                frete_tp=5.0,
                desp_fixa=5.0,
                ads=0.0,
                sku="B",
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
    imp = 12.34
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=imp)
    tabela = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)

    soma_resultado = sum(p.resultado for p in tabela)
    soma_receita = sum(p.receita for p in tabela)
    assert abs(soma_resultado - float(kpis["resultado"])) < 0.01
    assert abs(soma_receita - float(kpis["valor_venda_lista"])) < 0.01


def test_contagem_pedidos_bate_com_kpi_pedidos() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="05/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="PX",
                plataforma="X",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=1.0,
                cmv=15.0,
                resultado=20.0,
                sku="S1",
            ),
            _row(
                data="06/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="PY",
                plataforma="Y",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=1.0,
                cmv=15.0,
                resultado=20.0,
                sku="S2",
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
    kpis = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=0.0)
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=0.0)
    assert len(tab) == int(kpis["pedidos"])


def test_rateio_imposto_por_pedido_soma_imposto_total() -> None:
    rec = {"a": 60.0, "b": 40.0, "c": 0.0}
    keys = sorted(rec.keys())
    imp = 99.991
    out = _allocate_imposto_total_centavos(keys, rec, imp)
    esperado = int(round(float(imp) * 100)) / 100.0
    assert abs(sum(out.values()) - esperado) < 1e-9
    assert out["c"] == 0.0


def test_status_nf_parcial_detectado() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="PMIX",
                plataforma="ML",
                valor_total=40.0,
                comissao=4.0,
                frete_plat=1.0,
                cmv=10.0,
                resultado=20.0,
                sku="X1",
                nota_situacao="Autorizada",
            ),
            _row(
                data="01/03/2026",
                empresa="E1",
                org_id="o1",
                pedido="PMIX",
                plataforma="ML",
                valor_total=60.0,
                comissao=6.0,
                frete_plat=2.0,
                cmv=15.0,
                resultado=30.0,
                sku="X2",
                nota_situacao="Cancelada pelo emitente",
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
    assert len(tab) == 1
    assert tab[0].status_nf == "parcial"
