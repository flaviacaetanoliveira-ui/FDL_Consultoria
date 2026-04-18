"""Testes da camada Resultado Gerencial (slice por Data de venda)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from comercial_pedidos_analise import pedido_id_series
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


def test_slice_filters_by_data_venda() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="15/03/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=40.0,
                resultado=45.0,
            ),
            _row(
                data="20/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=2.0,
                cmv=20.0,
                resultado=23.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    assert len(sl.df_linha) == 1
    assert sl.stats.receita_total == 50.0


def test_slice_filters_by_empresa() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="10/04/2026",
                empresa="EmpX",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=40.0,
                resultado=45.0,
            ),
            _row(
                data="10/04/2026",
                empresa="EmpY",
                org_id="o2",
                pedido="P2",
                plataforma="ML",
                valor_total=200.0,
                comissao=20.0,
                frete_plat=5.0,
                cmv=80.0,
                resultado=95.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=("EmpY",),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    assert len(sl.df_linha) == 1
    assert sl.df_linha.iloc[0]["empresa"] == "EmpY"


def test_slice_filters_by_plataforma() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="10/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="Shopee",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=5.0,
                cmv=40.0,
                resultado=45.0,
            ),
            _row(
                data="10/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P2",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=2.0,
                cmv=20.0,
                resultado=23.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=("ML",),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    assert len(sl.df_linha) == 1
    assert sl.df_linha.iloc[0]["Nome da plataforma"] == "ML"


def test_aggregates_and_pedido_id_series() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="05/04/2026",
                empresa="A",
                org_id="o9",
                pedido="PX",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=6.0,
                cmv=30.0,
                resultado=10.0,
                frete_tp=7.0,
                desp_fixa=5.0,
                ads=2.0,
            ),
            _row(
                data="06/04/2026",
                empresa="A",
                org_id="o9",
                pedido="PX",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=4.0,
                cmv=15.0,
                resultado=8.0,
                frete_tp=3.0,
                desp_fixa=2.0,
                ads=1.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    assert sl.stats.n_linhas == 2
    assert sl.stats.n_pedidos_unicos == 1
    assert sl.stats.receita_total == pytest.approx(150.0)
    assert sl.stats.comissao_total == pytest.approx(15.0)
    assert sl.stats.frete_plataforma_total == pytest.approx(10.0)
    assert sl.stats.frete_transportadora_propria_total == pytest.approx(10.0)
    assert sl.stats.cmv_total == pytest.approx(45.0)
    assert sl.stats.resultado_linhas_total == pytest.approx(18.0)
    assert sl.stats.despesa_fixa_total == pytest.approx(7.0)
    assert sl.stats.ads_total == pytest.approx(3.0)

    expected_pid = pedido_id_series(sl.df_linha).astype(str).str.strip()
    pd.testing.assert_series_equal(sl.pedido_ids, expected_pid, check_names=False)


def test_kpis_match_manual_sum() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=200.0,
                comissao=20.0,
                frete_plat=10.0,
                cmv=60.0,
                resultado=50.0,
                frete_tp=15.0,
                desp_fixa=10.0,
                ads=5.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    fiscal = 12.0
    kp = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=fiscal)
    ded = 20 + 60 + 10 + 15 + fiscal + 10 + 5
    assert kp["valor_venda_lista"] == pytest.approx(200.0)
    assert kp["total_receita_dre"] == pytest.approx(215.0)
    assert kp["total_deducoes"] == pytest.approx(ded)
    assert kp["resultado"] == pytest.approx(200.0 - ded)
    assert kp["margem"] == pytest.approx((200.0 - ded) / 200.0)
    assert kp["ticket_medio"] == pytest.approx(200.0)
    assert kp["pedidos"] == 1


def test_fiscal_imposto_bridge_not_recomputed() -> None:
    df = pd.DataFrame(
        [
            _row(
                data="01/04/2026",
                empresa="A",
                org_id="o1",
                pedido="P1",
                plataforma="ML",
                valor_total=100.0,
                comissao=5.0,
                frete_plat=2.0,
                cmv=20.0,
                resultado=50.0,
                frete_tp=3.0,
                desp_fixa=1.0,
                ads=0.0,
            ),
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    base = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=10.0)
    hi = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=25.0)
    assert hi["fiscal_imposto_valor"] == 25.0
    assert base["fiscal_imposto_valor"] == 10.0
    assert hi["total_deducoes"] - base["total_deducoes"] == pytest.approx(15.0)
    assert hi["resultado"] - base["resultado"] == pytest.approx(-15.0)


def test_requires_expected_columns() -> None:
    bad = pd.DataFrame({"x": [1]})
    with pytest.raises(ValueError, match="dataset.parquet"):
        build_resultado_gerencial_slice(
            bad,
            empresas_sel=(),
            plataformas_sel=(),
            data_venda_ini=date(2026, 4, 1),
            data_venda_fim=date(2026, 4, 30),
        )


def test_vl_venda_overrides_valor_total_for_receita() -> None:
    df = pd.DataFrame(
        [
            {
                **_row(
                    data="01/04/2026",
                    empresa="A",
                    org_id="o1",
                    pedido="P1",
                    plataforma="ML",
                    valor_total=999.0,
                    comissao=1.0,
                    frete_plat=1.0,
                    cmv=1.0,
                    resultado=1.0,
                ),
                "Vl_Venda": 300.0,
            }
        ]
    )
    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        data_venda_ini=date(2026, 4, 1),
        data_venda_fim=date(2026, 4, 30),
    )
    assert sl.stats.receita_total == pytest.approx(300.0)
