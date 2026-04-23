"""Testes de comparação temporal MA3 / MoM nos KPIs do Resultado Gerencial."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.comparacao_temporal_kpis import (
    ComparacaoKpisTemporal,
    _delta_class_positive_good,
    compute_comparacao_kpis_temporal,
    compute_trailing_monthly_metrics,
)
from processing.faturamento.pace_mensal import compute_trailing_monthly_revenues
from processing.faturamento.rg_cache_keys import dataframe_cache_token


def _row(
    *,
    dt: str,
    receita: float,
    resultado: float,
    pedido: str,
) -> dict:
    return {
        "Valor total": receita,
        "Taxa de Comissão": 0.0,
        "Frete_Plataforma": 0.0,
        "Custo_Produto_Total": 0.0,
        "Resultado": resultado,
        "Data": pd.Timestamp(dt),
        "Nome da plataforma": "Mercado Livre",
        "empresa": "EAP",
        "org_id": "o1",
        "Número do pedido": pedido,
    }


def _df_from_rows(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


EMP = ("EAP",)
PLAT: tuple[str, ...] = ()


def test_ma3_calculado_para_mes_cheio():
    """Março 2026 fechado → MA3 usa dez/jan/fev (média dos resultados mensais)."""
    rows = [
        _row(dt="2025-12-10", receita=1000.0, resultado=30_000.0, pedido="d1"),
        _row(dt="2026-01-10", receita=1000.0, resultado=30_000.0, pedido="j1"),
        _row(dt="2026-02-10", receita=1000.0, resultado=30_000.0, pedido="f1"),
        _row(dt="2026-03-10", receita=5000.0, resultado=50_000.0, pedido="m1"),
    ]
    df = _df_from_rows(rows)
    trail = compute_trailing_monthly_metrics(
        df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        mes_referencia=(2026, 3),
        n_meses=6,
    )
    keys = sorted(trail.keys())
    assert keys[-3:] == ["2025-12", "2026-01", "2026-02"]
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 5000.0, "resultado": 50_000.0},
    )
    assert comp.modo_comparacao == "mes_cheio"
    assert comp.tem_ma3 is True
    assert comp.resultado_ma3 == pytest.approx(30_000.0)


def test_mom_calculado_para_mes_cheio():
    """Março filtrado → MoM = fevereiro civil completo."""
    rows = [
        _row(dt="2026-02-05", receita=1000.0, resultado=80_000.0, pedido="f1"),
        _row(dt="2026-03-15", receita=2000.0, resultado=100_000.0, pedido="m1"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 2000.0, "resultado": 100_000.0},
    )
    assert comp.tem_mom is True
    assert comp.resultado_mom == pytest.approx(80_000.0)


def test_delta_resultado_percentual_correto():
    """Resultado atual 100k, MA3 90k → ~+11,1%."""
    rows = [
        _row(dt="2025-12-05", receita=1000.0, resultado=90_000.0, pedido="d1"),
        _row(dt="2026-01-05", receita=1000.0, resultado=90_000.0, pedido="j1"),
        _row(dt="2026-02-05", receita=1000.0, resultado=90_000.0, pedido="f1"),
        _row(dt="2026-03-05", receita=1000.0, resultado=10_000.0, pedido="x"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 50_000.0, "resultado": 100_000.0},
    )
    assert comp.delta_resultado_ma3_pct == pytest.approx(100_000 / 90_000 * 100.0 - 100.0, rel=1e-9)


def test_delta_margem_em_pp():
    """Margem atual 20%, MA3 média 16,666…% → +3,333… pp."""
    rows = [
        _row(dt="2025-12-05", receita=100_000.0, resultado=20_000.0, pedido="d1"),
        _row(dt="2026-01-05", receita=100_000.0, resultado=15_000.0, pedido="j1"),
        _row(dt="2026-02-05", receita=100_000.0, resultado=15_000.0, pedido="f1"),
        _row(dt="2026-03-05", receita=100_000.0, resultado=10_000.0, pedido="x"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 100_000.0, "resultado": 20_000.0},
    )
    ma3_mg = (0.20 + 0.15 + 0.15) / 3.0
    assert comp.margem_ma3 == pytest.approx(ma3_mg)
    assert comp.delta_margem_ma3_pp == pytest.approx((0.20 - ma3_mg) * 100.0)


def test_multi_mes_sem_comparacao():
    rows = [_row(dt="2026-03-05", receita=1000.0, resultado=10_000.0, pedido="a")]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 1, 1),
        data_fim=date(2026, 4, 17),
        kp_rg={"valor_venda_lista": 1000.0, "resultado": 10_000.0},
    )
    assert isinstance(comp, ComparacaoKpisTemporal)
    assert comp.modo_comparacao == "multi_mes"
    assert comp.tem_ma3 is False
    assert comp.tem_mom is False


def test_recorte_parcial_compara_proporcional():
    """01/03 a 15/03 (15 dias) → janelas de 15 dias desde dia 1 em fev/jan/dez."""

    def pack_15(y: int, m: int, prefix: str, resultado_total: float) -> list[dict]:
        per = resultado_total / 15.0
        out: list[dict] = []
        for i in range(15):
            d = date(y, m, 1 + i)
            out.append(_row(dt=d.isoformat(), receita=100.0, resultado=per, pedido=f"{prefix}{i}"))
        return out

    rows: list[dict] = []
    rows.extend(pack_15(2025, 12, "dec", 15_000.0))
    rows.extend(pack_15(2026, 1, "jan", 15_000.0))
    rows.extend(pack_15(2026, 2, "feb", 15_000.0))
    rows.extend(pack_15(2026, 3, "mar", 30_000.0))
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 15),
        kp_rg={"valor_venda_lista": 1500.0, "resultado": 30_000.0},
    )
    assert comp.modo_comparacao == "recorte_parcial"
    assert comp.tem_ma3 is True
    assert comp.resultado_ma3 == pytest.approx(15_000.0)
    assert comp.resultado_mom == pytest.approx(15_000.0)


def test_sem_historico_fallback_mom():
    """Menos de 3 meses completos antes do mês atual → sem MA3; MoM disponível."""
    rows = [
        _row(dt="2026-02-10", receita=5000.0, resultado=40_000.0, pedido="f1"),
        _row(dt="2026-03-10", receita=5000.0, resultado=50_000.0, pedido="m1"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 5000.0, "resultado": 50_000.0},
    )
    assert comp.tem_ma3 is False
    assert comp.tem_mom is True
    assert comp.delta_resultado_ma3_pct is None


def test_retrocompatibilidade_compute_trailing_monthly_revenues():
    df = pd.DataFrame(
        [
            {
                "Valor total": 100.0,
                "Taxa de Comissão": 0.0,
                "Frete_Plataforma": 0.0,
                "Custo_Produto_Total": 0.0,
                "Resultado": 1.0,
                "Data": pd.Timestamp("2026-03-01"),
                "Nome da plataforma": "Mercado Livre",
                "empresa": "EAP",
                "org_id": "o1",
                "Número do pedido": "p",
            }
        ]
    )
    assert compute_trailing_monthly_revenues(df, empresas_sel=EMP, plataformas_sel=PLAT, mes_referencia=(2026, 3)) == [
        0.0,
        0.0,
        0.0,
    ]


def test_cache_invalida_com_mudanca_filtro():
    """Token do DataFrame muda com dados diferentes (proxy da chave de cache Streamlit)."""
    df_a = _df_from_rows([_row(dt="2026-03-01", receita=1.0, resultado=1.0, pedido="a")])
    df_b = _df_from_rows([_row(dt="2026-03-01", receita=2.0, resultado=2.0, pedido="b")])
    assert dataframe_cache_token(df_a) != dataframe_cache_token(df_b)


def test_cor_delta_negativo_em_indicador_maior_melhor():
    assert _delta_class_positive_good(-5.0, use_pp=False) == "fdl-fat-kpi-delta--neg"
    assert _delta_class_positive_good(5.0, use_pp=False) == "fdl-fat-kpi-delta--pos"
    assert _delta_class_positive_good(0.5, use_pp=False) == "fdl-fat-kpi-delta--neut"


def test_tooltip_ma3_inclui_meses_e_base():
    rows = [
        _row(dt="2025-12-10", receita=1000.0, resultado=30_000.0, pedido="d1"),
        _row(dt="2026-01-10", receita=1000.0, resultado=30_000.0, pedido="j1"),
        _row(dt="2026-02-10", receita=1000.0, resultado=30_000.0, pedido="f1"),
        _row(dt="2026-03-10", receita=5000.0, resultado=50_000.0, pedido="m1"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 5000.0, "resultado": 50_000.0},
    )
    assert comp.meses_ma3_labels == ("dez/2025", "jan/2026", "fev/2026")
    assert comp.resultado_ma3 == pytest.approx(30_000.0)
    from processing.faturamento.comparacao_temporal_kpis import build_temporal_kpi_captions_html

    res_html, _mg = build_temporal_kpi_captions_html(comp)
    assert "dez/2025" in res_html
    assert "Base:" in res_html


def test_tooltip_mom_inclui_mes_label():
    rows = [
        _row(dt="2025-12-10", receita=1000.0, resultado=30_000.0, pedido="d1"),
        _row(dt="2026-01-10", receita=1000.0, resultado=30_000.0, pedido="j1"),
        _row(dt="2026-02-10", receita=1000.0, resultado=30_000.0, pedido="f1"),
        _row(dt="2026-03-10", receita=5000.0, resultado=50_000.0, pedido="m1"),
    ]
    df = _df_from_rows(rows)
    comp = compute_comparacao_kpis_temporal(
        slice_rg=None,
        df_linha=df,
        empresas_sel=EMP,
        plataformas_sel=PLAT,
        data_inicio=date(2026, 3, 1),
        data_fim=date(2026, 3, 31),
        kp_rg={"valor_venda_lista": 5000.0, "resultado": 50_000.0},
    )
    assert comp.mom_mes_label == "fevereiro/2026"
