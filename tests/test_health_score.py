"""Testes do score de saude financeira (Faturamento / DRE)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.components.health_score import (
    AlertLevel,
    HealthLevel,
    calcular_health_score,
    obter_dados_grupo,
    slice_linhas_nf_periodo,
)
from processing.faturamento.config import STATUS_CUSTO_OK


def _criar_df_mock(receita: float, custo: float, resultado: float) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "SKU_Normalizado": ["SKU1", "SKU2"],
            "Vl_Venda": [receita * 0.6, receita * 0.4],
            "Custo_Produto_Total": [custo * 0.6, custo * 0.4],
            "Resultado": [resultado * 0.6, resultado * 0.4],
            "Quantidade": [10.0, 5.0],
            "Status_Custo": [STATUS_CUSTO_OK, STATUS_CUSTO_OK],
            "org_id": ["test", "test"],
            "empresa": ["Test Co", "Test Co"],
            "Nota_Data_Emissao": pd.to_datetime(["2026-03-15T12:00:00Z", "2026-03-20T12:00:00Z"], utc=True),
        }
    )


def test_score_saudavel() -> None:
    df = _criar_df_mock(receita=100_000.0, custo=40_000.0, resultado=15_000.0)
    health = calcular_health_score(df, "test", 2026, 3)
    assert health.score >= 70
    assert health.level == HealthLevel.SAUDAVEL
    assert health.margem_pct == pytest.approx(15.0, rel=0.01)


def test_score_critico() -> None:
    df = _criar_df_mock(receita=100_000.0, custo=60_000.0, resultado=-10_000.0)
    health = calcular_health_score(df, "test", 2026, 3)
    assert health.score <= 40
    assert health.level == HealthLevel.CRITICO
    assert health.margem_pct == pytest.approx(-10.0, rel=0.01)


def test_diagnostico_resultado_negativo() -> None:
    df = _criar_df_mock(receita=100_000.0, custo=60_000.0, resultado=-5000.0)
    health = calcular_health_score(df, "test", 2026, 3)
    diag_alerta = [d for d in health.diagnosticos if d.tipo == "ALERTA"]
    assert len(diag_alerta) >= 1
    assert diag_alerta[0].nivel == AlertLevel.CRITICAL


def test_diagnostico_custo_alto() -> None:
    df = _criar_df_mock(receita=100_000.0, custo=60_000.0, resultado=5000.0)
    health = calcular_health_score(df, "test", 2026, 3)
    diag_custo = [d for d in health.diagnosticos if "Custo" in d.titulo]
    assert len(diag_custo) >= 1


def test_skus_risco() -> None:
    df = pd.DataFrame(
        {
            "SKU_Normalizado": ["BOM", "RUIM"],
            "Vl_Venda": [80_000.0, 20_000.0],
            "Custo_Produto_Total": [30_000.0, 25_000.0],
            "Resultado": [10_000.0, -8000.0],
            "Quantidade": [100.0, 50.0],
            "Status_Custo": [STATUS_CUSTO_OK, STATUS_CUSTO_OK],
            "org_id": ["test", "test"],
            "empresa": ["Test Co", "Test Co"],
            "Nota_Data_Emissao": pd.to_datetime(["2026-03-15T12:00:00Z", "2026-03-20T12:00:00Z"], utc=True),
        }
    )
    health = calcular_health_score(df, "test", 2026, 3)
    assert len(health.skus_risco) >= 1
    assert health.skus_risco[0].sku == "RUIM"
    assert health.skus_risco[0].ajuste_breakeven > 0


def test_comparativo_tendencia() -> None:
    df_atual = _criar_df_mock(receita=100_000.0, custo=50_000.0, resultado=5000.0)
    df_anterior = _criar_df_mock(receita=100_000.0, custo=45_000.0, resultado=10_000.0)
    health = calcular_health_score(df_atual, "test", 2026, 3, df_anterior=df_anterior)
    assert health.margem_anterior == pytest.approx(10.0, rel=0.01)
    assert health.tendencia_pp == pytest.approx(-5.0, rel=0.01)


def test_slice_linhas_nf_periodo_emissao_default() -> None:
    df = _criar_df_mock(receita=100_000.0, custo=40_000.0, resultado=15_000.0)
    sl = slice_linhas_nf_periodo(
        df,
        d_ini=date(2026, 3, 1),
        d_fim=date(2026, 3, 31),
        empresas_sel=(),
    )
    assert len(sl) == 2


def test_slice_linhas_por_data_venda() -> None:
    df = pd.DataFrame(
        {
            "SKU_Normalizado": ["A", "B"],
            "Vl_Venda": [60_000.0, 40_000.0],
            "Custo_Produto_Total": [24_000.0, 16_000.0],
            "Resultado": [9000.0, 6000.0],
            "Quantidade": [10.0, 5.0],
            "Status_Custo": [STATUS_CUSTO_OK, STATUS_CUSTO_OK],
            "org_id": ["o1", "o1"],
            "empresa": ["Acme", "Acme"],
            "Nome da plataforma": ["Mercado Livre", "Mercado Livre"],
            "Data": ["15/03/2026", "25/03/2026"],
            "Nota_Data_Emissao": pd.to_datetime(["2026-04-01", "2026-04-02"]),
        }
    )
    sl = slice_linhas_nf_periodo(
        df,
        d_ini=date(2026, 3, 1),
        d_fim=date(2026, 3, 31),
        empresas_sel=(),
        coluna_temporal="Data",
    )
    assert len(sl) == 2


def test_slice_linhas_filtra_plataforma() -> None:
    df = pd.DataFrame(
        {
            "SKU_Normalizado": ["A", "B"],
            "Vl_Venda": [60_000.0, 40_000.0],
            "Custo_Produto_Total": [24_000.0, 16_000.0],
            "Resultado": [9000.0, 6000.0],
            "Quantidade": [10.0, 5.0],
            "Status_Custo": [STATUS_CUSTO_OK, STATUS_CUSTO_OK],
            "org_id": ["o1", "o1"],
            "empresa": ["Acme", "Acme"],
            "Nome da plataforma": ["Mercado Livre", "Shopee"],
            "Data": ["15/03/2026", "16/03/2026"],
        }
    )
    sl = slice_linhas_nf_periodo(
        df,
        d_ini=date(2026, 3, 1),
        d_fim=date(2026, 3, 31),
        empresas_sel=(),
        coluna_temporal="Data",
        plataformas_sel=("Mercado Livre",),
    )
    assert len(sl) == 1
    assert sl.iloc[0]["Nome da plataforma"] == "Mercado Livre"


def test_obter_dados_grupo_data_column() -> None:
    df = pd.DataFrame(
        {
            "Vl_Venda": [100.0],
            "Custo_Produto_Total": [40.0],
            "Resultado": [15.0],
            "Status_Custo": [STATUS_CUSTO_OK],
            "org_id": ["x"],
            "Data": ["10/03/2026"],
        }
    )
    g = obter_dados_grupo(df, 2026, 3, coluna_temporal="Data")
    assert g is not None
    assert len(g) == 1
