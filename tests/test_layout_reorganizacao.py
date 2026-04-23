"""
Regressão de layout do Resultado Gerencial (Bloco 2): UI sem duplicar KPIs do Painel,
sem benchmark «vs grupo», filtros/resumo e mensagens de estado.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_painel_saude_nao_tem_mais_kpis_duplicados() -> None:
    """Fluxo RG simplificado não injeta o bloco HTML das 4 métricas duplicadas."""
    hp = (PROJECT_ROOT / "app" / "components" / "health_panel_ui.py").read_text(encoding="utf-8")
    i0 = hp.find("if rg_streamlined:")
    assert i0 > 0
    i1 = hp.find("else:", i0)
    chunk = hp[i0:i1]
    assert "_metrics_block_html" not in chunk
    assert "_executive_summary_html" not in chunk


def test_vs_grupo_removido_completamente() -> None:
    """Benchmark de grupo removido do pipeline RG + chamada sem margem de grupo."""
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "_hp_mrg_grupo" not in ao
    assert "margem_grupo_pct=None" in ao
    assert "return margem_ant" in ao.split("def _fdl_health_panel_rg_benchmark_margins")[1][:4000]


def test_default_empresa_maior_receita() -> None:
    """Último mês fechado: empresa com maior receita no grão linha."""
    from app.components.rg_layout_helpers import rg_pick_empresa_maior_receita_mes_fechado

    # Abril 2026 → mês fechado março 2026
    df = pd.DataFrame(
        {
            "Data": pd.to_datetime(["2026-03-10", "2026-03-15", "2026-03-20"]),
            "empresa": ["Beta", "Alpha", "Alpha"],
            "Vl_Venda": [100.0, 300.0, 50.0],
        }
    )
    out = rg_pick_empresa_maior_receita_mes_fechado(
        df, ["Alpha", "Beta"], ref_date=pd.Timestamp("2026-04-18").date()
    )
    assert out == "Alpha"


def test_mensagem_zero_empresas_selecionadas() -> None:
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "FDL_RG_MSG_SEM_EMPRESA" in ao
    assert "Selecione pelo menos uma empresa" in ao


def test_badge_consolidacao_aparece_com_2_ou_mais_empresas() -> None:
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "Visão consolidada" in ao
    assert "len(_min_state.empresas) >= 2" in ao


def test_kpi_rg_usa_hierarquia_b_sem_meta_hero() -> None:
    """KPIs RG pedem omit_hero_meta e tier_b_layout ao builder."""
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "omit_hero_meta=True" in ao
    assert "tier_b_layout=True" in ao


def test_dre_rg_remove_periodo_footer_bloco_resultado_duplicado() -> None:
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "hide_period_in_header=True" in ao
    assert "hide_resultado_margem_block=True" in ao