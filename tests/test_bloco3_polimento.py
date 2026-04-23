"""
Bloco 3 — polimento visual (CSS/HTML/severidade), sem alterar números de negócio.
"""

from __future__ import annotations

from pathlib import Path

from app.components.faturamento_dre_ui import build_dre_gerencial_premium_html

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_dre_tem_linha_fechamento_discreta() -> None:
    html = build_dre_gerencial_premium_html(
        period_caption="x",
        valor_venda_fmt="R$ 1",
        rec_frete_fmt="R$ 0",
        total_receita_fmt="R$ 1",
        enc_rows=[("Comissão", "−R$ 1")],
        total_deducoes_fmt="−R$ 1",
        resultado_fmt="R$ 100",
        resultado_value=100.0,
        margem_str="10,0%",
        resultado_tooltip="t",
        margem_tooltip="m",
        hide_period_in_header=True,
        hide_footnote=True,
        hide_resultado_margem_block=True,
        show_resultado_discreto=True,
        rg_header_subtitle="Empresa X",
    )
    assert "fdl-dre-close-discrete" in html
    assert "Resultado líquido" in html
    assert "fdl-dre-result--positive" not in html
    assert "fdl-rg-block-head-label" in html


def test_severidade_cor_margem_em_queda_amarelo() -> None:
    hs = (PROJECT_ROOT / "app" / "components" / "health_score.py").read_text(encoding="utf-8")
    assert (
        "nivel_queda = (" in hs
        and "else AlertLevel.MEDIUM" in hs
        and "Margem em queda" in hs
    )


def test_severidade_cor_prejuizo_real_vermelho() -> None:
    hs = (PROJECT_ROOT / "app" / "components" / "health_score.py").read_text(encoding="utf-8")
    assert "SKUs em prejuízo real" in hs
    i = hs.index("SKUs em prejuízo real")
    assert "AlertLevel.CRITICAL" in hs[i - 250 : i + 120]


def test_cabecalho_dre_e_painel_mesma_estrutura() -> None:
    hp = (PROJECT_ROOT / "app" / "components" / "health_panel_ui.py").read_text(encoding="utf-8")
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "fdl-rg-block-head-label" in hp
    assert "fdl-rg-block-head-sub" in hp
    assert "rg_header_subtitle" in ao
    assert "_fdl_rg_header_context" in ao


def test_cobertura_comercial_sem_expander_aninhado() -> None:
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "rg_premium_single_expander=True" in ao
    assert "fdl-fat-cobertura--rg-premium" in ao


def test_kpi_hierarquia_b_alturas_distintas() -> None:
    ui = (PROJECT_ROOT / "app" / "components" / "faturamento_dre_ui.py").read_text(encoding="utf-8")
    assert "min-height: 120px" in ui and "min-height: 80px" in ui
    assert "fdl-fat-kpi-shell--rg-tierb" in ui


def test_cobertura_comercial_texto_em_tooltip() -> None:
    ao = (PROJECT_ROOT / "app_operacional.py").read_text(encoding="utf-8")
    assert "fdl-fat-cob-rg-info" in ao
    assert "rg_premium_single_expander" in ao
    _a = ao.index("if rg_premium_single_expander:")
    _b = ao.index("\n    _inner = (", _a)
    rg_branch = ao[_a:_b]
    assert "fdl-fat-cobertura-caption" not in rg_branch

