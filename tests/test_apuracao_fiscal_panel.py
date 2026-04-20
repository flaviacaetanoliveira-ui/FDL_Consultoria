"""Smoke dos cartões HTML da Apuração Fiscal (sem Streamlit)."""

from __future__ import annotations

from app.components.apuracao_fiscal_panel import (
    _build_fiscal_kpis_hero_html,
    _build_fiscal_kpis_secondary_html,
)


def _fmt_brl_stub(v: float) -> str:
    if abs(v - 100000.0) < 1:
        return "R$ 100.000,00"
    if abs(v - 6000.0) < 1:
        return "R$ 6.000,00"
    if abs(v - 6500.0) < 1:
        return "R$ 6.500,00"
    if abs(v - 6050.0) < 1:
        return "R$ 6.050,00"
    if abs(v - 244000.0) < 1:
        return "R$ 244.000,00"
    if abs(v - 54000.0) < 1:
        return "R$ 54.000,00"
    if abs(v - 8500.0) < 1:
        return "R$ 8.500,00"
    return str(v)


def test_fiscal_kpis_hero_html_contem_labels() -> None:
    html_out = _build_fiscal_kpis_hero_html(
        base_liquida=100000,
        imposto=6000,
        aliquota_efetiva_pct=6.0,
        aliquota_configurada_pct=6.0,
        ok_nf_dates=True,
        fmt_brl=_fmt_brl_stub,
    )
    assert "BASE TRIBUTÁVEL LÍQUIDA" in html_out
    assert "IMPOSTO APURADO" in html_out
    assert "R$ 100.000,00" in html_out


def test_fiscal_kpis_alerta_divergencia_aliquota() -> None:
    html_out = _build_fiscal_kpis_hero_html(
        base_liquida=100000,
        imposto=6600,
        aliquota_efetiva_pct=6.6,
        aliquota_configurada_pct=6.0,
        ok_nf_dates=True,
        fmt_brl=_fmt_brl_stub,
    )
    assert "diverge" in html_out.lower()
    assert "fdl-fat-kpi-aliquota-divergencia" in html_out


def test_fiscal_kpis_sem_alerta_quando_aliquotas_proximas() -> None:
    html_out = _build_fiscal_kpis_hero_html(
        base_liquida=100000,
        imposto=6050,
        aliquota_efetiva_pct=6.05,
        aliquota_configurada_pct=6.0,
        ok_nf_dates=True,
        fmt_brl=_fmt_brl_stub,
    )
    assert "fdl-fat-kpi-aliquota-divergencia" not in html_out


def test_fiscal_kpis_secondary_html_contem_3_cards() -> None:
    html_out = _build_fiscal_kpis_secondary_html(
        valor_faturado_nf=244000,
        n_nf=960,
        total_devolvido=54000,
        nfs_devolucao=295,
        diferenca_lista_nf=8500,
        valor_cancelado=0,
        ok_nf_dates=True,
        fmt_brl=_fmt_brl_stub,
        fmt_int=lambda i: f"{int(i):,}".replace(",", "."),
    )
    assert "VALOR FATURADO" in html_out
    assert "DEVOLUÇÕES" in html_out
    assert "DIFERENÇA" in html_out
