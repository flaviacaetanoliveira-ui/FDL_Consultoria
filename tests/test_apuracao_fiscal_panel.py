"""Smoke dos cartões HTML da Apuração Fiscal (sem Streamlit)."""

from __future__ import annotations

import html

import pytest

from app.components.apuracao_fiscal_panel import (
    _aliquota_imposto_caption_safe_html_and_divergencia_ref,
    _build_badge_regime_fora_escopo_html,
    _build_fiscal_kpis_hero_html,
    _build_fiscal_kpis_secondary_html,
)
from processing.faturamento.params import EmpresaFaturamentoEntry
from tests._helpers_fiscal import v2_min_params


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
        caption_aliquota_imposto_safe_html=html.escape("alíquota configurada: 6,0%"),
        divergencia_compare_pct=6.0,
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
        caption_aliquota_imposto_safe_html=html.escape("alíquota configurada: 6,0%"),
        divergencia_compare_pct=6.0,
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
        caption_aliquota_imposto_safe_html=html.escape("alíquota configurada: 6,0%"),
        divergencia_compare_pct=6.0,
        ok_nf_dates=True,
        fmt_brl=_fmt_brl_stub,
    )
    assert "fdl-fat-kpi-aliquota-divergencia" not in html_out


def test_aliquota_caption_empresa_unica_mostra_nome_na_legenda() -> None:
    """
    Quando o filtro tem uma única empresa com alíquota configurada,
    a caption HTML gerada por _aliquota_imposto_caption_safe_html_and_divergencia_ref
    contém o nome de exibição e o valor formatado.
    """
    aliquotas_info = {
        "modo": "unica",
        "valor_unico_pct": 9.0,
        "valores_por_empresa": {"gama_home": 9.0},
        "min_pct": 9.0,
        "max_pct": 9.0,
    }
    params = v2_min_params(
        (
            EmpresaFaturamentoEntry(
                org_id="gama_home",
                empresa="Gama Home",
                pedidos_dir="p",
                permite_faturamento_sem_nf=None,
                aliquota_imposto=0.09,
                regime_tributario="simples_nacional",
            ),
        )
    )
    caption_html, divergencia_ref = _aliquota_imposto_caption_safe_html_and_divergencia_ref(
        params_union=params,
        aliquotas_info=aliquotas_info,
        empresas_efetivas=["gama_home"],
        fallback_metadata_pct=11.0,
        ok_nf_dates=True,
    )
    assert "Gama Home" in caption_html
    assert "9,0%" in caption_html
    assert divergencia_ref == pytest.approx(9.0)


def test_aliquota_caption_multiplas_aliquotas_mostra_indicador_tooltip() -> None:
    """
    Quando o filtro tem múltiplas empresas com alíquotas diferentes,
    a caption HTML mostra indicador visual (ℹ) e tooltip com lista
    de alíquotas por empresa.
    """
    aliquotas_info = {
        "modo": "multipla",
        "valor_unico_pct": None,
        "min_pct": 9.0,
        "max_pct": 11.0,
        "valores_por_empresa": {"gama_home": 9.0, "moveis_eap": 11.0},
    }
    params = v2_min_params(
        (
            EmpresaFaturamentoEntry(
                org_id="gama_home",
                empresa="Gama Home",
                pedidos_dir="p",
                permite_faturamento_sem_nf=None,
                aliquota_imposto=0.09,
                regime_tributario="simples_nacional",
            ),
            EmpresaFaturamentoEntry(
                org_id="moveis_eap",
                empresa="Móveis EAP",
                pedidos_dir="p",
                permite_faturamento_sem_nf=None,
                aliquota_imposto=0.11,
                regime_tributario="simples_nacional",
            ),
        )
    )
    caption_html, divergencia_ref = _aliquota_imposto_caption_safe_html_and_divergencia_ref(
        params_union=params,
        aliquotas_info=aliquotas_info,
        empresas_efetivas=["gama_home", "moveis_eap"],
        fallback_metadata_pct=11.0,
        ok_nf_dates=True,
    )
    assert "múltiplas" in caption_html.lower()
    assert "ℹ" in caption_html
    assert "Gama Home" in caption_html
    assert "Móveis EAP" in caption_html
    assert divergencia_ref is None


def test_badge_aviso_nao_aparece_filtro_so_simples() -> None:
    """
    Quando nenhuma empresa do filtro está fora do escopo (só Simples),
    _build_badge_regime_fora_escopo_html retorna string vazia.
    """
    html_out = _build_badge_regime_fora_escopo_html(
        empresas_fora_escopo=[],
        regimes_nao_simples=frozenset(),
    )
    assert html_out == ""


def test_badge_aviso_aparece_com_mega_facil_lucro_presumido() -> None:
    """
    Quando Mega Fácil está no filtro com regime Lucro Presumido,
    _build_badge_regime_fora_escopo_html retorna HTML contendo nome,
    Lucro Presumido, Simples Nacional e orientação ao contador.
    """
    html_out = _build_badge_regime_fora_escopo_html(
        empresas_fora_escopo=["Mega Fácil"],
        regimes_nao_simples=frozenset({"lucro_presumido"}),
    )
    assert "Mega Fácil" in html_out
    assert "Lucro Presumido" in html_out
    assert "Simples Nacional" in html_out
    assert "contador" in html_out.lower()
    assert "<" in html_out and ">" in html_out


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
