"""Painel «Apuração Fiscal» — reutiliza funções de ``app_operacional`` (import tardio)."""

from __future__ import annotations

import html
import json
import logging
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Literal

import pandas as pd
import streamlit as st

from faturamento_dre_recorte import (
    _BR_TZ,
    _fdl_fr_etiquetas_empresa_recorte,
    _fdl_fr_filtrar_por_etiquetas_empresa,
    _fdl_fr_mask_nf_emissao_no_periodo,
)
from processing.faturamento.params import FaturamentoParams, FaturamentoParamsV2
from processing.faturamento.params_regime import (
    AliquotaConfiguradaInfo,
    aliquota_configurada_para_empresas_filtradas,
    detectar_regimes_tributarios,
    enrich_aliquota_ref_pct_for_stats,
    find_empresa_faturamento_entry,
    get_aliquota_imposto_por_empresa,
    load_faturamento_params_for_ui,
    resolve_faturamento_params_path_for_ui,
)
from processing.faturamento.simples_nacional import (
    ResultadoAliquotaEfetivaMes,
    ResultadoFaixaSimples,
    agregar_simples_nacional_para_painel_fiscal,
    texto_periodo_rbt12,
)
from faturamento_dre_recorte_minimo import (
    build_faturamento_fiscal_base_slice,
    build_nf_panel_aligned_to_fiscal_base,
    compute_nf_panel_kpis,
    dre_imposto_para_linha_dre_gerencial,
    enrich_faturamento_fiscal_base_stats,
    faturamento_min_series_nf_emissao_bounds_dates,
    faturamento_recorte_min_state_from_session,
    _min_cal_limits,
    _nf_fiscal_situacao_invalida,
)
from processing.faturamento.fiscal_materializado import fiscal_contract_dataframe_valid
from processing.faturamento.nf_materializado import nf_first_contract_dataframe_valid
from processing.faturamento.nf_panel_materializado import nf_panel_materializado_dataframe_valid

_ALIQUOTA_DIVERG_PP = 0.5
_LOG_AP = logging.getLogger(__name__)

FISCAL_KPIS_CSS = """<style>
.fdl-fat-kpi-hero-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 16px;
}
@media (max-width: 900px) {
  .fdl-fat-kpi-hero-grid { grid-template-columns: 1fr; }
}
.fdl-fat-kpi-hero-card {
  background: var(--color-background-primary, #ffffff);
  border: 0.5px solid var(--color-border-tertiary, #e2e8f0);
  border-radius: 12px;
  padding: 1.25rem 1.5rem;
  position: relative;
}
.fdl-fat-kpi-hero-card--base::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  background: #0F6E56;
  border-top-left-radius: 12px;
  border-bottom-left-radius: 12px;
}
.fdl-fat-kpi-hero-card--imposto::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  background: #888780;
  border-top-left-radius: 12px;
  border-bottom-left-radius: 12px;
}
.fdl-fat-kpi-hero-label {
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #64748b);
  margin-bottom: 6px;
}
.fdl-fat-kpi-hero-value {
  font-size: 28px;
  font-weight: 500;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 4px;
}
.fdl-fat-kpi-hero-caption {
  font-size: 12px;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-kpi-secondary-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
}
@media (max-width: 900px) {
  .fdl-fat-kpi-secondary-grid { grid-template-columns: 1fr; }
}
.fdl-fat-kpi-secondary-card {
  background: var(--color-background-secondary, #f8fafc);
  border-radius: 8px;
  padding: 1rem;
}
.fdl-fat-kpi-secondary-label {
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #64748b);
  margin-bottom: 4px;
}
.fdl-fat-kpi-secondary-value {
  font-size: 18px;
  font-weight: 500;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 2px;
}
.fdl-fat-kpi-secondary-caption {
  font-size: 11px;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-kpi-aliquota-divergencia {
  margin-top: 10px;
  padding: 8px 12px;
  background: #FAEEDA;
  border-radius: 8px;
  font-size: 12px;
  color: #854F0B;
}
</style>"""

COMPOSICAO_BASE_CSS = """<style>
.fdl-fat-composicao-wrap {
  margin-bottom: 20px;
  padding: 16px;
  border-radius: 12px;
  border: 0.5px solid var(--color-border-tertiary, #e2e8f0);
  background: var(--color-background-primary, #ffffff);
}
.fdl-fat-composicao-tit {
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 12px;
}
.fdl-fat-composicao-linha {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding: 6px 0;
  border-bottom: 0.5px solid var(--color-border-secondary, #f1f5f9);
  font-size: 13px;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-composicao-linha:last-child { border-bottom: none; }
.fdl-fat-composicao-mono {
  font-family: var(--font-mono, ui-monospace, monospace);
  font-size: 13px;
  color: var(--color-text-primary, #0f172a);
}
.fdl-fat-composicao-sinal-mais { color: var(--color-text-success, #0F6E56); font-weight: 600; margin-right: 6px; }
.fdl-fat-composicao-sinal-menos { color: var(--color-text-danger, #b91c1c); font-weight: 600; margin-right: 6px; }
.fdl-fat-composicao-sinal-igual { color: var(--color-text-secondary, #64748b); font-weight: 600; margin-right: 6px; }
</style>"""

ALIQUOTA_EFETIVA_CSS = """<style>
.fdl-fat-sn-wrap { margin-bottom: 20px; }
.fdl-fat-sn-tit {
  font-size: 14px;
  font-weight: 600;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 12px;
}
.fdl-fat-sn-cards {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
  margin-bottom: 14px;
}
@media (max-width: 900px) {
  .fdl-fat-sn-cards { grid-template-columns: 1fr; }
}
.fdl-fat-aliq-card {
  position: relative;
  background: var(--color-background-secondary, #f8fafc);
  border: 0.5px solid var(--color-border-tertiary, #e2e8f0);
  border-radius: 10px;
  padding: 12px 14px;
  min-height: 120px;
}
.fdl-fat-aliq-warmup { border-color: var(--color-border-secondary, #e2e8f0); }
.fdl-fat-aliq-indicator-warmup {
  position: absolute;
  top: 10px;
  right: 10px;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-text-warning, #d97706);
}
.fdl-fat-aliq-badge-calc {
  position: absolute;
  top: 8px;
  right: 10px;
  font-size: 9px;
  font-weight: 600;
  letter-spacing: 0.02em;
  padding: 2px 6px;
  border-radius: 4px;
  background: color-mix(in srgb, var(--color-text-success, #0F6E56) 12%, transparent);
  color: var(--color-text-success, #0F6E56);
}
.fdl-fat-aliq-nome {
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #64748b);
  margin-bottom: 6px;
  padding-right: 56px;
}
.fdl-fat-aliq-valor-warmup {
  font-size: 18px;
  font-weight: 500;
  color: var(--color-text-secondary, #475569);
  margin-bottom: 4px;
}
.fdl-fat-aliq-sublabel { font-size: 12px; color: var(--color-text-secondary, #64748b); margin-bottom: 8px; }
.fdl-fat-aliq-divider {
  height: 1px;
  background: var(--color-border-tertiary, #e2e8f0);
  margin: 8px 0;
}
.fdl-fat-aliq-meta {
  font-size: 11px;
  line-height: 1.45;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-aliq-card-mono {
  font-family: var(--font-mono, ui-monospace, monospace);
  font-size: 22px;
  font-weight: 500;
  color: var(--color-text-primary, #0f172a);
}
.fdl-fat-aliq-card-cap { font-size: 11px; color: var(--color-text-secondary, #475569); margin-top: 4px; padding-right: 48px; }
.fdl-fat-aliq-banner-warmup {
  display: flex;
  gap: 10px;
  align-items: flex-start;
  padding: 12px 14px;
  margin-bottom: 14px;
  border-radius: 10px;
  background: var(--color-background-warning, #FEF3C7);
  border: 0.5px solid color-mix(in srgb, var(--color-text-warning, #d97706) 35%, transparent);
}
.fdl-fat-aliq-banner-icon {
  flex-shrink: 0;
  width: 22px;
  height: 22px;
  border-radius: 50%;
  background: color-mix(in srgb, var(--color-text-warning, #92400e) 15%, transparent);
  color: var(--color-text-warning, #92400e);
  font-size: 12px;
  font-weight: 700;
  display: flex;
  align-items: center;
  justify-content: center;
}
.fdl-fat-aliq-banner-text {
  font-size: 12px;
  line-height: 1.5;
  color: var(--color-text-primary, #422006);
}
.fdl-fat-aliq-banner-text strong { color: var(--color-text-warning, #92400e); }
.fdl-fat-sn-details { margin: 10px 0 14px 0; }
.fdl-fat-sn-details > summary {
  cursor: pointer;
  font-size: 12px;
  color: var(--color-text-info, #0369a1);
  list-style-position: outside;
}
.fdl-fat-aliq-sem-expander {
  font-size: 12px;
  color: var(--color-text-secondary, #64748b);
  margin: 10px 0 14px 0;
  padding: 8px 0;
}
.fdl-fat-sn-table-wrap { overflow-x: auto; }
.fdl-fat-sn-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}
.fdl-fat-sn-table th, .fdl-fat-sn-table td {
  padding: 8px 10px;
  text-align: left;
  border-bottom: 0.5px solid var(--color-border-tertiary, #e2e8f0);
}
.fdl-fat-sn-table th {
  color: var(--color-text-tertiary, #64748b);
  font-weight: 500;
}
.fdl-fat-sn-mono { font-family: var(--font-mono, ui-monospace, monospace); }
.fdl-fat-sn-row-lp { color: var(--color-text-secondary, #64748b); }
.fdl-fat-sn-badge-lp {
  display: inline-block;
  margin-left: 8px;
  padding: 2px 8px;
  border-radius: 6px;
  font-size: 10px;
  font-weight: 600;
  background: var(--color-background-warning, #FEF3C7);
  color: var(--color-text-warning, #92400e);
}
.fdl-fat-sn-badge-json {
  display: inline-block;
  margin-left: 8px;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 600;
  background: var(--color-background-warning, #FEF3C7);
  color: var(--color-text-warning, #92400e);
}
.fdl-fat-sn-badge-calc-inline {
  display: inline-block;
  margin-left: 8px;
  padding: 2px 6px;
  border-radius: 4px;
  font-size: 10px;
  font-weight: 600;
  background: color-mix(in srgb, var(--color-text-success, #0F6E56) 12%, transparent);
  color: var(--color-text-success, #0F6E56);
}
.fdl-fat-sn-foot {
  margin-top: 10px;
  font-size: 11px;
  color: var(--color-text-tertiary, #64748b);
}
.fdl-fat-sn-foot-legend { margin-top: 6px; }
</style>"""

BADGE_REGIME_CSS = """<style>
.fdl-fat-badge-regime-aviso {
  background: #FAEEDA;
  border-left: 3px solid #BA7517;
  border-radius: 8px;
  padding: 12px 16px;
  margin-bottom: 16px;
  font-size: 13px;
  color: #412402;
}
.fdl-fat-badge-regime-header {
  font-weight: 500;
  color: #854F0B;
  margin-bottom: 4px;
}
.fdl-fat-badge-regime-body {
  font-size: 12.5px;
  line-height: 1.5;
  color: #633806;
}
</style>"""


def _fmt_pct_br(v: float) -> str:
    if v != v:  # NaN
        return "—"
    return f"{v:.1f}".replace(".", ",")


def _aliquota_configurada_pct_from_load_info(load_info: dict[str, object]) -> float:
    raw = load_info.get("faturamento_aliquota_imposto_pct")
    if isinstance(raw, (int, float)) and float(raw) > 0:
        x = float(raw)
        return x * 100.0 if x <= 1.0 else x
    path_final = load_info.get("faturamento_path_final_resolved")
    if not path_final:
        return 0.0
    try:
        meta_path = Path(str(path_final)).expanduser().resolve().parent / "metadata.json"
        if not meta_path.is_file():
            return 0.0
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        v = meta.get("aliquota_imposto_usada")
        if isinstance(v, (int, float)):
            return float(v) * 100.0
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return 0.0


def _diferenca_secondary_caption(valor_cancelado: float) -> str:
    base = "descontos/ajustes"
    if valor_cancelado > 1e-9:
        return f"{base} · cancelamentos fiscais"
    return base


def _formatar_regimes_para_aviso(regimes: frozenset[str]) -> str:
    mapa = {
        "lucro_presumido": "Lucro Presumido",
        "lucro_real": "Lucro Real",
        "mei": "MEI",
        "simples_nacional": "Simples Nacional",
    }
    xs = [mapa.get(r, r) for r in sorted(regimes) if r != "simples_nacional"]
    return ", ".join(xs) if xs else ""


def _build_badge_regime_fora_escopo_html(
    empresas_fora_escopo: list[str],
    regimes_nao_simples: frozenset[str],
) -> str:
    if not empresas_fora_escopo:
        return ""
    emp_txt = html.escape(", ".join(empresas_fora_escopo))
    reg_txt = html.escape(_formatar_regimes_para_aviso(regimes_nao_simples))
    return (
        '<div class="fdl-fat-badge-regime-aviso">'
        '<div class="fdl-fat-badge-regime-header">'
        f"⚠ Empresa(s) em regime especial: {emp_txt}"
        "</div>"
        '<div class="fdl-fat-badge-regime-body">'
        "Este módulo está calibrado para <strong>Simples Nacional</strong>. "
        f"{emp_txt} opera em <strong>{reg_txt}</strong> — os valores exibidos são "
        "<strong>agregados</strong>. Para apuração oficial dessa(s) empresa(s), consulte o contador."
        "</div>"
        "</div>"
    )


def _aliquota_imposto_caption_safe_html_and_divergencia_ref(
    *,
    params_union: FaturamentoParams | FaturamentoParamsV2 | None,
    aliquotas_info: AliquotaConfiguradaInfo,
    empresas_efetivas: list[str],
    fallback_metadata_pct: float,
    ok_nf_dates: bool,
) -> tuple[str, float | None]:
    """HTML seguro para a legenda do cartão Imposto + referência para alerta de divergência."""
    if not ok_nf_dates:
        return html.escape("alíquota configurada indisponível"), None

    if params_union is None:
        fp = fallback_metadata_pct
        return html.escape(f"alíquota configurada: {_fmt_pct_br(fp)}%"), fp if fp > 1e-9 else None

    modo = aliquotas_info["modo"]
    if modo == "desconhecida":
        return html.escape("alíq. não declarada no params"), None

    vu = aliquotas_info["valor_unico_pct"]

    if modo == "unica" and vu is not None:
        if len(empresas_efetivas) == 1 and isinstance(params_union, FaturamentoParamsV2):
            ent = find_empresa_faturamento_entry(params_union, empresas_efetivas[0])
            nome = ent.empresa if ent else empresas_efetivas[0]
            cap = f"alíq. {nome}: {_fmt_pct_br(vu)}%"
        else:
            cap = f"alíq. configurada: {_fmt_pct_br(vu)}%"
        return html.escape(cap), float(vu)

    if modo == "multipla":
        tip_parts: list[str] = []
        if isinstance(params_union, FaturamentoParamsV2):
            for oid, pct in sorted(aliquotas_info["valores_por_empresa"].items()):
                ent = next((e for e in params_union.empresas if e.org_id == oid), None)
                nome = ent.empresa if ent else oid
                tip_parts.append(f"{nome}: {_fmt_pct_br(pct)}%")
        tip_esc = html.escape(" | ".join(tip_parts))
        inner = (
            "alíq. múltiplas "
            f'<span title="{tip_esc}" aria-label="Detalhe por empresa">ℹ</span>'
        )
        return inner, None

    fp = fallback_metadata_pct
    return html.escape(f"alíquota configurada: {_fmt_pct_br(fp)}%"), fp if fp > 1e-9 else None


def _build_fiscal_kpis_hero_html(
    *,
    base_liquida: float,
    imposto: float,
    aliquota_efetiva_pct: float,
    caption_aliquota_imposto_safe_html: str,
    divergencia_compare_pct: float | None,
    ok_nf_dates: bool,
    fmt_brl: Callable[[float], str],
) -> str:
    """
    Dois cartões hero: Base Tributável Líquida e Imposto Apurado (Hierarquia B, ~28px).
    ``caption_aliquota_imposto_safe_html`` já escaped ou HTML seguro (multiselect ℹ).
    """
    dash = "—"
    if ok_nf_dates:
        v_base = html.escape(fmt_brl(float(base_liquida)))
        v_imp = html.escape(fmt_brl(float(imposto)))
        cap_ef = f"alíquota efetiva: {_fmt_pct_br(aliquota_efetiva_pct)}%"
        cap_cfg_inner = caption_aliquota_imposto_safe_html
    else:
        v_base = dash
        v_imp = dash
        cap_ef = "alíquota efetiva indisponível"
        cap_cfg_inner = html.escape("alíquota configurada indisponível")

    alert_block = ""
    if (
        ok_nf_dates
        and divergencia_compare_pct is not None
        and divergencia_compare_pct > 1e-9
        and abs(aliquota_efetiva_pct - divergencia_compare_pct) > _ALIQUOTA_DIVERG_PP
    ):
        alert_block = (
            '<div class="fdl-fat-kpi-aliquota-divergencia">'
            "⚠ Alíquota efetiva ("
            f"{html.escape(_fmt_pct_br(aliquota_efetiva_pct))}%) diverge da configurada ("
            f"{html.escape(_fmt_pct_br(divergencia_compare_pct))}%). Verificar composição."
            "</div>"
        )

    return (
        '<div class="fdl-fat-kpi-hero-grid">'
        '<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--base">'
        '<div class="fdl-fat-kpi-hero-label">BASE TRIBUTÁVEL LÍQUIDA</div>'
        f'<div class="fdl-fat-kpi-hero-value">{v_base}</div>'
        f'<div class="fdl-fat-kpi-hero-caption">{html.escape(cap_ef)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--imposto">'
        '<div class="fdl-fat-kpi-hero-label">IMPOSTO APURADO</div>'
        f'<div class="fdl-fat-kpi-hero-value">{v_imp}</div>'
        f'<div class="fdl-fat-kpi-hero-caption">{cap_cfg_inner}</div>'
        "</div>"
        "</div>"
        f"{alert_block}"
    )


def _build_fiscal_kpis_secondary_html(
    *,
    valor_faturado_nf: float,
    n_nf: int,
    total_devolvido: float,
    nfs_devolucao: int,
    diferenca_lista_nf: float,
    valor_cancelado: float,
    ok_nf_dates: bool,
    fmt_brl: Callable[[float], str],
    fmt_int: Callable[[int], str],
) -> str:
    """Três cartões secundários (~18px): Valor Faturado (NF), Devoluções, Diferença (lista − NF)."""
    dash = "—"
    if ok_nf_dates:
        vf = html.escape(fmt_brl(float(valor_faturado_nf)))
        dv = html.escape(fmt_brl(float(total_devolvido)))
        df = html.escape(fmt_brl(float(diferenca_lista_nf)))
        cap_nf = f"{fmt_int(int(n_nf))} NFs emitidas"
        cap_dev = f"{fmt_int(int(nfs_devolucao))} NFs de entrada"
    else:
        vf = dv = df = dash
        cap_nf = cap_dev = "período indisponível"

    cap_dif = _diferenca_secondary_caption(float(valor_cancelado))

    return (
        '<div class="fdl-fat-kpi-secondary-grid">'
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">VALOR FATURADO (NF)</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{vf}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_nf)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">DEVOLUÇÕES DO PERÍODO</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{dv}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_dev)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">DIFERENÇA (LISTA − NF)</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{df}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_dif)}</div>'
        "</div>"
        "</div>"
    )


def _apuracao_org_ids_do_filtro(
    params_union: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> list[str]:
    """Resolve rótulos do multiselect para ``org_id`` quando params V2 disponível."""
    from processing.faturamento.imposto_consolidado import org_ids_do_filtro_ui

    return org_ids_do_filtro_ui(params_union, empresas_chaves)


def _apuracao_org_ids_resolvidos_para_df(
    df: pd.DataFrame,
    params_union: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> list[str]:
    """Mapeia rótulos de UI para ``org_id`` da base fiscal quando ``params_union`` não resolve (ex.: Cloud)."""
    from processing.faturamento.imposto_consolidado import resolver_org_ids_para_consolidacao_imposto

    return resolver_org_ids_para_consolidacao_imposto(df, params_union, empresas_chaves)


def _classificar_nf_invalida_por_situacao(situacao: object) -> Literal["cancel", "deneg", "inutil", "outro"]:
    """Classifica situação inválida (já filtrada por ``_nf_fiscal_situacao_invalida``) para o expander."""
    s = str(situacao).strip().lower()
    if "inutil" in s:
        return "inutil"
    if "deneg" in s:
        return "deneg"
    if "cancel" in s:
        return "cancel"
    return "outro"


def _agregar_invalidas_por_tipo_no_periodo(
    df_fiscal: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
    ok_nf_dates: bool,
) -> tuple[tuple[int, float], tuple[int, float], int]:
    """
    Retorna ``((n_cancel, R$ cancel), (n_deneg, R$ deneg), n_inutil)`` no mesmo recorte empresa + emissão.
    Inutilizadas: apenas contagem (valor exibido como 0 na UI).
    """
    z = (0, 0.0), (0, 0.0), 0
    if df_fiscal.empty or not ok_nf_dates or nf_d_fim < nf_d_ini:
        return z
    need = {"Nota_Data_Emissao", "Nota_Situacao", "Nota_Numero_Normalizado", "empresa", "Valor_Liquido_NF"}
    if not need.issubset(df_fiscal.columns):
        return z
    out = df_fiscal.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(out)
    if emp_opts and empresas_sel:
        out = _fdl_fr_filtrar_por_etiquetas_empresa(out, list(empresas_sel))
    if out.empty:
        return z
    m_period = _fdl_fr_mask_nf_emissao_no_periodo(out["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    out = out.loc[m_period].copy()
    if out.empty or "Nota_Situacao" not in out.columns:
        return z
    inv = _nf_fiscal_situacao_invalida(out["Nota_Situacao"])
    inv_df = out.loc[inv].copy()
    if inv_df.empty:
        return z
    gb_keys: list[str] = []
    if "org_id" in inv_df.columns:
        gb_keys.append("org_id")
    gb_keys.extend(["empresa", "Nota_Numero_Normalizado"])
    tip = inv_df["Nota_Situacao"].map(_classificar_nf_invalida_por_situacao)
    inv_df = inv_df.assign(_tipo=tip)
    vl = pd.to_numeric(inv_df["Valor_Liquido_NF"], errors="coerce").fillna(0.0)
    inv_df = inv_df.assign(_vl=vl)

    def _agg(t: str) -> tuple[int, float]:
        sub = inv_df.loc[inv_df["_tipo"] == t]
        if sub.empty:
            return 0, 0.0
        g = sub.groupby(gb_keys, sort=False)["_vl"].sum()
        return int(len(g)), float(g.sum())

    nc, vc = _agg("cancel")
    nd, vd = _agg("deneg")
    ni, _ = _agg("inutil")
    no, _ = _agg("outro")
    ni += no
    return (nc, vc), (nd, vd), ni


def _count_nf_canceladas_periodo(
    df_fiscal: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
    ok_nf_dates: bool,
) -> int:
    if df_fiscal.empty or not ok_nf_dates or nf_d_fim < nf_d_ini:
        return 0
    need = {"Nota_Data_Emissao", "Nota_Situacao", "Nota_Numero_Normalizado", "empresa"}
    if not need.issubset(df_fiscal.columns):
        return 0
    out = df_fiscal.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(out)
    if emp_opts and empresas_sel:
        out = _fdl_fr_filtrar_por_etiquetas_empresa(out, list(empresas_sel))
    if out.empty:
        return 0
    m_period = _fdl_fr_mask_nf_emissao_no_periodo(out["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    out = out.loc[m_period].copy()
    if out.empty or "Nota_Situacao" not in out.columns:
        return 0
    inv = _nf_fiscal_situacao_invalida(out["Nota_Situacao"])
    out = out.loc[inv].copy()
    if out.empty:
        return 0
    gb_keys: list[str] = []
    if "org_id" in out.columns:
        gb_keys.append("org_id")
    gb_keys.extend(["empresa", "Nota_Numero_Normalizado"])
    return int(out.groupby(gb_keys, sort=False).ngroups)


def _build_composicao_base_tributavel_html(
    *,
    valor_faturado: float,
    nfs_emitidas: int,
    valor_cancelado: float,
    nfs_canceladas: int,
    valor_devolucoes: float,
    nfs_devolucoes: int,
    base_liquida: float,
    fmt_brl: Callable[[float], str],
    fmt_int: Callable[[int], str],
) -> str:
    """Bloco «Composição da Base Tributável» — DRE fiscal visual."""
    vf = html.escape(fmt_brl(float(valor_faturado)))
    vc = html.escape(fmt_brl(float(valor_cancelado)))
    vd = html.escape(fmt_brl(float(valor_devolucoes)))
    bl = html.escape(fmt_brl(float(base_liquida)))
    return (
        '<div class="fdl-fat-composicao-wrap">'
        '<div class="fdl-fat-composicao-tit">Composição da Base Tributável</div>'
        '<div class="fdl-fat-composicao-linha">'
        '<span><span class="fdl-fat-composicao-sinal-mais">(+)</span>Valor faturado (NF)</span>'
        f'<span class="fdl-fat-composicao-mono">{vf}</span>'
        "</div>"
        f'<div style="font-size:11px;color:var(--color-text-tertiary,#64748b);margin:-4px 0 6px 22px;">'
        f"{html.escape(fmt_int(int(nfs_emitidas)))} NFs no período (todas as situações)"
        "</div>"
        '<div class="fdl-fat-composicao-linha">'
        '<span><span class="fdl-fat-composicao-sinal-menos">(−)</span>Cancelamentos fiscais</span>'
        f'<span class="fdl-fat-composicao-mono">{vc}</span>'
        "</div>"
        f'<div style="font-size:11px;color:var(--color-text-tertiary,#64748b);margin:-4px 0 6px 22px;">'
        f"{html.escape(fmt_int(int(nfs_canceladas)))} NFs canceladas / denegadas / inutilizadas"
        "</div>"
        '<div class="fdl-fat-composicao-linha">'
        '<span><span class="fdl-fat-composicao-sinal-menos">(−)</span>Devoluções do período</span>'
        f'<span class="fdl-fat-composicao-mono">{vd}</span>'
        "</div>"
        f'<div style="font-size:11px;color:var(--color-text-tertiary,#64748b);margin:-4px 0 6px 22px;">'
        f"{html.escape(fmt_int(int(nfs_devolucoes)))} NFs de entrada (devolução)"
        "</div>"
        '<div class="fdl-fat-composicao-linha">'
        '<span><span class="fdl-fat-composicao-sinal-igual">(=)</span>Base tributável líquida</span>'
        f'<span class="fdl-fat-composicao-mono">{bl}</span>'
        "</div>"
        "</div>"
    )


def _fmt_pct_br2(v: float | None) -> str:
    if v is None or v != v:
        return "—"
    return f"{v:.2f}".replace(".", ",")


def _build_calculo_detalhado_expander_html(
    empresa_slug: str,
    empresa_nome: str,
    resultado: ResultadoAliquotaEfetivaMes,
    *,
    fmt_brl: Callable[[float], str],
) -> str:
    """Conteúdo do expander: passo a passo do cálculo para a competência de referência."""
    nome = html.escape(empresa_nome)
    slug = html.escape(empresa_slug)
    per = texto_periodo_rbt12(resultado.competencia)
    rbt = html.escape(fmt_brl(float(resultado.rbt12)))
    if resultado.faixa is None or resultado.aliquota_efetiva_pct is None:
        motivo = html.escape(resultado.motivo_indisponivel or "Indisponível")
        return (
            f'<div data-org="{slug}">'
            f"<p><strong>{nome}</strong></p>"
            f"<p>RBT12 (janela {html.escape(per)}): <span class=\"fdl-fat-sn-mono\">{rbt}</span></p>"
            f"<p>{motivo}</p>"
            "<p>Referência: LC 123/2006, art. 18, §1º (Simples Nacional — Anexo I).</p>"
            "</div>"
        )
    fx: ResultadoFaixaSimples = resultado.faixa
    nom_pct = html.escape(_fmt_pct_br2(fx.aliquota_nominal_pct))
    parc = html.escape(fmt_brl(float(fx.parcela_deduzir)))
    num = float(resultado.rbt12) * (fx.aliquota_nominal_pct / 100.0) - float(fx.parcela_deduzir)
    num_s = html.escape(fmt_brl(float(num)))
    ef = html.escape(_fmt_pct_br2(resultado.aliquota_efetiva_pct))
    rbt_fmt = html.escape(fmt_brl(float(resultado.rbt12)))
    return (
        f'<div data-org="{slug}">'
        f"<p><strong>{nome}</strong> · competência {html.escape(resultado.competencia.strftime('%m/%Y'))}</p>"
        "<ol style=\"margin:0;padding-left:18px;font-size:12px;color:var(--color-text-secondary,#475569);\">"
        f"<li>RBT12 (soma dos 12 meses anteriores: {html.escape(per)}): "
        f'<span class="fdl-fat-sn-mono">{rbt}</span></li>'
        f"<li>Faixa Anexo I: <strong>nº {int(fx.faixa_numero)}</strong> "
        f"(RBT entre {html.escape(fmt_brl(fx.rbt12_min))} e {html.escape(fmt_brl(fx.rbt12_max))})</li>"
        f"<li>Alíquota nominal: <span class=\"fdl-fat-sn-mono\">{nom_pct}%</span> · "
        f"Parcela a deduzir: <span class=\"fdl-fat-sn-mono\">{parc}</span></li>"
        "</ol>"
        "<p style=\"font-size:12px;color:var(--color-text-secondary,#475569);\">Fórmula (LC 123/2006, art. 18, §1º):</p>"
        "<p class=\"fdl-fat-sn-mono\" style=\"font-size:12px;\">"
        f"(({rbt_fmt} × {nom_pct}%) − {parc}) ÷ {rbt_fmt} = {ef}%"
        "</p>"
        f"<p style=\"font-size:12px;\">Numerador (RBT12 × alíq. nominal − parcela): <span class=\"fdl-fat-sn-mono\">{num_s}</span></p>"
        f"<p style=\"font-size:12px;\"><strong>Alíquota efetiva:</strong> <span class=\"fdl-fat-sn-mono\">{ef}%</span></p>"
        "</div>"
    )


def _tem_alguma_empresa_simples_no_filtro(simples_agregado: dict[str, Any]) -> bool:
    for v in simples_agregado.get("por_empresa", {}).values():
        if isinstance(v, dict) and v.get("regime") == "simples_nacional":
            return True
    return False


def _build_aliquota_efetiva_simples_html(
    simples_agregado: dict[str, Any],
    *,
    fmt_brl: Callable[[float], str],
) -> str:
    """Bloco «Alíquota Efetiva por Empresa · Simples Nacional» (calculado, warm-up JSON, L. Presumido)."""
    por: dict[str, Any] = simples_agregado.get("por_empresa", {})
    cards: list[str] = []
    for oid, row in por.items():
        if not isinstance(row, dict) or row.get("regime") != "simples_nacional":
            continue
        nome_esc = html.escape(str(row.get("empresa_nome", oid)).upper())
        nome_plain = str(row.get("empresa_nome", oid))
        ult = row.get("ultimo_mes")
        oa = row.get("origem_aliquota")
        json_pct = row.get("aliquota_referencia_json_pct")
        json_s = html.escape(_fmt_pct_br2(float(json_pct))) if isinstance(json_pct, (int, float)) else "—"

        if oa == "referencia_json" and isinstance(ult, ResultadoAliquotaEfetivaMes):
            n_m = int(ult.meses_historico_disponiveis)
            cards.append(
                '<div class="fdl-fat-aliq-card fdl-fat-aliq-warmup">'
                '<span class="fdl-fat-aliq-indicator-warmup" aria-hidden="true"></span>'
                f'<div class="fdl-fat-aliq-nome">{nome_esc}</div>'
                '<div class="fdl-fat-aliq-valor-warmup">Histórico parcial</div>'
                f'<div class="fdl-fat-aliq-sublabel">{html.escape(str(n_m))} de 12 meses disponíveis</div>'
                '<div class="fdl-fat-aliq-divider"></div>'
                '<div class="fdl-fat-aliq-meta">'
                f"RBT12 parcial: {html.escape(fmt_brl(float(ult.rbt12)))}<br/>"
                f"Referência JSON: {json_s}%"
                "</div>"
                "</div>"
            )
        elif (
            isinstance(ult, ResultadoAliquotaEfetivaMes)
            and ult.rbt12_suficiente
            and ult.aliquota_efetiva_pct is not None
        ):
            pct = html.escape(_fmt_pct_br2(ult.aliquota_efetiva_pct))
            cap = html.escape(f"Faixa {ult.faixa.faixa_numero if ult.faixa else '—'} · RBT12 {fmt_brl(float(ult.rbt12))}")
            cards.append(
                '<div class="fdl-fat-aliq-card fdl-fat-aliq-card--calc">'
                '<span class="fdl-fat-aliq-badge-calc">Calculado</span>'
                f'<div class="fdl-fat-aliq-nome">{html.escape(nome_plain)}</div>'
                f'<div class="fdl-fat-aliq-card-mono">{pct}%</div>'
                f'<div class="fdl-fat-aliq-card-cap">{cap}</div>'
                "</div>"
            )
        elif isinstance(ult, ResultadoAliquotaEfetivaMes):
            cap = html.escape(ult.motivo_indisponivel or "Indisponível")
            cards.append(
                '<div class="fdl-fat-aliq-card">'
                f'<div class="fdl-fat-aliq-nome">{html.escape(nome_plain)}</div>'
                '<div class="fdl-fat-aliq-valor-warmup">—</div>'
                f'<div class="fdl-fat-aliq-card-cap">{cap}</div>'
                "</div>"
            )
        else:
            cards.append(
                '<div class="fdl-fat-aliq-card">'
                f'<div class="fdl-fat-aliq-nome">{html.escape(nome_plain)}</div>'
                '<div class="fdl-fat-aliq-valor-warmup">—</div>'
                "</div>"
            )
    cards_html = "".join(cards) if cards else '<div class="fdl-fat-aliq-card"><div class="fdl-fat-aliq-nome">—</div></div>'

    warmup_slugs = list(simples_agregado.get("empresas_em_warmup") or [])
    warmup_nomes = [
        str(por[s].get("empresa_nome", s))
        for s in warmup_slugs
        if isinstance(por.get(s), dict)
    ]
    banner_html = ""
    if warmup_nomes:
        lista = html.escape(", ".join(warmup_nomes))
        banner_html = (
            '<div class="fdl-fat-aliq-banner-warmup">'
            '<div class="fdl-fat-aliq-banner-icon">i</div>'
            '<div class="fdl-fat-aliq-banner-text">'
            f"<strong>Histórico fiscal incompleto para {lista}</strong><br/>"
            "O cálculo da alíquota efetiva pela fórmula oficial do Simples Nacional "
            "requer receita dos 12 meses anteriores à competência. Quando mais meses "
            "forem materializados, a alíquota calculada aparecerá automaticamente. "
            "No momento, estas empresas usam a alíquota de referência configurada "
            "no arquivo de parâmetros."
            "</div></div>"
        )

    oficiais = list(simples_agregado.get("empresas_com_calculo_oficial") or [])
    oid_exp: str | None = None
    max_base = -1.0
    for o in oficiais:
        rw = por.get(o)
        if not isinstance(rw, dict):
            continue
        b = rw.get("base_liquida_periodo")
        if isinstance(b, (int, float)) and float(b) > max_base:
            max_base = float(b)
            oid_exp = o
    expander_or_nota = ""
    if oid_exp is not None:
        row_e = por[oid_exp]
        ult_e = row_e.get("ultimo_mes")
        if isinstance(ult_e, ResultadoAliquotaEfetivaMes):
            inner = _build_calculo_detalhado_expander_html(
                oid_exp,
                str(row_e.get("empresa_nome", oid_exp)),
                ult_e,
                fmt_brl=fmt_brl,
            )
            nome_e = html.escape(str(row_e.get("empresa_nome", oid_exp)))
            expander_or_nota = (
                "<details class=\"fdl-fat-sn-details\">"
                f"<summary>▸ Ver cálculo detalhado — {nome_e}</summary>"
                f"<div style=\"margin-top:8px;\">{inner}</div>"
                "</details>"
            )
    else:
        expander_or_nota = (
            "<p class=\"fdl-fat-aliq-sem-expander\">"
            "Quando pelo menos uma empresa tiver 12 meses de histórico, o passo a passo do cálculo oficial aparecerá aqui."
            "</p>"
        )

    rows_tb: list[str] = []
    for oid, row in sorted(por.items(), key=lambda kv: str(kv[1].get("empresa_nome", kv[0]) if isinstance(kv[1], dict) else kv[0])):
        if not isinstance(row, dict):
            continue
        nome = html.escape(str(row.get("empresa_nome", oid)))
        reg = str(row.get("regime", ""))
        base_l = row.get("base_liquida_periodo")
        base_s = html.escape(fmt_brl(float(base_l))) if isinstance(base_l, (int, float)) else "—"
        imp = row.get("imposto_calculado_periodo")
        imp_s = html.escape(fmt_brl(float(imp))) if isinstance(imp, (int, float)) else "—"
        oa = row.get("origem_aliquota")
        pond = row.get("aliquota_efetiva_ponderada_periodo_pct")
        if pond is None:
            pond = row.get("aliquota_media_periodo_pct")
        json_ref = row.get("aliquota_referencia_json_pct")

        badge = ""
        tr_cls = ""
        if reg == "lucro_presumido":
            tr_cls = "fdl-fat-sn-row-lp"
            badge = '<span class="fdl-fat-sn-badge-lp">[L. Presumido]</span>'
            imp_s = html.escape("cálculo em desenvolvimento")
            ali_s = "—"
        elif oa == "referencia_json":
            badge = '<span class="fdl-fat-sn-badge-json">[JSON]</span>'
            jr = float(json_ref) if isinstance(json_ref, (int, float)) else None
            if jr is not None:
                ali_s = html.escape(_fmt_pct_br2(jr)) + "% (ref.)"
            else:
                ali_s = "—"
        elif oa == "calculada" and isinstance(pond, (int, float)):
            badge = '<span class="fdl-fat-sn-badge-calc-inline">[Calculado]</span>'
            ali_s = html.escape(_fmt_pct_br2(float(pond))) + "%"
        else:
            if isinstance(pond, (int, float)):
                ali_s = html.escape(_fmt_pct_br2(float(pond))) + "%"
            else:
                ali_s = "—"

        rows_tb.append(
            f'<tr class="{tr_cls}">'
            f"<td>{nome}{badge}</td>"
            f'<td class="fdl-fat-sn-mono">{base_s}</td>'
            f'<td class="fdl-fat-sn-mono">{ali_s}</td>'
            f'<td class="fdl-fat-sn-mono">{imp_s}</td>'
            "</tr>"
        )

    tot = simples_agregado.get("total_simples", {})
    tb = float(tot.get("base_liquida", 0.0)) if isinstance(tot, dict) else 0.0
    ti = float(tot.get("imposto_total", 0.0)) if isinstance(tot, dict) else 0.0
    tap = tot.get("aliquota_media_ponderada_pct") if isinstance(tot, dict) else None
    if isinstance(tap, (int, float)):
        tap_cell = html.escape(_fmt_pct_br2(float(tap))) + "%"
    else:
        tap_cell = "—"
    tot_line = (
        "<tr>"
        "<td><strong>Total Simples</strong></td>"
        f'<td class="fdl-fat-sn-mono"><strong>{html.escape(fmt_brl(tb))}</strong></td>'
        f'<td class="fdl-fat-sn-mono"><strong>{tap_cell}</strong></td>'
        f'<td class="fdl-fat-sn-mono"><strong>{html.escape(fmt_brl(ti))}</strong></td>'
        "</tr>"
    )

    return (
        '<div class="fdl-fat-sn-wrap">'
        '<div class="fdl-fat-sn-tit">Alíquota Efetiva por Empresa · Simples Nacional</div>'
        f'<div class="fdl-fat-sn-cards">{cards_html}</div>'
        f"{banner_html}"
        f"{expander_or_nota}"
        '<div class="fdl-fat-sn-table-wrap">'
        '<table class="fdl-fat-sn-table">'
        "<thead><tr>"
        "<th>Empresa</th><th>Base líquida (período)</th><th>Alíquota</th><th>Imposto (SN estimado)</th>"
        "</tr></thead>"
        "<tbody>"
        f"{''.join(rows_tb)}"
        f"{tot_line}"
        "</tbody></table></div>"
        '<p class="fdl-fat-sn-foot">Alíquota efetiva conforme LC 123/2006, art. 18, §1º, com tabela do Anexo I (LC 155/2016). '
        "Imposto estimado = soma da receita bruta mensal válida × alíquota aplicada no mês (fórmula oficial ou referência JSON em warm-up).</p>"
        '<p class="fdl-fat-sn-foot fdl-fat-sn-foot-legend">'
        "[JSON] indica alíquota de referência configurada (histórico incompleto). "
        "[Calculado] indica alíquota efetiva pela fórmula oficial LC 123/2006."
        "</p>"
        "</div>"
    )


def render_apuracao_fiscal_panel(
    df: pd.DataFrame,
    load_info: dict[str, object],
    ts_proc: str,
    *,
    org_id: str,
) -> None:
    import app_operacional as ao

    ao._fdl_fat_min_inject_ui_styles()

    use_nf_panel_baked = bool(load_info.get("faturamento_nf_panel_baked"))
    _df_nf_panel = load_info.get("faturamento_nf_panel_df")
    _df_nf_contract = load_info.get("faturamento_nf_df")
    use_nf_panel_baked_effective = (
        use_nf_panel_baked
        and isinstance(_df_nf_panel, pd.DataFrame)
        and nf_panel_materializado_dataframe_valid(_df_nf_panel)
    )
    df_nf_pre = _df_nf_panel if use_nf_panel_baked_effective else _df_nf_contract
    use_nf_materializado = False
    if use_nf_panel_baked_effective:
        use_nf_materializado = isinstance(df_nf_pre, pd.DataFrame) and not df_nf_pre.empty
    elif (
        bool(load_info.get("faturamento_nf_first"))
        and isinstance(df_nf_pre, pd.DataFrame)
        and nf_first_contract_dataframe_valid(df_nf_pre)
    ):
        use_nf_materializado = True
    if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame) and df_nf_pre.empty and not df.empty:
        use_nf_materializado = False

    _nf_panel_ads_ui = bool(load_info.get("faturamento_nf_panel_ads", True))

    df_fiscal_pre = load_info.get("faturamento_fiscal_df")
    df_devolucoes_pre = load_info.get("faturamento_devolucoes_df")
    _df_dev_ok = isinstance(df_devolucoes_pre, pd.DataFrame)
    use_fiscal_parquet = (
        bool(load_info.get("faturamento_fiscal_first"))
        and isinstance(df_fiscal_pre, pd.DataFrame)
        and fiscal_contract_dataframe_valid(df_fiscal_pre)
    )
    use_fiscal_kpi = bool(
        use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame) and fiscal_contract_dataframe_valid(df_fiscal_pre)
    )

    if not use_nf_panel_baked_effective:
        st.error(
            "**Dados por nota fiscal indisponíveis.** "
            "Esta área usa a base consolidada **já publicada** para a sua organização."
        )
        st.caption(
            "Peça a atualização pelo processo habitual de fecho ou aguarde a próxima publicação de dados."
        )
        _pe = load_info.get("faturamento_nf_panel_error")
        if _pe:
            _pe_line = (
                f"Erro técnico: `{html.escape(str(_pe))}`"
                if ao._is_admin_mode()
                else "Não foi possível carregar a base consolidada. Tente novamente mais tarde ou contacte o suporte."
            )
            st.caption(_pe_line)
        elif not use_nf_panel_baked:
            st.caption(
                "A base consolidada ainda não está disponível neste ambiente ou não foi encontrada. "
                "Volte mais tarde ou contacte o suporte."
            )
        elif not isinstance(_df_nf_panel, pd.DataFrame):
            st.caption("Não foi possível preparar a tabela neste momento. Recarregue a página ou tente mais tarde.")
        elif _df_nf_panel.empty:
            st.info(
                "Não há linhas para o período e empresa selecionados. "
                "Verifique filtros ou o escopo na barra lateral."
            )
        elif ao._is_admin_mode():
            st.warning(
                "Contrato do painel incompleto (faltam colunas obrigatórias). "
                "Rematerialize o faturamento com a versão atual do pipeline."
            )
        return

    if df.empty and not use_nf_materializado:
        st.info(
            "Sem dados de faturamento para este escopo. Confirme **materialização**, **slug** do cliente "
            "e o **escopo** (empresa ativa / consolidado) na barra lateral."
        )
        return

    _bounds_parts: list[pd.DataFrame] = []
    _base_bounds = df_nf_pre if use_nf_materializado else df
    if isinstance(_base_bounds, pd.DataFrame) and not _base_bounds.empty:
        _bounds_parts.append(_base_bounds)
    if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame) and not df_fiscal_pre.empty:
        _bounds_parts.append(df_fiscal_pre)
    _df_bounds = (
        pd.concat(_bounds_parts, ignore_index=True)
        if len(_bounds_parts) > 1
        else (_bounds_parts[0] if _bounds_parts else pd.DataFrame())
    )
    if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame) and df_nf_pre.empty:
        st.info(
            "Sem notas fiscais neste recorte. Confirme filtros de data, empresa e consolidado na barra lateral "
            "e que a base consolidada está atualizada."
        )
        return
    nf_min, nf_max, ok_nf_dates = faturamento_min_series_nf_emissao_bounds_dates(_df_bounds)
    _emit_floor = ao._FDL_FAT_DRE_MIN_PANEL_NF_EMISSAO_DESDE
    if ok_nf_dates:
        nf_cal_min, nf_cal_max = _min_cal_limits(nf_min, nf_max)
        nf_cal_min = max(nf_cal_min, _emit_floor)
        if nf_max >= _emit_floor:
            nf_min = max(nf_min, _emit_floor)
        else:
            nf_min, nf_max = _emit_floor, _emit_floor
        nf_cal_max = max(nf_cal_max, nf_max, nf_min, nf_cal_min)
    else:
        nf_cal_min, nf_cal_max = (nf_min, nf_max)

    _nf_sig_k = "fdl_apu_nf_bounds_sig"
    _today = datetime.now(_BR_TZ).date()
    if ok_nf_dates:
        _nf_bs = (nf_min.isoformat(), nf_max.isoformat())
        if st.session_state.get(_nf_sig_k) != _nf_bs:
            st.session_state[_nf_sig_k] = _nf_bs
            _nfi = nf_min
            _nff = min(nf_max, _today)
            _nfi = min(max(_nfi, nf_cal_min), nf_cal_max)
            _nff = min(max(_nff, nf_cal_min), nf_cal_max)
            if _nff < _nfi:
                _nff = _nfi
            st.session_state["fdl_apu_nf_d_ini"] = _nfi
            st.session_state["fdl_apu_nf_d_fim"] = _nff
        if "fdl_apu_nf_d_ini" not in st.session_state:
            _nfi = nf_min
            _nff = min(nf_max, _today)
            _nfi = min(max(_nfi, nf_cal_min), nf_cal_max)
            _nff = min(max(_nff, nf_cal_min), nf_cal_max)
            if _nff < _nfi:
                _nff = _nfi
            st.session_state["fdl_apu_nf_d_ini"] = _nfi
            st.session_state["fdl_apu_nf_d_fim"] = _nff
        st.session_state["fdl_apu_nf_d_ini"] = min(
            max(ao._safe_streamlit_date(st.session_state["fdl_apu_nf_d_ini"], nf_min), nf_cal_min),
            nf_cal_max,
        )
        st.session_state["fdl_apu_nf_d_fim"] = min(
            max(ao._safe_streamlit_date(st.session_state["fdl_apu_nf_d_fim"], nf_max), nf_cal_min),
            nf_cal_max,
        )

    emp_opts = ao._faturamento_dre_etiquetas_empresa_recorte(_df_bounds)

    _FDL_APURACAO_RESET_KEYS = (
        "fdl_apu_emp",
        "fdl_apu_plat",
        "fdl_apu_nf_sit",
        "fdl_apu_nf_d_ini",
        "fdl_apu_nf_d_fim",
        "fdl_apu_nf_bounds_sig",
        "fdl_apu_prod",
        "fdl_apu_venda_sinal",
        "fdl_apu_sinal_resultado",
        "fdl_apu_sinais_resultado",
        "fdl_apu_nf_show_diferenca",
        "fdl_apu_nf_opt_plat",
        "fdl_apu_nf_opt_sit",
        "fdl_apu_nf_opt_ped",
        "fdl_apu_nf_opt_linhas",
        "fdl_apu_nf_opt_qtd",
        "fdl_apu_nf_opt_vf",
        "fdl_apu_nf_opt_rf",
        "fdl_apu_nf_opt_rp",
        "fdl_apu_nf_opt_tar",
        "fdl_apu_nf_opt_df",
        "fdl_apu_nf_opt_ads",
        "fdl_apu_nf_opt_alert",
        "fdl_apu_nf_tbl_plataforma",
        "fdl_apu_nf_tbl_busca",
        "fdl_apu_nf_pg",
    )

    with st.container(border=True):
        _fh_t, _fh_b = st.columns((4, 1))
        with _fh_t:
            st.markdown("**Filtros**")
        with _fh_b:
            if st.button(
                "Limpar filtros",
                key="fdl_apu_reset",
                type="secondary",
                use_container_width=True,
                help="Repor empresa, datas de emissão NF e filtros da tabela ao padrão (independentes do Resultado Gerencial).",
            ):
                for _k in _FDL_APURACAO_RESET_KEYS:
                    st.session_state.pop(_k, None)
                st.rerun()
        _fc1, _fc2 = st.columns(2)
        with _fc1:
            if emp_opts:
                if "fdl_apu_emp" not in st.session_state:
                    st.session_state["fdl_apu_emp"] = []
                else:
                    prev_e = st.session_state["fdl_apu_emp"]
                    if isinstance(prev_e, list):
                        st.session_state["fdl_apu_emp"] = [x for x in prev_e if x in emp_opts]
                    else:
                        st.session_state["fdl_apu_emp"] = []
                st.multiselect(
                    "Empresa",
                    emp_opts,
                    key="fdl_apu_emp",
                    help="**Vazio** = todas as empresas neste carregamento. Estado **independente** do módulo Resultado Gerencial.",
                    placeholder="Todas",
                )
            else:
                st.caption("Empresa: sem opções distintas no recorte atual.")
        with _fc2:
            st.caption(
                "Período de emissão abaixo aplica-se ao **recorte fiscal** e à **tabela**. "
                "Filtros deste módulo não alteram o Resultado Gerencial."
            )
        if ok_nf_dates:
            st.markdown(
                '<p class="fdl-fat-filtros-periodo-tit">Período de emissão</p>',
                unsafe_allow_html=True,
            )
            r_nf = st.columns((1, 1))
            with r_nf[0]:
                st.date_input(
                    "Início",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_apu_nf_d_ini",
                    help=ao._FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
            with r_nf[1]:
                st.date_input(
                    "Fim",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_apu_nf_d_fim",
                    help=ao._FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
        elif "Nota_Data_Emissao" in _df_bounds.columns:
            st.caption("Período de emissão indisponível (datas não utilizáveis em Nota_Data_Emissao).")
        else:
            st.caption("Período de emissão indisponível (sem coluna Nota_Data_Emissao).")

    ao._fdl_ui_gap_section()
    ao._fdl_fat_min_vsp(size="md")

    _min_state = faturamento_recorte_min_state_from_session(
        st.session_state,
        key_emp="fdl_apu_emp",
        key_plat="fdl_apu_plat",
        key_sit="fdl_apu_nf_sit",
    )
    _nf_kpi_ini = ao._safe_streamlit_date(st.session_state.get("fdl_apu_nf_d_ini"), nf_min)
    _nf_kpi_fim = ao._safe_streamlit_date(st.session_state.get("fdl_apu_nf_d_fim"), nf_max)
    if ok_nf_dates:
        _nf_kpi_ini = min(max(_nf_kpi_ini, nf_cal_min), nf_cal_max)
        _nf_kpi_fim = min(max(_nf_kpi_fim, nf_cal_min), nf_cal_max)
        if _nf_kpi_fim < _nf_kpi_ini:
            _nf_kpi_fim = _nf_kpi_ini

    _df_fiscal_base, _fiscal_base_stats = build_faturamento_fiscal_base_slice(
        df_fiscal_pre
        if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame)
        else pd.DataFrame(),
        empresas_sel=_min_state.empresas,
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
        situacoes_sel=_min_state.situacoes_nf,
        df_devolucoes=df_devolucoes_pre if _df_dev_ok else None,
    )
    ao._render_faturamento_dre_fiscal_base_top(
        stats=_fiscal_base_stats,
        ok_nf_dates=ok_nf_dates,
        empresas_sel=_min_state.empresas,
        emp_opts=emp_opts,
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        fiscal_parquet_ok=use_fiscal_parquet,
        situacoes_nf_sel=_min_state.situacoes_nf,
    )

    df_nf_scope_emissao = ao._faturamento_nf_apply_minimal_recorte(
        df_nf_pre,
        empresas_sel=_min_state.empresas,
        plataformas_sel=(),
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
    )
    df_nf_scope_emissao = ao._faturamento_nf_filter_by_situacao(df_nf_scope_emissao, _min_state.situacoes_nf)
    _aligned_fiscal = bool(not _df_fiscal_base.empty)
    if _aligned_fiscal:
        df_nf_commercial_kpi = build_nf_panel_aligned_to_fiscal_base(_df_fiscal_base, df_nf_scope_emissao)
    else:
        df_nf_commercial_kpi = df_nf_scope_emissao.copy()
    if _min_state.plataformas:
        df_nf_commercial_kpi = ao._nf_panel_filter_merged_fiscal_by_plataforma_resumo(
            df_nf_commercial_kpi, _min_state.plataformas
        )
    _kp_cards = compute_nf_panel_kpis(df_nf_commercial_kpi)

    _fallback_alq_meta = _aliquota_configurada_pct_from_load_info(load_info)
    params_union = load_faturamento_params_for_ui(load_info)
    _emp_ef = list(_min_state.empresas) if _min_state.empresas else list(emp_opts)
    _ali_info = aliquota_configurada_para_empresas_filtradas(params_union, _emp_ef)
    regimes_info = detectar_regimes_tributarios(params_union, _emp_ef)

    if regimes_info.get("tem_regime_fora_escopo"):
        _badge_r = _build_badge_regime_fora_escopo_html(
            regimes_info["empresas_fora_escopo"],
            frozenset(r for r in regimes_info["regimes_presentes"] if r != "simples_nacional"),
        )
        if _badge_r:
            st.html(BADGE_REGIME_CSS + _badge_r)

    _imp_simples_ponte = dre_imposto_para_linha_dre_gerencial(
        _kp_cards,
        fiscal_base_stats=_fiscal_base_stats if use_fiscal_parquet else None,
        aplicar_ponte_base_liquida=bool(use_fiscal_kpi),
    )
    _imp_num = float(_imp_simples_ponte)
    try:
        from processing.faturamento.imposto_consolidado import calcular_imposto_total_painel_fiscal

        _json_path = resolve_faturamento_params_path_for_ui(load_info)
        if (
            _json_path is not None
            and _json_path.is_file()
            and use_fiscal_parquet
            and isinstance(df_fiscal_pre, pd.DataFrame)
            and not df_fiscal_pre.empty
        ):
            _emp_chaves_ag = list(_min_state.empresas) if _min_state.empresas else list(emp_opts)
            _org_ids_con = _apuracao_org_ids_resolvidos_para_df(
                _df_fiscal_base, params_union, _emp_chaves_ag
            )
            _cons = calcular_imposto_total_painel_fiscal(
                df_fiscal=df_fiscal_pre,
                df_devolucoes=df_devolucoes_pre if _df_dev_ok else None,
                org_ids_filtro=_org_ids_con or None,
                periodo_inicio=pd.Timestamp(_nf_kpi_ini),
                periodo_fim=pd.Timestamp(_nf_kpi_fim),
                imposto_simples_ponte=float(_imp_simples_ponte),
                json_params_path=_json_path,
            )
            _imp_num = float(_cons.imposto_total)
    except Exception as exc:
        _LOG_AP.warning("consolidação imposto fiscal (painel): %s", exc, exc_info=True)

    _ref_enrich = _fallback_alq_meta
    if params_union is not None and _ali_info.get("valores_por_empresa"):
        _ref_enrich = enrich_aliquota_ref_pct_for_stats(_ali_info)

    _cap_imp_html, _div_cmp = _aliquota_imposto_caption_safe_html_and_divergencia_ref(
        params_union=params_union,
        aliquotas_info=_ali_info,
        empresas_efetivas=_emp_ef,
        fallback_metadata_pct=_fallback_alq_meta,
        ok_nf_dates=ok_nf_dates,
    )

    _stats_kpi = enrich_faturamento_fiscal_base_stats(
        _fiscal_base_stats,
        imposto_apurado=float(_imp_num),
        df_nf_aligned=df_nf_commercial_kpi,
        aliquota_configurada_pct=float(_ref_enrich),
    )

    if ao._fdl_rg_pace_debug_enabled():
        st.caption(
            f"🔍 fiscal regime debug: regimes_presentes={regimes_info.get('regimes_presentes')!s} · "
            f"aliquotas_modo={_ali_info.get('modo')} · "
            f"valor_global_metadata={_fallback_alq_meta:.2f}% · "
            f"valores_calculados={_ali_info.get('valores_por_empresa')!s}"
        )

    ao._fdl_fat_min_vsp(size="sm")
    st.html(
        FISCAL_KPIS_CSS
        + _build_fiscal_kpis_hero_html(
            base_liquida=_stats_kpi.base_fiscal_liquida,
            imposto=float(_stats_kpi.imposto),
            aliquota_efetiva_pct=float(_stats_kpi.aliquota_efetiva_pct),
            caption_aliquota_imposto_safe_html=_cap_imp_html,
            divergencia_compare_pct=_div_cmp,
            ok_nf_dates=ok_nf_dates,
            fmt_brl=ao._fmt_brl_ptbr_celula,
        )
        + _build_fiscal_kpis_secondary_html(
            valor_faturado_nf=float(_stats_kpi.valor_faturado_nf),
            n_nf=int(_stats_kpi.n_nf),
            total_devolvido=float(_stats_kpi.total_devolvido),
            nfs_devolucao=int(_stats_kpi.nfs_devolucao),
            diferenca_lista_nf=float(_stats_kpi.diferenca_lista_nf),
            valor_cancelado=float(_stats_kpi.valor_cancelado),
            ok_nf_dates=ok_nf_dates,
            fmt_brl=ao._fmt_brl_ptbr_celula,
            fmt_int=ao._fmt_int_ptbr,
        )
    )

    _emp_sel_t = tuple(_min_state.empresas) if _min_state.empresas else ()
    _nfs_canc = 0
    if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame):
        _nfs_canc = _count_nf_canceladas_periodo(
            df_fiscal_pre,
            empresas_sel=_emp_sel_t,
            nf_d_ini=_nf_kpi_ini,
            nf_d_fim=_nf_kpi_fim,
            ok_nf_dates=ok_nf_dates,
        )

    composicao_html = _build_composicao_base_tributavel_html(
        valor_faturado=float(_stats_kpi.valor_liquido_nf_periodo_todas_situacoes),
        nfs_emitidas=int(_stats_kpi.n_nf_periodo_todas_situacoes),
        valor_cancelado=float(_stats_kpi.valor_cancelado),
        nfs_canceladas=int(_nfs_canc),
        valor_devolucoes=float(_stats_kpi.total_devolvido),
        nfs_devolucoes=int(_stats_kpi.nfs_devolucao),
        base_liquida=float(_stats_kpi.base_fiscal_liquida),
        fmt_brl=ao._fmt_brl_ptbr_celula,
        fmt_int=ao._fmt_int_ptbr,
    )
    ao._fdl_fat_min_vsp(size="sm")
    st.html(COMPOSICAO_BASE_CSS + composicao_html)

    if use_fiscal_parquet and ok_nf_dates and isinstance(df_fiscal_pre, pd.DataFrame):
        (nc, vc), (nd, vd), ni = _agregar_invalidas_por_tipo_no_periodo(
            df_fiscal_pre,
            empresas_sel=_emp_sel_t,
            nf_d_ini=_nf_kpi_ini,
            nf_d_fim=_nf_kpi_fim,
            ok_nf_dates=ok_nf_dates,
        )
        if nc + nd + ni > 0:
            with st.expander("Detalhamento de NFs inválidas", expanded=False):
                st.markdown(
                    f"- **Canceladas:** {ao._fmt_int_ptbr(nc)} NFs · **{ao._fmt_brl_ptbr_celula(vc)}**"
                )
                st.caption("Valores em valor líquido da NF (export Bling).")
                st.markdown(
                    f"- **Denegadas:** {ao._fmt_int_ptbr(nd)} NFs · **{ao._fmt_brl_ptbr_celula(vd)}** "
                    "_(não afetam a base — nunca foram válidas fiscalmente)_"
                )
                st.markdown(
                    f"- **Inutilizadas:** {ao._fmt_int_ptbr(ni)} NFs _(números pulados, sem valor fiscal)_"
                )

        _emp_chaves_list = list(_min_state.empresas) if _min_state.empresas else list(emp_opts)
        _org_ids_ag = _apuracao_org_ids_resolvidos_para_df(_df_fiscal_base, params_union, _emp_chaves_list)
        simples_agregado = agregar_simples_nacional_para_painel_fiscal(
            _df_fiscal_base,
            _org_ids_ag,
            params_union,
            _nf_kpi_ini,
            _nf_kpi_fim,
            df_fiscal_full=df_fiscal_pre,
            df_devolucoes=df_devolucoes_pre if _df_dev_ok else None,
            ok_nf_dates=ok_nf_dates,
        )
        if _tem_alguma_empresa_simples_no_filtro(simples_agregado):
            aliquota_html = _build_aliquota_efetiva_simples_html(
                simples_agregado,
                fmt_brl=ao._fmt_brl_ptbr_celula,
            )
            ao._fdl_fat_min_vsp(size="sm")
            st.html(ALIQUOTA_EFETIVA_CSS + aliquota_html)

        if ao._fdl_rg_pace_debug_enabled():
            rbt_dbg: dict[str, str] = {}
            aliq_ef_dbg: dict[str, str] = {}
            aliq_json_dbg: dict[str, float] = {}
            divs: list[str] = []
            for oid, row in simples_agregado.get("por_empresa", {}).items():
                if not isinstance(row, dict) or row.get("regime") != "simples_nacional":
                    continue
                u = row.get("ultimo_mes")
                if isinstance(u, ResultadoAliquotaEfetivaMes):
                    rbt_dbg[oid] = f"{float(u.rbt12):.0f} [{int(u.meses_historico_disponiveis)}m]"
                    if u.aliquota_efetiva_pct is not None:
                        aliq_ef_dbg[oid] = f"{float(u.aliquota_efetiva_pct):.2f} [calc]"
                    cfg = get_aliquota_imposto_por_empresa(params_union, oid)
                    if cfg is not None and u.aliquota_efetiva_pct is not None:
                        cfg_pct = float(cfg) * 100.0 if float(cfg) <= 1.0 else float(cfg)
                        d = abs(float(u.aliquota_efetiva_pct) - cfg_pct)
                        if d > 2.0:
                            divs.append(f"{oid}: efetiva {u.aliquota_efetiva_pct:.2f}% vs JSON {cfg_pct:.2f}% (Δ {d:.2f} pp)")
                jr = row.get("aliquota_referencia_json_pct")
                if isinstance(jr, (int, float)):
                    aliq_json_dbg[oid] = round(float(jr), 2)
            cr = simples_agregado.get("competencia_referencia")
            em_calc = list(simples_agregado.get("empresas_com_calculo_oficial") or [])
            em_warm = list(simples_agregado.get("empresas_em_warmup") or [])
            dbg = (
                "🔍 Simples Nacional debug:\n"
                f"   competencia_referencia: {cr!s}\n"
                f"   empresas_calculadas: {em_calc!s}\n"
                f"   empresas_warmup: {em_warm!s}\n"
                f"   rbt12: {rbt_dbg!s}\n"
                f"   aliquota_efetiva: {aliq_ef_dbg!s}\n"
                f"   aliquota_referencia_json: {aliq_json_dbg!s}"
            )
            if divs:
                dbg += "\n   divergencias_json: " + "; ".join(divs)
            st.caption(dbg)

    ao._fdl_fat_min_vsp(size="md")
    ao._fdl_fat_divider_simple()
    ao._fdl_fat_min_vsp(size="sm")

    ao._render_faturamento_dre_nf_table_section(
        df_nf_pre=df_nf_pre,
        df=df,
        df_fiscal_pre=df_fiscal_pre,
        load_info=load_info,
        _min_state=_min_state,
        _nf_kpi_ini=_nf_kpi_ini,
        _nf_kpi_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
        use_fiscal_kpi=use_fiscal_kpi,
        use_nf_materializado=use_nf_materializado,
        use_fiscal_parquet=use_fiscal_parquet,
        _nf_panel_ads_ui=_nf_panel_ads_ui,
        _df_fiscal_base=_df_fiscal_base,
        _fiscal_base_stats=_fiscal_base_stats,
        _kp_cards=_kp_cards,
        org_id=org_id,
        prefix_main="fdl_apu",
        prefix_nf="fdl_apu_nf",
        csv_file_name="apuracao_fiscal_nf.csv",
        table_heading="### Tabela de NFs (visão fiscal)",
        nf_table_ui_mode="fiscal",
    )
