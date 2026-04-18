"""
UI premium Faturamento & DRE (HTML + CSS) — usado por ``app_operacional``.

Sem dependência de Streamlit: apenas strings e lógica pura para testes e reutilização.
"""

from __future__ import annotations

import html
import math
from dataclasses import dataclass


@dataclass(frozen=True)
class KpiPerfTone:
    """Cor semântica + seta para KPIs (margem, resultado, diferença)."""

    css_modifier: str
    arrow: str


def fat_dre_premium_css() -> str:
    """CSS adicional (injeta após estilos base do painel NF-first)."""
    return """
<style>
@import url("https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap");
.fdl-fat-premium {
  --fdl-primary: #1a56db;
  --fdl-success: #059669;
  --fdl-danger: #dc2626;
  --fdl-warning: #d97706;
  --fdl-neutral-100: #f8fafc;
  --fdl-neutral-200: #e2e8f0;
  --fdl-neutral-500: #6b7280;
  --fdl-neutral-800: #1e293b;
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
}
.fdl-fat-sec-rule {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  margin: 0.35rem 0 0.85rem 0;
  color: #64748b;
  font-size: 0.68rem;
  font-weight: 700;
  letter-spacing: 0.14em;
  text-transform: uppercase;
}
.fdl-fat-sec-rule::before,
.fdl-fat-sec-rule::after {
  content: "";
  flex: 1;
  height: 1px;
  background: linear-gradient(90deg, transparent, #cbd5e1 18%, #cbd5e1 82%, transparent);
}
.fdl-fat-kpi-hero-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
  gap: 14px;
  margin-bottom: 14px;
}
.fdl-fat-kpi-hero-card {
  border-radius: 14px;
  padding: 18px 20px 20px 20px;
  border: 1px solid var(--fdl-neutral-200);
  background: linear-gradient(165deg, #ffffff 0%, #f8fafc 55%, #f1f5f9 100%);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 8px 24px rgba(15, 23, 42, 0.06);
  position: relative;
  overflow: hidden;
}
.fdl-fat-kpi-hero-card::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  border-radius: 14px 0 0 14px;
  background: var(--fdl-primary);
  opacity: 0.85;
}
.fdl-fat-kpi-hero-card--result::before { background: var(--fdl-primary); }
.fdl-fat-kpi-hero-card--result.fdl-fat-kpi--pos::before { background: var(--fdl-success); }
.fdl-fat-kpi-hero-card--result.fdl-fat-kpi--neg::before { background: var(--fdl-danger); }
.fdl-fat-kpi-hero-card--result.fdl-fat-kpi--warn::before { background: var(--fdl-warning); }
.fdl-fat-kpi-hero-card--margem::before { background: #475569; }
.fdl-fat-kpi-hero-card--margem.fdl-fat-kpi--pos::before { background: var(--fdl-success); }
.fdl-fat-kpi-hero-card--margem.fdl-fat-kpi--mid::before { background: var(--fdl-warning); }
.fdl-fat-kpi-hero-card--margem.fdl-fat-kpi--neg::before { background: var(--fdl-danger); }
.fdl-fat-kpi-hero-label {
  font-size: 0.78rem;
  font-weight: 600;
  color: #64748b;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  margin: 0 0 6px 0;
}
.fdl-fat-kpi-hero-value {
  font-size: 2.5rem;
  font-weight: 700;
  line-height: 1.08;
  color: var(--fdl-neutral-800);
  letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.fdl-fat-kpi-hero-meta {
  margin-top: 8px;
  font-size: 0.82rem;
  font-weight: 600;
  color: #475569;
}
.fdl-fat-kpi-hero-meta--pos { color: var(--fdl-success); }
.fdl-fat-kpi-hero-meta--mid { color: var(--fdl-warning); }
.fdl-fat-kpi-hero-meta--neg { color: var(--fdl-danger); }
.fdl-fat-kpi-mid-row {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 12px;
  margin-bottom: 12px;
}
.fdl-fat-kpi-mid-card {
  border-radius: 12px;
  padding: 14px 16px;
  border: 1px solid #e2e8f0;
  background: var(--fdl-neutral-100);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
.fdl-fat-kpi-mid-label {
  font-size: 0.72rem;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  margin: 0 0 6px 0;
}
.fdl-fat-kpi-mid-value {
  font-size: 1.8rem;
  font-weight: 600;
  color: #0f172a;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.02em;
}
.fdl-fat-kpi-chip-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 10px;
  align-items: stretch;
  margin: 4px 0 6px 0;
}
.fdl-fat-kpi-chip {
  display: inline-flex;
  flex-direction: column;
  align-items: flex-start;
  padding: 8px 11px;
  border-radius: 999px;
  border: 1px solid #e2e8f0;
  background: #ffffff;
  min-width: 0;
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
}
.fdl-fat-kpi-chip-lab {
  font-size: 0.62rem;
  font-weight: 700;
  color: #64748b;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  margin-bottom: 2px;
}
.fdl-fat-kpi-chip-val {
  font-size: 0.82rem;
  font-weight: 600;
  color: #1e293b;
  font-variant-numeric: tabular-nums;
  white-space: nowrap;
}
.fdl-fat-dre-premium-wrap {
  max-width: min(52rem, 100%);
  margin: 0;
  padding: 0;
}
.fdl-fat-dre-card-v2 {
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  background: linear-gradient(180deg, #fafbfc 0%, #ffffff 48px);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04), 0 12px 32px rgba(15, 23, 42, 0.06);
  overflow: hidden;
}
.fdl-fat-dre-head-v2 {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px 12px;
  padding: 18px 22px 16px 22px;
  border-bottom: 1px solid #e2e8f0;
  background: #ffffff;
}
.fdl-fat-dre-head-v2 h3 {
  margin: 0;
  font-size: 1.25rem;
  font-weight: 600;
  color: #1e293b;
  letter-spacing: -0.02em;
}
.fdl-fat-dre-period-v2 {
  font-size: 0.875rem;
  font-weight: 500;
  color: #64748b;
}
.fdl-fat-dre-body-v2 {
  padding: 18px 22px 20px 22px;
}
.fdl-fat-dre-sec-v2 {
  margin-bottom: 20px;
}
.fdl-fat-dre-sec-title-v2 {
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #64748b;
  margin: 0 0 12px 0;
  padding-bottom: 8px;
  border-bottom: 1px solid #f1f5f9;
}
.fdl-fat-dre-line-v2 {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  flex-wrap: wrap;
  padding: 8px 0;
  border-bottom: 1px solid #f8fafc;
}
.fdl-fat-dre-line-v2:last-child {
  border-bottom: none;
}
.fdl-fat-dre-line-v2--indent {
  padding-left: 16px;
}
.fdl-fat-dre-label-v2 {
  font-size: 0.9rem;
  color: #334155;
  line-height: 1.4;
  min-width: min(14rem, 42vw);
  flex: 1 1 12rem;
}
.fdl-fat-dre-value-v2 {
  font-size: 0.9rem;
  font-weight: 500;
  font-variant-numeric: tabular-nums;
  text-align: right;
  white-space: nowrap;
  color: #1e293b;
}
.fdl-fat-dre-value-v2--warn {
  color: #d97706;
  font-weight: 600;
}
.fdl-fat-dre-value-v2--ded {
  color: #dc2626;
  font-weight: 500;
}
.fdl-fat-dre-section.fdl-fat-dre-deducoes {
  margin: 0 0 18px 0;
  border: 1px solid #e8edf3;
  border-radius: 10px;
  background: #f8fafc;
  overflow: hidden;
}
.fdl-fat-dre-deducoes summary.fdl-fat-dre-section-title {
  cursor: pointer;
  list-style: none;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 12px 16px;
  font-size: 0.72rem;
  font-weight: 600;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #475569;
}
.fdl-fat-dre-deducoes summary.fdl-fat-dre-section-title::-webkit-details-marker {
  display: none;
}
.fdl-fat-dre-deducoes summary.fdl-fat-dre-section-title::marker {
  content: "";
}
.fdl-fat-dre-deducoes[open] summary.fdl-fat-dre-section-title {
  border-bottom: 1px solid #e8edf3;
}
.fdl-fat-dre-toggle {
  font-size: 0.75rem;
  color: #64748b;
  transition: transform 0.2s ease;
  flex-shrink: 0;
  margin-left: 0.75rem;
}
.fdl-fat-dre-deducoes[open] .fdl-fat-dre-toggle {
  transform: rotate(0deg);
}
.fdl-fat-dre-deducoes:not([open]) .fdl-fat-dre-toggle {
  transform: rotate(-90deg);
}
.fdl-fat-dre-ded-body-v2 {
  padding: 4px 14px 14px 14px;
}
.fdl-fat-dre-result-shell {
  margin-top: 4px;
  padding: 16px 18px;
  border-radius: 10px;
  border-left: 4px solid #22c55e;
  background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
}
.fdl-fat-dre-result-shell--neg {
  border-left-color: #ef4444;
  background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
}
.fdl-fat-dre-line-v2--result-main {
  padding: 4px 0 10px 0;
  border-bottom: none;
}
.fdl-fat-dre-line-v2--margin-sub {
  padding-top: 10px;
  margin-top: 2px;
  border-top: 1px solid rgba(34, 197, 94, 0.28);
  border-bottom: none;
}
.fdl-fat-dre-result-shell--neg .fdl-fat-dre-line-v2--margin-sub {
  border-top-color: rgba(239, 68, 68, 0.28);
}
.fdl-fat-dre-result-title {
  font-size: 1rem;
  font-weight: 600;
}
.fdl-fat-dre-result-shell--pos .fdl-fat-dre-result-title {
  color: #166534;
}
.fdl-fat-dre-result-shell--neg .fdl-fat-dre-result-title {
  color: #991b1b;
}
.fdl-fat-dre-result-amount {
  font-size: 1.25rem;
  font-weight: 700;
  font-variant-numeric: tabular-nums;
}
.fdl-fat-dre-result-shell--pos .fdl-fat-dre-result-amount {
  color: #166534;
}
.fdl-fat-dre-result-shell--neg .fdl-fat-dre-result-amount {
  color: #991b1b;
}
.fdl-fat-dre-margin-lab {
  font-size: 0.875rem;
  font-weight: 600;
  color: #475569;
}
.fdl-fat-dre-margin-val {
  font-size: 0.95rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  color: #334155;
}
.fdl-fat-dre-hint-label { cursor: help; }
@media (max-width: 520px) {
  .fdl-fat-dre-body-v2 { padding: 14px 14px 16px 14px; }
  .fdl-fat-dre-head-v2 { padding: 14px 14px 12px 14px; }
  .fdl-fat-dre-value-v2,
  .fdl-fat-dre-result-amount,
  .fdl-fat-dre-margin-val {
    flex: 1 1 100%;
    text-align: right;
  }
}
.fdl-fat-dre-foot-note {
  margin: 14px 0 0 0;
  padding-top: 12px;
  border-top: 1px solid #f1f5f9;
  font-size: 0.68rem;
  color: #94a3b8;
  line-height: 1.5;
  max-width: 46rem;
}
</style>
"""


def _margin_ratio_from_pct_str(margem_s: str) -> float | None:
    s = (margem_s or "").strip().replace("%", "").replace(" ", "")
    if not s or s == "—":
        return None
    s = s.replace(".", "").replace(",", ".") if "," in s else s.replace(",", ".")
    try:
        return float(s) / 100.0
    except ValueError:
        return None


def kpi_perf_margin(margin_ratio: float | None) -> KpiPerfTone:
    if margin_ratio is None or math.isnan(margin_ratio):
        return KpiPerfTone("fdl-fat-kpi--mid", "→")
    if margin_ratio >= 0.20:
        return KpiPerfTone("fdl-fat-kpi--pos", "↑")
    if margin_ratio >= 0.10:
        return KpiPerfTone("fdl-fat-kpi--mid", "→")
    return KpiPerfTone("fdl-fat-kpi--neg", "↓")


def kpi_perf_resultado(resultado: float) -> KpiPerfTone:
    if math.isnan(resultado):
        return KpiPerfTone("fdl-fat-kpi--mid", "→")
    if resultado > 0:
        return KpiPerfTone("fdl-fat-kpi--pos", "↑")
    if resultado < 0:
        return KpiPerfTone("fdl-fat-kpi--neg", "↓")
    return KpiPerfTone("fdl-fat-kpi--mid", "→")


def kpi_perf_diferenca(valor_venda: float, diferenca: float) -> KpiPerfTone:
    if valor_venda <= 0 or math.isnan(valor_venda) or math.isnan(diferenca):
        return KpiPerfTone("fdl-fat-kpi--mid", "→")
    rel = abs(diferenca) / valor_venda
    if rel >= 0.30:
        return KpiPerfTone("fdl-fat-kpi--warn", "!")
    return KpiPerfTone("fdl-fat-kpi--mid", "→")


def build_kpi_nf_premium_shell_html(
    *,
    valor_venda_fmt: str,
    valor_faturado_fmt: str,
    resultado_fmt: str,
    margem_str: str,
    diferenca_fmt: str,
    valor_venda: float,
    resultado: float,
    diferenca: float,
    chips: list[tuple[str, str]],
    mode_pill_html: str,
) -> str:
    mr = _margin_ratio_from_pct_str(margem_str)
    t_res = kpi_perf_resultado(resultado)
    t_mg = kpi_perf_margin(mr)
    t_df = kpi_perf_diferenca(valor_venda, diferenca)

    chips_html = "".join(
        f'<div class="fdl-fat-kpi-chip"><span class="fdl-fat-kpi-chip-lab">{html.escape(lab)}</span>'
        f'<span class="fdl-fat-kpi-chip-val">{html.escape(val)}</span></div>'
        for lab, val in chips
    )

    return (
        '<div class="fdl-fat-premium fdl-fat-kpi-shell">'
        f"{mode_pill_html}"
        '<div class="fdl-fat-kpi-hero-row">'
        f'<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--result {html.escape(t_res.css_modifier)}" '
        'title="Soma do resultado por nota fiscal no recorte selecionado (empresa, período, situação, plataforma). '
        'Base consolidada por NF — pode diferir do Painel de Saúde que usa grão de linha de pedido.">'
        '<div class="fdl-fat-kpi-hero-label">Resultado</div>'
        f'<div class="fdl-fat-kpi-hero-value">{html.escape(resultado_fmt)}</div>'
        f'<div class="fdl-fat-kpi-hero-meta {html.escape(_meta_class(t_res.css_modifier))}">'
        f'{html.escape(t_res.arrow)} vs. zero</div></div>'
        f'<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--margem {html.escape(t_mg.css_modifier)}">'
        '<div class="fdl-fat-kpi-hero-label">Margem sobre venda</div>'
        f'<div class="fdl-fat-kpi-hero-value">{html.escape(margem_str)}</div>'
        f'<div class="fdl-fat-kpi-hero-meta {html.escape(_meta_class(t_mg.css_modifier))}">'
        f'{html.escape(t_mg.arrow)} meta</div></div>'
        "</div>"
        '<div class="fdl-fat-kpi-mid-row">'
        '<div class="fdl-fat-kpi-mid-card">'
        '<div class="fdl-fat-kpi-mid-label">Valor da venda (lista)</div>'
        f'<div class="fdl-fat-kpi-mid-value">{html.escape(valor_venda_fmt)}</div></div>'
        '<div class="fdl-fat-kpi-mid-card">'
        '<div class="fdl-fat-kpi-mid-label">Valor faturado (NF)</div>'
        f'<div class="fdl-fat-kpi-mid-value">{html.escape(valor_faturado_fmt)}</div></div>'
        "</div>"
        '<div class="fdl-fat-kpi-chip-row">'
        f'<div class="fdl-fat-kpi-chip"><span class="fdl-fat-kpi-chip-lab">Diferença (lista − NF)</span>'
        f'<span class="fdl-fat-kpi-chip-val">{html.escape(diferenca_fmt)}</span>'
        f'<span class="fdl-fat-kpi-hero-meta {html.escape(_meta_class(t_df.css_modifier))}" style="margin-top:4px">'
        f'{html.escape(t_df.arrow)} '
        f'{"alerta (>30% da venda)" if t_df.css_modifier == "fdl-fat-kpi--warn" else "em faixa"}</span></div>'
        f"{chips_html}</div></div>"
    )


def _meta_class(css_mod: str) -> str:
    if css_mod == "fdl-fat-kpi--pos":
        return "fdl-fat-kpi-hero-meta--pos"
    if css_mod == "fdl-fat-kpi--neg":
        return "fdl-fat-kpi-hero-meta--neg"
    if css_mod in ("fdl-fat-kpi--warn", "fdl-fat-kpi--mid"):
        return "fdl-fat-kpi-hero-meta--mid"
    return "fdl-fat-kpi-hero-meta--mid"


def dre_gerencial_result_shell_class(resultado_value: float) -> str:
    """Classe do bloco de resultado (gradiente verde ou vermelho)."""
    if resultado_value < 0:
        return "fdl-fat-dre-result-shell fdl-fat-dre-result-shell--neg"
    return "fdl-fat-dre-result-shell fdl-fat-dre-result-shell--pos"


def _dre_line_v2(
    label: str,
    value: str,
    *,
    value_extra_class: str = "",
    indent: bool = False,
) -> str:
    extra_line = " fdl-fat-dre-line-v2--indent" if indent else ""
    vc = "fdl-fat-dre-value-v2"
    if value_extra_class.strip():
        vc += " " + value_extra_class.strip()
    return (
        f'<div class="fdl-fat-dre-line-v2{extra_line}">'
        f'<span class="fdl-fat-dre-label-v2">{html.escape(label)}</span>'
        f'<span class="{vc}">{html.escape(value)}</span>'
        "</div>"
    )


def build_dre_gerencial_premium_html(
    *,
    period_caption: str,
    valor_venda_fmt: str,
    rec_frete_fmt: str,
    diferenca_fmt: str,
    enc_rows: list[tuple[str, str]],
    resultado_fmt: str,
    resultado_value: float,
    margem_str: str,
    resultado_tooltip: str,
    margem_tooltip: str,
    footnote_plain: str = "",
    dif_highlight: bool,
) -> str:
    dif_suffix = "fdl-fat-dre-value-v2--warn" if dif_highlight else ""
    enc_inner = (
        "".join(
            _dre_line_v2(lab, val, value_extra_class="fdl-fat-dre-value-v2--ded", indent=True)
            for lab, val in enc_rows
        )
        if enc_rows
        else '<div class="fdl-fat-dre-line-v2"><span class="fdl-fat-dre-label-v2">—</span></div>'
    )
    _shell = dre_gerencial_result_shell_class(resultado_value)
    _rt = html.escape(resultado_tooltip.strip(), quote=True)
    _mt = html.escape(margem_tooltip.strip(), quote=True)
    _foot_html = ""
    if footnote_plain.strip():
        _foot_html = f'<p class="fdl-fat-dre-foot-note">{html.escape(footnote_plain.strip())}</p>'
    return (
        '<div class="fdl-fat-premium fdl-fat-dre-premium-wrap">'
        '<div class="fdl-fat-dre-card-v2">'
        '<header class="fdl-fat-dre-head-v2">'
        "<h3>DRE gerencial</h3>"
        f'<span class="fdl-fat-dre-period-v2">{html.escape(period_caption)}</span>'
        "</header>"
        '<div class="fdl-fat-dre-body-v2">'
        '<section class="fdl-fat-dre-sec-v2">'
        '<div class="fdl-fat-dre-sec-title-v2">Receita</div>'
        + _dre_line_v2("Receita de venda (lista)", valor_venda_fmt)
        + _dre_line_v2("Receita frete (transp. própria)", rec_frete_fmt)
        + _dre_line_v2("Diferença (lista − fiscal)", diferenca_fmt, value_extra_class=dif_suffix)
        + "</section>"
        '<details class="fdl-fat-dre-section fdl-fat-dre-deducoes" open>'
        '<summary class="fdl-fat-dre-section-title">Deduções'
        '<span class="fdl-fat-dre-toggle" aria-hidden="true">▼</span></summary>'
        f'<div class="fdl-fat-dre-ded-body-v2">{enc_inner}</div>'
        "</details>"
        f'<div class="{_shell}">'
        '<div class="fdl-fat-dre-line-v2 fdl-fat-dre-line-v2--result-main">'
        f'<span class="fdl-fat-dre-hint-label fdl-fat-dre-result-title" title="{_rt}">Resultado</span>'
        f'<span class="fdl-fat-dre-result-amount">{html.escape(resultado_fmt)}</span>'
        "</div>"
        '<div class="fdl-fat-dre-line-v2 fdl-fat-dre-line-v2--margin-sub">'
        f'<span class="fdl-fat-dre-hint-label fdl-fat-dre-margin-lab" title="{_mt}">Margem sobre venda</span>'
        f'<span class="fdl-fat-dre-margin-val">{html.escape(margem_str)}</span>'
        "</div>"
        "</div>"
        f"{_foot_html}"
        "</div></div></div>"
    )


def faturamento_section_rule_html(label: str) -> str:
    return f'<div class="fdl-fat-premium fdl-fat-sec-rule"><span>{html.escape(label)}</span></div>'
