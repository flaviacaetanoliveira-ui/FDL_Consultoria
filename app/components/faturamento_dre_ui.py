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
  font-size: 0.75rem;
  font-weight: 600;
  color: #64748b;
  letter-spacing: 0.05em;
  text-transform: uppercase;
  margin: 0 0 6px 0;
}
.fdl-fat-kpi-hero-value {
  line-height: 1.08;
  color: var(--fdl-neutral-800);
  letter-spacing: -0.03em;
  font-variant-numeric: tabular-nums;
}
.fdl-fat-kpi-hero-card--result .fdl-fat-kpi-hero-value {
  font-size: 2.25rem;
  font-weight: 800;
}
.fdl-fat-kpi-hero-card--margem .fdl-fat-kpi-hero-value {
  font-size: 2rem;
  font-weight: 700;
}
.fdl-fat-kpi-hero-meta {
  margin-top: 8px;
  font-size: 0.85rem;
  font-weight: 600;
  color: #475569;
}
.fdl-fat-kpi-hero-meta--pos { color: var(--fdl-success); }
.fdl-fat-kpi-hero-meta--mid { color: var(--fdl-warning); }
.fdl-fat-kpi-hero-meta--neg { color: var(--fdl-danger); }
.fdl-fat-kpi-mid-row {
  display: grid;
  grid-template-columns: repeat(3, minmax(0, 1fr));
  gap: 12px;
  margin-bottom: 12px;
  align-items: stretch;
}
@media (max-width: 720px) {
  .fdl-fat-kpi-mid-row {
    grid-template-columns: 1fr;
  }
}
.fdl-fat-kpi-mid-card {
  display: flex;
  flex-direction: column;
  border-radius: 12px;
  padding: 14px 16px;
  border: 1px solid #e2e8f0;
  background: var(--fdl-neutral-100);
  box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
  min-height: 0;
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
  font-size: 1.5rem;
  font-weight: 700;
  color: #0f172a;
  font-variant-numeric: tabular-nums;
  letter-spacing: -0.02em;
}
.fdl-fat-kpi-mid-card--diferenca .fdl-fat-kpi-mid-value {
  font-size: 1.25rem;
  font-weight: 600;
}
.fdl-fat-kpi-mid-meta {
  margin-top: 8px;
  font-size: 0.85rem;
  font-weight: 600;
  color: #475569;
}
.fdl-fat-kpi-mid-diferenca-alerta {
  font-size: 0.8rem;
  color: #dc2626;
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
.fdl-dre-container {
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 16px;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05), 0 2px 4px -2px rgba(0, 0, 0, 0.05);
  padding: 28px 32px 32px 32px;
}
.fdl-dre-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 12px 16px;
  margin-bottom: 24px;
}
.fdl-dre-title {
  margin: 0;
  font-size: 1.5rem;
  font-weight: 700;
  color: #0f172a;
  letter-spacing: -0.02em;
  line-height: 1.2;
}
.fdl-dre-periodo {
  font-size: 0.875rem;
  font-weight: 500;
  color: #64748b;
  background: #f1f5f9;
  padding: 6px 12px;
  border-radius: 6px;
  white-space: nowrap;
}
.fdl-dre-section {
  background: #fafafa;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  padding: 18px 22px 16px 22px;
  margin-bottom: 16px;
}
.fdl-dre-section-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding-bottom: 12px;
  margin-bottom: 10px;
  border-bottom: 2px solid #e5e7eb;
}
.fdl-dre-section-heading {
  font-size: 0.8rem;
  font-weight: 700;
  color: #374151;
  letter-spacing: 0.1em;
  text-transform: uppercase;
}
.fdl-dre-line {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
  padding: 10px 0;
  border-bottom: 1px solid #f3f4f6;
}
.fdl-dre-line:last-child {
  border-bottom: none;
}
.fdl-dre-line--indent {
  padding-left: 14px;
}
.fdl-dre-line--total {
  padding-top: 12px;
  margin-top: 8px;
  border-top: 2px solid #d1d5db;
  border-bottom: none;
}
.fdl-dre-label {
  font-size: 0.95rem;
  color: #4b5563;
  font-weight: 400;
  line-height: 1.35;
  flex: 1 1 12rem;
  min-width: min(14rem, 46vw);
}
.fdl-dre-total-label {
  font-weight: 600;
  color: #1f2937;
}
.fdl-dre-value {
  font-size: 0.95rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
  color: #1f2937;
  text-align: right;
  white-space: nowrap;
}
.fdl-dre-total-value {
  font-size: 1.1rem;
  font-weight: 700;
}
.fdl-dre-value--negative {
  color: #b91c1c;
  font-weight: 600;
}
.fdl-dre-value--warning {
  color: #b45309;
  font-weight: 600;
}
.fdl-dre-divider {
  height: 1px;
  background: linear-gradient(90deg, transparent, #d1d5db 18%, #d1d5db 82%, transparent);
  margin: 18px 0;
}
.fdl-dre-details {
  margin-bottom: 16px;
  border: 1px solid #e5e7eb;
  border-radius: 12px;
  background: #fafafa;
  overflow: hidden;
}
.fdl-dre-details > summary {
  cursor: pointer;
  list-style: none;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 12px;
  width: 100%;
  box-sizing: border-box;
  padding: 16px 22px;
  margin: 0;
  border-bottom: 1px solid #e5e7eb;
}
.fdl-dre-details > summary::-webkit-details-marker {
  display: none;
}
.fdl-dre-details > summary::marker {
  content: "";
}
.fdl-dre-details[open] > summary {
  border-bottom: 2px solid #e5e7eb;
}
.fdl-dre-details-body {
  padding: 8px 22px 18px 22px;
}
.fdl-dre-ded-chevron {
  font-size: 0.75rem;
  color: #64748b;
}
.fdl-dre-result {
  margin-top: 16px;
  padding: 16px 20px;
  border-radius: 12px;
}
.fdl-dre-result--positive {
  background: linear-gradient(135deg, #ecfdf5 0%, #d1fae5 100%);
  border: 1px solid #6ee7b7;
  border-left: 5px solid #10b981;
}
.fdl-dre-result--negative {
  background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
  border: 1px solid #fca5a5;
  border-left: 5px solid #ef4444;
}
.fdl-dre-result--neutral {
  background: #f8fafc;
  border: 1px solid #cbd5e1;
  border-left: 5px solid #64748b;
}
.fdl-dre-result-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  flex-wrap: wrap;
}
.fdl-dre-result-row--margin {
  margin-top: 14px;
  padding-top: 14px;
  border-top: 1px solid rgba(16, 185, 129, 0.35);
}
.fdl-dre-result--negative .fdl-dre-result-row--margin {
  border-top-color: rgba(239, 68, 68, 0.35);
}
.fdl-dre-result--neutral .fdl-dre-result-row--margin {
  border-top-color: #e2e8f0;
}
.fdl-dre-result-label {
  font-size: 0.8rem;
  font-weight: 700;
  letter-spacing: 0.05em;
  text-transform: uppercase;
}
.fdl-dre-result--positive .fdl-dre-result-label {
  color: #065f46;
}
.fdl-dre-result--negative .fdl-dre-result-label {
  color: #991b1b;
}
.fdl-dre-result--neutral .fdl-dre-result-label {
  color: #475569;
}
.fdl-dre-result-value {
  font-size: 1.75rem;
  font-weight: 800;
  letter-spacing: -0.02em;
  font-variant-numeric: tabular-nums;
  line-height: 1.15;
}
.fdl-dre-result--positive .fdl-dre-result-value {
  color: #047857;
}
.fdl-dre-result--negative .fdl-dre-result-value {
  color: #b91c1c;
}
.fdl-dre-result--neutral .fdl-dre-result-value {
  color: #334155;
}
.fdl-dre-margin-label {
  font-size: 0.9rem;
  font-weight: 600;
}
.fdl-dre-result--positive .fdl-dre-margin-label {
  color: #065f46;
}
.fdl-dre-result--negative .fdl-dre-margin-label {
  color: #991b1b;
}
.fdl-dre-result--neutral .fdl-dre-margin-label {
  color: #64748b;
}
.fdl-dre-margin-value {
  font-size: 1rem;
  font-weight: 600;
  font-variant-numeric: tabular-nums;
}
.fdl-dre-result--positive .fdl-dre-margin-value {
  color: #047857;
}
.fdl-dre-result--negative .fdl-dre-margin-value {
  color: #b91c1c;
}
.fdl-dre-result--neutral .fdl-dre-margin-value {
  color: #475569;
}
.fdl-dre-hint { cursor: help; }
.fdl-dre-note {
  margin: 18px 0 0 0;
  padding-top: 14px;
  border-top: 1px solid #f1f5f9;
  font-size: 0.68rem;
  color: #94a3b8;
  line-height: 1.55;
  max-width: 46rem;
}
@media (max-width: 560px) {
  .fdl-dre-container { padding: 18px 16px 22px 16px; }
  .fdl-dre-result-value { font-size: 1.45rem; }
  .fdl-dre-value,
  .fdl-dre-result-value,
  .fdl-dre-margin-value {
    flex: 1 1 100%;
    text-align: right;
  }
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

    _df_mc = _meta_class(t_df.css_modifier)
    if t_df.css_modifier == "fdl-fat-kpi--warn":
        _df_meta_html = (
            f'<div class="fdl-fat-kpi-mid-meta {html.escape(_df_mc)}">'
            f"{html.escape(t_df.arrow)} "
            '<span class="fdl-fat-kpi-mid-diferenca-alerta">'
            f'{html.escape("alerta (>30% da venda)")}</span></div>'
        )
    else:
        _df_meta_html = (
            f'<div class="fdl-fat-kpi-mid-meta {html.escape(_df_mc)}">'
            f"{html.escape(t_df.arrow)} em faixa</div>"
        )

    _chip_row = f'<div class="fdl-fat-kpi-chip-row">{chips_html}</div>' if chips else ""

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
        '<div class="fdl-fat-kpi-mid-card fdl-fat-kpi-mid-card--diferenca">'
        '<div class="fdl-fat-kpi-mid-label">Diferença (lista − NF)</div>'
        f'<div class="fdl-fat-kpi-mid-value">{html.escape(diferenca_fmt)}</div>'
        f"{_df_meta_html}"
        "</div>"
        "</div>"
        f"{_chip_row}"
        "</div>"
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
    """Classes do bloco de resultado: positivo, negativo ou neutro (~zero)."""
    eps = 0.01
    if resultado_value > eps:
        return "fdl-dre-result fdl-dre-result--positive"
    if resultado_value < -eps:
        return "fdl-dre-result fdl-dre-result--negative"
    return "fdl-dre-result fdl-dre-result--neutral"


def _dre_line_premium(
    label: str,
    value: str,
    *,
    line_extra: str = "",
    value_class: str = "fdl-dre-value",
) -> str:
    le = f" fdl-dre-line {line_extra}".strip()
    return (
        f'<div class="{le}">'
        f'<span class="fdl-dre-label">{html.escape(label)}</span>'
        f'<span class="{value_class.strip()}">{html.escape(value)}</span>'
        "</div>"
    )


def _dre_total_line(label: str, value: str, *, negative: bool = False) -> str:
    vc = "fdl-dre-value fdl-dre-total-value"
    if negative:
        vc += " fdl-dre-value--negative"
    return (
        '<div class="fdl-dre-line fdl-dre-line--total">'
        f'<span class="fdl-dre-label fdl-dre-total-label">{html.escape(label)}</span>'
        f'<span class="{vc}">{html.escape(value)}</span>'
        "</div>"
    )


def build_dre_gerencial_premium_html(
    *,
    period_caption: str,
    valor_venda_fmt: str,
    rec_frete_fmt: str,
    diferenca_fmt: str,
    total_receita_fmt: str,
    enc_rows: list[tuple[str, str]],
    total_deducoes_fmt: str,
    resultado_fmt: str,
    resultado_value: float,
    margem_str: str,
    resultado_tooltip: str,
    margem_tooltip: str,
    footnote_plain: str = "",
    dif_highlight: bool,
) -> str:
    dif_vc = (
        "fdl-dre-value fdl-dre-value--warning"
        if dif_highlight
        else "fdl-dre-value"
    )
    enc_inner = (
        "".join(
            _dre_line_premium(
                lab,
                val,
                line_extra="fdl-dre-line--indent",
                value_class="fdl-dre-value fdl-dre-value--negative",
            )
            for lab, val in enc_rows
        )
        if enc_rows
        else '<div class="fdl-dre-line"><span class="fdl-dre-label">—</span></div>'
    )
    _shell = dre_gerencial_result_shell_class(resultado_value)
    _rt = html.escape(resultado_tooltip.strip(), quote=True)
    _mt = html.escape(margem_tooltip.strip(), quote=True)
    _foot_html = ""
    if footnote_plain.strip():
        _foot_html = f'<p class="fdl-dre-note">{html.escape(footnote_plain.strip())}</p>'
    return (
        '<div class="fdl-fat-premium fdl-fat-dre-premium-wrap">'
        '<div class="fdl-dre-container">'
        '<header class="fdl-dre-header">'
        '<h2 class="fdl-dre-title">DRE Gerencial</h2>'
        f'<span class="fdl-dre-periodo">{html.escape(period_caption)}</span>'
        "</header>"
        '<section class="fdl-dre-section">'
        '<div class="fdl-dre-section-header">'
        '<span class="fdl-dre-section-heading">Receita</span>'
        "</div>"
        + _dre_line_premium("Receita de venda (lista)", valor_venda_fmt)
        + _dre_line_premium("Receita frete (transp. própria)", rec_frete_fmt)
        + _dre_line_premium("Diferença (lista − fiscal)", diferenca_fmt, value_class=dif_vc)
        + _dre_total_line("Total Receita", total_receita_fmt, negative=False)
        + "</section>"
        '<div class="fdl-dre-divider"></div>'
        '<details class="fdl-dre-details" open>'
        '<summary>'
        '<span class="fdl-dre-section-heading">Deduções</span>'
        '<span class="fdl-dre-ded-chevron" aria-hidden="true">▼</span>'
        "</summary>"
        f'<div class="fdl-dre-details-body">{enc_inner}'
        + _dre_total_line("Total Deduções", total_deducoes_fmt, negative=True)
        + "</div>"
        "</details>"
        '<div class="fdl-dre-divider"></div>'
        f'<div class="{_shell}">'
        '<div class="fdl-dre-result-row">'
        f'<span class="fdl-dre-hint fdl-dre-result-label" title="{_rt}">Resultado</span>'
        f'<span class="fdl-dre-result-value">{html.escape(resultado_fmt)}</span>'
        "</div>"
        '<div class="fdl-dre-result-row fdl-dre-result-row--margin">'
        f'<span class="fdl-dre-hint fdl-dre-margin-label" title="{_mt}">Margem sobre venda</span>'
        f'<span class="fdl-dre-margin-value">{html.escape(margem_str)}</span>'
        "</div>"
        "</div>"
        f"{_foot_html}"
        "</div></div>"
    )


def faturamento_section_rule_html(label: str) -> str:
    return f'<div class="fdl-fat-premium fdl-fat-sec-rule"><span>{html.escape(label)}</span></div>'
