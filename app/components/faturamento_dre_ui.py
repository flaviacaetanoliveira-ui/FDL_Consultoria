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
.fdl-fat-dre-premium-card {
  border: 1px solid #e2e8f0;
  border-radius: 14px;
  background: #ffffff;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06), 0 10px 28px rgba(15, 23, 42, 0.05);
  overflow: hidden;
}
.fdl-fat-dre-premium-head {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding: 14px 18px 10px 18px;
  border-bottom: 1px solid #f1f5f9;
  background: #fafbfc;
}
.fdl-fat-dre-premium-title {
  font-size: 1rem;
  font-weight: 800;
  color: #0f172a;
  letter-spacing: -0.02em;
  margin: 0;
}
.fdl-fat-dre-premium-period {
  font-size: 0.72rem;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.08em;
}
.fdl-fat-dre-premium-body { padding: 4px 0 2px 0; }
.fdl-fat-dre-sec-title {
  font-size: 0.66rem;
  font-weight: 800;
  letter-spacing: 0.11em;
  text-transform: uppercase;
  color: #64748b;
  margin: 14px 18px 6px 18px;
}
.fdl-fat-dre-tree {
  margin: 0 12px 8px 12px;
  padding: 4px 6px 8px 10px;
  border-left: 2px solid #e2e8f0;
}
.fdl-fat-dre-tree-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(9.5rem, max-content);
  column-gap: 1rem;
  align-items: baseline;
  padding: 6px 0 6px 4px;
  font-size: 0.875rem;
  color: #1e293b;
}
.fdl-fat-premium .fdl-fat-dre-tree-glyph,
.fdl-fat-premium .fdl-fat-dre-tree-val {
  font-family: "JetBrains Mono", ui-monospace, "Cascadia Code", monospace;
}
.fdl-fat-dre-tree-glyph {
  font-size: 0.8rem;
  color: #94a3b8;
  margin-right: 6px;
  user-select: none;
}
.fdl-fat-dre-tree-lab { min-width: 0; line-height: 1.35; }
.fdl-fat-dre-tree-val {
  font-size: 1rem;
  font-weight: 500;
  text-align: right;
  white-space: nowrap;
  font-variant-numeric: tabular-nums;
  color: #0f172a;
}
.fdl-fat-dre-tree-val--neg { color: var(--fdl-danger); }
.fdl-fat-dre-tree-val--amber { color: var(--fdl-warning); font-weight: 600; }
.fdl-fat-dre-tree-val--pos { color: var(--fdl-success); font-weight: 700; }
.fdl-fat-dre-section.fdl-fat-dre-deducoes {
  margin: 4px 12px 12px 12px;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  background: #fafbfc;
  padding: 0;
}
.fdl-fat-dre-deducoes summary.fdl-fat-dre-section-title {
  cursor: pointer;
  list-style: none;
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 10px 14px;
  font-size: 0.66rem;
  font-weight: 800;
  letter-spacing: 0.11em;
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
  border-bottom: 1px solid #e2e8f0;
}
.fdl-fat-dre-toggle {
  font-size: 0.75rem;
  color: var(--fdl-neutral-500, #6b7280);
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
.fdl-fat-dre-close-block {
  margin: 10px 14px 14px 14px;
  padding: 12px 14px;
  border-radius: 10px;
  border: 1px solid #d8dee6;
  background: #f8fafc;
}
.fdl-fat-dre-close-row {
  display: grid;
  grid-template-columns: minmax(0, 1fr) minmax(9.5rem, max-content);
  align-items: baseline;
  column-gap: 1rem;
}
.fdl-fat-dre-close-row + .fdl-fat-dre-close-row { margin-top: 8px; padding-top: 8px; border-top: 1px dashed #e2e8f0; }
.fdl-fat-dre-foot-note {
  margin: 10px 18px 14px 18px;
  font-size: 0.65rem;
  color: #94a3b8;
  line-height: 1.45;
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
        f'<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--result {html.escape(t_res.css_modifier)}">'
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


def _tree_row(glyph: str, label: str, value: str, *, val_class: str = "") -> str:
    cls = "fdl-fat-dre-tree-val"
    if val_class.strip():
        cls += " " + val_class.strip()
    return (
        f'<div class="fdl-fat-dre-tree-row">'
        f'<div class="fdl-fat-dre-tree-lab"><span class="fdl-fat-dre-tree-glyph">{html.escape(glyph)}</span>'
        f"{html.escape(label)}</div>"
        f'<div class="{cls}">{html.escape(value)}</div></div>'
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
    footnote_plain: str,
    dif_highlight: bool,
) -> str:
    dif_cls = "fdl-fat-dre-tree-val--amber" if dif_highlight else ""
    enc_inner = (
        "".join(
            _tree_row(
                "├─" if i < len(enc_rows) - 1 else "└─",
                lab,
                val,
                val_class="fdl-fat-dre-tree-val--neg",
            )
            for i, (lab, val) in enumerate(enc_rows)
        )
        if enc_rows
        else '<div class="fdl-fat-dre-tree-row"><span class="fdl-fat-dre-tree-lab">—</span></div>'
    )
    if resultado_value > 0:
        res_cls = "fdl-fat-dre-tree-val--pos"
    elif resultado_value < 0:
        res_cls = "fdl-fat-dre-tree-val--neg"
    else:
        res_cls = ""
    return (
        '<div class="fdl-fat-premium fdl-fat-dre-premium-wrap">'
        '<div class="fdl-fat-dre-premium-card">'
        '<div class="fdl-fat-dre-premium-head">'
        '<h3 class="fdl-fat-dre-premium-title">DRE gerencial</h3>'
        f'<span class="fdl-fat-dre-premium-period">{html.escape(period_caption)}</span></div>'
        '<div class="fdl-fat-dre-premium-body">'
        '<div class="fdl-fat-dre-sec-title">Receita</div>'
        '<div class="fdl-fat-dre-tree">'
        + _tree_row("├─", "Receita de venda (lista)", valor_venda_fmt)
        + _tree_row("├─", "Receita frete (transp. própria)", rec_frete_fmt)
        + _tree_row("└─", "Diferença (lista − fiscal)", diferenca_fmt, val_class=dif_cls)
        + "</div>"
        '<details class="fdl-fat-dre-section fdl-fat-dre-deducoes" open>'
        '<summary class="fdl-fat-dre-section-title">DEDUÇÕES'
        '<span class="fdl-fat-dre-toggle" aria-hidden="true">▼</span></summary>'
        f'<div class="fdl-fat-dre-tree" style="border-left:none;margin-left:8px">{enc_inner}</div>'
        "</details>"
        '<div class="fdl-fat-dre-close-block">'
        '<div class="fdl-fat-dre-close-row">'
        '<div style="font-weight:800;font-size:0.95rem;color:#0f172a">Resultado</div>'
        f'<div class="fdl-fat-dre-tree-val {res_cls}">{html.escape(resultado_fmt)}</div></div>'
        '<div class="fdl-fat-dre-close-row">'
        '<div style="font-weight:600;font-size:0.85rem;color:#64748b">Margem sobre venda</div>'
        f'<div class="fdl-fat-dre-tree-val">{html.escape(margem_str)}</div></div>'
        '</div>'
        f'<p class="fdl-fat-dre-foot-note">{html.escape(footnote_plain)}</p>'
        "</div></div></div>"
    )


def faturamento_section_rule_html(label: str) -> str:
    return f'<div class="fdl-fat-premium fdl-fat-sec-rule"><span>{html.escape(label)}</span></div>'
