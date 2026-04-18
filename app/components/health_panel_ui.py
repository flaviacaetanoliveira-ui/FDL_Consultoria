"""
Painel Streamlit: saude financeira (score + diagnosticos + SKUs em risco).

Nota: evitar HTML dentro de ``st.markdown`` — o motor Markdown trata ``>`` como
blockquote e quebra as tags. Usamos ``st.html``, ``st.metric`` e callouts nativos.
"""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any

import pandas as pd
import streamlit as st

from app.components.health_score import (
    AlertLevel,
    Diagnostico,
    HealthLevel,
    SKURisco,
    calcular_health_score,
    health_level_meta,
    inferir_org_id_alvo,
    obter_dados_grupo,
    obter_dados_periodo_anterior,
    periodo_mes_de_datas,
    slice_linhas_nf_periodo,
)

if TYPE_CHECKING:
    from app.components.health_score import HealthScore

HEALTH_PANEL_CSS = """
<style>
.fdl-health-header {
  background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
  border-radius: 12px;
  padding: 20px 24px;
  margin-bottom: 16px;
}
.fdl-health-header-row {
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 16px;
}
.fdl-health-title {
  margin: 0;
  color: #f8fafc;
  font-size: 1.25rem;
  font-weight: 600;
}
.fdl-health-subtitle {
  margin: 6px 0 0 0;
  color: #94a3b8;
  font-size: 0.85rem;
}
.fdl-health-score-wrap {
  text-align: center;
  min-width: 72px;
}
.fdl-health-score-circle {
  width: 56px;
  height: 56px;
  border-radius: 50%;
  display: flex;
  align-items: center;
  justify-content: center;
  margin: 0 auto;
  box-sizing: border-box;
}
.fdl-health-score-value {
  font-size: 1.75rem;
  font-weight: 700;
  line-height: 1;
}
.fdl-health-status-label {
  margin: 8px 0 0 0;
  font-weight: 600;
  font-size: 0.8rem;
}
.fdl-health-panel-inner { font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; }
.fdl-health-metrics-container {
  display: flex;
  flex-wrap: wrap;
  gap: 0;
  background: #f8fafc;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  padding: 20px 24px;
  margin: 20px 0;
}
.fdl-health-metric-item {
  flex: 1 1 140px;
  text-align: center;
  padding: 4px 12px;
}
.fdl-health-metric-item:not(:last-child) {
  border-right: 1px solid #e2e8f0;
  padding-right: 24px;
}
.fdl-health-metric-label {
  font-size: 0.75rem;
  font-weight: 600;
  color: #64748b;
  text-transform: uppercase;
  letter-spacing: 0.06em;
  margin-bottom: 8px;
}
.fdl-health-metric-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: #1e293b;
  font-variant-numeric: tabular-nums;
  line-height: 1.2;
}
.fdl-health-metric-delta {
  font-size: 0.75rem;
  font-weight: 500;
  color: #64748b;
  margin-top: 6px;
}
.fdl-health-metric-delta--down { color: #b91c1c; }
.fdl-health-metric-delta--up { color: #047857; }
.fdl-health-summary {
  display: flex;
  align-items: flex-start;
  gap: 12px;
  border-radius: 8px;
  padding: 16px 20px;
  margin-bottom: 24px;
  border: 1px solid #e2e8f0;
  border-left: 4px solid #94a3b8;
}
.fdl-health-summary--saudavel {
  background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
  border-color: #bbf7d0;
  border-left-color: #22c55e;
}
.fdl-health-summary--atencao {
  background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
  border-color: #fcd34d;
  border-left-color: #f59e0b;
}
.fdl-health-summary--risco {
  background: linear-gradient(135deg, #fff7ed 0%, #ffedd5 100%);
  border-color: #fdba74;
  border-left-color: #f97316;
}
.fdl-health-summary--critico {
  background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
  border-color: #fca5a5;
  border-left-color: #ef4444;
}
.fdl-health-summary-icon { font-size: 1.25rem; flex-shrink: 0; line-height: 1.3; }
.fdl-health-summary-text {
  font-size: 0.95rem;
  color: #334155;
  line-height: 1.55;
}
.fdl-health-summary--atencao .fdl-health-summary-text,
.fdl-health-summary--risco .fdl-health-summary-text { color: #92400e; }
.fdl-health-summary--critico .fdl-health-summary-text { color: #7f1d1d; }
.fdl-health-summary--saudavel .fdl-health-summary-text { color: #14532d; }
.fdl-health-diagnostics-title {
  font-size: 1.1rem;
  font-weight: 700;
  color: #1e293b;
  margin: 8px 0 16px 0;
  display: flex;
  align-items: center;
  gap: 8px;
}
.fdl-health-diagnostics-title .fdl-health-diag-ico { font-size: 1rem; }
.fdl-health-diag-card {
  border-radius: 10px;
  padding: 16px 48px 16px 20px;
  margin-bottom: 12px;
  border-left: 4px solid;
  position: relative;
  box-sizing: border-box;
}
.fdl-health-severity-high {
  background: linear-gradient(135deg, #fef2f2 0%, #fee2e2 100%);
  border-left-color: #ef4444;
}
.fdl-health-severity-high .fdl-health-diagnostic-title { color: #991b1b; }
.fdl-health-severity-high .fdl-health-diag-ico-tr { position: absolute; right: 16px; top: 16px; font-size: 1.15rem; }
.fdl-health-severity-medium {
  background: linear-gradient(135deg, #fffbeb 0%, #fef3c7 100%);
  border-left-color: #f59e0b;
}
.fdl-health-severity-medium .fdl-health-diagnostic-title { color: #92400e; }
.fdl-health-severity-medium .fdl-health-diag-ico-tr { position: absolute; right: 16px; top: 16px; font-size: 1.15rem; }
.fdl-health-severity-low {
  background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%);
  border-left-color: #22c55e;
}
.fdl-health-severity-low .fdl-health-diagnostic-title { color: #166534; }
.fdl-health-severity-low .fdl-health-diag-ico-tr { position: absolute; right: 16px; top: 16px; font-size: 1.25rem; color: #22c55e; font-weight: 700; }
.fdl-health-severity-info {
  background: linear-gradient(135deg, #eff6ff 0%, #dbeafe 100%);
  border-left-color: #3b82f6;
}
.fdl-health-severity-info .fdl-health-diagnostic-title { color: #1e40af; }
.fdl-health-diagnostic-title {
  font-size: 0.95rem;
  font-weight: 600;
  margin-bottom: 6px;
}
.fdl-health-diagnostic-detail {
  font-size: 0.85rem;
  color: #4b5563;
  margin-bottom: 8px;
  line-height: 1.45;
}
.fdl-health-diagnostic-action {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  font-size: 0.85rem;
  font-weight: 600;
  color: #1e40af;
  background: rgba(59, 130, 246, 0.12);
  padding: 8px 14px;
  border-radius: 6px;
  margin-top: 4px;
}
.fdl-health-diagnostic-action::before { content: "→"; font-weight: 700; }
.fdl-health-skus-wrap { margin-top: 20px; }
.fdl-health-skus-details {
  background: #ffffff;
  border: 1px solid #e2e8f0;
  border-radius: 10px;
  overflow: hidden;
}
.fdl-health-skus-details > summary {
  list-style: none;
  cursor: pointer;
  display: flex;
  justify-content: space-between;
  align-items: center;
  flex-wrap: wrap;
  gap: 10px;
  padding: 14px 20px;
  background: #f8fafc;
  border-bottom: 1px solid #e2e8f0;
}
.fdl-health-skus-details > summary::-webkit-details-marker { display: none; }
.fdl-health-skus-head {
  display: flex;
  align-items: center;
  gap: 8px;
  font-size: 0.9rem;
  font-weight: 600;
  color: #374151;
}
.fdl-health-skus-badge {
  background: #fee2e2;
  color: #991b1b;
  font-size: 0.75rem;
  font-weight: 700;
  padding: 4px 10px;
  border-radius: 12px;
}
.fdl-health-skus-preview {
  font-size: 0.8rem;
  color: #6b7280;
  padding: 10px 20px 14px 20px;
  background: #fafafa;
  border-bottom: 1px solid #f1f5f9;
  line-height: 1.45;
}
.fdl-health-skus-body { padding: 12px 16px 16px 16px; }
.fdl-health-skus-preview-inline {
  font-size: 0.8rem;
  color: #6b7280;
  margin: 0 0 14px 0;
  line-height: 1.45;
}
.fdl-health-skus-spacer { height: 6px; }
@media (max-width: 640px) {
  .fdl-health-metric-item { border-right: none !important; padding-right: 8px !important; border-bottom: 1px solid #f1f5f9; }
  .fdl-health-metric-item:last-child { border-bottom: none; }
}
</style>
"""


def _diag_md_para_html(md: str) -> str:
    """``**texto**`` → negrito; restante escapado para HTML."""
    chunks = md.split("**")
    out: list[str] = []
    for i, seg in enumerate(chunks):
        if i % 2 == 0:
            out.append(html.escape(seg))
        else:
            out.append("<strong>" + html.escape(seg) + "</strong>")
    joined = "".join(out)
    return joined.replace("\n\n", "</p><p>").replace("\n", "<br/>")


def _fmt_brl0(x: float) -> str:
    ax = abs(float(x))
    body = f"{ax:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")
    return ("-R$ " if x < 0 else "R$ ") + body


def _diagnostic_severity_class(diag: Diagnostico) -> str:
    """Mapeia nível/tipo do diagnóstico para classe visual (sem alterar regras de negócio)."""
    if diag.nivel == AlertLevel.CRITICAL:
        return "fdl-health-diag-card fdl-health-severity-high"
    if diag.nivel == AlertLevel.HIGH:
        return "fdl-health-diag-card fdl-health-severity-high"
    if diag.nivel == AlertLevel.MEDIUM:
        return "fdl-health-diag-card fdl-health-severity-medium"
    if diag.tipo == "POSITIVO" and diag.nivel == AlertLevel.INFO:
        return "fdl-health-diag-card fdl-health-severity-low"
    return "fdl-health-diag-card fdl-health-severity-info"


def _severity_corner_icon(cls: str) -> str:
    if "severity-low" in cls:
        return '<span class="fdl-health-diag-ico-tr" aria-hidden="true">✓</span>'
    if "severity-high" in cls or "severity-medium" in cls:
        return '<span class="fdl-health-diag-ico-tr" aria-hidden="true">⚠️</span>'
    return ""


def _executive_summary_html(health: "HealthScore") -> str:
    """Resumo em uma frase a partir solely dos campos já calculados no HealthScore."""
    hl_name, _hl_c, _mk = health_level_meta(health.level)
    parts: list[str] = [
        f"Operação classificada como <strong>{html.escape(hl_name)}</strong>",
        f"margem <strong>{health.margem_pct:.1f}%</strong>",
        f"custo/receita <strong>{health.custo_pct:.1f}%</strong>",
    ]
    if health.tendencia_pp is not None:
        parts.append(f"tendência <strong>{health.tendencia_pp:+.1f} pp</strong> vs. mês anterior")
    if health.vs_grupo_pp is not None:
        parts.append(f"vs. grupo <strong>{health.vs_grupo_pp:+.1f} pp</strong>")
    text = " · ".join(parts) + "."
    summary_cls = {
        HealthLevel.SAUDAVEL: "fdl-health-summary--saudavel",
        HealthLevel.ATENCAO: "fdl-health-summary--atencao",
        HealthLevel.RISCO: "fdl-health-summary--risco",
        HealthLevel.CRITICO: "fdl-health-summary--critico",
    }[health.level]
    icons = {
        HealthLevel.SAUDAVEL: "✓",
        HealthLevel.ATENCAO: "⚡",
        HealthLevel.RISCO: "⚠️",
        HealthLevel.CRITICO: "⛔",
    }
    ico = icons[health.level]
    return (
        f'<div class="fdl-health-summary {summary_cls}">'
        f'<span class="fdl-health-summary-icon" aria-hidden="true">{ico}</span>'
        f'<span class="fdl-health-summary-text">{text}</span>'
        "</div>"
    )


def _metrics_block_html(health: "HealthScore") -> str:
    """Métricas principais em container único (substitui colunas soltas)."""
    h_margem = html.escape(
        "Margem = resultado ÷ receita nas linhas com custo válido no período."
    )
    h_res = html.escape(
        "Soma do resultado em linhas de pedido com custo válido no período. "
        "Usa receita de venda (lista) para cálculo de margem. "
        "Não aplica filtro de plataforma — por isso pode diferir do Resultado dos KPIs."
    )
    h_custo = html.escape("Custo produto ÷ receita. Referência orientativa ~50%.")
    d = health.tendencia_pp
    delta_html = ""
    if d is not None:
        dc = "fdl-health-metric-delta--down" if d < 0 else "fdl-health-metric-delta--up" if d > 0 else ""
        delta_html = (
            f'<div class="fdl-health-metric-delta {dc}">{html.escape(f"{d:+.1f} pp vs mês ant.")}</div>'
        )

    blocks: list[str] = [
        '<div class="fdl-health-metric-item">'
        f'<div class="fdl-health-metric-label" title="{h_margem}">Margem</div>'
        f'<div class="fdl-health-metric-value">{html.escape(f"{health.margem_pct:.1f}%")}</div>'
        f"{delta_html}"
        "</div>",
        '<div class="fdl-health-metric-item">'
        f'<div class="fdl-health-metric-label" title="{h_res}">Resultado</div>'
        f'<div class="fdl-health-metric-value">{html.escape(_fmt_brl0(health.resultado))}</div>'
        "</div>",
        '<div class="fdl-health-metric-item">'
        f'<div class="fdl-health-metric-label" title="{h_custo}">Custo / Receita</div>'
        f'<div class="fdl-health-metric-value">{html.escape(f"{health.custo_pct:.1f}%")}</div>'
        "</div>",
    ]
    vg = health.vs_grupo_pp
    if vg is not None:
        mg = health.margem_grupo
        h_vg = html.escape(
            f"Média de margem das outras empresas no mesmo mês civil: {mg:.1f}%."
            if mg is not None
            else "Sem benchmark (recorte consolidado ou uma única empresa)."
        )
        blocks.append(
            '<div class="fdl-health-metric-item">'
            f'<div class="fdl-health-metric-label" title="{h_vg}">vs Grupo</div>'
            f'<div class="fdl-health-metric-value">{html.escape(f"{vg:+.1f} pp")}</div>'
            "</div>"
        )

    inner = "".join(blocks)
    return (
        '<div class="fdl-health-panel-inner">'
        f'<div class="fdl-health-metrics-container">{inner}</div>'
        "</div>"
    )


def _render_diagnostico_card(diag: Diagnostico) -> None:
    scls = _diagnostic_severity_class(diag)
    ico = _severity_corner_icon(scls)
    tit = html.escape(diag.titulo)
    det_html = _diag_md_para_html(diag.detalhe)
    action_html = ""
    if diag.acao:
        action_html = (
            f'<div class="fdl-health-diagnostic-action">{html.escape(diag.acao)}</div>'
        )
    st.html(
        f'<div class="{html.escape(scls)}">'
        f"{ico}"
        f'<div class="fdl-health-diagnostic-title">{tit}</div>'
        f'<div class="fdl-health-diagnostic-detail">{det_html}</div>'
        f"{action_html}"
        "</div>"
    )


def _skus_preview_line(skus: list[SKURisco]) -> str:
    top = sorted(skus, key=lambda s: s.resultado)[:3]
    if not top:
        return ""
    parts = [f"{html.escape(str(s.sku))} ({html.escape(_fmt_brl0(s.resultado))})" for s in top]
    return " · ".join(parts)


def render_health_panel(
    health: "HealthScore",
    *,
    show_details: bool = True,
    header_diagnostic_checkbox: bool = False,
) -> None:
    lbl, color, mark = health_level_meta(health.level)
    esc_tit = html.escape(f"Saúde financeira – {health.periodo}")
    esc_sub = html.escape(str(health.empresa).replace("_", " ").title())
    esc_lbl = html.escape(f"{mark} {lbl}")
    if header_diagnostic_checkbox:
        _, _tb2 = st.columns([3, 1])
        with _tb2:
            st.checkbox(
                "Exibir diagnóstico",
                value=True,
                key="fdl_fat_min_health_panel_show",
                help="Mostra ou oculta detalhes de diagnóstico e SKUs em risco (o resumo do score permanece visível ao recarregar).",
            )
    st.html(
        HEALTH_PANEL_CSS
        + f"""
<section class="fdl-health-header">
  <div class="fdl-health-header-row">
    <div>
      <h2 class="fdl-health-title">{esc_tit}</h2>
      <p class="fdl-health-subtitle">{esc_sub}</p>
    </div>
    <div class="fdl-health-score-wrap">
      <div class="fdl-health-score-circle" style="background:{color}22;border:3px solid {color};">
        <span class="fdl-health-score-value" style="color:{color};">{int(health.score)}</span>
      </div>
      <p class="fdl-health-status-label" style="color:{color};">{esc_lbl}</p>
    </div>
  </div>
</section>
"""
    )

    st.html(_metrics_block_html(health))
    st.html(_executive_summary_html(health))

    if show_details and (health.diagnosticos or health.skus_risco):
        with st.container(border=True):
            if health.diagnosticos:
                st.html(
                    '<h3 class="fdl-health-diagnostics-title">'
                    '<span class="fdl-health-diag-ico" aria-hidden="true">🔍</span>'
                    "Diagnóstico automático"
                    "</h3>"
                )
                for diag in health.diagnosticos:
                    _render_diagnostico_card(diag)
            if health.skus_risco:
                n = len(health.skus_risco)
                preview_plain = _skus_preview_line(health.skus_risco)
                st.html('<div class="fdl-health-skus-spacer" aria-hidden="true"></div>')
                _exp_lab = f"📦 SKUs em risco ({n})"
                with st.expander(_exp_lab, expanded=False):
                    if preview_plain:
                        st.markdown(
                            f'<p class="fdl-health-skus-preview-inline"><strong>Top 3</strong> '
                            f"(pior resultado): {preview_plain}</p>",
                            unsafe_allow_html=True,
                        )
                    _render_skus_risco(health.skus_risco)


def _render_skus_risco(skus: list[SKURisco]) -> None:
    rows = []
    for sku in skus:
        rows.append(
            {
                "SKU": sku.sku,
                "Receita": sku.receita,
                "Margem %": sku.margem_pct,
                "Custo %": sku.custo_pct,
                "Resultado": sku.resultado,
                "Ajuste unit.": sku.ajuste_breakeven,
                "Ajuste % lista": sku.ajuste_breakeven_pct,
            }
        )
    st.dataframe(
        pd.DataFrame(rows),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Receita": st.column_config.NumberColumn("Receita", format="R$ %,.2f"),
            "Resultado": st.column_config.NumberColumn("Resultado", format="R$ %,.2f"),
            "Margem %": st.column_config.NumberColumn("Margem %", format="%.1f"),
            "Custo %": st.column_config.NumberColumn("Custo %", format="%.1f"),
            "Ajuste unit.": st.column_config.NumberColumn("Ajuste unit.", format="R$ %,.2f"),
            "Ajuste % lista": st.column_config.NumberColumn("Ajuste % lista", format="%.1f"),
        },
    )
    total_prejuizo = sum(s.resultado for s in skus)
    total_receita = sum(s.receita for s in skus)
    st.caption(
        f"Total prejuízo (amostra): R$ {abs(total_prejuizo):,.2f} · Receita afetada: R$ {total_receita:,.2f}"
    )


def render_faturamento_health_panel_if_enabled(
    df_faturamento: pd.DataFrame,
    *,
    nf_d_ini: Any,
    nf_d_fim: Any,
    empresas_sel: tuple[str, ...],
    org_sidebar: str,
    plataformas_sel: tuple[str, ...] = (),
    coluna_temporal: str = "Nota_Data_Emissao",
) -> None:
    """
    Recorte no eixo ``coluna_temporal`` (``Data`` = venda, alinhado ao Resultado Gerencial; ``Nota_Data_Emissao`` = legado).

    CUSTO_OK + empresas + plataformas opcionais. Benchmarks (mês anterior / grupo) usam o mesmo eixo temporal.
    Checkbox «Exibir diagnóstico» no topo do painel (``fdl_fat_min_health_panel_show``, padrão ``True``).
    """
    if df_faturamento is None or df_faturamento.empty:
        return
    req_base = {"Resultado", "Custo_Produto_Total", "org_id", "Status_Custo"}
    if not req_base.issubset(set(df_faturamento.columns)):
        return
    if coluna_temporal not in df_faturamento.columns:
        return
    if "Vl_Venda" not in df_faturamento.columns and "Valor total" not in df_faturamento.columns:
        return

    df_w = df_faturamento.copy()
    if "Vl_Venda" not in df_w.columns:
        df_w["Vl_Venda"] = pd.to_numeric(df_w["Valor total"], errors="coerce").fillna(0.0)

    sl = slice_linhas_nf_periodo(
        df_w,
        d_ini=nf_d_ini,
        d_fim=nf_d_fim,
        empresas_sel=empresas_sel,
        coluna_temporal=coluna_temporal,
        plataformas_sel=plataformas_sel,
    )
    if sl.empty:
        _eixo = "data da venda" if coluna_temporal == "Data" else "emissão NF"
        st.caption(f"Painel de saúde: sem linhas com custo válido no intervalo ({_eixo}) selecionado.")
        return

    ano, mes, periodo_lbl = periodo_mes_de_datas(nf_d_ini, nf_d_fim)
    org_alvo = inferir_org_id_alvo(sl, org_sidebar)
    df_ant = obter_dados_periodo_anterior(
        df_w, org_alvo, ano, mes, coluna_temporal=coluna_temporal
    )
    df_grupo = obter_dados_grupo(df_w, ano, mes, coluna_temporal=coluna_temporal)

    health = calcular_health_score(
        sl,
        org_alvo,
        ano,
        mes,
        df_anterior=df_ant,
        df_grupo=df_grupo,
        periodo_override=periodo_lbl,
    )
    show_diag = bool(st.session_state.get("fdl_fat_min_health_panel_show", True))
    render_health_panel(health, show_details=show_diag, header_diagnostic_checkbox=True)


def render_health_mini(health: "HealthScore") -> None:
    lbl, color, mark = health_level_meta(health.level)
    esc = html.escape(f"{mark} {lbl}")
    st.html(
        f"""
<div style="display:flex;align-items:center;gap:12px;padding:12px;background:#f8fafc;border-radius:8px;border-left:4px solid {color};max-width:28rem;">
  <div style="min-width:48px;height:48px;border-radius:50%;background:{color}22;display:flex;align-items:center;justify-content:center;">
    <span style="font-size:1.1rem;font-weight:700;color:{color};">{int(health.score)}</span>
  </div>
  <div>
    <p style="margin:0;font-weight:600;color:#1e293b;">{esc}</p>
    <p style="margin:2px 0 0 0;font-size:0.8rem;color:#64748b;">Margem: {health.margem_pct:.1f}%</p>
  </div>
</div>
"""
    )
