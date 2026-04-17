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


def _fmt_brl0(x: float) -> str:
    ax = abs(float(x))
    body = f"{ax:,.0f}".replace(",", "v").replace(".", ",").replace("v", ".")
    return ("-R$ " if x < 0 else "R$ ") + body


def render_health_panel(health: "HealthScore", *, show_details: bool = True) -> None:
    lbl, color, mark = health_level_meta(health.level)
    esc_tit = html.escape(f"Saude financeira - {health.periodo}")
    esc_sub = html.escape(str(health.empresa).replace("_", " ").title())
    esc_lbl = html.escape(f"{mark} {lbl}")
    st.html(
        f"""
<section style="background:linear-gradient(135deg,#1e293b 0%,#334155 100%);border-radius:12px;padding:24px;margin-bottom:16px;">
  <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:16px;">
    <div>
      <h2 style="margin:0;color:#f8fafc;font-size:1.45rem;font-weight:700;">{esc_tit}</h2>
      <p style="margin:6px 0 0 0;color:#94a3b8;font-size:0.92rem;">{esc_sub}</p>
    </div>
    <div style="text-align:center;min-width:96px;">
      <div style="width:80px;height:80px;border-radius:50%;background:{color}22;border:4px solid {color};display:flex;align-items:center;justify-content:center;margin:0 auto;">
        <span style="font-size:1.75rem;font-weight:700;color:{color};">{int(health.score)}</span>
      </div>
      <p style="margin:8px 0 0 0;color:{color};font-weight:600;font-size:0.95rem;">{esc_lbl}</p>
    </div>
  </div>
</section>
"""
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        d = health.tendencia_pp
        st.metric(
            "Margem",
            f"{health.margem_pct:.1f}%",
            delta=(f"{d:+.1f} pp vs mes ant." if d is not None else None),
            delta_color=(
                "inverse"
                if d is not None and d < 0
                else "normal"
                if d is not None and d > 0
                else "off"
            ),
            help="Margem = resultado / receita (Vl_Venda) no recorte.",
        )
    with col2:
        st.metric(
            "Resultado",
            _fmt_brl0(health.resultado),
            delta=None,
            delta_color="normal",
            help=health.periodo,
        )
    with col3:
        st.metric(
            "Custo / Receita",
            f"{health.custo_pct:.1f}%",
            delta=None,
            delta_color="inverse" if health.custo_pct > 50 else "normal",
            help="Custo produto / receita. Benchmark referencia 50%.",
        )
    with col4:
        vg = health.vs_grupo_pp
        mg = health.margem_grupo
        h = (
            f"Media de margem das outras orgs no mesmo mes civil: {mg:.1f}%."
            if mg is not None
            else "Sem benchmark (recorte consolidado ou uma unica org)."
        )
        st.metric(
            "vs Grupo",
            (f"{vg:+.1f} pp" if vg is not None else "N/A"),
            delta=None,
            delta_color="inverse" if (vg is not None and vg < 0) else "normal",
            help=h,
        )

    st.divider()

    if health.diagnosticos:
        st.markdown("#### Diagnostico automatico")
        for diag in health.diagnosticos:
            _render_diagnostico(diag)

    if health.skus_risco and show_details:
        with st.expander(f"SKUs em risco ({len(health.skus_risco)})", expanded=False):
            _render_skus_risco(health.skus_risco)


def _render_diagnostico(diag: Diagnostico) -> None:
    parts: list[str] = [f"**{diag.titulo}**", diag.detalhe]
    if diag.acao:
        parts.append(f"**Acao sugerida:** {diag.acao}")
    body = "\n\n".join(parts)
    if diag.nivel == AlertLevel.CRITICAL:
        st.error(body)
    elif diag.nivel in (AlertLevel.HIGH, AlertLevel.MEDIUM):
        st.warning(body)
    else:
        st.info(body)


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
        f"Total prejuizo (amostra): R$ {abs(total_prejuizo):,.2f} · Receita afetada: R$ {total_receita:,.2f}"
    )


def render_faturamento_health_panel_if_enabled(
    df_faturamento: pd.DataFrame,
    *,
    nf_d_ini: Any,
    nf_d_fim: Any,
    empresas_sel: tuple[str, ...],
    org_sidebar: str,
) -> None:
    """
    Filtro: emissao NF + CUSTO_OK + empresas (rotulos), alinhado ao painel NF minimo.
    Score e benchmark usam o mesmo mes civil do inicio do intervalo (tendencia = mes anterior).
    """
    if df_faturamento is None or df_faturamento.empty:
        return
    req = {"Nota_Data_Emissao", "Vl_Venda", "Resultado", "Custo_Produto_Total", "org_id", "Status_Custo"}
    if not req.issubset(set(df_faturamento.columns)):
        return
    if not st.checkbox(
        "Mostrar painel de saude financeira (score, diagnosticos, SKUs em risco)",
        value=True,
        key="fdl_fat_min_health_panel_show",
    ):
        return

    sl = slice_linhas_nf_periodo(df_faturamento, d_ini=nf_d_ini, d_fim=nf_d_fim, empresas_sel=empresas_sel)
    if sl.empty:
        st.caption("Painel de saude: sem linhas CUSTO_OK no intervalo de emissao NF selecionado.")
        return

    ano, mes, periodo_lbl = periodo_mes_de_datas(nf_d_ini, nf_d_fim)
    org_alvo = inferir_org_id_alvo(sl, org_sidebar)
    df_ant = obter_dados_periodo_anterior(df_faturamento, org_alvo, ano, mes)
    df_grupo = obter_dados_grupo(df_faturamento, ano, mes)

    health = calcular_health_score(
        sl,
        org_alvo,
        ano,
        mes,
        df_anterior=df_ant,
        df_grupo=df_grupo,
        periodo_override=periodo_lbl,
    )
    render_health_panel(health, show_details=True)


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
