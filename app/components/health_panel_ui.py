"""
Painel Streamlit: saude financeira (score + diagnosticos + SKUs em risco).
"""

from __future__ import annotations

import html
from typing import TYPE_CHECKING, Any

import pandas as pd
import streamlit as st

from app.components.health_score import (
    Diagnostico,
    SKURisco,
    alert_level_meta,
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
    st.markdown(
        f"""
<div style="
    background: linear-gradient(135deg, #1e293b 0%, #334155 100%);
    border-radius: 12px;
    padding: 24px;
    margin-bottom: 24px;
">
    <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 16px;">
        <div>
            <h2 style="margin: 0; color: #f8fafc; font-size: 1.5rem;">{esc_tit}</h2>
            <p style="margin: 4px 0 0 0; color: #94a3b8; font-size: 0.9rem;">{esc_sub}</p>
        </div>
        <div style="text-align: center;">
            <div style="
                width: 80px;
                height: 80px;
                border-radius: 50%;
                background: {color}22;
                border: 4px solid {color};
                display: flex;
                align-items: center;
                justify-content: center;
                margin: 0 auto;
            ">
                <span style="font-size: 1.8rem; font-weight: 700; color: {color};">
                    {int(health.score)}
                </span>
            </div>
            <p style="margin: 8px 0 0 0; color: {color}; font-weight: 600;">
                {html.escape(mark)} {html.escape(lbl)}
            </p>
        </div>
    </div>
</div>
""",
        unsafe_allow_html=True,
    )

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        _render_kpi_card(
            titulo="Margem",
            valor=f"{health.margem_pct:.1f}%",
            subtitulo="sobre receita",
            variacao=health.tendencia_pp,
            variacao_label="vs mes ant.",
            cor_positivo=health.margem_pct >= 0,
        )
    with col2:
        _render_kpi_card(
            titulo="Resultado",
            valor=_fmt_brl0(health.resultado),
            subtitulo=health.periodo,
            variacao=None,
            cor_positivo=health.resultado >= 0,
        )
    with col3:
        _render_kpi_card(
            titulo="Custo / Receita",
            valor=f"{health.custo_pct:.1f}%",
            subtitulo="benchmark 50%",
            variacao=None,
            cor_positivo=health.custo_pct <= 50,
        )
    with col4:
        vg = health.vs_grupo_pp
        sub4 = ""
        if health.margem_grupo is not None:
            sub4 = f"media outras: {health.margem_grupo:.1f}%"
        _render_kpi_card(
            titulo="vs Grupo",
            valor=(f"{vg:+.1f} pp" if vg is not None else "N/A"),
            subtitulo=sub4,
            variacao=vg,
            cor_positivo=(vg >= 0) if vg is not None else True,
        )

    st.markdown("<div style='height: 16px'></div>", unsafe_allow_html=True)

    if health.diagnosticos:
        st.markdown("#### Diagnostico automatico")
        for diag in health.diagnosticos:
            _render_diagnostico(diag)

    if health.skus_risco and show_details:
        with st.expander(f"SKUs em risco ({len(health.skus_risco)})", expanded=False):
            _render_skus_risco(health.skus_risco)


def _render_kpi_card(
    titulo: str,
    valor: str,
    subtitulo: str = "",
    variacao: float | None = None,
    variacao_label: str = "",
    cor_positivo: bool = True,
) -> None:
    cor_valor = "#22c55e" if cor_positivo else "#ef4444"
    variacao_html = ""
    if variacao is not None:
        cor_var = "#22c55e" if variacao >= 0 else "#ef4444"
        seta = "^" if variacao >= 0 else "v"
        variacao_html = f"""
        <div style="margin-top: 4px;">
            <span style="color: {cor_var}; font-size: 0.85rem; font-weight: 600;">
                {seta} {abs(variacao):.1f} pp
            </span>
            <span style="color: #64748b; font-size: 0.75rem;"> {html.escape(variacao_label)}</span>
        </div>
        """
    st.markdown(
        f"""
<div style="
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 16px;
    text-align: center;
">
    <p style="margin: 0; color: #64748b; font-size: 0.8rem; text-transform: uppercase;">
        {html.escape(titulo)}
    </p>
    <p style="margin: 8px 0 4px 0; color: {cor_valor}; font-size: 1.5rem; font-weight: 700;">
        {html.escape(valor)}
    </p>
    <p style="margin: 0; color: #94a3b8; font-size: 0.75rem;">
        {html.escape(subtitulo)}
    </p>
    {variacao_html}
</div>
""",
        unsafe_allow_html=True,
    )


def _render_diagnostico(diag: Diagnostico) -> None:
    key, cor_borda, sym = alert_level_meta(diag.nivel)
    cores_bg = {
        "info": ("#eff6ff", "#1e40af"),
        "medio": ("#fefce8", "#a16207"),
        "alto": ("#fff7ed", "#c2410c"),
        "critico": ("#fef2f2", "#b91c1c"),
    }
    cor_bg, cor_texto = cores_bg.get(key, ("#eff6ff", "#1e40af"))
    acao_html = ""
    if diag.acao:
        acao_html = f"""
        <div style="margin-top: 8px; padding: 8px 12px; background: #f1f5f9; border-radius: 4px; font-size: 0.85rem; color: #475569;">
            <strong>Acao sugerida:</strong> {html.escape(diag.acao)}
        </div>
        """
    st.markdown(
        f"""
<div style="background: {cor_bg}; border-left: 4px solid {cor_borda}; border-radius: 0 8px 8px 0; padding: 16px; margin-bottom: 12px;">
    <div style="display: flex; align-items: center; gap: 8px;">
        <span style="font-weight: 700; color: {cor_texto};">{html.escape(sym)}</span>
        <strong style="color: {cor_texto};">{html.escape(diag.titulo)}</strong>
    </div>
    <p style="margin: 8px 0 0 24px; color: #475569; font-size: 0.9rem;">{html.escape(diag.detalhe)}</p>
    {acao_html}
</div>
""",
        unsafe_allow_html=True,
    )


def _render_skus_risco(skus: list[SKURisco]) -> None:
    import pandas as pd

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
    st.markdown(
        f"""
<div style="display:flex;align-items:center;gap:12px;padding:12px;background:#f8fafc;border-radius:8px;border-left:4px solid {color};">
  <div style="min-width:48px;height:48px;border-radius:50%;background:{color}22;display:flex;align-items:center;justify-content:center;">
    <span style="font-size:1.1rem;font-weight:700;color:{color};">{int(health.score)}</span>
  </div>
  <div>
    <p style="margin:0;font-weight:600;color:#1e293b;">{html.escape(mark)} {html.escape(lbl)}</p>
    <p style="margin:2px 0 0 0;font-size:0.8rem;color:#64748b;">Margem: {health.margem_pct:.1f}%</p>
  </div>
</div>
""",
        unsafe_allow_html=True,
    )
