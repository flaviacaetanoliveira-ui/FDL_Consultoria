"""Tabela «Análise por plataforma» — Resultado Gerencial."""

from __future__ import annotations

import html

import pandas as pd
import streamlit as st

from processing.faturamento.analise_plataforma import AnalisePlataforma

from app.components.faturamento_dre_ui import fat_dre_premium_css

DEFAULT_BENCHMARK_MARGEM_LIQ_PCT = 10.0


def _tier_label(m_liq_pct: float, benchmark: float) -> str:
    if m_liq_pct < 0:
        return "Risco"
    if m_liq_pct >= benchmark:
        return "Alto"
    return "Neutro"


def _fmt_brl_compact(v: float) -> str:
    av = abs(v)
    if av >= 100_000:
        return f"R$ {v/1000:.1f}k".replace(".", ",")
    if av >= 1_000:
        return f"R$ {v/1000:.1f}k".replace(".", ",")
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


def _build_df(analise: AnalisePlataforma, benchmark: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ln in analise.linhas:
        tier = _tier_label(ln.margem_liquida_pct, benchmark)
        rows.append(
            {
                "Plataforma": ln.plataforma,
                "Pedidos": int(ln.pedidos),
                "Receita (R$)": round(float(ln.receita), 2),
                "Margem op. (%)": round(float(ln.margem_operacional_pct), 2),
                "Margem líq. (%)": round(float(ln.margem_liquida_pct), 2),
                "% receita (barra)": round(float(ln.pct_da_receita) * 100.0, 4),
                "Nível": tier,
                "_sort_mliq": float(ln.margem_liquida_pct),
                "_tier_ord": 2 if tier == "Neutro" else (3 if tier == "Alto" else 1),
            }
        )
    return pd.DataFrame(rows)


def render_analise_plataforma(
    analise: AnalisePlataforma,
    *,
    benchmark_margem_liq_pct: float = DEFAULT_BENCHMARK_MARGEM_LIQ_PCT,
    debug_enabled: bool = False,
) -> None:
    """Renderiza bloco HTML + ``st.dataframe`` com ordenação nativa por cabeçalho."""
    n = len(analise.linhas)
    if n == 0:
        return
    if n < 2:
        if debug_enabled:
            st.caption(
                "🔍 analise plat: seção omitida — apenas 1 canal no recorte (sem comparação multi-canal)."
            )
        return

    st.markdown(fat_dre_premium_css(), unsafe_allow_html=True)
    head = (
        '<div class="fdl-fat-premium">'
        '<p class="fdl-rg-block-head-label">Análise por plataforma</p>'
        f'<p class="fdl-rg-block-head-sub">{html.escape("Contribuição de cada canal ao resultado")}</p>'
        "</div>"
    )
    st.markdown(head, unsafe_allow_html=True)

    leg = (
        "<p style='font-size:0.8rem;color:#64748b;margin:0 0 8px 0'>"
        f"Legenda margem líquida (benchmark {benchmark_margem_liq_pct:.0f}%): "
        '<span style="color:#0f6e56;font-weight:600">●</span> ≥ benchmark · '
        '<span style="color:#64748b;font-weight:600">●</span> entre 0% e benchmark · '
        '<span style="color:#a32d2d;font-weight:600">●</span> negativa'
        "</p>"
    )
    st.markdown(leg, unsafe_allow_html=True)

    df = _build_df(analise, benchmark_margem_liq_pct)
    show = df.drop(columns=["_sort_mliq", "_tier_ord"], errors="ignore")

    st.dataframe(
        show,
        column_config={
            "Plataforma": st.column_config.TextColumn("Plataforma", width="medium"),
            "Pedidos": st.column_config.NumberColumn("Pedidos", format="%d"),
            "Receita (R$)": st.column_config.NumberColumn(
                "Receita",
                format="%.2f",
                help="Soma da receita lista no canal",
            ),
            "Margem op. (%)": st.column_config.NumberColumn(
                "Margem op.",
                format="%.1f%%",
            ),
            "Margem líq. (%)": st.column_config.NumberColumn(
                "Margem líq.",
                format="%.1f%%",
            ),
            "% receita (barra)": st.column_config.ProgressColumn(
                "% receita",
                format="%d%%",
                min_value=0,
                max_value=100,
                help="Participação na receita total do recorte",
            ),
            "Nível": st.column_config.TextColumn(
                "Nível",
                width="small",
                help="Alto ≥ benchmark · Neutro 0%–benchmark · Risco &lt; 0%",
            ),
        },
        hide_index=True,
        width="stretch",
        height=min(420, 56 + n * 36),
    )
