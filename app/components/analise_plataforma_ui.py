"""Tabela «Análise por plataforma» — Resultado Gerencial."""

from __future__ import annotations

import html
from functools import partial

import pandas as pd
import streamlit as st

from processing.faturamento.analise_plataforma import AnalisePlataforma
from processing.faturamento.formatacao_display_rg import fmt_brl_ptbr_celula, fmt_pct_um_decimal

from app.components.faturamento_dre_ui import fat_dre_premium_css

DEFAULT_BENCHMARK_MARGEM_LIQ_PCT = 10.0

_COLOR_MLIQ_NEG = "#A32D2D"
_COLOR_MLIQ_POS = "#0F6E56"
_COLOR_MLIQ_NEUT = "#64748B"


def _tier_label(m_liq_pct: float, benchmark: float) -> str:
    if m_liq_pct < 0:
        return "Risco"
    if m_liq_pct >= benchmark:
        return "Alto"
    return "Neutro"


def _pct_bar_text(pct_0_1: float, *, width: int = 10) -> str:
    p = max(0.0, min(1.0, float(pct_0_1)))
    filled = int(round(p * width))
    bar = "█" * filled + "░" * (width - filled)
    return f"{bar} {p * 100:.0f}%"


def _mliq_css_col(s: pd.Series, *, benchmark: float) -> pd.Series:
    out: list[str] = []
    for v in s:
        if pd.isna(v):
            out.append("")
            continue
        fv = float(v)
        if fv < 0:
            out.append(f"color: {_COLOR_MLIQ_NEG}; font-weight: 600")
        elif fv >= benchmark:
            out.append(f"color: {_COLOR_MLIQ_POS}; font-weight: 600")
        else:
            out.append(f"color: {_COLOR_MLIQ_NEUT}")
    return pd.Series(out, index=s.index)


def _build_df(analise: AnalisePlataforma, benchmark: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ln in analise.linhas:
        tier = _tier_label(ln.margem_liquida_pct, benchmark)
        rows.append(
            {
                "Plataforma": ln.plataforma,
                "Pedidos": int(ln.pedidos),
                "Receita": round(float(ln.receita), 2),
                "Margem op.": round(float(ln.margem_operacional_pct), 6),
                "Margem líq.": round(float(ln.margem_liquida_pct), 6),
                "Participação": _pct_bar_text(ln.pct_da_receita),
                "Nível": tier,
            }
        )
    df = pd.DataFrame(rows)
    df = df.dropna(how="all")
    if not df.empty:
        df = df.loc[~df.isna().all(axis=1)].copy()
    return df


def render_analise_plataforma(
    analise: AnalisePlataforma,
    *,
    benchmark_margem_liq_pct: float = DEFAULT_BENCHMARK_MARGEM_LIQ_PCT,
    debug_enabled: bool = False,
) -> None:
    """Renderiza bloco HTML + ``pandas.Styler`` em ``st.dataframe`` (pt-BR + cor na margem líquida)."""
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
        f'<span style="color:{_COLOR_MLIQ_POS};font-weight:600">●</span> ≥ benchmark · '
        f'<span style="color:{_COLOR_MLIQ_NEUT};font-weight:600">●</span> entre 0% e benchmark · '
        f'<span style="color:{_COLOR_MLIQ_NEG};font-weight:600">●</span> negativa'
        "</p>"
    )
    st.markdown(leg, unsafe_allow_html=True)

    df = _build_df(analise, benchmark_margem_liq_pct)
    if df.empty:
        return

    fn = partial(_mliq_css_col, benchmark=benchmark_margem_liq_pct)
    sty = df.style.apply(fn, axis=0, subset=["Margem líq."]).format(
        {
            "Receita": lambda x: fmt_brl_ptbr_celula(x),
            "Margem op.": lambda x: fmt_pct_um_decimal(float(x)) if pd.notna(x) else "—",
            "Margem líq.": lambda x: fmt_pct_um_decimal(float(x)) if pd.notna(x) else "—",
        },
        na_rep="—",
    )

    st.dataframe(
        sty,
        hide_index=True,
        width="stretch",
        height=min(420, 56 + len(df) * 36),
    )
