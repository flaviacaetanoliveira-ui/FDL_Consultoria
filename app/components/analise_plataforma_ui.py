"""Tabela «Análise por plataforma» — Resultado Gerencial."""

from __future__ import annotations

import html
from functools import partial

import pandas as pd
import streamlit as st

from processing.faturamento.analise_plataforma import (
    AnalisePlataforma,
    classifica_nivel_plataforma,
)
from processing.faturamento.formatacao_display_rg import fmt_brl_ptbr_celula, fmt_pct_um_decimal

from app.components.faturamento_dre_ui import fat_dre_premium_css

DEFAULT_BENCHMARK_MARGEM_LIQ_PCT = 10.0

_COLOR_MLIQ_NEG = "#A32D2D"
_COLOR_MLIQ_POS = "#0F6E56"
_COLOR_MLIQ_NEUT = "#64748B"


def _mliq_row_style(row: pd.Series, *, benchmark: float) -> pd.Series:
    """Cor na margem líquida; «Não identificado» permanece cinza (exceção cadastral)."""
    out = pd.Series("", index=row.index)
    if str(row.get("Plataforma", "")).strip() == "Não identificado":
        out["Margem líq."] = f"color: {_COLOR_MLIQ_NEUT}"
        return out
    v = row["Margem líq."]
    if pd.isna(v):
        return out
    fv = float(v)
    if fv < 0:
        out["Margem líq."] = f"color: {_COLOR_MLIQ_NEG}; font-weight: 600"
    elif fv >= benchmark:
        out["Margem líq."] = f"color: {_COLOR_MLIQ_POS}; font-weight: 600"
    else:
        out["Margem líq."] = f"color: {_COLOR_MLIQ_NEUT}"
    return out


def _build_df(analise: AnalisePlataforma, benchmark: float) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ln in analise.linhas:
        tier = classifica_nivel_plataforma(ln.plataforma, ln.margem_liquida_pct, benchmark)
        rows.append(
            {
                "Plataforma": ln.plataforma,
                "Pedidos": int(ln.pedidos),
                "Receita": round(float(ln.receita), 2),
                "Margem op.": round(float(ln.margem_operacional_pct), 6),
                "Margem líq.": round(float(ln.margem_liquida_pct), 6),
                "participacao_pct": round(float(ln.pct_da_receita) * 100.0, 6),
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

    fn_row = partial(_mliq_row_style, benchmark=benchmark_margem_liq_pct)
    sty = df.style.apply(fn_row, axis=1).format(
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
        column_config={
            "participacao_pct": st.column_config.ProgressColumn(
                "Participação",
                min_value=0,
                max_value=100,
                format="%d%%",
            ),
            "Nível": st.column_config.TextColumn(
                "Nível",
                help="Alto / Neutro / Risco pela margem vs benchmark; «—» = exceção (cadastro incompleto).",
            ),
        },
    )
