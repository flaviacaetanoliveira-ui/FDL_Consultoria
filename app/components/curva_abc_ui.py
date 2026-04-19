"""Curva ABC de produtos — Resultado Gerencial."""

from __future__ import annotations

import html
from functools import partial

import pandas as pd
import streamlit as st

from processing.faturamento.curva_abc import CurvaAbc
from processing.faturamento.formatacao_display_rg import fmt_brl_ptbr_celula, fmt_pct_um_decimal

from app.components.faturamento_dre_ui import fat_dre_premium_css

DEFAULT_BENCHMARK_MARGEM_LIQ_PCT = 10.0

_COLOR_MLIQ_NEG = "#A32D2D"
_COLOR_MLIQ_POS = "#0F6E56"
_COLOR_MLIQ_NEUT = "#64748B"

_ABC_ICON = {"A": "🟢", "B": "🟡", "C": "🔴"}


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


def _abc_col_display(classes: pd.Series) -> pd.Series:
    out: list[str] = []
    for c in classes:
        cs = str(c).strip().upper()
        icon = _ABC_ICON.get(cs, "")
        out.append(f"{icon} {cs}".strip())
    return pd.Series(out, index=classes.index)


def _build_df(curva: CurvaAbc, *, incluir_desc: bool) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for ln in curva.linhas:
        row: dict[str, object] = {
            "_cls": ln.classe_abc,
            "SKU": ln.sku,
            "Pedidos": int(ln.pedidos),
            "Receita": round(float(ln.receita), 2),
            "Margem líq.": round(float(ln.margem_liquida_pct), 6),
            "% acum": round(float(ln.pct_acumulado), 6),
            "Participação": _pct_bar_text(ln.pct_da_receita),
        }
        if incluir_desc:
            row["Descrição"] = ln.descricao if ln.descricao else "—"
        rows.append(row)
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ABC"] = _abc_col_display(df["_cls"])
    return df.drop(columns=["_cls"])


def render_curva_abc(
    curva: CurvaAbc,
    *,
    benchmark_margem_liq_pct: float = DEFAULT_BENCHMARK_MARGEM_LIQ_PCT,
    debug_enabled: bool = False,
    checkbox_key: str = "curva_abc_ver_todos",
) -> None:
    """Top 20 SKUs por padrão; checkbox expande a lista completa."""
    n = len(curva.linhas)
    if n == 0:
        return
    if n < 2:
        if debug_enabled:
            st.caption(
                "🔍 curva ABC: seção omitida — apenas 1 SKU no recorte (curva ABC requer pelo menos 2 itens)."
            )
        return

    tem_desc = any(ln.descricao for ln in curva.linhas)
    st.markdown(fat_dre_premium_css(), unsafe_allow_html=True)
    head = (
        '<div class="fdl-fat-premium">'
        '<p class="fdl-rg-block-head-label">CURVA ABC DE PRODUTOS</p>'
        f'<p class="fdl-rg-block-head-sub">{html.escape("Contribuição por SKU e classificação ABC")}</p>'
        "</div>"
    )
    st.markdown(head, unsafe_allow_html=True)

    leg = (
        "<p style='font-size:0.8rem;color:#64748b;margin:0 0 8px 0'>"
        "Legenda: 🟢 Classe A (até 70% acum.) · 🟡 Classe B (70–90%) · 🔴 Classe C (acima de 90%)"
        "</p>"
    )
    st.markdown(leg, unsafe_allow_html=True)

    badges = (
        "<p style='font-size:0.85rem;margin:0 0 12px 0;line-height:1.5'>"
        f"🟢 Classe A: <strong>{curva.qtd_classe_a}</strong> SKUs · "
        f"{curva.pct_receita_classe_a * 100:.0f}% da receita<br/>"
        f"🟡 Classe B: <strong>{curva.qtd_classe_b}</strong> SKUs · "
        f"{curva.pct_receita_classe_b * 100:.0f}% da receita<br/>"
        f"🔴 Classe C: <strong>{curva.qtd_classe_c}</strong> SKUs · "
        f"{curva.pct_receita_classe_c * 100:.0f}% da receita"
        "</p>"
    )
    st.markdown(badges, unsafe_allow_html=True)

    ml_leg = (
        "<p style='font-size:0.8rem;color:#64748b;margin:0 0 8px 0'>"
        f"Margem líquida (benchmark {benchmark_margem_liq_pct:.0f}%): "
        f'<span style="color:{_COLOR_MLIQ_POS};font-weight:600">●</span> ≥ benchmark · '
        f'<span style="color:{_COLOR_MLIQ_NEUT};font-weight:600">●</span> entre 0% e benchmark · '
        f'<span style="color:{_COLOR_MLIQ_NEG};font-weight:600">●</span> negativa'
        "</p>"
    )
    st.markdown(ml_leg, unsafe_allow_html=True)

    mostrar_todos = st.checkbox(
        f"Ver todos os {curva.total_skus} SKUs",
        value=False,
        key=checkbox_key,
    )
    linhas_exibir = curva.linhas if mostrar_todos else curva.linhas[:20]

    sub = CurvaAbc(
        linhas=tuple(linhas_exibir),
        receita_total=curva.receita_total,
        total_skus=len(linhas_exibir),
        qtd_classe_a=curva.qtd_classe_a,
        qtd_classe_b=curva.qtd_classe_b,
        qtd_classe_c=curva.qtd_classe_c,
        pct_receita_classe_a=curva.pct_receita_classe_a,
        pct_receita_classe_b=curva.pct_receita_classe_b,
        pct_receita_classe_c=curva.pct_receita_classe_c,
    )

    df = _build_df(sub, incluir_desc=tem_desc)
    if df.empty:
        return

    fn = partial(_mliq_css_col, benchmark=benchmark_margem_liq_pct)
    subset_cols = ["Margem líq."]
    sty = df.style.apply(fn, axis=0, subset=subset_cols).format(
        {
            "Receita": lambda x: fmt_brl_ptbr_celula(x),
            "Margem líq.": lambda x: fmt_pct_um_decimal(float(x)) if pd.notna(x) else "—",
            "% acum": lambda x: fmt_pct_um_decimal(float(x) * 100.0) if pd.notna(x) else "—",
        },
        na_rep="—",
    )

    st.dataframe(
        sty,
        hide_index=True,
        width="stretch",
        height=min(520, 56 + len(df) * 36),
    )

    if not mostrar_todos and curva.total_skus > 20:
        st.caption(f"Mostrando top 20 de {curva.total_skus} SKUs.")
