"""Página «Apuração Fiscal» — visão fiscal (notas, base e imposto)."""

from __future__ import annotations

import html
from textwrap import dedent

import streamlit as st


def _build_apuracao_fiscal_page_header_html(*, updated_at: str) -> str:
    """Cabeçalho da página (classes ``fdl-page-*``, mesmo sistema visual que Resultado Gerencial)."""
    esc = html.escape(updated_at)
    return (
        '<div class="fdl-page-header">'
        '<div class="fdl-page-header-main">'
        '<p class="fdl-apuracao-breadcrumb">Fiscal &gt; Apuração Fiscal</p>'
        '<h1 class="fdl-page-title">🧾 Apuração Fiscal</h1>'
        '<p class="fdl-page-subtitle">Notas, base tributável e imposto</p>'
        "</div>"
        '<div class="fdl-page-header-meta">'
        f'<span class="fdl-page-updated">{esc}</span>'
        "</div>"
        "</div>"
    )


def render_apuracao_fiscal_page(
    df,
    load_info: dict[str, object],
    ts_proc: str,
    *,
    org_id: str,
    org_display_name: str,
) -> None:
    """Cabeçalho + painel fiscal (import tardio de ``app_operacional`` evita ciclo)."""
    _ = org_display_name
    import app_operacional as ao

    ao._fdl_fat_min_inject_ui_styles()
    _upd_disp = ao._fdl_fat_min_format_updated_at(str(ts_proc))
    st.markdown(
        dedent(
            """
            <style>
            .fdl-apuracao-breadcrumb {
              font-size: 0.75rem;
              font-weight: 600;
              letter-spacing: 0.06em;
              text-transform: uppercase;
              color: var(--fdl-neutral-400, #94a3b8);
              margin: 0 0 0.35rem 0;
            }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )
    st.html(_build_apuracao_fiscal_page_header_html(updated_at=_upd_disp))
    with st.expander("ℹ️ Sobre este módulo", expanded=False):
        st.caption(
            "Este módulo apresenta a apuração fiscal consolidada. Exibe notas emitidas, canceladas e devoluções "
            "no período, calcula a **base fiscal líquida** (emitidas − canceladas − devoluções) e o **imposto** aplicável. "
            "A base líquida calculada aqui é consumida pela DRE do **Resultado Gerencial**."
        )
    ao._fdl_fat_min_vsp(size="sm")

    from app.components.apuracao_fiscal_panel import render_apuracao_fiscal_panel

    render_apuracao_fiscal_panel(df, load_info, ts_proc, org_id=org_id)
