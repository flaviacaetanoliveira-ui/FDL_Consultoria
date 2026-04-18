"""Placeholder do módulo Apuração Fiscal — Etapa 1 (estrutura vazia)."""

from __future__ import annotations

import html
from textwrap import dedent

import streamlit as st


def _build_apuracao_fiscal_page_header_html(*, updated_at: str) -> str:
    """Cabeçalho alinhado ao módulo Faturamento & DRE (classes ``fdl-page-*``)."""
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


def render_apuracao_fiscal_placeholder(*, updated_at_display: str) -> None:
    """Renderiza o esqueleto da página (sem dados nem componentes de negócio)."""
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
    st.html(_build_apuracao_fiscal_page_header_html(updated_at=updated_at_display))
    with st.expander("ℹ️ Sobre este módulo", expanded=False):
        st.caption(
            "Área dedicada à apuração fiscal (notas, bases e impostos). Nesta fase o conteúdo ainda "
            "será organizado; a navegação e o cabeçalho seguem o padrão FDL Analytics."
        )
    st.markdown('<div style="margin-top:0.75rem"></div>', unsafe_allow_html=True)
    st.caption(
        "Módulo em construção — componentes serão migrados do atual Faturamento & DRE nas próximas etapas."
    )
