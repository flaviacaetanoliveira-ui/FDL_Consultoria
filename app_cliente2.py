"""Entrypoint Streamlit Cloud — Grupo Mega Fácil (Gama Home, Mega Star, Móveis EAP, Mega Fácil).

Define um Main file path distinto no Streamlit Cloud para o segundo deploy não colidir com o app do cliente 1.
**Comercial & pedidos** e **Faturamento & DRE** leem só dados em ``data_products/cliente_2/...``
(materialização); ``setdefault`` abaixo não sobrescreve secrets/env já definidos.
"""

from __future__ import annotations

import os


def _configure_grupo_mega_facil() -> None:
    os.environ.setdefault("FDL_MATERIALIZED_CLIENTE_SLUG", "cliente_2")
    os.environ.setdefault("FDL_ENABLED_FINANCE_MODULES", "repasse,frete,faturamento")


_configure_grupo_mega_facil()

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente2.py",
    page_title="FDL Analytics — Financeiro (Grupo Mega Fácil)",
)
