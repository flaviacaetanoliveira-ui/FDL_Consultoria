"""Entrypoint Streamlit Cloud — Grupo Mega Fácil (Gama Home, Mega Star, Móveis EAP, Mega Fácil).

Define um Main file path distinto no Streamlit Cloud para o segundo deploy não colidir com o app do cliente 1.

**Materializado:** Repasse e Frete usam por defeito ``data_products/cliente_2/<org_id>/{repasse,frete}/current/``
(``FDL_MATERIALIZED_PATH_MODE=dynamic`` + consume modes), **sem** correr o pipeline ao vivo em
``BASE_DIR``. Faturamento já é só materializado no ``app_operacional``.

``setdefault`` não sobrescreve env/secrets já definidos no host (Streamlit Cloud).
"""

from __future__ import annotations

import os


def _configure_grupo_mega_facil() -> None:
    os.environ.setdefault("FDL_MATERIALIZED_CLIENTE_SLUG", "cliente_2")
    os.environ.setdefault("FDL_MATERIALIZED_PATH_MODE", "dynamic")
    os.environ.setdefault("FDL_REPASSE_CONSUME_MODE", "materialized")
    os.environ.setdefault("FDL_FRETE_CONSUME_MODE", "materialized")
    os.environ.setdefault("FDL_DEVOLUCOES_CONSUME_MODE", "materialized")
    os.environ.setdefault("FDL_ENABLED_FINANCE_MODULES", "repasse,frete,devolucoes,faturamento")


_configure_grupo_mega_facil()

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente2.py",
    page_title="FDL Analytics — Financeiro (Grupo Mega Fácil)",
)
