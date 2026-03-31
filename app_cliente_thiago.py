"""Entrypoint Streamlit Cloud — cliente Thiago (4 empresas), no mesmo padrão de app_cliente2/app_cliente5.

Use este main file com:
- FDL_MATERIALIZED_PATH_MODE=dynamic
- FDL_MATERIALIZED_CLIENTE_SLUG=cliente_thiago
- FDL_ENABLED_FINANCE_MODULES=repasse,frete
- FDL_REPASSE_SEM_BLING=true
"""

from __future__ import annotations

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente_thiago.py",
    page_title="FDL Analytics — Financeiro (Thiago)",
)
