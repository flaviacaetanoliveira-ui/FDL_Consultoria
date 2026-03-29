"""Entrypoint Streamlit Cloud — Cliente 5 (Flávio), mesmo repositório que `app.py` / `app_cliente2.py`.

Define um Main file path distinto no Streamlit Cloud para um deploy dedicado (secrets dynamic,
ex.: FDL_MATERIALIZED_CLIENTE_SLUG=cliente_5). A lógica continua só em `app_operacional`
(via `fdl_streamlit_bootstrap`).
"""

from __future__ import annotations

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente5.py",
    page_title="FDL Analytics — Cliente Flávio",
)
