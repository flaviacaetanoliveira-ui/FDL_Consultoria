"""Entrypoint Streamlit Cloud — cliente 2 (ex.: Gama Home), mesmo repositório que `app.py`.

Define um Main file path distinto no Streamlit Cloud para o segundo deploy não colidir com o app do cliente 1.
A lógica do sistema continua só em `app_operacional` (carregada via `fdl_streamlit_bootstrap`).
"""

from __future__ import annotations

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente2.py",
    page_title="FDL Analytics — Financeiro (Grupo Mega Fácil)",
)
