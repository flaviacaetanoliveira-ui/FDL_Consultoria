"""Entrypoint para Streamlit Community Cloud e local."""

import streamlit as st

st.set_page_config(page_title="FDL Analytics — Financeiro", layout="wide")

try:
    import app_operacional  # noqa: F401 — executa o painel operacional
except Exception as exc:
    st.error("Falha ao iniciar a aplicação. Se o erro persistir, verifique **Manage app → Logs** na Cloud.")
    st.exception(exc)

