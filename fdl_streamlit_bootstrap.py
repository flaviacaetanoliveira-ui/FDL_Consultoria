"""
Bootstrap partilhado entre entrypoints Streamlit (`app.py`, `app_cliente2.py`, …).

Em cada rerun o Streamlit reexecuta o ficheiro principal, mas um segundo `import app_operacional`
não volta a correr o corpo do módulo (cache de import do Python). Por isso usamos
`importlib.reload` para forçar a reexecução do painel em todo o rerun.
"""
from __future__ import annotations

import importlib
import os
import sys

import streamlit as st


def _bootstrap_debug_app() -> bool:
    raw = os.environ.get("FDL_DEBUG_BOOTSTRAP", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_DEBUG_BOOTSTRAP", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def run_operacional_app(
    *,
    entrypoint_label: str,
    page_title: str = "FDL Analytics — Financeiro",
) -> None:
    """
    `entrypoint_label` identifica o ficheiro de entrada (ex.: ``app.py``) para mensagens de debug.
    """
    st.set_page_config(page_title=page_title, layout="wide")

    try:
        if "app_operacional" in sys.modules:
            importlib.reload(sys.modules["app_operacional"])
        else:
            import app_operacional  # noqa: F401
    except Exception as exc:
        st.error("Erro global no rerun (import/reload de app_operacional).")
        st.exception(exc)
        st.caption(f"Última etapa: {st.session_state.get('_fdl_bootstrap_stage', '—')}")
        st.stop()

    if _bootstrap_debug_app():
        st.session_state["_fdl_bootstrap_stage"] = f"{entrypoint_label}: após reload(app_operacional)"
        with st.expander(f"FDL_DEBUG_BOOTSTRAP — saída ({entrypoint_label})", expanded=False):
            st.write("**Última etapa:**", st.session_state.get("_fdl_bootstrap_stage", "—"))
            _lg = st.session_state.get("_fdl_bootstrap_log")
            if isinstance(_lg, list) and _lg:
                st.write("**Log (últimas etapas):**")
                for _i, _line in enumerate(_lg[-20:], 1):
                    st.text(f"{_i}. {_line}")
