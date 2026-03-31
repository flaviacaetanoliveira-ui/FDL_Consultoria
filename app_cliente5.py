"""Entrypoint Streamlit Cloud — Cliente 5 (Flávio: Esquilo + Wood), igual ao fluxo do `app_cliente2.py`.

**Secrets (espelho do cliente 2):** o mesmo `app_operacional` e a mesma regra de troca de empresa na sidebar
(`org_id` → `data_products/<slug>/<org_id>/…`). Neste deploy defina **`FDL_MATERIALIZED_CLIENTE_SLUG=cliente_5`**
(junto com `FDL_MATERIALIZED_PATH_MODE=dynamic` e `FDL_DATA_PRODUCTS_ROOT`, como no Mega Fácil).

- `app_cliente2.py` → `cliente_2` (Gama, Mega Fácil, …)
- `app_cliente5.py` → `cliente_5` (Esquilo, Wood)
"""

from __future__ import annotations

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente5.py",
    page_title="FDL Analytics — Cliente Flávio",
)
