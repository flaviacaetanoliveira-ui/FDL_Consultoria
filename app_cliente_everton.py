"""Entrypoint Streamlit — Everton (Antomóveis).

Os artefatos materializados deste cliente ficam em ``data_products/default/antomoveis/`` (repasse/frete).
O slug ``cliente_2`` no repositório é do Grupo Mega Fácil (gama_home, mega_facil, …) e **não** contém
``antomoveis``; usar esse slug quebra repasse/frete para Antomóveis.

**Streamlit Cloud:** Main file = ``app_cliente_everton.py`` e os mesmos secrets de materializado
(``FDL_MATERIALIZED_PATH_MODE=dynamic``, ``FDL_STRICT_MATERIALIZED``, etc.) que o deploy do Antomóveis.

**Faturamento & DRE** e **Comercial & pedidos** exigem ``faturamento`` em ``FDL_ENABLED_FINANCE_MODULES``
e ficheiros em ``data_products/default/antomoveis/faturamento/current/`` (gerar com
``processing/materialize_financeiro.py`` para ``--cliente default --empresa antomoveis --modulo faturamento``).
"""

from __future__ import annotations

import os


def _configure_everton_anto_moveis() -> None:
    os.environ["FDL_MATERIALIZED_CLIENTE_SLUG"] = "default"
    # Antes do Streamlit carregar secrets, só existe os.environ — forçar aqui garante
    # Faturamento & DRE + Comercial & pedidos mesmo se secrets tiver só repasse,frete.
    os.environ["FDL_ENABLED_FINANCE_MODULES"] = "repasse,frete,faturamento"


_configure_everton_anto_moveis()

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente_everton.py",
    page_title="FDL Analytics — Antomóveis",
)
