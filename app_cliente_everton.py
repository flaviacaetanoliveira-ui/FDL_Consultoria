"""Entrypoint Streamlit — Everton (Antomóveis).

Os artefatos materializados deste cliente ficam em ``data_products/default/antomoveis/`` (repasse/frete).
O slug ``cliente_2`` no repositório é do Grupo Mega Fácil (gama_home, mega_facil, …) e **não** contém
``antomoveis``; usar esse slug quebra repasse/frete para Antomóveis.

**Streamlit Cloud:** Main file = ``app_cliente_everton.py``. O entrypoint define ``FDL_MATERIALIZED_PATH_MODE=dynamic``,
``FDL_REPASSE_CONSUME_MODE=materialized`` e ``FDL_FRETE_CONSUME_MODE=materialized`` (base final em
``data_products/default/<org_id>/``). Secrets podem acrescentar ``FDL_STRICT_MATERIALIZED``, etc.

**Faturamento & DRE** e **Comercial & pedidos** exigem ``faturamento`` em ``FDL_ENABLED_FINANCE_MODULES``
e ficheiros em ``data_products/default/antomoveis/faturamento/current/`` (gerar com
``processing/materialize_financeiro.py`` para ``--cliente default --empresa antomoveis --modulo faturamento``).
"""

from __future__ import annotations

import os


def _configure_everton_anto_moveis() -> None:
    os.environ["FDL_MATERIALIZED_CLIENTE_SLUG"] = "default"
    # Base final em data_products/default/<org_id>/… — dynamic evita filtrar pela coluna «empresa»
    # do CSV (muitas materializações trazem FDL_DATASET_EMPRESA global errado e esvaziavam o repasse).
    os.environ["FDL_MATERIALIZED_PATH_MODE"] = "dynamic"
    os.environ["FDL_REPASSE_CONSUME_MODE"] = "materialized"
    os.environ["FDL_FRETE_CONSUME_MODE"] = "materialized"
    # Antes do Streamlit carregar secrets, só existe os.environ — forçar aqui garante
    # Faturamento & DRE + Comercial & pedidos mesmo se secrets tiver só repasse,frete.
    os.environ["FDL_ENABLED_FINANCE_MODULES"] = "repasse,frete,faturamento"


_configure_everton_anto_moveis()

from fdl_streamlit_bootstrap import run_operacional_app

run_operacional_app(
    entrypoint_label="app_cliente_everton.py",
    page_title="FDL Analytics — Antomóveis",
)
