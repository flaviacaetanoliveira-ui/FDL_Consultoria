"""
Identificação da empresa nas linhas do dataset operacional.

Para ambientes com um único BASE_DIR (mono-empresa), todas as linhas recebem este rótulo.
Deve coincidir com nomes em `USUARIOS[].empresas` e com o registro em `operacional_app_context`.

Materialização de outra empresa (ex.: Gama Home): defina `FDL_DATASET_EMPRESA` no ambiente
antes de correr o pipeline / `materialize_financeiro.py` (default continua Antomóveis).
"""
from __future__ import annotations

import os

_DATASET_RAW = os.environ.get("FDL_DATASET_EMPRESA", "").strip()
DATASET_EMPRESA = _DATASET_RAW if _DATASET_RAW else "Antomóveis"
