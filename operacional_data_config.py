"""
Identificação da empresa nas linhas do dataset operacional.

Para ambientes com um único BASE_DIR (mono-empresa), todas as linhas recebem este rótulo.
Deve coincidir com nomes em `USUARIOS[].empresas` e com o registro em `operacional_app_context`.
"""
from __future__ import annotations

DATASET_EMPRESA = "Antomóveis"
