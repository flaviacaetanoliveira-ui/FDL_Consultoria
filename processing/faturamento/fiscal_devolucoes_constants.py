"""Constantes para identificação de NF de devolução (entrada Bling) na base fiscal."""

from __future__ import annotations

# Filtros exatos validados nos exports CSV (Natureza / Situação).
NATUREZAS_DEVOLUCAO = ("Entrada de Devolução",)
SITUACOES_DEVOLUCAO_VALIDAS = ("Autorizada",)

TIPO_ABATIMENTO_DEVOLUCAO_VENDA = "devolucao_venda"
COL_TIPO_ABATIMENTO = "_tipo_abatimento"
