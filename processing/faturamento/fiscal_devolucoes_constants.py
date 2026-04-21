"""Constantes para identificação de NF de devolução (entrada Bling) na base fiscal."""

from __future__ import annotations

# Variações aceitas da natureza "Entrada de Devolução".
# Bling permite cadastro com ou sem "de" dependendo da empresa.
# Situação "Autorizada" continua sendo o filtro de validade fiscal.
NATUREZAS_DEVOLUCAO = (
    "Entrada de Devolução",  # grafia padrão (Gama Home, Mega Star, Móveis EAP)
    "Entrada Devolução",  # grafia da Mega Fácil
)
SITUACOES_DEVOLUCAO_VALIDAS = ("Autorizada",)

TIPO_ABATIMENTO_DEVOLUCAO_VENDA = "devolucao_venda"
COL_TIPO_ABATIMENTO = "_tipo_abatimento"
