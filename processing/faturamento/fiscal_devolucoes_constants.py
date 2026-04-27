"""
Regras fiscais de inclusão/exclusão de notas para apuração.

PRINCÍPIO: estas constantes são a fonte única da verdade sobre quais
notas de entrada entram na base fiscal. Toda mudança aqui afeta
Apuração Fiscal e a ponte do imposto na DRE Gerencial.
"""

from __future__ import annotations

NATUREZAS_DEVOLUCAO = (
    "Entrada de Devolução",
    "Entrada Devolução",
)

# Situações fiscalmente válidas — NFs nestes status entram na base.
# Regra de negócio: 'Autorizada' e 'Emitida DANFE' são fiscalmente
# computáveis. Status como 'Cancelada', 'Denegada', 'Inutilizada' e
# 'Em digitação' são excluídos.
SITUACOES_DEVOLUCAO_VALIDAS = (
    "Autorizada",
    "Emitida DANFE",
)

TIPO_ABATIMENTO_DEVOLUCAO_VENDA = "devolucao_venda"
COL_TIPO_ABATIMENTO = "_tipo_abatimento"
