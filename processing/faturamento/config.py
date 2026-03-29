"""
Constantes do pipeline de faturamento (Fase 1).
"""
from __future__ import annotations

CUSTO_SHEET_NAME = "Planilha1"
CUSTO_COL_PRECO = "PREÇO DE CUSTO com IPI"
CUSTO_SKU_COL = "Código"

REQUIRED_PEDIDO_COLUMNS: tuple[str, ...] = (
    "Preço de lista",
    "Valor total",
    "Custo de Frete",
    "Taxa de Comissão",
    "Situação",
    "Existe Nota Fiscal gerada",
    "Número da nota",
    "Código",
    "Nome da plataforma",
    "Número do pedido",
    "Número do pedido multiloja",
)

OPTIONAL_DATA_COL = "Data"

DIVERGENCIA_VALOR_TOL = 0.01

PIPELINE_REVISION_FATURAMENTO = "faturamento-v1"

# Lock órfão: remover se mais velho que isto (segundos)
LOCK_STALE_MAX_AGE_SEC = 7200
