"""
Constantes do pipeline de faturamento (Fase 1).
"""
from __future__ import annotations

CUSTO_SHEET_NAME = "Planilha1"
CUSTO_COL_PRECO = "PREÇO DE CUSTO com IPI"
CUSTO_SKU_COL = "Código"

# Preço unitário de custo vindo do XLSX; custo total na linha = Quantidade × Custo_Unitario
CUSTO_UNITARIO_COL = "Custo_Unitario"

# Chave usada no join pedidos↔custo (normalização); o «Código» original mantém-se em ``Código``.
SKU_NORMALIZADO_COL = "SKU_Normalizado"

REQUIRED_PEDIDO_COLUMNS: tuple[str, ...] = (
    "Quantidade",
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
OUTRAS_DESPESAS_COL = "Outras Despesas"

STATUS_CUSTO_OK = "CUSTO_OK"
STATUS_SKU_SEM_CORRESPONDENCIA = "SKU_SEM_CORRESPONDENCIA"
STATUS_SKU_DUPLICADO_CUSTO = "SKU_DUPLICADO_NA_TABELA_CUSTO"

DIVERGENCIA_VALOR_TOL = 0.01

PIPELINE_REVISION_FATURAMENTO = "faturamento-v2"

# Lock órfão: remover se mais velho que isto (segundos)
LOCK_STALE_MAX_AGE_SEC = 7200
