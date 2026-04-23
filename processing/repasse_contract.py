"""
Contrato compartilhado do dataset materializado de **Conciliação de Repasse**.

Este módulo existe para alinhar vocabulário e expectativas entre:
pipeline (etapa4b), materializador (`processing/materialize_financeiro.py`) e app
(`app_operacional.py`), **sem** implementar lógica de negócio aqui.

Antecipa a migração controlada para o artefato canônico **Parquet**; o consumo no
app continua definido noutros módulos até PRs posteriores.

PR1: apenas constantes e documentação — nenhum efeito no runtime do Streamlit.
"""

from __future__ import annotations

# -----------------------------------------------------------------------------
# Artefato principal (futuro consumo canónico pelo app)
# -----------------------------------------------------------------------------

REPASSE_ARTIFACT_FILENAME: str = "dataset.parquet"
"""Nome do ficheiro Parquet sob `.../repasse/current/`."""

# Raiz convencional no repositório; o path absoluto depende de FDL_DATA_PRODUCTS_ROOT.
REPASSE_DATA_PRODUCTS_ROOT_DEFAULT: str = "data_products"

# Path relativo à raiz de produtos de dados: <cliente_slug>/<org_id>/repasse/current/dataset.parquet
# (prefixo `data_products/` é convenção do repo, não parte deste fragmento).


def repasse_parquet_relative_path(cliente_slug: str, org_id: str) -> str:
    """
    Caminho POSIX relativo à raiz de *data products* (ex.: pasta `data_products` no repo).

    Ex.: ``cliente_2/gama_home/repasse/current/dataset.parquet``
    """
    c = str(cliente_slug).strip().strip("/\\")
    o = str(org_id).strip().strip("/\\")
    return f"{c}/{o}/repasse/current/{REPASSE_ARTIFACT_FILENAME}"


def repasse_parquet_path_under_data_products(cliente_slug: str, org_id: str) -> str:
    """
    Caminho completo convencional incluindo o segmento ``data_products/``.

    Ex.: ``data_products/cliente_2/gama_home/repasse/current/dataset.parquet``
    """
    root = REPASSE_DATA_PRODUCTS_ROOT_DEFAULT.strip().strip("/\\")
    rel = repasse_parquet_relative_path(cliente_slug, org_id)
    return f"{root}/{rel}"


# -----------------------------------------------------------------------------
# Coluna canónica de ação (única para filtro, KPI e exibição)
# -----------------------------------------------------------------------------

REPASSE_ACTION_COLUMN: str = "Ação sugerida"
"""Valor final já decidido no materializador; não usar coluna paralela na UI."""


# -----------------------------------------------------------------------------
# Colunas obrigatórias (mínimo de negócio no dataset consumido)
# -----------------------------------------------------------------------------

REPASSE_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
        "N° de venda",
        "ID do pedido",
        "Total BRL",
        "Número da nota",
        "Valor da nota",
        "Plataforma",
        "Situação",
        REPASSE_ACTION_COLUMN,
        "Valor a receber",
        "Valor pago",
        "Diferença",
        "Data de pagamento",
        "Data de emissão",
        "Data período repasse",
    }
)


# -----------------------------------------------------------------------------
# Identidade (enriquecimento no Parquet pelo materializador)
# -----------------------------------------------------------------------------

REPASSE_IDENTITY_COLUMNS: frozenset[str] = frozenset(
    {
        "cliente_id",
        "empresa_id",
        "cnpj",
        "empresa",
    }
)


# -----------------------------------------------------------------------------
# Filtros de sessão (subset; o app aplica máscaras sobre o dataset final)
# -----------------------------------------------------------------------------

REPASSE_FILTER_COLUMNS: frozenset[str] = frozenset(
    {
        "Data período repasse",
        "Plataforma",
        REPASSE_ACTION_COLUMN,
        "Situação",
        "N° de venda",
        "ID do pedido",
        "Número da nota",
    }
)


# -----------------------------------------------------------------------------
# KPIs (agregações sobre o recorte filtrado; sem recomputar regra de negócio)
# -----------------------------------------------------------------------------

REPASSE_KPI_COLUMNS: frozenset[str] = frozenset(
    {
        REPASSE_ACTION_COLUMN,
        "Valor a receber",
        "Valor pago",
        "Diferença",
    }
)


# -----------------------------------------------------------------------------
# Exportação (ordem sugerida; subset do contrato; valores canónicos do Parquet)
# -----------------------------------------------------------------------------

REPASSE_EXPORT_COLUMN_ORDER: tuple[str, ...] = (
    "N° de venda",
    "ID do pedido",
    "Número da nota",
    "Data de emissão",
    "Data período repasse",
    "Data de pagamento",
    "Valor da nota",
    "Valor a receber",
    "Valor pago",
    "Diferença",
    "Situação",
    REPASSE_ACTION_COLUMN,
    "Plataforma",
)


# -----------------------------------------------------------------------------
# Colunas técnicas (pipeline; normalmente ocultas ao utilizador final)
# -----------------------------------------------------------------------------

REPASSE_TECHNICAL_COLUMNS: frozenset[str] = frozenset(
    {
        "Numero_sem_parcela",
    }
)


# -----------------------------------------------------------------------------
# Invariantes documentadas (a aplicar no pipeline/app em PRs futuros)
# -----------------------------------------------------------------------------

REPASSE_CONTRACT_INVARIANTS: tuple[str, ...] = (
    "O dataset final operacional representa apenas linhas válidas para a fila operacional de repasse.",
    "Linhas sem N° de venda não pertencem ao dataset final operacional.",
    "A coluna Data período repasse deve existir no dataset final consumido pelo app (após migração para o contrato completo).",
    f"A coluna {REPASSE_ACTION_COLUMN!r} é a única coluna canónica de ação para filtro, KPI e exibição.",
    "O artefato principal futuro é o Parquet nomeado por REPASSE_ARTIFACT_FILENAME sob repasse/current/.",
)


def repasse_all_contract_column_names() -> frozenset[str]:
    """União de obrigatórias, identidade e técnicas (útil para validações futuras)."""
    return REPASSE_REQUIRED_COLUMNS | REPASSE_IDENTITY_COLUMNS | REPASSE_TECHNICAL_COLUMNS
