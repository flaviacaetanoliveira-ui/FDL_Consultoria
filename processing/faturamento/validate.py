"""Validações essenciais do pipeline de faturamento."""
from __future__ import annotations

from collections.abc import Set

import pandas as pd

from .config import (
    CUSTO_SKU_COL,
    CUSTO_UNITARIO_COL,
    REQUIRED_PEDIDO_COLUMNS,
    STATUS_CUSTO_OK,
    STATUS_SKU_DUPLICADO_CUSTO,
    STATUS_SKU_SEM_CORRESPONDENCIA,
)
from .normalize import normalize_sku_key


class FaturamentoValidationError(ValueError):
    pass


def assert_required_columns_pedido(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_PEDIDO_COLUMNS if c not in df.columns]
    if missing:
        raise FaturamentoValidationError(f"Colunas obrigatórias ausentes no CSV de pedidos: {missing}")


def normalized_duplicate_sku_keys_custo(df_custo: pd.DataFrame) -> frozenset[str]:
    """Chaves SKU normalizadas que aparecem mais do que uma vez na tabela de custo (antes do drop no join)."""
    s = normalize_sku_key(df_custo[CUSTO_SKU_COL])
    s = s[s.ne("")]
    if s.empty:
        return frozenset()
    vc = s.value_counts()
    return frozenset(vc[vc > 1].index.astype(str).tolist())


def assert_sku_unique_custo(df_custo: pd.DataFrame) -> None:
    dup = normalized_duplicate_sku_keys_custo(df_custo)
    if dup:
        bad = sorted(dup)[:20]
        raise FaturamentoValidationError(f"SKU duplicado na tabela de custo (exemplos): {bad}")


def assert_all_skus_have_custo(df: pd.DataFrame) -> None:
    """Regra estrita usada só no build **V1** (legado). V2 audita com ``Status_Custo`` sem abortar."""
    if CUSTO_UNITARIO_COL not in df.columns:
        raise FaturamentoValidationError(f"Coluna {CUSTO_UNITARIO_COL!r} ausente após join com custo.")
    if df[CUSTO_UNITARIO_COL].isna().any():
        miss = df.loc[df[CUSTO_UNITARIO_COL].isna(), "Código"].astype(str).unique().tolist()[:50]
        raise FaturamentoValidationError(f"SKU sem custo na tabela de custo (amostra): {miss}")


def assign_custo_audit_columns(df: pd.DataFrame, duplicate_normalized_skus: Set[str]) -> pd.DataFrame:
    """
    Preenche Status_Custo e flags de auditoria após join (schema v2 ou enriquecimento v1).

    SKUs duplicados na folha de custo: linhas com match podem ficar com status de duplicidade
    (primeira ocorrência usada no merge).
    """
    out = df.copy()
    sku_j = normalize_sku_key(out["Código"])
    cu = out[CUSTO_UNITARIO_COL]
    miss = cu.isna()
    dup_set = frozenset(duplicate_normalized_skus)
    in_dup = sku_j.isin(dup_set)

    status = pd.Series(STATUS_CUSTO_OK, index=out.index, dtype=object)
    status.loc[miss] = STATUS_SKU_SEM_CORRESPONDENCIA
    status.loc[in_dup & ~miss] = STATUS_SKU_DUPLICADO_CUSTO
    status.loc[miss & in_dup] = STATUS_SKU_SEM_CORRESPONDENCIA
    out["Status_Custo"] = status
    out["Flag_SKU_Sem_Custo"] = miss
    out["Flag_Produto_Sem_Correspondencia_SKU"] = miss
    out["Flag_SKU_Duplicado_Na_Tabela_Custo"] = in_dup
    return out
