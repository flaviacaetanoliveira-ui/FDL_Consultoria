"""Validações essenciais do pipeline de faturamento."""
from __future__ import annotations

import pandas as pd

from .config import CUSTO_SKU_COL, REQUIRED_PEDIDO_COLUMNS


class FaturamentoValidationError(ValueError):
    pass


def assert_required_columns_pedido(df: pd.DataFrame) -> None:
    missing = [c for c in REQUIRED_PEDIDO_COLUMNS if c not in df.columns]
    if missing:
        raise FaturamentoValidationError(f"Colunas obrigatórias ausentes no CSV de pedidos: {missing}")


def assert_sku_unique_custo(df_custo: pd.DataFrame) -> None:
    from .normalize import normalize_sku_key

    s = normalize_sku_key(df_custo[CUSTO_SKU_COL])
    dup = s[s.ne("")].duplicated(keep=False)
    if dup.any():
        bad = sorted(df_custo.loc[dup, CUSTO_SKU_COL].astype(str).unique().tolist())[:20]
        raise FaturamentoValidationError(f"SKU duplicado na tabela de custo (exemplos): {bad}")


def assert_all_skus_have_custo(df: pd.DataFrame) -> None:
    if df["Custo do Produto"].isna().any():
        miss = df.loc[df["Custo do Produto"].isna(), "Código"].astype(str).unique().tolist()[:50]
        raise FaturamentoValidationError(f"SKU sem custo na tabela de custo (amostra): {miss}")
