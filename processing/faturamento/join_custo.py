"""Join pedidos ↔ custo por SKU (Código)."""
from __future__ import annotations

import pandas as pd

from .config import CUSTO_COL_PRECO, CUSTO_SKU_COL
from .normalize import normalize_sku_key, to_numeric_br


def join_custo_produto(df_pedidos: pd.DataFrame, df_custo: pd.DataFrame) -> pd.DataFrame:
    p = df_pedidos.copy()
    c = df_custo[[CUSTO_SKU_COL, CUSTO_COL_PRECO]].copy()
    p["_sku_join"] = normalize_sku_key(p[CUSTO_SKU_COL])
    c["_sku_join"] = normalize_sku_key(c[CUSTO_SKU_COL])
    c = c.drop_duplicates(subset=["_sku_join"], keep="first")
    right = c.rename(columns={CUSTO_COL_PRECO: "Custo do Produto"})[
        ["_sku_join", "Custo do Produto"]
    ]
    right["Custo do Produto"] = to_numeric_br(right["Custo do Produto"])
    out = p.merge(right, on="_sku_join", how="left")
    out = out.drop(columns=["_sku_join"])
    return out
