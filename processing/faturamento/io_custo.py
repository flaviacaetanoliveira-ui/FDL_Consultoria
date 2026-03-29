"""Leitura da tabela de custo (XLSX, aba Planilha1)."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import CUSTO_COL_PRECO, CUSTO_SHEET_NAME, CUSTO_SKU_COL


def load_custo_xlsx(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Tabela de custo não encontrada: {path}")
    df = pd.read_excel(path, sheet_name=CUSTO_SHEET_NAME, dtype=str)
    df = df.dropna(axis=1, how="all")
    missing = [c for c in (CUSTO_SKU_COL, CUSTO_COL_PRECO) if c not in df.columns]
    if missing:
        raise KeyError(f"Colunas ausentes na aba {CUSTO_SHEET_NAME!r}: {missing}. Encontradas: {list(df.columns)}")
    meta: dict[str, Any] = {"path": str(path), "sheet": CUSTO_SHEET_NAME}
    return df, meta
