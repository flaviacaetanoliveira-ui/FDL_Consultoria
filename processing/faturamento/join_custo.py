"""Join pedidos ↔ custo por SKU (Código); coluna de preço conforme a empresa na planilha wide."""
from __future__ import annotations

import pandas as pd

from .custo_por_empresa import join_custo_produto_por_empresa


def join_custo_produto(df_pedidos: pd.DataFrame, df_custo: pd.DataFrame, *, empresa: str | None = None) -> pd.DataFrame:
    """
    ``empresa``: rótulo da empresa (ex.: ``Gama Home``) para escolher a coluna de custo em ``Custos.xlsx`` wide.
    ``None`` no build V1 ou quando a folha só tem uma coluna de preço.
    """
    return join_custo_produto_por_empresa(df_pedidos, df_custo, empresa)
