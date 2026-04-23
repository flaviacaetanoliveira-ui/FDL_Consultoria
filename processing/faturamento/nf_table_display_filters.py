"""Filtros de exibição da Tabela por NF (plataforma + busca NF/pedido)."""

from __future__ import annotations

import re
from typing import Sequence

import pandas as pd


def nf_table_filter_mask(
    df: pd.DataFrame,
    *,
    plataformas_sel: Sequence[str],
    busca: str,
) -> pd.Series:
    """Máscara booleana sobre colunas ``Plataforma``, ``NF`` e ``Pedido`` da tabela exibida."""
    if df.empty:
        return pd.Series(dtype=bool)
    m = pd.Series(True, index=df.index)
    ps = tuple(str(x).strip() for x in plataformas_sel if str(x).strip())
    if ps:
        if "Plataforma" not in df.columns:
            return pd.Series(False, index=df.index)
        m &= df["Plataforma"].astype(str).isin(ps)
    q = str(busca).strip()
    if q:
        ql = q.lower()
        nf_s = df["NF"].astype(str).str.lower()
        ped_s = df["Pedido"].astype(str).str.lower()
        m &= nf_s.str.contains(re.escape(ql), na=False) | ped_s.str.contains(re.escape(ql), na=False)
    return m
