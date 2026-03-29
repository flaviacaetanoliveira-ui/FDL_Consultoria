"""Cálculos de imposto, despesas fixas e resultado."""
from __future__ import annotations

import pandas as pd

from .normalize import to_numeric_br


def compute_financial_columns(
    df: pd.DataFrame,
    *,
    aliquota_imposto: float,
    aliquota_despesas_fixas: float,
    data_processamento_iso: str,
) -> pd.DataFrame:
    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    cf = "Custo de Frete"
    tc = "Taxa de Comissão"

    out[pl] = to_numeric_br(out[pl])
    out[vt] = to_numeric_br(out[vt])
    out[cf] = to_numeric_br(out[cf])
    out[tc] = to_numeric_br(out[tc])
    out["Custo do Produto"] = to_numeric_br(out["Custo do Produto"])

    out["Imposto"] = out[vt] * aliquota_imposto
    out["Despesas Fixas"] = out[pl] * aliquota_despesas_fixas
    out["Resultado"] = (
        out[pl]
        - out["Custo do Produto"]
        - out[cf]
        - out["Imposto"]
        - out[tc]
        - out["Despesas Fixas"]
    )
    out["Resultado_Pct"] = pd.NA
    mask = out[pl].notna() & (out[pl] > 0)
    out.loc[mask, "Resultado_Pct"] = out.loc[mask, "Resultado"] / out.loc[mask, pl]

    out["Aliquota_Imposto_Utilizada"] = aliquota_imposto
    out["Aliquota_Despesas_Fixas_Utilizada"] = aliquota_despesas_fixas
    out["Data_Processamento"] = data_processamento_iso
    return out
