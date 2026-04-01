"""Cálculos de imposto, despesas fixas e resultado (v2 — receita bruta × quantidade)."""
from __future__ import annotations

import pandas as pd

from .config import CUSTO_UNITARIO_COL
from .config import OUTRAS_DESPESAS_COL
from .config import STATUS_CUSTO_OK
from .normalize import to_numeric_br

_COL_MODALIDADE_ENVIO_CANDIDATES: tuple[str, ...] = (
    "Modalidade de envio",
    "Tipo de envio",
    "Transportadora",
    "Serviço de envio",
    "Servico de envio",
    "Tipo de logística",
    "Tipo de logistica",
)


def _frete_mercado_envios_vs_transportadora(df: pd.DataFrame, cf_numeric: pd.Series) -> tuple[pd.Series, pd.Series]:
    """
    Parte ``Custo de Frete`` entre Mercado Envios e transportadora própria (ou terceiros não-ME).

    Sem coluna de modalidade no CSV de pedidos, trata todo o frete como ME (comportamento anterior).
    ``Receita_Bruta`` no pipeline passa a incluir o frete de transportadora própria (ver ``compute_financial_columns``).
    """
    cf0 = cf_numeric.fillna(0.0)
    col = next((c for c in _COL_MODALIDADE_ENVIO_CANDIDATES if c in df.columns), None)
    if col is None:
        return cf0, pd.Series(0.0, index=df.index, dtype=float)
    txt = df[col].fillna("").astype(str).str.strip().str.casefold()
    is_me = txt.str.contains("mercado envio", regex=False) | txt.str.contains(
        "coleta do mercado", regex=False
    )
    # Texto vazio: manter legado (conta como ME quando há custo de frete).
    is_me = is_me | txt.eq("")
    frete_me = cf0.where(is_me, 0.0)
    frete_tp = cf0.where(~is_me, 0.0)
    return frete_me, frete_tp


def resolve_coluna_base_imposto(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c and c in df.columns:
            return c
    return None


def compute_financial_columns(
    df: pd.DataFrame,
    *,
    aliquota_imposto: float,
    aliquota_despesas_fixas: float,
    data_processamento_iso: str,
    base_imposto_column: str | None,
) -> pd.DataFrame:
    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    cf = "Custo de Frete"
    tc = "Taxa de Comissão"
    qtd_col = "Quantidade"

    out[qtd_col] = to_numeric_br(out[qtd_col])
    out[pl] = to_numeric_br(out[pl])
    out[vt] = to_numeric_br(out[vt])
    out[cf] = to_numeric_br(out[cf])
    out[tc] = to_numeric_br(out[tc])
    out[CUSTO_UNITARIO_COL] = to_numeric_br(out[CUSTO_UNITARIO_COL])

    if OUTRAS_DESPESAS_COL in out.columns:
        out[OUTRAS_DESPESAS_COL] = to_numeric_br(out[OUTRAS_DESPESAS_COL])
    else:
        out[OUTRAS_DESPESAS_COL] = 0.0

    frete_me, frete_tp = _frete_mercado_envios_vs_transportadora(out, out[cf])
    out["Frete Mercado Envios"] = frete_me
    out["Frete transportadora própria"] = frete_tp
    # Frete da transportadora própria integra receita bruta (contabilidade pedida na validação).
    out["Receita_Bruta"] = out[qtd_col] * out[pl] + frete_tp
    out["Custo_Produto_Total"] = out[qtd_col] * out[CUSTO_UNITARIO_COL]

    if base_imposto_column:
        out["Base_Imposto"] = to_numeric_br(out[base_imposto_column])
    else:
        out["Base_Imposto"] = pd.NA

    out["Imposto"] = out["Base_Imposto"] * aliquota_imposto
    out["Despesas Fixas"] = out["Receita_Bruta"] * aliquota_despesas_fixas
    out["Resultado"] = (
        out["Receita_Bruta"]
        - out["Custo_Produto_Total"]
        - out[cf]
        - out[tc]
        - out[OUTRAS_DESPESAS_COL]
        - out["Imposto"]
        - out["Despesas Fixas"]
    )
    out["Resultado_Pct"] = pd.NA
    mask_rb = out["Receita_Bruta"].notna() & (out["Receita_Bruta"] > 0)
    out.loc[mask_rb, "Resultado_Pct"] = (
        out.loc[mask_rb, "Resultado"] / out.loc[mask_rb, "Receita_Bruta"]
    )

    if "Status_Custo" in out.columns:
        ok = out["Status_Custo"].eq(STATUS_CUSTO_OK)
        bad = ~ok.fillna(True)
        out.loc[bad, "Resultado"] = pd.NA
        out.loc[bad, "Resultado_Pct"] = pd.NA

    out["Aliquota_Imposto_Utilizada"] = aliquota_imposto
    out["Aliquota_Despesas_Fixas_Utilizada"] = aliquota_despesas_fixas
    out["Data_Processamento"] = data_processamento_iso
    return out
