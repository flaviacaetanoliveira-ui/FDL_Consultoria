"""
Materializado em grão NF (contrato: ``docs/faturamento_materializado_nf_first_contrato.md``).

Gera ``dataset_faturamento_nf.parquet`` a partir do dataset em grão linha, reutilizando
``build_nf_grain_dataframe`` com recorte de emissão = universo completo do ficheiro linha.
"""

from __future__ import annotations

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoRecorteMinState,
    build_nf_grain_dataframe,
    faturamento_min_series_nf_emissao_bounds_dates,
)

SCHEMA_VERSION_NF_FIRST = 1

# Ordem canónica do contrato (sem ``plataforma_resumo`` — substituída por ``plataforma``).
NF_FIRST_CONTRACT_COLUMNS: tuple[str, ...] = (
    "org_id",
    "empresa",
    "Nota_Numero_Normalizado",
    "Nota_Data_Emissao",
    "Nota_Situacao",
    "valor_faturado_nf",
    "plataforma",
    "valor_venda",
    "n_linhas_pedido",
    "pedido_resumo",
    "produto_resumo",
    "comissao",
    "frete",
    "imposto",
    "despesa_fixa",
    "diferenca",
    "resultado",
    "faturamento_nota_vinculada",
    "schema_version_nf",
)

NF_FIRST_CONTRACT_REQUIRED_READ: frozenset[str] = frozenset(
    c
    for c in NF_FIRST_CONTRACT_COLUMNS
    if c not in frozenset({"schema_version_nf"})
)


def nf_first_contract_dataframe_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    return NF_FIRST_CONTRACT_REQUIRED_READ.issubset(df.columns)


def build_nf_materializado_dataframe(df_line: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega o dataset linha ao grão NF conforme contrato NF-first.
    """
    empty = pd.DataFrame(columns=list(NF_FIRST_CONTRACT_COLUMNS))
    if df_line.empty:
        return empty

    lo, hi, ok = faturamento_min_series_nf_emissao_bounds_dates(df_line)
    if not ok or hi < lo:
        return empty

    st = FaturamentoRecorteMinState((), ())
    df_nf, _warn = build_nf_grain_dataframe(
        df_line,
        st,
        ok_nf_dates=True,
        nf_d_ini=lo,
        nf_d_fim=hi,
    )
    if df_nf.empty:
        return empty

    out = df_nf.copy()
    out["plataforma"] = out["plataforma_resumo"].fillna("").astype(str)
    out["schema_version_nf"] = SCHEMA_VERSION_NF_FIRST

    for c in NF_FIRST_CONTRACT_COLUMNS:
        if c not in out.columns:
            if c == "faturamento_nota_vinculada":
                out[c] = True
            elif c == "schema_version_nf":
                out[c] = SCHEMA_VERSION_NF_FIRST
            else:
                out[c] = pd.NA

    out["faturamento_nota_vinculada"] = out["faturamento_nota_vinculada"].fillna(False).astype(bool)
    out["schema_version_nf"] = pd.to_numeric(out["schema_version_nf"], errors="coerce").fillna(
        SCHEMA_VERSION_NF_FIRST
    ).astype(int)

    return out[list(NF_FIRST_CONTRACT_COLUMNS)].copy()
