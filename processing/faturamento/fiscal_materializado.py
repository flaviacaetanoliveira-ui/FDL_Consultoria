"""
Materializado fiscal em grão NF (contrato: ``docs/faturamento_materializado_fiscal_contrato.md``).

Gera ``dataset_faturamento_fiscal.parquet`` a partir dos exports de notas de saída (Bling),
sem depender de linhas de pedido. Alinhado a ``enrich_pedidos_com_notas`` no filtro org/empresa.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .io_notas_saida import (
    detectar_col_valor_total_liquido,
    filtrar_notas_canceladas,
    load_notas_saida_from_dir,
)
from .join_notas import (
    _filtrar_notas_por_empresa,
    _prep_notas_dataframe,
    _situacao_por_nf_agregada,
)
from .normalize import normalize_nf_fiscal_commercial_join_key_scalar
from .params import FaturamentoParams, FaturamentoParamsV2, load_faturamento_params
from .validate import FaturamentoValidationError

def _find_col_valor_total_bruto(columns: list[str], col_liq: str) -> str:
    for c in columns:
        if c == col_liq:
            continue
        n = str(c).strip().lower()
        if "valor" in n and "total" in n and "liquido" not in n and "líquido" not in n:
            return c
    if "Valor total" in columns and col_liq != "Valor total":
        return "Valor total"
    return ""


SCHEMA_VERSION_FISCAL = 1

FISCAL_CONTRACT_COLUMNS: tuple[str, ...] = (
    "org_id",
    "empresa",
    "Nota_Numero_Normalizado",
    "Nota_Data_Emissao",
    "Nota_Situacao",
    "Valor_Liquido_NF",
    "Frete_Nota_Export",
    "Valor_Total_NF",
    "schema_version_fiscal",
)

_FISCAL_OPTIONAL_READ_COLS = frozenset({"Valor_Total_NF", "Frete_Nota_Export"})
FISCAL_CONTRACT_REQUIRED_READ: frozenset[str] = frozenset(
    c for c in FISCAL_CONTRACT_COLUMNS if c not in _FISCAL_OPTIONAL_READ_COLS
)


def fiscal_contract_dataframe_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    return FISCAL_CONTRACT_REQUIRED_READ.issubset(df.columns)


def _empty_fiscal_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(FISCAL_CONTRACT_COLUMNS))


def _aggregate_bruto_from_raw(
    raw_slice: pd.DataFrame,
    prep: pd.DataFrame,
    *,
    col_bruto: str,
) -> pd.Series:
    """Soma valor bruto por nf_key (linhas alinhadas a ``prep`` por índice)."""
    if not col_bruto or col_bruto not in raw_slice.columns or len(raw_slice) != len(prep):
        return pd.Series(dtype=float)
    from .normalize import to_numeric_br

    r = raw_slice.copy()
    r["_nf_key"] = prep["nf_key"].to_numpy()
    r["_v"] = to_numeric_br(r[col_bruto])
    return r.groupby("_nf_key", sort=False)["_v"].sum()


def build_fiscal_notas_from_directory(
    notas_dir: Path,
    *,
    org_id: str,
    empresa: str,
    excluir_nf_keys: frozenset[str] | None = None,
) -> pd.DataFrame:
    """
    Lê todos os CSV/XLSX em ``notas_dir``, aplica o mesmo filtro canceladas + prep + filtro empresa
    que o join de pedidos, e devolve 1 linha por NF.
    """
    notas_dir = notas_dir.expanduser().resolve()
    raw = load_notas_saida_from_dir(notas_dir)
    if raw.empty:
        return _empty_fiscal_frame()

    raw = filtrar_notas_canceladas(raw)
    if raw.empty:
        return _empty_fiscal_frame()

    cols = list(raw.columns)
    col_liq = detectar_col_valor_total_liquido(cols) or (
        "Valor total" if "Valor total" in cols else ""
    )
    col_br = _find_col_valor_total_bruto(cols, col_liq) if col_liq else ""

    try:
        prep = _prep_notas_dataframe(raw)
    except FaturamentoValidationError:
        return _empty_fiscal_frame()

    if prep.empty:
        return _empty_fiscal_frame()

    prep = _filtrar_notas_por_empresa(prep, org_id, empresa)
    if prep.empty:
        return _empty_fiscal_frame()

    sit_by_nf = _situacao_por_nf_agregada(prep)

    bruto_by_nf = (
        _aggregate_bruto_from_raw(raw.loc[prep.index].copy(), prep, col_bruto=col_br)
        if col_br
        else None
    )

    g = prep.groupby("nf_key", sort=False)
    agg = g.agg(
        Valor_Liquido_NF=("vl_liq", "sum"),
        Frete_Nota_Export=("frete_linha", "sum"),
        Nota_Data_Emissao=("dt_emissao", "min"),
    ).reset_index()

    agg = agg.rename(columns={"nf_key": "Nota_Numero_Normalizado"})
    if excluir_nf_keys:
        _nk_ex = agg["Nota_Numero_Normalizado"].map(normalize_nf_fiscal_commercial_join_key_scalar)
        agg = agg.loc[~_nk_ex.isin(excluir_nf_keys)].copy()
        if agg.empty:
            return _empty_fiscal_frame()
        sit_by_nf = _situacao_por_nf_agregada(prep.loc[prep["nf_key"].isin(agg["Nota_Numero_Normalizado"])])
    agg["Nota_Situacao"] = agg["Nota_Numero_Normalizado"].map(sit_by_nf).fillna("").astype(str)
    agg["org_id"] = str(org_id).strip()
    agg["empresa"] = str(empresa).strip()
    if bruto_by_nf is not None:
        agg["Valor_Total_NF"] = agg["Nota_Numero_Normalizado"].map(bruto_by_nf)
    else:
        agg["Valor_Total_NF"] = np.nan
    agg["Frete_Nota_Export"] = pd.to_numeric(agg["Frete_Nota_Export"], errors="coerce").fillna(0.0)
    agg["schema_version_fiscal"] = SCHEMA_VERSION_FISCAL

    out = agg[list(FISCAL_CONTRACT_COLUMNS)].copy()
    if out["Nota_Data_Emissao"].notna().any():
        out = out.sort_values("Nota_Data_Emissao", ascending=False, na_position="last")
    return out.reset_index(drop=True)


def build_fiscal_materializado_dataframe(params_path: Path) -> pd.DataFrame:
    """
    Constrói o DataFrame fiscal a partir de ``faturamento_params.json``.

    - **V2:** uma passagem por ``empresas[]`` (mesmos ``notas_saida_dir`` relativos que o build V2).
    - **V1:** devolve frame vazio (sem notas no contrato V1).
    """
    params_union = load_faturamento_params(params_path)
    if isinstance(params_union, FaturamentoParams):
        return _empty_fiscal_frame()

    if not isinstance(params_union, FaturamentoParamsV2):
        return _empty_fiscal_frame()

    parts: list[pd.DataFrame] = []
    for emp in params_union.empresas:
        rel_notas = (emp.notas_saida_dir or params_union.notas_saida_dir).strip() or params_union.notas_saida_dir
        notas_dir = (params_union.cliente_root / rel_notas).resolve()
        df_e = build_fiscal_notas_from_directory(
            notas_dir,
            org_id=emp.org_id,
            empresa=emp.empresa,
        )
        if not df_e.empty:
            parts.append(df_e)

    if not parts:
        return _empty_fiscal_frame()

    out = pd.concat(parts, ignore_index=True)
    dup = out.duplicated(subset=["org_id", "empresa", "Nota_Numero_Normalizado"], keep=False)
    if dup.any():
        out = (
            out.groupby(["org_id", "empresa", "Nota_Numero_Normalizado"], sort=False)
            .agg(
                Nota_Data_Emissao=("Nota_Data_Emissao", "min"),
                Nota_Situacao=("Nota_Situacao", "first"),
                Valor_Liquido_NF=("Valor_Liquido_NF", "sum"),
                Frete_Nota_Export=("Frete_Nota_Export", "sum"),
                Valor_Total_NF=("Valor_Total_NF", "sum"),
                schema_version_fiscal=("schema_version_fiscal", "first"),
            )
            .reset_index()
        )
    return out[list(FISCAL_CONTRACT_COLUMNS)].reset_index(drop=True)


def fiscal_materializado_meta_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    return {"fiscal_row_count": int(len(df))}
