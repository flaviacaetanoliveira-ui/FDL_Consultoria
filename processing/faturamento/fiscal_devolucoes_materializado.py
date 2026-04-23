"""
Materialização de NF de entrada — devoluções de venda (abatimento fiscal).

Grava ``dataset_faturamento_devolucoes.parquet`` sem alterar ``dataset_faturamento_fiscal.parquet``.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .fiscal_devolucoes_constants import (
    COL_TIPO_ABATIMENTO,
    NATUREZAS_DEVOLUCAO,
    TIPO_ABATIMENTO_DEVOLUCAO_VENDA,
)
from .io_notas_entrada import load_notas_entrada_devolucoes_from_dir
from .join_notas import (
    _filtrar_notas_por_empresa,
    _prep_notas_dataframe,
    _situacao_por_nf_agregada,
)
from .normalize import normalize_nf_fiscal_commercial_join_key_scalar
from .params import FaturamentoParams, FaturamentoParamsV2, load_faturamento_params
from .validate import FaturamentoValidationError


SCHEMA_VERSION_DEVOLUCOES = 1

DEVOLUCOES_CONTRACT_COLUMNS: tuple[str, ...] = (
    "org_id",
    "empresa",
    "Nota_Numero_Normalizado",
    "Nota_Data_Emissao",
    "Nota_Situacao",
    "Valor_Liquido_Devolucao",
    "Natureza",
    COL_TIPO_ABATIMENTO,
    "schema_version_devolucoes",
)


def devolucoes_contract_dataframe_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    required = frozenset(
        {
            "org_id",
            "empresa",
            "Nota_Numero_Normalizado",
            "Nota_Data_Emissao",
            "Valor_Liquido_Devolucao",
        }
    )
    return required.issubset(df.columns)


def _empty_devolucoes_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(DEVOLUCOES_CONTRACT_COLUMNS))


def build_devolucoes_fiscal_dataframe(params_path: Path) -> pd.DataFrame:
    """
    Constrói o DataFrame de devoluções (entrada Bling) a partir de ``faturamento_params.json`` (V2).
    """
    params_union = load_faturamento_params(params_path)
    if isinstance(params_union, FaturamentoParams):
        return _empty_devolucoes_frame()
    if not isinstance(params_union, FaturamentoParamsV2):
        return _empty_devolucoes_frame()

    parts: list[pd.DataFrame] = []
    default_entrada = getattr(params_union, "notas_entrada_dir", None)
    default_entrada_s = str(default_entrada).strip() if default_entrada else ""

    for emp in params_union.empresas:
        rel = (emp.notas_entrada_dir or default_entrada_s).strip() or default_entrada_s
        if not rel:
            continue
        notas_dir = (params_union.cliente_root / rel).resolve()
        raw = load_notas_entrada_devolucoes_from_dir(notas_dir)
        if raw.empty:
            continue
        try:
            prep = _prep_notas_dataframe(raw)
        except FaturamentoValidationError:
            continue
        if prep.empty:
            continue
        prep = _filtrar_notas_por_empresa(prep, emp.org_id, emp.empresa)
        if prep.empty:
            continue

        sit_by_nf = _situacao_por_nf_agregada(prep)
        g = prep.groupby("nf_key", sort=False)
        agg = g.agg(
            Valor_Liquido_Devolucao=("vl_liq", "sum"),
            Nota_Data_Emissao=("dt_emissao", "min"),
        ).reset_index()
        agg = agg.rename(columns={"nf_key": "Nota_Numero_Normalizado"})
        agg["Nota_Numero_Normalizado"] = agg["Nota_Numero_Normalizado"].map(
            normalize_nf_fiscal_commercial_join_key_scalar
        )
        agg["Nota_Situacao"] = agg["Nota_Numero_Normalizado"].map(sit_by_nf).fillna("").astype(str)
        agg["org_id"] = str(emp.org_id).strip()
        agg["empresa"] = str(emp.empresa).strip()
        agg["Natureza"] = NATUREZAS_DEVOLUCAO[0]
        agg[COL_TIPO_ABATIMENTO] = TIPO_ABATIMENTO_DEVOLUCAO_VENDA
        agg["Valor_Liquido_Devolucao"] = pd.to_numeric(agg["Valor_Liquido_Devolucao"], errors="coerce").fillna(0.0)
        agg["schema_version_devolucoes"] = SCHEMA_VERSION_DEVOLUCOES
        parts.append(agg[list(DEVOLUCOES_CONTRACT_COLUMNS)].copy())

    if not parts:
        return _empty_devolucoes_frame()

    out = pd.concat(parts, ignore_index=True)
    dup = out.duplicated(subset=["org_id", "empresa", "Nota_Numero_Normalizado"], keep=False)
    if dup.any():
        ta = COL_TIPO_ABATIMENTO
        out = (
            out.groupby(["org_id", "empresa", "Nota_Numero_Normalizado"], sort=False)
            .agg(
                Nota_Data_Emissao=("Nota_Data_Emissao", "min"),
                Nota_Situacao=("Nota_Situacao", "first"),
                Valor_Liquido_Devolucao=("Valor_Liquido_Devolucao", "sum"),
                Natureza=("Natureza", "first"),
                **{ta: (ta, "first")},
                schema_version_devolucoes=("schema_version_devolucoes", "first"),
            )
            .reset_index()
        )
    if out["Nota_Data_Emissao"].notna().any():
        out = out.sort_values("Nota_Data_Emissao", ascending=False, na_position="last")
    return out.reset_index(drop=True)


def devolucoes_materializado_meta_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty or not devolucoes_contract_dataframe_valid(df):
        return {
            "total_devolvido": 0.0,
            "nfs_devolucao": 0,
            "base_fiscal_composicao": "emitidas - canceladas - devolucoes",
        }
    vl = pd.to_numeric(df["Valor_Liquido_Devolucao"], errors="coerce").fillna(0.0)
    return {
        "total_devolvido": float(vl.sum()),
        "nfs_devolucao": int(len(df)),
        "base_fiscal_composicao": "emitidas - canceladas - devolucoes",
    }
