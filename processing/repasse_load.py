"""
Leitura e validação do repasse materializado em Parquet (PR4).

Mantido fora de ``app_operacional`` para permitir testes sem inicializar Streamlit.
"""

from __future__ import annotations

import math
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from processing.repasse_contract import REPASSE_REQUIRED_COLUMNS


def repasse_use_parquet_flag(
    environ: Mapping[str, str],
    *,
    secret_raw: str | bool | None = None,
) -> bool:
    """
    FDL_REPASSE_USE_PARQUET: 1/true/yes/on → Parquet; 0/false/no/off → força CSV quando explícito.
    ``secret_raw``: valor de st.secrets (opcional), alinhado a outros flags no app.
    """
    raw = str(environ.get("FDL_REPASSE_USE_PARQUET", "") or "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        if isinstance(secret_raw, bool):
            return secret_raw
        s = str(secret_raw or "").strip().lower()
        if s in {"1", "true", "yes", "on"}:
            return True
        if s in {"0", "false", "no", "off"}:
            return False
    except Exception:  # noqa: BLE001
        pass
    return False


def validate_repasse_contract_columns(df: pd.DataFrame, required: frozenset[str] | None = None) -> None:
    """Garante colunas mínimas do contrato PR1; não exige células preenchidas em Data período repasse."""
    req = required if required is not None else REPASSE_REQUIRED_COLUMNS
    missing = sorted(req - set(df.columns))
    if missing:
        raise ValueError(
            "Repasse Parquet: colunas obrigatórias do contrato em falta: " + ", ".join(missing)
        )


def validate_repasse_empresa_id_column(df: pd.DataFrame, org_id: str) -> None:
    """empresa_id deve existir e coincidir com a org ativa em todas as linhas (df vazio: só exige coluna)."""
    if "empresa_id" not in df.columns:
        raise ValueError(
            "Repasse Parquet: falta a coluna «empresa_id» — não é possível validar a organização."
        )
    oid = str(org_id).strip()
    if not oid:
        raise ValueError("Repasse Parquet: org_id da sessão vazio.")
    if df.empty:
        return
    s = df["empresa_id"].fillna("").astype(str).str.strip()
    if not (s == oid).all():
        raise ValueError(
            f"Repasse Parquet: «empresa_id» inconsistente — esperado {oid!r} em todas as linhas. "
            "Verifique se o ficheiro corresponde à empresa selecionada."
        )


def coerce_numero_nota_fiscal_text(df: pd.DataFrame) -> pd.DataFrame:
    """Preserva texto e zeros à esquerda quando possível; evita float silencioso na coluna «Número da nota»."""
    col = "Número da nota"
    if col not in df.columns:
        return df
    out = df.copy()

    def cell(v: Any) -> str:
        if v is None:
            return ""
        if isinstance(v, float) and math.isnan(v):
            return ""
        if isinstance(v, bool):
            return str(v)
        if isinstance(v, int) and not isinstance(v, bool):
            return str(int(v))
        if isinstance(v, float):
            fv = float(v)
            iv = int(round(fv))
            if abs(fv - iv) < 1e-9:
                return str(iv)
            t = str(v).strip()
            return "" if t.lower() in {"nan", "none"} else t
        t = str(v).strip()
        if t.lower() in {"nan", "none", "nat", "<na>", "null"}:
            return ""
        if t.endswith(".0") and t[:-2].lstrip("-").isdigit():
            return t[:-2]
        return t

    out[col] = out[col].map(cell)
    return out


def read_repasse_parquet(path: Path) -> pd.DataFrame:
    """Lê Parquet do repasse (pyarrow)."""
    return pd.read_parquet(path, engine="pyarrow")


def postprocess_repasse_parquet_dataframe(df: pd.DataFrame, org_id: str) -> pd.DataFrame:
    """Valida contrato + identidade e normaliza «Número da nota»."""
    validate_repasse_contract_columns(df)
    validate_repasse_empresa_id_column(df, org_id)
    return coerce_numero_nota_fiscal_text(df)
