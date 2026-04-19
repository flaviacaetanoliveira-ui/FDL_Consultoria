"""Chaves puras para cache do Resultado Gerencial (testável sem Streamlit)."""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from datetime import date

import numpy as np
import pandas as pd

PIPELINE_VERSION_ENV_NAME = "FDL_RG_PIPELINE_VERSION"


def normalize_sorted_str_tuple(items: Iterable[str]) -> tuple[str, ...]:
    xs = {str(x).strip() for x in items if str(x).strip()}
    return tuple(sorted(xs, key=lambda t: t.casefold()))


def dataframe_cache_token(df: pd.DataFrame) -> str:
    """Fingerprint estável e barato para chaves de cache (não criptográfico)."""
    h = hashlib.blake2b(digest_size=16)
    h.update(f"{df.shape[0]}:{df.shape[1]}".encode())
    h.update("|".join(str(c) for c in df.columns).encode())
    n = len(df)
    if n == 0:
        return h.hexdigest()
    head = min(120, n)
    tail = min(120, n)
    sub = df.iloc[list(range(head)) + ([] if head >= n else list(range(n - tail, n)))]
    try:
        hv = pd.util.hash_pandas_object(sub, index=True).values
        h.update(memoryview(np.asarray(hv)))
    except Exception:
        h.update(sub.to_csv(index=True).encode("utf-8", errors="replace"))
    return h.hexdigest()


def rg_core_identity(
    df_token: str,
    empresas: tuple[str, ...],
    plataformas: tuple[str, ...],
    data_ini: date,
    data_fim: date,
    fiscal_imposto_valor: float,
    pipeline_version: str,
    cliente_slug: str,
) -> tuple[object, ...]:
    return (
        df_token,
        empresas,
        plataformas,
        data_ini.isoformat(),
        data_fim.isoformat(),
        round(float(fiscal_imposto_valor), 8),
        str(pipeline_version).strip(),
        str(cliente_slug or "").strip(),
    )


def slice_hash_for_dependents(
    df_token: str,
    empresas: tuple[str, ...],
    plataformas: tuple[str, ...],
    data_ini: date,
    data_fim: date,
    pipeline_version: str,
    cliente_slug: str,
) -> str:
    raw = "|".join(
        [
            df_token,
            ",".join(empresas),
            ",".join(plataformas),
            data_ini.isoformat(),
            data_fim.isoformat(),
            pipeline_version,
            cliente_slug,
        ]
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
