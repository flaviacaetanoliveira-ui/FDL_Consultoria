"""Leitura da tabela de custo (XLSX, aba Planilha1)."""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path
from typing import Any

import pandas as pd

from .config import (
    CUSTO_COL_PRECO,
    CUSTO_COL_VALOR_EAP,
    CUSTO_COL_VALOR_GENERIC,
    CUSTO_COL_VALOR_MEGA,
    CUSTO_COL_VALOR_STAR_GAMA,
    CUSTO_SHEET_NAME,
    CUSTO_SKU_COL,
)


def _header_token(cell: object) -> str:
    t = unicodedata.normalize("NFKD", str(cell).strip()).encode("ascii", "ignore").decode().casefold()
    return re.sub(r"\s+", " ", t).strip()


def _detect_custo_header_row(raw: pd.DataFrame) -> tuple[int, int, int] | None:
    """
    Encontra linha de cabeçalho com colunas de SKU e preço unitário de custo
    (planilhas com título / linhas vazias antes da tabela).
    """
    max_scan = min(45, len(raw))
    ncols = int(raw.shape[1])
    for i in range(max_scan):
        row_text = [_header_token(raw.iat[i, j]) for j in range(ncols)]
        if sum(1 for x in row_text if x) < 3:
            continue
        sj: int | None = None
        pj: int | None = None
        for j, h in enumerate(row_text):
            if not h:
                continue
            if h in ("codigo sku", "sku") or (h == "codigo" and sj is None):
                sj = j
            # «PREÇO DE CUSTO com IPI» (layout legado) ou «VALOR DE COMPRA» (sem sufixo MEGA/OUTRAS).
            if (
                h == "custo unit"
                or h == "preco de custo com ipi"
                or ("custo" in h and "unit" in h)
                or h == "valor de compra"
            ):
                pj = j
        if sj is not None and pj is not None and sj != pj:
            return (i, sj, pj)
    return None


def _normalize_wide_custo_sheet_if_applicable(df0: pd.DataFrame) -> pd.DataFrame | None:
    """
    Planilha com cabeçalho na linha 0: ``Código``/``CÓDIGO`` + colunas de preço por empresa
    (``VALOR DE COMPRA MEGA``, ``VALOR COMPRA EAP``, ``VALOR COMPRA STAR/GAMA``, ``VALOR DE COMPRA`` genérico).

    Não altera o fluxo legado (só ``Código`` + ``PREÇO DE CUSTO com IPI`` em nomes canónicos): devolve ``None``.
    """
    code_src: str | None = None
    price_src: dict[str, str] = {}
    has_legacy_preco = False

    for c in df0.columns:
        t = _header_token(c)
        if t in ("codigo", "codigo sku", "sku"):
            if code_src is None:
                code_src = str(c)
        elif t == "valor de compra mega":
            price_src[CUSTO_COL_VALOR_MEGA] = str(c)
        elif t == "valor compra eap":
            price_src[CUSTO_COL_VALOR_EAP] = str(c)
        elif t in ("valor compra star/gama", "valor compra star gama"):
            price_src[CUSTO_COL_VALOR_STAR_GAMA] = str(c)
        elif t == "valor de compra":
            price_src[CUSTO_COL_VALOR_GENERIC] = str(c)
        elif t == "preco de custo com ipi":
            has_legacy_preco = True
            price_src[CUSTO_COL_PRECO] = str(c)

    if code_src is None:
        return None

    multi_empresa = any(
        k in price_src for k in (CUSTO_COL_VALOR_MEGA, CUSTO_COL_VALOR_EAP, CUSTO_COL_VALOR_STAR_GAMA)
    )
    if not multi_empresa and not (CUSTO_COL_VALOR_GENERIC in price_src):
        return None
    if not multi_empresa and has_legacy_preco and len(price_src) == 1:
        return None

    out = pd.DataFrame()
    out[CUSTO_SKU_COL] = df0[code_src].astype(str)
    for canon, src in sorted(price_src.items(), key=lambda x: x[0]):
        out[canon] = df0[src].astype(str)
    return out


def _frame_from_detection(raw: pd.DataFrame, header_idx: int, sj: int, pj: int) -> pd.DataFrame:
    rows: list[tuple[str, str]] = []
    for ii in range(header_idx + 1, len(raw)):
        sku_c = raw.iat[ii, sj]
        pr_c = raw.iat[ii, pj]
        if pd.isna(sku_c) and pd.isna(pr_c):
            continue
        sku_s = "" if pd.isna(sku_c) else str(sku_c).strip()
        pr_s = "" if pd.isna(pr_c) else str(pr_c).strip()
        if not sku_s:
            continue
        rows.append((sku_s, pr_s))
    return pd.DataFrame(rows, columns=[CUSTO_SKU_COL, CUSTO_COL_PRECO])


def load_custo_xlsx(path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FileNotFoundError(f"Tabela de custo não encontrada: {path}")

    df0 = pd.read_excel(path, sheet_name=CUSTO_SHEET_NAME, dtype=str).dropna(axis=1, how="all")

    df_wide = _normalize_wide_custo_sheet_if_applicable(df0)
    if df_wide is not None:
        meta_w: dict[str, Any] = {
            "path": str(path),
            "sheet": CUSTO_SHEET_NAME,
            "custo_reader": "wide_empresa_columns",
            "custo_columns": [c for c in df_wide.columns if c != CUSTO_SKU_COL],
        }
        return df_wide, meta_w

    missing0 = [c for c in (CUSTO_SKU_COL, CUSTO_COL_PRECO) if c not in df0.columns]
    if not missing0:
        meta: dict[str, Any] = {"path": str(path), "sheet": CUSTO_SHEET_NAME, "custo_reader": "header_row_0"}
        return df0, meta

    raw = pd.read_excel(path, sheet_name=CUSTO_SHEET_NAME, header=None, dtype=str)
    det = _detect_custo_header_row(raw)
    if det is None:
        raise KeyError(
            f"Colunas ausentes na aba {CUSTO_SHEET_NAME!r}: {missing0}. "
            f"Tentativa automática (cabeçalho deslocado) não encontrou SKU + preço de custo. "
            f"Encontradas (linha 0): {list(df0.columns)}"
        )
    hi, sj, pj = det
    df = _frame_from_detection(raw, hi, sj, pj)
    meta = {
        "path": str(path),
        "sheet": CUSTO_SHEET_NAME,
        "custo_reader": "header_autodetect",
        "custo_header_row": hi,
        "custo_col_sku_index": sj,
        "custo_col_preco_index": pj,
    }
    return df, meta
