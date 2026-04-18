"""Carga de CSV/XLSX de notas de entrada (Bling) — devoluções de venda."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .fiscal_devolucoes_constants import (
    COL_TIPO_ABATIMENTO,
    NATUREZAS_DEVOLUCAO,
    SITUACOES_DEVOLUCAO_VALIDAS,
    TIPO_ABATIMENTO_DEVOLUCAO_VENDA,
)
from .io_notas_saida import (
    _read_notas_file,
    detectar_col_data_emissao,
    detectar_col_valor_total_liquido,
)


def _norm_txt(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _detect_col_natureza(columns: list[str]) -> str:
    for c in columns:
        if str(c).strip().casefold() == "natureza":
            return c
    for c in columns:
        cl = str(c).strip().lower()
        if "natur" in cl:
            return c
    return ""


def _detect_col_numero_nf(columns: list[str]) -> str:
    if "Número" in columns:
        return "Número"
    for c in columns:
        cl = str(c).strip().lower()
        if cl in {"numero", "número", "nr nota", "nr_nota"} and "pedido" not in cl:
            return c
    return ""


def _detect_col_situacao(columns: list[str]) -> str:
    for c in columns:
        n = str(c).lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            return c
    return ""


def load_notas_entrada_devolucoes_from_dir(notas_dir: Path) -> pd.DataFrame:
    """
    Lê todos os CSV/XLSX sob ``notas_dir``, mantém apenas linhas com Natureza de devolução
    e situação autorizada; deduplica e marca ``_tipo_abatimento``.
    """
    notas_dir = notas_dir.expanduser().resolve()
    if not notas_dir.is_dir():
        return pd.DataFrame()

    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in notas_dir.rglob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    partes: list[pd.DataFrame] = []
    for f in files:
        df = _read_notas_file(f).dropna(axis=1, how="all").copy()
        df["__arquivo_nota__"] = f.name
        partes.append(df)
    if not partes:
        return pd.DataFrame()

    out = pd.concat(partes, ignore_index=True)
    cols = list(out.columns)
    col_nat = _detect_col_natureza(cols)
    col_sit = _detect_col_situacao(cols)
    if not col_nat or not col_sit:
        return pd.DataFrame()

    nat_ok = frozenset(NATUREZAS_DEVOLUCAO)
    sit_ok = frozenset(SITUACOES_DEVOLUCAO_VALIDAS)

    n_raw = _norm_txt(out[col_nat])
    s_raw = _norm_txt(out[col_sit])
    m_nat = n_raw.isin(nat_ok)
    m_sit = s_raw.isin(sit_ok)
    out = out.loc[m_nat & m_sit].copy()
    if out.empty:
        return pd.DataFrame()

    col_nf = _detect_col_numero_nf(list(out.columns))
    col_dt = detectar_col_data_emissao(list(out.columns))
    col_vl = detectar_col_valor_total_liquido(list(out.columns))
    if not col_vl and "Valor total" in out.columns:
        col_vl = "Valor total"

    dedup_keys = ["__arquivo_nota__", col_nat]
    if col_nf:
        dedup_keys.append(col_nf)
    if col_dt:
        dedup_keys.append(col_dt)
    if col_vl:
        dedup_keys.append(col_vl)
    dedup_keys = [k for k in dedup_keys if k in out.columns]
    if dedup_keys:
        out = out.drop_duplicates(subset=dedup_keys, keep="first")

    out[COL_TIPO_ABATIMENTO] = TIPO_ABATIMENTO_DEVOLUCAO_VENDA
    return out.reset_index(drop=True)
