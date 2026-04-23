"""Carga de CSV/XLSX de notas de saída para o pipeline de faturamento."""
from __future__ import annotations

import csv
import re
import unicodedata
from pathlib import Path

import pandas as pd


def _norm_txt(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _read_notas_file(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    last_err: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t", "|"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", dtype=str)
            except Exception as e:  # noqa: BLE001
                last_err = e
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                sep=";",
                engine="python",
                dtype=str,
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
            )
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"Falha ao ler notas: {path} ({last_err})")


def filtrar_notas_canceladas(notas: pd.DataFrame) -> pd.DataFrame:
    if notas.empty:
        return notas
    col_status = ""
    for c in notas.columns:
        n = c.lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            col_status = c
            break
    if not col_status:
        return notas
    s = _norm_txt(notas[col_status]).str.lower()
    invalidas = (
        s.str.contains("cancel", na=False)
        | s.str.contains("deneg", na=False)
        | s.str.contains("inutil", na=False)
    )
    return notas.loc[~invalidas].copy()


def _strip_header_ascii_lower(name: str) -> str:
    s = unicodedata.normalize("NFKD", str(name).strip()).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", s).strip()


def detectar_col_valor_total_liquido(columns: list[str]) -> str:
    for c in columns:
        if _strip_header_ascii_lower(c) == "valor total liquido":
            return c
    for c in columns:
        if "valor" in _strip_header_ascii_lower(c) and "liquido" in _strip_header_ascii_lower(c):
            return c
    return ""


def detectar_col_data_emissao(columns: list[str]) -> str:
    alvos = {
        "data de emissão",
        "data de emissao",
        "data emissão",
        "data emissao",
        "emissão",
        "emissao",
        "data de saida",
        "data saída",
    }
    norm = {c: str(c).strip().lower() for c in columns}
    for c, n in norm.items():
        if n in alvos:
            return c
    for c, n in norm.items():
        if "emiss" in n or "saida" in n or "saída" in n:
            return c
    return ""


def load_notas_saida_from_dir(notas_dir: Path) -> pd.DataFrame:
    """Concatena todos os CSV/XLSX sob ``notas_dir`` (rglob)."""
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
    _arq = "__arquivo_nota__"
    _dedup = [c for c in out.columns if c != _arq]
    if _dedup:
        out = out.drop_duplicates(subset=_dedup, keep="first")
    return out
