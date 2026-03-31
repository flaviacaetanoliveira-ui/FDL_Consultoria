from __future__ import annotations

import os
import re
import sys
import unicodedata
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from fdl_paths import CLIENTE_BASE_DIR, resolve_pasta_vendas_ml

PASTA_VENDAS = resolve_pasta_vendas_ml(CLIENTE_BASE_DIR)


def _strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch)
    )


def normalize_col_name(name: object) -> str:
    s = "" if name is None else str(name)
    s = s.strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"[\u00ba\u00b0\u00aa]", "o", s)  # º ° ª -> o (aproximações úteis)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokens(s: str) -> set[str]:
    return set(s.split())


def find_latest_sales_file(folder: Path) -> Path:
    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")

    patterns = ["*.xlsx", "*.xls", "*.csv"]
    candidates: list[Path] = []
    for pat in patterns:
        candidates.extend(p for p in folder.rglob(pat) if p.is_file())

    if not candidates:
        raise FileNotFoundError(f"Nenhum arquivo CSV/Excel encontrado em: {folder}")

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def list_sales_files(folder: Path) -> list[Path]:
    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")
    candidates: list[Path] = []
    for pat in ("*.xlsx", "*.xls", "*.csv"):
        candidates.extend(p for p in folder.rglob(pat) if p.is_file())
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates


@dataclass(frozen=True)
class DetectedColumns:
    sale_col: str
    total_col: str


def detect_columns(df: pd.DataFrame) -> DetectedColumns:
    norm_map: dict[str, str] = {c: normalize_col_name(c) for c in df.columns}

    sale_candidates: list[tuple[int, str]] = []
    total_candidates: list[tuple[int, str]] = []

    for original, norm in norm_map.items():
        t = _tokens(norm)

        sale_score = 0
        if "venda" in t or "vendas" in t:
            sale_score += 3
        if any(x in t for x in {"n", "no", "numero", "nro"}):
            sale_score += 2
        if norm in {"n de venda", "no de venda", "numero de venda"}:
            sale_score += 4
        if "id" in t and ("venda" in t or "vendas" in t):
            sale_score += 1
        if sale_score:
            sale_candidates.append((sale_score, original))

        total_score = 0
        if "total" in t:
            total_score += 3
        if "brl" in t:
            total_score += 2
        if any(x in t for x in {"valor", "valor_total", "valor total"}):
            total_score += 1
        if norm in {"total brl", "total br", "total brl r", "total"}:
            total_score += 1
        if total_score:
            total_candidates.append((total_score, original))

    if not sale_candidates:
        raise KeyError(
            "Não consegui identificar a coluna de venda (ex.: 'N° de venda'). "
            f"Colunas encontradas: {list(df.columns)}"
        )
    if not total_candidates:
        raise KeyError(
            "Não consegui identificar a coluna de total (ex.: 'Total (BRL)'). "
            f"Colunas encontradas: {list(df.columns)}"
        )

    sale_col = sorted(sale_candidates, key=lambda x: (-x[0], str(x[1])))[0][1]
    total_col = sorted(total_candidates, key=lambda x: (-x[0], str(x[1])))[0][1]
    return DetectedColumns(sale_col=sale_col, total_col=total_col)


def _score_header_row(row_values: Iterable[object]) -> int:
    score = 0
    norms = [normalize_col_name(v) for v in row_values]
    joined = " ".join(n for n in norms if n)
    t = _tokens(joined)

    if "venda" in t or "vendas" in t:
        score += 3
    if "total" in t:
        score += 3
    if "brl" in t:
        score += 2
    if any(x in t for x in {"n", "no", "numero", "nro"}):
        score += 2

    # bônus se houver uma célula parecida especificamente com "n de venda"
    if any(n in {"n de venda", "no de venda", "numero de venda"} for n in norms):
        score += 3

    return score


def detect_excel_header_row(path: Path, max_rows: int = 40) -> int:
    preview = pd.read_excel(path, header=None, nrows=max_rows, engine="openpyxl")
    best_idx = 0
    best_score = -1

    for i in range(len(preview)):
        score = _score_header_row(preview.iloc[i].tolist())
        if score > best_score:
            best_score = score
            best_idx = i

    if best_score <= 0:
        return 0
    return int(best_idx)


def read_sales_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()

    if ext in {".xlsx", ".xls"}:
        header_row = detect_excel_header_row(path)
        df = pd.read_excel(path, header=header_row, engine="openpyxl")
        return df

    if ext == ".csv":
        last_err: Optional[Exception] = None
        for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
            try:
                df = pd.read_csv(
                    path,
                    encoding=encoding,
                    sep=None,
                    engine="python",
                    dtype=str,
                )
                return df
            except Exception as e:  # noqa: BLE001
                last_err = e
        raise RuntimeError(f"Falha ao ler CSV: {path} ({last_err})")

    raise ValueError(f"Extensão não suportada: {path.suffix}")


def parse_brl_number(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    s = series.astype(str).fillna("")
    s = s.str.strip()
    s = s.str.replace("\u00a0", " ", regex=False)
    s = s.str.replace("R$", "", regex=False)
    s = s.str.replace("BRL", "", regex=False)
    s = s.str.replace(" ", "", regex=False)

    # mantém apenas dígitos, separadores e sinal
    s = s.str.replace(r"[^0-9,\.\-]", "", regex=True)

    # Heurística pt-BR: se tem vírgula, ela é decimal; ponto vira milhar
    has_comma = s.str.contains(",", regex=False)
    s = s.where(~has_comma, s.str.replace(".", "", regex=False))
    s = s.where(~has_comma, s.str.replace(",", ".", regex=False))

    return pd.to_numeric(s, errors="coerce")


def build_vendas_tratadas(df_raw: pd.DataFrame) -> pd.DataFrame:
    # remove colunas totalmente vazias (muito comum em Excel)
    df = df_raw.copy()
    df = df.dropna(axis=1, how="all")

    detected = detect_columns(df)

    df = df.rename(
        columns={
            detected.sale_col: "N° de venda",
            detected.total_col: "Total (BRL)",
        }
    )

    df["N° de venda"] = df["N° de venda"].astype(str).fillna("").str.strip()
    df["Total (BRL)"] = parse_brl_number(df["Total (BRL)"])

    # remove linhas sem número de venda
    df = df[df["N° de venda"].ne("")].copy()

    vendas_tratadas = (
        df.groupby("N° de venda", as_index=False, dropna=False)["Total (BRL)"]
        .sum(min_count=1)
        .rename(columns={"Total (BRL)": "Total BRL"})
    )

    return vendas_tratadas


def build_vendas_tratadas_from_folder(folder: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    files = list_sales_files(folder)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo CSV/Excel encontrado em: {folder}")

    tratados_por_arquivo: list[pd.DataFrame] = []
    diagnostico: list[dict[str, object]] = []

    for path in files:
        df_raw = read_sales_file(path)
        diag_item: dict[str, object] = {
            "Arquivo": path.name,
            "Caminho": str(path),
            "Linhas brutas": int(len(df_raw)),
        }
        try:
            df_tratado = build_vendas_tratadas(df_raw)
        except KeyError as exc:
            # Alguns ficheiros podem vir corrompidos ou sem cabeçalho válido; manter os demais.
            diag_item["Ignorado"] = True
            diag_item["Motivo"] = str(exc)
            diagnostico.append(diag_item)
            continue

        diag_item["Ignorado"] = False
        diag_item["Linhas tratadas"] = int(len(df_tratado))
        diagnostico.append(diag_item)
        tratados_por_arquivo.append(df_tratado)

    if not tratados_por_arquivo:
        raise ValueError(
            "Nenhum arquivo de vendas válido encontrado após leitura da pasta. "
            "Verifique cabeçalhos/colunas dos arquivos de vendas."
        )

    consolidado = pd.concat(tratados_por_arquivo, ignore_index=True)
    vendas_tratadas = (
        consolidado.groupby("N° de venda", as_index=False)["Total BRL"]
        .sum(min_count=1)
        .reset_index(drop=True)
    )
    return vendas_tratadas, pd.DataFrame(diagnostico)


def main() -> int:
    # Evita "�" no PowerShell quando houver caracteres como "°".
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    warnings.filterwarnings(
        "ignore",
        message="Workbook contains no default style, apply openpyxl's default",
        category=UserWarning,
    )

    latest = find_latest_sales_file(PASTA_VENDAS)
    df_raw = read_sales_file(latest)
    vendas_tratadas = build_vendas_tratadas(df_raw)

    print(f"Arquivo mais recente: {latest}")
    print("\nHead (vendas_tratadas):")
    print(vendas_tratadas.head(10).to_string(index=False))

    qtd = int(vendas_tratadas["N° de venda"].nunique(dropna=False))
    soma = float(pd.to_numeric(vendas_tratadas["Total BRL"], errors="coerce").sum())

    print("\nQuantidade de vendas:", qtd)
    print("Soma total de Total BRL:", soma)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

