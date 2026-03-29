from __future__ import annotations

import csv
import re
import sys
import unicodedata
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from fdl_paths import CLIENTE_BASE_DIR

PASTA_LIBERACOES = CLIENTE_BASE_DIR / "Liberações_ML"


def _strip_accents(s: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch)
    )


def normalize_col_name(name: object) -> str:
    s = "" if name is None else str(name)
    s = s.strip()
    s = _strip_accents(s).lower()
    s = re.sub(r"[\u00ba\u00b0\u00aa]", "o", s)
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _tokens(s: str) -> set[str]:
    return set(s.split())


def find_latest_file(folder: Path) -> Path:
    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")

    candidates: list[Path] = []
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        candidates.extend(p for p in folder.rglob(pattern) if p.is_file())

    if not candidates:
        raise FileNotFoundError(f"Nenhum arquivo CSV/Excel encontrado em: {folder}")

    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0]


def list_liberacoes_files(folder: Path) -> list[Path]:
    if not folder.exists():
        raise FileNotFoundError(f"Pasta não encontrada: {folder}")
    candidates: list[Path] = []
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        candidates.extend(p for p in folder.rglob(pattern) if p.is_file())
    candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates


def _score_header_row(row_values: Iterable[object]) -> int:
    score = 0
    norms = [normalize_col_name(v) for v in row_values]
    joined = " ".join(n for n in norms if n)
    t = _tokens(joined)

    if "external" in t and "reference" in t:
        score += 5
    if "order" in t and "id" in t:
        score += 4
    if "pack" in t and "id" in t:
        score += 4
    if ("data" in t and "pagamento" in t) or ("payment" in t and "date" in t):
        score += 4
    if ("valor" in t and "pago" in t) or ("paid" in t and "amount" in t):
        score += 4
    if "valor" in t or "amount" in t:
        score += 1

    return score


def detect_excel_header_row(path: Path, max_rows: int = 50) -> int:
    preview = pd.read_excel(path, header=None, nrows=max_rows, engine="openpyxl")
    best_idx = 0
    best_score = -1

    for i in range(len(preview)):
        score = _score_header_row(preview.iloc[i].tolist())
        if score > best_score:
            best_score = score
            best_idx = i

    return 0 if best_score <= 0 else int(best_idx)


def read_input_file(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()

    if ext in {".xlsx", ".xls"}:
        header_row = detect_excel_header_row(path)
        return pd.read_excel(path, header=header_row, engine="openpyxl")

    if ext == ".csv":
        last_err: Optional[Exception] = None
        best_df: Optional[pd.DataFrame] = None
        best_score = -1

        def score_columns(df: pd.DataFrame) -> int:
            if df is None or len(df.columns) == 0:
                return -1
            norm_cols = [normalize_col_name(c) for c in df.columns]
            joined = " ".join(norm_cols)
            score = min(len(df.columns), 50)
            if "external reference" in joined:
                score += 20
            if "order id" in joined:
                score += 15
            if "pack id" in joined:
                score += 15
            if "payment date" in joined or "data de pagamento" in joined:
                score += 15
            if "valor pago" in joined or "amount" in joined:
                score += 10
            return score

        for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
            for sep in (";", ",", "\t", "|"):
                try:
                    df_try = pd.read_csv(
                        path,
                        encoding=encoding,
                        sep=sep,
                        engine="python",
                        dtype=str,
                    )
                    score = score_columns(df_try)
                    if score > best_score:
                        best_score = score
                        best_df = df_try
                except Exception as e:  # noqa: BLE001
                    last_err = e
            # Fallback para CSV com aspas malformadas.
            try:
                df_try = pd.read_csv(
                    path,
                    encoding=encoding,
                    sep=";",
                    engine="python",
                    dtype=str,
                    on_bad_lines="skip",
                    quoting=csv.QUOTE_NONE,
                )
                score = score_columns(df_try)
                if score > best_score:
                    best_score = score
                    best_df = df_try
            except Exception as e:  # noqa: BLE001
                last_err = e

        if best_df is not None and best_score > 0:
            return best_df
        raise RuntimeError(f"Falha ao ler CSV: {path} ({last_err})")

    raise ValueError(f"Extensão não suportada: {path.suffix}")


def parse_brl_number(series: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce")

    s = series.astype(str).fillna("").str.strip()
    s = s.str.replace("\u00a0", " ", regex=False)
    s = s.str.replace("R$", "", regex=False)
    s = s.str.replace("BRL", "", regex=False)
    s = s.str.replace(" ", "", regex=False)
    s = s.str.replace(r"[^0-9,\.\-]", "", regex=True)

    has_comma = s.str.contains(",", regex=False)
    s = s.where(~has_comma, s.str.replace(".", "", regex=False))
    s = s.where(~has_comma, s.str.replace(",", ".", regex=False))
    return pd.to_numeric(s, errors="coerce")


@dataclass(frozen=True)
class DetectedColumns:
    external_reference: str
    order_id: str
    pack_id: str
    data_pagamento: str
    valor_pago: str
    record_type: str = ""
    description: str = ""
    net_debit_amount: str = ""


def _rank_columns(df: pd.DataFrame, score_fn) -> list[tuple[int, str]]:
    scored: list[tuple[int, str]] = []
    for col in df.columns:
        score = score_fn(normalize_col_name(col))
        if score > 0:
            scored.append((score, col))
    return sorted(scored, key=lambda x: (-x[0], str(x[1])))


def _pick_unique(ranked: list[tuple[int, str]], used: set[str]) -> str:
    for _, col in ranked:
        if col not in used:
            used.add(col)
            return col
    return ""


def detect_columns(df: pd.DataFrame) -> DetectedColumns:
    def score_external(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "external" in t:
            score += 4
        if "reference" in t:
            score += 4
        if "external" in t and "reference" in t:
            score += 3
        if n in {"external reference", "external reference id"}:
            score += 2
        return score

    def score_order(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "order" in t:
            score += 4
        if "id" in t:
            score += 2
        if "order" in t and "id" in t:
            score += 3
        if n in {"order id", "orderid", "id order"}:
            score += 2
        return score

    def score_pack(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "pack" in t:
            score += 4
        if "id" in t:
            score += 2
        if "pack" in t and "id" in t:
            score += 3
        if n in {"pack id", "packid", "id pack"}:
            score += 2
        return score

    def score_data_pagamento(n: str) -> int:
        t = _tokens(n)
        score = 0
        # Regra de negócio: a data de pagamento deve vir de DATE.
        if n == "date":
            score += 100
        if "data" in t and "pagamento" in t:
            score += 8
        if "payment" in t and "date" in t:
            score += 8
        if "transaction" in t and "approval" in t and "date" in t:
            score += 3
        if n in {"transaction approval date", "approved date", "approval date"}:
            score += 1
        if "data" in t:
            score += 1
        if "pagamento" in t or "payment" in t:
            score += 2
        if "date" in t:
            score += 1
        return score

    def score_valor_pago(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "valor" in t and "pago" in t:
            score += 8
        if "paid" in t and "amount" in t:
            score += 8
        if "total" in t and ("pago" in t or "paid" in t):
            score += 6
        if "amount" in t or "valor" in t:
            score += 2
        if "brl" in t:
            score += 1
        if n in {"seller amount", "net credit amount"}:
            score += 6
        if n in {"gross amount", "balance amount"}:
            score += 4
        return score

    def score_record_type(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "record" in t and "type" in t:
            score += 10
        if n in {"record type", "record_type", "tipo de registro", "tipo"}:
            score += 6
        return score

    def score_description(n: str) -> int:
        t = _tokens(n)
        score = 0
        if "description" in t:
            score += 10
        if "descricao" in t or "descricao" in t:
            score += 10
        if "natureza" in t:
            score += 6
        return score

    def score_net_debit_amount(n: str) -> int:
        t = _tokens(n)
        score = 0
        if n == "net debit amount":
            score += 100
        if "net" in t and "debit" in t and "amount" in t:
            score += 50
        if "debit" in t and "amount" in t:
            score += 20
        return score

    ranked_external = _rank_columns(df, score_external)
    ranked_order = _rank_columns(df, score_order)
    ranked_pack = _rank_columns(df, score_pack)
    ranked_data = _rank_columns(df, score_data_pagamento)
    ranked_valor = _rank_columns(df, score_valor_pago)
    ranked_record_type = _rank_columns(df, score_record_type)
    ranked_description = _rank_columns(df, score_description)
    ranked_net_debit = _rank_columns(df, score_net_debit_amount)

    used: set[str] = set()
    detected = DetectedColumns(
        external_reference=_pick_unique(ranked_external, used),
        order_id=_pick_unique(ranked_order, used),
        pack_id=_pick_unique(ranked_pack, used),
        data_pagamento=_pick_unique(ranked_data, used),
        valor_pago=_pick_unique(ranked_valor, used),
        record_type=_pick_unique(ranked_record_type, used),
        description=_pick_unique(ranked_description, used),
        net_debit_amount=_pick_unique(ranked_net_debit, used),
    )

    _optional_detected = frozenset({"net_debit_amount"})
    missing = [
        field
        for field, col_name in detected.__dict__.items()
        if field not in _optional_detected
        and (not col_name or col_name not in df.columns)
    ]
    if missing:
        raise KeyError(
            "Não consegui identificar todas as colunas de liberações. "
            f"Faltando: {missing}. Colunas encontradas: {list(df.columns)}"
        )

    return detected


def build_liberacoes(df_raw: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = df_raw.copy()
    df = df.dropna(axis=1, how="all")

    detected = detect_columns(df)

    cred = parse_brl_number(df[detected.valor_pago]).fillna(0)
    if detected.net_debit_amount and detected.net_debit_amount in df.columns:
        deb = parse_brl_number(df[detected.net_debit_amount]).fillna(0)
    else:
        deb = pd.Series(0.0, index=df.index, dtype="float64")

    cred_cent = (cred * 100).round()
    deb_cent = (deb * 100).round()
    net_cent = cred_cent - deb_cent
    cred = cred_cent / 100.0
    deb = deb_cent / 100.0
    net_cred_deb = net_cent / 100.0
    df["NET_CREDIT_AMOUNT"] = cred
    df["NET_DEBIT_AMOUNT"] = deb
    df["Valor pago líquido"] = net_cred_deb
    df["Valor pago"] = net_cred_deb

    df = df.rename(
        columns={
            detected.external_reference: "EXTERNAL_REFERENCE",
            detected.order_id: "ORDER_ID",
            detected.pack_id: "PACK_ID",
            detected.data_pagamento: "Data de pagamento",
        }
    )
    if detected.record_type and detected.record_type in df.columns:
        df = df.rename(columns={detected.record_type: "RECORD_TYPE"})
    if detected.description and detected.description in df.columns:
        df = df.rename(columns={detected.description: "DESCRIPTION"})

    # Tipos exigidos
    for col in ("EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID"):
        df[col] = df[col].fillna("").astype(str).str.strip()
    df["Data de pagamento"] = pd.to_datetime(
        df["Data de pagamento"], errors="coerce", dayfirst=True, format="mixed"
    )
    if "RECORD_TYPE" not in df.columns:
        df["RECORD_TYPE"] = pd.NA
    if "DESCRIPTION" not in df.columns:
        df["DESCRIPTION"] = pd.NA

    liberacoes_tratadas = df[
        [
            "EXTERNAL_REFERENCE",
            "ORDER_ID",
            "PACK_ID",
            "Data de pagamento",
            "NET_CREDIT_AMOUNT",
            "NET_DEBIT_AMOUNT",
            "Valor pago líquido",
            "Valor pago",
            "RECORD_TYPE",
            "DESCRIPTION",
        ]
    ].copy()

    # Remove chave vazia para agregação por referência externa.
    liberacoes_base = liberacoes_tratadas[
        liberacoes_tratadas["EXTERNAL_REFERENCE"].ne("")
    ].copy()

    liberacoes_agregadas = (
        liberacoes_base.groupby("EXTERNAL_REFERENCE", as_index=False, dropna=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .sort_values("Data de pagamento", kind="stable")
        .reset_index(drop=True)
    )
    liberacoes_agregadas["Valor pago"] = pd.to_numeric(
        liberacoes_agregadas["Valor pago"], errors="coerce"
    ).round(2)

    return liberacoes_tratadas, liberacoes_agregadas


def build_liberacoes_from_folder(folder: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    files = list_liberacoes_files(folder)
    if not files:
        raise FileNotFoundError(f"Nenhum arquivo CSV/Excel encontrado em: {folder}")

    tratadas_por_arquivo: list[pd.DataFrame] = []
    diagnostico: list[dict[str, object]] = []
    for path in files:
        df_raw = read_input_file(path)
        diagnostico.append(
            {
                "Arquivo": path.name,
                "Caminho": str(path),
                "Linhas brutas": int(len(df_raw)),
            }
        )
        liberacoes_tratadas, _ = build_liberacoes(df_raw)
        tratadas_por_arquivo.append(liberacoes_tratadas)

    liberacoes_tratadas_all = pd.concat(tratadas_por_arquivo, ignore_index=True)
    base = liberacoes_tratadas_all[
        liberacoes_tratadas_all["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip().ne("")
    ].copy()
    liberacoes_agregadas = (
        base.groupby("EXTERNAL_REFERENCE", as_index=False, dropna=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .sort_values("Data de pagamento", kind="stable")
        .reset_index(drop=True)
    )
    liberacoes_agregadas["Valor pago"] = pd.to_numeric(
        liberacoes_agregadas["Valor pago"], errors="coerce"
    ).round(2)

    return liberacoes_tratadas_all, liberacoes_agregadas, pd.DataFrame(diagnostico)


def main() -> int:
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

    latest = find_latest_file(PASTA_LIBERACOES)
    df_raw = read_input_file(latest)
    liberacoes_tratadas, liberacoes_agregadas = build_liberacoes(df_raw)

    print(f"Arquivo mais recente: {latest}")
    print("\nHead (liberacoes_tratadas):")
    print(liberacoes_tratadas.head(10).to_string(index=False))
    print("\nHead (liberacoes_agregadas):")
    print(liberacoes_agregadas.head(10).to_string(index=False))

    qtd_tratadas = int(len(liberacoes_tratadas))
    qtd_agregadas = int(len(liberacoes_agregadas))
    soma_total = float(pd.to_numeric(liberacoes_agregadas["Valor pago"], errors="coerce").sum())

    print("\nQuantidade de linhas (liberacoes_tratadas):", qtd_tratadas)
    print("Quantidade de linhas (liberacoes_agregadas):", qtd_agregadas)
    print("Soma total de Valor pago:", soma_total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

