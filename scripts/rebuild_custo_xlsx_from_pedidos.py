"""
Regenera Planilha1 (Código + PREÇO DE CUSTO com IPI) a partir de exports CSV de pedidos ML.

Útil quando um Custo.xlsx foi preenchido com valores placeholder (ex.: 1,00) e ainda não há
tabela de custo real: estima custo unitário = mediana do «Preço de lista» por SKU × fator
(omissão 0,45). Quando tiver custos reais, edite o XLSX ou substitua por export do ERP.

Exemplo (Antomóveis):
  python scripts/rebuild_custo_xlsx_from_pedidos.py ^
    --pedidos-dir "C:/.../cliente_1/Pedidos" ^
    --output "C:/.../cliente_1/Custo.xlsx" ^
    --ratio 0.45
"""
from __future__ import annotations

import argparse
import sys
import unicodedata
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from processing.faturamento.config import CUSTO_COL_PRECO, CUSTO_SHEET_NAME, CUSTO_SKU_COL
from processing.faturamento.normalize import to_numeric_br


def _find_sku_col(cols: list[str]) -> str | None:
    for c in cols:
        if "SKU" in c.upper():
            return c
    return None


def _norm_col(s: str) -> str:
    return unicodedata.normalize("NFKC", s).strip().casefold()


def _find_lista_col(cols: list[str]) -> str | None:
    """Coluna de preço de venda em lista (não confundir com «Nome - Lista de preço»)."""
    want = _norm_col("Preço de lista")
    for c in cols:
        if _norm_col(c) == want:
            return c
    return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Rebuild Custo.xlsx from pedidos CSVs (estimate from preço lista).")
    ap.add_argument("--pedidos-dir", type=Path, required=True, help="Pasta com subpastas/CSVs de pedidos ML")
    ap.add_argument("--output", type=Path, required=True, help="Caminho do Custo.xlsx a gravar")
    ap.add_argument(
        "--ratio",
        type=float,
        default=0.45,
        help="Multiplicador sobre a mediana do preço de lista (omissão 0,45)",
    )
    args = ap.parse_args()
    ped_root: Path = args.pedidos_dir.expanduser().resolve()
    out: Path = args.output.expanduser().resolve()
    ratio = float(args.ratio)
    if not ped_root.is_dir():
        print(f"Diretório de pedidos inexistente: {ped_root}", file=sys.stderr)
        return 1
    if ratio <= 0 or ratio > 1:
        print("--ratio deve estar em ]0, 1].", file=sys.stderr)
        return 1

    rows: list[pd.DataFrame] = []
    for p in sorted(ped_root.rglob("*.csv")):
        if not p.is_file():
            continue
        df = None
        # Exports ML em UTF-8: se ler como latin-1, «Preço» fica corrompido e não casa com o cabeçalho esperado.
        for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
            try:
                df = pd.read_csv(p, sep=";", encoding=enc, on_bad_lines="skip", low_memory=False)
                if _find_lista_col(list(df.columns)) and _find_sku_col(list(df.columns)):
                    break
                df = None
            except Exception:
                df = None
        if df is None or df.empty:
            continue
        sku_col = _find_sku_col(list(df.columns))
        if not sku_col:
            continue
        lista_col = _find_lista_col(list(df.columns))
        if not lista_col:
            sub = df[[sku_col]].copy()
            sub["_lista_num"] = float("nan")
        else:
            sub = df[[sku_col, lista_col]].copy()
            sub["_lista_num"] = to_numeric_br(sub[lista_col])
        sub = sub.rename(columns={sku_col: "_sku"})
        sub["_sku"] = sub["_sku"].astype(str).str.strip()
        sub = sub[sub["_sku"].ne("") & sub["_sku"].str.lower().ne("nan") & sub["_sku"].ne("-")]
        rows.append(sub[["_sku", "_lista_num"]])

    if not rows:
        print("Nenhum CSV de pedidos com coluna de SKU encontrado.", file=sys.stderr)
        return 1

    all_df = pd.concat(rows, ignore_index=True)
    g = all_df.groupby("_sku", as_index=False)["_lista_num"].median()
    g["custo"] = (g["_lista_num"] * ratio).round(2)
    # Sem preço de lista: não inventar 1,00 — deixa custo NaN (pipeline marcará sem custo)
    g.loc[g["_lista_num"].isna(), "custo"] = float("nan")
    g = g.dropna(subset=["custo"])

    out_df = pd.DataFrame({CUSTO_SKU_COL: g["_sku"].astype(str), CUSTO_COL_PRECO: g["custo"]})
    out.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(out, engine="openpyxl") as xw:
        out_df.to_excel(xw, sheet_name=CUSTO_SHEET_NAME, index=False)
    print(f"Gravado {out} com {len(out_df)} SKUs (ratio={ratio}).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
