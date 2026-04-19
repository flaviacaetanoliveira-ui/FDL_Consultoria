"""Cobertura temporal do Parquet por empresa — cliente_2 faturamento."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

PARQUET = ROOT / "data_products" / "cliente_2" / "faturamento" / "current" / "dataset.parquet"


def main() -> None:
    if not PARQUET.is_file():
        print(f"ERRO: {PARQUET} não encontrado")
        sys.exit(1)

    df = pd.read_parquet(PARQUET)
    df["Data"] = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.loc[df["Data"].notna()].copy()
    df["mes"] = df["Data"].dt.to_period("M")

    rec_col = "Valor total" if "Valor total" in df.columns else None
    if rec_col is None:
        print("Coluna de receita não encontrada (Valor total).")
        sys.exit(1)

    print("Cobertura temporal por empresa (cliente_2 / faturamento):\n")
    if "empresa" not in df.columns:
        print("Sem coluna empresa.")
        return

    for empresa in sorted(df["empresa"].astype(str).unique()):
        df_e = df[df["empresa"].astype(str) == empresa]
        print(f"{empresa}:")
        print(f"  min data: {df_e['Data'].min()}")
        print(f"  max data: {df_e['Data'].max()}")
        meses = sorted(df_e["mes"].dropna().unique())
        print(f"  meses com dados ({len(meses)}): {meses[:8]}{'...' if len(meses) > 8 else ''}")
        receita_por_mes = df_e.groupby("mes", sort=True)[rec_col].apply(
            lambda s: pd.to_numeric(s, errors="coerce").fillna(0.0).sum()
        )
        abril_2026 = None
        for mes, v in receita_por_mes.items():
            if str(mes) == "2026-04":
                abril_2026 = float(v)
            print(f"    {mes}: R$ {float(v):,.2f}")
        if abril_2026 is not None:
            print(f"  >> abril/2026 receita: R$ {abril_2026:,.2f}")
        else:
            print("  >> abril/2026: sem linhas")
        print()


if __name__ == "__main__":
    main()
