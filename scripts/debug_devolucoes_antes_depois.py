"""
Compara distribuição de devoluções por empresa antes e depois do fix.
Roda após rematerialização.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd


def main() -> None:
    parquet_path = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_devolucoes.parquet")
    if not parquet_path.is_file():
        print(f"ERRO: não encontrado: {parquet_path}")
        return

    df = pd.read_parquet(parquet_path, engine="pyarrow")

    print(f"Parquet rematerializado: {len(df)} linhas totais\n")

    df = df.copy()
    df["Nota_Data_Emissao"] = pd.to_datetime(df["Nota_Data_Emissao"], errors="coerce")
    mask = (df["Nota_Data_Emissao"] >= pd.Timestamp("2026-01-01")) & (
        df["Nota_Data_Emissao"] <= pd.Timestamp("2026-04-17 23:59:59")
    )
    df_periodo = df.loc[mask]

    print(f"Devoluções no período 01/01 a 17/04/2026: {len(df_periodo)} linhas\n")

    print("Distribuição por empresa:")
    por_empresa = (
        df_periodo.groupby("empresa", dropna=False)
        .agg(
            nfs=("Nota_Numero_Normalizado", "count"),
            valor_total=("Valor_Liquido_Devolucao", "sum"),
        )
        .reset_index()
    )

    print(por_empresa.to_string(index=False))

    nfs_sum = int(por_empresa["nfs"].sum())
    valor_sum = float(por_empresa["valor_total"].sum())
    print(f"\nTotal de NFs de devolução no período: {nfs_sum}")
    print(f"Valor total devolvido: R$ {valor_sum:,.2f}")

    print("\n=== ANTES DO FIX (referência relatório cliente): ===")
    print("  22 NFs · R$ 3.330,19")
    print("\n=== DEPOIS DO FIX (real): ===")
    print(f"  {nfs_sum} NFs · R$ {valor_sum:,.2f}")


if __name__ == "__main__":
    main()
