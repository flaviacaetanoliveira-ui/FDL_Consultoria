"""
Valida distribuição temporal das devoluções após repopulamento
dos arquivos de janeiro a abril/2026.

Objetivo: confirmar que as 4 empresas têm devoluções distribuídas
pelos 4 meses de 2026, não concentradas em abril.
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
    df = df.copy()
    df["Nota_Data_Emissao"] = pd.to_datetime(df["Nota_Data_Emissao"], errors="coerce")

    print(f"Total no parquet: {len(df)} linhas")
    print(f"Data min: {df['Nota_Data_Emissao'].min()}")
    print(f"Data max: {df['Nota_Data_Emissao'].max()}\n")

    df["ano"] = df["Nota_Data_Emissao"].dt.year
    print("Por ano:")
    print(df.groupby("ano").size().to_string())
    print()

    df_2026 = df.loc[df["ano"] == 2026].copy()
    if len(df_2026) == 0:
        print("❌ Zero registros em 2026 — arquivos não foram lidos")
        return

    df_2026["mes"] = df_2026["Nota_Data_Emissao"].dt.month
    print(f"2026 total: {len(df_2026)} linhas\n")

    print("Distribuição 2026 por empresa x mês:")
    pivot = df_2026.pivot_table(
        index="empresa",
        columns="mes",
        values="Nota_Numero_Normalizado",
        aggfunc="count",
        fill_value=0,
    )
    print(pivot.to_string())
    print()

    print("=== Período 01/01/2026 a 17/04/2026 ===")
    mask = (df["Nota_Data_Emissao"] >= pd.Timestamp("2026-01-01")) & (
        df["Nota_Data_Emissao"] <= pd.Timestamp("2026-04-17 23:59:59")
    )
    df_periodo = df.loc[mask]

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
    print(f"\nTotal NFs no período: {nfs_sum}")
    print(f"Valor total devolvido no período: R$ {valor_sum:,.2f}")


if __name__ == "__main__":
    main()
