from __future__ import annotations

import pandas as pd

from etapa2_liberacoes import (
    PASTA_LIBERACOES,
    detect_columns,
    find_latest_file,
    read_input_file,
)


def main() -> int:
    latest = find_latest_file(PASTA_LIBERACOES)
    df_raw = read_input_file(latest)
    df = df_raw.dropna(axis=1, how="all").copy()

    detected = detect_columns(df)
    original_col = detected.data_pagamento

    before = df[original_col].head(10).tolist()
    after = pd.to_datetime(df[original_col], errors="coerce", dayfirst=True, format="mixed")
    after_10 = after.head(10).tolist()

    print(f"Arquivo: {latest}")
    print("Coluna usada como `Data de pagamento`: Data de pagamento")
    print(f"Nome original da coluna no arquivo: {original_col}")
    print("\n10 valores antes da conversão:")
    for i, v in enumerate(before, 1):
        print(f"{i:02d}. {v}")
    print("\n10 valores depois da conversão:")
    for i, v in enumerate(after_10, 1):
        print(f"{i:02d}. {v}")
    print(f"\nTipo final: {after.dtype}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

