from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from etapa1_vendas import PASTA_VENDAS, build_vendas_tratadas_from_folder
from etapa2_liberacoes import PASTA_LIBERACOES, build_liberacoes_from_folder


def _print_diagnostico(titulo: str, df_diag: pd.DataFrame) -> None:
    print(f"\n{titulo}")
    print(f"Arquivos lidos: {len(df_diag)}")
    for _, row in df_diag.iterrows():
        print(f"- Nome: {row['Arquivo']}")
        print(f"  Caminho: {row['Caminho']}")
        print(f"  Linhas: {int(row['Linhas brutas'])}")


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    vendas_tratadas, diag_vendas = build_vendas_tratadas_from_folder(PASTA_VENDAS)
    liberacoes_tratadas, liberacoes_agregadas, diag_liberacoes = build_liberacoes_from_folder(
        PASTA_LIBERACOES
    )

    _print_diagnostico("[1] VENDAS - MERCADO LIVRE", diag_vendas)
    _print_diagnostico("[2] LIBERAÇÕES_ML", diag_liberacoes)

    print("\n[3] TOTAIS CONSOLIDADOS")
    print(f"Total consolidado de vendas_tratadas: {len(vendas_tratadas)}")
    print(f"Total consolidado de liberacoes_tratadas: {len(liberacoes_tratadas)}")
    print(f"Total consolidado de liberacoes_agregadas: {len(liberacoes_agregadas)}")

    print("\nHead (vendas_tratadas):")
    print(vendas_tratadas.head(10).to_string(index=False))

    print("\nHead (liberacoes_agregadas):")
    print(liberacoes_agregadas.head(10).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

