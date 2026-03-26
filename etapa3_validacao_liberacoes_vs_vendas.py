from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def build_validacao_liberacoes_vs_vendas(
    base_dir: str | Path,
) -> pd.DataFrame:
    vendas_tratadas, _, liberacoes_agregadas, _ = carregar_bases_consolidadas(base_dir)

    validacao = liberacoes_agregadas.merge(
        vendas_tratadas[["N° de venda", "Total BRL"]],
        how="left",
        left_on="EXTERNAL_REFERENCE",
        right_on="N° de venda",
    )

    validacao_liberacoes_vs_vendas = validacao[
        ["EXTERNAL_REFERENCE", "Valor pago", "Data de pagamento", "N° de venda", "Total BRL"]
    ].copy()
    validacao_liberacoes_vs_vendas["Tem venda"] = validacao_liberacoes_vs_vendas[
        "N° de venda"
    ].notna().map({True: "Sim", False: "Não"})
    return validacao_liberacoes_vs_vendas


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    validacao_liberacoes_vs_vendas = build_validacao_liberacoes_vs_vendas(BASE_DIR)

    total_liberacoes = int(len(validacao_liberacoes_vs_vendas))
    com_venda = int((validacao_liberacoes_vs_vendas["Tem venda"] == "Sim").sum())
    sem_venda = int((validacao_liberacoes_vs_vendas["Tem venda"] == "Não").sum())
    cobertura = (com_venda / total_liberacoes * 100.0) if total_liberacoes else 0.0

    print("Head (validacao_liberacoes_vs_vendas):")
    print(validacao_liberacoes_vs_vendas.head(10).to_string(index=False))

    print("\nTotal de liberações:", total_liberacoes)
    print("Liberações com venda:", com_venda)
    print("Liberações sem venda:", sem_venda)
    print("Percentual de cobertura:", round(cobertura, 2), "%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

