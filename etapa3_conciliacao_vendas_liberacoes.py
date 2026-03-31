from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from etapa1_vendas import build_vendas_tratadas, find_latest_sales_file, read_sales_file
from etapa2_liberacoes import (
    PASTA_LIBERACOES,
    build_liberacoes,
    find_latest_file,
    read_input_file,
)

from fdl_paths import CLIENTE_BASE_DIR, resolve_pasta_vendas_ml

PASTA_VENDAS = resolve_pasta_vendas_ml(CLIENTE_BASE_DIR)


def classificar_status_financeiro(df: pd.DataFrame, tolerancia: float = 0.01) -> pd.Series:
    valor_pago = pd.to_numeric(df["Valor pago"], errors="coerce")
    total_brl = pd.to_numeric(df["Total BRL"], errors="coerce")
    diff_abs = (total_brl - valor_pago).abs()

    status = pd.Series("Pago a maior", index=df.index, dtype="object")
    status[(valor_pago.isna()) | (valor_pago <= 0)] = "Sem pagamento"
    status[(valor_pago > 0) & (diff_abs <= tolerancia)] = "Pago correto"
    status[(valor_pago > 0) & (valor_pago < total_brl) & (diff_abs > tolerancia)] = "Pago a menor"
    return status


def build_conciliacao() -> pd.DataFrame:
    # ETAPA 1
    arq_vendas = find_latest_sales_file(PASTA_VENDAS)
    vendas_raw = read_sales_file(arq_vendas)
    vendas_tratadas = build_vendas_tratadas(vendas_raw)

    # ETAPA 2 (somente tabela agregada)
    arq_liberacoes = find_latest_file(PASTA_LIBERACOES)
    liberacoes_raw = read_input_file(arq_liberacoes)
    _, liberacoes_agregadas = build_liberacoes(liberacoes_raw)

    # Conciliação solicitada: base vendas + LEFT JOIN por N° de venda = EXTERNAL_REFERENCE
    conciliacao = vendas_tratadas.merge(
        liberacoes_agregadas,
        how="left",
        left_on="N° de venda",
        right_on="EXTERNAL_REFERENCE",
    )

    conciliacao_vendas_liberacoes = conciliacao[
        ["N° de venda", "Total BRL", "Data de pagamento", "Valor pago"]
    ].copy()

    conciliacao_vendas_liberacoes["Tem pagamento"] = (
        conciliacao_vendas_liberacoes["Valor pago"].notna()
        & (conciliacao_vendas_liberacoes["Valor pago"] > 0)
    ).map({True: "Sim", False: "Não"})

    conciliacao_vendas_liberacoes["Diferença"] = (
        conciliacao_vendas_liberacoes["Total BRL"]
        - conciliacao_vendas_liberacoes["Valor pago"]
    )
    # Se Valor pago estiver vazio, manter nulo
    conciliacao_vendas_liberacoes.loc[
        conciliacao_vendas_liberacoes["Valor pago"].isna(), "Diferença"
    ] = pd.NA

    conciliacao_vendas_liberacoes["Status financeiro"] = classificar_status_financeiro(
        conciliacao_vendas_liberacoes
    )

    return conciliacao_vendas_liberacoes


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conciliacao_vendas_liberacoes = build_conciliacao()

    total_vendas = int(len(conciliacao_vendas_liberacoes))
    vendas_com_pagamento = int((conciliacao_vendas_liberacoes["Tem pagamento"] == "Sim").sum())
    vendas_sem_pagamento = int((conciliacao_vendas_liberacoes["Tem pagamento"] == "Não").sum())
    soma_total_brl = float(
        pd.to_numeric(conciliacao_vendas_liberacoes["Total BRL"], errors="coerce").sum()
    )
    soma_valor_pago = float(
        pd.to_numeric(conciliacao_vendas_liberacoes["Valor pago"], errors="coerce").sum()
    )
    perc_com_pagamento = (vendas_com_pagamento / total_vendas * 100.0) if total_vendas else 0.0

    print("Head (conciliacao_vendas_liberacoes):")
    print(conciliacao_vendas_liberacoes.head(10).to_string(index=False))
    print("\nTotal de vendas:", total_vendas)
    print("Vendas com pagamento:", vendas_com_pagamento)
    print("Vendas sem pagamento:", vendas_sem_pagamento)
    print("Soma de Total BRL:", soma_total_brl)
    print("Soma de Valor pago:", soma_valor_pago)
    print("Percentual de vendas com pagamento:", round(perc_com_pagamento, 2), "%")

    ordem_status = ["Sem pagamento", "Pago correto", "Pago a menor", "Pago a maior"]
    analise = (
        conciliacao_vendas_liberacoes.groupby("Status financeiro", as_index=False)
        .agg({"N° de venda": "count", "Total BRL": "sum", "Valor pago": "sum"})
        .rename(columns={"N° de venda": "Quantidade"})
    )
    analise = analise.set_index("Status financeiro").reindex(ordem_status)
    analise["Quantidade"] = analise["Quantidade"].fillna(0).astype(int)
    analise["Total BRL"] = analise["Total BRL"].fillna(0.0)
    analise["Valor pago"] = analise["Valor pago"].fillna(0.0)
    analise["Percentual"] = (
        (analise["Quantidade"] / total_vendas * 100.0) if total_vendas else 0.0
    )
    analise = analise.reset_index()

    print("\nAnálise financeira por status:")
    print(analise.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

