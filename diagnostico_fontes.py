from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from etapa1_vendas import (
    build_vendas_tratadas,
    find_latest_sales_file,
    read_sales_file,
)
from etapa2_liberacoes import (
    PASTA_LIBERACOES,
    build_liberacoes,
    find_latest_file,
    read_input_file,
)

from fdl_paths import CLIENTE_BASE_DIR, resolve_pasta_vendas_ml

PASTA_VENDAS = resolve_pasta_vendas_ml(CLIENTE_BASE_DIR)


def list_files(folder: Path) -> list[Path]:
    files: list[Path] = []
    for pattern in ("*.xlsx", "*.xls", "*.csv"):
        files.extend(p for p in folder.glob(pattern) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def count_rows_vendas(path: Path) -> int:
    df = read_sales_file(path)
    return int(len(df))


def count_rows_liberacoes(path: Path) -> int:
    df = read_input_file(path)
    return int(len(df))


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    arquivos_vendas = list_files(PASTA_VENDAS)
    arquivos_liberacoes = list_files(PASTA_LIBERACOES)

    arquivo_vendas_lido = find_latest_sales_file(PASTA_VENDAS)
    arquivo_liberacoes_lido = find_latest_file(PASTA_LIBERACOES)

    print("=== DIAGNÓSTICO DAS FONTES ===")
    print("\n[1] VENDAS")
    print(f"Total de arquivos encontrados: {len(arquivos_vendas)}")
    for p in arquivos_vendas:
        linhas = count_rows_vendas(p)
        flag = " (LIDO NO PROCESSAMENTO ATUAL)" if p == arquivo_vendas_lido else ""
        print(f"- Nome: {p.name}{flag}")
        print(f"  Caminho: {p}")
        print(f"  Linhas: {linhas}")

    print("\n[2] LIBERAÇÕES")
    print(f"Total de arquivos encontrados: {len(arquivos_liberacoes)}")
    for p in arquivos_liberacoes:
        linhas = count_rows_liberacoes(p)
        flag = " (LIDO NO PROCESSAMENTO ATUAL)" if p == arquivo_liberacoes_lido else ""
        print(f"- Nome: {p.name}{flag}")
        print(f"  Caminho: {p}")
        print(f"  Linhas: {linhas}")

    # Consolidação com a lógica atual (apenas arquivo mais recente de cada fonte)
    vendas_tratadas = build_vendas_tratadas(read_sales_file(arquivo_vendas_lido))
    _, liberacoes_agregadas = build_liberacoes(read_input_file(arquivo_liberacoes_lido))
    conciliacao = vendas_tratadas.merge(
        liberacoes_agregadas,
        how="left",
        left_on="N° de venda",
        right_on="EXTERNAL_REFERENCE",
    )

    print("\n[3] CONSOLIDAÇÃO (LÓGICA ATUAL)")
    print(f"Arquivo de vendas usado: {arquivo_vendas_lido.name}")
    print(f"Arquivo de liberações usado: {arquivo_liberacoes_lido.name}")
    print(f"Linhas em vendas_tratadas: {len(vendas_tratadas)}")
    print(f"Linhas em liberacoes_agregadas: {len(liberacoes_agregadas)}")
    print(f"Linhas em conciliacao_vendas_liberacoes: {len(conciliacao)}")

    print("\n[4] RESPOSTA DIRETA")
    print("- O processamento ATUAL usa apenas o arquivo mais recente de vendas.")
    print("- O processamento ATUAL usa apenas o arquivo mais recente de liberações.")
    print("- Os demais arquivos são encontrados no diagnóstico, mas não entram no cálculo atual.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

