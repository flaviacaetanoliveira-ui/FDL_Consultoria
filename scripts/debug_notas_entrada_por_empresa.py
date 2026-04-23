"""
Auditar quais pastas de notas de entrada estão sendo lidas por empresa,
quantos arquivos cada uma tem, e quantas linhas de devolução resultam após filtros.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from processing.faturamento.io_notas_entrada import (
    NATUREZAS_DEVOLUCAO,
    SITUACOES_DEVOLUCAO_VALIDAS,
    _detect_col_natureza,
    _detect_col_situacao,
    _norm_txt,
    load_notas_entrada_devolucoes_from_dir,
)
from processing.faturamento.io_notas_saida import _read_notas_file
from processing.faturamento.params_regime import load_faturamento_params_for_ui

_FAT_PATH = "data_products/cliente_2/faturamento/current/dataset_faturamento_app.csv"


def _load_info() -> dict[str, object]:
    return {
        "cliente_slug": "cliente_2",
        "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
        "faturamento_path_final_resolved": _FAT_PATH,
    }


def _read_raw_notas_dir(notas_dir: Path) -> pd.DataFrame:
    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in notas_dir.rglob(ptn) if p.is_file())
    if not files:
        return pd.DataFrame()
    parts = []
    for fp in sorted(files):
        try:
            part = _read_notas_file(fp).dropna(axis=1, how="all").copy()
            part["__arquivo_nota__"] = fp.name
            parts.append(part)
        except Exception:
            continue
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def _print_prefilter_info(df_raw: pd.DataFrame) -> None:
    if df_raw.empty:
        print("  Pré-filtro: sem linhas lidas")
        return
    print(f"  Pré-filtro total de linhas: {len(df_raw)}")
    cols = list(df_raw.columns)
    col_nat = _detect_col_natureza(cols)
    col_sit = _detect_col_situacao(cols)
    print(f"  Coluna natureza detectada: {col_nat or 'N/A'}")
    print(f"  Coluna situação detectada: {col_sit or 'N/A'}")
    if col_nat:
        nvals = _norm_txt(df_raw[col_nat]).value_counts().head(20).to_dict()
        print(f"  Naturezas brutas (top 20): {nvals}")
    if col_sit:
        svals = _norm_txt(df_raw[col_sit]).value_counts().head(20).to_dict()
        print(f"  Situações brutas (top 20): {svals}")


def main() -> None:
    params = load_faturamento_params_for_ui(load_info=_load_info())
    if params is None:
        print("❌ params não carregado")
        return

    print(f"NATUREZAS_DEVOLUCAO aceitas: {NATUREZAS_DEVOLUCAO}")
    print(f"SITUACOES_DEVOLUCAO_VALIDAS: {SITUACOES_DEVOLUCAO_VALIDAS}")
    print(f"\n{'=' * 80}\n")

    for empresa in params.empresas:
        print(f"### EMPRESA: {empresa.empresa} ({empresa.org_id})")

        notas_dir = getattr(empresa, "notas_entrada_dir", None)
        print(f"  notas_entrada_dir: {notas_dir}")

        if not notas_dir:
            print("  ❌ Sem notas_entrada_dir configurado")
            print()
            continue

        dir_path = (params.cliente_root / str(notas_dir)).resolve()
        print(f"  Pasta resolvida: {dir_path}")
        print(f"  Pasta existe: {dir_path.exists()}")

        if not dir_path.exists():
            print("  ❌ Pasta não encontrada no filesystem")
            print()
            continue

        arquivos_csv = list(dir_path.rglob("*.csv"))
        arquivos_xlsx = list(dir_path.rglob("*.xlsx"))
        arquivos_xls = list(dir_path.rglob("*.xls"))
        print(f"  Arquivos CSV: {len(arquivos_csv)}")
        print(f"  Arquivos XLSX: {len(arquivos_xlsx)}")
        print(f"  Arquivos XLS: {len(arquivos_xls)}")

        if not arquivos_csv and not arquivos_xlsx and not arquivos_xls:
            print("  ⚠️ Pasta vazia")
            print()
            continue

        try:
            df_raw = _read_raw_notas_dir(dir_path)
            _print_prefilter_info(df_raw)
            df_dev = load_notas_entrada_devolucoes_from_dir(dir_path)
            print(f"  Linhas após filtros (natureza + situação): {len(df_dev)}")

            if len(df_dev) > 0:
                if "Nota_Data_Emissao" in df_dev.columns:
                    print(f"  Primeira data: {df_dev['Nota_Data_Emissao'].min()}")
                    print(f"  Última data: {df_dev['Nota_Data_Emissao'].max()}")
                if "Natureza" in df_dev.columns:
                    print(f"  Naturezas encontradas: {df_dev['Natureza'].value_counts().to_dict()}")
                if "Nota_Situacao" in df_dev.columns:
                    print(f"  Situações encontradas: {df_dev['Nota_Situacao'].value_counts().to_dict()}")
            else:
                print(
                    "  ⚠️ Zero linhas após filtros — verificar variações de natureza/situação "
                    "nas amostras de pré-filtro acima"
                )
        except Exception as exc:
            print(f"  ❌ ERRO ao ler: {type(exc).__name__}: {exc}")

        print()

    print(f"{'=' * 80}\n")


if __name__ == "__main__":
    main()
