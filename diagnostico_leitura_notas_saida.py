from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

from integracao_notas_pedidos import PASTA_NOTAS


def _list_files(folder: Path) -> list[Path]:
    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in folder.glob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _read_notas(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    last_err = None
    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t", "|"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", dtype=str)
            except Exception as e:  # noqa: BLE001
                last_err = e
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                sep=";",
                engine="python",
                dtype=str,
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
            )
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"Falha ao ler {path} ({last_err})")


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    arquivos = _list_files(PASTA_NOTAS)
    partes: list[pd.DataFrame] = []
    diag: list[dict[str, object]] = []

    for f in arquivos:
        df = _read_notas(f).dropna(axis=1, how="all").copy()
        df["__arquivo__"] = f.name
        partes.append(df)
        diag.append(
            {
                "Arquivo": f.name,
                "Caminho": str(f),
                "Linhas lidas": int(len(df)),
            }
        )

    total_encontrados = len(arquivos)
    total_lidos = len(diag)
    consolidada = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()

    print("=== DIAGNÓSTICO DE LEITURA — NOTAS FISCAIS DE SAÍDA ===")
    print("\n[1] Arquivos encontrados na pasta:")
    for d in diag:
        print(f"- Nome: {d['Arquivo']}")
        print(f"  Caminho: {d['Caminho']}")
        print(f"  Linhas lidas: {d['Linhas lidas']}")

    print("\n[2] Totais")
    print(f"- Total de arquivos encontrados: {total_encontrados}")
    print(f"- Total de arquivos efetivamente lidos: {total_lidos}")

    print("\n[3] Confirmação da estratégia de leitura")
    if total_encontrados == total_lidos and total_lidos > 0:
        print("- Os dados estão sendo consolidados por append (concat de todos os arquivos).")
        print("- Não está sendo usado apenas um arquivo.")
    elif total_lidos == 0:
        print("- Nenhum arquivo foi lido.")
    else:
        print("- Nem todos os arquivos encontrados foram lidos.")

    print(f"\n[4] Total consolidado de linhas da tabela de notas: {len(consolidada)}")
    print("\n[5] Head da tabela final consolidada:")
    if consolidada.empty:
        print("Tabela consolidada vazia.")
    else:
        print(consolidada.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

