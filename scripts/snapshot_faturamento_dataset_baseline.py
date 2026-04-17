"""
Guarda uma cópia do ``dataset.parquet`` de faturamento atual como baseline para comparações DRE.

Uso (a partir da raiz do repositório):
  python scripts/snapshot_faturamento_dataset_baseline.py
  python scripts/snapshot_faturamento_dataset_baseline.py --parquet data_products/cliente_2/faturamento/current/dataset.parquet

O ficheiro gerado fica na mesma pasta que o parquet de origem, com nome:
  ``dataset_baseline_<YYYYMMDDTHHMMSS>.parquet``
"""
from __future__ import annotations

import argparse
import shutil
from datetime import datetime, timezone
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--parquet",
        type=Path,
        default=_REPO_ROOT / "data_products/cliente_2/faturamento/current/dataset.parquet",
        help="Parquet de faturamento materializado a copiar.",
    )
    args = ap.parse_args()
    src = Path(args.parquet).expanduser()
    if not src.is_absolute():
        src = (_REPO_ROOT / src).resolve()
    else:
        src = src.resolve()
    if not src.is_file():
        print(f"ERRO: ficheiro inexistente: {src}")
        return 1
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dst = src.parent / f"dataset_baseline_{tag}.parquet"
    shutil.copy2(src, dst)
    print(f"Baseline guardado:\n  {dst}")
    print("\nPara comparar com uma materialização futura:")
    print(f"  python scripts/compare_faturamento_dre_orgs.py --parquet-antes \"{dst}\" --parquet-depois \"{src}\"")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
