from __future__ import annotations

import json
from pathlib import Path


OUT_DIR = Path(__file__).resolve().parent / "output"
CURRENT = OUT_DIR / "metrics.json"
SNAPSHOT = OUT_DIR / "metrics.snapshot.json"


def main() -> int:
    if not CURRENT.exists():
        print("metrics.json não encontrado. Rode export_powerbi_dataset.py primeiro.")
        return 1

    current = json.loads(CURRENT.read_text(encoding="utf-8"))
    if not SNAPSHOT.exists():
        SNAPSHOT.write_text(json.dumps(current, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Snapshot criado em metrics.snapshot.json")
        return 0

    snapshot = json.loads(SNAPSHOT.read_text(encoding="utf-8"))
    keys = ["rows", "sum_valor_pago", "sum_valor_receber", "sum_diferenca_abs"]
    print("Comparação rápida:")
    for key in keys:
        print(f"- {key}: atual={current.get(key)} | snapshot={snapshot.get(key)}")

    print("\nPara atualizar snapshot, substitua metrics.snapshot.json pelo metrics.json atual.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
