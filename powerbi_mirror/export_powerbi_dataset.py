from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
import sys
import os

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional
from fdl_paths import resolve_pasta_vendas_ml


OUT_DIR = Path(__file__).resolve().parent / "output"
OUT_CSV = OUT_DIR / "conciliacao_operacional.csv"
OUT_XLSX = OUT_DIR / "conciliacao_operacional.xlsx"
OUT_METRICS = OUT_DIR / "metrics.json"
OUT_SCHEMA = OUT_DIR / "schema.txt"


def _build_metrics(df: pd.DataFrame, info: dict[str, object]) -> dict[str, object]:
    diferenca = pd.to_numeric(df.get("Diferença"), errors="coerce")
    valor_pago = pd.to_numeric(df.get("Valor pago"), errors="coerce")
    valor_receber = pd.to_numeric(df.get("Valor a receber"), errors="coerce")
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "base_dir": str(BASE_DIR),
        "rows": int(len(df)),
        "arquivos_contas_lidos": int(info.get("arquivos_contas_lidos", 0)),
        "acao_sugerida_counts": {
            str(k): int(v) for k, v in df["Ação sugerida"].value_counts(dropna=False).to_dict().items()
        },
        "situacao_counts": {
            str(k): int(v) for k, v in df["Situação"].value_counts(dropna=False).to_dict().items()
        },
        "sum_valor_pago": float(valor_pago.fillna(0).sum()),
        "sum_valor_receber": float(valor_receber.fillna(0).sum()),
        "sum_diferenca_abs": float(diferenca.fillna(0).abs().sum()),
    }


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    effective_base_dir = Path(os.environ.get("FDL_BASE_DIR", str(BASE_DIR))).resolve()
    required_dirs = [
        resolve_pasta_vendas_ml(effective_base_dir),
        effective_base_dir / "Liberações_ML",
        effective_base_dir / "notas_saida",
        effective_base_dir / "contas_receber",
    ]
    missing_dirs = [str(p) for p in required_dirs if not p.exists()]
    if missing_dirs:
        print("Não foi possível exportar para Power BI.")
        print("Pastas de entrada ausentes:")
        for d in missing_dirs:
            print(f"- {d}")
        print("\nDefina FDL_BASE_DIR com o caminho da base do cliente antes de executar.")
        return 1

    tabela, info = carregar_tabela_final_operacional(effective_base_dir)
    tabela.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    tabela.to_excel(OUT_XLSX, index=False, sheet_name="Conciliação", engine="openpyxl")

    metrics = _build_metrics(tabela, info)
    OUT_METRICS.write_text(
        json.dumps(metrics, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    schema_lines = [f"{col}\t{dtype}" for col, dtype in tabela.dtypes.items()]
    OUT_SCHEMA.write_text("\n".join(schema_lines), encoding="utf-8")

    print(f"Base usada: {effective_base_dir}")
    print(f"CSV exportado: {OUT_CSV}")
    print(f"Excel exportado: {OUT_XLSX}")
    print(f"Métricas: {OUT_METRICS}")
    print(f"Schema: {OUT_SCHEMA}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
