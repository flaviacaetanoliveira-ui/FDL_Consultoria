from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)


def _preparar_grupo(df: pd.DataFrame, status: str) -> pd.DataFrame:
    cols = [
        "N° de venda",
        "Total BRL",
        "Valor pago",
        "Diferença",
        "Data de pagamento",
        "Chave usada",
    ]
    out = df[df["Status financeiro"].eq(status)][cols].copy()
    out["Diferença abs"] = pd.to_numeric(out["Diferença"], errors="coerce").abs()
    out = out.sort_values("Diferença abs", ascending=False, kind="stable").reset_index(drop=True)
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conciliacao = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)

    pago_a_maior = _preparar_grupo(conciliacao, "Pago a maior")
    pago_a_menor = _preparar_grupo(conciliacao, "Pago a menor")

    qtd_maior = int(len(pago_a_maior))
    qtd_menor = int(len(pago_a_menor))
    soma_diff_maior = float(pd.to_numeric(pago_a_maior["Diferença"], errors="coerce").sum())
    soma_diff_menor = float(pd.to_numeric(pago_a_menor["Diferença"], errors="coerce").sum())

    print("=== RELATÓRIO DE AUDITORIA FINANCEIRA ===")
    print("\n[1] Quantidade de casos")
    print(f"- Pago a maior: {qtd_maior}")
    print(f"- Pago a menor: {qtd_menor}")

    print("\n[2] Soma total das diferenças")
    print(f"- Pago a maior (Total BRL - Valor pago): {soma_diff_maior:.2f}")
    print(f"- Pago a menor (Total BRL - Valor pago): {soma_diff_menor:.2f}")

    print("\n[3] Top 10 maiores diferenças - Pago a maior")
    print(pago_a_maior.head(10).drop(columns=["Diferença abs"]).to_string(index=False))

    print("\n[4] Top 10 maiores diferenças - Pago a menor")
    print(pago_a_menor.head(10).drop(columns=["Diferença abs"]).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

