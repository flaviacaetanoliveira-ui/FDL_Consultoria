from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    _, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(BASE_DIR)

    base = liberacoes_tratadas.copy()
    rec = base["RECORD_TYPE"].fillna("").astype(str).str.strip().str.lower()
    desc = base["DESCRIPTION"].fillna("").astype(str).str.strip().str.lower()

    mask_venda_real = rec.eq("release") & desc.eq("payment")
    liberacoes_venda_real = base.loc[mask_venda_real].copy()
    liberacoes_nao_venda = base.loc[~mask_venda_real].copy()

    qtd_total = int(len(base))
    qtd_venda = int(len(liberacoes_venda_real))
    qtd_nao = int(len(liberacoes_nao_venda))

    soma_total = float(pd.to_numeric(base["Valor pago"], errors="coerce").sum())
    soma_venda = float(pd.to_numeric(liberacoes_venda_real["Valor pago"], errors="coerce").sum())
    soma_nao = float(pd.to_numeric(liberacoes_nao_venda["Valor pago"], errors="coerce").sum())

    pct_venda = (soma_venda / soma_total * 100.0) if soma_total else 0.0
    pct_nao = (soma_nao / soma_total * 100.0) if soma_total else 0.0

    print("=== SEPARAÇÃO DE LIBERAÇÕES POR NATUREZA ===")
    print("\n[1] Quantidade de linhas")
    print(f"- liberacoes_tratadas: {qtd_total}")
    print(f"- liberacoes_venda_real: {qtd_venda}")
    print(f"- liberacoes_nao_venda: {qtd_nao}")

    print("\n[2] Soma de valores (Valor pago)")
    print(f"- total original: {soma_total:.2f}")
    print(f"- total venda real: {soma_venda:.2f}")
    print(f"- total não venda: {soma_nao:.2f}")

    print("\n[3] Percentual financeiro")
    print(f"- venda real: {pct_venda:.2f}%")
    print(f"- não venda: {pct_nao:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

