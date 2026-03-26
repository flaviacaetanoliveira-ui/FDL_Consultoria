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
    ext = base["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    pack = base["PACK_ID"].fillna("").astype(str).str.strip()

    mask_validas = ext.ne("") | pack.ne("")
    liberacoes_validas = base.loc[mask_validas].copy()
    liberacoes_invalidas = base.loc[~mask_validas].copy()

    total = int(len(base))
    qtd_validas = int(len(liberacoes_validas))
    qtd_invalidas = int(len(liberacoes_invalidas))

    soma_validas = float(pd.to_numeric(liberacoes_validas["Valor pago"], errors="coerce").sum())
    soma_invalidas = float(pd.to_numeric(liberacoes_invalidas["Valor pago"], errors="coerce").sum())
    soma_total = float(pd.to_numeric(base["Valor pago"], errors="coerce").sum())

    perc_validas = (qtd_validas / total * 100.0) if total else 0.0
    perc_invalidas = (qtd_invalidas / total * 100.0) if total else 0.0

    perc_fin_validas = (soma_validas / soma_total * 100.0) if soma_total else 0.0
    perc_fin_invalidas = (soma_invalidas / soma_total * 100.0) if soma_total else 0.0

    print("=== SEPARAÇÃO DE LIBERAÇÕES (VÁLIDAS x INVÁLIDAS) ===")
    print(f"Total de liberações_tratadas: {total}")

    print("\n[liberacoes_validas]")
    print(f"- Quantidade de linhas: {qtd_validas}")
    print(f"- Soma de Valor pago: {soma_validas:.2f}")
    print(f"- % sobre total de linhas: {perc_validas:.2f}%")
    print(f"- % sobre impacto financeiro total: {perc_fin_validas:.2f}%")

    print("\n[liberacoes_invalidas]")
    print(f"- Quantidade de linhas: {qtd_invalidas}")
    print(f"- Soma de Valor pago: {soma_invalidas:.2f}")
    print(f"- % sobre total de linhas: {perc_invalidas:.2f}%")
    print(f"- % sobre impacto financeiro total: {perc_fin_invalidas:.2f}%")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

