from __future__ import annotations

import sys

import pandas as pd

from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)
from etapa3_conciliacao_vendas_pagamento_real import build_conciliacao_vendas_pagamento_real


ORDEM_STATUS = ["Pago correto", "Pago a maior", "Pago a menor", "Sem pagamento"]


def _resumo(nome: str, df: pd.DataFrame) -> tuple[dict[str, float], pd.DataFrame]:
    total = int(len(df))
    com_pag = int(df["Tem pagamento"].eq("Sim").sum())
    soma_pago = float(pd.to_numeric(df["Valor pago"], errors="coerce").sum())

    dist = (
        df["Status financeiro"]
        .value_counts(dropna=False)
        .reindex(ORDEM_STATUS, fill_value=0)
        .rename_axis("Status financeiro")
        .reset_index(name="Quantidade")
    )
    dist["Percentual"] = (dist["Quantidade"] / total * 100.0) if total else 0.0
    dist["Base"] = nome

    meta = {
        "Base": nome,
        "Total vendas": total,
        "Vendas com pagamento": com_pag,
        "Soma Valor pago": soma_pago,
    }
    return meta, dist


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc_validas = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)
    conc_real = build_conciliacao_vendas_pagamento_real(BASE_DIR)

    m1, d1 = _resumo("Todas válidas", conc_validas)
    m2, d2 = _resumo("Pagamento real", conc_real)

    metas = pd.DataFrame([m1, m2])
    dist = pd.concat([d1, d2], ignore_index=True)
    pivot_qtd = dist.pivot(index="Status financeiro", columns="Base", values="Quantidade").reset_index()
    pivot_pct = dist.pivot(index="Status financeiro", columns="Base", values="Percentual").reset_index()

    qtd_maior_validas = int(
        d1.loc[d1["Status financeiro"].eq("Pago a maior"), "Quantidade"].iloc[0]
    )
    qtd_maior_real = int(
        d2.loc[d2["Status financeiro"].eq("Pago a maior"), "Quantidade"].iloc[0]
    )
    qtd_menor_validas = int(
        d1.loc[d1["Status financeiro"].eq("Pago a menor"), "Quantidade"].iloc[0]
    )
    qtd_menor_real = int(
        d2.loc[d2["Status financeiro"].eq("Pago a menor"), "Quantidade"].iloc[0]
    )

    soma_validas = float(m1["Soma Valor pago"])
    soma_real = float(m2["Soma Valor pago"])

    print("=== COMPARAÇÃO: TODAS VÁLIDAS vs PAGAMENTO REAL ===")
    print("\n[1] Quantidade de vendas com pagamento + soma Valor pago")
    print(metas.to_string(index=False))

    print("\n[2] Distribuição por Status financeiro (quantidade)")
    print(pivot_qtd.to_string(index=False))

    print("\n[3] Distribuição por Status financeiro (percentual)")
    print(pivot_pct.to_string(index=False))

    print("\n[4] Comparação final (impacto do filtro)")
    print(f"- Redução de 'Pago a maior': {qtd_maior_validas} -> {qtd_maior_real} (delta {qtd_maior_real - qtd_maior_validas})")
    print(f"- Mudança de 'Pago a menor': {qtd_menor_validas} -> {qtd_menor_real} (delta {qtd_menor_real - qtd_menor_validas})")
    print(f"- Diferença total de Valor pago: {soma_real - soma_validas:.2f} (Pagamento real - Todas válidas)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

