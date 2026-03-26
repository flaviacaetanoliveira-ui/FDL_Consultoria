from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conciliacao = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)

    # Base equivalente ao grupo pago_a_maior
    pago_a_maior = conciliacao[conciliacao["Status financeiro"].eq("Pago a maior")].copy()

    pagamentos_sem_venda = pago_a_maior[
        (pd.to_numeric(pago_a_maior["Total BRL"], errors="coerce") == 0)
        & (pd.to_numeric(pago_a_maior["Valor pago"], errors="coerce") > 0)
    ][["N° de venda", "Valor pago", "Data de pagamento", "Chave usada"]].copy()

    qtd = int(len(pagamentos_sem_venda))
    soma = float(pd.to_numeric(pagamentos_sem_venda["Valor pago"], errors="coerce").sum())

    print("=== PAGAMENTOS SEM VENDA (Total BRL = 0 e Valor pago > 0) ===")
    print(f"Quantidade de casos: {qtd}")
    print(f"Soma total dos valores: {soma:.2f}")

    print("\nPrimeiras 20 linhas (pagamentos_sem_venda):")
    print(pagamentos_sem_venda.head(20).to_string(index=False))

    # Checagem "em outras bases", usando dados já carregados do projeto.
    vendas_tratadas, liberacoes_tratadas, liberacoes_agregadas, _ = carregar_bases_consolidadas(
        BASE_DIR
    )
    ids = set(pagamentos_sem_venda["N° de venda"].fillna("").astype(str).str.strip())

    v = vendas_tratadas.copy()
    v["N° de venda"] = v["N° de venda"].fillna("").astype(str).str.strip()
    v_match = v[v["N° de venda"].isin(ids)].copy()
    v_match_total_zero = int((pd.to_numeric(v_match["Total BRL"], errors="coerce") == 0).sum())
    v_match_total_pos = int((pd.to_numeric(v_match["Total BRL"], errors="coerce") > 0).sum())

    l = liberacoes_tratadas.copy()
    for c in ("EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID"):
        l[c] = l[c].fillna("").astype(str).str.strip()
    ref_hit = int(l["EXTERNAL_REFERENCE"].isin(ids).sum())
    ord_hit = int(l["ORDER_ID"].isin(ids).sum())
    pack_hit = int(l["PACK_ID"].isin(ids).sum())

    la = liberacoes_agregadas.copy()
    la["EXTERNAL_REFERENCE"] = la["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    agg_hit = int(la["EXTERNAL_REFERENCE"].isin(ids).sum())

    print("\nChecagem em outras bases (possível no escopo atual):")
    print(f"- IDs encontrados em vendas_tratadas: {len(v_match)}")
    print(f"  - com Total BRL = 0: {v_match_total_zero}")
    print(f"  - com Total BRL > 0: {v_match_total_pos}")
    print(f"- Ocorrências em liberacoes_tratadas por chave:")
    print(f"  - EXTERNAL_REFERENCE: {ref_hit}")
    print(f"  - ORDER_ID: {ord_hit}")
    print(f"  - PACK_ID: {pack_hit}")
    print(f"- IDs encontrados em liberacoes_agregadas (EXTERNAL_REFERENCE): {agg_hit}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

