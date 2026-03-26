from __future__ import annotations

import sys

import pandas as pd

from modelagem_por_pedido import BASE_DIR, construir_modelagem_por_pedido


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    out = construir_modelagem_por_pedido(BASE_DIR)
    de_para = out["de_para_venda_pedido"].copy()

    de_para["N° de venda"] = de_para["N° de venda"].fillna("").astype(str).str.strip()
    de_para["ID do pedido"] = de_para["ID do pedido"].fillna("").astype(str).str.strip()

    total_linhas = int(len(de_para))
    id_preenchido = int(de_para["ID do pedido"].ne("").sum())
    id_nulo = int(de_para["ID do pedido"].eq("").sum())

    base_ids = de_para[de_para["ID do pedido"].ne("")].copy()
    total_ids_distintos = int(base_ids["ID do pedido"].nunique())

    card = (
        base_ids.groupby("ID do pedido", as_index=False)["N° de venda"]
        .nunique()
        .rename(columns={"N° de venda": "qtd_vendas"})
    )

    pedidos_1 = int((card["qtd_vendas"] == 1).sum())
    pedidos_2 = int((card["qtd_vendas"] == 2).sum())
    pedidos_3_mais = int((card["qtd_vendas"] >= 3).sum())

    exemplos_1n = (
        card[card["qtd_vendas"] >= 2]
        .merge(de_para, how="left", on="ID do pedido")
        .sort_values(["qtd_vendas", "ID do pedido", "N° de venda"], ascending=[False, True, True])
        .head(20)
    )

    print("=== VALIDAÇÃO DE CARDINALIDADE - DE/PARA COMPLETO ===")
    print(f"Total de linhas do de/para: {total_linhas}")
    print(f"Total de IDs de pedido distintos (preenchidos): {total_ids_distintos}")
    print(f"Linhas com ID do pedido nulo: {id_nulo}")
    print(f"Linhas com ID do pedido preenchido: {id_preenchido}")

    print("\nContagem de pedidos por cardinalidade:")
    print(f"- Pedidos com 1 venda: {pedidos_1}")
    print(f"- Pedidos com 2 vendas: {pedidos_2}")
    print(f"- Pedidos com 3 ou mais vendas: {pedidos_3_mais}")

    print("\n20 exemplos reais com 1 ID do pedido -> mais de 1 N° de venda:")
    if exemplos_1n.empty:
        print("Nenhum exemplo encontrado.")
    else:
        print(
            exemplos_1n[
                ["ID do pedido", "qtd_vendas", "N° de venda", "Chave usada"]
            ].to_string(index=False)
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

