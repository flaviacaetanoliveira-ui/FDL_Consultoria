from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from etapa3_conciliacao_multichave import BASE_DIR, build_conciliacao_multichave


def _avaliar_campo_id(df: pd.DataFrame, campo_id: str) -> dict[str, object]:
    base = df.copy()
    base[campo_id] = base[campo_id].fillna("").astype(str).str.strip()
    base["N° de venda"] = base["N° de venda"].fillna("").astype(str).str.strip()

    # análise apenas onde há ID candidato e venda vinculada
    m = base[campo_id].ne("") & base["N° de venda"].ne("")
    rel = base.loc[m, [campo_id, "N° de venda"]].drop_duplicates()

    if rel.empty:
        return {
            "campo": campo_id,
            "ids_com_match": 0,
            "casos_1_1": 0,
            "casos_1_n": 0,
            "percentual_1_1": 0.0,
            "percentual_1_n": 0.0,
            "media_vendas_por_id": 0.0,
            "max_vendas_por_id": 0,
        }

    agg = (
        rel.groupby(campo_id, as_index=False)["N° de venda"]
        .nunique()
        .rename(columns={"N° de venda": "qtd_vendas_por_id"})
    )
    casos_1_1 = int((agg["qtd_vendas_por_id"] == 1).sum())
    casos_1_n = int((agg["qtd_vendas_por_id"] >= 2).sum())
    total_ids = int(len(agg))

    return {
        "campo": campo_id,
        "ids_com_match": total_ids,
        "casos_1_1": casos_1_1,
        "casos_1_n": casos_1_n,
        "percentual_1_1": (casos_1_1 / total_ids * 100.0) if total_ids else 0.0,
        "percentual_1_n": (casos_1_n / total_ids * 100.0) if total_ids else 0.0,
        "media_vendas_por_id": float(agg["qtd_vendas_por_id"].mean()),
        "max_vendas_por_id": int(agg["qtd_vendas_por_id"].max()),
    }


def _montar_tabela_diagnostico(df: pd.DataFrame, campo_escolhido: str) -> pd.DataFrame:
    base = df.copy()
    for col in ("EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID", "N° de venda"):
        base[col] = base[col].fillna("").astype(str).str.strip()
    base["ID do pedido"] = base[campo_escolhido]

    rel = base[
        ["N° de venda", "EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID", "ID do pedido"]
    ].drop_duplicates()
    rel = rel[rel["ID do pedido"].ne("") & rel["N° de venda"].ne("")].copy()

    contagem = (
        rel.groupby("ID do pedido", as_index=False)["N° de venda"]
        .nunique()
        .rename(columns={"N° de venda": "qtd_vendas_mesmo_id_pedido"})
    )
    tabela = rel.merge(contagem, how="left", on="ID do pedido")
    return tabela


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc_multi = build_conciliacao_multichave(BASE_DIR)

    candidatos = ["EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID"]
    avaliacoes = [_avaliar_campo_id(conc_multi, c) for c in candidatos]
    resumo = pd.DataFrame(avaliacoes).sort_values(
        by=["percentual_1_1", "ids_com_match"], ascending=[False, False]
    )

    campo_escolhido = str(resumo.iloc[0]["campo"]) if not resumo.empty else "EXTERNAL_REFERENCE"
    tabela_diag = _montar_tabela_diagnostico(conc_multi, campo_escolhido)

    casos_1_1 = int((tabela_diag["qtd_vendas_mesmo_id_pedido"] == 1).sum())
    casos_1_n = int((tabela_diag["qtd_vendas_mesmo_id_pedido"] >= 2).sum())

    exemplos_1_n = (
        tabela_diag[tabela_diag["qtd_vendas_mesmo_id_pedido"] >= 2]
        .sort_values(
            by=["qtd_vendas_mesmo_id_pedido", "ID do pedido"], ascending=[False, True]
        )
        .head(30)
    )

    print("=== DIAGNÓSTICO DE CARDINALIDADE DA CHAVE ===")
    print("\n[1] Comparativo dos campos candidatos a ID do pedido")
    print(resumo.to_string(index=False))

    print(f"\nCampo escolhido como melhor ID do pedido: {campo_escolhido}")
    print("(critério: maior percentual 1:1 com maior volume de IDs com match)")

    print("\n[2] Cardinalidade com campo escolhido")
    print(f"- Casos 1:1: {casos_1_1}")
    print(f"- Casos 1:N: {casos_1_n}")

    print("\n[3] Tabela de diagnóstico (amostra de 30 linhas)")
    print(tabela_diag.head(30).to_string(index=False))

    print("\n[4] Exemplos reais de 1 ID do pedido com 2+ N° de venda")
    if exemplos_1_n.empty:
        print("Nenhum caso 1:N encontrado no campo escolhido.")
    else:
        print(exemplos_1_n.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

