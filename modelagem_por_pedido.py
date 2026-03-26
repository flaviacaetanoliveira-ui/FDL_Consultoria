from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def _normalizar_id(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip()


def _id_pedido_linha(df: pd.DataFrame) -> pd.Series:
    """
    ID do pedido real nas liberações:
    prioridade ORDER_ID; fallback EXTERNAL_REFERENCE; fallback PACK_ID.
    """
    order_id = _normalizar_id(df["ORDER_ID"])
    ext_ref = _normalizar_id(df["EXTERNAL_REFERENCE"])
    pack_id = _normalizar_id(df["PACK_ID"])
    return order_id.where(order_id.ne(""), ext_ref.where(ext_ref.ne(""), pack_id))


def _id_pedido_por_chave(df: pd.DataFrame, chave: str) -> pd.DataFrame:
    tmp = df.copy()
    tmp[chave] = _normalizar_id(tmp[chave])
    tmp["ID do pedido"] = _id_pedido_linha(tmp)
    tmp = tmp[tmp[chave].ne("") & tmp["ID do pedido"].ne("")].copy()
    if tmp.empty:
        return pd.DataFrame(columns=[chave, "ID do pedido"])

    # Resolve conflitos por chave escolhendo o ID mais frequente.
    freq = (
        tmp.groupby([chave, "ID do pedido"], as_index=False)
        .size()
        .rename(columns={"size": "freq"})
        .sort_values([chave, "freq", "ID do pedido"], ascending=[True, False, True])
    )
    best = freq.drop_duplicates(subset=[chave], keep="first")[[chave, "ID do pedido"]]
    return best.reset_index(drop=True)


def construir_modelagem_por_pedido(base_dir: str | Path) -> dict[str, pd.DataFrame]:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    # Liberações válidas: EXTERNAL_REFERENCE OU PACK_ID preenchido
    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = _normalizar_id(lib["EXTERNAL_REFERENCE"])
    lib["ORDER_ID"] = _normalizar_id(lib["ORDER_ID"])
    lib["PACK_ID"] = _normalizar_id(lib["PACK_ID"])
    mask_validas = lib["EXTERNAL_REFERENCE"].ne("") | lib["PACK_ID"].ne("")
    liberacoes_validas = lib.loc[mask_validas].copy()
    liberacoes_validas["ID do pedido"] = _id_pedido_linha(liberacoes_validas)

    # Mapeamentos para fallback de chave venda -> liberação
    map_ext = _id_pedido_por_chave(liberacoes_validas, "EXTERNAL_REFERENCE")
    map_pack = _id_pedido_por_chave(liberacoes_validas, "PACK_ID")

    vendas = vendas_tratadas.copy()
    vendas["N° de venda"] = _normalizar_id(vendas["N° de venda"])

    de_para = vendas[["N° de venda"]].copy()
    de_para = de_para.merge(
        map_ext.rename(columns={"EXTERNAL_REFERENCE": "N° de venda", "ID do pedido": "ID_EXT"}),
        how="left",
        on="N° de venda",
    )
    de_para = de_para.merge(
        map_pack.rename(columns={"PACK_ID": "N° de venda", "ID do pedido": "ID_PACK"}),
        how="left",
        on="N° de venda",
    )
    de_para["ID do pedido"] = de_para["ID_EXT"].where(de_para["ID_EXT"].notna(), de_para["ID_PACK"])
    de_para["Chave usada"] = pd.Series(pd.NA, index=de_para.index, dtype="object")
    de_para.loc[de_para["ID_EXT"].notna(), "Chave usada"] = "EXTERNAL_REFERENCE"
    de_para.loc[de_para["ID_EXT"].isna() & de_para["ID_PACK"].notna(), "Chave usada"] = "PACK_ID"
    de_para_venda_pedido = de_para[["N° de venda", "ID do pedido", "Chave usada"]].copy()

    # Validação cardinalidade pedido -> vendas
    card = (
        de_para_venda_pedido[de_para_venda_pedido["ID do pedido"].notna()]
        .groupby("ID do pedido", as_index=False)["N° de venda"]
        .nunique()
        .rename(columns={"N° de venda": "qtd_vendas_por_pedido"})
    )

    # Vendas agregadas no nível pedido
    vendas_por_pedido = (
        de_para_venda_pedido.merge(vendas_tratadas, how="left", on="N° de venda")
        .dropna(subset=["ID do pedido"])
        .groupby("ID do pedido", as_index=False)["Total BRL"]
        .sum(min_count=1)
        .rename(columns={"Total BRL": "valor vendido"})
    )

    # Pagamentos agregados no nível pedido
    pagamentos_por_pedido = (
        liberacoes_validas[liberacoes_validas["ID do pedido"].ne("")]
        .groupby("ID do pedido", as_index=False)["Valor pago"]
        .sum(min_count=1)
    )

    conciliacao_por_pedido = vendas_por_pedido.merge(
        pagamentos_por_pedido, how="outer", on="ID do pedido"
    )
    conciliacao_por_pedido["valor vendido"] = pd.to_numeric(
        conciliacao_por_pedido["valor vendido"], errors="coerce"
    )
    conciliacao_por_pedido["Valor pago"] = pd.to_numeric(
        conciliacao_por_pedido["Valor pago"], errors="coerce"
    )
    conciliacao_por_pedido["diferença"] = (
        conciliacao_por_pedido["valor vendido"] - conciliacao_por_pedido["Valor pago"]
    )

    return {
        "de_para_venda_pedido": de_para_venda_pedido,
        "cardinalidade_pedido": card,
        "vendas_por_pedido": vendas_por_pedido,
        "pagamentos_por_pedido": pagamentos_por_pedido,
        "conciliacao_por_pedido": conciliacao_por_pedido,
    }


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    out = construir_modelagem_por_pedido(BASE_DIR)
    de_para = out["de_para_venda_pedido"]
    card = out["cardinalidade_pedido"]
    vendas_p = out["vendas_por_pedido"]
    pgto_p = out["pagamentos_por_pedido"]
    conc_p = out["conciliacao_por_pedido"]

    casos_1 = int((card["qtd_vendas_por_pedido"] == 1).sum()) if not card.empty else 0
    casos_n = int((card["qtd_vendas_por_pedido"] >= 2).sum()) if not card.empty else 0

    print("=== MODELAGEM POR ID DO PEDIDO ===")
    print("ID do pedido escolhido nas liberações: ORDER_ID (fallback EXTERNAL_REFERENCE, depois PACK_ID)")

    print("\n[de_para_venda_pedido] head")
    print(de_para.head(15).to_string(index=False))

    print("\nValidação cardinalidade (pedido -> vendas):")
    print(f"- Pedidos com 1 venda: {casos_1}")
    print(f"- Pedidos com 2+ vendas: {casos_n}")

    exemplos_1n = (
        card[card["qtd_vendas_por_pedido"] >= 2]
        .merge(de_para, how="left", on="ID do pedido")
        .sort_values(["qtd_vendas_por_pedido", "ID do pedido"], ascending=[False, True])
        .head(30)
    )
    print("\nExemplos de 1 pedido com 2+ vendas (até 30 linhas):")
    if exemplos_1n.empty:
        print("Nenhum caso encontrado.")
    else:
        print(exemplos_1n.to_string(index=False))

    print("\nTotais por tabela:")
    print(f"- de_para_venda_pedido: {len(de_para)}")
    print(f"- vendas_por_pedido: {len(vendas_p)}")
    print(f"- pagamentos_por_pedido: {len(pgto_p)}")
    print(f"- conciliacao_por_pedido: {len(conc_p)}")

    print("\nHead vendas_por_pedido:")
    print(vendas_p.head(10).to_string(index=False))

    print("\nHead pagamentos_por_pedido:")
    print(pgto_p.head(10).to_string(index=False))

    print("\nHead conciliacao_por_pedido:")
    print(conc_p.head(10).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

