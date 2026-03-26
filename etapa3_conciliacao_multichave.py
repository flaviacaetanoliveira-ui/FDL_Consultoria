from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def build_conciliacao_multichave(base_dir: str | Path) -> pd.DataFrame:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    vendas_idx = vendas_tratadas.copy()
    vendas_idx["N° de venda"] = vendas_idx["N° de venda"].fillna("").astype(str).str.strip()
    map_total = dict(zip(vendas_idx["N° de venda"], vendas_idx["Total BRL"]))
    vendas_ids = set(vendas_idx["N° de venda"])

    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = lib["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    lib["ORDER_ID"] = lib["ORDER_ID"].fillna("").astype(str).str.strip()
    lib["PACK_ID"] = lib["PACK_ID"].fillna("").astype(str).str.strip()

    matched_id = pd.Series(pd.NA, index=lib.index, dtype="object")
    chave_usada = pd.Series(pd.NA, index=lib.index, dtype="object")

    # 1) EXTERNAL_REFERENCE
    m1 = lib["EXTERNAL_REFERENCE"].isin(vendas_ids) & lib["EXTERNAL_REFERENCE"].ne("")
    matched_id.loc[m1] = lib.loc[m1, "EXTERNAL_REFERENCE"]
    chave_usada.loc[m1] = "EXTERNAL_REFERENCE"

    # 2) ORDER_ID (somente sem match anterior)
    pend = matched_id.isna()
    m2 = pend & lib["ORDER_ID"].isin(vendas_ids) & lib["ORDER_ID"].ne("")
    matched_id.loc[m2] = lib.loc[m2, "ORDER_ID"]
    chave_usada.loc[m2] = "ORDER_ID"

    # 3) PACK_ID (somente sem match anterior)
    pend = matched_id.isna()
    m3 = pend & lib["PACK_ID"].isin(vendas_ids) & lib["PACK_ID"].ne("")
    matched_id.loc[m3] = lib.loc[m3, "PACK_ID"]
    chave_usada.loc[m3] = "PACK_ID"

    out = lib[
        ["EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID", "Data de pagamento", "Valor pago"]
    ].copy()
    out["N° de venda"] = matched_id
    out["Total BRL"] = out["N° de venda"].map(map_total)
    out["Chave match"] = chave_usada
    out["Match sucesso"] = out["N° de venda"].notna().map({True: "Sim", False: "Não"})

    # Tabela pedida: chave utilizada, indicador sucesso e dados da venda vinculada
    conciliacao_multichave = out[
        [
            "EXTERNAL_REFERENCE",
            "ORDER_ID",
            "PACK_ID",
            "Valor pago",
            "Data de pagamento",
            "N° de venda",
            "Total BRL",
            "Chave match",
            "Match sucesso",
        ]
    ].copy()
    return conciliacao_multichave


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    validacao = build_conciliacao_multichave(BASE_DIR)

    total = int(len(validacao))
    com_match = int(validacao["Match sucesso"].eq("Sim").sum())
    sem_match = int(validacao["Match sucesso"].eq("Não").sum())
    cobertura = (com_match / total * 100.0) if total else 0.0

    por_chave = (
        validacao["Chave match"]
        .fillna("SEM_MATCH")
        .value_counts(dropna=False)
        .rename_axis("Chave")
        .reset_index(name="Quantidade")
    )

    print("Head (conciliacao_multichave):")
    print(validacao.head(10).to_string(index=False))

    print("\nCobertura total após fallback:")
    print(f"- Total de liberações: {total}")
    print(f"- Com match: {com_match}")
    print(f"- Sem match: {sem_match}")
    print(f"- Cobertura: {cobertura:.2f}%")

    print("\nQuantos vieram de cada chave:")
    print(por_chave.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

