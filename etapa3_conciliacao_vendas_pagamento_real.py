from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def classificar_status_financeiro(df: pd.DataFrame, tolerancia: float = 0.01) -> pd.Series:
    valor_pago = pd.to_numeric(df["Valor pago"], errors="coerce")
    total_brl = pd.to_numeric(df["Total BRL"], errors="coerce")
    diff_abs = (total_brl - valor_pago).abs()

    status = pd.Series("Pago a maior", index=df.index, dtype="object")
    status[(valor_pago.isna()) | (valor_pago <= 0)] = "Sem pagamento"
    status[(valor_pago > 0) & (diff_abs <= tolerancia)] = "Pago correto"
    status[(valor_pago > 0) & (valor_pago < total_brl) & (diff_abs > tolerancia)] = "Pago a menor"
    return status


def build_conciliacao_vendas_pagamento_real(base_dir: str | Path) -> pd.DataFrame:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = lib["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    lib["PACK_ID"] = lib["PACK_ID"].fillna("").astype(str).str.strip()
    rec = lib["RECORD_TYPE"].fillna("").astype(str).str.strip().str.lower()
    desc = lib["DESCRIPTION"].fillna("").astype(str).str.strip().str.lower()

    # Apenas liberações de venda real
    liberacoes_venda_real = lib[(rec.eq("release")) & (desc.eq("payment"))].copy()

    agg_ext = (
        liberacoes_venda_real[liberacoes_venda_real["EXTERNAL_REFERENCE"].ne("")]
        .groupby("EXTERNAL_REFERENCE", as_index=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .rename(
            columns={
                "EXTERNAL_REFERENCE": "N° de venda",
                "Data de pagamento": "Data de pagamento_EXT",
                "Valor pago": "Valor pago_EXT",
            }
        )
    )

    agg_pack = (
        liberacoes_venda_real[liberacoes_venda_real["PACK_ID"].ne("")]
        .groupby("PACK_ID", as_index=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .rename(
            columns={
                "PACK_ID": "N° de venda",
                "Data de pagamento": "Data de pagamento_PACK",
                "Valor pago": "Valor pago_PACK",
            }
        )
    )

    base = vendas_tratadas.copy()
    base["N° de venda"] = base["N° de venda"].fillna("").astype(str).str.strip()

    c = base.merge(agg_ext, how="left", on="N° de venda")
    c = c.merge(agg_pack, how="left", on="N° de venda")

    tem_ext = c["Valor pago_EXT"].notna()
    c["Valor pago"] = c["Valor pago_EXT"].where(tem_ext, c["Valor pago_PACK"])
    c["Valor pago"] = pd.to_numeric(c["Valor pago"], errors="coerce").round(2)
    c["Data de pagamento"] = c["Data de pagamento_EXT"].where(tem_ext, c["Data de pagamento_PACK"])
    c["Chave usada"] = pd.Series(pd.NA, index=c.index, dtype="object")
    c.loc[tem_ext, "Chave usada"] = "EXTERNAL_REFERENCE"
    c.loc[~tem_ext & c["Valor pago_PACK"].notna(), "Chave usada"] = "PACK_ID"

    c["Tem pagamento"] = (c["Valor pago"].notna() & (c["Valor pago"] > 0)).map(
        {True: "Sim", False: "Não"}
    )
    c["Diferença"] = c["Total BRL"] - c["Valor pago"]
    c.loc[c["Valor pago"].isna(), "Diferença"] = pd.NA
    c["Status financeiro"] = classificar_status_financeiro(c)

    conciliacao_vendas_pagamento_real = c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
        ]
    ].copy()
    return conciliacao_vendas_pagamento_real


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_vendas_pagamento_real(BASE_DIR)

    total_vendas = int(len(conc))
    vendas_com_pagamento = int(conc["Tem pagamento"].eq("Sim").sum())
    perc = (vendas_com_pagamento / total_vendas * 100.0) if total_vendas else 0.0
    soma_total = float(pd.to_numeric(conc["Total BRL"], errors="coerce").sum())
    soma_pago = float(pd.to_numeric(conc["Valor pago"], errors="coerce").sum())

    ordem = ["Sem pagamento", "Pago correto", "Pago a maior", "Pago a menor"]
    dist = (
        conc["Status financeiro"]
        .value_counts(dropna=False)
        .reindex(ordem, fill_value=0)
        .rename_axis("Status financeiro")
        .reset_index(name="Quantidade")
    )

    print("Head (conciliacao_vendas_pagamento_real):")
    print(conc.head(10).to_string(index=False))

    print("\nMétricas:")
    print(f"- Total de vendas: {total_vendas}")
    print(f"- Vendas com pagamento: {vendas_com_pagamento}")
    print(f"- Percentual com pagamento: {perc:.2f}%")
    print(f"- Soma de Total BRL: {soma_total:.2f}")
    print(f"- Soma de Valor pago: {soma_pago:.2f}")

    print("\nClassificação financeira:")
    print(dist.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

