from __future__ import annotations

import sys
from pathlib import Path
import unicodedata
import re

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR
from etapa1_vendas import list_sales_files, parse_brl_number, read_sales_file
from etapa2_liberacoes import list_liberacoes_files, read_input_file


def classificar_status_financeiro(df: pd.DataFrame, tolerancia: float = 0.01) -> pd.Series:
    valor_pago = pd.to_numeric(df["Valor pago"], errors="coerce")
    total_brl = pd.to_numeric(df["Total BRL"], errors="coerce")
    diff_abs = (total_brl - valor_pago).abs()

    status = pd.Series("Pago a maior", index=df.index, dtype="object")
    status[(valor_pago.isna()) | (valor_pago <= 0)] = "Sem pagamento"
    status[(valor_pago > 0) & (diff_abs <= tolerancia)] = "Pago correto"
    status[(valor_pago > 0) & (valor_pago < total_brl) & (diff_abs > tolerancia)] = "Pago a menor"
    return status


def _normalize_name(name: object) -> str:
    s = str(name or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _find_col(df: pd.DataFrame, aliases: set[str]) -> str:
    cmap = {_normalize_name(c): c for c in df.columns}
    for alias in aliases:
        key = _normalize_name(alias)
        if key in cmap:
            return cmap[key]
    for alias in aliases:
        key = _normalize_name(alias)
        if not key:
            continue
        for norm_col, original_col in cmap.items():
            if key in norm_col or norm_col in key:
                return original_col
    return ""


def _first_existing(base: Path, candidates: tuple[str, ...]) -> Path | None:
    for name in candidates:
        p = base / name
        if p.is_dir():
            return p
    return None


def _build_conciliacao_shopee(base_dir: str | Path) -> pd.DataFrame:
    root = Path(base_dir)
    pasta_vendas = _first_existing(root, ("Vendas_Shopee", "Vendas Shopee"))
    pasta_lib = _first_existing(
        root,
        ("Liberações_Shopee", "Liberacoes_Shopee", "Liberações Shopee", "Liberacoes Shopee"),
    )
    if pasta_vendas is None or pasta_lib is None:
        return pd.DataFrame()

    vendas_parts: list[pd.DataFrame] = []
    for path in list_sales_files(pasta_vendas):
        raw = read_sales_file(path)
        col_pedido = _find_col(raw, {"ID do pedido", "Order ID"})
        col_total = _find_col(
            raw,
            {
                "Quantia total lançada (R$)",
                "Quantia total lancada (R$)",
                "Seller Amount",
                "Net Credit Amount",
                "Total global",
                "Valor Total",
            },
        )
        if not col_pedido or not col_total:
            continue
        part = pd.DataFrame()
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["Total BRL"] = parse_brl_number(raw[col_total])
        part = part[part["N° de venda"].ne("")].copy()
        vendas_parts.append(part)
    if not vendas_parts:
        return pd.DataFrame()
    vendas = pd.concat(vendas_parts, ignore_index=True)
    vendas = vendas.groupby("N° de venda", as_index=False)["Total BRL"].sum(min_count=1)

    lib_parts: list[pd.DataFrame] = []
    for path in list_liberacoes_files(pasta_lib):
        raw = read_input_file(path)
        col_pedido = _find_col(
            raw,
            {"ID do pedido", "Order ID", "EXTERNAL_REFERENCE", "External Reference"},
        )
        col_data = _find_col(
            raw,
            {
                "Data de conclusão do pagamento",
                "Data de conclusao do pagamento",
                "Date",
                "Payment Date",
            },
        )
        col_valor = _find_col(
            raw,
            {
                "Quantia total lançada (R$)",
                "Quantia total lancada (R$)",
                "Valor pago",
                "NET_CREDIT_AMOUNT",
                "Seller Amount",
            },
        )
        if not col_pedido or not col_data or not col_valor:
            continue
        part = pd.DataFrame()
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["Data de pagamento"] = pd.to_datetime(
            raw[col_data], errors="coerce", dayfirst=True, format="mixed"
        )
        part["Valor pago"] = parse_brl_number(raw[col_valor])
        part = part[part["N° de venda"].ne("")].copy()
        lib_parts.append(part)
    if not lib_parts:
        return pd.DataFrame()
    liberacoes = pd.concat(lib_parts, ignore_index=True)
    liberacoes = liberacoes.groupby("N° de venda", as_index=False).agg(
        {"Data de pagamento": "min", "Valor pago": "sum"}
    )

    c = vendas.merge(liberacoes, how="left", on="N° de venda")
    c["Valor pago"] = pd.to_numeric(c["Valor pago"], errors="coerce").round(2)
    c["Tem pagamento"] = (c["Valor pago"].notna() & (c["Valor pago"] > 0)).map(
        {True: "Sim", False: "Não"}
    )
    c["Diferença"] = c["Total BRL"] - c["Valor pago"]
    c.loc[c["Valor pago"].isna(), "Diferença"] = pd.NA
    c["Status financeiro"] = classificar_status_financeiro(c)
    c["Chave usada"] = "ID do pedido"
    c["Plataforma"] = "Shopee"
    return c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
            "Plataforma",
        ]
    ].copy()


def build_conciliacao_vendas_liberacoes_validas(base_dir: str | Path) -> pd.DataFrame:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    # liberações válidas: EXTERNAL_REFERENCE não vazio OU PACK_ID não vazio
    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = lib["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    lib["PACK_ID"] = lib["PACK_ID"].fillna("").astype(str).str.strip()
    mask_validas = lib["EXTERNAL_REFERENCE"].ne("") | lib["PACK_ID"].ne("")
    liberacoes_validas = lib.loc[mask_validas].copy()

    # agregação por chave para fallback
    agg_ext = (
        liberacoes_validas[liberacoes_validas["EXTERNAL_REFERENCE"].ne("")]
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
        liberacoes_validas[liberacoes_validas["PACK_ID"].ne("")]
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
    c["Plataforma"] = "Mercado Livre"

    conciliacao_vendas_liberacoes_validas = c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
            "Plataforma",
        ]
    ].copy()
    conc_shopee = _build_conciliacao_shopee(base_dir)
    if conc_shopee.empty:
        return conciliacao_vendas_liberacoes_validas
    return pd.concat([conciliacao_vendas_liberacoes_validas, conc_shopee], ignore_index=True)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)

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

    print("Head (conciliacao_vendas_liberacoes_validas):")
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

