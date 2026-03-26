from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from etapa2_liberacoes import PASTA_LIBERACOES, read_input_file
from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _read_liberacoes_raw_all() -> pd.DataFrame:
    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in PASTA_LIBERACOES.glob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    parts: list[pd.DataFrame] = []
    for f in files:
        df = read_input_file(f).dropna(axis=1, how="all").copy()
        df["__arquivo__"] = f.name
        parts.append(df)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _to_num(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _choose_justificativa_menor(r: pd.Series) -> str:
    fee = abs(float(r["MP_FEE_AMOUNT"])) + abs(float(r["FINANCING_FEE_AMOUNT"]))
    ship = abs(float(r["SHIPPING_FEE_AMOUNT"]))
    taxes = abs(float(r["TAXES_AMOUNT"]))
    coupon = abs(float(r["COUPON_AMOUNT"])) + abs(float(r["EFFECTIVE_COUPON_AMOUNT"]))
    desc = str(r["DESCRIPTION_TOP"]).lower()

    if "refund" in desc or "reembolso" in desc or "estorno" in desc:
        return "Estorno"
    if fee > 0 and ship > 0:
        return "Comissão + frete"
    if fee > 0:
        return "Comissão"
    if ship > 0:
        return "Frete"
    if coupon > 0:
        return "Desconto"
    if taxes > 0:
        return "Retenção"
    return "Não identificado"


def _choose_justificativa_maior(r: pd.Series) -> str:
    ship = abs(float(r["SHIPPING_FEE_AMOUNT"]))
    gross = float(r["GROSS_AMOUNT"])
    seller = float(r["SELLER_AMOUNT"])
    bal = float(r["BALANCE_AMOUNT"])
    desc = str(r["DESCRIPTION_TOP"]).lower()

    if "shipping" in desc or ship > 0:
        return "Frete repassado"
    if "payout" in desc or "release" in str(r["RECORD_TYPE_TOP"]).lower():
        return "Ajuste financeiro"
    if gross > seller and abs(gross - seller) > 0:
        return "Intermediação logística"
    if abs(bal) > 0:
        return "Ajuste financeiro"
    return "Não identificado"


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)
    lib = _read_liberacoes_raw_all()

    # Normaliza chaves para match por venda usando chave já definida na conciliação.
    for c in ("EXTERNAL_REFERENCE", "PACK_ID"):
        if c in lib.columns:
            lib[c] = _norm(lib[c])
        else:
            lib[c] = ""

    # Colunas financeiras possíveis na tabela de liberações
    fin_cols = [
        "MP_FEE_AMOUNT",
        "FINANCING_FEE_AMOUNT",
        "SHIPPING_FEE_AMOUNT",
        "TAXES_AMOUNT",
        "COUPON_AMOUNT",
        "EFFECTIVE_COUPON_AMOUNT",
        "GROSS_AMOUNT",
        "SELLER_AMOUNT",
        "NET_CREDIT_AMOUNT",
        "NET_DEBIT_AMOUNT",
        "BALANCE_AMOUNT",
    ]
    for c in fin_cols:
        lib[c] = _to_num(lib, c)

    desc_col = "DESCRIPTION" if "DESCRIPTION" in lib.columns else None
    rec_col = "RECORD_TYPE" if "RECORD_TYPE" in lib.columns else None
    if desc_col is None:
        lib["DESCRIPTION"] = ""
        desc_col = "DESCRIPTION"
    if rec_col is None:
        lib["RECORD_TYPE"] = ""
        rec_col = "RECORD_TYPE"

    # Mapeia cada venda para as linhas de liberações conforme chave usada.
    detalhes: list[dict[str, object]] = []
    base_status = conc[conc["Status financeiro"].isin(["Pago a menor", "Pago a maior"])].copy()
    base_status["N° de venda"] = _norm(base_status["N° de venda"])

    for _, row in base_status.iterrows():
        venda = row["N° de venda"]
        chave = str(row["Chave usada"]) if pd.notna(row["Chave usada"]) else ""
        if chave == "EXTERNAL_REFERENCE":
            subset = lib[lib["EXTERNAL_REFERENCE"].eq(venda)]
        elif chave == "PACK_ID":
            subset = lib[lib["PACK_ID"].eq(venda)]
        else:
            subset = lib.iloc[0:0]

        agg = {c: float(subset[c].sum()) for c in fin_cols}
        desc_top = (
            subset[desc_col].fillna("").astype(str).str.strip().value_counts().index[0]
            if not subset.empty and subset[desc_col].fillna("").astype(str).str.strip().ne("").any()
            else ""
        )
        rec_top = (
            subset[rec_col].fillna("").astype(str).str.strip().value_counts().index[0]
            if not subset.empty and subset[rec_col].fillna("").astype(str).str.strip().ne("").any()
            else ""
        )

        d = {
            "N° de venda": venda,
            "Status financeiro": row["Status financeiro"],
            "Total BRL": float(pd.to_numeric(row["Total BRL"], errors="coerce")),
            "Valor pago": float(pd.to_numeric(row["Valor pago"], errors="coerce")),
            "Diferença": float(pd.to_numeric(row["Diferença"], errors="coerce")),
            "Data de pagamento": row["Data de pagamento"],
            "Chave usada": chave,
            "DESCRIPTION_TOP": desc_top,
            "RECORD_TYPE_TOP": rec_top,
            "qtd_linhas_liberacao": int(len(subset)),
        }
        d.update(agg)
        detalhes.append(d)

    analise = pd.DataFrame(detalhes)
    if analise.empty:
        print("Sem casos de 'Pago a menor' ou 'Pago a maior' para analisar.")
        return 0

    # Justificativa financeira
    analise["Justificativa financeira"] = "Não identificado"
    m_menor = analise["Status financeiro"].eq("Pago a menor")
    m_maior = analise["Status financeiro"].eq("Pago a maior")
    analise.loc[m_menor, "Justificativa financeira"] = analise[m_menor].apply(
        _choose_justificativa_menor, axis=1
    )
    analise.loc[m_maior, "Justificativa financeira"] = analise[m_maior].apply(
        _choose_justificativa_maior, axis=1
    )

    resumo = (
        analise.groupby(["Status financeiro", "Justificativa financeira"], as_index=False)
        .agg(Quantidade=("N° de venda", "count"), Soma_diferenca=("Diferença", "sum"))
        .sort_values(["Status financeiro", "Quantidade"], ascending=[True, False])
    )

    print("=== ANÁLISE FINANCEIRA DE DIVERGÊNCIAS ===")
    print("\n[1] Colunas disponíveis na tabela de liberações (raw consolidada)")
    print(", ".join(lib.columns))

    print("\n[2] Colunas candidatas por natureza financeira")
    print("- comissão: MP_FEE_AMOUNT, FINANCING_FEE_AMOUNT")
    print("- frete: SHIPPING_FEE_AMOUNT")
    print("- valor bruto: GROSS_AMOUNT")
    print("- valor líquido/seller: SELLER_AMOUNT")
    print("- desconto: COUPON_AMOUNT, EFFECTIVE_COUPON_AMOUNT")
    print("- reembolso/estorno (indício): DESCRIPTION com refund/reembolso")
    print("- tipo de movimentação: RECORD_TYPE")
    print("- descrição: DESCRIPTION")

    print("\n[3] Quantidade e soma por Justificativa financeira")
    print(resumo.to_string(index=False))

    cols_ex = [
        "N° de venda",
        "Status financeiro",
        "Justificativa financeira",
        "Total BRL",
        "Valor pago",
        "Diferença",
        "Chave usada",
        "DESCRIPTION_TOP",
        "RECORD_TYPE_TOP",
    ]
    print("\n[4] Exemplos reais (10) - Pago a menor")
    print(
        analise[analise["Status financeiro"].eq("Pago a menor")]
        .head(10)[cols_ex]
        .to_string(index=False)
    )
    print("\n[5] Exemplos reais (10) - Pago a maior")
    print(
        analise[analise["Status financeiro"].eq("Pago a maior")]
        .head(10)[cols_ex]
        .to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

