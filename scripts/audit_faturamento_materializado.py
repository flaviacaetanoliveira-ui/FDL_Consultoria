"""Auditoria pontual do materializado de faturamento (sem dependências extra)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    csv_path = ROOT / "data_products/cliente_5/faturamento/current/dataset_faturamento_app.csv"
    meta_path = ROOT / "data_products/cliente_5/faturamento/current/metadata.json"
    if not csv_path.is_file() or not meta_path.is_file():
        print("CSV ou metadata não encontrados em data_products/cliente_5/faturamento/current/")
        return 1

    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    df = pd.read_csv(csv_path, dtype=str, low_memory=False)

    d = pd.to_datetime(df["Data"], dayfirst=True, errors="coerce")
    df["_data_parsed"] = d
    df["_vt"] = pd.to_numeric(
        df["Valor total"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )

    print("=== METADATA (última materialização registrada) ===")
    print("generated_at:", meta.get("generated_at"))
    print("row_count metadata:", meta.get("row_count"), "| linhas CSV:", len(df))
    print()

    print("=== 1) ARQUIVOS QUE ENTRARAM (pedidos_fonte) ===")
    for i, p in enumerate(meta.get("pedidos_fonte", []), 1):
        oid = p.get("org_id")
        arq = p.get("arquivo")
        print(f"  {i}. org={oid} | {arq}")
        print(f"     path: {p.get('path')}")
        print(f"     mtime: {p.get('mtime_iso')}")
        if p.get("arquivos"):
            for a in p["arquivos"]:
                print(f"       · {a}")
    print()

    print("=== 2) INTERVALO DE Data (materializado) ===")
    valid = df["_data_parsed"].dropna()
    print("  min:", valid.min())
    print("  max:", valid.max())
    print("  linhas com Data inválida:", int(df["_data_parsed"].isna().sum()))
    print()

    print("=== 3) Valor total: soma e linhas por mês (Data) ===")
    dfp = df.dropna(subset=["_data_parsed"]).copy()
    dfp["_ym"] = dfp["_data_parsed"].dt.to_period("M")
    g = (
        dfp.groupby("_ym", dropna=False)
        .agg(linhas=("Valor total", "count"), soma_vt=("_vt", "sum"))
        .sort_index()
    )
    for idx, row in g.iterrows():
        sm = float(row.soma_vt)
        print(f"  {idx}: linhas={int(row.linhas):5d}  soma Valor total={sm:,.2f}")
    print(
        "  TOTAL:",
        len(dfp),
        "linhas | soma VT=",
        f"{float(dfp['_vt'].sum()):,.2f}",
    )
    print()

    print("=== 3b) Por empresa + mês ===")
    for emp in sorted(dfp["empresa"].dropna().unique()):
        sub = dfp[dfp["empresa"] == emp]
        g2 = (
            sub.groupby(sub["_data_parsed"].dt.to_period("M"))
            .agg(n=("Valor total", "count"), s=("_vt", "sum"))
            .sort_index()
        )
        print(f"  [{emp}]")
        for idx, row in g2.iterrows():
            print(f"    {idx}: n={int(row.n)} soma={float(row.s):,.2f}")
    print()

    print("=== 4) pedidos_arquivo distintos no CSV ===")
    vc = df["pedidos_arquivo"].value_counts()
    for k, v in vc.items():
        print(f"  {v:5d}  {k}")
    print()

    print("=== 5) NF — preenchimento ===")
    for col in ["Número da nota", "Existe Nota Fiscal gerada"]:
        if col in df.columns:
            s = df[col].fillna("").astype(str).str.strip()
            nempty = int((s.eq("") | s.str.lower().eq("nan")).sum())
            nonempty = s[s.ne("") & s.str.lower().ne("nan")]
            sample = nonempty.head(5).tolist()
            print(f"  {col}: vazias ~ {nempty}/{len(df)} | amostra não-vazias: {sample}")
    print()

    print("=== 6) Colunas de frete / modalidade no materializado ===")
    freightish = [
        c
        for c in df.columns
        if any(
            x in c.lower()
            for x in ("frete", "envio", "logist", "transport", "modalidade", "mercado env")
        )
    ]
    print(" ", freightish)
    print("  Tem Frete Mercado Envios?", "Frete Mercado Envios" in df.columns)
    print()

    print("=== 7) Data do faturamento (parse) — min/max onde parseável ===")
    dfat = pd.to_datetime(df["Data do faturamento"], dayfirst=True, errors="coerce")
    ok = dfat.notna()
    print("  parseáveis:", int(ok.sum()), "| inválidas/vazias:", int((~ok).sum()))
    if ok.any():
        print("  min:", dfat[ok].min(), " max:", dfat[ok].max())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
