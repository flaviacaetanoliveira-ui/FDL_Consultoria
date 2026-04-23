"""
Compara pedidos CSV originais (Mega Star) com o dataset materializado — março/2026.

Replica renomeação ``Código (SKU)`` → ``Código`` e ``to_numeric_br`` em ``Valor total`` como no build.

Uso:
  python scripts/compare_mega_star_marco_csv_vs_materializado.py
  python scripts/compare_mega_star_marco_csv_vs_materializado.py --year 2026 --month 3
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from processing.faturamento.build import _normalize_pedidos_export
from processing.faturamento.io_pedidos import load_all_pedidos_csv_concatenated
from processing.faturamento.normalize import normalize_sku_join_key_scalar, to_numeric_br


def _month_mask(ts: pd.Series, year: int, month: int) -> pd.Series:
    t = pd.to_datetime(ts, errors="coerce", dayfirst=True)
    if getattr(t.dt, "tz", None) is not None:
        t = t.dt.tz_localize(None)
    return (t.dt.year == year) & (t.dt.month == month)


def _nf_month_mask_br(ts: pd.Series, year: int, month: int) -> pd.Series:
    """Emissão NF em (year, month) no fuso America/Sao_Paulo (alinhado à auditoria DRE)."""
    try:
        from zoneinfo import ZoneInfo

        br = ZoneInfo("America/Sao_Paulo")
    except Exception:
        t = pd.to_datetime(ts, errors="coerce", utc=False)
        return (t.dt.year == year) & (t.dt.month == month)
    t = pd.to_datetime(ts, errors="coerce", utc=True)
    if t.dt.tz is None:
        t = t.dt.tz_localize("UTC")
    t = t.dt.tz_convert(br)
    return (t.dt.year == year) & (t.dt.month == month)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--pedidos-dir",
        type=Path,
        default=Path(
            r"C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Pedro\Cliente_2\Mega Star\Pedidos"
        ),
    )
    ap.add_argument(
        "--parquet",
        type=Path,
        default=ROOT / "data_products/cliente_2/faturamento/current/dataset.parquet",
    )
    ap.add_argument("--org-id", type=str, default="mega_star")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--month", type=int, default=3)
    args = ap.parse_args()

    if not args.pedidos_dir.is_dir():
        print(f"ERRO: pasta pedidos inexistente: {args.pedidos_dir}", file=sys.stderr)
        return 1
    if not args.parquet.is_file():
        print(f"ERRO: parquet inexistente: {args.parquet}", file=sys.stderr)
        return 1

    df_raw, meta = load_all_pedidos_csv_concatenated(args.pedidos_dir)
    df_ped = _normalize_pedidos_export(df_raw)
    y, m = args.year, args.month
    m_data = _month_mask(df_ped["Data"], y, m)
    mar_csv = df_ped.loc[m_data].copy()
    mar_csv["_vt"] = to_numeric_br(mar_csv["Valor total"])
    if "Preço de lista" in mar_csv.columns and "Quantidade" in mar_csv.columns:
        mar_csv["_vl_lista"] = to_numeric_br(mar_csv["Preço de lista"]) * to_numeric_br(mar_csv["Quantidade"])
    else:
        mar_csv["_vl_lista"] = float("nan")

    print("=== 1. CSV originais Mega Star (concat todos *.csv em Pedidos/) ===")
    print(f"Pasta: {args.pedidos_dir.resolve()}")
    print(f"Ficheiros: {len(meta.get('arquivos', []))} CSV")
    print(f"Março/{y} (coluna Data, dayfirst): {len(mar_csv)} linhas")
    print(f"Soma Valor total (to_numeric_br): R$ {float(mar_csv['_vt'].sum()):,.2f}")
    if mar_csv["_vl_lista"].notna().any():
        print(
            f"Soma Preço lista x Qtd (proxy Vl_Venda): R$ {float(mar_csv['_vl_lista'].sum()):,.2f}"
        )

    df_mat = pd.read_parquet(args.parquet)
    oid = args.org_id.strip()
    ms = df_mat.loc[df_mat["org_id"].astype(str).str.strip().eq(oid)].copy()
    ms["_vl"] = pd.to_numeric(ms.get("Vl_Venda", 0), errors="coerce").fillna(0.0)

    m_nf = _nf_month_mask_br(ms["Nota_Data_Emissao"], y, m)
    m_dt = _month_mask(ms["Data"], y, m)

    sub_nf = ms.loc[m_nf.fillna(False)]
    sub_dt = ms.loc[m_dt.fillna(False)]

    print(f"\n=== 2. Dataset materializado (org_id={oid!r}) ===")
    print(f"Ficheiro: {args.parquet.resolve()}")
    print(f"Total linhas org: {len(ms)}")
    print(f"Março/{y} por **Nota_Data_Emissao** (BR): {len(sub_nf)} linhas")
    print(f"  Soma Vl_Venda: R$ {float(sub_nf['_vl'].sum()):,.2f}")
    print(f"Março/{y} por **Data** (pedido): {len(sub_dt)} linhas")
    print(f"  Soma Vl_Venda: R$ {float(sub_dt['_vl'].sum()):,.2f}")

    csv_sum = float(mar_csv["_vt"].sum())
    mat_nf = float(sub_nf["_vl"].sum())
    mat_dt = float(sub_dt["_vl"].sum())
    print("\n=== 3. Comparação (receita comercial Valor total / Vl_Venda) ===")
    print(f"CSV original (Data março):     R$ {csv_sum:,.2f}")
    print(f"Materializado (NF em março):   R$ {mat_nf:,.2f}")
    print(f"Materializado (Data março):    R$ {mat_dt:,.2f}")
    print(f"Diff CSV - materializado (NF):    R$ {csv_sum - mat_nf:,.2f}")
    print(f"Diff CSV - materializado (Data): R$ {csv_sum - mat_dt:,.2f}")
    print(
        "\nNota: a DRE NF-first usa emissao da NF no recorte; Data do pedido em marco pode ter "
        "NF emitida fora de marco — por isso Vl_Venda(NF marco) <= Vl_Venda(Data marco) em geral."
    )

    # Q1 e todas orgs março NF (contexto R$ 699k)
    tnf = pd.to_datetime(ms["Nota_Data_Emissao"], errors="coerce", utc=True)
    if tnf.dt.tz is None:
        tnf = tnf.dt.tz_localize("UTC")
    tnf = tnf.dt.tz_convert("America/Sao_Paulo")
    q1 = (tnf.dt.year == y) & (tnf.dt.month.isin((1, 2, 3)))
    sub_q1 = ms.loc[q1.fillna(False)]

    all_mar = df_mat.loc[_nf_month_mask_br(df_mat["Nota_Data_Emissao"], y, m).fillna(False)]
    all_vl = pd.to_numeric(all_mar.get("Vl_Venda", 0), errors="coerce").fillna(0.0)

    print(f"\n=== 4. Contexto «R$ 699k» (materializado atual) ===")
    print(f"Mega Star Q1/{y} (NF emissão jan–mar): {len(sub_q1)} linhas, Vl_Venda R$ {float(sub_q1['_vl'].sum()):,.2f}")
    print(f"Todas empresas março/{y} (NF emissão): {len(all_mar)} linhas, Vl_Venda R$ {float(all_vl.sum()):,.2f}")
    print("Se o valor antigo (~699k) era outro recorte (ex.: todas orgs, outro mês ou soma lista noutro ecrã),")
    print("confrontar explicitamente com a mesma definição.")

    # SKUs: chaves normalizadas no CSV março vs materializado março NF
    mar_csv["_sku_n"] = mar_csv["Código"].map(normalize_sku_join_key_scalar)
    sub_nf = sub_nf.copy()
    sub_nf["_sku_n"] = sub_nf["SKU_Normalizado"].astype(str).str.strip().str.casefold()

    set_csv = frozenset(mar_csv["_sku_n"].unique())
    set_mat = frozenset(sub_nf["_sku_n"].unique())
    only_csv = set_csv - set_mat
    only_mat = set_mat - set_csv
    print(f"\n=== 5. SKUs (chave normalizada) — CSV março Data vs materializado março NF ===")
    print(f"Únicos no CSV março: {len(set_csv)} | no materializado (NF março): {len(set_mat)}")
    print(f"Só no CSV: {len(only_csv)} | só no materializado: {len(only_mat)}")
    if only_csv:
        lost = mar_csv.loc[mar_csv["_sku_n"].isin(only_csv)]
        print(f"Receita (Valor total) em SKUs só no CSV: R$ {float(lost['_vt'].sum()):,.2f} em {len(lost)} linhas")
        top = lost.groupby("_sku_n")["_vt"].sum().sort_values(ascending=False).head(15)
        print("Top SKUs só no CSV:")
        print(top.to_string())

    nf_panel = ROOT / "data_products/cliente_2/faturamento/current/dataset_faturamento_nf_panel.parquet"
    if nf_panel.is_file():
        pn = pd.read_parquet(nf_panel)
        pms = pn.loc[pn["org_id"].astype(str).str.strip().eq(oid)].copy()
        t2 = pd.to_datetime(pms["Nota_Data_Emissao"], errors="coerce", utc=True).dt.tz_convert(
            "America/Sao_Paulo"
        )
        pm = (t2.dt.year == y) & (t2.dt.month == m)
        psub = pms.loc[pm.fillna(False)]
        vv = pd.to_numeric(psub["valor_venda"], errors="coerce").fillna(0.0)
        print(f"\n=== 6. Painel NF-first (gran NF) março/{y} ===")
        print(f"Linhas: {len(psub)}, soma valor_venda: R$ {float(vv.sum()):,.2f} (deve coincidir com DRE)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
