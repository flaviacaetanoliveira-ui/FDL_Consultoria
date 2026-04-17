"""
Auditoria DRE por org (cliente_2): marco por ano (recorte emissao NF, America/Sao_Paulo).

- Prova dos 9 (CUSTO_OK) vs soma Resultado
- Top SKUs, comparacao entre orgs, CSV Pedidos vs dataset
- Checklist + rascunho executivo (ASCII)

Uso:
  python scripts/audit_mega_star_marco_dre.py --org-id mega_star
  python scripts/audit_mega_star_marco_dre.py --org-id moveis_eap --year 2026 --month 3
  python scripts/audit_mega_star_marco_dre.py --all-orgs
  python scripts/audit_mega_star_marco_dre.py --all-orgs --save-txt-dir relatorios_marco2026
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from processing.faturamento.build import _normalize_pedidos_export
from processing.faturamento.config import OUTRAS_DESPESAS_COL, STATUS_CUSTO_OK
from processing.faturamento.io_pedidos import load_all_pedidos_csv_concatenated
from processing.faturamento.normalize import to_numeric_br

TOL_RESULTADO = 10.0

DEFAULT_PARAMS = ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
ALL_ORGS_ORDER = ("moveis_eap", "gama_home", "mega_facil", "mega_star")


def money(v: float) -> str:
    return f"R$ {v:,.2f}"


def _nf_month_mask_br(ts: pd.Series, year: int, month: int) -> pd.Series:
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


def _month_mask_pedido(ts: pd.Series, year: int, month: int) -> pd.Series:
    t = pd.to_datetime(ts, errors="coerce", dayfirst=True)
    if getattr(t.dt, "tz", None) is not None:
        t = t.dt.tz_localize(None)
    return (t.dt.year == year) & (t.dt.month == month)


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def _pedidos_dir_from_params(params_path: Path, org_id: str) -> Path | None:
    if not params_path.is_file():
        return None
    raw = json.loads(params_path.read_text(encoding="utf-8"))
    root = Path(str(raw.get("cliente_root", "")).strip())
    for e in raw.get("empresas") or []:
        if str(e.get("org_id", "")).strip() == org_id:
            rel = str(e.get("pedidos_dir", "")).strip()
            if rel and root.is_dir():
                return (root / rel).resolve()
    return None


def _custo_col_doc(org_id: str) -> str:
    oid = org_id.strip().casefold()
    if oid in ("mega_star", "gama_home"):
        return "VALOR COMPRA STAR/GAMA (ver custo_por_empresa.py)"
    if oid == "mega_facil":
        return "VALOR DE COMPRA MEGA"
    if oid == "moveis_eap":
        return "VALOR COMPRA EAP"
    return "coluna de custo conforme empresa no XLSX"


def build_audit_report(
    *,
    df: pd.DataFrame,
    org_id: str,
    year: int,
    month: int,
    pedidos_dir: Path | None,
    metadata_path: Path,
) -> str:
    y, m = year, month
    oid = org_id.strip()

    nf_m = _nf_month_mask_br(df["Nota_Data_Emissao"], y, m)
    org_df = df.loc[df["org_id"].astype(str).str.strip().eq(oid) & nf_m].copy()
    org_ok = org_df.loc[org_df["Status_Custo"].astype(str).eq(STATUS_CUSTO_OK)].copy()

    lines: list[str] = []
    L = lines.append
    sep = "=" * 70
    L(sep)
    L("PROVA DOS 9 - ORG %s - MES %02d/%d (emisso NF, BR)" % (oid.upper(), m, y))
    L(sep)
    L("")
    L("Linhas totais (NF no mes):       %d" % len(org_df))
    L("Linhas com CUSTO_OK:             %d" % len(org_ok))
    L("Linhas sem custo OK:             %d" % (len(org_df) - len(org_ok)))
    cov = (len(org_ok) / len(org_df) * 100.0) if len(org_df) else 0.0
    L("Cobertura custo (CUSTO_OK/linhas): %.2f%%" % cov)

    tc_col = "Taxa de Comissão"
    vv = _num(org_ok["Vl_Venda"])
    fp = _num(org_ok["Frete_Plataforma"]) if "Frete_Plataforma" in org_ok.columns else pd.Series(0.0, index=org_ok.index)
    com = _num(org_ok[tc_col]) if tc_col in org_ok.columns else pd.Series(0.0, index=org_ok.index)
    cpt = _num(org_ok["Custo_Produto_Total"])
    od = _num(org_ok[OUTRAS_DESPESAS_COL]) if OUTRAS_DESPESAS_COL in org_ok.columns else pd.Series(0.0, index=org_ok.index)
    imp = _num(org_ok["Imposto"]) if "Imposto" in org_ok.columns else pd.Series(0.0, index=org_ok.index)
    dfw = _num(org_ok["Despesas Fixas"]) if "Despesas Fixas" in org_ok.columns else pd.Series(0.0, index=org_ok.index)

    receita = float(vv.sum())
    d_frete = float(fp.sum())
    d_com = float(com.sum())
    d_custo = float(cpt.sum())
    d_out = float(od.sum())
    d_imp = float(imp.sum())
    d_dfx = float(dfw.sum())

    deducoes: list[tuple[str, float]] = [
        ("Frete_Plataforma", d_frete),
        ("Taxa de Comissao", d_com),
        ("Custo_Produto_Total", d_custo),
        (OUTRAS_DESPESAS_COL.replace(" ", "_"), d_out),
        ("Imposto", d_imp),
        ("Despesas_Fixas", d_dfx),
    ]

    L("")
    L(sep)
    L("RECONSTRUCAO DA DRE (linhas CUSTO_OK)")
    L(sep)
    L("")
    L(f"(+) Receita (Vl_Venda):              {money(receita):>18}  (100.0%)")
    L("")
    L("DEDUCOES:")
    total_d = 0.0
    for nome, valor in deducoes:
        pct = (valor / receita * 100.0) if receita > 0 else 0.0
        L(f"(-) {nome:<28}  {money(valor):>18}  ({pct:5.1f}%)")
        total_d += valor
    pct_tot = (total_d / receita * 100.0) if receita > 0 else 0.0
    L("")
    L(f"    {'Total deducoes':<28}  {money(total_d):>18}  ({pct_tot:5.1f}%)")

    resultado_calc = receita - total_d
    margem_calc = (resultado_calc / receita * 100.0) if receita > 0 else 0.0
    L("")
    L("-" * 70)
    L(f"(=) RESULTADO CALCULADO:             {money(resultado_calc):>18}  ({margem_calc:5.1f}%)")

    resultado_dataset = float(_num(org_ok["Resultado"]).sum()) if "Resultado" in org_ok.columns else float("nan")
    L(f"(=) RESULTADO NO DATASET (soma):     {money(resultado_dataset):>18}")
    diff = resultado_calc - resultado_dataset
    L("")
    L(f"    Diferenca (calc - dataset):        {money(diff):>18}")
    if abs(diff) < TOL_RESULTADO:
        L(f"    [OK] Validado - diferenca < R$ {TOL_RESULTADO:.0f}")
    else:
        L("    [!!] Divergencia - rever arredondamento ou linhas excluidas do Resultado")

    L("")
    L(sep)
    L("ANALISE POR SKU (CUSTO_OK)")
    L(sep)
    sku_analise = (
        org_ok.groupby("SKU_Normalizado", dropna=False)
        .agg(
            Vl_Venda=("Vl_Venda", "sum"),
            Custo_Produto_Total=("Custo_Produto_Total", "sum"),
            Resultado=("Resultado", "sum"),
            Quantidade=("Quantidade", "sum"),
        )
        .reset_index()
    )
    sku_analise["Margem_pct"] = sku_analise.apply(
        lambda r: (r["Resultado"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] and r["Vl_Venda"] != 0 else 0.0, axis=1
    )
    sku_analise["Custo_pct"] = sku_analise.apply(
        lambda r: (r["Custo_Produto_Total"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] and r["Vl_Venda"] != 0 else 0.0, axis=1
    )
    sku_analise["Custo_Unitario"] = sku_analise.apply(
        lambda r: (r["Custo_Produto_Total"] / r["Quantidade"]) if r["Quantidade"] else 0.0, axis=1
    )
    sku_analise["Preco_Medio"] = sku_analise.apply(
        lambda r: (r["Vl_Venda"] / r["Quantidade"]) if r["Quantidade"] else 0.0, axis=1
    )

    L("")
    L(">>> TOP 15 SKUs com PIOR Resultado (soma) <<<")
    L("")
    piores = sku_analise.nsmallest(15, "Resultado")
    L("%-18s %12s %12s %12s %8s %8s" % ("SKU", "Receita", "Custo", "Result", "Marg%", "Custo%"))
    L("-" * 80)
    for _, row in piores.iterrows():
        L(
            f"{str(row['SKU_Normalizado'])[:18]:<18} {row['Vl_Venda']:12,.2f} {row['Custo_Produto_Total']:12,.2f} "
            f"{row['Resultado']:12,.2f} {row['Margem_pct']:7.1f}% {row['Custo_pct']:7.1f}%"
        )
    prej15 = float(piores["Resultado"].sum())
    L("")
    L(f"Prejuizo total TOP 15 (soma Resultado): {money(prej15)}")

    custo_alto = sku_analise.loc[sku_analise["Custo_pct"] > 60].sort_values("Vl_Venda", ascending=False)
    L("")
    L(">>> SKUs com Custo > 60% da receita <<<")
    L("Total SKUs: %d" % len(custo_alto))
    L(f"Receita agregada: {money(float(custo_alto['Vl_Venda'].sum()))}")
    L(f"Resultado agregado: {money(float(custo_alto['Resultado'].sum()))}")
    L("")
    for _, row in custo_alto.head(15).iterrows():
        L(
            f"{str(row['SKU_Normalizado'])[:18]:<18} {money(float(row['Vl_Venda'])):>14}  "
            f"cu={row['Custo_Unitario']:.2f}  pm={row['Preco_Medio']:.2f}  custo%={row['Custo_pct']:.1f}"
        )

    melhores = sku_analise.loc[sku_analise["Vl_Venda"] > 500].nlargest(10, "Margem_pct")
    L("")
    L(">>> TOP 10 SKUs com melhor Margem % (Receita > 500) <<<")
    for _, row in melhores.iterrows():
        L(
            f"{str(row['SKU_Normalizado'])[:18]:<18} {money(float(row['Vl_Venda'])):>14}  "
            f"{money(float(row['Resultado'])):>14}  marg%={row['Margem_pct']:.1f}"
        )

    L("")
    L(sep)
    L("COMPARACAO ENTRE EMPRESAS - MES %02d/%d (NF, CUSTO_OK)" % (m, y))
    L(sep)
    mar_ok = df.loc[nf_m & (df["Status_Custo"].astype(str).eq(STATUS_CUSTO_OK))].copy()
    emp_a = (
        mar_ok.groupby("org_id")
        .agg(Vl_Venda=("Vl_Venda", "sum"), Custo_Produto_Total=("Custo_Produto_Total", "sum"), Resultado=("Resultado", "sum"))
        .reset_index()
    )
    emp_a["Margem_pct"] = emp_a.apply(lambda r: (r["Resultado"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] else 0.0, axis=1)
    emp_a["Custo_pct"] = emp_a.apply(
        lambda r: (r["Custo_Produto_Total"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] else 0.0, axis=1
    )
    emp_a = emp_a.sort_values("Vl_Venda", ascending=False)
    L("")
    L("%-14s %15s %15s %15s %8s %8s" % ("org_id", "Receita", "Custo", "Resultado", "Marg%", "Custo%"))
    L("-" * 85)
    for _, row in emp_a.iterrows():
        tag = "[-]" if row["Resultado"] < 0 else "[+]"
        L(
            f"{str(row['org_id']):<14} {row['Vl_Venda']:15,.2f} {row['Custo_Produto_Total']:15,.2f} "
            f"{row['Resultado']:15,.2f} {row['Margem_pct']:7.1f}% {row['Custo_pct']:7.1f}% {tag}"
        )
    L("-" * 85)
    tr = float(emp_a["Vl_Venda"].sum())
    tc_ = float(emp_a["Custo_Produto_Total"].sum())
    trs = float(emp_a["Resultado"].sum())
    L(
        f"{'TOTAL':<14} {tr:15,.2f} {tc_:15,.2f} {trs:15,.2f} "
        f"{(trs / tr * 100.0) if tr else 0.0:7.1f}% {(tc_ / tr * 100.0) if tr else 0.0:7.1f}%"
    )
    media_custo_pct = float(emp_a["Custo_pct"].mean()) if len(emp_a) else 0.0
    row_oid = emp_a.loc[emp_a["org_id"].astype(str).eq(oid)]
    custo_pct_org = float(row_oid["Custo_pct"].iloc[0]) if len(row_oid) else 0.0
    L("")
    L(f"Media Custo % (entre orgs no mes): {media_custo_pct:.1f}%")
    L(f"Custo % desta org ({oid}):        {custo_pct_org:.1f}%")
    L("Desvio (esta org - media):       %+.1f p.p." % (custo_pct_org - media_custo_pct))

    L("")
    L(sep)
    L("CONSISTENCIA - CSV PEDIDOS (Data no mes) vs DATASET (Vl_Venda, NF no mes)")
    L(sep)
    receita_ds_nf = float(_num(org_df["Vl_Venda"]).sum())
    if pedidos_dir is not None and pedidos_dir.is_dir():
        raw, meta = load_all_pedidos_csv_concatenated(pedidos_dir)
        ped = _normalize_pedidos_export(raw)
        mask_d = _month_mask_pedido(ped["Data"], y, m)
        csv_m = ped.loc[mask_d]
        lista_qtd = (
            to_numeric_br(csv_m["Preço de lista"]) * to_numeric_br(csv_m["Quantidade"])
            if "Preço de lista" in csv_m.columns
            else None
        )
        receita_csv = float(lista_qtd.sum()) if lista_qtd is not None else 0.0
        L("Pedidos dir: %s" % pedidos_dir.resolve())
        L("CSV linhas (Data no mes):      %d" % len(csv_m))
        L("Dataset linhas (NF no mes):    %d" % len(org_df))
        L(f"Receita CSV (lista x qtd):     {money(receita_csv)}")
        L(f"Receita dataset (NF no mes):   {money(receita_ds_nf)}")
        L(f"Diferenca (CSV - dataset NF):  {money(receita_csv - receita_ds_nf)}")
        L("(Nota: CSV por competencia pedido; dataset por emissao NF.)")
    else:
        L("[SKIP] Pasta pedidos inexistente ou nao resolvida para esta org.")

    L("")
    L(sep)
    L("CHECKLIST DE VALIDACAO")
    L(sep)
    checklist: list[str] = []
    checklist.append("[%s] Cobertura custo: %.2f%% (meta >= 95%%)" % ("OK" if cov >= 95.0 else "!!", cov))
    checklist.append("[%s] Prova dos 9: |diff| < R$ %.0f  =>  %.2f" % ("OK" if abs(diff) < TOL_RESULTADO else "!!", TOL_RESULTADO, abs(diff)))
    dup_cols = [
        c
        for c in ("Nota_Numero_Normalizado", "SKU_Normalizado", "Quantidade", "Número do pedido")
        if c in org_df.columns
    ]
    dups = int(org_df.duplicated(subset=dup_cols).sum()) if dup_cols else 0
    checklist.append("[%s] Duplicados (chave NF+SKU+Qtd+Pedido): %d" % ("OK" if dups == 0 else "!!", dups))
    nulos = 0
    for col in ("Vl_Venda", "Custo_Produto_Total", "Resultado"):
        if col in org_ok.columns:
            nulos += int(org_ok[col].isna().sum())
    checklist.append("[%s] Nulos em Vl_Venda/Custo/Resultado (CUSTO_OK): %d" % ("OK" if nulos == 0 else "!!", nulos))
    outliers = int((sku_analise["Custo_pct"] > 100).sum())
    checklist.append("[%s] SKUs com Custo pct > 100: %d" % ("OK" if outliers == 0 else "!!", outliers))
    margem_cli = (resultado_dataset / receita * 100.0) if receita > 0 else 0.0
    checklist.append("[%s] Margem resultado: %.1f%% (alerta se < -10%%)" % ("OK" if margem_cli > -10.0 else "!!", margem_cli))

    data_proc = "N/A"
    if metadata_path.is_file():
        try:
            meta_j = json.loads(metadata_path.read_text(encoding="utf-8"))
            data_proc = str(meta_j.get("generated_at", "N/A"))
        except OSError:
            pass
    checklist.append("[i] Ultima materializacao: %s" % data_proc)

    L("")
    for item in checklist:
        L("  %s" % item)

    n_ok = sum(1 for c in checklist if c.startswith("[OK]"))
    n_warn = sum(1 for c in checklist if c.startswith("[!!]"))
    L("")
    L("  Aprovados: %d  |  Alertas: %d" % (n_ok, n_warn))

    n_skus_neg = int((sku_analise["Resultado"] < 0).sum())
    prej_skus = float(sku_analise.loc[sku_analise["Resultado"] < 0, "Resultado"].sum())
    custo_pct_org2 = (d_custo / receita * 100.0) if receita > 0 else 0.0
    outras_ded = total_d - d_custo
    outras_pct = (outras_ded / receita * 100.0) if receita > 0 else 0.0

    L("")
    L(sep)
    L("RELATORIO EXECUTIVO (rascunho - org %s)" % oid)
    L(sep)
    L("")
    L("Periodo: emissao de NF em %02d/%d (fuso America/Sao_Paulo)." % (m, y))
    L("Ultima materializacao dataset: %s" % data_proc)
    L("")
    L("Resumo financeiro (apenas linhas CUSTO_OK):")
    L(f"  Receita (Vl_Venda):           {money(receita)}")
    L(f"  Custo produto (total):       -{money(d_custo)}  ({custo_pct_org2:.1f}% da receita)")
    L(f"  Demais deducoes (soma):      -{money(outras_ded)}  ({outras_pct:.1f}% da receita)")
    L(f"  Resultado (dataset):          {money(resultado_dataset)}  ({margem_cli:.1f}% margem)")
    L("")
    L("Validacao interna:")
    L(f"  - Coluna de custo esperada: {_custo_col_doc(oid)}")
    L("  - Cobertura CUSTO_OK: %.2f%%." % cov)
    L("  - Prova dos 9 vs soma Resultado: %s (diff %.2f)." % ("OK" if abs(diff) < TOL_RESULTADO else "REVISAR", diff))
    L("")
    L("Diagnostico quantitativo:")
    L(f"  - SKUs com resultado negativo: {n_skus_neg} (soma dos negativos: {money(prej_skus)}).")
    L("")
    L("Recomendacoes (genericas):")
    L("  1. Rever precificacao dos SKUs com resultado negativo recorrente.")
    L("  2. Rever mix e custo de fornecimento nos itens com Custo % elevado.")
    L("  3. Manter alinhamento DRE com criterio NF-first ja usado no painel.")

    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--parquet",
        type=Path,
        default=ROOT / "data_products/cliente_2/faturamento/current/dataset.parquet",
    )
    ap.add_argument(
        "--metadata",
        type=Path,
        default=ROOT / "data_products/cliente_2/faturamento/current/metadata.json",
    )
    ap.add_argument(
        "--params-json",
        type=Path,
        default=DEFAULT_PARAMS,
        help="faturamento_params (schema 2) para resolver pasta Pedidos por org_id",
    )
    ap.add_argument(
        "--pedidos-dir",
        type=Path,
        default=None,
        help="Sobrescreve pasta Pedidos (senao usa params-json + org-id)",
    )
    ap.add_argument("--org-id", type=str, default="mega_star")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--month", type=int, default=3)
    ap.add_argument("--save-txt", type=Path, default=None, help="Um ficheiro UTF-8 (single org ou concat com --all-orgs)")
    ap.add_argument(
        "--save-txt-dir",
        type=Path,
        default=None,
        help="Escreve um ficheiro por org: audit_marco_<org>_<YYYY>_<MM>.txt",
    )
    ap.add_argument(
        "--all-orgs",
        action="store_true",
        help="Corre a auditoria para: %s" % ", ".join(ALL_ORGS_ORDER),
    )
    args = ap.parse_args()

    if not args.parquet.is_file():
        print(f"ERRO: parquet inexistente: {args.parquet}", file=sys.stderr)
        return 1

    df = pd.read_parquet(args.parquet)
    if "Nota_Data_Emissao" not in df.columns:
        print("ERRO: coluna Nota_Data_Emissao ausente.", file=sys.stderr)
        return 1

    orgs = list(ALL_ORGS_ORDER) if args.all_orgs else [args.org_id.strip()]
    chunks: list[str] = []
    for oid in orgs:
        ped = args.pedidos_dir
        if ped is None:
            ped = _pedidos_dir_from_params(args.params_json, oid)
        text = build_audit_report(
            df=df,
            org_id=oid,
            year=args.year,
            month=args.month,
            pedidos_dir=ped,
            metadata_path=args.metadata,
        )
        chunks.append(text)
        if args.save_txt_dir:
            args.save_txt_dir.mkdir(parents=True, exist_ok=True)
            fn = args.save_txt_dir / ("audit_marco_%s_%d_%02d.txt" % (oid, args.year, args.month))
            fn.write_text(text, encoding="utf-8")
            print("Guardado: %s" % fn.resolve(), file=sys.stderr)

    full = ("\n" + "#" * 70 + "\n\n").join(chunks) if len(chunks) > 1 else chunks[0]
    print(full, end="")

    if args.save_txt and not args.save_txt_dir:
        args.save_txt.parent.mkdir(parents=True, exist_ok=True)
        args.save_txt.write_text(full, encoding="utf-8")
        print("Guardado: %s" % args.save_txt.resolve(), file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
