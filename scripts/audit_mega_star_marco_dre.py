"""
Auditoria completa: Mega Star, marco/2026 (recorte emissao NF, fuso America/Sao_Paulo).

- Prova dos 9: soma das deducoes vs Resultado materializado (linhas CUSTO_OK)
- Top SKUs com pior resultado
- Comparacao com outras orgs (mesmo recorte)
- CSV Pedidos (Data marco) vs dataset (Vl_Venda NF marco) - criterios distintos
- Checklist + texto executivo (ASCII)

Uso:
  python scripts/audit_mega_star_marco_dre.py
  python scripts/audit_mega_star_marco_dre.py --save-txt relatorio.txt
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


def money(v: float) -> str:
    """Valor monetario BR (texto), sem dependencia de %% com separador de milhares."""
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
        "--pedidos-dir",
        type=Path,
        default=Path(
            r"C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Pedro\Cliente_2\Mega Star\Pedidos"
        ),
    )
    ap.add_argument("--org-id", type=str, default="mega_star")
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--month", type=int, default=3)
    ap.add_argument("--save-txt", type=Path, default=None, help="Guardar saida completa em ficheiro UTF-8")
    args = ap.parse_args()

    if not args.parquet.is_file():
        print(f"ERRO: parquet inexistente: {args.parquet}", file=sys.stderr)
        return 1

    y, m = args.year, args.month
    oid = args.org_id.strip()

    df = pd.read_parquet(args.parquet)
    if "Nota_Data_Emissao" not in df.columns:
        print("ERRO: coluna Nota_Data_Emissao ausente.", file=sys.stderr)
        return 1

    nf_m = _nf_month_mask_br(df["Nota_Data_Emissao"], y, m)
    ms = df.loc[df["org_id"].astype(str).str.strip().eq(oid) & nf_m].copy()
    ms_ok = ms.loc[ms["Status_Custo"].astype(str).eq(STATUS_CUSTO_OK)].copy()

    lines = []
    L = lines.append

    sep = "=" * 70
    L(sep)
    L("PROVA DOS 9 - MEGA STAR MARCO/%d (emisso NF, BR)" % y)
    L(sep)
    L("")
    L("Linhas totais (NF em marco):     %d" % len(ms))
    L("Linhas com CUSTO_OK:             %d" % len(ms_ok))
    L("Linhas sem custo OK:             %d" % (len(ms) - len(ms_ok)))
    cov = (len(ms_ok) / len(ms) * 100.0) if len(ms) else 0.0
    L("Cobertura custo (CUSTO_OK/linhas): %.2f%%" % cov)

    # --- Prova dos 9 (formula fechada: calc.compute_financial_columns_regras_fechadas)
    tc_col = "Taxa de Comissão"
    vv = _num(ms_ok["Vl_Venda"])
    fp = _num(ms_ok["Frete_Plataforma"]) if "Frete_Plataforma" in ms_ok.columns else pd.Series(0.0, index=ms_ok.index)
    com = _num(ms_ok[tc_col]) if tc_col in ms_ok.columns else pd.Series(0.0, index=ms_ok.index)
    cpt = _num(ms_ok["Custo_Produto_Total"])
    od = _num(ms_ok[OUTRAS_DESPESAS_COL]) if OUTRAS_DESPESAS_COL in ms_ok.columns else pd.Series(0.0, index=ms_ok.index)
    imp = _num(ms_ok["Imposto"]) if "Imposto" in ms_ok.columns else pd.Series(0.0, index=ms_ok.index)
    dfw = _num(ms_ok["Despesas Fixas"]) if "Despesas Fixas" in ms_ok.columns else pd.Series(0.0, index=ms_ok.index)

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

    resultado_dataset = float(_num(ms_ok["Resultado"]).sum()) if "Resultado" in ms_ok.columns else float("nan")
    L(f"(=) RESULTADO NO DATASET (soma):     {money(resultado_dataset):>18}")
    diff = resultado_calc - resultado_dataset
    L("")
    L(f"    Diferenca (calc - dataset):        {money(diff):>18}")
    if abs(diff) < TOL_RESULTADO:
        L(f"    [OK] Validado - diferenca < R$ {TOL_RESULTADO:.0f}")
    else:
        L("    [!!] Divergencia - rever arredondamento ou linhas excluidas do Resultado")

    # --- Parte 2: SKUs
    L("")
    L(sep)
    L("ANALISE POR SKU (CUSTO_OK)")
    L(sep)
    sku_analise = (
        ms_ok.groupby("SKU_Normalizado", dropna=False)
        .agg(Vl_Venda=("Vl_Venda", "sum"), Custo_Produto_Total=("Custo_Produto_Total", "sum"), Resultado=("Resultado", "sum"), Quantidade=("Quantidade", "sum"))
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

    # --- Parte 3: outras empresas
    L("")
    L(sep)
    L("COMPARACAO ENTRE EMPRESAS - MARCO/%d (NF, CUSTO_OK)" % y)
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
    ms_row = emp_a.loc[emp_a["org_id"].astype(str).eq(oid)]
    ms_custo_pct = float(ms_row["Custo_pct"].iloc[0]) if len(ms_row) else 0.0
    L("")
    L(f"Media Custo % (entre orgs na lista): {media_custo_pct:.1f}%")
    L(f"Mega Star Custo %: {ms_custo_pct:.1f}%")
    L("Desvio (MS - media): %+.1f p.p." % (ms_custo_pct - media_custo_pct))

    # --- Parte 4: CSV vs dataset
    L("")
    L(sep)
    L("CONSISTENCIA - CSV PEDIDOS (Data marco) vs DATASET (Vl_Venda, NF marco)")
    L(sep)
    receita_ds_nf = float(_num(ms["Vl_Venda"]).sum())
    if args.pedidos_dir.is_dir():
        raw, meta = load_all_pedidos_csv_concatenated(args.pedidos_dir)
        ped = _normalize_pedidos_export(raw)
        mask_d = _month_mask_pedido(ped["Data"], y, m)
        csv_m = ped.loc[mask_d]
        lista_qtd = to_numeric_br(csv_m["Preço de lista"]) * to_numeric_br(csv_m["Quantidade"]) if "Preço de lista" in csv_m.columns else None
        receita_csv = float(lista_qtd.sum()) if lista_qtd is not None else 0.0
        L("Pedidos dir: %s" % args.pedidos_dir.resolve())
        L("CSV linhas (Data marco): %d" % len(csv_m))
        L("Dataset linhas (NF marco, todas): %d" % len(ms))
        L(f"Receita CSV (lista x qtd):     {money(receita_csv)}")
        L(f"Receita dataset (NF marco):    {money(receita_ds_nf)}")
        L(f"Diferenca (CSV - dataset NF):  {money(receita_csv - receita_ds_nf)}")
        L("(Nota: CSV por competencia pedido; dataset por emissao NF - diferenca esperada.)")
    else:
        L("[SKIP] Pasta pedidos inexistente: %s" % args.pedidos_dir)

    # --- Parte 5: checklist
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
        if c in ms.columns
    ]
    if dup_cols:
        dups = int(ms.duplicated(subset=dup_cols).sum())
    else:
        dups = 0
    checklist.append(
        "[%s] Duplicados (chave NF+SKU+Qtd+Pedido): %d"
        % ("OK" if dups == 0 else "!!", dups)
    )
    nulos = 0
    for col in ("Vl_Venda", "Custo_Produto_Total", "Resultado"):
        if col in ms_ok.columns:
            nulos += int(ms_ok[col].isna().sum())
    checklist.append("[%s] Nulos em Vl_Venda/Custo/Resultado (CUSTO_OK): %d" % ("OK" if nulos == 0 else "!!", nulos))
    outliers = int((sku_analise["Custo_pct"] > 100).sum())
    checklist.append("[%s] SKUs com Custo%% > 100: %d" % ("OK" if outliers == 0 else "!!", outliers))
    margem_cli = (resultado_dataset / receita * 100.0) if receita > 0 else 0.0
    checklist.append("[%s] Margem resultado: %.1f%% (alerta se < -10%%)" % ("OK" if margem_cli > -10.0 else "!!", margem_cli))

    data_proc = "N/A"
    if args.metadata.is_file():
        try:
            meta_j = json.loads(args.metadata.read_text(encoding="utf-8"))
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

    # --- Parte 6: relatorio executivo (texto simples)
    n_skus_neg = int((sku_analise["Resultado"] < 0).sum())
    prej_skus = float(sku_analise.loc[sku_analise["Resultado"] < 0, "Resultado"].sum())
    custo_pct_ms = (d_custo / receita * 100.0) if receita > 0 else 0.0
    outras_ded = total_d - d_custo
    outras_pct = (outras_ded / receita * 100.0) if receita > 0 else 0.0

    L("")
    L(sep)
    L("RELATORIO EXECUTIVO (rascunho - rever antes de enviar ao cliente)")
    L(sep)
    L("")
    L("Periodo: emissao de NF em %02d/%d (fuso America/Sao_Paulo)." % (m, y))
    L("Ultima materializacao dataset: %s" % data_proc)
    L("")
    L("Resumo financeiro (apenas linhas CUSTO_OK):")
    L(f"  Receita (Vl_Venda):           {money(receita)}")
    L(f"  Custo produto (total):       -{money(d_custo)}  ({custo_pct_ms:.1f}% da receita)")
    L(f"  Demais deducoes (soma):      -{money(outras_ded)}  ({outras_pct:.1f}% da receita)")
    L(f"  Resultado (dataset):          {money(resultado_dataset)}  ({margem_cli:.1f}% margem)")
    L("")
    L("Validacao interna:")
    L("  - Coluna de custo por empresa: ver documentacao / custo_por_empresa (Mega Star: STAR/GAMA).")
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

    out_text = "\n".join(lines) + "\n"
    print(out_text, end="")
    if args.save_txt:
        args.save_txt.parent.mkdir(parents=True, exist_ok=True)
        args.save_txt.write_text(out_text, encoding="utf-8")
        print("Guardado: %s" % args.save_txt.resolve(), file=sys.stderr)

    return 0 if n_warn == 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
