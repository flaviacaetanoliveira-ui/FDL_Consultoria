"""Relatório único de validação Cliente_4 (gerado para esta rodada)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
CLIENTE = Path(r"C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Flavio\Cliente_4")
OUT = REPO / "tests" / "_validacao_cliente4"

sys.path.insert(0, str(REPO))
from processing.faturamento.io_notas_saida import filtrar_notas_canceladas, load_notas_saida_from_dir


def main() -> None:
    df = pd.read_parquet(OUT / "dataset_validacao.parquet")

    print("=== PATHS (estrutura real usada nesta validação) ===")
    for org, sub in [("Esquilo", "Esquilo"), ("Wood", "Wood")]:
        print(f"  {org} — Pedidos:     {CLIENTE / sub / 'Pedidos' / '2026'}")
        print(f"  {org} — notas_saida: {CLIENTE / sub / 'notas_saida'}")

    print("\n=== CABEÇALHOS NOTAS (primeiro CSV encontrado por empresa, rglob) ===")
    for org in ("Esquilo", "Wood"):
        nd = CLIENTE / org / "notas_saida"
        files = sorted(nd.rglob("*.csv"))
        if not files:
            print(f"  [{org}] (sem CSV)")
            continue
        f = files[0]
        h = pd.read_csv(f, sep=";", encoding="utf-8-sig", nrows=0, engine="python")
        print(f"  [{org}] ficheiro: {f.relative_to(CLIENTE)}")
        print(f"       colunas ({len(h.columns)}): {list(h.columns)}")

    print("\n=== NOTAS CANCELADAS (ficheiro Esquilo Mar 2026) ===")
    p = CLIENTE / "Esquilo" / "notas_saida" / "2026" / "Esquilo - Saídas Mar 2026.csv"
    raw = pd.read_csv(p, sep=";", encoding="utf-8-sig", dtype=str, engine="python")
    col_s = next((c for c in raw.columns if "situa" in c.lower() or "status" in c.lower()), None)
    if col_s:
        s = raw[col_s].fillna("").astype(str).str.lower()
        n_bad = int(s.str.contains("cancel|deneg|inutil", regex=True).sum())
        print(f"  Linhas no CSV: {len(raw)} | linhas com texto cancel/deneg/inutil em «{col_s}»: {n_bad}")
    fil = filtrar_notas_canceladas(raw.copy())
    print(f"  Após filtrar_notas_canceladas: {len(fil)} linhas (removidas {len(raw) - len(fil)})")

    print("\n=== SEM NOTA => IMPOSTO 0 ===")
    m = df["faturamento_nota_vinculada"].fillna(False)
    sem = df[~m]
    imp_sem = pd.to_numeric(sem["Imposto"], errors="coerce").fillna(0.0)
    print(f"  Linhas sem vínculo NF: {len(sem)} | soma Imposto: {float(imp_sem.sum()):.8f}")

    print("\n=== 5 NOTAS (exemplos: prioridade pares org_id+NF com 2+ linhas de pedido) ===")
    sub = df[m].copy()
    sub["nf"] = sub["Nota_Numero_Normalizado"].astype(str)
    vc = sub.groupby(["org_id", "nf"], sort=False).size()
    multi_idx = [(str(o), str(n)) for (o, n) in vc[vc >= 2].index.tolist() if str(n).strip()]
    single_idx = [(str(o), str(n)) for (o, n) in vc[vc == 1].index.tolist() if str(n).strip()]
    picked_pairs: list[tuple[str, str]] = []
    for pair in multi_idx[:4]:
        picked_pairs.append(pair)
    for pair in single_idx:
        if len(picked_pairs) >= 5:
            break
        if pair not in picked_pairs:
            picked_pairs.append(pair)

    # Situação da NF: chave (org_id, numero NF) — nunca só o número isolado no consolidado
    folder_to_org_id = {"Esquilo": "esquilo", "Wood": "wood"}
    situ_map: dict[tuple[str, str], str] = {}
    for org_folder, oid in folder_to_org_id.items():
        nd = CLIENTE / org_folder / "notas_saida" / "2026"
        if not nd.is_dir():
            continue
        for fp in nd.glob("*.csv"):
            nm = pd.read_csv(fp, sep=";", encoding="utf-8-sig", dtype=str, engine="python")
            col_num = "Número" if "Número" in nm.columns else None
            col_sit = next((c for c in nm.columns if "situa" in c.lower()), None)
            if col_num and col_sit:
                for _, r in nm.iterrows():
                    situ_map[(oid, str(r[col_num]).strip())] = str(r[col_sit]).strip()

    for org_id, nf in picked_pairs[:5]:
        g = df[
            (df["org_id"].astype(str) == org_id) & (df["Nota_Numero_Normalizado"].astype(str) == nf)
        ].copy()
        if g.empty:
            continue
        ttot = float(pd.to_numeric(g["Nota_Valor_Liquido_Total"], errors="coerce").iloc[0])
        srate = float(pd.to_numeric(g["Nota_Valor_Liquido_Rateado"], errors="coerce").sum())
        simp = float(pd.to_numeric(g["Imposto"], errors="coerce").fillna(0).sum())
        situ = situ_map.get((org_id, nf.strip()), "(NF nao encontrada em notas_saida/2026)")
        print(f"\n--- org_id={org_id} | NF {nf} | Situacao nota: {situ}")
        print(f"    Linhas pedido: {len(g)} | Valor líquido total NF: {ttot:.2f}")
        print(f"    Sum valor liquido rateado: {srate:.2f} | delta: {srate - ttot:+.4f} | Sum Imposto: {simp:.2f}")
        cols = [
            "org_id",
            "Número do pedido",
            "Número do pedido multiloja",
            "Situação",
            "Data",
            "Vl_Venda",
            "Nota_Valor_Liquido_Total",
            "Nota_Rateio_Participacao",
            "Nota_Valor_Liquido_Rateado",
            "Aliquota_Imposto_Utilizada",
            "Imposto",
            "Despesas Fixas",
            "Aliquota_Despesas_Fixas_Utilizada",
        ]
        cols = [c for c in cols if c in g.columns]
        with pd.option_context("display.max_columns", None, "display.width", 240):
            print(g[cols].to_string(index=False))

    print("\n=== DESPESA FIXA vs competência da Data do pedido (10 linhas aleatórias) ===")
    samp = df.sample(min(10, len(df)), random_state=42)
    dt = pd.to_datetime(samp["Data"], errors="coerce", dayfirst=True)
    for i, r in samp.iterrows():
        comp = dt.loc[i].strftime("%Y-%m") if pd.notna(dt.loc[i]) else "—"
        vl = float(r["Vl_Venda"]) if pd.notna(r["Vl_Venda"]) else 0.0
        ad = float(r["Aliquota_Despesas_Fixas_Utilizada"]) if pd.notna(r["Aliquota_Despesas_Fixas_Utilizada"]) else 0.0
        df_fix = float(r["Despesas Fixas"]) if pd.notna(r["Despesas Fixas"]) else 0.0
        esperado = vl * ad
        ok = abs(df_fix - esperado) < 0.02
        print(
            f"  Data={r['Data']!s} comp={comp} Vl_Venda={vl:.2f} "
            f"aliq_desp={ad:.4f} DespFixas={df_fix:.2f} esperado_Vl_x_aliq={esperado:.2f} OK={ok}"
        )

    print("\n=== MATERIALIZADO (coerencia agregada) ===")
    gm = df[m]["Nota_Numero_Normalizado"].astype(str).str.strip().ne("")
    sub2 = df.loc[m & gm]
    fecho = (
        sub2.groupby(["org_id", "Nota_Numero_Normalizado"], as_index=False)
        .agg(
            linhas=("Nota_Valor_Liquido_Rateado", "count"),
            total_nf=("Nota_Valor_Liquido_Total", "first"),
            sum_rateado=("Nota_Valor_Liquido_Rateado", "sum"),
        )
    )
    fecho["delta"] = fecho["sum_rateado"] - pd.to_numeric(fecho["total_nf"], errors="coerce")
    worst = fecho["delta"].abs().max()
    print(f"  Pares (org_id, NF) com vinculo: {len(fecho)}")
    print(f"  Max abs(sum rateado - total NF) por org+NF: {float(worst):.6f} (esperado ~0 por arredondamento)")
    print(f"  Linhas totais dataset: {len(df)} | pipeline: faturamento-v3")


if __name__ == "__main__":
    main()
