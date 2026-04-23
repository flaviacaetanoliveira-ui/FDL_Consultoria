#!/usr/bin/env python3
"""
Validação prática do pipeline faturamento-v3: notas reais, rateio, imposto e despesa fixa.

Imprime:
  - cabeçalhos do primeiro ficheiro CSV/XLSX encontrado em cada pasta de notas usada no build;
  - amostra de 3–5 NFs com linhas de pedido, Vl_Venda, total líquido, participação, base, imposto;
  - fechamento Σ rateado vs total da nota e Σ imposto;
  - linhas sem nota (imposto 0);
  - amostra de competência da Data do pedido vs alíquota de despesa fixa utilizada.

Uso (na máquina onde existem cliente_root, notas e opcionalmente params_mensais):

  python scripts/validar_faturamento_notas_rateio.py --params ops/faturamento_params_cliente_5_flavio.json

  python scripts/validar_faturamento_notas_rateio.py --params caminho/para/faturamento_params.json --top-nf 5
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))


def _primeiro_arquivo_notas(notas_dir: Path) -> Path | None:
    if not notas_dir.is_dir():
        return None
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        found = sorted(notas_dir.rglob(ptn))
        for p in found:
            if p.is_file():
                return p
    return None


def _cabecalhos_arquivo(path: Path) -> list[str]:
    suf = path.suffix.lower()
    if suf in {".xlsx", ".xls"}:
        import pandas as pd

        return [str(c) for c in pd.read_excel(path, nrows=0).columns]
    import pandas as pd

    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t"):
            try:
                return [str(c) for c in pd.read_csv(path, encoding=enc, sep=sep, nrows=0, engine="python").columns]
            except Exception:  # noqa: BLE001
                continue
    return []


def main() -> int:
    ap = argparse.ArgumentParser(description="Validação faturamento notas + rateio")
    ap.add_argument("--params", type=Path, required=True, help="Caminho para faturamento_params.json")
    ap.add_argument("--top-nf", type=int, default=5, help="Quantas NFs detalhar (default 5)")
    args = ap.parse_args()
    params_path = args.params.expanduser().resolve()
    if not params_path.is_file():
        print(f"ERRO: ficheiro de params não encontrado: {params_path}", file=sys.stderr)
        return 1

    import pandas as pd

    from processing.faturamento.build import build_faturamento_dataset
    from processing.faturamento.params import load_faturamento_params, FaturamentoParamsV2

    raw_preview = json.loads(params_path.read_text(encoding="utf-8"))
    print("=== Params (trecho) ===")
    print(f"  params_path: {params_path}")
    print(f"  params_mensais (JSON): {raw_preview.get('params_mensais', '(omitido — fallback JSON)')}")
    print(f"  notas_saida_dir (JSON): {raw_preview.get('notas_saida_dir', 'notas_saida')}")

    p_union = load_faturamento_params(params_path)
    if not isinstance(p_union, FaturamentoParamsV2):
        print("ERRO: este script é para schema_version >= 2.", file=sys.stderr)
        return 1

    root = p_union.cliente_root
    print("\n=== Cabeçalhos reais — notas de saída (primeiro ficheiro por pasta) ===")
    seen: set[str] = set()
    rel_def = (p_union.notas_saida_dir or "notas_saida").strip() or "notas_saida"
    for emp in p_union.empresas:
        rel = (emp.notas_saida_dir or rel_def).strip() or rel_def
        nd = (root / rel).resolve()
        key = str(nd)
        if key in seen:
            continue
        seen.add(key)
        f = _primeiro_arquivo_notas(nd)
        if not f:
            print(f"  [{emp.org_id}] {nd} → (sem CSV/XLSX)")
            continue
        hdr = _cabecalhos_arquivo(f)
        print(f"  [{emp.org_id}] {f}")
        print(f"      colunas ({len(hdr)}): {hdr}")

    print("\n=== Build do dataset ===")
    try:
        df, meta = build_faturamento_dataset(params_path)
    except Exception as exc:  # noqa: BLE001
        print(f"ERRO no build: {exc}", file=sys.stderr)
        return 1

    print(f"  pipeline_revision: {meta.get('pipeline_revision')}")
    print(f"  linhas: {len(df)}")
    if meta.get("params_mensais_path"):
        print(f"  params_mensais_path: {meta.get('params_mensais_path')}")

    need = [
        "Nota_Numero_Normalizado",
        "Vl_Venda",
        "Nota_Valor_Liquido_Total",
        "Nota_Rateio_Participacao",
        "Nota_Valor_Liquido_Rateado",
        "Base_Imposto",
        "Imposto",
        "faturamento_nota_vinculada",
        "Número do pedido",
        "Número do pedido multiloja",
        "Data",
        "Aliquota_Despesas_Fixas_Utilizada",
        "Despesas Fixas",
    ]
    miss = [c for c in need if c not in df.columns]
    if miss:
        print(f"AVISO: colunas em falta no DataFrame: {miss}", file=sys.stderr)

    m_nf = df["faturamento_nota_vinculada"].fillna(False).astype(bool)
    sem = df.loc[~m_nf]
    print("\n=== Confirmações rápidas ===")
    if len(sem):
        imp_sem = pd.to_numeric(sem["Imposto"], errors="coerce").fillna(0.0)
        print(f"  Linhas sem nota: {len(sem)} | soma Imposto (esperado 0): {float(imp_sem.sum()):.6f}")
    else:
        print("  Linhas sem nota: 0 (todas com vínculo ou dataset só com NF)")

    # Despesa fixa: competência Data pedido (amostra)
    if "Data" in df.columns and "Aliquota_Despesas_Fixas_Utilizada" in df.columns:
        dt = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
        comp = dt.dt.strftime("%Y-%m")
        samp = df.loc[df.index[: min(5, len(df))], :].copy()
        samp["_comp_pedido"] = comp.loc[samp.index]
        print("  Amostra competência (Data pedido) → Aliquota_Despesas_Fixas_Utilizada:")
        for i, r in samp.iterrows():
            print(
                f"    linha {i}: Data={r.get('Data')} → competência={r.get('_comp_pedido')} | "
                f"alíq. desp.={r.get('Aliquota_Despesas_Fixas_Utilizada')} | "
                f"Despesas Fixas={r.get('Despesas Fixas')}"
            )

    sub = df.loc[m_nf & df["Nota_Numero_Normalizado"].astype(str).str.strip().ne("")]
    if sub.empty:
        print("\n=== Nenhuma linha com nota vinculada — não há NF para detalhar ===")
        return 0

    sub = sub.copy()
    sub["_nf"] = sub["Nota_Numero_Normalizado"].astype(str).str.strip()
    if "org_id" not in sub.columns:
        sub["org_id"] = ""
    vc = sub.groupby(["org_id", "_nf"], sort=False).size().sort_values(ascending=False)
    n_take = max(3, min(args.top_nf, len(vc)))
    top_pairs = list(vc.head(n_take).index)

    print(f"\n=== Detalhe de {len(top_pairs)} par(es) org_id+NF (amostra por volume de linhas) ===")
    for org_id, nf in top_pairs:
        g = df[
            (df["org_id"].astype(str).eq(str(org_id)))
            & (df["Nota_Numero_Normalizado"].astype(str).eq(str(nf)))
        ].copy()
        t_total = float(pd.to_numeric(g["Nota_Valor_Liquido_Total"], errors="coerce").fillna(0).iloc[0])
        s_rate = float(pd.to_numeric(g["Nota_Valor_Liquido_Rateado"], errors="coerce").fillna(0).sum())
        s_imp = float(pd.to_numeric(g["Imposto"], errors="coerce").fillna(0).sum())
        print(f"\n--- org_id={org_id!r} | NF={nf!r} | linhas pedido: {len(g)} ---")
        print(f"  Valor total líquido da nota (header): {t_total:.2f}")
        print(f"  Sum Nota_Valor_Liquido_Rateado:      {s_rate:.2f}  (delta {s_rate - t_total:+.4f})")
        print(f"  Sum Imposto:                         {s_imp:.2f}")
        cols_show = [
            "org_id",
            "Número do pedido",
            "Número do pedido multiloja",
            "Código",
            "Vl_Venda",
            "Nota_Valor_Liquido_Total",
            "Nota_Rateio_Participacao",
            "Nota_Valor_Liquido_Rateado",
            "Base_Imposto",
            "Aliquota_Imposto_Utilizada",
            "Imposto",
        ]
        cols_show = [c for c in cols_show if c in g.columns]
        with pd.option_context("display.max_columns", None, "display.width", 200):
            print(g[cols_show].to_string(index=True))

    print("\n=== Notas canceladas ===")
    print(
        "  O pipeline remove situações cancel/deneg/inutil **à entrada** dos ficheiros de notas "
        "(ver processing.faturamento.io_notas_saida.filtrar_notas_canceladas). "
        "Nenhuma linha removida aparece no DataFrame final."
    )
    print("\nConcluído.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
