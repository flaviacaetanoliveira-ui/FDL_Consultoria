"""
Compara agregados da DRE (Visão Geral) entre dois ``dataset.parquet`` por ``org_id``,
espelhando a lógica de ``_faturamento_agg_recorte`` em ``app_operacional.py`` (sem Streamlit).

Métricas: receita_bruta, receita_liquida, desconto_comercial, custo_produto, frete,
comissao_plataforma, imposto, despesas_fixas, outras_despesas, resultado (sum skipna),
margem_principal_pct, n_linhas, n_linhas_sem_custo_ok, pedidos_atendidos_distintos.

Notas de produto (impressas no fim e na folha «Notas» do Excel):
  - O app Faturamento & DRE carrega em modo **consolidado**; o recorte por empresa no UI é o multiselect
    do módulo, não a sidebar.
  - **Resultado** no materializado fica ``NaN`` quando ``Status_Custo != CUSTO_OK`` (``calc.py``); a soma
    na DRE usa ``skipna=True`` — remover só linhas sem custo pode **não alterar** o total de Resultado.
  - Opcional: ``--data-ini`` / ``--data-fim`` filtram pela coluna ``Data`` (se existir).

Uso:
  python scripts/compare_faturamento_dre_orgs.py \\
    --parquet-antes data_products/cliente_2/faturamento/current/dataset_baseline_20260417T120000Z.parquet \\
    --parquet-depois data_products/cliente_2/faturamento/current/dataset.parquet \\
    --org-ids mega_star,moveis_eap
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _num_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _painel_custo_produto_col(columns: list[str]) -> str | None:
    if "Custo_Produto_Total" in columns:
        return "Custo_Produto_Total"
    if "Custo do Produto" in columns:
        return "Custo do Produto"
    return None


def _painel_receita_series(df: pd.DataFrame, pl_col: str) -> pd.Series:
    if "Vl_Venda" in df.columns:
        return pd.to_numeric(df["Vl_Venda"], errors="coerce")
    if "Receita_Bruta" in df.columns:
        return pd.to_numeric(df["Receita_Bruta"], errors="coerce")
    if "Quantidade" in df.columns and pl_col in df.columns:
        return pd.to_numeric(df[pl_col], errors="coerce") * pd.to_numeric(df["Quantidade"], errors="coerce")
    if pl_col in df.columns:
        return pd.to_numeric(df[pl_col], errors="coerce")
    return pd.Series(float("nan"), index=df.index, dtype=float)


def _atendido_mask(df: pd.DataFrame) -> pd.Series:
    if "Situação" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    situ = df["Situação"].fillna("").astype(str).str.strip().str.casefold()
    return situ.eq("atendido")


def _pedido_id_series(df: pd.DataFrame) -> pd.Series:
    ml = df["Número do pedido multiloja"].fillna("").astype(str).str.strip()
    ped = df["Número do pedido"].fillna("").astype(str).str.strip()
    core = ml.mask(ml.eq(""), ped)
    if "org_id" in df.columns:
        oid = df["org_id"].fillna("").astype(str).str.strip()
        return oid + "|" + core
    return core


def agg_recorte(df: pd.DataFrame) -> dict[str, Any]:
    """Espelho de ``_faturamento_agg_recorte`` (app_operacional.py ~4674)."""
    out: dict[str, Any] = {
        "n_linhas": int(len(df)),
        "receita_bruta": 0.0,
        "desconto_comercial": 0.0,
        "receita_liquida": 0.0,
        "custo_produto": 0.0,
        "frete": 0.0,
        "frete_me": 0.0,
        "frete_tp": 0.0,
        "comissao_plataforma": 0.0,
        "imposto": 0.0,
        "despesas_fixas": 0.0,
        "outras_despesas": 0.0,
        "resultado": 0.0,
        "margem_principal_pct": float("nan"),
        "pedidos_atendidos_distintos": 0,
        "n_linhas_sem_custo_ok": 0,
        "diag_plug_rb_desc_vt": None,
    }
    if df.empty:
        return out
    pl_col = "Preço de lista"
    rb_s = _painel_receita_series(df, pl_col).fillna(0.0)
    out["receita_bruta"] = float(rb_s.sum())
    desc_col = "Desconto proporcional total"
    has_vt = "Valor total" in df.columns
    has_desc = desc_col in df.columns
    has_nvlr = "Nota_Valor_Liquido_Rateado" in df.columns
    vt_sum = float(_num_col(df, "Valor total").sum()) if has_vt else None
    desc_sum = float(_num_col(df, desc_col).sum()) if has_desc else None

    if vt_sum is not None:
        out["desconto_comercial"] = float(out["receita_bruta"] - vt_sum)
    elif desc_sum is not None:
        out["desconto_comercial"] = desc_sum
    else:
        out["desconto_comercial"] = 0.0

    if has_nvlr:
        out["receita_liquida"] = float(
            pd.to_numeric(df["Nota_Valor_Liquido_Rateado"], errors="coerce").fillna(0.0).sum()
        )
    elif vt_sum is not None:
        out["receita_liquida"] = vt_sum
    elif desc_sum is not None:
        out["receita_liquida"] = float(out["receita_bruta"] - desc_sum)
    else:
        out["receita_liquida"] = float(out["receita_bruta"])

    if has_vt and has_desc and vt_sum is not None and desc_sum is not None:
        out["diag_plug_rb_desc_vt"] = float(out["receita_bruta"] - desc_sum - vt_sum)

    ccol = _painel_custo_produto_col(list(df.columns))
    if ccol and ccol in df.columns:
        out["custo_produto"] = float(pd.to_numeric(df[ccol], errors="coerce").fillna(0.0).sum())
    if "Frete_Plataforma" in df.columns:
        out["frete"] = float(_num_col(df, "Frete_Plataforma").sum())
    else:
        out["frete"] = float(_num_col(df, "Custo de Frete").sum())
    if "Frete Mercado Envios" in df.columns and "Frete transportadora própria" in df.columns:
        out["frete_me"] = float(_num_col(df, "Frete Mercado Envios").sum())
        out["frete_tp"] = float(_num_col(df, "Frete transportadora própria").sum())
    else:
        out["frete_me"] = float(out["frete"])
        out["frete_tp"] = 0.0
    out["comissao_plataforma"] = float(_num_col(df, "Taxa de Comissão").sum())
    out["imposto"] = float(_num_col(df, "Imposto").sum())
    out["despesas_fixas"] = float(_num_col(df, "Despesas Fixas").sum())
    if "Outras Despesas" in df.columns:
        out["outras_despesas"] = float(_num_col(df, "Outras Despesas").sum())
    res_s = (
        pd.to_numeric(df["Resultado"], errors="coerce")
        if "Resultado" in df.columns
        else pd.Series(dtype=float)
    )
    out["resultado"] = float(res_s.sum(skipna=True))
    rb = float(out["receita_bruta"])
    if rb not in (0.0, -0.0) and not math.isnan(rb):
        out["margem_principal_pct"] = float(out["resultado"] / rb)
    m_at = _atendido_mask(df)
    if m_at.any():
        pids = _pedido_id_series(df.loc[m_at]).astype(str).str.strip()
        pids = pids[pids.ne("")]
        out["pedidos_atendidos_distintos"] = int(pids.nunique()) if len(pids) else 0
    try:
        from processing.faturamento.config import STATUS_CUSTO_OK
    except Exception:
        STATUS_CUSTO_OK = "CUSTO_OK"
    if "Status_Custo" in df.columns:
        sc = df["Status_Custo"].astype(str).str.strip()
        out["n_linhas_sem_custo_ok"] = int((~sc.eq(STATUS_CUSTO_OK)).sum())
    return out


def _apply_date_filter(df: pd.DataFrame, d_ini: str | None, d_fim: str | None) -> pd.DataFrame:
    if not d_ini and not d_fim:
        return df
    if "Data" not in df.columns:
        print("AVISO: sem coluna «Data» — filtro de datas ignorado.", file=sys.stderr)
        return df
    ts = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    m = pd.Series(True, index=df.index)
    if d_ini:
        di = pd.to_datetime(d_ini).normalize()
        m &= ts >= di
    if d_fim:
        df_ = pd.to_datetime(d_fim).normalize()
        m &= ts <= df_ + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    out = df.loc[m].copy()
    print(f"Recorte datas: {len(out)} linhas (de {len(df)})", file=sys.stderr)
    return out


def _scope_df(df: pd.DataFrame, org_id: str) -> pd.DataFrame:
    if "org_id" not in df.columns:
        return df.copy()
    return df.loc[df["org_id"].astype(str).str.strip().eq(org_id)].copy()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Baseline: antes de alterar regras, execute\n"
            "  python scripts/snapshot_faturamento_dataset_baseline.py\n"
            "e use o path gerado em --parquet-antes."
        ),
    )
    ap.add_argument("--parquet-antes", type=Path, required=True)
    ap.add_argument("--parquet-depois", type=Path, required=True)
    ap.add_argument(
        "--org-ids",
        type=str,
        default="mega_star,moveis_eap",
        help="Lista separada por vírgulas (default: mega_star,moveis_eap).",
    )
    ap.add_argument("--data-ini", type=str, default="", help="YYYY-MM-DD (coluna Data, se existir).")
    ap.add_argument("--data-fim", type=str, default="", help="YYYY-MM-DD inclusive.")
    ap.add_argument(
        "--out-dir",
        type=Path,
        default=ROOT / "data_products" / "cliente_2" / "faturamento" / "current",
        help="Pasta para gravar CSV/XLSX de comparação.",
    )
    args = ap.parse_args()

    pa = args.parquet_antes.resolve()
    pd_ = args.parquet_depois.resolve()
    if not pa.is_file() or not pd_.is_file():
        print("ERRO: --parquet-antes ou --parquet-depois inexistente.", file=sys.stderr)
        return 1

    org_ids = [x.strip() for x in str(args.org_ids).split(",") if x.strip()]
    d_ini = args.data_ini.strip() or None
    d_fim = args.data_fim.strip() or None

    df_a = pd.read_parquet(pa)
    df_b = pd.read_parquet(pd_)
    df_a = _apply_date_filter(df_a, d_ini, d_fim)
    df_b = _apply_date_filter(df_b, d_ini, d_fim)

    metrics_order = [
        "n_linhas",
        "receita_bruta",
        "receita_liquida",
        "desconto_comercial",
        "custo_produto",
        "frete",
        "comissao_plataforma",
        "imposto",
        "despesas_fixas",
        "outras_despesas",
        "resultado",
        "margem_principal_pct",
        "pedidos_atendidos_distintos",
        "n_linhas_sem_custo_ok",
    ]

    rows: list[dict[str, Any]] = []
    for oid in org_ids:
        a = _scope_df(df_a, oid)
        b = _scope_df(df_b, oid)
        ga = agg_recorte(a)
        gb = agg_recorte(b)
        for m in metrics_order:
            va, vb = ga[m], gb[m]
            if isinstance(va, float) and isinstance(vb, float):
                if m == "margem_principal_pct" and (math.isnan(va) or math.isnan(vb)):
                    delta = float("nan")
                else:
                    delta = float(vb) - float(va)
            elif va is None or vb is None:
                delta = None
            else:
                delta = int(vb) - int(va)  # type: ignore[arg-type]
            rows.append(
                {
                    "org_id": oid,
                    "metric": m,
                    "antes": va,
                    "depois": vb,
                    "delta": delta,
                }
            )

    out_df = pd.DataFrame(rows)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    stem = "dre_compare_mega_star_moveis_eap"
    csv_path = args.out_dir / f"{stem}.csv"
    xlsx_path = args.out_dir / f"{stem}.xlsx"
    out_df.to_csv(csv_path, index=False, encoding="utf-8-sig")

    notes = [
        "App Faturamento & DRE: modo consolidado; filtro por empresa = multiselect no módulo (não a sidebar).",
        "Resultado agregado = soma(Resultado, skipna=True). Linhas Status_Custo != CUSTO_OK têm Resultado NaN em processing/faturamento/calc.py — removê-las pode não mudar esse total.",
        "Cache Streamlit + path do materializado: confirmar que o app lê o mesmo dataset.parquet que este --parquet-depois.",
        f"Parquet antes: {pa}",
        f"Parquet depois: {pd_}",
        f"Filtro datas: data_ini={d_ini or '(nenhum)'} data_fim={d_fim or '(nenhum)'}",
    ]

    try:
        from openpyxl import Workbook

        wb = Workbook()
        ws = wb.active
        ws.title = "Comparacao"
        ws.append(list(out_df.columns))
        for r in out_df.itertuples(index=False):
            ws.append(list(r))
        wn = wb.create_sheet("Notas")
        for line in notes:
            wn.append([line])
        wb.save(xlsx_path)
    except Exception as ex:
        print(f"AVISO: Excel não gravado ({ex}); CSV disponível.", file=sys.stderr)

    def _fmt_cell(m: str, v: object) -> str:
        if m != "margem_principal_pct":
            return str(v)
        try:
            x = float(v)  # type: ignore[arg-type]
        except (TypeError, ValueError):
            return str(v)
        if math.isnan(x):
            return "nan"
        return f"{x * 100:.4f}%"

    print("=== Comparação DRE (agg_recorte) por org_id ===\n")
    hdr = f"{'metric':28} {'antes':>16} {'depois':>16} {'delta':>16}"
    for oid in org_ids:
        print(f"--- {oid} ---")
        print(hdr)
        sub = out_df[out_df["org_id"].eq(oid)]
        for m in metrics_order:
            r = sub[sub["metric"].eq(m)].iloc[0]
            av, bv, dv = r["antes"], r["depois"], r["delta"]
            print(
                f"  {m:28} {_fmt_cell(m, av):>16} {_fmt_cell(m, bv):>16} {_fmt_cell(m, dv):>16}"
            )
        print()

    print("Ficheiros:")
    print(f"  {csv_path}")
    if xlsx_path.is_file():
        print(f"  {xlsx_path}")
    print("\n--- Notas (produto / UI) ---")
    for line in notes:
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
