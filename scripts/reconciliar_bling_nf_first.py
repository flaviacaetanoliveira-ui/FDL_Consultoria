#!/usr/bin/env python3
"""
Confronta o export de notas fiscais de saída do Bling com ``dataset_faturamento_nf.parquet``
(e opcionalmente o grão linha) para explicar divergências de contagem e soma.

Uso (na raiz do repositório):
  python scripts/reconciliar_bling_nf_first.py \\
    --bling "C:/caminho/notas_saida_bling.csv" \\
    --parquet "C:/caminho/dataset_faturamento_nf.parquet" \\
    --d-ini 2026-01-01 --d-fim 2026-01-05 \\
    --empresa "Esquilo"

Opcional:
  --line "C:/caminho/dataset_faturamento_app.csv"
  --saida-csv "C:/caminho/nf_somente_bling.csv"
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from faturamento_dre_recorte import _fdl_fr_mask_nf_emissao_no_periodo  # noqa: E402
from processing.faturamento.io_notas_saida import (  # noqa: E402
    _read_notas_file,
    detectar_col_data_emissao,
    detectar_col_valor_total_liquido,
    filtrar_notas_canceladas,
)
from processing.faturamento.normalize import normalize_pedido_join_key, to_numeric_br  # noqa: E402


def nf_key_reconciliacao(raw: object) -> str:
    """
    Chave canónica para cruzar Bling ↔ materializado.

    Aplica ``normalize_pedido_join_key`` (ex.: remove sufixo Excel ``.0``) e, se o resultado for
    só dígitos, remove zeros à esquerda — alinhado ao ``Nota_Numero_Normalizado`` típico no Parquet
    (ex.: Bling ``038476`` ↔ app ``38476``).
    """
    s = normalize_pedido_join_key(pd.Series([str(raw)])).iloc[0]
    if not s:
        return ""
    if "." in s and s.replace(".", "").replace("-", "").isdigit():
        s = s.split(".")[0]
    return s.lstrip("0") if s.isdigit() else s


def _fmt_brl(x: float) -> str:
    s = f"{x:,.2f}"
    return s.replace(",", "X").replace(".", ",").replace("X", ".")


def _find_col_numero_nf(columns: list[str]) -> str:
    if "Número" in columns:
        return "Número"
    for c in columns:
        cl = str(c).strip().lower()
        if cl in ("numero", "número", "nr nota", "nr_nota") and "pedido" not in cl:
            return c
    return ""


def _find_col_valor_total_bruto(columns: list[str], col_liq: str) -> str:
    for c in columns:
        if c == col_liq:
            continue
        n = str(c).strip().lower()
        if "valor" in n and "total" in n and "liquido" not in n and "líquido" not in n:
            return c
    if "Valor total" in columns and col_liq != "Valor total":
        return "Valor total"
    return ""


def _find_col_empresa(columns: list[str]) -> str:
    for c in columns:
        if str(c).strip().casefold() == "empresa":
            return c
    return ""


def _find_col_situacao(columns: list[str]) -> str:
    for c in columns:
        n = c.lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            return c
    return ""


def agregar_bling_raw(raw: pd.DataFrame, *, arquivo: str) -> tuple[pd.DataFrame, dict[str, str]]:
    """1 linha por NF; ``nf_key`` = :func:`nf_key_reconciliacao` (cruzamento com materializado)."""
    cols = list(raw.columns)
    col_nf = _find_col_numero_nf(cols)
    if not col_nf:
        raise SystemExit("Bling: coluna do número da NF não encontrada (esperado «Número» ou similar).")
    col_dt = detectar_col_data_emissao(cols)
    if not col_dt:
        raise SystemExit("Bling: coluna de data de emissão não encontrada.")
    col_liq = detectar_col_valor_total_liquido(cols) or (
        "Valor total" if "Valor total" in cols else ""
    )
    if not col_liq:
        raise SystemExit("Bling: coluna de valor líquido / total não encontrada.")
    col_br = _find_col_valor_total_bruto(cols, col_liq)
    col_emp = _find_col_empresa(cols)
    col_sit = _find_col_situacao(cols)

    prep = raw.copy()
    prep["_nf_key"] = prep[col_nf].map(nf_key_reconciliacao)
    prep["_dt"] = pd.to_datetime(prep[col_dt], errors="coerce", dayfirst=True)
    prep["_vl_liq"] = to_numeric_br(prep[col_liq])
    if col_br and col_br in prep.columns:
        prep["_vl_bruto"] = to_numeric_br(prep[col_br])
    else:
        prep["_vl_bruto"] = np.nan
    prep["_sit"] = prep[col_sit].astype(str).str.strip() if col_sit else ""
    prep["_emp"] = prep[col_emp].astype(str).str.strip() if col_emp else ""
    prep = prep[prep["_nf_key"].ne("")].copy()

    meta = {
        "arquivo": arquivo,
        "col_numero": col_nf,
        "col_emissao": col_dt,
        "col_valor_liquido": col_liq,
        "col_valor_bruto": col_br or "(não detetada)",
        "col_empresa": col_emp or "(não detetada)",
        "col_situacao": col_sit or "(não detetada)",
    }

    agg_spec: dict[str, str] = {
        "_dt": "min",
        "_vl_liq": "sum",
        "_sit": "first",
        "_emp": "first",
    }
    if col_br and col_br in prep.columns:
        agg_spec["_vl_bruto"] = "sum"

    g = prep.groupby("_nf_key", sort=False).agg(agg_spec).reset_index()
    g = g.rename(
        columns={
            "_nf_key": "nf_key",
            "_dt": "data_emissao",
            "_vl_liq": "valor_liquido_bling",
            "_sit": "situacao_bling",
            "_emp": "empresa_bling",
        }
    )
    if col_br and col_br in prep.columns:
        g = g.rename(columns={"_vl_bruto": "valor_bruto_bling"})
    else:
        g["valor_bruto_bling"] = np.nan
    return g, meta


def _filtrar_empresa(df: pd.DataFrame, col: str, empresas: tuple[str, ...]) -> pd.DataFrame:
    if not empresas or col not in df.columns:
        return df
    want = {str(e).strip().casefold() for e in empresas if str(e).strip()}
    if not want:
        return df
    s = df[col].fillna("").astype(str).str.strip().str.casefold()
    return df.loc[s.isin(want)].copy()


def carregar_nf_first(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    if "Nota_Numero_Normalizado" not in df.columns:
        raise SystemExit("Parquet NF-first: falta coluna Nota_Numero_Normalizado.")
    df = df.copy()
    df["nf_key"] = df["Nota_Numero_Normalizado"].map(nf_key_reconciliacao)
    return df


def carregar_grao_linha(path: Path) -> pd.DataFrame:
    p = path.expanduser().resolve()
    if p.suffix.lower() in {".xlsx", ".xls"}:
        df = pd.read_excel(p)
    else:
        df = None
        for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
            for sep in (";", ",", "\t"):
                try:
                    df = pd.read_csv(p, encoding=enc, sep=sep, low_memory=False)
                    if len(df.columns) > 5:
                        break
                except Exception:
                    df = None
            if df is not None and len(df.columns) > 5:
                break
        if df is None:
            df = pd.read_csv(p, low_memory=False)
    if "Nota_Numero_Normalizado" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["_nf_key"] = df["Nota_Numero_Normalizado"].map(nf_key_reconciliacao)
    return df


def classificar_exclusao(
    nf_key: str,
    *,
    bling_recorte: pd.DataFrame,
    parquet_tudo: pd.DataFrame,
    parquet_recorte: pd.DataFrame,
    line_df: pd.DataFrame | None,
) -> str:
    pr_keys = set(parquet_recorte["nf_key"].astype(str))
    pt_keys = set(parquet_tudo["nf_key"].astype(str))
    if nf_key in pr_keys:
        return "(presente no recorte app — não devia estar só no Bling)"

    row_b = bling_recorte.loc[bling_recorte["nf_key"].astype(str) == nf_key]
    sit_b = str(row_b["situacao_bling"].iloc[0]).lower() if len(row_b) else ""
    if any(x in sit_b for x in ("cancel", "deneg", "inutil")):
        return "Situação no relatório Bling: cancelada / denegada / inutilizada (excluída no pipeline app)."

    if nf_key in pt_keys:
        return "NF existe no Parquet NF-first mas fora do período de emissão / recorte (data ou filtros)."

    if line_df is None or line_df.empty:
        return "Sem grão linha para auditar: provável ausência de linhas de pedido com esta NF no materializado linha (universo NF-first)."

    sub = line_df.loc[line_df["_nf_key"].astype(str) == nf_key]
    if sub.empty:
        return "Sem vínculo: nenhuma linha no dataset linha com Nota_Numero_Normalizado = esta NF (não entra no NF-first)."

    if "Nota_Situacao" in sub.columns:
        sit = sub["Nota_Situacao"].fillna("").astype(str).str.lower()
        if sit.str.contains("cancel|deneg|inutil", regex=True).any():
            return "Situação excluída no grão linha (cancelada/denegada/inutilizada) — filtrada na agregação NF-first."

    nn = sub["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    if nn.eq("").all():
        return "Linhas de pedido sem Nota_Numero_Normalizado preenchido para esta venda."

    return (
        "NF aparece no grão linha mas não no Parquet NF-first: rever materialização (build), "
        "empresa/org no pipeline ou desatualização do Parquet face ao CSV linha."
    )


def main() -> None:
    ap = argparse.ArgumentParser(description="Reconcilia Bling (saída) vs dataset_faturamento_nf.parquet.")
    ap.add_argument("--bling", type=Path, required=True, help="CSV ou XLSX export notas saída Bling")
    ap.add_argument("--parquet", type=Path, required=True, help="dataset_faturamento_nf.parquet")
    ap.add_argument("--line", type=Path, default=None, help="Opcional: dataset_faturamento_app.csv (grão linha)")
    ap.add_argument("--d-ini", type=lambda s: date.fromisoformat(s), required=True, help="YYYY-MM-DD")
    ap.add_argument("--d-fim", type=lambda s: date.fromisoformat(s), required=True, help="YYYY-MM-DD")
    ap.add_argument(
        "--empresa",
        action="append",
        default=[],
        help="Filtrar por etiqueta empresa (pode repetir). Vazio no app = todas.",
    )
    ap.add_argument(
        "--bling-sem-filtrar-canceladas",
        action="store_true",
        help="Não aplicar filtrar_notas_canceladas (default: aplicar, alinhado ao pipeline).",
    )
    ap.add_argument("--saida-csv", type=Path, default=None, help="Opcional: CSV com NFs só no Bling + motivo")
    args = ap.parse_args()

    path_bling = args.bling.expanduser().resolve()
    path_pq = args.parquet.expanduser().resolve()
    if not path_bling.is_file():
        raise SystemExit(f"Ficheiro Bling não encontrado: {path_bling}")
    if not path_pq.is_file():
        raise SystemExit(f"Parquet não encontrado: {path_pq}")

    raw_bling = _read_notas_file(path_bling).dropna(axis=1, how="all")
    if not args.bling_sem_filtrar_canceladas:
        raw_bling = filtrar_notas_canceladas(raw_bling)

    bling_full, meta = agregar_bling_raw(raw_bling, arquivo=str(path_bling))
    empresas = tuple(args.empresa) if args.empresa else ()

    line_df: pd.DataFrame | None = None
    if args.line:
        lp = args.line.expanduser().resolve()
        if lp.is_file():
            line_df = carregar_grao_linha(lp)
            if line_df.empty:
                print(f"Aviso: grão linha sem Nota_Numero_Normalizado ou vazio: {lp}", file=sys.stderr)
                line_df = None
            elif empresas and "empresa" in line_df.columns:
                _n0 = len(line_df)
                line_df = _filtrar_empresa(line_df, "empresa", empresas)
                if line_df.empty and _n0:
                    print(
                        "Aviso: grão linha sem linhas após filtro --empresa (motivos de exclusão usam só esta marca).",
                        file=sys.stderr,
                    )
        else:
            print(f"Aviso: ficheiro linha não encontrado: {lp}", file=sys.stderr)

    bling_u = bling_full.copy()
    if empresas:
        if "empresa_bling" in bling_u.columns and bling_u["empresa_bling"].astype(str).str.strip().ne("").any():
            bling_u = _filtrar_empresa(bling_u, "empresa_bling", empresas)
        else:
            print(
                "Aviso: filtro --empresa pedido mas coluna Empresa vazia/ausente no Bling; "
                "recorte Bling não filtrado por empresa (compare com o app).",
                file=sys.stderr,
            )

    m_b = _fdl_fr_mask_nf_emissao_no_periodo(bling_u["data_emissao"], args.d_ini, args.d_fim)
    bling_recorte = bling_u.loc[m_b].copy()

    pq = carregar_nf_first(path_pq)
    pq_u = pq.copy()
    if empresas and "empresa" in pq_u.columns:
        pq_u = _filtrar_empresa(pq_u, "empresa", empresas)

    m_p = _fdl_fr_mask_nf_emissao_no_periodo(pq_u["Nota_Data_Emissao"], args.d_ini, args.d_fim)
    pq_recorte = pq_u.loc[m_p].copy()
    pq_recorte_dedup = pq_recorte.drop_duplicates(subset=["nf_key"], keep="first")

    keys_b = set(bling_recorte["nf_key"].astype(str))
    keys_p = set(pq_recorte_dedup["nf_key"].astype(str))
    so_bling = sorted(keys_b - keys_p)
    so_app = sorted(keys_p - keys_b)

    sum_b_liq = float(pd.to_numeric(bling_recorte["valor_liquido_bling"], errors="coerce").fillna(0.0).sum())
    sum_b_br = float(pd.to_numeric(bling_recorte["valor_bruto_bling"], errors="coerce").fillna(0.0).sum())
    sum_p_vf = float(
        pd.to_numeric(pq_recorte_dedup["valor_faturado_nf"], errors="coerce").fillna(0.0).sum()
    )

    print("=== Colunas detetadas (Bling) ===")
    for k, v in meta.items():
        print(f"  {k}: {v}")
    print()
    print("=== Recorte ===")
    print(f"  Período emissão: {args.d_ini.isoformat()} a {args.d_fim.isoformat()} (inclusive, dia civil; app = mesma regra)")
    print(f"  Empresa(s) filtro: {empresas if empresas else '(todas)'}")
    print(
        f"  Bling: canceladas/denegadas/inutilizadas {'mantidas' if args.bling_sem_filtrar_canceladas else 'removidas (como no pipeline)'}"
    )
    print()
    print("=== Contagens (NFs distintas, chave normalizada) ===")
    print(f"  NFs no Bling (recorte):       {len(bling_recorte)}")
    print(f"  NFs no app Parquet (recorte): {len(pq_recorte_dedup)}")
    print(f"  Só no Bling: {len(so_bling)}  |  Só no app: {len(so_app)}")
    print()
    print("=== Somas (recorte) ===")
    print(f"  Bling soma valor liquido (col. detetada): R$ {_fmt_brl(sum_b_liq)}")
    if not (np.isnan(sum_b_br) or sum_b_br == 0.0):
        print(f"  Bling soma valor bruto (se coluna existir): R$ {_fmt_brl(sum_b_br)}")
    print(f"  App soma valor_faturado_nf:                R$ {_fmt_brl(sum_p_vf)}")
    print(f"  Delta (Bling liquido - app):               R$ {_fmt_brl(sum_b_liq - sum_p_vf)}")
    print()

    n_mismatch = 0
    merged = bling_recorte.merge(
        pq_recorte_dedup[["nf_key", "valor_faturado_nf", "Nota_Data_Emissao", "empresa"]],
        on="nf_key",
        how="inner",
        suffixes=("_b", "_p"),
    )
    if len(merged):
        merged["_delta_v"] = pd.to_numeric(merged["valor_liquido_bling"], errors="coerce").fillna(0.0) - pd.to_numeric(
            merged["valor_faturado_nf"], errors="coerce"
        ).fillna(0.0)
        n_mismatch = int((merged["_delta_v"].abs() > 0.02).sum())
        print(f"=== NFs em ambos ({len(merged)}): diferença valor líquido Bling vs valor_faturado_nf ===")
        print(f"  Linhas com |delta| > 0,02: {n_mismatch}")
        if n_mismatch:
            bad = merged.loc[merged["_delta_v"].abs() > 0.02].copy()
            bad["_abs"] = bad["_delta_v"].abs()
            bad = bad.sort_values("_abs", ascending=False).head(15)
            print("  Exemplos (até 15):")
            for _, r in bad.iterrows():
                print(
                    f"    NF {r['nf_key']}: Bling {float(r['valor_liquido_bling']):.2f} vs app {float(r['valor_faturado_nf']):.2f}  d={float(r['_delta_v']):.2f}"
                )
        print()

    print("=== NFs só no Bling (até 40 exemplos + motivo) ===")
    rows_motivo: list[dict[str, object]] = []
    for nk in so_bling[:40]:
        mot = classificar_exclusao(
            nk,
            bling_recorte=bling_recorte,
            parquet_tudo=pq_u,
            parquet_recorte=pq_recorte_dedup,
            line_df=line_df,
        )
        row_b = bling_recorte.loc[bling_recorte["nf_key"] == nk].iloc[0]
        print(f"  {nk} | R$ {float(row_b['valor_liquido_bling']):.2f} | {mot}")
        rows_motivo.append(
            {
                "nf_key": nk,
                "valor_liquido_bling": row_b["valor_liquido_bling"],
                "data_emissao": row_b["data_emissao"],
                "situacao_bling": row_b["situacao_bling"],
                "empresa_bling": row_b.get("empresa_bling", ""),
                "motivo_exclusao_app": mot,
            }
        )
    if len(so_bling) > 40:
        print(f"  ... +{len(so_bling) - 40} NFs (use --saida-csv para lista completa)")
    print()

    if so_bling:
        _motivos = [
            classificar_exclusao(
                nk,
                bling_recorte=bling_recorte,
                parquet_tudo=pq_u,
                parquet_recorte=pq_recorte_dedup,
                line_df=line_df,
            )
            for nk in so_bling
        ]
        _cnt = Counter(_motivos)
        print("=== Resumo motivos (NFs so no Bling) ===")
        for mot, n in _cnt.most_common():
            print(f"  ({n}x) {mot}")
        print()

    if so_app:
        print(f"=== NFs só no app ({min(len(so_app), 20)} de {len(so_app)}) ===")
        for nk in so_app[:20]:
            r = pq_recorte_dedup.loc[pq_recorte_dedup["nf_key"] == nk].iloc[0]
            print(f"  {nk} | valor_faturado_nf R$ {float(r['valor_faturado_nf']):.2f}")
        print()

    if args.saida_csv:
        full_rows = []
        for nk in so_bling:
            mot = classificar_exclusao(
                nk,
                bling_recorte=bling_recorte,
                parquet_tudo=pq_u,
                parquet_recorte=pq_recorte_dedup,
                line_df=line_df,
            )
            row_b = bling_recorte.loc[bling_recorte["nf_key"] == nk].iloc[0]
            full_rows.append(
                {
                    "nf_key": nk,
                    "valor_liquido_bling": row_b["valor_liquido_bling"],
                    "data_emissao": row_b["data_emissao"],
                    "situacao_bling": row_b["situacao_bling"],
                    "empresa_bling": row_b.get("empresa_bling", ""),
                    "motivo_exclusao_app": mot,
                }
            )
        pd.DataFrame(full_rows).to_csv(args.saida_csv, index=False, encoding="utf-8-sig")
        print(f"CSV escrito: {args.saida_csv.resolve()}")

    print("=== Conclusão (automática) ===")
    if len(so_bling) and (sum_b_liq - sum_p_vf) > 1.0:
        print(
            "  A divergência de soma é explicada em grande parte por NFs presentes no Bling e ausentes do recorte Parquet "
            f"({len(so_bling)} NF(s)). Isto é coerente com o contrato NF-first (notas sem linha no materializado linha "
            "não entram). Passar --line reforça o diagnóstico por NF."
        )
    elif n_mismatch:
        print(
            "  Há NFs comuns com valores diferentes: confrontar coluna de valor no export Bling vs "
            "Nota_Valor_Liquido_Total no pipeline / materialização."
        )
    else:
        print("  Recorte e totais alinhados entre Bling (líquido) e app no universo comparável.")


if __name__ == "__main__":
    main()
