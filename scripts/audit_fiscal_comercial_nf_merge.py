#!/usr/bin/env python3
"""
Auditoria fiscal ↔ comercial (merge NF-first) com dados reais em disco.

Compara merge **estrito** (só org_id + empresa + NF) com merge **com fallback**
(empresa + NF quando o comercial tem org_id vazio).

Uso (na raiz do repositório V2):

  python scripts/audit_fiscal_comercial_nf_merge.py \\
    --nf-parquet "C:/caminho/dataset_faturamento_nf.parquet" \\
    --fiscal-parquet "C:/caminho/dataset_faturamento_fiscal.parquet"

Ou com faturamento_params.json (tenta ``data_products/<cliente_slug>/faturamento/current/``):

  python scripts/audit_fiscal_comercial_nf_merge.py --params ops/faturamento_params_cliente_5_diieg.json

Saída: resumo no stdout; opcionalmente CSVs em ``--out-dir``.

Não altera o aplicativo nem os Parquets — só leitura.
"""

from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import pandas as pd

warnings.filterwarnings("ignore", message="Downcasting object dtype arrays", category=FutureWarning)

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from processing.faturamento.normalize import (  # noqa: E402
    normalize_empresa_fiscal_commercial_join_key_scalar,
    normalize_nf_fiscal_commercial_join_key_scalar,
)
from processing.faturamento.fiscal_commercial_nf_merge import (  # noqa: E402
    merge_fiscal_base_with_commercial_nf_dataframe,
)

FISCAL_COLS_NEED = ("Nota_Numero_Normalizado", "Valor_Liquido_NF", "Nota_Data_Emissao", "empresa")
COMM_COLS_NEED = ("Nota_Numero_Normalizado", "empresa")


def _resolve_from_params(params_path: Path) -> tuple[Path | None, Path | None, str]:
    raw = json.loads(params_path.read_text(encoding="utf-8"))
    slug = str(raw.get("cliente_slug") or "").strip() or "cliente_5"
    base = _REPO_ROOT / "data_products" / slug / "faturamento" / "current"
    nf = base / "dataset_faturamento_nf.parquet"
    fi = base / "dataset_faturamento_fiscal.parquet"
    note = f"Tentativa canónica: {base}"
    return (nf if nf.is_file() else None, fi if fi.is_file() else None, note)


def _pedido_linked(s: object) -> bool:
    p = str(s).strip() if s is not None and not (isinstance(s, float) and pd.isna(s)) else ""
    return bool(p) and p != "—"


def _nf_key_series(df: pd.DataFrame) -> pd.Series:
    nn = df["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    je = df["empresa"].fillna("").astype(str).str.strip()
    jm = je.map(normalize_empresa_fiscal_commercial_join_key_scalar)
    jn = nn.map(normalize_nf_fiscal_commercial_join_key_scalar)
    return jm.astype(str) + "|" + jn.astype(str)


def _load_fiscal(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    miss = [c for c in FISCAL_COLS_NEED if c not in df.columns]
    if miss:
        raise SystemExit(f"Fiscal parquet sem colunas: {miss}. Tem: {list(df.columns)[:40]}…")
    if "org_id" not in df.columns:
        df = df.copy()
        df["org_id"] = ""
    if "Nota_Situacao" not in df.columns:
        df = df.copy()
        df["Nota_Situacao"] = ""
    return df


def _load_commercial(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path)
    miss = [c for c in COMM_COLS_NEED if c not in df.columns]
    if miss:
        raise SystemExit(f"NF parquet sem colunas: {miss}. Tem: {list(df.columns)[:40]}…")
    if "org_id" not in df.columns:
        df = df.copy()
        df["org_id"] = ""
    for c in (
        "valor_venda",
        "comissao",
        "receita_frete_tp",
        "tarifa_custo_envio",
        "imposto",
        "despesa_fixa",
        "resultado",
        "plataforma_resumo",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
        "faturamento_nota_vinculada",
    ):
        if c not in df.columns:
            if c == "plataforma_resumo" and "plataforma" in df.columns:
                continue
            if c == "n_linhas_pedido":
                df[c] = 0
            elif c == "faturamento_nota_vinculada":
                df[c] = True
            elif c in ("pedido_resumo", "produto_resumo", "plataforma_resumo"):
                df[c] = "—"
            else:
                df[c] = 0.0
    if "plataforma_resumo" not in df.columns and "plataforma" in df.columns:
        df = df.copy()
        df["plataforma_resumo"] = df["plataforma"].astype(str)
    return df


def _trace_line_csv(line_csv: Path, nf_literal: str) -> None:
    """Procura a NF no materializado linha (pedidos + colunas de nota)."""
    nk = normalize_nf_fiscal_commercial_join_key_scalar(nf_literal.strip())
    print("\n--- Rastreio materializado LINHA (dataset_faturamento_app.csv) ---")
    print(f"  Ficheiro: {line_csv.resolve()}")
    if not line_csv.is_file():
        print("  (ficheiro inexistente)")
        return
    header = pd.read_csv(line_csv, nrows=0, low_memory=False).columns.tolist()
    use_cols = [c for c in ("Nota_Numero_Normalizado", "Número da nota", "Número", "empresa", "org_id") if c in header]
    if not use_cols:
        print("  Sem colunas de nota reconhecidas.")
        return
    nota_cols = [c for c in use_cols if c in ("Nota_Numero_Normalizado", "Número da nota", "Número")]

    def _row_match_nf(r: pd.Series) -> bool:
        for c in nota_cols:
            if normalize_nf_fiscal_commercial_join_key_scalar(r.get(c)) == nk:
                return True
        return False

    total = 0
    for chunk in pd.read_csv(line_csv, usecols=use_cols, chunksize=100_000, low_memory=False):
        m = chunk.apply(_row_match_nf, axis=1)
        if m.any():
            total += int(m.sum())
            print(chunk.loc[m].head(15).to_string(index=False))
    print(f"  Total de linhas de pedido com esta NF: {total}")
    if total == 0:
        print(
            "  => Nenhum pedido no CSV linha com esta nota: o Parquet NF-first não pode agregar comercial."
        )


def _trace_nf(commercial: pd.DataFrame, fiscal: pd.DataFrame, nf_literal: str) -> None:
    keys = {nf_literal, normalize_nf_fiscal_commercial_join_key_scalar(nf_literal)}
    print("\n--- Rastreio NF solicitada ---")
    print(f"Chaves normalizadas (dígitos): {sorted(k for k in keys if k)}")
    for side, df in ("comercial", commercial), ("fiscal", fiscal):
        col = "Nota_Numero_Normalizado"
        m = df[col].fillna("").astype(str).str.strip().isin(keys) | df[col].fillna("").astype(str).map(
            normalize_nf_fiscal_commercial_join_key_scalar
        ).isin(keys)
        sub = df.loc[m]
        print(f"  {side}: {len(sub)} linha(s)")
        if len(sub):
            cols = [c for c in ("org_id", "empresa", "Nota_Numero_Normalizado", "pedido_resumo", "plataforma_resumo", "plataforma") if c in sub.columns]
            print(sub[cols].head(20).to_string(index=False))


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--params", type=Path, help="faturamento_params.json (resolve data_products/…/current)")
    ap.add_argument("--nf-parquet", type=Path, help="dataset_faturamento_nf.parquet")
    ap.add_argument("--fiscal-parquet", type=Path, help="dataset_faturamento_fiscal.parquet")
    ap.add_argument("--out-dir", type=Path, help="Grava CSVs de auditoria (opcional)")
    ap.add_argument("--trace-nf", type=str, default="", help="Ex.: 042480 — mostra linhas que batem no comercial/fiscal")
    args = ap.parse_args()

    nf_path = args.nf_parquet
    fi_path = args.fiscal_parquet
    note = ""
    if args.params:
        n2, f2, note = _resolve_from_params(args.params)
        if nf_path is None:
            nf_path = n2
        if fi_path is None:
            fi_path = f2
        print(f"Params: {args.params.resolve()}")
        if note:
            print(note)

    if nf_path is None or not nf_path.is_file():
        raise SystemExit(
            "Defina --nf-parquet ou --params com data_products/…/dataset_faturamento_nf.parquet disponível.\n"
            f"  Recebido: {nf_path}"
        )
    if fi_path is None or not fi_path.is_file():
        raise SystemExit(
            "Defina --fiscal-parquet ou --params com dataset_faturamento_fiscal.parquet disponível.\n"
            f"  Recebido: {fi_path}"
        )

    print(f"NF parquet:    {nf_path.resolve()}")
    print(f"Fiscal parquet: {fi_path.resolve()}")

    fiscal = _load_fiscal(fi_path)
    commercial = _load_commercial(nf_path)

    org_c = commercial["org_id"].fillna("").astype(str).str.strip()
    n_empty_org = int(org_c.eq("").sum())
    print(f"\nLinhas comerciais (grão NF): {len(commercial)}")
    print(f"  Com org_id vazio: {n_empty_org} ({100.0 * n_empty_org / max(len(commercial), 1):.1f}%)")

    print(f"\nLinhas fiscais (grão NF): {len(fiscal)}")

    out_strict = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, commercial, strict_org_only=True)
    out_full = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, commercial, strict_org_only=False)

    st_link = out_strict["pedido_resumo"].map(_pedido_linked)
    fu_link = out_full["pedido_resumo"].map(_pedido_linked)

    n_st = int(st_link.sum())
    n_fu = int(fu_link.sum())
    n_fiscal = len(fiscal)

    print("\n=== Resultado merge (fiscal <- comercial) ===")
    print(f"Com vínculo comercial (pedido preenchido, nao traco), merge ESTRITO:  {n_st} / {n_fiscal}")
    print(f"Com vínculo comercial (pedido preenchido, nao traco), com FALLBACK:   {n_fu} / {n_fiscal}")
    print(f"Ganho com fallback: {n_fu - n_st} nota(s) fiscal(is)")

    fixed = out_full.loc[fu_link & ~st_link].copy()
    if len(fixed):
        print(f"\nNotas fiscais que o fallback preenche ({len(fixed)}) — amostra até 30:")
        show = fixed[
            [
                "empresa",
                "org_id",
                "Nota_Numero_Normalizado",
                "pedido_resumo",
                "plataforma_resumo",
                "valor_venda",
            ]
        ].head(30)
        print(show.to_string(index=False))

    still = out_full.loc[~fu_link].copy()
    if len(still):
        print(f"\nSem vínculo comercial mesmo com fallback: {len(still)} (amostra até 25)")
        print(
            still[["empresa", "org_id", "Nota_Numero_Normalizado", "valor_faturado_nf"]]
            .head(25)
            .to_string(index=False)
        )

    fk = set(_nf_key_series(fiscal))
    ck = set(_nf_key_series(commercial))
    only_fiscal = fk - ck
    print(f"\nChaves (empresa_norm|nf_norm) só no fiscal (sem linha comercial): {len(only_fiscal)}")
    if len(only_fiscal) <= 15:
        for k in sorted(only_fiscal):
            print(f"  {k}")
    elif len(only_fiscal) > 0:
        for k in sorted(list(only_fiscal))[:15]:
            print(f"  {k}")
        print(f"  … (+{len(only_fiscal) - 15} mais)")

    if args.trace_nf.strip():
        _trace_nf(commercial, fiscal, args.trace_nf.strip())
        _trace_line_csv(nf_path.parent / "dataset_faturamento_app.csv", args.trace_nf.strip())

    if args.out_dir:
        args.out_dir.mkdir(parents=True, exist_ok=True)
        if len(fixed):
            fixed.to_csv(args.out_dir / "audit_merge_ganho_fallback.csv", index=False, encoding="utf-8-sig")
        if len(still):
            still.to_csv(args.out_dir / "audit_merge_sem_vinculo.csv", index=False, encoding="utf-8-sig")
        print(f"\nCSVs gravados em: {args.out_dir.resolve()}")

    print("\nConclusão: zero à esquerda na NF já é normalizado (042480 ≡ 42480).")
    print("Se o ganho com fallback > 0, havia desalinhamento de org_id no materializado comercial.")


if __name__ == "__main__":
    main()
