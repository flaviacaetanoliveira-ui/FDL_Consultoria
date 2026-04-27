#!/usr/bin/env python3
"""
Compara ``dataset_faturamento_devolucoes.parquet`` no snapshot mais recente em
``faturamento/archive/v*`` com o ``current/`` (versão do pipeline variável — v2, v3, …).

Filtra explicitamente o período auditado: 01/01/2026 a 24/04/2026 (inclusive)
em ``Nota_Data_Emissao``.
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

PERIODO_INI = pd.Timestamp(date(2026, 1, 1))
PERIODO_FIM = pd.Timestamp(date(2026, 4, 24)).replace(hour=23, minute=59, second=59)


def _parse_archive_dir(name: str) -> tuple[int, str] | None:
    m = re.match(r"^v(.+)_(\d{8}T\d{6}Z)$", name)
    if not m:
        return None
    return (m.group(2), name)


def _latest_archive_devolucoes_parquet(cliente_dir: Path) -> tuple[Path | None, str]:
    """Retorna (path parquet arquivado, etiqueta da pasta) ou (None, motivo)."""
    archive_root = cliente_dir / "faturamento" / "archive"
    if not archive_root.is_dir():
        return None, "sem pasta archive"
    subs = [p for p in archive_root.iterdir() if p.is_dir()]
    parsed: list[tuple[str, Path]] = []
    for p in subs:
        pr = _parse_archive_dir(p.name)
        if pr:
            parsed.append((pr[0], p))
    if not parsed:
        return None, "nenhuma pasta v*_TIMESTAMP em archive"
    parsed.sort(key=lambda x: x[0], reverse=True)
    latest = parsed[0][1]
    cand = latest / "dataset_faturamento_devolucoes.parquet"
    if not cand.is_file():
        return None, f"parquet ausente em {latest.name}"
    return cand.resolve(), latest.name


def _read_devolucoes(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    return pd.read_parquet(path, engine="pyarrow")


def _filtrar_periodo_auditado(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "Nota_Data_Emissao" not in df.columns:
        return pd.DataFrame()
    dt = pd.to_datetime(df["Nota_Data_Emissao"], errors="coerce")
    m = (dt >= PERIODO_INI) & (dt <= PERIODO_FIM)
    return df.loc[m].copy()


def _nf_key(row: pd.Series) -> tuple:
    org = str(row.get("org_id", "") or "").strip()
    emp = str(row.get("empresa", "") or "").strip().casefold()
    nf = str(row.get("Nota_Numero_Normalizado", "") or "").strip()
    return (org, emp, nf)


def _agregar_por_empresa(df: pd.DataFrame) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if df.empty or "empresa" not in df.columns:
        return out
    for emp, sub in df.groupby(df["empresa"].fillna("").astype(str).str.strip()):
        emp_s = str(emp).strip()
        if not emp_s:
            continue
        keys = {_nf_key(sub.iloc[i]) for i in range(len(sub))}
        vl = pd.to_numeric(sub.get("Valor_Liquido_Devolucao", 0), errors="coerce").fillna(0.0)
        soma = float(vl.sum())
        out[emp_s] = {"keys": keys, "qtd": len(keys), "valor": soma}
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Diff devoluções archive (v1 snapshot) × current.")
    ap.add_argument("--cliente-dir", type=Path, required=True, help="Ex.: data_products/cliente_2")
    ap.add_argument("--saida", type=Path, required=True, help="CSV consolidado")
    args = ap.parse_args()

    cliente_dir = args.cliente_dir.expanduser().resolve()
    cur_path = cliente_dir / "faturamento" / "current" / "dataset_faturamento_devolucoes.parquet"

    arch_path, arch_label = _latest_archive_devolucoes_parquet(cliente_dir)

    df_v1 = _filtrar_periodo_auditado(_read_devolucoes(arch_path)) if arch_path else pd.DataFrame()
    df_v2 = _filtrar_periodo_auditado(_read_devolucoes(cur_path))

    agg1 = _agregar_por_empresa(df_v1)
    agg2 = _agregar_por_empresa(df_v2)
    empresas = sorted(set(agg1.keys()) | set(agg2.keys()))

    rows: list[dict] = []
    for emp in empresas:
        a1 = agg1.get(emp, {"keys": set(), "qtd": 0, "valor": 0.0})
        a2 = agg2.get(emp, {"keys": set(), "qtd": 0, "valor": 0.0})
        k1, k2 = a1["keys"], a2["keys"]
        novas = k2 - k1
        removidas = k1 - k2
        rows.append(
            {
                "empresa": emp,
                "periodo_auditado": "2026-01-01 a 2026-04-24",
                "archive_snapshot": arch_label or "",
                "v1_qtd": a1["qtd"],
                "v2_qtd": a2["qtd"],
                "nfs_novas": len(novas),
                "nfs_removidas": len(removidas),
                "v1_valor_total": round(a1["valor"], 2),
                "v2_valor_total": round(a2["valor"], 2),
                "delta_valor": round(a2["valor"] - a1["valor"], 2),
            }
        )

    out_df = pd.DataFrame(rows)
    args.saida.parent.mkdir(parents=True, exist_ok=True)
    out_df.to_csv(args.saida, index=False, encoding="utf-8-sig")

    meta = {
        "cliente_dir": str(cliente_dir),
        "archive_parquet": str(arch_path) if arch_path else None,
        "archive_label": arch_label,
        "current_parquet": str(cur_path),
        "periodo": "2026-01-01 .. 2026-04-24",
        "linhas_csv": len(rows),
    }
    print(json.dumps(meta, ensure_ascii=False, indent=2))
    print(out_df.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
