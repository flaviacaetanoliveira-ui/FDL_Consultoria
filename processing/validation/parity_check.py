"""
Compara dataset materializado (Parquet) com uma execução direta das mesmas funções
de carga do pipeline, gerando parity_report.json e parity_report.md.

Não altera regras de negócio nem o app.

Uso:
  $env:FDL_BASE_DIR = "C:\\..."
  python processing/validation/parity_check.py --cliente fdl_cli --empresa antomoveis --modulo all
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent.parent

IDENTITY_COLS = ("cliente_id", "empresa_id", "cnpj")

# Colunas espelho do export ML (ex.: segunda "Unidades"); não entram no cálculo do frete — ignorar só no rowwise.
FRETE_ROWWISE_IGNORE_COLS = ("Unidades.1",)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _ensure_repo_on_path() -> None:
    r = str(REPO_ROOT)
    if r not in sys.path:
        sys.path.insert(0, r)


def _set_base_dir(base_dir: Path) -> Path:
    resolved = base_dir.expanduser().resolve()
    os.environ["FDL_BASE_DIR"] = str(resolved)
    return resolved


def _strip_identity(df: Any) -> Any:
    import pandas as pd

    drop = [c for c in IDENTITY_COLS if c in df.columns]
    return df.drop(columns=drop) if drop else df


def _float_eq(a: float, b: float, tol: float = 1e-6) -> bool:
    return abs(a - b) <= tol


def _repasse_metrics(df: Any) -> dict[str, Any]:
    import pandas as pd

    diferenca = pd.to_numeric(df.get("Diferença"), errors="coerce")
    valor_pago = pd.to_numeric(df.get("Valor pago"), errors="coerce")
    valor_receber = pd.to_numeric(df.get("Valor a receber"), errors="coerce")
    return {
        "rows": int(len(df)),
        "sum_valor_pago": float(valor_pago.fillna(0).sum()),
        "sum_valor_receber": float(valor_receber.fillna(0).sum()),
        "sum_diferenca_abs": float(diferenca.fillna(0).abs().sum()),
        "acao_sugerida_counts": {
            str(k): int(v) for k, v in df["Ação sugerida"].value_counts(dropna=False).items()
        }
        if "Ação sugerida" in df.columns
        else {},
        "situacao_counts": {
            str(k): int(v) for k, v in df["Situação"].value_counts(dropna=False).items()
        }
        if "Situação" in df.columns
        else {},
    }


def _frete_metrics(df: Any) -> dict[str, Any]:
    import pandas as pd

    from operacional_frete import FRETE_ML_COL, FRETE_UI_N_VENDA

    col_frete = FRETE_ML_COL
    fm = pd.to_numeric(df.get(col_frete), errors="coerce")
    out: dict[str, Any] = {
        "rows": int(len(df)),
        "sum_frete_ml": float(fm.fillna(0).sum()),
        "n_frete_notna": int(fm.notna().sum()),
    }
    if FRETE_UI_N_VENDA in df.columns:
        out["n_distinct_venda"] = int(df[FRETE_UI_N_VENDA].astype(str).nunique())
    return out


def _compare_metrics(a: dict[str, Any], b: dict[str, Any], *, numeric_tol: float) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    keys = set(a.keys()) | set(b.keys())
    for k in sorted(keys):
        va, vb = a.get(k), b.get(k)
        if isinstance(va, dict) and isinstance(vb, dict):
            sk = set(va.keys()) | set(vb.keys())
            for kk in sorted(sk):
                ca, cb = va.get(kk), vb.get(kk)
                if ca != cb:
                    findings.append(
                        {
                            "field": f"{k}.{kk}",
                            "materialized": ca,
                            "fresh": cb,
                            "match": False,
                        }
                    )
            continue
        if isinstance(va, (int, float)) and isinstance(vb, (int, float)):
            ok = _float_eq(float(va), float(vb), numeric_tol)
            if not ok:
                findings.append({"field": k, "materialized": va, "fresh": vb, "match": False})
            continue
        if va != vb:
            findings.append({"field": k, "materialized": va, "fresh": vb, "match": False})
    return findings


def _align_repasse_frames(a: Any, b: Any) -> tuple[Any, Any]:
    import pandas as pd

    key_cols = [c for c in ("N° de venda", "ID do pedido", "Número da nota") if c in a.columns and c in b.columns]
    if len(key_cols) < 2:
        return a, b
    a2 = a.sort_values(key_cols).reset_index(drop=True)
    b2 = b.sort_values(key_cols).reset_index(drop=True)
    return a2, b2


def _frames_close(a: Any, b: Any, *, rtol: float = 1e-5, atol: float = 1e-6) -> tuple[bool, str | None]:
    import pandas as pd

    if list(a.columns) != list(b.columns):
        return False, f"columns differ: {a.columns.tolist()[:12]} vs {b.columns.tolist()[:12]}"
    if len(a) != len(b):
        return False, f"row count {len(a)} vs {len(b)}"
    for c in a.columns:
        s1, s2 = a[c], b[c]
        if pd.api.types.is_numeric_dtype(s1) and pd.api.types.is_numeric_dtype(s2):
            n1 = pd.to_numeric(s1, errors="coerce")
            n2 = pd.to_numeric(s2, errors="coerce")
            if not ((n1 - n2).fillna(0).abs() <= (atol + rtol * n2.abs().fillna(0))).all():
                return False, f"numeric column mismatch: {c}"
        elif pd.api.types.is_datetime64_any_dtype(s1) or pd.api.types.is_datetime64_any_dtype(s2):
            d1 = pd.to_datetime(s1, errors="coerce")
            d2 = pd.to_datetime(s2, errors="coerce")
            if not (((d1 == d2) | (d1.isna() & d2.isna())).all()):
                return False, f"datetime column mismatch: {c}"
        else:
            v1 = s1.astype(str).fillna("").str.strip()
            v2 = s2.astype(str).fillna("").str.strip()
            v1 = v1.str.replace("NaT", "", regex=False).str.replace("<NA>", "", regex=False)
            v2 = v2.str.replace("NaT", "", regex=False).str.replace("<NA>", "", regex=False)
            if not (v1 == v2).all():
                return False, f"text column mismatch: {c}"
    return True, None


def _check_repasse(base_dir: Path, materialized_dir: Path) -> dict[str, Any]:
    import pandas as pd

    from etapa4b_integracao_contas_receber import carregar_tabela_final_operacional
    from processing.materialize_financeiro import build_repasse_source_signature

    parquet_path = materialized_dir / "dataset.parquet"
    meta_path = materialized_dir / "metadata.json"
    if not parquet_path.is_file():
        return {"modulo": "repasse", "status": "error", "detail": f"missing {parquet_path}"}
    mat = pd.read_parquet(parquet_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
    mat_core = _strip_identity(mat)

    fresh, info = carregar_tabela_final_operacional(base_dir)
    sig_fresh = build_repasse_source_signature(base_dir)

    m_mat = _repasse_metrics(mat_core)
    m_fresh = _repasse_metrics(fresh)
    metric_findings = _compare_metrics(m_mat, m_fresh, numeric_tol=1e-3)

    a2, b2 = _align_repasse_frames(mat_core.copy(), fresh.copy())
    close, close_reason = _frames_close(a2, b2)

    sig_meta = meta.get("source_signature")
    sig_ok = sig_meta == sig_fresh if sig_meta else None

    status = "pass"
    if metric_findings or not close:
        status = "fail"
    if sig_ok is False:
        status = "fail"

    return {
        "modulo": "repasse",
        "status": status,
        "row_count_materialized": int(len(mat)),
        "row_count_fresh": int(len(fresh)),
        "source_signature_metadata": sig_meta,
        "source_signature_fresh": sig_fresh,
        "source_signature_match": sig_ok,
        "metrics_materialized": m_mat,
        "metrics_fresh": m_fresh,
        "metrics_diffs": metric_findings,
        "rowwise_close": close,
        "rowwise_detail": close_reason,
        "loader_info_fresh": {k: str(v) for k, v in info.items()},
    }


def _check_frete(base_dir: Path, materialized_dir: Path, org_id: str) -> dict[str, Any]:
    import pandas as pd

    from operacional_frete import (
        FRETE_UI_N_VENDA,
        carregar_tabela_final_frete_operacional,
        descobrir_fontes_frete,
        stable_mtime_ns_for_frete_url,
    )
    from processing.materialize_financeiro import build_frete_source_signature

    parquet_path = materialized_dir / "dataset.parquet"
    meta_path = materialized_dir / "metadata.json"
    if not parquet_path.is_file():
        return {"modulo": "frete", "status": "error", "detail": f"missing {parquet_path}"}
    mat = pd.read_parquet(parquet_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8")) if meta_path.is_file() else {}
    mat_core = _strip_identity(mat)

    fontes = descobrir_fontes_frete(base_dir)
    vendas_ref = (fontes.vendas_url or "").strip() or (
        str(fontes.vendas_path.resolve()) if fontes.vendas_path else ""
    )
    if not vendas_ref:
        return {"modulo": "frete", "status": "error", "detail": "no vendas source for fresh run"}
    if (fontes.vendas_url or "").strip():
        v_ns = stable_mtime_ns_for_frete_url(fontes.vendas_url)
    else:
        assert fontes.vendas_path is not None
        v_ns = int(fontes.vendas_path.stat().st_mtime_ns)
    frete_ref = (fontes.frete_url or "").strip() or (
        str(fontes.frete_path.resolve()) if fontes.frete_path and fontes.frete_path.is_file() else None
    )
    if (fontes.frete_url or "").strip():
        f_ns = stable_mtime_ns_for_frete_url(fontes.frete_url)
    elif fontes.frete_path and fontes.frete_path.is_file():
        f_ns = int(fontes.frete_path.stat().st_mtime_ns)
    else:
        f_ns = None

    fresh, _meta = carregar_tabela_final_frete_operacional(org_id, vendas_ref, v_ns, frete_ref, f_ns)
    sig_fresh = build_frete_source_signature(
        vendas_ref=vendas_ref,
        vendas_mtime_ns=v_ns,
        frete_ref=frete_ref,
        frete_mtime_ns=f_ns,
    )

    m_mat = _frete_metrics(mat_core)
    m_fresh = _frete_metrics(fresh)
    metric_findings = _compare_metrics(m_mat, m_fresh, numeric_tol=1e-3)

    key_cols = [c for c in (FRETE_UI_N_VENDA,) if c in mat_core.columns and c in fresh.columns]
    if key_cols:
        a2 = mat_core.sort_values(key_cols).reset_index(drop=True)
        b2 = fresh.sort_values(key_cols).reset_index(drop=True)
    else:
        a2, b2 = mat_core.reset_index(drop=True), fresh.reset_index(drop=True)
    drop_rw = [c for c in FRETE_ROWWISE_IGNORE_COLS if c in a2.columns and c in b2.columns]
    if drop_rw:
        a2 = a2.drop(columns=drop_rw)
        b2 = b2.drop(columns=drop_rw)
    close, close_reason = _frames_close(a2, b2)

    sig_meta = meta.get("source_signature")
    sig_ok = sig_meta == sig_fresh if sig_meta else None
    status = "pass"
    if metric_findings or not close:
        status = "fail"
    if sig_ok is False:
        status = "fail"

    return {
        "modulo": "frete",
        "status": status,
        "row_count_materialized": int(len(mat)),
        "row_count_fresh": int(len(fresh)),
        "source_signature_metadata": sig_meta,
        "source_signature_fresh": sig_fresh,
        "source_signature_match": sig_ok,
        "metrics_materialized": m_mat,
        "metrics_fresh": m_fresh,
        "metrics_diffs": metric_findings,
        "rowwise_close": close,
        "rowwise_detail": close_reason,
    }


def _write_md(report: dict[str, Any], path: Path) -> None:
    lines: list[str] = [
        "# Parity report",
        "",
        f"- generated_at: `{report.get('generated_at')}`",
        f"- base_dir: `{report.get('base_dir')}`",
        f"- data_products: `{report.get('data_products_root')}`",
        f"- cliente / empresa: `{report.get('cliente')}` / `{report.get('empresa')}`",
        f"- overall: **{report.get('overall_status')}**",
        "",
    ]
    for m in report.get("modules", []) or []:
        lines.append(f"## {m.get('modulo')}")
        lines.append("")
        lines.append(f"- status: **{m.get('status')}**")
        if m.get("rowwise_detail"):
            lines.append(f"- rowwise: {m.get('rowwise_close')} — {m.get('rowwise_detail')}")
        if m.get("metrics_diffs"):
            lines.append("- metric diffs:")
            for d in m["metrics_diffs"][:20]:
                lines.append(f"  - `{d.get('field')}`: materialized={d.get('materialized')} fresh={d.get('fresh')}")
            if len(m["metrics_diffs"]) > 20:
                lines.append(f"  - … ({len(m['metrics_diffs']) - 20} more)")
        if m.get("detail"):
            lines.append(f"- detail: {m['detail']}")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Paridade materializado vs execução direta")
    parser.add_argument("--base-dir", default=os.environ.get("FDL_BASE_DIR", "").strip())
    parser.add_argument("--root", default=str(REPO_ROOT / "data_products"))
    parser.add_argument("--cliente", default=os.environ.get("FDL_MATERIALIZE_CLIENTE", "default").strip())
    parser.add_argument("--empresa", default=os.environ.get("FDL_MATERIALIZE_EMPRESA", "").strip())
    parser.add_argument("--modulo", choices=("repasse", "frete", "all"), default="all")
    parser.add_argument("--org-id", default=os.environ.get("FDL_MATERIALIZE_ORG_ID", "antomoveis"))
    parser.add_argument(
        "--report-dir",
        default=str(REPO_ROOT / "processing" / "validation" / "reports"),
        help="Pasta onde gravar parity_report.json e parity_report.md",
    )
    args = parser.parse_args()

    if not args.base_dir:
        print("Defina --base-dir ou FDL_BASE_DIR.", file=sys.stderr)
        return 1

    base_dir = _set_base_dir(Path(args.base_dir))
    _ensure_repo_on_path()

    from processing.materialize_financeiro import _default_empresa_segment, _path_segment

    path_cliente = _path_segment(args.cliente)
    path_empresa = _path_segment(args.empresa or _default_empresa_segment())
    root = Path(args.root).expanduser().resolve()
    report_dir = Path(args.report_dir).expanduser().resolve()
    report_dir.mkdir(parents=True, exist_ok=True)

    modules: list[str] = ["repasse", "frete"] if args.modulo == "all" else [args.modulo]
    results: list[dict[str, Any]] = []
    for mod in modules:
        mat_dir = root / path_cliente / path_empresa / mod / "current"
        if mod == "repasse":
            results.append(_check_repasse(base_dir, mat_dir))
        else:
            results.append(_check_frete(base_dir, mat_dir, str(args.org_id).strip()))

    overall = "pass" if all(r.get("status") == "pass" for r in results) else "fail"
    if any(r.get("status") == "error" for r in results):
        overall = "error"

    report: dict[str, Any] = {
        "generated_at": _utc_now_iso(),
        "base_dir": str(base_dir),
        "data_products_root": str(root),
        "cliente": path_cliente,
        "empresa": path_empresa,
        "overall_status": overall,
        "modules": results,
    }

    (report_dir / "parity_report.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    _write_md(report, report_dir / "parity_report.md")

    print(f"overall_status={overall}  reports -> {report_dir}")
    return 0 if overall == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
