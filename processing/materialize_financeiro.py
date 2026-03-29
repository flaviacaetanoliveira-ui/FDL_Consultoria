"""
Materialização paralela (Fase 1): chama as mesmas funções do pipeline operacional
e grava Parquet + metadata em data_products/<cliente>/<empresa>/<modulo>/current/.

Não altera regras de negócio, cálculos ou o app Streamlit.

Uso típico (PowerShell):
  $env:FDL_BASE_DIR = "C:\\caminho\\base\\cliente"
  python processing/materialize_financeiro.py --cliente fdl_cli --empresa antomoveis --modulo all

IDs opcionais (metadados e colunas no dataset):
  FDL_CLIENTE_ID, FDL_EMPRESA_ID, FDL_CNPJ
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent

PIPELINE_REVISION_DEFAULT = "phase1-v1"

def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _path_segment(raw: str) -> str:
    s = raw.strip().replace("..", "").replace("/", "-").replace("\\", "-")
    return s or "default"


def _slug_empresa_folder(name: str) -> str:
    s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-zA-Z0-9_-]+", "_", s).strip("_").lower()
    return s or "default"


def _ensure_repo_on_path() -> None:
    r = str(REPO_ROOT)
    if r not in sys.path:
        sys.path.insert(0, r)


def _set_base_dir(base_dir: Path) -> Path:
    resolved = base_dir.expanduser().resolve()
    os.environ["FDL_BASE_DIR"] = str(resolved)
    return resolved


def _collect_repasse_signature_files(base: Path) -> list[tuple[str, int]]:
    """Lista (caminho relativo, mtime_ns) de ficheiros nas pastas do repasse."""
    subs = (
        "Vendas - Mercado Livre",
        "Liberações_ML",
        "notas_saida",
        "contas_receber",
    )
    out: list[tuple[str, int]] = []
    for sub in subs:
        d = base / sub
        if not d.is_dir():
            continue
        for f in sorted(d.rglob("*")):
            if f.is_file():
                rel = str(f.relative_to(base)).replace("\\", "/")
                try:
                    out.append((rel, int(f.stat().st_mtime_ns)))
                except OSError:
                    out.append((rel, 0))
    out.sort(key=lambda x: x[0])
    return out


def build_repasse_source_signature(base: Path) -> str:
    payload = json.dumps(_collect_repasse_signature_files(base), ensure_ascii=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:32]


def build_frete_source_signature(
    *,
    vendas_ref: str,
    vendas_mtime_ns: int,
    frete_ref: str | None,
    frete_mtime_ns: int | None,
) -> str:
    parts = [
        f"vendas_ref={vendas_ref}",
        f"vendas_mtime_ns={vendas_mtime_ns}",
        f"frete_ref={frete_ref or ''}",
        f"frete_mtime_ns={frete_mtime_ns if frete_mtime_ns is not None else ''}",
    ]
    return hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()[:32]


def _resolve_identity(path_cliente: str, path_empresa: str) -> tuple[str, str, str | None]:
    cliente_id = (os.environ.get("FDL_CLIENTE_ID") or os.environ.get("FDL_MATERIALIZE_CLIENTE_ID") or path_cliente).strip()
    empresa_id = (os.environ.get("FDL_EMPRESA_ID") or os.environ.get("FDL_MATERIALIZE_ORG_ID") or path_empresa).strip()
    cnpj_raw = (os.environ.get("FDL_CNPJ") or "").strip()
    cnpj: str | None = cnpj_raw if cnpj_raw else None
    return cliente_id, empresa_id, cnpj


def _enrich_identity_columns(df: Any, *, cliente_id: str, empresa_id: str, cnpj: str | None) -> Any:
    import pandas as pd

    out = df.copy()
    out["cliente_id"] = cliente_id
    out["empresa_id"] = empresa_id
    out["cnpj"] = pd.NA if cnpj is None else cnpj
    return out


def _atomic_replace(tmp: Path, final: Path) -> None:
    """Substitui ficheiro final de forma atómica no mesmo volume (evita leitores a verem metade do ficheiro)."""
    tmp.parent.mkdir(parents=True, exist_ok=True)
    os.replace(tmp, final)


def _write_parquet(df: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_parquet(tmp, index=False, engine="pyarrow")
        _atomic_replace(tmp, path)
    finally:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_metadata(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        _atomic_replace(tmp, path)
    finally:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_repasse_app_mirror_csv(df: Any, path: Path) -> None:
    """CSV espelho para o app (schema precomputed); sem cliente_id/empresa_id/cnpj — ficam só no Parquet."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _atomic_replace(tmp, path)
    finally:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_faturamento_app_mirror_csv(df: Any, path: Path) -> None:
    """CSV espelho do faturamento (sem colunas cliente_id/empresa_id/cnpj — ficam no Parquet)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _atomic_replace(tmp, path)
    finally:
        if tmp.is_file():
            try:
                tmp.unlink()
            except OSError:
                pass


def _write_frete_app_mirror_csv(df: Any, path: Path) -> None:
    """CSV espelho do frete para o app; mesmo DataFrame que carregar_tabela_final_frete_operacional (sem colunas de identidade)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8-sig")
        _atomic_replace(tmp, path)
    finally:
        try:
            if tmp.is_file():
                tmp.unlink()
        except OSError:
            pass


def _materialize_repasse(
    *,
    base_dir: Path,
    out_dir: Path,
    path_cliente: str,
    path_empresa: str,
    pipeline_revision: str,
) -> None:
    from etapa4b_integracao_contas_receber import carregar_tabela_final_operacional

    cliente_id, empresa_id, cnpj = _resolve_identity(path_cliente, path_empresa)
    df, info = carregar_tabela_final_operacional(base_dir)
    sig = build_repasse_source_signature(base_dir)
    df_out = _enrich_identity_columns(df, cliente_id=cliente_id, empresa_id=empresa_id, cnpj=cnpj)

    generated_at = _utc_now_iso()
    meta: dict[str, Any] = {
        "generated_at": generated_at,
        "source_signature": sig,
        "pipeline_revision": pipeline_revision,
        "cliente": path_cliente,
        "empresa": path_empresa,
        "modulo": "repasse",
        "cliente_id": cliente_id,
        "empresa_id": empresa_id,
        "cnpj": cnpj,
        "row_count": int(len(df_out)),
        "columns": [str(c) for c in df_out.columns],
        "source_mode": "local_folder",
        "base_dir": str(base_dir),
        "loader_info": {k: (v if isinstance(v, (str, int, float, bool, type(None))) else str(v)) for k, v in info.items()},
        "app_mirror_csv": "dataset_repasse_app.csv",
    }
    _write_parquet(df_out, out_dir / "dataset.parquet")
    _write_repasse_app_mirror_csv(df, out_dir / "dataset_repasse_app.csv")
    _write_metadata(out_dir / "metadata.json", meta)


def _materialize_frete(
    *,
    base_dir: Path,
    out_dir: Path,
    path_cliente: str,
    path_empresa: str,
    org_id: str,
    pipeline_revision: str,
) -> None:
    from operacional_frete import (
        carregar_tabela_final_frete_operacional,
        descobrir_fontes_frete,
        stable_mtime_ns_for_frete_url,
    )

    fontes = descobrir_fontes_frete(base_dir)
    vendas_ref = (fontes.vendas_url or "").strip() or (
        str(fontes.vendas_path.resolve()) if fontes.vendas_path else ""
    )
    if not vendas_ref:
        raise SystemExit(
            "Frete: sem fonte de vendas ML. Defina FDL_FRETE_VENDAS_URL ou coloque ficheiros em "
            f"'{base_dir / 'Vendas - Mercado Livre'}'."
        )
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

    fontes_diag = {
        "vendas_url": bool((fontes.vendas_url or "").strip()),
        "frete_url": bool((fontes.frete_url or "").strip()),
        "vendas_path": str(fontes.vendas_path) if fontes.vendas_path else None,
        "frete_path": str(fontes.frete_path) if fontes.frete_path else None,
    }

    df, meta_loader = carregar_tabela_final_frete_operacional(org_id, vendas_ref, v_ns, frete_ref, f_ns)
    cliente_id, empresa_id, cnpj = _resolve_identity(path_cliente, path_empresa)
    sig = build_frete_source_signature(
        vendas_ref=vendas_ref,
        vendas_mtime_ns=v_ns,
        frete_ref=frete_ref,
        frete_mtime_ns=f_ns,
    )
    df_out = _enrich_identity_columns(df, cliente_id=cliente_id, empresa_id=empresa_id, cnpj=cnpj)

    source_mode = "urls" if fontes_diag["vendas_url"] or fontes_diag["frete_url"] else "local_folder"
    generated_at = _utc_now_iso()
    meta: dict[str, Any] = {
        "generated_at": generated_at,
        "source_signature": sig,
        "pipeline_revision": pipeline_revision,
        "cliente": path_cliente,
        "empresa": path_empresa,
        "modulo": "frete",
        "cliente_id": cliente_id,
        "empresa_id": empresa_id,
        "cnpj": cnpj,
        "row_count": int(len(df_out)),
        "columns": [str(c) for c in df_out.columns],
        "source_mode": source_mode,
        "base_dir": str(base_dir),
        "org_id": org_id,
        "fontes": fontes_diag,
        "loader_meta": {
            "vendas_arquivo": meta_loader.get("vendas_arquivo"),
            "frete_arquivo": meta_loader.get("frete_arquivo"),
            "frete_tabular": meta_loader.get("frete_tabular"),
            "linhas": meta_loader.get("linhas"),
            "avisos": meta_loader.get("avisos"),
        },
        "app_mirror_csv": "dataset_frete_app.csv",
    }
    _write_parquet(df_out, out_dir / "dataset.parquet")
    _write_frete_app_mirror_csv(df, out_dir / "dataset_frete_app.csv")
    _write_metadata(out_dir / "metadata.json", meta)


def _materialize_faturamento(
    *,
    params_path: Path,
    out_dir: Path,
    path_cliente: str,
    path_empresa: str,
    pipeline_revision: str,
) -> None:
    from processing.faturamento.build import build_faturamento_dataset

    df, loader_meta = build_faturamento_dataset(params_path)
    cliente_id, empresa_id, cnpj = _resolve_identity(path_cliente, path_empresa)
    df_out = _enrich_identity_columns(df, cliente_id=cliente_id, empresa_id=empresa_id, cnpj=cnpj)

    generated_at = _utc_now_iso()
    meta: dict[str, Any] = {
        "generated_at": generated_at,
        "pipeline_revision": str(loader_meta.get("pipeline_revision", pipeline_revision)),
        "cliente": path_cliente,
        "empresa": path_empresa,
        "modulo": "faturamento",
        "cliente_id": cliente_id,
        "empresa_id": empresa_id,
        "cnpj": cnpj,
        "row_count": int(len(df_out)),
        "columns": [str(c) for c in df_out.columns],
        "params_path": str(params_path.resolve()),
        "aliquota_imposto_usada": loader_meta.get("aliquota_imposto"),
        "aliquota_despesas_fixas_usada": loader_meta.get("aliquota_despesas_fixas"),
        "permite_faturamento_sem_nf": loader_meta.get("permite_faturamento_sem_nf"),
        "pedidos_fonte": loader_meta.get("pedidos"),
        "custo_fonte": loader_meta.get("custo"),
        "data_processamento": loader_meta.get("data_processamento"),
        "validation_ok": True,
        "app_mirror_csv": "dataset_faturamento_app.csv",
    }
    _write_parquet(df_out, out_dir / "dataset.parquet")
    _write_faturamento_app_mirror_csv(df, out_dir / "dataset_faturamento_app.csv")
    _write_metadata(out_dir / "metadata.json", meta)


def _default_empresa_segment() -> str:
    _ensure_repo_on_path()
    from operacional_data_config import DATASET_EMPRESA

    return _slug_empresa_folder(DATASET_EMPRESA)


def main() -> int:
    _ensure_repo_on_path()
    from materialize_lock import MaterializeLockError, acquire_materialize_lock, release_materialize_lock

    parser = argparse.ArgumentParser(
        description="Materializa repasse, frete e/ou faturamento em Parquet + metadata.json em data_products/.../current/"
    )
    parser.add_argument("--base-dir", default=os.environ.get("FDL_BASE_DIR", "").strip(), help="Pasta raiz do cliente (vendas, liberações, …). Obrigatório para repasse/frete.")
    parser.add_argument("--root", default=str(REPO_ROOT / "data_products"), help="Raiz data_products")
    parser.add_argument("--cliente", default=os.environ.get("FDL_MATERIALIZE_CLIENTE", "").strip(), help="Segmento de pasta cliente")
    parser.add_argument("--empresa", default=os.environ.get("FDL_MATERIALIZE_EMPRESA", "").strip(), help="Segmento de pasta empresa")
    parser.add_argument("--modulo", choices=("repasse", "frete", "faturamento", "all"), default="all")
    parser.add_argument("--org-id", default=os.environ.get("FDL_MATERIALIZE_ORG_ID", "antomoveis"), help="org_id para carregar_tabela_final_frete_operacional")
    parser.add_argument(
        "--faturamento-params",
        default=os.environ.get("FDL_FATURAMENTO_PARAMS", "").strip(),
        help="Caminho para faturamento_params.json (obrigatório para --modulo faturamento; opcional para all).",
    )
    parser.add_argument(
        "--pipeline-revision",
        default=os.environ.get("FDL_PIPELINE_REVISION", PIPELINE_REVISION_DEFAULT),
    )
    parser.add_argument(
        "--pipeline-revision-faturamento",
        default=os.environ.get("FDL_PIPELINE_REVISION_FATURAMENTO", "").strip(),
        help="Revisão gravada no metadata do faturamento (default: faturamento-v1 no build).",
    )
    parser.add_argument(
        "--no-lock",
        action="store_true",
        help="Não adquirir lock de execução (apenas para testes isolados).",
    )
    args = parser.parse_args()

    fp_raw = (args.faturamento_params or "").strip()
    faturamento_params_path = Path(fp_raw).expanduser().resolve() if fp_raw else None

    if args.modulo == "faturamento" and not faturamento_params_path:
        print("Para --modulo faturamento defina --faturamento-params ou FDL_FATURAMENTO_PARAMS.", file=sys.stderr)
        return 1

    if args.modulo in ("repasse", "frete", "all") and not args.base_dir:
        print("Defina --base-dir ou FDL_BASE_DIR para repasse/frete.", file=sys.stderr)
        return 1

    base_dir: Path | None
    if args.base_dir:
        base_dir = _set_base_dir(Path(args.base_dir))
    else:
        base_dir = None

    _ensure_repo_on_path()

    path_cliente = _path_segment(args.cliente or "default")
    path_empresa = _path_segment(args.empresa or _default_empresa_segment())

    root = Path(args.root).expanduser().resolve()
    rev = args.pipeline_revision

    if args.modulo == "all":
        modules: list[str] = ["repasse", "frete"]
        if faturamento_params_path:
            modules.append("faturamento")
        else:
            print(
                "[materialize] AVISO: faturamento omitido (defina --faturamento-params ou FDL_FATURAMENTO_PARAMS).",
                file=sys.stderr,
            )
    elif args.modulo == "faturamento":
        modules = ["faturamento"]
    else:
        modules = [args.modulo]

    lock_path: Path | None = None
    if not args.no_lock:
        try:
            lock_path = acquire_materialize_lock(REPO_ROOT)
        except MaterializeLockError as e:
            print(str(e), file=sys.stderr)
            return 2

    exit_code = 0
    try:
        from processing.faturamento.config import PIPELINE_REVISION_FATURAMENTO

        rev_fat = args.pipeline_revision_faturamento or PIPELINE_REVISION_FATURAMENTO

        for mod in modules:
            out_dir = root / path_cliente / path_empresa / mod / "current"
            print(f"[materialize] modulo={mod} -> {out_dir}")
            if mod == "repasse":
                assert base_dir is not None
                _materialize_repasse(
                    base_dir=base_dir,
                    out_dir=out_dir,
                    path_cliente=path_cliente,
                    path_empresa=path_empresa,
                    pipeline_revision=rev,
                )
            elif mod == "frete":
                assert base_dir is not None
                _materialize_frete(
                    base_dir=base_dir,
                    out_dir=out_dir,
                    path_cliente=path_cliente,
                    path_empresa=path_empresa,
                    org_id=str(args.org_id).strip(),
                    pipeline_revision=rev,
                )
            else:
                assert faturamento_params_path is not None
                _materialize_faturamento(
                    params_path=faturamento_params_path,
                    out_dir=out_dir,
                    path_cliente=path_cliente,
                    path_empresa=path_empresa,
                    pipeline_revision=rev_fat,
                )
            print(f"  OK dataset.parquet + metadata.json (pipeline_revision={rev})")
    except SystemExit:
        raise
    except BaseException as exc:  # noqa: BLE001
        from processing.faturamento.params import FaturamentoParamsError
        from processing.faturamento.validate import FaturamentoValidationError

        exit_code = 1
        if isinstance(exc, (FaturamentoValidationError, FaturamentoParamsError)):
            print(f"ERRO faturamento: {exc}", file=sys.stderr)
        else:
            print(f"ERRO materialize: {exc}", file=sys.stderr)
            import traceback

            traceback.print_exc()
    finally:
        if lock_path is not None:
            release_materialize_lock(lock_path)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
