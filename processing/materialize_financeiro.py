"""
Materialização paralela (Fase 1): repasse e frete reutilizam o pipeline operacional; **devoluções**
usa ``processing.devolucoes_ml`` (fila só de candidatas). Saída em
``data_products/<cliente>/<empresa>/<modulo>/current/``.

O app Streamlit só lê os artefatos materializados (sem recalcular estes módulos ao vivo).

Uso típico (PowerShell):
  $env:FDL_BASE_DIR = "C:\\caminho\\base\\cliente"
  python processing/materialize_financeiro.py --cliente fdl_cli --empresa antomoveis --modulo all

Cliente 5 (Flávio), origem ``Cliente_4/Esquilo`` → saída ``data_products/cliente_5/esquilo/...``:
  python processing/materialize_financeiro.py ^
    --base-dir "D:\\...\\Cliente_4\\Esquilo" --cliente cliente_5 --empresa esquilo --org-id esquilo ^
    --dataset-empresa Esquilo --modulo all

IDs opcionais (metadados e colunas no dataset):
  FDL_CLIENTE_ID, FDL_EMPRESA_ID, FDL_CNPJ

Coluna «empresa» no CSV de repasse: defina ``--dataset-empresa`` (ou ``FDL_DATASET_EMPRESA``) **antes**
do pipeline; deve coincidir com o nome no app (ex.: Esquilo, Wood).

Debug repasse (stderr + metadata repasse_debug):
  FDL_DEBUG_REPASSE_PIPELINE=1 — liberações, etapa3, notas_saida/contas_receber, merges, colunas finais.
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
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))
from fdl_paths import resolve_pasta_vendas_ml  # noqa: E402

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
    dirs: list[Path] = []
    vd = resolve_pasta_vendas_ml(base)
    if vd.is_dir():
        dirs.append(vd)
    for sub in ("Liberações_ML", "notas_saida", "contas_receber"):
        d = base / sub
        if d.is_dir():
            dirs.append(d)
    out: list[tuple[str, int]] = []
    for d in dirs:
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
    vendas_ref: str | tuple[str, ...],
    vendas_mtime_ns: int,
    frete_ref: str | None,
    frete_mtime_ns: int | None,
) -> str:
    vr = "\n".join(vendas_ref) if isinstance(vendas_ref, tuple) else vendas_ref
    parts = [
        f"vendas_ref={vr}",
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


def _dataframe_safe_for_parquet(df: Any) -> Any:
    """
    PyArrow infere às vezes colunas object como int64 se a maioria for numérica; valores com zeros
    à esquerda (ex. CPF) falham. Uniformiza colunas object como texto antes de to_parquet.
    """
    import pandas as pd

    out = df.copy()
    for c in out.columns:
        if out[c].dtype != object:
            continue
        out[c] = out[c].map(lambda v: "" if pd.isna(v) else str(v))
    return out


def _write_parquet(df: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        _dataframe_safe_for_parquet(df).to_parquet(tmp, index=False, engine="pyarrow")
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


def _write_devolucoes_app_mirror_csv(df: Any, path: Path) -> None:
    """CSV espelho do módulo devoluções (sem cliente_id/empresa_id/cnpj — ficam no Parquet)."""
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




def _debug_repasse_pipeline_enabled() -> bool:
    return os.environ.get("FDL_DEBUG_REPASSE_PIPELINE", "").strip().lower() in {"1", "true", "yes", "on"}


def _emit_repasse_pipeline_debug(base_dir: Path, df_final: Any) -> dict[str, Any]:
    """Métricas temporárias (FDL_DEBUG_REPASSE_PIPELINE=1): liberações, etapa3, integração notas/contas, tabela final."""
    import pandas as pd

    from carregamento_bases import carregar_bases_consolidadas
    from etapa3_conciliacao_vendas_liberacoes_validas import build_conciliacao_vendas_liberacoes_validas
    from etapa4b_integracao_contas_receber import _read_contas
    from integracao_notas_pedidos import _carregar_notas_saida, build_conciliacao_com_notas

    def _count_dp(d: Any) -> int:
        if getattr(d, "empty", True) or "Data de pagamento" not in d.columns:
            return 0
        s = pd.to_datetime(d["Data de pagamento"], errors="coerce")
        n = int(s.notna().sum())
        if n == 0:
            raw = d["Data de pagamento"].astype(str).str.strip()
            n = int((raw.ne("") & ~raw.str.lower().isin({"nan", "none", "nat"})).sum())
        return n

    def _count_nonempty_col(d: Any, col: str) -> int:
        if getattr(d, "empty", True) or col not in d.columns:
            return 0
        s = d[col].fillna("").astype(str).str.strip()
        return int(((s.ne("")) & (~s.str.lower().isin({"nan", "none", "nat"}))).sum())

    def _count_data_emissao(d: Any) -> int:
        if getattr(d, "empty", True) or "Data de emissão" not in d.columns:
            return 0
        s = pd.to_datetime(d["Data de emissão"], errors="coerce")
        n = int(s.notna().sum())
        if n == 0:
            return _count_nonempty_col(d, "Data de emissão")
        return n

    _vt, lib_t, _, _ = carregar_bases_consolidadas(base_dir)
    conc = build_conciliacao_vendas_liberacoes_validas(base_dir)
    apos = build_conciliacao_com_notas()

    notas_df = _carregar_notas_saida()
    notas_rows = int(len(notas_df))
    notas_saida_vazio = bool(notas_df.empty)

    pasta_contas = base_dir / "contas_receber"
    files_contas: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files_contas.extend(p for p in pasta_contas.rglob(ptn) if p.is_file())
    files_contas.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    partes_contas = []
    for f in files_contas:
        partes_contas.append(_read_contas(f).dropna(axis=1, how="all").copy())
    contas_df = pd.concat(partes_contas, ignore_index=True) if partes_contas else pd.DataFrame()
    contas_files = len(files_contas)
    contas_rows = int(len(contas_df))

    vp = pd.to_numeric(conc.get("Valor pago"), errors="coerce")
    matches = int((vp.fillna(0) > 0).sum()) if "Valor pago" in conc.columns else 0

    merge_notas_matches = _count_nonempty_col(apos, "Número da nota")

    out: dict[str, Any] = {
        "liberacoes_tratadas_rows": int(len(lib_t)),
        "etapa3_rows": int(len(conc)),
        "etapa3_linhas_data_pagamento_preenchida": _count_dp(conc),
        "etapa3_linhas_valor_pago_positivo": matches,
        "notas_saida_linhas_carregadas": notas_rows,
        "notas_saida_vazio": notas_saida_vazio,
        "contas_receber_arquivos": contas_files,
        "contas_receber_linhas_carregadas": contas_rows,
        "apos_integracao_notas_rows": int(len(apos)),
        "apos_integracao_linhas_data_pagamento_preenchida": _count_dp(apos),
        "apos_integracao_merge_notas_linhas_numero_nota_preenchida": merge_notas_matches,
        "apos_integracao_linhas_data_emissao_preenchida": _count_data_emissao(apos),
        "tabela_final_operacional_rows": int(len(df_final)),
        "tabela_final_linhas_data_pagamento_preenchida": _count_dp(df_final),
        "tabela_final_linhas_numero_nota_preenchida": _count_nonempty_col(df_final, "Número da nota"),
        "tabela_final_linhas_data_emissao_preenchida": _count_data_emissao(df_final),
        "tabela_final_linhas_situacao_preenchida": _count_nonempty_col(df_final, "Situação"),
    }
    for k, v in out.items():
        print(f"[materialize][repasse-debug] {k}={v}", file=sys.stderr)
    return out

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
    repasse_debug: dict[str, Any] | None = None
    if _debug_repasse_pipeline_enabled():
        repasse_debug = _emit_repasse_pipeline_debug(base_dir, df)
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
    if repasse_debug:
        meta["repasse_debug"] = repasse_debug
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
        frete_vendas_loader_args,
    )

    fontes = descobrir_fontes_frete(base_dir)
    vendas_ref, v_ns = frete_vendas_loader_args(fontes)
    if not vendas_ref:
        raise SystemExit(
            "Frete: sem fonte de vendas ML. Defina FDL_FRETE_VENDAS_URL ou coloque ficheiros em "
            f"'{resolve_pasta_vendas_ml(base_dir)}' (ou pasta irmã **Vendas_ML** / **Vendas - Mercado Livre**)."
        )

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
        "vendas_paths": [str(p) for p in fontes.vendas_paths],
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


def _materialize_devolucoes(
    *,
    base_dir: Path,
    out_dir: Path,
    path_cliente: str,
    path_empresa: str,
    org_id: str,
    dataset_empresa: str,
    pipeline_revision: str,
) -> None:
    from processing.devolucoes_ml.build import (
        PIPELINE_REVISION_DEVOLUCOES,
        build_devolucoes_dataset,
        build_devolucoes_source_signature,
    )

    cliente_id, empresa_id, cnpj = _resolve_identity(path_cliente, path_empresa)
    df, loader_meta = build_devolucoes_dataset(
        base_dir,
        org_id=str(org_id).strip(),
        dataset_empresa=str(dataset_empresa).strip(),
        cliente_id=cliente_id,
    )
    sig = build_devolucoes_source_signature(base_dir)
    df_out = _enrich_identity_columns(df, cliente_id=cliente_id, empresa_id=empresa_id, cnpj=cnpj)

    generated_at = _utc_now_iso()
    meta: dict[str, Any] = {
        "generated_at": generated_at,
        "source_signature": sig,
        "pipeline_revision": pipeline_revision,
        "devolucoes_contract_revision": PIPELINE_REVISION_DEVOLUCOES,
        "cliente": path_cliente,
        "empresa": path_empresa,
        "modulo": "devolucoes",
        "cliente_id": cliente_id,
        "empresa_id": empresa_id,
        "cnpj": cnpj,
        "row_count": int(len(df_out)),
        "columns": [str(c) for c in df_out.columns],
        "source_mode": "local_folder",
        "base_dir": str(base_dir),
        "org_id": str(org_id).strip(),
        "dataset_empresa": str(dataset_empresa).strip(),
        "app_mirror_csv": "dataset_devolucoes_app.csv",
        "build_meta": {k: v for k, v in loader_meta.items() if isinstance(v, (str, int, float, bool, type(None)))},
    }
    for k in ("row_count_vendas_total", "row_count_liberacoes", "sales_from_lib_financial", "erro"):
        if k in loader_meta:
            meta[k] = loader_meta[k]
    _write_parquet(df_out, out_dir / "dataset.parquet")
    _write_devolucoes_app_mirror_csv(df, out_dir / "dataset_devolucoes_app.csv")
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
        "permite_faturamento_sem_nf": loader_meta.get("permite_faturamento_sem_nf")
        if loader_meta.get("schema_version") != 2
        else loader_meta.get("permite_faturamento_sem_nf_default"),
        "pedidos_fonte": loader_meta.get("pedidos") or loader_meta.get("empresas_fonte"),
        "custo_fonte": loader_meta.get("custo"),
        "data_processamento": loader_meta.get("data_processamento"),
        "validation_ok": True,
        "app_mirror_csv": "dataset_faturamento_app.csv",
    }
    for k in (
        "schema_version",
        "cliente_slug",
        "cliente_root",
        "coluna_base_imposto_resolvida",
        "coluna_base_imposto_candidatas",
        "empresas_fonte",
        "status_custo_counts",
        "params_mensais_path",
        "notas_saida_dir_default",
        "notas_por_empresa",
    ):
        if k in loader_meta:
            meta[k] = loader_meta[k]
    _write_parquet(df_out, out_dir / "dataset.parquet")
    _write_faturamento_app_mirror_csv(df, out_dir / "dataset_faturamento_app.csv")

    from processing.faturamento.nf_materializado import (
        SCHEMA_VERSION_NF_FIRST,
        build_nf_materializado_dataframe,
    )
    from processing.faturamento.fiscal_materializado import (
        SCHEMA_VERSION_FISCAL,
        build_fiscal_materializado_dataframe,
        fiscal_materializado_meta_snapshot,
    )

    df_nf = build_nf_materializado_dataframe(df)
    _write_parquet(_dataframe_safe_for_parquet(df_nf), out_dir / "dataset_faturamento_nf.parquet")
    meta["dataset_faturamento_nf_parquet"] = "dataset_faturamento_nf.parquet"
    meta["nf_first_row_count"] = int(len(df_nf))
    meta["schema_version_nf_first"] = SCHEMA_VERSION_NF_FIRST

    df_fiscal = build_fiscal_materializado_dataframe(params_path)
    from processing.faturamento.nf_panel_materializado import (
        NF_PANEL_PARQUET_FILENAME,
        build_nf_panel_materializado_dataframe,
    )

    df_nf_panel = build_nf_panel_materializado_dataframe(df_nf, df_fiscal)
    _write_parquet(_dataframe_safe_for_parquet(df_nf_panel), out_dir / NF_PANEL_PARQUET_FILENAME)
    meta["dataset_faturamento_nf_panel_parquet"] = NF_PANEL_PARQUET_FILENAME
    meta["nf_panel_row_count"] = int(len(df_nf_panel))

    _write_parquet(_dataframe_safe_for_parquet(df_fiscal), out_dir / "dataset_faturamento_fiscal.parquet")
    meta["dataset_faturamento_fiscal_parquet"] = "dataset_faturamento_fiscal.parquet"
    meta["schema_version_fiscal"] = SCHEMA_VERSION_FISCAL
    meta.update(fiscal_materializado_meta_snapshot(df_fiscal))

    _write_metadata(out_dir / "metadata.json", meta)


def _default_empresa_segment() -> str:
    _ensure_repo_on_path()
    from operacional_data_config import DATASET_EMPRESA

    return _slug_empresa_folder(DATASET_EMPRESA)


def _preflight_sources(base_dir: Path) -> int:
    """
    Verifica pastas esperadas pelo repasse e fontes mínimas do frete (sem executar o pipeline).
    """
    import sys

    code = 0
    print(f"[preflight] base_dir={base_dir.resolve()}")
    opcionais = ("notas_saida",)
    v_dir = resolve_pasta_vendas_ml(base_dir)
    if not v_dir.is_dir():
        print(f"[preflight] ERRO: pasta de vendas ML em falta: {v_dir} (ou **Vendas_ML**)", file=sys.stderr)
        code = 1
    else:
        n = sum(1 for p in v_dir.rglob("*") if p.is_file())
        print(f"[preflight] OK vendas ({v_dir.name})/ ({n} ficheiros)")
    for sub in ("Liberações_ML", "contas_receber"):
        d = base_dir / sub
        if not d.is_dir():
            print(f"[preflight] ERRO: pasta obrigatória em falta: {d}", file=sys.stderr)
            code = 1
        else:
            n = sum(1 for p in d.rglob("*") if p.is_file())
            print(f"[preflight] OK {sub}/ ({n} ficheiros)")
    for sub in opcionais:
        d = base_dir / sub
        if d.is_dir():
            n = sum(1 for p in d.rglob("*") if p.is_file())
            print(f"[preflight] OK {sub}/ ({n} ficheiros)")
        else:
            print(f"[preflight] AVISO (opcional): sem pasta {d}")
    _ensure_repo_on_path()
    from operacional_frete import descobrir_fontes_frete

    f = descobrir_fontes_frete(base_dir)
    v_ok = bool((f.vendas_url or "").strip() or f.vendas_path or f.vendas_paths)
    fr_ok = bool((f.frete_url or "").strip() or (f.frete_path is not None and f.frete_path.is_file()))
    print(
        f"[preflight] Frete — vendas ML: {'OK' if v_ok else 'FALTA'} "
        f"({f.vendas_path or f.vendas_url or '—'})"
    )
    print(
        f"[preflight] Frete — planilha «Frete por Anúncio» (ou URL): "
        f"{'OK' if fr_ok else 'AVISO — pode falhar o módulo frete'} "
        f"({f.frete_path or f.frete_url or '—'})"
    )
    if not v_ok:
        code = 1
    return code


def main() -> int:
    _ensure_repo_on_path()
    from materialize_lock import MaterializeLockError, acquire_materialize_lock, release_materialize_lock

    parser = argparse.ArgumentParser(
        description="Materializa repasse, frete, devoluções e/ou faturamento em Parquet + metadata.json em data_products/.../current/"
    )
    parser.add_argument("--base-dir", default=os.environ.get("FDL_BASE_DIR", "").strip(), help="Pasta raiz do cliente (vendas, liberações, …). Obrigatório para repasse/frete.")
    parser.add_argument("--root", default=str(REPO_ROOT / "data_products"), help="Raiz data_products")
    parser.add_argument("--cliente", default=os.environ.get("FDL_MATERIALIZE_CLIENTE", "").strip(), help="Segmento de pasta cliente")
    parser.add_argument("--empresa", default=os.environ.get("FDL_MATERIALIZE_EMPRESA", "").strip(), help="Segmento de pasta empresa")
    parser.add_argument(
        "--modulo",
        choices=("repasse", "frete", "devolucoes", "faturamento", "all"),
        default="all",
    )
    parser.add_argument("--org-id", default=os.environ.get("FDL_MATERIALIZE_ORG_ID", "antomoveis"), help="org_id para carregar_tabela_final_frete_operacional")
    parser.add_argument(
        "--dataset-empresa",
        default=os.environ.get("FDL_DATASET_EMPRESA", "").strip(),
        help="Valor da coluna «empresa» no dataset (repasse). Deve coincidir com o nome no app, ex.: Esquilo, Wood.",
    )
    parser.add_argument(
        "--preflight",
        action="store_true",
        help="Só valida pastas/fontes em --base-dir e sai (não materializa).",
    )
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

    ds_emp = (getattr(args, "dataset_empresa", None) or "").strip()
    if ds_emp:
        os.environ["FDL_DATASET_EMPRESA"] = ds_emp

    fp_raw = (args.faturamento_params or "").strip()
    faturamento_params_path = Path(fp_raw).expanduser().resolve() if fp_raw else None

    if args.modulo == "faturamento" and not faturamento_params_path:
        print("Para --modulo faturamento defina --faturamento-params ou FDL_FATURAMENTO_PARAMS.", file=sys.stderr)
        return 1

    if args.modulo in ("repasse", "frete", "devolucoes", "all") and not args.base_dir:
        print("Defina --base-dir ou FDL_BASE_DIR para repasse/frete/devoluções.", file=sys.stderr)
        return 1

    base_dir: Path | None
    if args.base_dir:
        base_dir = _set_base_dir(Path(args.base_dir))
    else:
        base_dir = None

    if getattr(args, "preflight", False):
        if base_dir is None:
            print("--preflight requer --base-dir.", file=sys.stderr)
            return 1
        return _preflight_sources(base_dir)

    _ensure_repo_on_path()

    path_cliente = _path_segment(args.cliente or "default")
    path_empresa = _path_segment(args.empresa or _default_empresa_segment())

    root = Path(args.root).expanduser().resolve()
    rev = args.pipeline_revision

    if args.modulo == "all":
        modules: list[str] = ["repasse", "frete", "devolucoes"]
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
            path_cli_cur = path_cliente
            path_emp_cur = path_empresa
            if mod == "faturamento" and faturamento_params_path:
                from processing.faturamento.params import peek_faturamento_schema_version, read_cliente_slug_v2

                if peek_faturamento_schema_version(faturamento_params_path) >= 2:
                    path_cli_cur = _path_segment(read_cliente_slug_v2(faturamento_params_path))
                    path_emp_cur = "_multi"
                    out_dir = root / path_cli_cur / "faturamento" / "current"
                else:
                    # Faturamento V1 (params legado, deprecado): saída por --cliente / --empresa na CLI.
                    out_dir = root / path_cliente / path_empresa / mod / "current"
            else:
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
            elif mod == "devolucoes":
                assert base_dir is not None
                ds_emp = (os.environ.get("FDL_DATASET_EMPRESA") or "").strip() or str(args.empresa).strip()
                _materialize_devolucoes(
                    base_dir=base_dir,
                    out_dir=out_dir,
                    path_cliente=path_cliente,
                    path_empresa=path_empresa,
                    org_id=str(args.org_id).strip(),
                    dataset_empresa=ds_emp,
                    pipeline_revision=rev,
                )
            else:
                assert faturamento_params_path is not None
                _materialize_faturamento(
                    params_path=faturamento_params_path,
                    out_dir=out_dir,
                    path_cliente=path_cli_cur,
                    path_empresa=path_emp_cur,
                    pipeline_revision=rev_fat,
                )
            mod_rev = rev_fat if mod == "faturamento" else rev
            print(f"  OK dataset.parquet + metadata.json (pipeline_revision={mod_rev})")
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
