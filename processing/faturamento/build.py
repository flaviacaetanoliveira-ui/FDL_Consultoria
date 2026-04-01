"""
Orquestração do dataset de faturamento (nível item do pedido).

schema_version 2: várias empresas sob cliente_root, custo compartilhado, uma base concatenada (**padrão**).
schema_version 1: uma pasta de pedidos + custo — **deprecado (legado)**; sem novas evoluções.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from .calc import compute_financial_columns, resolve_coluna_base_imposto
from .config import PIPELINE_REVISION_FATURAMENTO
from .flags import apply_faturamento_flags
from .io_custo import load_custo_xlsx
from .io_pedidos import load_all_pedidos_csv_concatenated, load_latest_pedidos_csv
from .join_custo import join_custo_produto
from .params import (
    FaturamentoParams,
    FaturamentoParamsV2,
    load_faturamento_params,
)
from .validate import (
    FaturamentoValidationError,
    assert_all_skus_have_custo,
    assert_required_columns_pedido,
    assert_sku_unique_custo,
    assign_custo_audit_columns,
    normalized_duplicate_sku_keys_custo,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _strip_str_series(s: pd.Series) -> pd.Series:
    t = s.fillna("").astype(str).str.strip()
    return t.mask(t.str.lower().eq("nan"), "")


def _coalesce_nota_fiscal_em_numero_da_nota(out: pd.DataFrame) -> pd.DataFrame:
    """
    Só copia valores de colunas cuja designação deixa claro que é nota fiscal (nunca a coluna genérica «Número»).

    O export típico de **pedidos** do ML não traz NF explícita; nesse caso «Número da nota» fica vazio até haver
    outra fonte (ex. notas / repasse). Isto não inventa dados: só alinha cabeçalhos alternativos **já nomeados** como NF.
    """
    col = "Número da nota"
    if col not in out.columns:
        return out
    merged = _strip_str_series(out[col])
    for alias in (
        "Número da nota fiscal",
        "Numero da nota fiscal",
        "Nº da nota fiscal",
        "Nº nota fiscal",
        "Numero nota fiscal",
        "Número NF-e",
        "Numero NF-e",
        "NF-e número",
        "NF-e numero",
        "Número NF",
        "Numero NF",
        "NF número",
        "NF numero",
        "Nota fiscal número",
        "Nota fiscal numero",
    ):
        if alias in out.columns and alias != col:
            fb = _strip_str_series(out[alias])
            merged = merged.where(merged.ne(""), fb)
    out[col] = merged
    return out


def _normalize_pedidos_export(df: pd.DataFrame) -> pd.DataFrame:
    """Exports ML frequentes: «Código (SKU)»; colunas de NF por vezes ausentes."""
    out = df.copy()
    if "Código" not in out.columns and "Código (SKU)" in out.columns:
        out = out.rename(columns={"Código (SKU)": "Código"})
    if "Existe Nota Fiscal gerada" not in out.columns:
        out["Existe Nota Fiscal gerada"] = ""
    if "Número da nota" not in out.columns:
        out["Número da nota"] = ""
    out = _coalesce_nota_fiscal_em_numero_da_nota(out)
    return out


def _resolve_pedidos_dir(params: FaturamentoParams) -> Path:
    raw = params.pedidos_dir or os.environ.get("FDL_PEDIDOS_DIR", "").strip()
    if not raw:
        raise FaturamentoValidationError(
            "Defina pedidos_dir em faturamento_params.json ou a variável de ambiente FDL_PEDIDOS_DIR."
        )
    return Path(raw).expanduser().resolve()


def _resolve_custo_xlsx(params: FaturamentoParams) -> Path:
    raw = params.custo_xlsx or os.environ.get("FDL_TABELA_CUSTO_PATH", "").strip()
    if not raw:
        raise FaturamentoValidationError(
            "Defina custo_xlsx em faturamento_params.json ou a variável de ambiente FDL_TABELA_CUSTO_PATH."
        )
    return Path(raw).expanduser().resolve()


def _build_faturamento_dataset_v1(params: FaturamentoParams, params_path: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """Build faturamento **V1 deprecada** (uma org, assert SKU com custo). Preferir V2."""
    pedidos_dir = _resolve_pedidos_dir(params)
    custo_path = _resolve_custo_xlsx(params)

    df_p, meta_ped = load_latest_pedidos_csv(pedidos_dir)
    assert_required_columns_pedido(df_p)

    df_c, meta_custo = load_custo_xlsx(custo_path)
    assert_sku_unique_custo(df_c)

    df = join_custo_produto(df_p, df_c)
    assert_all_skus_have_custo(df)
    df = assign_custo_audit_columns(df, frozenset())

    data_proc = _utc_now_iso()
    base_col = "Valor total"
    df = compute_financial_columns(
        df,
        aliquota_imposto=params.aliquota_imposto,
        aliquota_despesas_fixas=params.aliquota_despesas_fixas,
        data_processamento_iso=data_proc,
        base_imposto_column=base_col,
    )
    df = apply_faturamento_flags(df, permite_sem_nf=params.permite_faturamento_sem_nf)

    meta: dict[str, object] = {
        "schema_version": 1,
        "pipeline_revision": PIPELINE_REVISION_FATURAMENTO,
        "params_path": str(params_path.resolve()),
        "pedidos": meta_ped,
        "custo": meta_custo,
        "permite_faturamento_sem_nf": params.permite_faturamento_sem_nf,
        "aliquota_imposto": params.aliquota_imposto,
        "aliquota_despesas_fixas": params.aliquota_despesas_fixas,
        "coluna_base_imposto_resolvida": base_col,
        "data_processamento": data_proc,
        "row_count": len(df),
    }
    return df, meta


def _build_faturamento_dataset_v2(
    params: FaturamentoParamsV2, params_path: Path
) -> tuple[pd.DataFrame, dict[str, object]]:
    df_c, meta_custo = load_custo_xlsx(params.custo_xlsx_resolved)
    dup_keys = normalized_duplicate_sku_keys_custo(df_c)

    parts: list[pd.DataFrame] = []
    emp_sources: list[dict[str, object]] = []

    for emp in params.empresas:
        ped_dir = (params.cliente_root / emp.pedidos_dir).resolve()
        df_p, meta_ped = load_all_pedidos_csv_concatenated(ped_dir)
        df_p = _normalize_pedidos_export(df_p)
        assert_required_columns_pedido(df_p)
        df_p = df_p.copy()
        df_p["empresa"] = emp.empresa
        df_p["org_id"] = emp.org_id
        df_p["cliente_slug"] = params.cliente_slug
        if "pedidos_arquivo" not in df_p.columns:
            df_p["pedidos_arquivo"] = str(meta_ped.get("arquivo", ""))

        df_j = join_custo_produto(df_p, df_c)
        df_j = assign_custo_audit_columns(df_j, dup_keys)

        permite = emp.permite_faturamento_sem_nf
        if permite is None:
            permite = params.permite_faturamento_sem_nf_default
        df_j["_permite_sem_nf"] = bool(permite)

        parts.append(df_j)
        emp_sources.append(
            {
                "org_id": emp.org_id,
                "empresa": emp.empresa,
                "pedidos_dir": str(ped_dir),
                **meta_ped,
            }
        )

    df = pd.concat(parts, ignore_index=True)
    base_resolved = resolve_coluna_base_imposto(df, params.coluna_base_imposto)

    data_proc = _utc_now_iso()
    df = compute_financial_columns(
        df,
        aliquota_imposto=params.aliquota_imposto,
        aliquota_despesas_fixas=params.aliquota_despesas_fixas,
        data_processamento_iso=data_proc,
        base_imposto_column=base_resolved,
    )
    df = apply_faturamento_flags(df, permite_sem_nf=df["_permite_sem_nf"])
    df = df.drop(columns=["_permite_sem_nf"], errors="ignore")

    status_counts = {}
    if "Status_Custo" in df.columns:
        status_counts = {str(k): int(v) for k, v in df["Status_Custo"].value_counts().items()}

    meta: dict[str, object] = {
        "schema_version": 2,
        "pipeline_revision": PIPELINE_REVISION_FATURAMENTO,
        "params_path": str(params_path.resolve()),
        "cliente_slug": params.cliente_slug,
        "cliente_root": str(params.cliente_root.resolve()),
        "custo": meta_custo,
        "empresas_fonte": emp_sources,
        "coluna_base_imposto_candidatas": list(params.coluna_base_imposto),
        "coluna_base_imposto_resolvida": base_resolved,
        "aliquota_imposto": params.aliquota_imposto,
        "aliquota_despesas_fixas": params.aliquota_despesas_fixas,
        "permite_faturamento_sem_nf_default": params.permite_faturamento_sem_nf_default,
        "data_processamento": data_proc,
        "row_count": len(df),
        "status_custo_counts": status_counts,
    }
    return df, meta


def build_faturamento_dataset(params_path: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Devolve (DataFrame final, meta dict com paths de fonte e pipeline_revision).

    Params V1 (deprecado) continuam suportados até remoção planejada — ver documentação do pipeline.
    """
    params_union = load_faturamento_params(params_path)
    if isinstance(params_union, FaturamentoParamsV2):
        return _build_faturamento_dataset_v2(params_union, params_path)
    return _build_faturamento_dataset_v1(params_union, params_path)
