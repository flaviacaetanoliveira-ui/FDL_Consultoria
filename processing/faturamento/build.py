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

from .calc import compute_financial_columns, compute_financial_columns_regras_fechadas, resolve_coluna_base_imposto
from .config import PIPELINE_REVISION_FATURAMENTO
from .flags import apply_faturamento_flags
from .io_custo import load_custo_xlsx
from .io_pedidos import dedupe_pedidos_multiloja_codigo, load_all_pedidos_csv_concatenated, load_latest_pedidos_csv
from .join_custo import join_custo_produto
from .join_custo_notas_itens import enrich_custo_from_notas_itens
from .join_notas import enrich_pedidos_com_notas
from .ml_order_fees import allocate_multiloja_order_level_fees
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


def _per_row_aliquotas_v2(df: pd.DataFrame, params: FaturamentoParamsV2) -> tuple[pd.Series, pd.Series]:
    imp_map = {
        e.org_id: float(e.aliquota_imposto if e.aliquota_imposto is not None else params.aliquota_imposto)
        for e in params.empresas
    }
    desp_map = {
        e.org_id: float(
            e.aliquota_despesas_fixas if e.aliquota_despesas_fixas is not None else params.aliquota_despesas_fixas
        )
        for e in params.empresas
    }
    oid = df["org_id"].astype(str).str.strip()
    s_imp = oid.map(lambda x: imp_map.get(x, params.aliquota_imposto)).astype(float)
    s_desp = oid.map(lambda x: desp_map.get(x, params.aliquota_despesas_fixas)).astype(float)
    return s_imp, s_desp


def _normalize_pedidos_export(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajustes leves entre exports equivalentes (ML / mesmo layout «completo»).

    O faturamento exige as colunas de ``REQUIRED_PEDIDO_COLUMNS`` (incl. «Quantidade»,
    «Preço de lista», «Valor total», «Número do pedido») — export padrão completo do ML
    ou ficheiros no mesmo layout; não se inventam valores quando faltam.
    """
    out = df.copy()
    if "Código" not in out.columns and "Código (SKU)" in out.columns:
        out = out.rename(columns={"Código (SKU)": "Código"})
    _col_multiloja = "Número do pedido multiloja"
    if _col_multiloja not in out.columns:
        for alias in (
            "Numero do pedido multiloja",
            "Nº do pedido multiloja",
            "Nº do Pedido Multiloja",
            "Pedido multiloja",
            "Número pedido multiloja",
        ):
            if alias in out.columns:
                out = out.rename(columns={alias: _col_multiloja})
                break
    if _col_multiloja not in out.columns and "Número do pedido" in out.columns:
        # Export incompleto: sem coluna multiloja — usar o próprio nº do pedido (1 linha = 1 chave de taxa).
        out[_col_multiloja] = out["Número do pedido"]
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
    df, meta_ml_fees_v1 = allocate_multiloja_order_level_fees(df)

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
        **{k: v for k, v in meta_ml_fees_v1.items() if str(k).startswith("ml_fee_")},
    }
    return df, meta


def _build_faturamento_dataset_v2(
    params: FaturamentoParamsV2, params_path: Path
) -> tuple[pd.DataFrame, dict[str, object]]:
    from .io_params_mensais import load_params_mensais_dataframe

    df_c, meta_custo = load_custo_xlsx(params.custo_xlsx_resolved)
    dup_keys = normalized_duplicate_sku_keys_custo(df_c)

    params_mensais_df = None
    if params.params_mensais_resolved is not None and params.params_mensais_resolved.is_file():
        params_mensais_df = load_params_mensais_dataframe(params.params_mensais_resolved)

    parts: list[pd.DataFrame] = []
    emp_sources: list[dict[str, object]] = []
    notas_meta_por_empresa: list[dict[str, object]] = []

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
        df_j, meta_ml_fees = allocate_multiloja_order_level_fees(df_j)

        permite = emp.permite_faturamento_sem_nf
        if permite is None:
            permite = params.permite_faturamento_sem_nf_default
        df_j["_permite_sem_nf"] = bool(permite)

        rel_notas = (emp.notas_saida_dir or params.notas_saida_dir).strip() or params.notas_saida_dir
        notas_dir = (params.cliente_root / rel_notas).resolve()
        df_j, meta_notas = enrich_pedidos_com_notas(
            df_j,
            notas_dir=notas_dir,
            org_id=emp.org_id,
            empresa=emp.empresa,
        )
        df_j, meta_custo_nf = enrich_custo_from_notas_itens(
            df_j,
            df_c,
            notas_dir=notas_dir,
            org_id=emp.org_id,
            empresa=emp.empresa,
        )
        df_j, meta_dedupe = dedupe_pedidos_multiloja_codigo(df_j)
        notas_meta_por_empresa.append(
            {"org_id": emp.org_id, **meta_notas, **meta_custo_nf, **meta_ml_fees, **meta_dedupe}
        )

        parts.append(df_j)
        emp_sources.append(
            {
                "org_id": emp.org_id,
                "empresa": emp.empresa,
                "pedidos_dir": str(ped_dir),
                **meta_ped,
                **meta_dedupe,
            }
        )

    df = pd.concat(parts, ignore_index=True)
    base_resolved = resolve_coluna_base_imposto(df, params.coluna_base_imposto)

    data_proc = _utc_now_iso()
    s_imp, s_desp = _per_row_aliquotas_v2(df, params)
    df = compute_financial_columns_regras_fechadas(
        df,
        df_params_mensais=params_mensais_df,
        fallback_aliquota_imposto=params.aliquota_imposto,
        fallback_despesa_fixa=params.aliquota_despesas_fixas,
        data_processamento_iso=data_proc,
        per_row_aliquota_imposto=s_imp,
        per_row_despesa_fixa=s_desp,
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
        "aliquota_por_org_id": {
            e.org_id: {
                "aliquota_imposto": float(
                    e.aliquota_imposto if e.aliquota_imposto is not None else params.aliquota_imposto
                ),
                "aliquota_despesas_fixas": float(
                    e.aliquota_despesas_fixas if e.aliquota_despesas_fixas is not None else params.aliquota_despesas_fixas
                ),
            }
            for e in params.empresas
        },
        "permite_faturamento_sem_nf_default": params.permite_faturamento_sem_nf_default,
        "data_processamento": data_proc,
        "row_count": len(df),
        "status_custo_counts": status_counts,
        "params_mensais_path": str(params.params_mensais_resolved.resolve())
        if params.params_mensais_resolved
        else None,
        "notas_saida_dir_default": params.notas_saida_dir,
        "notas_por_empresa": notas_meta_por_empresa,
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
