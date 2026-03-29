"""
Orquestração do dataset de faturamento (nível item do pedido).
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import os

import pandas as pd

from .calc import compute_financial_columns
from .config import PIPELINE_REVISION_FATURAMENTO
from .flags import apply_faturamento_flags
from .io_custo import load_custo_xlsx
from .io_pedidos import load_latest_pedidos_csv
from .join_custo import join_custo_produto
from .params import FaturamentoParams, load_faturamento_params
from .validate import (
    FaturamentoValidationError,
    assert_all_skus_have_custo,
    assert_required_columns_pedido,
    assert_sku_unique_custo,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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


def build_faturamento_dataset(params_path: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Devolve (DataFrame final, meta dict com paths de fonte e pipeline_revision).
    """
    params = load_faturamento_params(params_path)
    pedidos_dir = _resolve_pedidos_dir(params)
    custo_path = _resolve_custo_xlsx(params)

    df_p, meta_ped = load_latest_pedidos_csv(pedidos_dir)
    assert_required_columns_pedido(df_p)

    df_c, meta_custo = load_custo_xlsx(custo_path)
    assert_sku_unique_custo(df_c)

    df = join_custo_produto(df_p, df_c)
    assert_all_skus_have_custo(df)

    data_proc = _utc_now_iso()
    df = compute_financial_columns(
        df,
        aliquota_imposto=params.aliquota_imposto,
        aliquota_despesas_fixas=params.aliquota_despesas_fixas,
        data_processamento_iso=data_proc,
    )
    df = apply_faturamento_flags(df, permite_sem_nf=params.permite_faturamento_sem_nf)

    meta: dict[str, object] = {
        "pipeline_revision": PIPELINE_REVISION_FATURAMENTO,
        "params_path": str(params_path.resolve()),
        "pedidos": meta_ped,
        "custo": meta_custo,
        "permite_faturamento_sem_nf": params.permite_faturamento_sem_nf,
        "aliquota_imposto": params.aliquota_imposto,
        "aliquota_despesas_fixas": params.aliquota_despesas_fixas,
        "data_processamento": data_proc,
        "row_count": len(df),
    }
    return df, meta
