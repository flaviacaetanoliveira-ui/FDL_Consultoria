"""Pipeline de faturamento (Fase 1 — sem UI).

V2 (schema_version >= 2) é o padrão oficial. Params/dataset V1 permanecem como legado deprecado
até migração completa no app e dados — ver ``docs/faturamento_pipeline.md``.
"""
from __future__ import annotations

from .build import build_faturamento_dataset
from .params import (
    EmpresaFaturamentoEntry,
    FaturamentoParams,
    FaturamentoParamsError,
    FaturamentoParamsV2,
    load_faturamento_params,
    peek_faturamento_schema_version,
    read_cliente_slug_v2,
)
from .validate import FaturamentoValidationError

__all__ = [
    "EmpresaFaturamentoEntry",
    "FaturamentoParams",
    "FaturamentoParamsError",
    "FaturamentoParamsV2",
    "FaturamentoValidationError",
    "build_faturamento_dataset",
    "load_faturamento_params",
    "peek_faturamento_schema_version",
    "read_cliente_slug_v2",
]
