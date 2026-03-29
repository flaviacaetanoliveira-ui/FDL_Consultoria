"""Pipeline de faturamento (Fase 1 — sem UI)."""
from __future__ import annotations

from .build import build_faturamento_dataset
from .params import FaturamentoParams, FaturamentoParamsError, load_faturamento_params
from .validate import FaturamentoValidationError

__all__ = [
    "FaturamentoParams",
    "FaturamentoParamsError",
    "FaturamentoValidationError",
    "build_faturamento_dataset",
    "load_faturamento_params",
]
