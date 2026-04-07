"""Materialização Controle de Devoluções ML (fila operacional, só candidatas)."""

from __future__ import annotations

from processing.devolucoes_ml.build import PIPELINE_REVISION_DEVOLUCOES, build_devolucoes_dataset

__all__ = ["PIPELINE_REVISION_DEVOLUCOES", "build_devolucoes_dataset"]
