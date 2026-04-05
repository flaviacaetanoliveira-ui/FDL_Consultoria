from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Any, Callable

import pandas as pd

from etapa1_vendas import build_vendas_tratadas_from_folder
from etapa2_liberacoes import build_liberacoes_from_folder
from fdl_paths import resolve_pasta_vendas_ml

# Incrementar quando o pipeline de bases mudar (ex.: conceito de Valor pago nas liberações).
# Invalida st.cache_data sem depender só do botão «Atualizar dados».
PIPELINE_DATA_REVISION = 3

try:
    import streamlit as st
except Exception:  # noqa: BLE001
    st = None


def _cache_data_compat(*args: Any, **kwargs: Any) -> Callable:
    """
    Decorator compatível com ambiente Streamlit ou script puro.
    Se streamlit não estiver disponível, não aplica cache.
    """

    def deco(func: Callable) -> Callable:
        if st is None:
            return func
        return st.cache_data(*args, **kwargs)(func)

    return deco


@dataclass
class DiagnosticoFonte:
    arquivos_lidos: int
    linhas_tabela: int
    tempo_segundos: float
    detalhes_arquivos: pd.DataFrame


@dataclass
class DiagnosticoBases:
    vendas: DiagnosticoFonte
    liberacoes_tratadas: DiagnosticoFonte
    liberacoes_agregadas: DiagnosticoFonte
    tempo_total_segundos: float


def _paths_por_fonte(base_dir: str | Path) -> tuple[Path, Path]:
    base = Path(base_dir)
    pasta_vendas = resolve_pasta_vendas_ml(base)
    pasta_liberacoes = base / "Liberações_ML"
    return pasta_vendas, pasta_liberacoes


@_cache_data_compat(show_spinner=False)
def carregar_vendas_consolidadas(
    base_dir: str | Path, _revisao: int = PIPELINE_DATA_REVISION
) -> tuple[pd.DataFrame, dict[str, Any]]:
    t0 = perf_counter()
    pasta_vendas, _ = _paths_por_fonte(base_dir)
    vendas_tratadas, diag_vendas = build_vendas_tratadas_from_folder(pasta_vendas)
    elapsed = perf_counter() - t0

    diagnostico = {
        "arquivos_lidos": int(len(diag_vendas)),
        "linhas_tabela": int(len(vendas_tratadas)),
        "tempo_segundos": float(elapsed),
        "detalhes_arquivos": diag_vendas,
    }
    return vendas_tratadas, diagnostico


@_cache_data_compat(show_spinner=False)
def carregar_liberacoes_consolidadas(
    base_dir: str | Path, _revisao: int = PIPELINE_DATA_REVISION
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any], dict[str, Any]]:
    t0 = perf_counter()
    _, pasta_liberacoes = _paths_por_fonte(base_dir)
    liberacoes_tratadas, liberacoes_agregadas, diag_liberacoes = build_liberacoes_from_folder(
        pasta_liberacoes
    )
    elapsed = perf_counter() - t0

    diag_tratadas = {
        "arquivos_lidos": int(len(diag_liberacoes)),
        "linhas_tabela": int(len(liberacoes_tratadas)),
        "tempo_segundos": float(elapsed),
        "detalhes_arquivos": diag_liberacoes,
    }
    diag_agregadas = {
        "arquivos_lidos": int(len(diag_liberacoes)),
        "linhas_tabela": int(len(liberacoes_agregadas)),
        "tempo_segundos": float(elapsed),
        "detalhes_arquivos": diag_liberacoes,
    }
    return liberacoes_tratadas, liberacoes_agregadas, diag_tratadas, diag_agregadas


@_cache_data_compat(show_spinner=False)
def carregar_bases_consolidadas(
    base_dir: str | Path, _revisao: int = PIPELINE_DATA_REVISION
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, DiagnosticoBases]:
    t0 = perf_counter()

    vendas_tratadas, diag_vendas = carregar_vendas_consolidadas(base_dir, _revisao)
    (
        liberacoes_tratadas,
        liberacoes_agregadas,
        diag_lib_tratadas,
        diag_lib_agregadas,
    ) = carregar_liberacoes_consolidadas(base_dir, _revisao)

    total_elapsed = perf_counter() - t0
    diagnostico = DiagnosticoBases(
        vendas=DiagnosticoFonte(**diag_vendas),
        liberacoes_tratadas=DiagnosticoFonte(**diag_lib_tratadas),
        liberacoes_agregadas=DiagnosticoFonte(**diag_lib_agregadas),
        tempo_total_segundos=float(total_elapsed),
    )
    return vendas_tratadas, liberacoes_tratadas, liberacoes_agregadas, diagnostico

