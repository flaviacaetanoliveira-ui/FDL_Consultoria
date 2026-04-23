"""Regressão: chaves de cache RG, tabela única, ficha lazy (sem executar Streamlit)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from app.components.rg_cached_compute import DEFAULT_RG_PIPELINE_VERSION, pipeline_version
from processing.faturamento.rg_cache_keys import (
    PIPELINE_VERSION_ENV_NAME,
    dataframe_cache_token,
    normalize_sorted_str_tuple,
    rg_core_identity,
    slice_hash_for_dependents,
)


def test_cache_invalida_com_mudanca_de_filtro() -> None:
    df = pd.DataFrame({"a": [1]})
    tok = dataframe_cache_token(df)
    k_a = rg_core_identity(
        tok,
        ("Gama Home",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        10.0,
        "v1",
        "sl",
    )
    k_b = rg_core_identity(
        tok,
        ("Mega Star",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        10.0,
        "v1",
        "sl",
    )
    assert k_a != k_b


def test_cache_invalida_com_nova_pipeline_version() -> None:
    df = pd.DataFrame({"a": [1]})
    tok = dataframe_cache_token(df)
    k1 = rg_core_identity(
        tok,
        ("E",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        5.0,
        "pv-a",
        "",
    )
    k2 = rg_core_identity(
        tok,
        ("E",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        5.0,
        "pv-b",
        "",
    )
    assert k1 != k2
    assert slice_hash_for_dependents(
        tok,
        ("E",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        "pv-a",
        "",
    ) != slice_hash_for_dependents(
        tok,
        ("E",),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        "pv-b",
        "",
    )


def test_ordem_de_empresas_nao_afeta_cache() -> None:
    assert normalize_sorted_str_tuple(["Gama", "Mega"]) == normalize_sorted_str_tuple(["Mega", "Gama"])
    df = pd.DataFrame({"a": [1]})
    tok = dataframe_cache_token(df)
    k1 = rg_core_identity(
        tok,
        normalize_sorted_str_tuple(["Gama", "Mega"]),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        1.0,
        "v",
        "",
    )
    k2 = rg_core_identity(
        tok,
        normalize_sorted_str_tuple(["Mega", "Gama"]),
        (),
        date(2026, 1, 1),
        date(2026, 4, 17),
        1.0,
        "v",
        "",
    )
    assert k1 == k2


def test_tabela_unica_renderizada() -> None:
    root = Path(__file__).resolve().parents[1]
    src = (root / "app" / "components" / "tabela_pedidos_gerencial.py").read_text(encoding="utf-8")
    assert "Detalhe por pedido" not in src
    assert src.count("st.dataframe(") <= 2
    assert 'on_select="rerun"' in src


def test_ficha_calculada_apenas_quando_aberta() -> None:
    """Ficha só é calculada dentro do ramo ``if pids_ficha`` (linhas selecionadas na tabela)."""
    root = Path(__file__).resolve().parents[1]
    src = (root / "app" / "components" / "tabela_pedidos_gerencial.py").read_text(encoding="utf-8")
    assert "if pids_ficha:" in src
    assert "tab_linhas_full=linhas_full_list" in src
    block_start = src.find("if pids_ficha:")
    assert block_start != -1
    assert "compute_ficha_pedido(" in src[block_start:]


def test_pipeline_version_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PIPELINE_VERSION_ENV_NAME, "custom_rg_pv")
    assert pipeline_version() == "custom_rg_pv"


def test_pipeline_version_env_vazio_usa_padrao(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(PIPELINE_VERSION_ENV_NAME, "")
    assert pipeline_version() == DEFAULT_RG_PIPELINE_VERSION


def test_pipeline_version_sem_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(PIPELINE_VERSION_ENV_NAME, raising=False)
    assert pipeline_version() == DEFAULT_RG_PIPELINE_VERSION
