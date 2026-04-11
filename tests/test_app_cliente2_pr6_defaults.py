"""PR6 — defaults do entrypoint Grupo Mega Fácil (Parquet + strict materialized)."""

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[1]
_APP_CLIENTE2 = _REPO_ROOT / "app_cliente2.py"

_CLIENTE2_SETDEFAULT_KEYS = (
    "FDL_MATERIALIZED_CLIENTE_SLUG",
    "FDL_MATERIALIZED_PATH_MODE",
    "FDL_REPASSE_CONSUME_MODE",
    "FDL_REPASSE_USE_PARQUET",
    "FDL_STRICT_MATERIALIZED",
    "FDL_FRETE_CONSUME_MODE",
    "FDL_DEVOLUCOES_CONSUME_MODE",
    "FDL_ENABLED_FINANCE_MODULES",
)


def _install_fake_streamlit_bootstrap() -> None:
    mod = types.ModuleType("fdl_streamlit_bootstrap")

    def _noop(**_kwargs: object) -> None:
        return None

    mod.run_operacional_app = _noop
    sys.modules["fdl_streamlit_bootstrap"] = mod


def _exec_app_cliente2(*, module_name: str) -> None:
    spec = importlib.util.spec_from_file_location(module_name, _APP_CLIENTE2)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    sys.modules.pop(module_name, None)


@pytest.fixture
def _isolate_cliente2_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in _CLIENTE2_SETDEFAULT_KEYS:
        monkeypatch.delenv(k, raising=False)


def test_app_cliente2_sets_parquet_and_strict_materialized_by_default(
    _isolate_cliente2_env: None,
) -> None:
    """Carrega o ficheiro como módulo isolado: aplica setdefault sem correr Streamlit real."""
    _install_fake_streamlit_bootstrap()
    try:
        _exec_app_cliente2(module_name="_app_cliente2_pr6_under_test")
    finally:
        sys.modules.pop("fdl_streamlit_bootstrap", None)

    assert os.environ.get("FDL_REPASSE_USE_PARQUET") == "1"
    assert os.environ.get("FDL_STRICT_MATERIALIZED", "").strip().lower() == "true"
    assert os.environ.get("FDL_REPASSE_CONSUME_MODE") == "materialized"
    assert os.environ.get("FDL_MATERIALIZED_CLIENTE_SLUG") == "cliente_2"


def test_app_cliente2_does_not_override_explicit_env(
    _isolate_cliente2_env: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """setdefault preserva valores já definidos no ambiente (ex.: Cloud / CI)."""
    monkeypatch.setenv("FDL_REPASSE_USE_PARQUET", "0")
    monkeypatch.setenv("FDL_STRICT_MATERIALIZED", "false")
    _install_fake_streamlit_bootstrap()
    try:
        _exec_app_cliente2(module_name="_app_cliente2_pr6_under_test_b")
    finally:
        sys.modules.pop("fdl_streamlit_bootstrap", None)

    assert os.environ.get("FDL_REPASSE_USE_PARQUET") == "0"
    assert os.environ.get("FDL_STRICT_MATERIALIZED", "").strip().lower() == "false"
