"""Resolução de path de params (cwd vs repo) e carga para UI."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from processing.faturamento.params_regime import (
    _REPO_ROOT,
    load_faturamento_params_for_ui,
    resolve_faturamento_params_path_for_ui,
)


def test_resolve_branch1_load_info_com_params_path_absoluto() -> None:
    p_json = _REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    if not p_json.is_file():
        pytest.skip("JSON ops cliente_2 ausente")
    info: dict[str, object] = {"params_path": str(p_json.resolve())}
    got = resolve_faturamento_params_path_for_ui(info)
    assert got is not None and got.is_file()


def test_resolve_somente_cliente_slug_fallback_absoluto() -> None:
    info: dict[str, object] = {"cliente_slug": "cliente_2"}
    got = resolve_faturamento_params_path_for_ui(info)
    assert got is not None and got.is_file()
    assert "ops" in str(got).replace("\\", "/")


def test_resolve_metadata_params_path_relativo_via_repo_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    p_json = _REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    if not p_json.is_file():
        pytest.skip("JSON ops cliente_2 ausente")
    cur = tmp_path / "current"
    cur.mkdir()
    fake_parquet = cur / "dataset.parquet"
    fake_parquet.write_bytes(b"")
    meta = {"cliente": "cliente_2", "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json"}
    (cur / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    info: dict[str, object] = {"faturamento_path_final_resolved": str(fake_parquet.resolve())}
    got = resolve_faturamento_params_path_for_ui(info)
    assert got is not None and got.resolve() == p_json.resolve()


def test_resolve_load_info_vazio_retorna_none() -> None:
    assert resolve_faturamento_params_path_for_ui({}) is None


def test_load_faturamento_params_for_ui_relaxado_quando_cliente_root_inexistente(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Simula JSON V2 com cliente_root Windows inexistente — UI relaxada ainda devolve V2."""
    ref = _REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    if not ref.is_file():
        pytest.skip("JSON ops cliente_2 ausente")
    raw = json.loads(ref.read_text(encoding="utf-8"))
    raw["cliente_root"] = "C:\\NaoExiste\\Cliente_2_fake"
    fake = tmp_path / "params_ui.json"
    fake.write_text(json.dumps(raw), encoding="utf-8")
    info: dict[str, object] = {"params_path": str(fake)}
    pu = load_faturamento_params_for_ui(info)
    assert pu is not None
    assert hasattr(pu, "empresas")
