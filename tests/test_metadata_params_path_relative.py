"""Portabilidade de ``params_path`` em metadata.json (Windows → Linux / Streamlit Cloud)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


def test_params_path_str_relative_when_json_inside_repo() -> None:
    from processing.materialize_financeiro import REPO_ROOT as MR, _params_path_str_for_metadata

    p = MR / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    if not p.is_file():
        pytest.skip("params JSON ausente no clone")
    s = _params_path_str_for_metadata(p)
    assert not s.startswith("C:\\"), f"path ficou absoluto Windows: {s!r}"
    assert s.startswith("ops/"), f"esperado relativo ops/: {s!r}"
    assert s.endswith("faturamento_params_cliente_2_gama_star_eap.json")


def test_params_path_str_absolute_when_outside_repo(tmp_path: Path) -> None:
    from processing.materialize_financeiro import _params_path_str_for_metadata

    ef = tmp_path / "outside_params.json"
    ef.write_text("{}", encoding="utf-8")
    s = _params_path_str_for_metadata(ef)
    assert Path(s).is_file()
    rp = Path(s).resolve()
    try:
        rp.relative_to(REPO_ROOT.resolve())
    except ValueError:
        return
    pytest.fail(f"path deveria estar fora do repo mas resolveu como dentro: {s!r}")


def test_resolve_params_with_relative_path_in_metadata(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """metadata com ``params_path`` relativo: ``resolve`` encontra o ficheiro (cwd = raiz do repo)."""
    from processing.faturamento.params_regime import resolve_faturamento_params_path_for_ui

    p_json = REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    if not p_json.is_file():
        pytest.skip("params JSON ausente no clone")

    monkeypatch.chdir(REPO_ROOT)

    parquet_dir = tmp_path / "current"
    parquet_dir.mkdir()
    fake_parquet = parquet_dir / "dataset.parquet"
    fake_parquet.write_bytes(b"")

    meta = {
        "cliente": "cliente_2",
        "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
    }
    (parquet_dir / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")

    load_info: dict[str, object] = {
        "faturamento_path_final_resolved": str(fake_parquet.resolve()),
        "cliente_slug": "cliente_2",
    }
    resolved = resolve_faturamento_params_path_for_ui(load_info)
    assert resolved is not None
    assert resolved.is_file()
