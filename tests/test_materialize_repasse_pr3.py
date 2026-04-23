"""PR3 — materializador repasse: empresa_label e mesmo DataFrame para Parquet/CSV."""
from __future__ import annotations

from unittest.mock import patch

import pandas as pd

from processing.materialize_financeiro import (
    _materialize_repasse,
    _repasse_empresa_label_for_materialize,
)


def test_repasse_empresa_label_slug_gama_home() -> None:
    assert _repasse_empresa_label_for_materialize("gama_home") == "Gama Home"


def test_repasse_empresa_label_explicit_env_overrides_map(monkeypatch) -> None:
    monkeypatch.setenv("FDL_DATASET_EMPRESA", "Nome Custom")
    try:
        assert _repasse_empresa_label_for_materialize("gama_home") == "Nome Custom"
    finally:
        monkeypatch.delenv("FDL_DATASET_EMPRESA", raising=False)


def test_repasse_empresa_label_unknown_slug_fallback() -> None:
    assert _repasse_empresa_label_for_materialize("foo_bar") == "Foo Bar"


def test_materialize_repasse_passes_empresa_label_to_carregar(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("FDL_DATASET_EMPRESA", raising=False)
    captured: dict[str, object] = {}

    def fake_carregar(base_dir, *, empresa_label=None):
        captured["base_dir"] = base_dir
        captured["empresa_label"] = empresa_label
        return (
            pd.DataFrame(
                {
                    "N° de venda": ["1"],
                    "empresa": ["Gama Home"],
                    "x": [1],
                }
            ),
            {"linhas": 1},
        )

    parquet_received: list[pd.DataFrame] = []
    csv_received: list[pd.DataFrame] = []

    def capture_parquet(df, path):
        parquet_received.append(df.copy())

    def capture_csv(df, path):
        csv_received.append(df.copy())

    base = tmp_path / "base"
    base.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with (
        patch(
            "etapa4b_integracao_contas_receber.carregar_tabela_final_operacional",
            side_effect=fake_carregar,
        ),
        patch("processing.materialize_financeiro.build_repasse_source_signature", return_value="abc"),
        patch("processing.materialize_financeiro._enrich_identity_columns", side_effect=lambda df, **kw: df.assign(cliente_id="c", empresa_id="e", cnpj=pd.NA)),
        patch("processing.materialize_financeiro._write_parquet", side_effect=capture_parquet),
        patch("processing.materialize_financeiro._write_repasse_app_mirror_csv", side_effect=capture_csv),
        patch("processing.materialize_financeiro._write_metadata"),
    ):
        _materialize_repasse(
            base_dir=base,
            out_dir=out_dir,
            path_cliente="cliente_2",
            path_empresa="gama_home",
            pipeline_revision="test-rev",
        )

    assert captured.get("empresa_label") == "Gama Home"
    assert captured.get("base_dir") == base
    assert len(csv_received) == 1
    assert len(parquet_received) == 1
    # Mesmo núcleo de negócio: CSV == Parquet sem colunas de identidade técnica
    pq_business = parquet_received[0].drop(columns=["cliente_id", "empresa_id", "cnpj"], errors="ignore")
    pd.testing.assert_frame_equal(csv_received[0].reset_index(drop=True), pq_business.reset_index(drop=True))


def test_materialize_repasse_empresa_column_matches_label_in_dataframe(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("FDL_DATASET_EMPRESA", raising=False)

    def fake_carregar(base_dir, *, empresa_label=None):
        return (
            pd.DataFrame({"N° de venda": ["1"], "empresa": [empresa_label]}),
            {},
        )

    stored: list[pd.DataFrame] = []

    def capture_csv(df, path):
        stored.append(df.copy())

    base = tmp_path / "base"
    base.mkdir()
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    with (
        patch(
            "etapa4b_integracao_contas_receber.carregar_tabela_final_operacional",
            side_effect=fake_carregar,
        ),
        patch("processing.materialize_financeiro.build_repasse_source_signature", return_value="sig"),
        patch("processing.materialize_financeiro._enrich_identity_columns", side_effect=lambda df, **kw: df.assign(cliente_id="c", empresa_id="e", cnpj=pd.NA)),
        patch("processing.materialize_financeiro._write_parquet"),
        patch("processing.materialize_financeiro._write_repasse_app_mirror_csv", side_effect=capture_csv),
        patch("processing.materialize_financeiro._write_metadata"),
    ):
        _materialize_repasse(
            base_dir=base,
            out_dir=out_dir,
            path_cliente="x",
            path_empresa="mega_star",
            pipeline_revision="r",
        )

    assert len(stored) == 1
    assert stored[0].iloc[0]["empresa"] == "Mega Star"
