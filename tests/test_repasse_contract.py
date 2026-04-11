"""Contrato compartilhado do repasse materializado (PR1 — sem lógica de pipeline/app)."""
from __future__ import annotations

from processing.repasse_contract import (
    REPASSE_ACTION_COLUMN,
    REPASSE_ARTIFACT_FILENAME,
    REPASSE_CONTRACT_INVARIANTS,
    REPASSE_EXPORT_COLUMN_ORDER,
    REPASSE_FILTER_COLUMNS,
    REPASSE_IDENTITY_COLUMNS,
    REPASSE_KPI_COLUMNS,
    REPASSE_REQUIRED_COLUMNS,
    REPASSE_TECHNICAL_COLUMNS,
    repasse_parquet_path_under_data_products,
    repasse_parquet_relative_path,
)


def test_acao_sugerida_is_canonical_action_and_in_required() -> None:
    assert REPASSE_ACTION_COLUMN == "Ação sugerida"
    assert REPASSE_ACTION_COLUMN in REPASSE_REQUIRED_COLUMNS


def test_data_periodo_repasse_in_required() -> None:
    assert "Data período repasse" in REPASSE_REQUIRED_COLUMNS


def test_artifact_filename_is_dataset_parquet() -> None:
    assert REPASSE_ARTIFACT_FILENAME == "dataset.parquet"


def test_relative_path_matches_convention() -> None:
    rel = repasse_parquet_relative_path("cliente_2", "gama_home")
    assert rel == "cliente_2/gama_home/repasse/current/dataset.parquet"


def test_full_path_under_data_products() -> None:
    full = repasse_parquet_path_under_data_products("cliente_2", "gama_home")
    assert full == "data_products/cliente_2/gama_home/repasse/current/dataset.parquet"


def test_filter_and_kpi_reference_action_column() -> None:
    assert REPASSE_ACTION_COLUMN in REPASSE_FILTER_COLUMNS
    assert REPASSE_ACTION_COLUMN in REPASSE_KPI_COLUMNS


def test_identity_columns_expected() -> None:
    assert REPASSE_IDENTITY_COLUMNS == frozenset(
        {"cliente_id", "empresa_id", "cnpj", "empresa"}
    )


def test_export_order_includes_action_column_once() -> None:
    assert REPASSE_EXPORT_COLUMN_ORDER.count(REPASSE_ACTION_COLUMN) == 1
    assert REPASSE_ACTION_COLUMN in REPASSE_EXPORT_COLUMN_ORDER


def test_technical_columns_disjoint_from_required_minimum() -> None:
    assert not (REPASSE_TECHNICAL_COLUMNS & REPASSE_REQUIRED_COLUMNS)


def test_invariants_non_empty() -> None:
    assert len(REPASSE_CONTRACT_INVARIANTS) >= 3
    assert any("N° de venda" in inv for inv in REPASSE_CONTRACT_INVARIANTS)
    assert any("Parquet" in inv or "REPASSE_ARTIFACT" in inv for inv in REPASSE_CONTRACT_INVARIANTS)
