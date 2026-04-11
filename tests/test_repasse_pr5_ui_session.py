"""PR5 — caminho UI Parquet vs legado (helpers sem Streamlit)."""

from __future__ import annotations

from datetime import datetime

import pandas as pd

from processing.repasse_contract import REPASSE_ACTION_COLUMN
from processing.repasse_ui_session import (
    COL_ACAO_LEGACY_UI,
    COL_DATA_PERIODO_REPASSE,
    repasse_ui_acao_column,
    repasse_ui_apply_filtro_somente_linhas_com_data_pagamento,
    repasse_ui_apply_pipeline_exclusao_na_ui,
    repasse_ui_periodo_series_parquet,
)


def test_acao_column_parquet_uses_canonical() -> None:
    assert repasse_ui_acao_column(use_parquet=True) == REPASSE_ACTION_COLUMN


def test_acao_column_legacy_uses_operacional() -> None:
    assert repasse_ui_acao_column(use_parquet=False) == COL_ACAO_LEGACY_UI


def test_parquet_periodo_only_reads_materialized_column_not_payment() -> None:
    """Se só existisse pagamento, o caminho Parquet não o usaria — aqui divergem datas."""
    df = pd.DataFrame(
        {
            COL_DATA_PERIODO_REPASSE: ["2026-02-01"],
            "Data de pagamento": ["2025-12-01"],
        }
    )
    s, label = repasse_ui_periodo_series_parquet(
        df,
        parse_data_periodo_repasse_column=lambda d: pd.to_datetime(
            d[COL_DATA_PERIODO_REPASSE], errors="coerce"
        ),
    )
    assert label == COL_DATA_PERIODO_REPASSE
    assert pd.Timestamp(s.iloc[0]).date() == datetime(2026, 2, 1).date()


def test_parquet_periodo_missing_column_nat_not_error() -> None:
    df = pd.DataFrame({"x": [1]})
    s, label = repasse_ui_periodo_series_parquet(
        df,
        parse_data_periodo_repasse_column=lambda d: pd.Series(pd.NaT, index=d.index),
    )
    assert "ausente" in label
    assert bool(s.isna().all())


def test_parquet_periodo_empty_strings_tolerated() -> None:
    df = pd.DataFrame({COL_DATA_PERIODO_REPASSE: ["", None, "2026-01-15"]})
    s, _ = repasse_ui_periodo_series_parquet(
        df,
        parse_data_periodo_repasse_column=lambda d: pd.to_datetime(
            d[COL_DATA_PERIODO_REPASSE], errors="coerce"
        ),
    )
    assert s.isna().sum() == 2
    assert pd.notna(s.iloc[2])


def test_legacy_applies_ui_exclusao_parquet_skips() -> None:
    assert repasse_ui_apply_pipeline_exclusao_na_ui(use_parquet=False) is True
    assert repasse_ui_apply_pipeline_exclusao_na_ui(use_parquet=True) is False


def test_legacy_filters_data_pagamento_parquet_skips() -> None:
    assert repasse_ui_apply_filtro_somente_linhas_com_data_pagamento(use_parquet=False) is True
    assert repasse_ui_apply_filtro_somente_linhas_com_data_pagamento(use_parquet=True) is False
