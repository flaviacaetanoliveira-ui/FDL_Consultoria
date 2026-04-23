"""Guardrail de grelha do repasse (limite de linhas + styler)."""

from __future__ import annotations

import pandas as pd

from processing.repasse_ui_grid import (
    REPASSE_UI_GRID_ROW_CAP,
    repasse_ui_apply_grid_styler,
    repasse_ui_grid_display_slice,
)
from processing.repasse_contract import REPASSE_ACTION_COLUMN


def test_slice_small_no_truncation() -> None:
    df = pd.DataFrame({REPASSE_ACTION_COLUMN: ["Ok"] * 100, "x": range(100)})
    out, n, trunc = repasse_ui_grid_display_slice(df, cap=REPASSE_UI_GRID_ROW_CAP)
    assert n == 100
    assert trunc is False
    assert len(out) == 100


def test_slice_large_truncates_to_cap() -> None:
    n_rows = REPASSE_UI_GRID_ROW_CAP + 500
    df = pd.DataFrame({REPASSE_ACTION_COLUMN: ["Ok"] * n_rows, "i": range(n_rows)})
    out, n, trunc = repasse_ui_grid_display_slice(df, cap=REPASSE_UI_GRID_ROW_CAP)
    assert n == n_rows
    assert trunc is True
    assert len(out) == REPASSE_UI_GRID_ROW_CAP
    assert int(out["i"].iloc[-1]) == REPASSE_UI_GRID_ROW_CAP - 1


def test_slice_empty() -> None:
    df = pd.DataFrame()
    out, n, trunc = repasse_ui_grid_display_slice(df)
    assert out.empty and n == 0 and trunc is False


def test_styler_on_when_not_truncated() -> None:
    assert repasse_ui_apply_grid_styler(grid_truncated=False) is True


def test_styler_off_when_truncated() -> None:
    assert repasse_ui_apply_grid_styler(grid_truncated=True) is False


def test_cap_matches_export_guardrail_comment() -> None:
    """Documenta alinhamento com _max_rows_heavy_export no painel (3000)."""
    assert REPASSE_UI_GRID_ROW_CAP == 3000
