"""Chaves de merge fiscal ↔ comercial (NF + empresa).

O merge em si vive em ``app_operacional`` (import pesado); aqui testam-se só as normalizações
usadas em ``_merge_fiscal_base_with_commercial_nf``.
"""

from __future__ import annotations

from processing.faturamento.normalize import (
    normalize_empresa_fiscal_commercial_join_key_scalar,
    normalize_nf_fiscal_commercial_join_key_scalar,
)


def test_nf_merge_key_strips_leading_zeros_when_all_digits() -> None:
    assert normalize_nf_fiscal_commercial_join_key_scalar("042517") == "42517"
    assert normalize_nf_fiscal_commercial_join_key_scalar("42517") == "42517"
    assert normalize_nf_fiscal_commercial_join_key_scalar("42517.0") == "42517"


def test_nf_merge_key_preserves_alphanumeric() -> None:
    assert normalize_nf_fiscal_commercial_join_key_scalar("NF-042517") == "NF-042517"


def test_empresa_merge_key_casefold() -> None:
    assert normalize_empresa_fiscal_commercial_join_key_scalar("Wood") == normalize_empresa_fiscal_commercial_join_key_scalar(
        "WOOD"
    )
