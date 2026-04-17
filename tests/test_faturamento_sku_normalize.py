"""Normalização de chave SKU para join pedidos ↔ custo."""
from __future__ import annotations

import unittest

import pandas as pd

from processing.faturamento.normalize import normalize_sku_join_key_scalar, normalize_sku_key


class TestSkuJoinNormalize(unittest.TestCase):
    def test_leading_zeros(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("03160"), "3160")
        self.assertEqual(normalize_sku_join_key_scalar("02640"), "2640")
        self.assertEqual(normalize_sku_join_key_scalar("01696"), "1696")

    def test_excel_dot_zero(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("3160.0"), "3160")
        self.assertEqual(normalize_sku_join_key_scalar("03160.0"), "3160")

    def test_pure_zero(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("0"), "0")
        self.assertEqual(normalize_sku_join_key_scalar("000"), "0")

    def test_alphanumeric_casefold(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("SKU-A"), "sku-a")
        self.assertEqual(normalize_sku_join_key_scalar("  SKU-B  "), "sku-b")
        self.assertEqual(normalize_sku_join_key_scalar("Bela4P1"), "bela4p1")
        self.assertEqual(normalize_sku_join_key_scalar("BELA4P1"), "bela4p1")

    def test_series(self) -> None:
        s = pd.Series(["03160", "3160", "SKU-A"])
        out = normalize_sku_key(s)
        self.assertListEqual(out.tolist(), ["3160", "3160", "sku-a"])


if __name__ == "__main__":
    unittest.main()
