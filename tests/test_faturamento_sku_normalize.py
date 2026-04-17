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

    def test_variant_suffix_separators(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("170555-1"), "170555")
        self.assertEqual(normalize_sku_join_key_scalar("170555_2"), "170555")
        self.assertEqual(normalize_sku_join_key_scalar("170555.3"), "170555")
        self.assertEqual(normalize_sku_join_key_scalar("ANP2P4.1"), "anp2p4")

    def test_variant_suffix_glued_numeric_only(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("17055501"), "170555")
        self.assertEqual(normalize_sku_join_key_scalar("1705550102"), "170555")
        # Código curto: não remove par 01 colado (evita 031601 → 0316); só lstrip de zeros
        self.assertEqual(normalize_sku_join_key_scalar("031601"), "31601")

    def test_variant_suffix_does_not_strip_alphanumeric_glued(self) -> None:
        # KIT05: últimos dois caracteres são "05" mas não é par 0[1-9] com corpo só dígitos
        self.assertEqual(normalize_sku_join_key_scalar("KIT05"), "kit05")

    def test_series(self) -> None:
        s = pd.Series(["03160", "3160", "SKU-A"])
        out = normalize_sku_key(s)
        self.assertListEqual(out.tolist(), ["3160", "3160", "sku-a"])


if __name__ == "__main__":
    unittest.main()
