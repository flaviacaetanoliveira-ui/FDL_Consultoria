"""Normalização de chave SKU para join pedidos ↔ custo."""
from __future__ import annotations

import unittest

import pandas as pd

from processing.faturamento.normalize import (
    is_sku_assistencia,
    normalize_sku_join_key_scalar,
    normalize_sku_key,
)


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

    def test_conjunto_kit_trailing_digits(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("CONJBANP2"), "conjbanp")
        self.assertEqual(normalize_sku_join_key_scalar("conjbanp1"), "conjbanp")
        self.assertEqual(normalize_sku_join_key_scalar("KITJL3"), "kitjl")
        self.assertEqual(normalize_sku_join_key_scalar("COZBERGAMOS2"), "cozbergamo")
        self.assertEqual(normalize_sku_join_key_scalar("COZBERGAMO2"), "cozbergamo")
        self.assertEqual(normalize_sku_join_key_scalar("cozbergamos1"), "cozbergamo")
        self.assertEqual(normalize_sku_join_key_scalar("COZMADEIRA2"), "cozmadeira")
        self.assertEqual(normalize_sku_join_key_scalar("CONJKATE1"), "conjkate")
        self.assertEqual(normalize_sku_join_key_scalar("CONJRB2"), "conjrb")
        # Não reduzir a só o prefixo (evita kit50 → kit)
        self.assertEqual(normalize_sku_join_key_scalar("KIT50"), "kit50")
        self.assertEqual(normalize_sku_join_key_scalar("CONJ1"), "conj1")
        # Numéricos e outros alfanuméricos
        self.assertEqual(normalize_sku_join_key_scalar("170555"), "170555")
        self.assertEqual(normalize_sku_join_key_scalar("BELA4P1"), "bela4p1")

    def test_prefix_f_sku_normalize(self) -> None:
        self.assertEqual(normalize_sku_join_key_scalar("6513"), "6513")
        self.assertEqual(normalize_sku_join_key_scalar("F6513"), "f6513")
        self.assertEqual(normalize_sku_join_key_scalar("1642"), "1642")
        self.assertEqual(normalize_sku_join_key_scalar("f1642"), "f1642")

    def test_is_sku_assistencia(self) -> None:
        self.assertTrue(is_sku_assistencia("ai3p1"))
        self.assertTrue(is_sku_assistencia("INT7"))
        self.assertTrue(is_sku_assistencia("b3p4"))
        self.assertTrue(is_sku_assistencia("w8"))
        self.assertTrue(is_sku_assistencia("ptk11"))
        self.assertTrue(is_sku_assistencia("a3pe01"))
        self.assertTrue(is_sku_assistencia("ra04"))
        self.assertTrue(is_sku_assistencia("brs11"))
        self.assertTrue(is_sku_assistencia("aipb1"))
        self.assertTrue(is_sku_assistencia("pnp9"))
        self.assertTrue(is_sku_assistencia("bnp13"))
        self.assertTrue(is_sku_assistencia("ln2p8"))
        self.assertTrue(is_sku_assistencia("ln7"))
        self.assertTrue(is_sku_assistencia("crs18"))
        self.assertTrue(is_sku_assistencia("bcp1"))
        self.assertTrue(is_sku_assistencia("bcs15"))
        self.assertTrue(is_sku_assistencia("pc3"))
        self.assertTrue(is_sku_assistencia("p7"))
        self.assertTrue(is_sku_assistencia("rcc4p28"))
        self.assertFalse(is_sku_assistencia("170555"))
        self.assertFalse(is_sku_assistencia("CONJBANP"))
        self.assertFalse(is_sku_assistencia("BELA4P1"))
        self.assertFalse(is_sku_assistencia(""))

    def test_series(self) -> None:
        s = pd.Series(["03160", "3160", "SKU-A"])
        out = normalize_sku_key(s)
        self.assertListEqual(out.tolist(), ["3160", "3160", "sku-a"])


if __name__ == "__main__":
    unittest.main()
