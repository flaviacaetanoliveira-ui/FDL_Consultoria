"""Filtro «Sinal da venda (lista por NF)» — mesma regra que ``_faturamento_dre_apply_produto_e_sinal_venda``."""
from __future__ import annotations

import unittest

import pandas as pd

REPO = __import__("pathlib").Path(__file__).resolve().parent.parent


def _apply_sinal_venda(df_nf: pd.DataFrame, *, venda_sinal: str) -> pd.DataFrame:
    """Cópia da lógica em app_operacional (sem Streamlit)."""
    if df_nf.empty:
        return df_nf
    out = df_nf
    vs = str(venda_sinal or "todos").strip().lower()
    if vs not in {"", "todos"} and "valor_venda" in out.columns:
        vv = pd.to_numeric(out["valor_venda"], errors="coerce").fillna(0.0)
        if vs == "positiva":
            out = out.loc[vv > 0.0].copy()
        elif vs == "negativa":
            out = out.loc[vv < 0.0].copy()
        elif vs == "zero":
            out = out.loc[vv.eq(0.0)].copy()
    return out


class TestSinalVendaNf(unittest.TestCase):
    def test_positiva_zero_negativa_todos(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["A", "B", "C", "D"],
                "valor_venda": [100.0, 0.0, -10.0, float("nan")],
            }
        )
        self.assertEqual(len(_apply_sinal_venda(df, venda_sinal="todos")), 4)
        p = _apply_sinal_venda(df, venda_sinal="positiva")
        self.assertEqual(list(p["Nota_Numero_Normalizado"]), ["A"])
        z = _apply_sinal_venda(df, venda_sinal="zero")
        self.assertEqual(set(z["Nota_Numero_Normalizado"]), {"B", "D"})
        n = _apply_sinal_venda(df, venda_sinal="negativa")
        self.assertEqual(list(n["Nota_Numero_Normalizado"]), ["C"])

    def test_sem_coluna_valor_venda_nao_quebra(self) -> None:
        df = pd.DataFrame({"x": [1]})
        self.assertEqual(len(_apply_sinal_venda(df, venda_sinal="positiva")), 1)


if __name__ == "__main__":
    unittest.main()
