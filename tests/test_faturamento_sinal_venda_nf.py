"""Filtro produto + «Sinal do resultado (por NF)» — mesma regra que ``_faturamento_dre_apply_produto_e_sinal_venda``."""
from __future__ import annotations

import unittest

import pandas as pd


def _apply_produto_e_sinal_resultado(
    df_nf: pd.DataFrame,
    *,
    produtos_sel: tuple[str, ...] = (),
    sinal_resultado: str = "todos",
) -> pd.DataFrame:
    """Cópia da lógica em app_operacional (sem Streamlit)."""
    if df_nf.empty:
        return df_nf
    out = df_nf
    sel_p = tuple(str(x).strip() for x in produtos_sel if str(x).strip())
    if sel_p and "produto_resumo" in out.columns:
        pr = out["produto_resumo"].fillna("").astype(str).str.strip()
        out = out.loc[pr.isin(sel_p)].copy()
    sr = str(sinal_resultado or "todos").strip().lower()
    if sr in {"", "todos"}:
        return out
    if sr == "incompletas":
        if "comercial_incompleto" not in out.columns:
            return out
        inc = out["comercial_incompleto"].fillna(False).astype(bool)
        return out.loc[inc].copy()
    if "resultado" not in out.columns:
        return out
    res = pd.to_numeric(out["resultado"], errors="coerce")
    _eps = 1e-9
    if sr == "lucro":
        out = out.loc[res.notna() & (res > _eps)].copy()
    elif sr == "prejuizo":
        out = out.loc[res.notna() & (res < -_eps)].copy()
    elif sr == "zerado":
        out = out.loc[res.notna() & (res.abs() <= _eps)].copy()
    return out


class TestSinalResultadoNf(unittest.TestCase):
    def test_lucro_prejuizo_zerado_todos(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["A", "B", "C", "D", "E"],
                "resultado": [10.0, -5.0, 0.0, 1e-12, float("nan")],
                "comercial_incompleto": [False] * 5,
            }
        )
        self.assertEqual(len(_apply_produto_e_sinal_resultado(df, sinal_resultado="todos")), 5)
        luc = _apply_produto_e_sinal_resultado(df, sinal_resultado="lucro")
        self.assertEqual(set(luc["Nota_Numero_Normalizado"]), {"A"})
        prej = _apply_produto_e_sinal_resultado(df, sinal_resultado="prejuizo")
        self.assertEqual(list(prej["Nota_Numero_Normalizado"]), ["B"])
        zer = _apply_produto_e_sinal_resultado(df, sinal_resultado="zerado")
        self.assertEqual(set(zer["Nota_Numero_Normalizado"]), {"C", "D"})

    def test_incompletas(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["X", "Y"],
                "resultado": [1.0, 2.0],
                "comercial_incompleto": [True, False],
            }
        )
        inc = _apply_produto_e_sinal_resultado(df, sinal_resultado="incompletas")
        self.assertEqual(list(inc["Nota_Numero_Normalizado"]), ["X"])

    def test_sem_coluna_resultado_nao_quebra(self) -> None:
        df = pd.DataFrame({"x": [1]})
        self.assertEqual(len(_apply_produto_e_sinal_resultado(df, sinal_resultado="lucro")), 1)

    def test_incompletas_sem_coluna_retorna_sem_filtrar(self) -> None:
        df = pd.DataFrame({"Nota_Numero_Normalizado": ["A"], "resultado": [1.0]})
        self.assertEqual(len(_apply_produto_e_sinal_resultado(df, sinal_resultado="incompletas")), 1)

    def test_filtro_produto(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["N1", "N2"],
                "produto_resumo": ["P1", "P2"],
                "resultado": [1.0, 100.0],
                "comercial_incompleto": [False, False],
            }
        )
        out = _apply_produto_e_sinal_resultado(df, produtos_sel=("P2",), sinal_resultado="lucro")
        self.assertEqual(list(out["Nota_Numero_Normalizado"]), ["N2"])


if __name__ == "__main__":
    unittest.main()
