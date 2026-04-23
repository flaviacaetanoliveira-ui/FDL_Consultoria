"""Filtro produto + resultado (lucro e/ou prejuízo) — mesma regra que ``_faturamento_dre_apply_produto_e_sinal_venda``."""
from __future__ import annotations

import unittest

import pandas as pd


def _apply_produto_e_sinal_resultado(
    df_nf: pd.DataFrame,
    *,
    produtos_sel: tuple[str, ...] = (),
    sinais_resultado: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Cópia da lógica em app_operacional (sem Streamlit)."""
    if df_nf.empty:
        return df_nf
    out = df_nf
    sel_p = tuple(str(x).strip() for x in produtos_sel if str(x).strip())
    if sel_p and "produto_resumo" in out.columns:
        pr = out["produto_resumo"].fillna("").astype(str).str.strip()
        out = out.loc[pr.isin(sel_p)].copy()
    raw = [str(x).strip().lower() for x in sinais_resultado if str(x).strip()]
    sel = {x for x in raw if x in {"lucro", "prejuizo", "empate"}}
    if not sel:
        return out
    if "resultado" not in out.columns:
        return out
    if "lucro" in sel and "prejuizo" in sel:
        return out
    res = pd.to_numeric(out["resultado"], errors="coerce")
    _eps = 1e-9
    mask = pd.Series(False, index=out.index)
    if "lucro" in sel:
        mask |= res.notna() & (res > _eps)
    if "prejuizo" in sel:
        mask |= res.notna() & (res < -_eps)
    if "empate" in sel:
        mask |= res.notna() & (res >= -_eps) & (res <= _eps)
    return out.loc[mask].copy()


class TestSinalResultadoNf(unittest.TestCase):
    def test_lucro_e_prejuizo_uniao(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["A", "B", "C", "D", "E"],
                "resultado": [10.0, -5.0, 0.0, 1e-12, float("nan")],
            }
        )
        both = _apply_produto_e_sinal_resultado(df, sinais_resultado=("lucro", "prejuizo"))
        self.assertEqual(set(both["Nota_Numero_Normalizado"]), {"A", "B", "C", "D", "E"})
        both3 = _apply_produto_e_sinal_resultado(df, sinais_resultado=("lucro", "prejuizo", "empate"))
        self.assertEqual(set(both3["Nota_Numero_Normalizado"]), {"A", "B", "C", "D", "E"})
        luc = _apply_produto_e_sinal_resultado(df, sinais_resultado=("lucro",))
        self.assertEqual(set(luc["Nota_Numero_Normalizado"]), {"A"})
        prej = _apply_produto_e_sinal_resultado(df, sinais_resultado=("prejuizo",))
        self.assertEqual(set(prej["Nota_Numero_Normalizado"]), {"B"})
        emp = _apply_produto_e_sinal_resultado(df, sinais_resultado=("empate",))
        self.assertEqual(set(emp["Nota_Numero_Normalizado"]), {"C", "D"})
        luc_emp = _apply_produto_e_sinal_resultado(df, sinais_resultado=("lucro", "empate"))
        self.assertEqual(set(luc_emp["Nota_Numero_Normalizado"]), {"A", "C", "D"})

    def test_sinais_vazio_sem_filtro_por_sinal(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["A", "B"],
                "resultado": [1.0, -1.0],
            }
        )
        out = _apply_produto_e_sinal_resultado(df, sinais_resultado=())
        self.assertEqual(set(out["Nota_Numero_Normalizado"]), {"A", "B"})
        out_default = _apply_produto_e_sinal_resultado(df)
        self.assertEqual(set(out_default["Nota_Numero_Normalizado"]), {"A", "B"})

    def test_sem_coluna_resultado_nao_quebra(self) -> None:
        df = pd.DataFrame({"x": [1]})
        self.assertEqual(len(_apply_produto_e_sinal_resultado(df, sinais_resultado=("lucro",))), 1)

    def test_filtro_produto(self) -> None:
        df = pd.DataFrame(
            {
                "Nota_Numero_Normalizado": ["N1", "N2"],
                "produto_resumo": ["P1", "P2"],
                "resultado": [1.0, 100.0],
            }
        )
        out = _apply_produto_e_sinal_resultado(df, produtos_sel=("P2",), sinais_resultado=("lucro",))
        self.assertEqual(list(out["Nota_Numero_Normalizado"]), ["N2"])


if __name__ == "__main__":
    unittest.main()
