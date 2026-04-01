"""Flags de visão de faturamento e qualidade."""
from __future__ import annotations

from typing import Union

import pandas as pd

from .config import DIVERGENCIA_VALOR_TOL
from .normalize import to_numeric_br


def _norm_txt(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.casefold()


def apply_faturamento_flags(
    df: pd.DataFrame,
    *,
    permite_sem_nf: Union[bool, pd.Series],
) -> pd.DataFrame:
    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    situ = _norm_txt(out["Situação"])
    nf = _norm_txt(out["Existe Nota Fiscal gerada"])

    atendido = situ == "atendido"
    com_nf = nf.eq("sim")
    sem_nf = nf.eq("não") | nf.eq("nao")

    if isinstance(permite_sem_nf, bool):
        mask_perm = pd.Series(permite_sem_nf, index=out.index, dtype=bool)
    else:
        mask_perm = permite_sem_nf.reindex(out.index).fillna(False).astype(bool)

    out["faturamento_com_nf"] = atendido & com_nf
    out["faturamento_sem_nf"] = atendido & sem_nf & mask_perm
    out["faturamento_consolidado"] = out["faturamento_com_nf"] | out["faturamento_sem_nf"]

    pln = to_numeric_br(out[pl])
    vtn = to_numeric_br(out[vt])
    out["flag_preco_lista_zero"] = pln.notna() & (pln == 0)

    if "Receita_Bruta" in out.columns:
        rbn = to_numeric_br(out["Receita_Bruta"])
        desc_col = "Desconto proporcional total"
        base_ok = rbn.notna() & vtn.notna()
        if desc_col in out.columns:
            dcn = to_numeric_br(out[desc_col])
            residual = (rbn - dcn - vtn).abs()
            out["flag_divergencia_preco_lista_valor_total"] = base_ok & (
                dcn.notna() & (residual > DIVERGENCIA_VALOR_TOL)
                | dcn.isna() & ((rbn - vtn).abs() > DIVERGENCIA_VALOR_TOL)
            )
        else:
            out["flag_divergencia_preco_lista_valor_total"] = base_ok & (
                (rbn - vtn).abs() > DIVERGENCIA_VALOR_TOL
            )
    else:
        diff = (pln - vtn).abs()
        out["flag_divergencia_preco_lista_valor_total"] = (
            pln.notna() & vtn.notna() & (diff > DIVERGENCIA_VALOR_TOL)
        )

    if "Base_Imposto" in out.columns:
        bi = to_numeric_br(out["Base_Imposto"])
        out["Flag_Base_Imposto_Ausente"] = bi.isna()
    else:
        out["Flag_Base_Imposto_Ausente"] = True

    if "faturamento_consolidado" in out.columns:
        fc = out["faturamento_consolidado"].fillna(False).astype(bool)
    else:
        fc = pd.Series(False, index=out.index)
    out["_ab_sem_nf_np"] = atendido & sem_nf & ~fc
    return out
