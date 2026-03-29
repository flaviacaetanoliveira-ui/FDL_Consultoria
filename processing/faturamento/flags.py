"""Flags de visão de faturamento e qualidade."""
from __future__ import annotations

import pandas as pd

from .config import DIVERGENCIA_VALOR_TOL
from .normalize import to_numeric_br


def _norm_txt(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip().str.casefold()


def apply_faturamento_flags(
    df: pd.DataFrame,
    *,
    permite_sem_nf: bool,
) -> pd.DataFrame:
    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    situ = _norm_txt(out["Situação"])
    nf = _norm_txt(out["Existe Nota Fiscal gerada"])

    atendido = situ == "atendido"
    com_nf = nf.eq("sim")
    # "não" / "nao" (export pode variar grafia)
    sem_nf = nf.eq("não") | nf.eq("nao")

    out["faturamento_com_nf"] = atendido & com_nf
    out["faturamento_sem_nf"] = atendido & sem_nf & permite_sem_nf
    out["faturamento_consolidado"] = out["faturamento_com_nf"] | out["faturamento_sem_nf"]

    pln = to_numeric_br(out[pl])
    vtn = to_numeric_br(out[vt])
    out["flag_preco_lista_zero"] = pln.notna() & (pln == 0)
    diff = (pln - vtn).abs()
    out["flag_divergencia_preco_lista_valor_total"] = pln.notna() & vtn.notna() & (diff > DIVERGENCIA_VALOR_TOL)
    return out
