"""
Rateio de comissão e frete por linha quando o export ML repete o valor **do pedido** em cada item.

Vários SKUs no mesmo «Número do pedido multiloja» costumam trazer a mesma «Taxa de Comissão» e o mesmo
«Custo de Frete» (total da venda) em cada linha. Sem rateio, o resultado subtrai N vezes o valor real.

Executar no **build** (materialização), antes de ``enrich_pedidos_com_notas``, para ``Frete_Plataforma``
derivar do frete já corrigido.
"""
from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from .normalize import normalize_pedido_join_key, to_numeric_br

COL_MULTILOJA = "Número do pedido multiloja"
COL_TC = "Taxa de Comissão"
COL_CF = "Custo de Frete"
COL_QTD = "Quantidade"
COL_PL = "Preço de lista"
FLAG_RATEIO = "Flag_ML_Comissao_Frete_Rateado"

# Tolerância (R$) para considerar comissão/frete «iguais» entre linhas do mesmo pedido multiloja.
_TOL_RS = 0.05


def _group_keys(df: pd.DataFrame) -> list[str]:
    keys: list[str] = []
    if "org_id" in df.columns:
        keys.append("org_id")
    if "empresa" in df.columns:
        keys.append("empresa")
    keys.append("_ml_key")
    return keys


def _uniform_positive_order_fee(vals: pd.Series, tol: float) -> tuple[bool, float]:
    """True se todas as linhas têm o mesmo valor finito > 0 (dentro de tol)."""
    v = pd.to_numeric(vals, errors="coerce").astype(float)
    if len(v) < 2 or not v.notna().all():
        return False, 0.0
    t0 = float(v.iloc[0])
    if not np.isfinite(t0) or t0 <= 0:
        return False, 0.0
    if float((v - t0).abs().max()) > tol:
        return False, 0.0
    return True, t0


def allocate_multiloja_order_level_fees(
    df: pd.DataFrame,
    *,
    tol_rs: float = _TOL_RS,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Reparte comissão e frete proporcionalmente a ``Quantidade * Preço de lista`` por grupo
    ``(org_id, empresa, pedido multiloja)`` quando há ≥2 linhas e os valores são idênticos linha a linha.
    """
    out = df.copy()
    meta: dict[str, Any] = {
        "ml_fee_rateio_grupos_comissao": 0,
        "ml_fee_rateio_grupos_frete": 0,
        "ml_fee_rateio_linhas_tocadas": 0,
    }

    missing = [c for c in (COL_MULTILOJA, COL_TC, COL_CF, COL_QTD, COL_PL) if c not in out.columns]
    if missing:
        meta["ml_fee_rateio_skip"] = f"missing_columns:{missing}"
        if FLAG_RATEIO not in out.columns:
            out[FLAG_RATEIO] = False
        return out, meta

    if FLAG_RATEIO not in out.columns:
        out[FLAG_RATEIO] = False
    else:
        out[FLAG_RATEIO] = out[FLAG_RATEIO].fillna(False).astype(bool)

    ml = normalize_pedido_join_key(out[COL_MULTILOJA].astype(str))
    out["_ml_key"] = ml.astype(str).str.strip()

    w = to_numeric_br(out[COL_QTD]).astype(float) * to_numeric_br(out[COL_PL]).astype(float)
    w = w.fillna(0.0).clip(lower=0.0)

    tc_num = to_numeric_br(out[COL_TC]).astype(float)
    cf_num = to_numeric_br(out[COL_CF]).astype(float)

    keys = _group_keys(out)
    touched = pd.Series(False, index=out.index)

    for _, sub in out.groupby(keys, sort=False):
        idx = sub.index
        if len(idx) < 2:
            continue
        mlk = str(sub["_ml_key"].iloc[0]).strip()
        if not mlk:
            continue

        sw = float(w.loc[idx].sum())
        if sw <= 0:
            continue

        ok_tc, total_tc = _uniform_positive_order_fee(tc_num.loc[idx], tol_rs)
        if ok_tc:
            alloc = w.loc[idx] / sw * total_tc
            out.loc[idx, COL_TC] = alloc
            meta["ml_fee_rateio_grupos_comissao"] = int(meta["ml_fee_rateio_grupos_comissao"]) + 1
            touched.loc[idx] = True

        ok_cf, total_cf = _uniform_positive_order_fee(cf_num.loc[idx], tol_rs)
        if ok_cf:
            alloc_f = w.loc[idx] / sw * total_cf
            out.loc[idx, COL_CF] = alloc_f
            meta["ml_fee_rateio_grupos_frete"] = int(meta["ml_fee_rateio_grupos_frete"]) + 1
            touched.loc[idx] = True

    out.loc[touched, FLAG_RATEIO] = True
    out = out.drop(columns=["_ml_key"], errors="ignore")

    meta["ml_fee_rateio_linhas_tocadas"] = int(touched.sum())
    return out, meta
