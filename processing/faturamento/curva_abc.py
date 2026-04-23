"""
Curva ABC por SKU — Resultado Gerencial.

Agrega o slice em grão linha por SKU com o mesmo rateio fiscal por receita que os KPIs.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import pandas as pd

from comercial_pedidos_analise import sku_key_series
from processing.faturamento.formatacao_display_rg import fmt_brl_ptbr_celula, fmt_pct_um_decimal
from processing.faturamento.resultado_gerencial_slice import (
    ResultadoGerencialSlice,
    _allocate_imposto_total_centavos,
    _ads_var_fix_linha_series,
    _frete_tp_linha_series,
    _receita_linha_series,
)

SKU_VAZIO_LABEL = "Não identificado"
_INTERNAL_SEM_SKU = "__fdl_sem_sku__"


@dataclass(frozen=True)
class LinhaCurvaAbc:
    sku: str
    descricao: Optional[str]
    classe_abc: str
    pedidos: int
    receita: float
    receita_display: str
    resultado_operacional: float
    resultado_liquido: float
    margem_operacional_pct: float
    margem_operacional_display: str
    margem_liquida_pct: float
    margem_liquida_display: str
    pct_da_receita: float
    pct_acumulado: float


@dataclass(frozen=True)
class CurvaAbc:
    linhas: tuple[LinhaCurvaAbc, ...]
    receita_total: float
    total_skus: int
    qtd_classe_a: int
    qtd_classe_b: int
    qtd_classe_c: int
    pct_receita_classe_a: float
    pct_receita_classe_b: float
    pct_receita_classe_c: float


def _class_from_cum(cum: float, threshold_a: float, threshold_b: float) -> str:
    if cum <= threshold_a + 1e-15:
        return "A"
    if cum <= threshold_b + 1e-15:
        return "B"
    return "C"


def compute_curva_abc(
    *,
    slice_rg: ResultadoGerencialSlice,
    kp_rg: dict[str, float | int],
    fiscal_imposto_valor: float,
    sku_descricao_map: Optional[dict[str, str]] = None,
    threshold_classe_a: float = 0.70,
    threshold_classe_b: float = 0.90,
) -> CurvaAbc:
    """
    Agrega linhas por SKU com rateio de imposto proporcional à receita (mesmo critério dos KPIs).

    Classificação ABC pela participação acumulada na receita (ordenada por receita decrescente).
    """
    df = slice_rg.df_linha
    receita_kpi = float(kp_rg["valor_venda_lista"])

    if df.empty or receita_kpi <= 1e-18:
        return CurvaAbc(
            linhas=(),
            receita_total=receita_kpi,
            total_skus=0,
            qtd_classe_a=0,
            qtd_classe_b=0,
            qtd_classe_c=0,
            pct_receita_classe_a=0.0,
            pct_receita_classe_b=0.0,
            pct_receita_classe_c=0.0,
        )

    rec = _receita_linha_series(df)
    com = pd.to_numeric(df["Taxa de Comissão"], errors="coerce").fillna(0.0)
    fp = pd.to_numeric(df["Frete_Plataforma"], errors="coerce").fillna(0.0)
    cmv = pd.to_numeric(df["Custo_Produto_Total"], errors="coerce").fillna(0.0)
    ftp = _frete_tp_linha_series(df)
    desp = (
        pd.to_numeric(df["Despesas Fixas"], errors="coerce").fillna(0.0)
        if "Despesas Fixas" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    ads_v, ads_f = _ads_var_fix_linha_series(df)
    sku_k = sku_key_series(df)
    sku_key_col = sku_k.mask(sku_k.eq(""), _INTERNAL_SEM_SKU)
    sku_disp = sku_k.mask(sku_k.eq(""), SKU_VAZIO_LABEL)
    pids = slice_rg.pedido_ids.astype(str).str.strip()

    work = pd.DataFrame(
        {
            "_sku_key": sku_key_col.values,
            "_sku_disp": sku_disp.values,
            "_rec": rec.values,
            "_com": com.values,
            "_fp": fp.values,
            "_cmv": cmv.values,
            "_ftp": ftp.values,
            "_desp": desp.values,
            "_ads_v": ads_v.values,
            "_ads_f": ads_f.values,
            "_pid": pids.values,
        },
        index=df.index,
    )

    grp = work.groupby("_sku_key", sort=False)
    sums = grp[
        ["_rec", "_com", "_fp", "_cmv", "_ftp", "_desp", "_ads_v", "_ads_f"]
    ].sum()
    disp_one = grp["_sku_disp"].first()
    pcount = grp["_pid"].apply(lambda s: s[s.ne("")].nunique())

    receita_por = {str(k): float(sums.loc[k, "_rec"]) for k in sums.index}
    keys_sorted = sorted(receita_por.keys())
    imp_por = _allocate_imposto_total_centavos(keys_sorted, receita_por, float(fiscal_imposto_valor))

    rows_tmp: list[dict[str, object]] = []
    for sku_key_s in keys_sorted:
        r = float(receita_por[sku_key_s])
        if r <= 1e-18:
            continue
        c = float(sums.loc[sku_key_s, "_com"])
        fp_ = float(sums.loc[sku_key_s, "_fp"])
        cmv_ = float(sums.loc[sku_key_s, "_cmv"])
        ftp_ = float(sums.loc[sku_key_s, "_ftp"])
        desp_ = float(sums.loc[sku_key_s, "_desp"])
        ads_v_ = float(sums.loc[sku_key_s, "_ads_v"])
        ads_f_ = float(sums.loc[sku_key_s, "_ads_f"])
        imp_ = float(imp_por.get(sku_key_s, 0.0))
        ro = r - c - fp_ - cmv_ - ftp_ - imp_ - ads_v_
        rl = r - c - fp_ - cmv_ - ftp_ - imp_ - desp_ - ads_v_ - ads_f_
        mop = (ro / r * 100.0) if r > 1e-12 else 0.0
        mlq = (rl / r * 100.0) if r > 1e-12 else 0.0
        lab = str(disp_one.loc[sku_key_s])
        ped_n = int(pcount.loc[sku_key_s]) if sku_key_s in pcount.index else 0
        rows_tmp.append(
            {
                "sku_disp": lab,
                "pedidos": max(ped_n, 0),
                "rec": r,
                "rop": ro,
                "rliq": rl,
                "m_op": mop,
                "m_liq": mlq,
                "pct": 0.0,
                "pct_cum": 0.0,
                "classe": "C",
            }
        )

    if not rows_tmp:
        return CurvaAbc(
            linhas=(),
            receita_total=receita_kpi,
            total_skus=0,
            qtd_classe_a=0,
            qtd_classe_b=0,
            qtd_classe_c=0,
            pct_receita_classe_a=0.0,
            pct_receita_classe_b=0.0,
            pct_receita_classe_c=0.0,
        )

    rows_tmp.sort(key=lambda x: float(x["rec"]), reverse=True)

    denom = receita_kpi if receita_kpi > 1e-18 else sum(float(x["rec"]) for x in rows_tmp)
    gross = [float(x["rec"]) / denom for x in rows_tmp]
    drift = 1.0 - sum(gross)
    adj = list(gross)
    if adj and abs(drift) > 1e-12:
        i_max = max(range(len(adj)), key=lambda i: adj[i])
        adj[i_max] = max(0.0, adj[i_max] + drift)

    cum = 0.0
    linhas_final: list[LinhaCurvaAbc] = []
    map_use = sku_descricao_map or {}
    for row, p_share in zip(rows_tmp, adj, strict=True):
        cum += float(p_share)
        classe = _class_from_cum(cum, threshold_classe_a, threshold_classe_b)
        sku_lab = str(row["sku_disp"])
        desc = map_use.get(sku_lab) if map_use else None
        rop = float(row["rop"])
        rliq = float(row["rliq"])
        rec_v = float(row["rec"])
        mop = float(row["m_op"])
        mlq = float(row["m_liq"])
        linhas_final.append(
            LinhaCurvaAbc(
                sku=sku_lab,
                descricao=desc,
                classe_abc=classe,
                pedidos=int(row["pedidos"]),
                receita=rec_v,
                receita_display=fmt_brl_ptbr_celula(rec_v),
                resultado_operacional=rop,
                resultado_liquido=rliq,
                margem_operacional_pct=mop,
                margem_operacional_display=fmt_pct_um_decimal(mop),
                margem_liquida_pct=mlq,
                margem_liquida_display=fmt_pct_um_decimal(mlq),
                pct_da_receita=float(p_share),
                pct_acumulado=float(cum),
            )
        )

    linhas_t = tuple(linhas_final)
    n_a = sum(1 for x in linhas_t if x.classe_abc == "A")
    n_b = sum(1 for x in linhas_t if x.classe_abc == "B")
    n_c = sum(1 for x in linhas_t if x.classe_abc == "C")
    pa = sum(x.pct_da_receita for x in linhas_t if x.classe_abc == "A")
    pb = sum(x.pct_da_receita for x in linhas_t if x.classe_abc == "B")
    pc = sum(x.pct_da_receita for x in linhas_t if x.classe_abc == "C")

    return CurvaAbc(
        linhas=linhas_t,
        receita_total=receita_kpi,
        total_skus=len(linhas_t),
        qtd_classe_a=n_a,
        qtd_classe_b=n_b,
        qtd_classe_c=n_c,
        pct_receita_classe_a=float(pa),
        pct_receita_classe_b=float(pb),
        pct_receita_classe_c=float(pc),
    )
