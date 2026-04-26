"""
Cálculo de imposto fiscal estimado por NF.

Combina:
- SN: alíquota efetiva do mês de emissão × valor líquido da NF
- LP: tributos por NF do motor LP (``LucroPresumidoBreakdown.tributos_por_nf``)

Usado pela tabela de NFs no painel Apuração Fiscal (visão fiscal).
"""

from __future__ import annotations

import logging
from typing import Mapping

import numpy as np
import pandas as pd

from processing.faturamento.lucro_presumido import LucroPresumidoBreakdown

logger = logging.getLogger(__name__)
_LOG_IMP_NF = logging.getLogger("imposto_por_nf")

_LP_TRIB_COLS = (
    "pis_nf",
    "cofins_nf",
    "irpj_nf",
    "csll_nf",
    "icms_interno_nf",
    "icms_interestadual_nf",
    "difal_nf",
    "fcp_nf",
)


def _situacao_invalida_fiscal(situ: pd.Series) -> pd.Series:
    s = situ.fillna("").astype(str).str.strip().str.lower()
    return s.str.contains("cancel", na=False) | s.str.contains("deneg", na=False) | s.str.contains("inutil", na=False)


def enriquecer_nfs_com_imposto_calculado(
    df_nf: pd.DataFrame,
    *,
    aliquotas_mensais_sn: Mapping[str, Mapping[str, float]],
    breakdowns_lp: Mapping[str, LucroPresumidoBreakdown],
    org_ids_lp: set[str],
) -> pd.DataFrame:
    """
    Adiciona colunas de imposto calculado ao DataFrame de NFs (cópia; não altera a entrada).

    Colunas adicionadas/atualizadas:
    - regime_nf: "SN" ou "LP"
    - mes_emissao_nf: "YYYY-MM"
    - imposto_calculavel_nf: bool
    - imposto_estimado_nf: float (NaN se não calculável ou sem dados)
    - aliquota_mensal_nf: float decimal (só SN; NaN para LP)
    - pis_nf, cofins_nf, irpj_nf, csll_nf, icms_interno_nf, icms_interestadual_nf, difal_nf, fcp_nf: float (LP; NaN SN)
    """
    if df_nf is None:
        return pd.DataFrame()
    if df_nf.empty:
        return df_nf.copy()

    out = df_nf.copy()
    # Painel NF materializado usa ``valor_faturado_nf``; motor fiscal usa ``Valor_Liquido_NF``.
    if "Valor_Liquido_NF" not in out.columns and "valor_faturado_nf" in out.columns:
        out["Valor_Liquido_NF"] = out["valor_faturado_nf"]

    if "org_id" not in out.columns:
        raise ValueError("enriquecer_nfs_com_imposto_calculado: coluna org_id ausente.")
    if "Nota_Data_Emissao" not in out.columns:
        raise ValueError("enriquecer_nfs_com_imposto_calculado: coluna Nota_Data_Emissao ausente.")
    if "Valor_Liquido_NF" not in out.columns:
        raise ValueError(
            "enriquecer_nfs_com_imposto_calculado: coluna Valor_Liquido_NF ausente "
            "(nem alias valor_faturado_nf)."
        )
    if "Nota_Numero_Normalizado" not in out.columns:
        raise ValueError("enriquecer_nfs_com_imposto_calculado: coluna Nota_Numero_Normalizado ausente.")

    oid_s = out["org_id"].astype(str).str.strip()
    org_lp_norm = {str(x).strip() for x in org_ids_lp}
    out["regime_nf"] = oid_s.map(lambda x: "LP" if x in org_lp_norm else "SN")

    emi = pd.to_datetime(out["Nota_Data_Emissao"], errors="coerce")
    out["mes_emissao_nf"] = emi.dt.strftime("%Y-%m")

    situ = out["Nota_Situacao"] if "Nota_Situacao" in out.columns else pd.Series("", index=out.index, dtype=str)
    out["imposto_calculavel_nf"] = ~_situacao_invalida_fiscal(situ)

    out["imposto_estimado_nf"] = pd.NA
    out["aliquota_mensal_nf"] = pd.NA
    for col in _LP_TRIB_COLS:
        out[col] = pd.NA

    # --- Simples Nacional ---
    sn_mask = (out["regime_nf"] == "SN") & out["imposto_calculavel_nf"]
    if bool(sn_mask.any()):
        rows: list[dict[str, object]] = []
        for oid, months in aliquotas_mensais_sn.items():
            if not isinstance(months, Mapping):
                continue
            oks = str(oid).strip()
            for mes_key, aliq in months.items():
                mk = str(mes_key).strip()
                try:
                    av = float(aliq)
                except (TypeError, ValueError):
                    continue
                rows.append({"_oid_k": oks, "mes_emissao_nf": mk, "_aliq_sn": av})
        if rows:
            aliq_df = pd.DataFrame(rows).drop_duplicates(subset=["_oid_k", "mes_emissao_nf"], keep="first")
            tmp = out.loc[sn_mask, ["org_id", "mes_emissao_nf", "Valor_Liquido_NF"]].copy()
            tmp["_oid_k"] = tmp["org_id"].astype(str).str.strip()
            tmp["_nf_orig_idx"] = tmp.index
            merged_sn = tmp.merge(
                aliq_df,
                left_on=["_oid_k", "mes_emissao_nf"],
                right_on=["_oid_k", "mes_emissao_nf"],
                how="left",
            )
            vl = pd.to_numeric(merged_sn["Valor_Liquido_NF"], errors="coerce").fillna(0.0)
            aliq_series = pd.to_numeric(merged_sn["_aliq_sn"], errors="coerce")
            for i in range(len(merged_sn)):
                orig_idx = merged_sn["_nf_orig_idx"].iloc[i]
                aliq_v = aliq_series.iloc[i]
                vlnf = float(vl.iloc[i])
                if pd.isna(aliq_v):
                    oid = str(out.at[orig_idx, "org_id"]).strip()
                    mes = str(out.at[orig_idx, "mes_emissao_nf"])
                    logger.warning(
                        "imposto_por_nf: alíquota mensal não encontrada para org_id=%s mês=%s — imposto SN 0",
                        oid,
                        mes,
                    )
                    out.at[orig_idx, "imposto_estimado_nf"] = 0.0
                    out.at[orig_idx, "aliquota_mensal_nf"] = 0.0
                else:
                    av = float(aliq_v)
                    out.at[orig_idx, "imposto_estimado_nf"] = vlnf * av
                    out.at[orig_idx, "aliquota_mensal_nf"] = av
        else:
            for orig_idx in list(out.index[sn_mask]):
                oid = str(out.at[orig_idx, "org_id"]).strip()
                mes = str(out.at[orig_idx, "mes_emissao_nf"])
                logger.warning(
                    "imposto_por_nf: nenhuma alíquota mensal SN em aliquotas_mensais_sn — org_id=%s mês=%s imposto 0",
                    oid,
                    mes,
                )
                out.at[orig_idx, "imposto_estimado_nf"] = 0.0
                out.at[orig_idx, "aliquota_mensal_nf"] = 0.0

    # --- Lucro Presumido ---
    _LOG_IMP_NF.warning(
        "imposto_por_nf merge LP: org_ids_lp=%s breakdowns_keys=%s",
        sorted(str(x) for x in org_ids_lp)[:5],
        sorted(str(k) for k in breakdowns_lp.keys())[:5] if hasattr(breakdowns_lp, "keys") else [],
    )
    trib_parts: list[pd.DataFrame] = []
    for oid_lp in org_lp_norm:
        bd = breakdowns_lp.get(oid_lp)
        if bd is None:
            _LOG_IMP_NF.warning("imposto_por_nf: oid_lp %s nao esta em breakdowns_lp", oid_lp)
            continue
        t = getattr(bd, "tributos_por_nf", None)
        if t is None or getattr(t, "empty", True):
            _LOG_IMP_NF.warning("imposto_por_nf: tributos_por_nf vazio para %s", oid_lp)
            continue
        df_trib = t
        mask_lp_oid = (
            (out["regime_nf"] == "LP")
            & (out["org_id"].astype(str).str.strip() == str(oid_lp).strip())
            & out["imposto_calculavel_nf"]
        )
        n_linhas_alvo = int(mask_lp_oid.sum())
        n_linhas_trib = len(df_trib)
        _LOG_IMP_NF.warning(
            "imposto_por_nf %s: linhas_target_no_df_nf=%d linhas_no_breakdown=%d",
            oid_lp,
            n_linhas_alvo,
            n_linhas_trib,
        )
        chaves_df_nf = (
            out.loc[mask_lp_oid, "Nota_Numero_Normalizado"].astype(str).str.strip().unique()
        )
        chaves_breakdown = df_trib["Nota_Numero_Normalizado"].astype(str).str.strip().unique()
        intersecao = set(chaves_df_nf) & set(chaves_breakdown)
        _df_nf_sample = list(chaves_df_nf[:3]) if len(chaves_df_nf) else []
        _bd_sample = list(chaves_breakdown[:3]) if len(chaves_breakdown) else []
        _LOG_IMP_NF.warning(
            "imposto_por_nf %s: chaves df_nf=%d chaves breakdown=%d intersecao=%d sample_df_nf=%s sample_breakdown=%s",
            oid_lp,
            len(chaves_df_nf),
            len(chaves_breakdown),
            len(intersecao),
            _df_nf_sample,
            _bd_sample,
        )
        tt = t.copy()
        tt["_org_lp"] = str(oid_lp).strip()
        tt["_nf_k"] = tt["Nota_Numero_Normalizado"].astype(str).str.strip()
        trib_parts.append(tt)
    if trib_parts:
        trib_all = pd.concat(trib_parts, ignore_index=True)
        trib_all = trib_all.drop_duplicates(subset=["_org_lp", "_nf_k"], keep="first")
        sel_cols = ["_org_lp", "_nf_k"] + [c for c in _LP_TRIB_COLS if c in trib_all.columns]
        if "imposto_total_nf" in trib_all.columns and "imposto_total_nf" not in sel_cols:
            sel_cols.append("imposto_total_nf")
        # Excluir colunas de identificação da NF no trib (evita sufixos _x/_y no merge).
        trib_u = trib_all[[c for c in sel_cols if c in trib_all.columns]].copy()

        tmp_lp = out.loc[out["regime_nf"] == "LP"].copy()
        if not tmp_lp.empty:
            tmp_lp = tmp_lp.drop(columns=[c for c in _LP_TRIB_COLS if c in tmp_lp.columns], errors="ignore")
            tmp_lp["_nf_orig_idx"] = tmp_lp.index
            tmp_lp["_org_s"] = tmp_lp["org_id"].astype(str).str.strip()
            tmp_lp["_nf_k"] = tmp_lp["Nota_Numero_Normalizado"].astype(str).str.strip()
            m = tmp_lp.merge(trib_u, left_on=["_org_s", "_nf_k"], right_on=["_org_lp", "_nf_k"], how="left")
            orig_ix = m["_nf_orig_idx"].to_numpy()
            for col in _LP_TRIB_COLS:
                if col in m.columns:
                    vals = pd.to_numeric(m[col], errors="coerce").to_numpy()
                    for j, oix in enumerate(orig_ix):
                        out.at[oix, col] = vals[j]
            if "imposto_total_nf" in m.columns:
                vals = pd.to_numeric(m["imposto_total_nf"], errors="coerce").to_numpy()
                for j, oix in enumerate(orig_ix):
                    out.at[oix, "imposto_estimado_nf"] = vals[j]
            for oix in orig_ix:
                out.at[oix, "aliquota_mensal_nf"] = np.nan

    return out
