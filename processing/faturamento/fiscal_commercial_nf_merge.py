"""
Merge base fiscal (1 linha por NF) com grão comercial NF-first.

A chave ``normalize_nf_fiscal_commercial_join_key`` alinha «042480» e «42480» (zeros à esquerda) e
«NF042480» com «042480» quando o sufixo é só dígitos.
Quando ainda assim não casa, o caso típico é **org_id** vazio no materializado comercial e preenchido
no fiscal (ou o inverso): o merge estrito em (org_id, empresa, NF) falha. O fallback usa só
(empresa, NF) nas linhas comerciais sem org_id.

**Receita de frete (painel):** ``receita_frete_tp`` vem **somente** de ``Frete_Nota_Export`` (frete na NF).
**Custo frete plataforma** e **repasse transportadora própria** vêm do comercial (split ``Frete_Plataforma`` / ME vs TP
em ``Custo de Frete``). **tarifa_custo_envio** continua sendo a soma total do frete no pedido (CF ou ``Frete_Plataforma``).
"""

from __future__ import annotations

import pandas as pd

from processing.faturamento.normalize import (
    normalize_empresa_fiscal_commercial_join_key,
    normalize_nf_fiscal_commercial_join_key,
)


def merge_fiscal_base_with_commercial_nf_dataframe(
    df_fiscal: pd.DataFrame,
    df_commercial: pd.DataFrame,
    *,
    strict_org_only: bool = False,
) -> pd.DataFrame:
    """
    Uma linha por NF do recorte fiscal; colunas comerciais quando existir vínculo.

    ``strict_org_only=True``: só merge por (org_id, empresa, NF) — para auditoria vs fallback
    por ``org_id`` vazio no comercial.

    Igual ao contrato de ``app_operacional._merge_fiscal_base_with_commercial_nf`` (com
    ``strict_org_only=False``, o padrão).
    """
    cols_out = [
        "org_id",
        "Nota_Numero_Normalizado",
        "Nota_Data_Emissao",
        "Nota_Situacao",
        "empresa",
        "valor_faturado_nf",
        "valor_venda",
        "diferenca",
        "comissao",
        "custo_produto",
        "receita_frete_tp",
        "custo_frete_plataforma",
        "repasse_frete_transportadora_propria",
        "tarifa_custo_envio",
        "imposto",
        "despesa_fixa",
        "resultado",
        "plataforma_resumo",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
        "faturamento_nota_vinculada",
        "comercial_incompleto",
    ]
    if df_fiscal.empty:
        return pd.DataFrame(columns=cols_out)

    fc = df_fiscal.copy()
    fc["_jo"] = (
        fc["org_id"].fillna("").astype(str).str.strip() if "org_id" in fc.columns else ""
    )
    if "org_id" not in fc.columns:
        fc["_jo"] = ""
    fc["_je"] = fc["empresa"].fillna("").astype(str).str.strip()
    fc["_jn"] = fc["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    fc["_je_m"] = normalize_empresa_fiscal_commercial_join_key(fc["_je"])
    fc["_jn_m"] = normalize_nf_fiscal_commercial_join_key(fc["_jn"])

    if df_commercial.empty:
        merged = fc
        merged["valor_venda"] = 0.0
        merged["comissao"] = 0.0
        merged["custo_produto"] = 0.0
        merged["receita_frete_tp"] = 0.0
        merged["custo_frete_plataforma"] = 0.0
        merged["repasse_frete_transportadora_propria"] = 0.0
        merged["tarifa_custo_envio"] = 0.0
        merged["imposto"] = 0.0
        merged["despesa_fixa"] = 0.0
        merged["resultado"] = 0.0
        merged["plataforma_resumo"] = "—"
        merged["pedido_resumo"] = "—"
        merged["n_linhas_pedido"] = 0
        merged["produto_resumo"] = "—"
        merged["faturamento_nota_vinculada"] = False
        merged["comercial_incompleto"] = False
    else:
        co = df_commercial.copy()
        co["_jo"] = (
            co["org_id"].fillna("").astype(str).str.strip() if "org_id" in co.columns else ""
        )
        if "org_id" not in co.columns:
            co["_jo"] = ""
        co["_je"] = co["empresa"].fillna("").astype(str).str.strip()
        co["_jn"] = co["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
        co["_je_m"] = normalize_empresa_fiscal_commercial_join_key(co["_je"])
        co["_jn_m"] = normalize_nf_fiscal_commercial_join_key(co["_jn"])
        if "plataforma_resumo" not in co.columns and "plataforma" in co.columns:
            co["plataforma_resumo"] = co["plataforma"].astype(str)
        elif "plataforma_resumo" not in co.columns:
            co["plataforma_resumo"] = "—"
        take = [
            "_jo",
            "_je_m",
            "_jn_m",
            "valor_venda",
            "comissao",
            "custo_produto",
            "receita_frete_tp",
            "custo_frete_plataforma",
            "repasse_frete_transportadora_propria",
            "tarifa_custo_envio",
            "imposto",
            "despesa_fixa",
            "resultado",
            "plataforma_resumo",
            "pedido_resumo",
            "n_linhas_pedido",
            "produto_resumo",
            "faturamento_nota_vinculada",
            "comercial_incompleto",
        ]
        use = [c for c in take if c in co.columns]
        fill_commercial = [
            "valor_venda",
            "comissao",
            "custo_produto",
            "receita_frete_tp",
            "custo_frete_plataforma",
            "repasse_frete_transportadora_propria",
            "tarifa_custo_envio",
            "imposto",
            "despesa_fixa",
            "resultado",
            "plataforma_resumo",
            "pedido_resumo",
            "n_linhas_pedido",
            "produto_resumo",
            "faturamento_nota_vinculada",
            "comercial_incompleto",
        ]

        co_n = co.loc[co["_jo"].ne(""), use].drop_duplicates(
            subset=["_jo", "_je_m", "_jn_m"], keep="first"
        )
        co_e = co.loc[co["_jo"].eq(""), use].drop_duplicates(subset=["_je_m", "_jn_m"], keep="first")

        if co_n.empty:
            if co_e.empty or strict_org_only:
                merged = fc.copy()
                for _c in fill_commercial:
                    merged[_c] = pd.NA
            else:
                co_e2 = co_e.drop(columns=["_jo"], errors="ignore")
                merged = fc.merge(co_e2, on=["_je_m", "_jn_m"], how="left")
        else:
            merged = fc.merge(co_n, on=["_jo", "_je_m", "_jn_m"], how="left", indicator="__mf")
            unmatched = merged["__mf"].eq("left_only")
            merged = merged.drop(columns=["__mf"])
            if unmatched.any() and not co_e.empty and not strict_org_only:
                co_e2 = co_e.drop(columns=["_jo"], errors="ignore")
                fill = (
                    merged.loc[unmatched, ["_je_m", "_jn_m"]]
                    .reset_index()
                    .merge(co_e2, on=["_je_m", "_jn_m"], how="left")
                )
                orig = fill["index"].to_numpy()
                for c in fill_commercial:
                    if c in fill.columns:
                        merged.loc[orig, c] = fill[c].to_numpy()

        for _c in (
            "valor_venda",
            "comissao",
            "custo_produto",
            "receita_frete_tp",
            "custo_frete_plataforma",
            "repasse_frete_transportadora_propria",
            "tarifa_custo_envio",
            "imposto",
            "despesa_fixa",
            "resultado",
            "plataforma_resumo",
            "pedido_resumo",
            "n_linhas_pedido",
            "produto_resumo",
            "faturamento_nota_vinculada",
            "comercial_incompleto",
        ):
            if _c not in merged.columns:
                if _c == "n_linhas_pedido":
                    merged[_c] = 0
                elif _c == "faturamento_nota_vinculada":
                    merged[_c] = False
                elif _c == "comercial_incompleto":
                    merged[_c] = False
                elif _c == "plataforma_resumo":
                    merged[_c] = "—"
                elif _c in {"pedido_resumo", "produto_resumo"}:
                    merged[_c] = "—"
                else:
                    merged[_c] = 0.0
        merged["comercial_incompleto"] = (
            merged["comercial_incompleto"].astype("boolean").fillna(False).astype(bool)
        )
        merged["valor_venda"] = pd.to_numeric(merged["valor_venda"], errors="coerce").fillna(0.0)
        merged["comissao"] = pd.to_numeric(merged["comissao"], errors="coerce").fillna(0.0)
        merged["custo_produto"] = pd.to_numeric(merged["custo_produto"], errors="coerce").fillna(0.0)
        merged["receita_frete_tp"] = pd.to_numeric(merged["receita_frete_tp"], errors="coerce").fillna(0.0)
        merged["custo_frete_plataforma"] = pd.to_numeric(
            merged["custo_frete_plataforma"], errors="coerce"
        ).fillna(0.0)
        merged["repasse_frete_transportadora_propria"] = pd.to_numeric(
            merged["repasse_frete_transportadora_propria"], errors="coerce"
        ).fillna(0.0)
        merged["tarifa_custo_envio"] = pd.to_numeric(merged["tarifa_custo_envio"], errors="coerce").fillna(0.0)
        merged["imposto"] = pd.to_numeric(merged["imposto"], errors="coerce").fillna(0.0)
        merged["despesa_fixa"] = pd.to_numeric(merged["despesa_fixa"], errors="coerce").fillna(0.0)
        rnum = pd.to_numeric(merged["resultado"], errors="coerce")
        m_inc = merged["comercial_incompleto"].astype(bool)
        merged["resultado"] = rnum
        merged.loc[~m_inc, "resultado"] = merged.loc[~m_inc, "resultado"].fillna(0.0)
        merged["n_linhas_pedido"] = (
            pd.to_numeric(merged["n_linhas_pedido"], errors="coerce").fillna(0).astype(int)
        )
        merged["plataforma_resumo"] = merged["plataforma_resumo"].fillna("—").astype(str)
        merged["pedido_resumo"] = merged["pedido_resumo"].fillna("—")
        merged["produto_resumo"] = merged["produto_resumo"].fillna("—")
        merged["faturamento_nota_vinculada"] = (
            merged["faturamento_nota_vinculada"].astype("boolean").fillna(False).astype(bool)
        )

    if "Frete_Nota_Export" in merged.columns:
        merged["receita_frete_tp"] = (
            pd.to_numeric(merged["Frete_Nota_Export"], errors="coerce").fillna(0.0)
        )
    else:
        merged["receita_frete_tp"] = pd.to_numeric(merged["receita_frete_tp"], errors="coerce").fillna(0.0)
    merged["custo_frete_plataforma"] = pd.to_numeric(
        merged["custo_frete_plataforma"], errors="coerce"
    ).fillna(0.0)
    merged["repasse_frete_transportadora_propria"] = pd.to_numeric(
        merged["repasse_frete_transportadora_propria"], errors="coerce"
    ).fillna(0.0)
    merged["tarifa_custo_envio"] = pd.to_numeric(merged["tarifa_custo_envio"], errors="coerce").fillna(0.0)

    # Parquet comercial antigo (sem split): não perder o total de frete na DRE — atribui a tarifa à plataforma.
    _eps_fb = 1e-9
    tcf = merged["tarifa_custo_envio"]
    m_fb = (
        merged["custo_frete_plataforma"].abs() <= _eps_fb
    ) & (merged["repasse_frete_transportadora_propria"].abs() <= _eps_fb) & (tcf > _eps_fb)
    if m_fb.any():
        merged.loc[m_fb, "custo_frete_plataforma"] = tcf.loc[m_fb].astype(float)

    v_fat = pd.to_numeric(merged["Valor_Liquido_NF"], errors="coerce").fillna(0.0)
    vv = pd.to_numeric(merged["valor_venda"], errors="coerce").fillna(0.0)
    org_raw = merged["org_id"] if "org_id" in merged.columns else merged["_jo"]
    org_s = org_raw.fillna("").map(lambda v: str(v).strip())
    org_s = org_s.mask(org_s.str.lower().isin({"nan", "none", "nat", "<na>", "null"}), "")
    if "Nota_Situacao" in merged.columns:
        sit_raw = merged["Nota_Situacao"].fillna("").astype(str).str.strip()
        sit_s = sit_raw.mask(sit_raw.eq(""), "—")
    else:
        sit_s = pd.Series("—", index=merged.index, dtype=str)
    fv = merged["faturamento_nota_vinculada"].fillna(False)
    try:
        fv = fv.astype(bool)
    except (TypeError, ValueError):
        fv = fv.ne(0) if fv.dtype != bool else fv
    out = pd.DataFrame(
        {
            "org_id": org_s.astype(str),
            "Nota_Numero_Normalizado": merged["_jn"].astype(str),
            "Nota_Data_Emissao": merged["Nota_Data_Emissao"],
            "Nota_Situacao": sit_s,
            "empresa": merged["_je"].astype(str),
            "valor_faturado_nf": v_fat,
            "valor_venda": vv,
            "diferenca": vv - v_fat,
            "comissao": merged["comissao"],
            "custo_produto": merged["custo_produto"],
            "receita_frete_tp": merged["receita_frete_tp"],
            "custo_frete_plataforma": merged["custo_frete_plataforma"],
            "repasse_frete_transportadora_propria": merged["repasse_frete_transportadora_propria"],
            "tarifa_custo_envio": merged["tarifa_custo_envio"],
            "imposto": merged["imposto"],
            "despesa_fixa": merged["despesa_fixa"],
            "resultado": merged["resultado"],
            "plataforma_resumo": merged["plataforma_resumo"].astype(str),
            "pedido_resumo": merged["pedido_resumo"],
            "n_linhas_pedido": merged["n_linhas_pedido"].astype(int),
            "produto_resumo": merged["produto_resumo"],
            "faturamento_nota_vinculada": fv,
            "comercial_incompleto": merged["comercial_incompleto"].astype(bool),
        }
    )
    return out[cols_out]
