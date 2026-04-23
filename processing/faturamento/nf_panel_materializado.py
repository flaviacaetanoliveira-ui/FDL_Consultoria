"""
Painel NF-first **pré-calculado** (merge fiscal↔comercial + gap receita só sem fiscal + resultado frete + ADS).

Com fiscal válido, **receita de frete** vem de ``Frete_Nota_Export`` no merge; o gap NF×lista **não** redefine essa
receita. Sem fiscal, mantém-se o fallback de gap no painel (mesmo universo que o grão comercial).

Inclui custo de **ADS** (3,5% sobre ``valor_venda`` + R$ 2 por NF com venda lista > 0), gravado em colunas
``custo_ads_*`` e descontado de ``resultado``. Gravado em ``dataset_faturamento_nf_panel.parquet``.
"""

from __future__ import annotations

import pandas as pd

from faturamento_dre_recorte_minimo import (
    apply_nf_panel_custo_ads,
    apply_nf_panel_frete_gap_fallback,
    apply_nf_panel_frete_repasse_e_plataforma_coerencia,
    apply_nf_panel_resultado_frete_nota_lista,
)

from .fiscal_commercial_nf_merge import merge_fiscal_base_with_commercial_nf_dataframe
from .fiscal_materializado import fiscal_contract_dataframe_valid


NF_PANEL_PARQUET_FILENAME = "dataset_faturamento_nf_panel.parquet"

# Colunas do merge + coluna ``plataforma`` (filtros da UI) — sem schema_version_nf.
NF_PANEL_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {
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
        "custo_ads_variavel",
        "custo_ads_fixo",
        "custo_ads",
        "resultado",
        "plataforma_resumo",
        "plataforma",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
        "faturamento_nota_vinculada",
        "comercial_incompleto",
    }
)


def nf_panel_materializado_dataframe_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    return NF_PANEL_REQUIRED_COLUMNS.issubset(df.columns)


def _commercial_nf_to_panel_shape(df_nf: pd.DataFrame) -> pd.DataFrame:
    """Mesmo formato que ``merge_fiscal_base_with_commercial_nf_dataframe`` devolve, só com dados comerciais."""
    out = df_nf.drop(columns=["schema_version_nf"], errors="ignore").copy()
    if "plataforma_resumo" not in out.columns and "plataforma" in out.columns:
        out["plataforma_resumo"] = out["plataforma"].fillna("").astype(str)
    elif "plataforma_resumo" not in out.columns:
        out["plataforma_resumo"] = "—"
    vv = pd.to_numeric(out["valor_venda"], errors="coerce").fillna(0.0)
    vf = pd.to_numeric(out["valor_faturado_nf"], errors="coerce").fillna(0.0)
    out["diferenca"] = vv - vf
    out["plataforma"] = out["plataforma_resumo"].astype(str)
    if "comercial_incompleto" not in out.columns:
        out["comercial_incompleto"] = False
    else:
        out["comercial_incompleto"] = out["comercial_incompleto"].fillna(False).astype(bool)
    return out


def build_nf_panel_materializado_dataframe(
    df_nf: pd.DataFrame,
    df_fiscal: pd.DataFrame,
    *,
    aplicar_ads: bool = True,
) -> pd.DataFrame:
    """
    ``df_nf`` = saída de ``build_nf_materializado_dataframe`` (contrato NF-first).
    ``df_fiscal`` = ``build_fiscal_materializado_dataframe`` (pode ser vazio).
    """
    if df_nf.empty:
        return pd.DataFrame(columns=sorted(NF_PANEL_REQUIRED_COLUMNS))

    used_fiscal_merge = False
    if fiscal_contract_dataframe_valid(df_fiscal) and not df_fiscal.empty:
        comm = df_nf.drop(columns=["schema_version_nf"], errors="ignore").copy()
        comm["plataforma_resumo"] = (
            comm["plataforma"].fillna("").astype(str) if "plataforma" in comm.columns else "—"
        )
        base = merge_fiscal_base_with_commercial_nf_dataframe(df_fiscal, comm)
        used_fiscal_merge = True
    else:
        base = _commercial_nf_to_panel_shape(df_nf)

    if base.empty:
        return pd.DataFrame(columns=sorted(NF_PANEL_REQUIRED_COLUMNS))

    if not used_fiscal_merge:
        base = apply_nf_panel_frete_gap_fallback(base)
    base = apply_nf_panel_frete_repasse_e_plataforma_coerencia(base)
    base = apply_nf_panel_resultado_frete_nota_lista(base)
    base = apply_nf_panel_custo_ads(base, aplicar_ads=aplicar_ads)
    if "plataforma" not in base.columns:
        base = base.copy()
        base["plataforma"] = base["plataforma_resumo"].fillna("").astype(str)
    need = sorted(NF_PANEL_REQUIRED_COLUMNS - frozenset(base.columns))
    for c in need:
        base[c] = False if c == "comercial_incompleto" else pd.NA
    return base[list(sorted(NF_PANEL_REQUIRED_COLUMNS))].copy()
