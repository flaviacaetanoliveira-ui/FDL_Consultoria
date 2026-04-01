"""Cálculos de imposto, despesas fixas e resultado (regras fechadas: Vl. Venda, nota fiscal, rateio)."""
from __future__ import annotations

import pandas as pd

from .config import CUSTO_UNITARIO_COL
from .config import OUTRAS_DESPESAS_COL
from .config import STATUS_CUSTO_OK
from .normalize import to_numeric_br
from .params import FaturamentoParamsError

_COL_MODALIDADE_ENVIO_CANDIDATES: tuple[str, ...] = (
    "Modalidade de envio",
    "Tipo de envio",
    "Transportadora",
    "Serviço de envio",
    "Servico de envio",
    "Tipo de logística",
    "Tipo de logistica",
)


def _frete_mercado_envios_vs_transportadora(df: pd.DataFrame, cf_numeric: pd.Series) -> tuple[pd.Series, pd.Series]:
    cf0 = cf_numeric.fillna(0.0)
    col = next((c for c in _COL_MODALIDADE_ENVIO_CANDIDATES if c in df.columns), None)
    if col is None:
        return cf0, pd.Series(0.0, index=df.index, dtype=float)
    txt = df[col].fillna("").astype(str).str.strip().str.casefold()
    is_me = txt.str.contains("mercado envio", regex=False) | txt.str.contains(
        "coleta do mercado", regex=False
    )
    is_me = is_me | txt.eq("")
    frete_me = cf0.where(is_me, 0.0)
    frete_tp = cf0.where(~is_me, 0.0)
    return frete_me, frete_tp


def frete_plataforma_series(df: pd.DataFrame, cf_numeric: pd.Series) -> pd.Series:
    """Frete da plataforma (Mercado Envios / vazio ⇒ todo o CF) para a fórmula do resultado."""
    frete_me, _frete_tp = _frete_mercado_envios_vs_transportadora(df, cf_numeric)
    return frete_me.fillna(0.0)


def resolve_coluna_base_imposto(df: pd.DataFrame, candidates: tuple[str, ...]) -> str | None:
    for c in candidates:
        if c and c in df.columns:
            return c
    return None


def compute_financial_columns(
    df: pd.DataFrame,
    *,
    aliquota_imposto: float,
    aliquota_despesas_fixas: float,
    data_processamento_iso: str,
    base_imposto_column: str | None,
) -> pd.DataFrame:
    """Legado V1 / compat.: receita bruta com frete TP e base de imposto por coluna."""
    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    cf = "Custo de Frete"
    tc = "Taxa de Comissão"
    qtd_col = "Quantidade"

    out[qtd_col] = to_numeric_br(out[qtd_col])
    out[pl] = to_numeric_br(out[pl])
    out[vt] = to_numeric_br(out[vt])
    out[cf] = to_numeric_br(out[cf])
    out[tc] = to_numeric_br(out[tc])
    out[CUSTO_UNITARIO_COL] = to_numeric_br(out[CUSTO_UNITARIO_COL])

    if OUTRAS_DESPESAS_COL in out.columns:
        out[OUTRAS_DESPESAS_COL] = to_numeric_br(out[OUTRAS_DESPESAS_COL])
    else:
        out[OUTRAS_DESPESAS_COL] = 0.0

    frete_me, frete_tp = _frete_mercado_envios_vs_transportadora(out, out[cf])
    out["Frete Mercado Envios"] = frete_me
    out["Frete transportadora própria"] = frete_tp
    out["Receita_Bruta"] = out[qtd_col] * out[pl] + frete_tp
    out["Custo_Produto_Total"] = out[qtd_col] * out[CUSTO_UNITARIO_COL]

    if base_imposto_column:
        out["Base_Imposto"] = to_numeric_br(out[base_imposto_column])
    else:
        out["Base_Imposto"] = pd.NA

    out["Imposto"] = out["Base_Imposto"] * aliquota_imposto
    out["Despesas Fixas"] = out["Receita_Bruta"] * aliquota_despesas_fixas
    out["Resultado"] = (
        out["Receita_Bruta"]
        - out["Custo_Produto_Total"]
        - out[cf]
        - out[tc]
        - out[OUTRAS_DESPESAS_COL]
        - out["Imposto"]
        - out["Despesas Fixas"]
    )
    out["Resultado_Pct"] = pd.NA
    mask_rb = out["Receita_Bruta"].notna() & (out["Receita_Bruta"] > 0)
    out.loc[mask_rb, "Resultado_Pct"] = (
        out.loc[mask_rb, "Resultado"] / out.loc[mask_rb, "Receita_Bruta"]
    )

    if "Status_Custo" in out.columns:
        ok = out["Status_Custo"].eq(STATUS_CUSTO_OK)
        bad = ~ok.fillna(True)
        out.loc[bad, "Resultado"] = pd.NA
        out.loc[bad, "Resultado_Pct"] = pd.NA

    out["Aliquota_Imposto_Utilizada"] = aliquota_imposto
    out["Aliquota_Despesas_Fixas_Utilizada"] = aliquota_despesas_fixas
    out["Data_Processamento"] = data_processamento_iso
    return out


def _competencia_from_timestamp(ts: pd.Timestamp | object) -> str | None:
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return None
    if isinstance(ts, pd.Timestamp):
        if pd.isna(ts):
            return None
        return f"{int(ts.year):04d}-{int(ts.month):02d}"
    t = pd.to_datetime(ts, errors="coerce")
    if pd.isna(t):
        return None
    return f"{int(t.year):04d}-{int(t.month):02d}"


def compute_financial_columns_regras_fechadas(
    df: pd.DataFrame,
    *,
    df_params_mensais: pd.DataFrame | None,
    fallback_aliquota_imposto: float,
    fallback_despesa_fixa: float,
    data_processamento_iso: str,
) -> pd.DataFrame:
    """
    Resultado = Vl_Venda - Frete_Plataforma - Comissão - Custo_Produto_Total - Outras - Imposto - Despesas Fixas.

    Imposto = (sem nota) 0; (com nota) alíquota(empresa, mês emissão NF) × Nota_Valor_Liquido_Rateado.
    Despesas Fixas = despesa_fixa(empresa, mês Data pedido) × Vl_Venda.
    Receita_Bruta = Vl_Venda (compatível com painéis que somam receita comercial).
    """
    from .io_params_mensais import lookup_parametros_mensais

    out = df.copy()
    pl = "Preço de lista"
    vt = "Valor total"
    cf = "Custo de Frete"
    tc = "Taxa de Comissão"
    qtd_col = "Quantidade"

    out[qtd_col] = to_numeric_br(out[qtd_col])
    out[pl] = to_numeric_br(out[pl])
    if vt in out.columns:
        out[vt] = to_numeric_br(out[vt])
    out[cf] = to_numeric_br(out[cf])
    out[tc] = to_numeric_br(out[tc])
    out[CUSTO_UNITARIO_COL] = to_numeric_br(out[CUSTO_UNITARIO_COL])

    if OUTRAS_DESPESAS_COL in out.columns:
        out[OUTRAS_DESPESAS_COL] = to_numeric_br(out[OUTRAS_DESPESAS_COL])
    else:
        out[OUTRAS_DESPESAS_COL] = 0.0

    if "Vl_Venda" not in out.columns:
        out["Vl_Venda"] = out[qtd_col] * out[pl]
    if "Frete_Plataforma" not in out.columns:
        out["Frete_Plataforma"] = frete_plataforma_series(out, out[cf])

    frete_me, frete_tp = _frete_mercado_envios_vs_transportadora(out, out[cf])
    out["Frete Mercado Envios"] = frete_me
    out["Frete transportadora própria"] = frete_tp

    out["Receita_Bruta"] = out["Vl_Venda"]
    out["Custo_Produto_Total"] = out[qtd_col] * out[CUSTO_UNITARIO_COL]

    if "Nota_Valor_Liquido_Rateado" not in out.columns:
        out["Nota_Valor_Liquido_Rateado"] = 0.0
    if "faturamento_nota_vinculada" not in out.columns:
        out["faturamento_nota_vinculada"] = False

    base_rateada = out["Nota_Valor_Liquido_Rateado"].fillna(0.0).astype(float)

    ali_imp = pd.Series(0.0, index=out.index, dtype=float)
    ali_desp = pd.Series(fallback_despesa_fixa, index=out.index, dtype=float)

    if "Data" in out.columns:
        dt_ped = pd.to_datetime(out["Data"], errors="coerce", dayfirst=True)
    else:
        dt_ped = pd.Series(pd.NaT, index=out.index)

    comp_ped = dt_ped.map(_competencia_from_timestamp)
    comp_nf = (
        out["Nota_Data_Emissao"].map(_competencia_from_timestamp)
        if "Nota_Data_Emissao" in out.columns
        else pd.Series(pd.NA, index=out.index)
    )

    org_ids = out["org_id"].astype(str).str.strip() if "org_id" in out.columns else pd.Series("", index=out.index)
    emps = out["empresa"].astype(str).str.strip() if "empresa" in out.columns else pd.Series("", index=out.index)

    if df_params_mensais is not None and not df_params_mensais.empty:
        for i in out.index:
            oid = str(org_ids.loc[i])
            emp = str(emps.loc[i])
            cp = comp_ped.loc[i]
            cn = comp_nf.loc[i]
            vinc = bool(out.at[i, "faturamento_nota_vinculada"])

            if pd.notna(cp) and str(cp):
                try:
                    _, d = lookup_parametros_mensais(oid, emp, str(cp), df_params_mensais)
                    ali_desp.loc[i] = d
                except FaturamentoParamsError:
                    ali_desp.loc[i] = fallback_despesa_fixa
            else:
                ali_desp.loc[i] = fallback_despesa_fixa

            if not vinc:
                ali_imp.loc[i] = 0.0
            elif pd.notna(cn) and str(cn):
                try:
                    ai, _ = lookup_parametros_mensais(oid, emp, str(cn), df_params_mensais)
                    ali_imp.loc[i] = ai
                except FaturamentoParamsError:
                    ali_imp.loc[i] = fallback_aliquota_imposto
            else:
                ali_imp.loc[i] = fallback_aliquota_imposto
    else:
        mask_nf = out["faturamento_nota_vinculada"].fillna(False).astype(bool)
        ali_imp.loc[mask_nf] = fallback_aliquota_imposto
        ali_desp[:] = fallback_despesa_fixa

    out["Imposto"] = base_rateada * ali_imp
    out["Despesas Fixas"] = out["Vl_Venda"].fillna(0.0).astype(float) * ali_desp

    out["Resultado"] = (
        out["Vl_Venda"]
        - out["Frete_Plataforma"]
        - out[tc]
        - out["Custo_Produto_Total"]
        - out[OUTRAS_DESPESAS_COL]
        - out["Imposto"]
        - out["Despesas Fixas"]
    )
    out["Resultado_Pct"] = pd.NA
    mask_v = out["Vl_Venda"].notna() & (out["Vl_Venda"] > 0)
    out.loc[mask_v, "Resultado_Pct"] = out.loc[mask_v, "Resultado"] / out.loc[mask_v, "Vl_Venda"]

    if "Status_Custo" in out.columns:
        ok = out["Status_Custo"].eq(STATUS_CUSTO_OK)
        bad = ~ok.fillna(True)
        out.loc[bad, "Resultado"] = pd.NA
        out.loc[bad, "Resultado_Pct"] = pd.NA

    out["Base_Imposto"] = base_rateada
    out["Aliquota_Imposto_Utilizada"] = ali_imp
    out["Aliquota_Despesas_Fixas_Utilizada"] = ali_desp
    out["Data_Processamento"] = data_processamento_iso
    return out
