"""
Motor de cálculo fiscal para empresas em regime de Lucro Presumido.

Calcula impostos federais (PIS, COFINS, IRPJ, CSLL com presunção) e estaduais
(ICMS interno, ICMS interestadual, DIFAL e FCP) com base no fiscal materializado v3.

Premissas e fontes de negócio: docs/pesquisa_fcp_lucro_presumido_2026.md.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

import pandas as pd

from faturamento_dre_recorte import (
    calcular_devolucoes_fiscais_no_periodo,
    mask_nf_emissao_no_periodo,
)
from faturamento_dre_recorte_minimo import _nf_fiscal_situacao_invalida

PIS_DEFAULT = 0.0065
COFINS_DEFAULT = 0.03
PRESUNCAO_IRPJ_ATE_LIMITE_DEFAULT = 0.08
PRESUNCAO_IRPJ_ACIMA_LIMITE_DEFAULT = 0.088
PRESUNCAO_CSLL_ATE_LIMITE_DEFAULT = 0.12
PRESUNCAO_CSLL_ACIMA_LIMITE_DEFAULT = 0.132
ALIQUOTA_IRPJ_DEFAULT = 0.15
ALIQUOTA_ADICIONAL_IRPJ_DEFAULT = 0.10
LIMITE_ADICIONAL_IRPJ_TRIMESTRAL_DEFAULT = 60000.00
ALIQUOTA_CSLL_DEFAULT = 0.09
LIMITE_RECEITA_MAJORACAO_ANUAL_DEFAULT = 5000000.00

ALIQUOTA_INTERESTADUAL_DEFAULT = 0.07
ALIQUOTA_DESTINO_DIFAL_DEFAULT = 0.18
CFOP_INTERNO_VENDA = "5102"
CFOP_INTERESTADUAL_NAO_CONTRIBUINTE = "6108"
SITUACOES_VALIDAS_PADRAO = frozenset({"emitida danfe", "autorizada"})


def _default_icms_interestadual_origem_sp() -> dict[str, float]:
    ufs = {
        "AC": 0.07,
        "AL": 0.07,
        "AM": 0.07,
        "AP": 0.07,
        "BA": 0.07,
        "CE": 0.07,
        "DF": 0.07,
        "ES": 0.07,
        "GO": 0.07,
        "MA": 0.07,
        "MG": 0.12,
        "MS": 0.07,
        "MT": 0.07,
        "PA": 0.07,
        "PB": 0.07,
        "PE": 0.07,
        "PI": 0.07,
        "PR": 0.12,
        "RJ": 0.12,
        "RN": 0.07,
        "RO": 0.07,
        "RR": 0.07,
        "RS": 0.12,
        "SC": 0.12,
        "SE": 0.07,
        "TO": 0.07,
    }
    return ufs


@dataclass(frozen=True)
class LucroPresumidoParams:
    """Parâmetros do motor de Lucro Presumido (federais e regra LC 224/2025)."""

    pis: float = PIS_DEFAULT
    cofins: float = COFINS_DEFAULT
    presuncao_irpj_ate_limite: float = PRESUNCAO_IRPJ_ATE_LIMITE_DEFAULT
    presuncao_irpj_acima_limite: float = PRESUNCAO_IRPJ_ACIMA_LIMITE_DEFAULT
    presuncao_csll_ate_limite: float = PRESUNCAO_CSLL_ATE_LIMITE_DEFAULT
    presuncao_csll_acima_limite: float = PRESUNCAO_CSLL_ACIMA_LIMITE_DEFAULT
    aliquota_irpj: float = ALIQUOTA_IRPJ_DEFAULT
    aliquota_adicional_irpj: float = ALIQUOTA_ADICIONAL_IRPJ_DEFAULT
    limite_adicional_irpj_trimestral: float = LIMITE_ADICIONAL_IRPJ_TRIMESTRAL_DEFAULT
    aliquota_csll: float = ALIQUOTA_CSLL_DEFAULT
    limite_receita_majoracao_anual: float = LIMITE_RECEITA_MAJORACAO_ANUAL_DEFAULT
    aplicar_majoracao_lc_224: bool = True


@dataclass(frozen=True)
class IcmsParams:
    """Parâmetros estaduais de ICMS/FCP para origem SP (etapa atual)."""

    icms_interno_moveis_9403_completos: float = 0.133
    icms_interno_moveis_9403_partes_pecas: float = 0.18
    icms_interestadual_origem_sp: Mapping[str, float] = field(default_factory=_default_icms_interestadual_origem_sp)
    aliquota_destino_generica_difal: float = ALIQUOTA_DESTINO_DIFAL_DEFAULT
    fcp_destino: Mapping[str, float] = field(default_factory=lambda: {"RJ": 0.02})
    fcp_default: float = 0.00


@dataclass(frozen=True)
class LucroPresumidoBreakdown:
    """Resultado detalhado do cálculo fiscal de um período."""

    receita_bruta: float
    nfs: int
    receita_devolucoes: float

    pis_aliquota: float
    pis_valor: float
    cofins_aliquota: float
    cofins_valor: float
    irpj_base: float
    irpj_valor: float
    irpj_adicional_valor: float
    csll_base: float
    csll_valor: float
    total_federal: float

    icms_interno_base: float
    icms_interno_valor: float
    icms_interestadual_base: float
    icms_interestadual_valor: float
    difal_valor: float
    fcp_valor: float
    total_estadual: float

    fcp_base_zero: float
    fcp_base_aplicado: float
    fcp_ufs_aplicadas: tuple[str, ...]
    fcp_ufs_zeradas: tuple[str, ...]

    total_imposto: float
    aliquota_efetiva: float

    aplicou_majoracao_lc_224: bool
    receita_anual_referencia: float
    cfops_outros_base: float
    # Tributos por NF (federais rateados por Valor_Liquido_NF; estaduais por linha).
    tributos_por_nf: pd.DataFrame
    avisos: tuple[str, ...] = ()


def _meses_cobertos(nf_d_ini: pd.Timestamp, nf_d_fim: pd.Timestamp) -> int:
    ini = pd.Timestamp(nf_d_ini)
    fim = pd.Timestamp(nf_d_fim)
    if fim < ini:
        return 1
    return int((fim.year - ini.year) * 12 + (fim.month - ini.month) + 1)


def _filtrar_nfs_validas_periodo(
    df_fiscal: pd.DataFrame,
    *,
    org_id: str,
    nf_d_ini: pd.Timestamp,
    nf_d_fim: pd.Timestamp,
) -> pd.DataFrame:
    """
    Recorte por ``org_id`` + emissão no intervalo + situações válidas.

    O filtro de datas usa ``mask_nf_emissao_no_periodo`` (dia civil, como o slice
    fiscal e o agregador SN), evitando excluir NFs do último dia após 00:00 quando
    ``nf_d_fim`` é meia-noite desse dia.
    """
    if df_fiscal.empty:
        return df_fiscal.copy()
    out = df_fiscal.copy()
    out["org_id"] = out.get("org_id", "").astype(str)
    out = out[out["org_id"] == str(org_id).strip()]
    out["Nota_Data_Emissao"] = pd.to_datetime(out.get("Nota_Data_Emissao"), errors="coerce")
    periodo_ini_d = pd.Timestamp(nf_d_ini).date()
    periodo_fim_d = pd.Timestamp(nf_d_fim).date()
    m_period = mask_nf_emissao_no_periodo(
        out["Nota_Data_Emissao"], periodo_ini_d, periodo_fim_d
    )
    out = out.loc[m_period].copy()
    situacao = out.get("Nota_Situacao", "").fillna("").astype(str).str.strip().str.lower()
    if not situacao.empty:
        out = out[situacao.isin(SITUACOES_VALIDAS_PADRAO) & (~_nf_fiscal_situacao_invalida(situacao))]
    out["Valor_Liquido_NF"] = pd.to_numeric(out.get("Valor_Liquido_NF"), errors="coerce").fillna(0.0)
    out["Nota_CFOP"] = out.get("Nota_CFOP", "").fillna("").astype(str).str.strip()
    out["Nota_UF_Destino"] = out.get("Nota_UF_Destino", "").fillna("").astype(str).str.strip().str.upper()
    out["Nota_NCM"] = out.get("Nota_NCM", "").fillna("").astype(str).str.strip()
    return out


def _separar_internas_interestaduais(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    if df.empty:
        return df.copy(), df.copy(), df.copy()
    interno = df[df["Nota_CFOP"] == CFOP_INTERNO_VENDA].copy()
    interestadual = df[df["Nota_CFOP"] == CFOP_INTERESTADUAL_NAO_CONTRIBUINTE].copy()
    outros = df[~df["Nota_CFOP"].isin({CFOP_INTERNO_VENDA, CFOP_INTERESTADUAL_NAO_CONTRIBUINTE})].copy()
    return interno, interestadual, outros


def _aplicar_lc_224_se_aplicavel(
    *,
    receita_bruta: float,
    receita_anual_ref: float,
    params: LucroPresumidoParams,
) -> tuple[float, float, bool]:
    if not params.aplicar_majoracao_lc_224:
        return receita_bruta, 0.0, False
    limite = float(params.limite_receita_majoracao_anual)
    if receita_anual_ref <= limite + 1e-9:
        return receita_bruta, 0.0, False
    parcela_acima_ratio = max(0.0, min(1.0, (receita_anual_ref - limite) / receita_anual_ref))
    base_acima = receita_bruta * parcela_acima_ratio
    base_ate = max(0.0, receita_bruta - base_acima)
    return base_ate, base_acima, base_acima > 1e-12


def _calcular_adicional_irpj_pro_rata(
    irpj_base: float,
    meses_periodo: int,
    params: LucroPresumidoParams,
) -> float:
    """
    Estimativa gerencial do adicional IRPJ para período parcial.

    Regra legal é trimestral (R$ 60 mil/trimestre). Aqui aplicamos pro-rata mensal
    para períodos não fechados em trimestre; o fechamento oficial deve ser feito com contador.
    """
    limite_periodo = (params.limite_adicional_irpj_trimestral / 3.0) * max(1, int(meses_periodo))
    excedente = max(0.0, float(irpj_base) - float(limite_periodo))
    return excedente * params.aliquota_adicional_irpj


def _calcular_federal(
    *,
    receita_bruta: float,
    meses_periodo: int,
    receita_anual_ref: float,
    params: LucroPresumidoParams,
) -> dict[str, float | bool]:
    base_ate, base_acima, aplicou_majoracao = _aplicar_lc_224_se_aplicavel(
        receita_bruta=receita_bruta,
        receita_anual_ref=receita_anual_ref,
        params=params,
    )

    pis_valor = receita_bruta * params.pis
    cofins_valor = receita_bruta * params.cofins
    irpj_base = base_ate * params.presuncao_irpj_ate_limite + base_acima * params.presuncao_irpj_acima_limite
    csll_base = base_ate * params.presuncao_csll_ate_limite + base_acima * params.presuncao_csll_acima_limite
    irpj_valor = irpj_base * params.aliquota_irpj
    irpj_adicional = _calcular_adicional_irpj_pro_rata(irpj_base, meses_periodo, params)
    csll_valor = csll_base * params.aliquota_csll
    total_federal = pis_valor + cofins_valor + irpj_valor + irpj_adicional + csll_valor

    return {
        "pis_valor": pis_valor,
        "cofins_valor": cofins_valor,
        "irpj_base": irpj_base,
        "irpj_valor": irpj_valor,
        "irpj_adicional_valor": irpj_adicional,
        "csll_base": csll_base,
        "csll_valor": csll_valor,
        "total_federal": total_federal,
        "aplicou_majoracao_lc_224": aplicou_majoracao,
    }


def _aliquota_icms_interno_por_ncm(ncm: str | None, icms_params: IcmsParams) -> tuple[float, str | None]:
    """
    Retorna (aliquota, aviso_se_aplicavel) para ICMS interno SP.

    Regras:
    - NCM começa com '9403.9' (partes/peças): 18%
    - NCM começa com '9403' ou '9401' ou '9404' (móveis): 13,3%
    - NCM ausente/vazio/desconhecido: 13,3% + aviso
    """
    if ncm is None or pd.isna(ncm) or str(ncm).strip() == "":
        return icms_params.icms_interno_moveis_9403_completos, "ncm_ausente"
    ncm_str = str(ncm).strip()
    if ncm_str.startswith("9403.9"):
        return icms_params.icms_interno_moveis_9403_partes_pecas, None
    if ncm_str.startswith(("9403", "9401", "9404")):
        return icms_params.icms_interno_moveis_9403_completos, None
    return icms_params.icms_interno_moveis_9403_completos, "ncm_fora_cap_94"


def _calcular_icms_interno(df_interno: pd.DataFrame, icms_params: IcmsParams) -> tuple[dict[str, float], list[str]]:
    if df_interno.empty:
        return {"base": 0.0, "valor": 0.0}, []
    base = float(pd.to_numeric(df_interno["Valor_Liquido_NF"], errors="coerce").fillna(0.0).sum())
    valor = 0.0
    avisos: list[str] = []
    for _, row in df_interno.iterrows():
        aliquota, aviso = _aliquota_icms_interno_por_ncm(row.get("Nota_NCM"), icms_params)
        vl = float(pd.to_numeric(row.get("Valor_Liquido_NF"), errors="coerce"))
        valor += vl * aliquota
        if aviso == "ncm_ausente":
            avisos.append("NCM ausente em operação interna; aplicado ICMS interno 13,3% por premissa.")
        elif aviso == "ncm_fora_cap_94":
            avisos.append("NCM fora do capítulo 94 em operação interna; aplicado ICMS interno 13,3% por premissa.")
    return {"base": base, "valor": valor}, sorted(set(avisos))


def _aliquota_interestadual_por_uf_destino(uf_destino: str, icms_params: IcmsParams, uf_origem: str) -> float:
    if str(uf_origem).strip().upper() != "SP":
        return ALIQUOTA_INTERESTADUAL_DEFAULT
    uf = str(uf_destino or "").strip().upper()
    if not uf:
        return ALIQUOTA_INTERESTADUAL_DEFAULT
    return float(icms_params.icms_interestadual_origem_sp.get(uf, ALIQUOTA_INTERESTADUAL_DEFAULT))


def _calcular_icms_interestadual_e_difal(
    df_interestadual: pd.DataFrame,
    *,
    icms_params: IcmsParams,
    uf_origem: str,
) -> dict[str, float | set[str]]:
    if df_interestadual.empty:
        return {
            "base": 0.0,
            "icms_interestadual_valor": 0.0,
            "difal_valor": 0.0,
            "fcp_valor": 0.0,
            "fcp_base_zero": 0.0,
            "fcp_base_aplicado": 0.0,
            "fcp_ufs_aplicadas": set(),
            "fcp_ufs_zeradas": set(),
            "ufs_destino_ausentes": 0.0,
        }
    base = 0.0
    v_inter = 0.0
    v_difal = 0.0
    v_fcp = 0.0
    base_fcp_zero = 0.0
    base_fcp_aplicado = 0.0
    fcp_ufs_aplicadas: set[str] = set()
    fcp_ufs_zeradas: set[str] = set()
    uf_ausente = 0.0

    for _, row in df_interestadual.iterrows():
        vl = float(pd.to_numeric(row.get("Valor_Liquido_NF"), errors="coerce"))
        uf = str(row.get("Nota_UF_Destino") or "").strip().upper()
        aliq_inter = _aliquota_interestadual_por_uf_destino(uf, icms_params, uf_origem)
        aliq_dest = float(icms_params.aliquota_destino_generica_difal)
        aliq_difal = max(0.0, aliq_dest - aliq_inter)
        aliq_fcp = float(icms_params.fcp_destino.get(uf, icms_params.fcp_default))

        base += vl
        v_inter += vl * aliq_inter
        v_difal += vl * aliq_difal
        v_fcp += vl * aliq_fcp

        if aliq_fcp > 0:
            base_fcp_aplicado += vl
            if uf:
                fcp_ufs_aplicadas.add(uf)
        else:
            base_fcp_zero += vl
            if uf:
                fcp_ufs_zeradas.add(uf)
        if not uf:
            uf_ausente += 1

    return {
        "base": base,
        "icms_interestadual_valor": v_inter,
        "difal_valor": v_difal,
        "fcp_valor": v_fcp,
        "fcp_base_zero": base_fcp_zero,
        "fcp_base_aplicado": base_fcp_aplicado,
        "fcp_ufs_aplicadas": fcp_ufs_aplicadas,
        "fcp_ufs_zeradas": fcp_ufs_zeradas,
        "ufs_destino_ausentes": uf_ausente,
    }


TRIBUTOS_POR_NF_COLUMNS: tuple[str, ...] = (
    "Nota_Numero_Normalizado",
    "Nota_Data_Emissao",
    "Valor_Liquido_NF",
    "pis_nf",
    "cofins_nf",
    "irpj_nf",
    "csll_nf",
    "icms_interno_nf",
    "icms_interestadual_nf",
    "difal_nf",
    "fcp_nf",
    "imposto_total_nf",
)


def _empty_tributos_por_nf() -> pd.DataFrame:
    return pd.DataFrame(columns=list(TRIBUTOS_POR_NF_COLUMNS))


def _tributos_estaduais_por_linha(
    row: pd.Series,
    icms_params: IcmsParams,
    uf_origem: str,
) -> tuple[float, float, float, float]:
    """ICMS interno (5102) ou bloco interestadual (6108 + CFOPs tratados como interestadual)."""
    cfop = str(row.get("Nota_CFOP") or "").strip()
    vl = float(pd.to_numeric(row.get("Valor_Liquido_NF"), errors="coerce"))
    uf = str(row.get("Nota_UF_Destino") or "").strip().upper()
    ncm = row.get("Nota_NCM")
    if cfop == CFOP_INTERNO_VENDA:
        aliq, _ = _aliquota_icms_interno_por_ncm(ncm, icms_params)
        return (vl * aliq, 0.0, 0.0, 0.0)
    aliq_inter = _aliquota_interestadual_por_uf_destino(uf, icms_params, uf_origem)
    aliq_dest = float(icms_params.aliquota_destino_generica_difal)
    aliq_difal = max(0.0, aliq_dest - aliq_inter)
    aliq_fcp = float(icms_params.fcp_destino.get(uf, icms_params.fcp_default))
    return (0.0, vl * aliq_inter, vl * aliq_difal, vl * aliq_fcp)


def _construir_tributos_por_nf(
    df_validas: pd.DataFrame,
    *,
    receita_nf_soma: float,
    pis_valor: float,
    cofins_valor: float,
    irpj_valor: float,
    irpj_adicional_valor: float,
    csll_valor: float,
    icms_params: IcmsParams,
    uf_origem: str,
) -> pd.DataFrame:
    if df_validas.empty:
        return _empty_tributos_por_nf()
    denom = max(float(receita_nf_soma), 1e-12)
    irpj_total = float(irpj_valor) + float(irpj_adicional_valor)
    rows: list[dict[str, Any]] = []
    for _, row in df_validas.iterrows():
        vl = float(pd.to_numeric(row.get("Valor_Liquido_NF"), errors="coerce"))
        peso = vl / denom
        ic_i, ic_e, dif, fcp = _tributos_estaduais_por_linha(row, icms_params, uf_origem)
        pis_n = float(pis_valor) * peso
        cof_n = float(cofins_valor) * peso
        irpj_n = irpj_total * peso
        csll_n = float(csll_valor) * peso
        tot = pis_n + cof_n + irpj_n + csll_n + ic_i + ic_e + dif + fcp
        emi = row.get("Nota_Data_Emissao")
        rows.append(
            {
                "Nota_Numero_Normalizado": str(row.get("Nota_Numero_Normalizado") or "").strip(),
                "Nota_Data_Emissao": pd.Timestamp(emi) if emi is not None and not (isinstance(emi, float) and pd.isna(emi)) else pd.NaT,
                "Valor_Liquido_NF": vl,
                "pis_nf": pis_n,
                "cofins_nf": cof_n,
                "irpj_nf": irpj_n,
                "csll_nf": csll_n,
                "icms_interno_nf": ic_i,
                "icms_interestadual_nf": ic_e,
                "difal_nf": dif,
                "fcp_nf": fcp,
                "imposto_total_nf": tot,
            }
        )
    return pd.DataFrame(rows, columns=list(TRIBUTOS_POR_NF_COLUMNS))


def _validar_consistencia_tributos_por_nf(bd: LucroPresumidoBreakdown, df_nf: pd.DataFrame) -> None:
    """Garante soma por NF ≈ totais agregados e linha = soma dos 8 tributos."""
    if df_nf.empty:
        return
    tol = 0.10
    if abs(float(df_nf["pis_nf"].sum()) - float(bd.pis_valor)) > tol:
        raise ValueError("Soma pis_nf diverge do pis_valor agregado.")
    if abs(float(df_nf["cofins_nf"].sum()) - float(bd.cofins_valor)) > tol:
        raise ValueError("Soma cofins_nf diverge do cofins_valor agregado.")
    if abs(float(df_nf["irpj_nf"].sum()) - (float(bd.irpj_valor) + float(bd.irpj_adicional_valor))) > tol:
        raise ValueError("Soma irpj_nf diverge de irpj_valor + irpj_adicional_valor.")
    if abs(float(df_nf["csll_nf"].sum()) - float(bd.csll_valor)) > tol:
        raise ValueError("Soma csll_nf diverge do csll_valor agregado.")
    if abs(float(df_nf["icms_interno_nf"].sum()) - float(bd.icms_interno_valor)) > tol:
        raise ValueError("Soma icms_interno_nf diverge do icms_interno_valor agregado.")
    if abs(float(df_nf["icms_interestadual_nf"].sum()) - float(bd.icms_interestadual_valor)) > tol:
        raise ValueError("Soma icms_interestadual_nf diverge do icms_interestadual_valor agregado.")
    if abs(float(df_nf["difal_nf"].sum()) - float(bd.difal_valor)) > tol:
        raise ValueError("Soma difal_nf diverge do difal_valor agregado.")
    if abs(float(df_nf["fcp_nf"].sum()) - float(bd.fcp_valor)) > tol:
        raise ValueError("Soma fcp_nf diverge do fcp_valor agregado.")
    if abs(float(df_nf["imposto_total_nf"].sum()) - float(bd.total_imposto)) > tol:
        raise ValueError("Soma imposto_total_nf diverge do total_imposto agregado.")
    partes = df_nf[["pis_nf", "cofins_nf", "irpj_nf", "csll_nf", "icms_interno_nf", "icms_interestadual_nf", "difal_nf", "fcp_nf"]]
    linha = partes.sum(axis=1)
    if (linha - df_nf["imposto_total_nf"].astype(float)).abs().max() > 1e-4:
        raise ValueError("imposto_total_nf não bate com a soma dos 8 tributos por linha.")


def calcular_lucro_presumido(
    df_fiscal: pd.DataFrame,
    df_devolucoes: pd.DataFrame | None = None,
    *,
    org_id: str,
    nf_d_ini: pd.Timestamp,
    nf_d_fim: pd.Timestamp,
    receita_anual_estimada: Optional[float] = None,
    params: Optional[LucroPresumidoParams] = None,
    icms_params: Optional[IcmsParams] = None,
    uf_origem: str = "SP",
) -> LucroPresumidoBreakdown:
    """
    Calcula impostos de Lucro Presumido para um período.

    Args:
        df_fiscal: DataFrame fiscal v3 (com CFOP/NCM/UF).
        df_devolucoes: DataFrame opcional com ``Nota_Data_Emissao``, ``Valor_Liquido_Devolucao``
            e ``org_id`` ou ``empresa`` (critério fiscal de período = emissão da NF de devolução).
        org_id: empresa alvo.
        nf_d_ini: data inicial (inclusive).
        nf_d_fim: data final (inclusive).
        receita_anual_estimada: se informado, usado na LC 224/2025; se ``None``,
            extrapola receita do período por meses cobertos.
        params: parâmetros federais do motor.
        icms_params: parâmetros estaduais (origem SP nesta etapa).
        uf_origem: UF de origem da operação (etapa atual: SP).

    Returns:
        ``LucroPresumidoBreakdown`` com breakdown federal, estadual, totais e avisos.
    """
    p = params or LucroPresumidoParams()
    ip = icms_params or IcmsParams()
    avisos: list[str] = []

    meses_periodo = _meses_cobertos(nf_d_ini, nf_d_fim)
    df_validas = _filtrar_nfs_validas_periodo(df_fiscal, org_id=org_id, nf_d_ini=nf_d_ini, nf_d_fim=nf_d_fim)
    df_interno, df_inter, df_outros = _separar_internas_interestaduais(df_validas)

    base_outros = float(pd.to_numeric(df_outros.get("Valor_Liquido_NF"), errors="coerce").fillna(0.0).sum()) if not df_outros.empty else 0.0
    if not df_outros.empty:
        cfops = sorted(set(df_outros["Nota_CFOP"].fillna("").astype(str).str.strip().tolist()))
        receita_total_validas = float(pd.to_numeric(df_validas["Valor_Liquido_NF"], errors="coerce").fillna(0.0).sum())
        pct = (base_outros / receita_total_validas * 100.0) if receita_total_validas > 0 else 0.0
        avisos.append(
            f"CFOPs não classificados encontrados: {cfops} — valor R$ {base_outros:.2f} ({pct:.2f}%); tratados como interestadual default."
        )

    df_inter_full = pd.concat([df_inter, df_outros], ignore_index=True) if (not df_inter.empty or not df_outros.empty) else df_inter.copy()

    receita_nf = float(pd.to_numeric(df_validas.get("Valor_Liquido_NF"), errors="coerce").fillna(0.0).sum())
    periodo_ini = pd.Timestamp(nf_d_ini).date()
    periodo_fim_d = pd.Timestamp(nf_d_fim).date()
    receita_devolucoes = calcular_devolucoes_fiscais_no_periodo(
        df_devolucoes,
        chave_empresa=org_id,
        periodo_inicio=periodo_ini,
        periodo_fim=periodo_fim_d,
        ok_nf_dates=True,
    )
    receita_bruta = max(0.0, receita_nf - receita_devolucoes)

    receita_anual_ref = (
        float(receita_anual_estimada)
        if receita_anual_estimada is not None
        else receita_bruta * (12.0 / float(max(1, meses_periodo)))
    )

    fed = _calcular_federal(
        receita_bruta=receita_bruta,
        meses_periodo=meses_periodo,
        receita_anual_ref=receita_anual_ref,
        params=p,
    )
    int_calc, int_avisos = _calcular_icms_interno(df_interno, ip)
    avisos.extend(int_avisos)
    inter_calc = _calcular_icms_interestadual_e_difal(df_inter_full, icms_params=ip, uf_origem=uf_origem)

    uf_ausente = int(inter_calc["ufs_destino_ausentes"])
    if uf_ausente > 0:
        total_inter_nfs = max(1, len(df_inter_full))
        pct = (uf_ausente / total_inter_nfs) * 100.0
        avisos.append(f"UF destino não preenchida em {uf_ausente} NFs ({pct:.2f}%).")

    ufs_zeradas = sorted(inter_calc["fcp_ufs_zeradas"])
    if ufs_zeradas:
        avisos.append(f"FCP estimado em 0% para UFs: {ufs_zeradas}.")
    avisos.append("Estimativa DIFAL com alíquota destino padrão 18%.")

    total_estadual = float(int_calc["valor"] + inter_calc["icms_interestadual_valor"] + inter_calc["difal_valor"] + inter_calc["fcp_valor"])
    total_federal = float(fed["total_federal"])
    total_imposto = total_federal + total_estadual
    aliquota_efetiva = (total_imposto / receita_bruta) if receita_bruta > 1e-12 else 0.0

    tributos_por_nf = _construir_tributos_por_nf(
        df_validas,
        receita_nf_soma=receita_nf,
        pis_valor=float(fed["pis_valor"]),
        cofins_valor=float(fed["cofins_valor"]),
        irpj_valor=float(fed["irpj_valor"]),
        irpj_adicional_valor=float(fed["irpj_adicional_valor"]),
        csll_valor=float(fed["csll_valor"]),
        icms_params=ip,
        uf_origem=uf_origem,
    )
    bd = LucroPresumidoBreakdown(
        receita_bruta=receita_bruta,
        nfs=int(len(df_validas)),
        receita_devolucoes=receita_devolucoes,
        pis_aliquota=p.pis,
        pis_valor=float(fed["pis_valor"]),
        cofins_aliquota=p.cofins,
        cofins_valor=float(fed["cofins_valor"]),
        irpj_base=float(fed["irpj_base"]),
        irpj_valor=float(fed["irpj_valor"]),
        irpj_adicional_valor=float(fed["irpj_adicional_valor"]),
        csll_base=float(fed["csll_base"]),
        csll_valor=float(fed["csll_valor"]),
        total_federal=total_federal,
        icms_interno_base=float(int_calc["base"]),
        icms_interno_valor=float(int_calc["valor"]),
        icms_interestadual_base=float(inter_calc["base"]),
        icms_interestadual_valor=float(inter_calc["icms_interestadual_valor"]),
        difal_valor=float(inter_calc["difal_valor"]),
        fcp_valor=float(inter_calc["fcp_valor"]),
        total_estadual=total_estadual,
        fcp_base_zero=float(inter_calc["fcp_base_zero"]),
        fcp_base_aplicado=float(inter_calc["fcp_base_aplicado"]),
        fcp_ufs_aplicadas=tuple(sorted(inter_calc["fcp_ufs_aplicadas"])),
        fcp_ufs_zeradas=tuple(ufs_zeradas),
        total_imposto=total_imposto,
        aliquota_efetiva=aliquota_efetiva,
        aplicou_majoracao_lc_224=bool(fed["aplicou_majoracao_lc_224"]),
        receita_anual_referencia=receita_anual_ref,
        cfops_outros_base=base_outros,
        tributos_por_nf=tributos_por_nf,
        avisos=tuple(avisos),
    )
    _validar_consistencia_tributos_por_nf(bd, tributos_por_nf)
    return bd
