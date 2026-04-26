"""
Consolidação de imposto fiscal entre regimes (Simples Nacional + Lucro Presumido).

Fonte única de verdade para imposto agregado no projeto.
Usado por: painel Apuração Fiscal (KPI Imposto Apurado), DRE Gerencial.

Princípio: cada empresa contribui com seu imposto conforme regime.
- Empresas SN: imposto via ponte fiscal (valor já calculado por ``dre_imposto_para_linha_dre_gerencial``)
- Empresas LP: imposto via motor ``calcular_lucro_presumido``
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping, Sequence

import pandas as pd

from processing.faturamento.lucro_presumido import LucroPresumidoBreakdown, calcular_lucro_presumido
from processing.faturamento.lucro_presumido_loader import load_lucro_presumido_params_from_json
from processing.faturamento.params import (
    FaturamentoParams,
    FaturamentoParamsError,
    FaturamentoParamsV2,
    load_faturamento_params,
)
from processing.faturamento.params_regime import (
    find_empresa_faturamento_entry,
    get_regime_tributario_por_empresa,
)

logger = logging.getLogger(__name__)


def org_ids_do_filtro_ui(
    params_union: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> list[str]:
    """Resolve rótulos do multiselect para ``org_id`` quando params V2 disponível."""
    out: list[str] = []
    seen: set[str] = set()
    for ch in empresas_chaves:
        k = str(ch).strip()
        if not k:
            continue
        oid = k
        if isinstance(params_union, FaturamentoParamsV2):
            ent = find_empresa_faturamento_entry(params_union, k)
            if ent is not None:
                oid = ent.org_id
        if oid not in seen:
            seen.add(oid)
            out.append(oid)
    return out


def resolver_org_ids_para_consolidacao_imposto(
    df: pd.DataFrame,
    params_union: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> list[str]:
    """
    Mapeia rótulos de UI para ``org_id`` da base fiscal (mesma regra do painel Apuração Fiscal).
    """
    out_base = org_ids_do_filtro_ui(params_union, empresas_chaves)
    if df is None or getattr(df, "empty", True) or "org_id" not in df.columns:
        return out_base
    known = {str(x).strip() for x in df["org_id"].dropna().unique().tolist() if str(x).strip()}
    label_to_oid: dict[str, str] = {}
    if "empresa" in df.columns:
        sub = df[["empresa", "org_id"]].dropna()
        sub = sub.assign(
            _e=sub["empresa"].astype(str).str.strip(),
            _o=sub["org_id"].astype(str).str.strip(),
        )
        for _, row in sub.drop_duplicates(subset=["_e"]).iterrows():
            lab = str(row["_e"]).strip()
            oid = str(row["_o"]).strip()
            if lab and oid and lab not in label_to_oid:
                label_to_oid[lab] = oid
    out: list[str] = []
    seen: set[str] = set()
    for oid in out_base:
        k = str(oid).strip()
        if not k:
            continue
        resolved = k if k in known else label_to_oid.get(k, k)
        if resolved not in seen:
            seen.add(resolved)
            out.append(resolved)
    return out


def _regime_eh_lucro_presumido(reg: str | None) -> bool:
    if not reg:
        return False
    r = str(reg).strip().lower().replace(" ", "_")
    return r == "lucro_presumido"


@dataclass(frozen=True)
class ImpostoConsolidado:
    """Resultado da consolidação de imposto entre regimes."""

    imposto_simples_ponte: float
    imposto_lucro_presumido: float
    imposto_total: float
    breakdown_lp_por_empresa: Mapping[str, LucroPresumidoBreakdown]
    empresas_lp_calculadas: tuple[str, ...]
    empresas_lp_sem_params: tuple[str, ...]


def _identificar_empresas_lp_no_recorte(
    df_fiscal: pd.DataFrame,
    org_ids_filtro: Sequence[str] | None,
    json_params_path: Path,
) -> list[str]:
    """
    Retorna ``org_id`` que:
    - aparecem em ``df_fiscal`` (quando há coluna ``org_id``);
    - estão no filtro (ou todos os presentes no DF se filtro ``None``);
    - são Lucro Presumido segundo o JSON de params.
    """
    if not json_params_path.is_file():
        return []
    try:
        params = load_faturamento_params(json_params_path)
    except (FaturamentoParamsError, OSError, ValueError) as exc:
        logger.warning("imposto_consolidado: falha ao ler params %s: %s", json_params_path, exc)
        return []

    if isinstance(params, FaturamentoParams):
        return []

    if df_fiscal is None or df_fiscal.empty or "org_id" not in df_fiscal.columns:
        return []

    present = {
        str(x).strip()
        for x in df_fiscal["org_id"].dropna().astype(str).tolist()
        if str(x).strip()
    }
    if not present:
        return []

    if org_ids_filtro is not None:
        filt = {str(x).strip() for x in org_ids_filtro if str(x).strip()}
        if filt:
            present = present & filt

    out: list[str] = []
    seen: set[str] = set()
    for oid in sorted(present):
        reg = get_regime_tributario_por_empresa(params, oid)
        if _regime_eh_lucro_presumido(reg) and oid not in seen:
            seen.add(oid)
            out.append(oid)
    return out


def _calcular_lp_para_empresa(
    *,
    org_id: str,
    df_fiscal: pd.DataFrame,
    df_devolucoes: pd.DataFrame | None,
    periodo_inicio: pd.Timestamp,
    periodo_fim: pd.Timestamp,
    json_params_path: Path,
    receita_anual_estimada: float | None,
) -> tuple[LucroPresumidoBreakdown | None, bool]:
    """
    Returns:
        (breakdown, sem_params) — ``sem_params`` True quando não há params LP para a empresa.
    """
    lp, icms = load_lucro_presumido_params_from_json(json_params_path, org_id)
    if lp is None or icms is None:
        return None, True
    try:
        bd = calcular_lucro_presumido(
            df_fiscal,
            df_devolucoes,
            org_id=org_id,
            nf_d_ini=periodo_inicio,
            nf_d_fim=periodo_fim,
            receita_anual_estimada=receita_anual_estimada,
            params=lp,
            icms_params=icms,
        )
        return bd, False
    except Exception as exc:
        logger.warning(
            "imposto_consolidado: falha ao calcular LP para org_id=%s: %s",
            org_id,
            exc,
            exc_info=True,
        )
        return None, False


def calcular_imposto_total_painel_fiscal(
    *,
    df_fiscal: pd.DataFrame,
    df_devolucoes: pd.DataFrame | None,
    org_ids_filtro: Sequence[str] | None,
    periodo_inicio: pd.Timestamp,
    periodo_fim: pd.Timestamp,
    imposto_simples_ponte: float,
    json_params_path: Path,
    receita_anual_estimada_por_empresa: Mapping[str, float] | None = None,
) -> ImpostoConsolidado:
    """
    Consolida imposto fiscal de todas as empresas no recorte selecionado.

    Args:
        df_fiscal: DataFrame fiscal materializado (schema v3), grão linha (como o motor LP).
        df_devolucoes: DataFrame de devoluções ou ``None``.
        org_ids_filtro: ``org_id`` selecionados na UI; ``None`` = todas as empresas presentes no fiscal.
        periodo_inicio, periodo_fim: limites do recorte (``pd.Timestamp``).
        imposto_simples_ponte: imposto já calculado pela ponte SN / comercial (entrada).
        json_params_path: caminho do JSON de params do cliente.
        receita_anual_estimada_por_empresa: opcional ``org_id`` → receita anual para LC 224.

    Returns:
        ``ImpostoConsolidado`` com totais e breakdown LP por empresa.
    """
    ponte = float(imposto_simples_ponte)
    if not json_params_path.is_file():
        return ImpostoConsolidado(
            imposto_simples_ponte=ponte,
            imposto_lucro_presumido=0.0,
            imposto_total=ponte,
            breakdown_lp_por_empresa=MappingProxyType({}),
            empresas_lp_calculadas=(),
            empresas_lp_sem_params=(),
        )

    lp_ids = _identificar_empresas_lp_no_recorte(df_fiscal, org_ids_filtro, json_params_path)
    breakdown: dict[str, LucroPresumidoBreakdown] = {}
    sem_params: list[str] = []
    total_lp = 0.0

    ra_map = receita_anual_estimada_por_empresa or {}

    for oid in lp_ids:
        ra = ra_map.get(oid)
        ra_f = float(ra) if ra is not None else None
        bd, no_par = _calcular_lp_para_empresa(
            org_id=oid,
            df_fiscal=df_fiscal,
            df_devolucoes=df_devolucoes,
            periodo_inicio=periodo_inicio,
            periodo_fim=periodo_fim,
            json_params_path=json_params_path,
            receita_anual_estimada=ra_f,
        )
        if no_par:
            sem_params.append(oid)
            continue
        if bd is None:
            continue
        breakdown[oid] = bd
        total_lp += float(bd.total_imposto)

    total = ponte + total_lp
    return ImpostoConsolidado(
        imposto_simples_ponte=ponte,
        imposto_lucro_presumido=total_lp,
        imposto_total=total,
        breakdown_lp_por_empresa=MappingProxyType(dict(breakdown)),
        empresas_lp_calculadas=tuple(sorted(breakdown.keys())),
        empresas_lp_sem_params=tuple(sorted(set(sem_params))),
    )
