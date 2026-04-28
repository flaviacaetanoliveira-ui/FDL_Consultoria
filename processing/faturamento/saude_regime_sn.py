"""
Indicador Saúde do Regime — Simples Nacional.

Calcula RBT12 (Receita Bruta dos últimos 12 meses) por empresa SN
e classifica em faixas de proximidade do limite legal de R$ 4.800.000.

Reusa ``calcular_rbt12_para_competencia`` de ``simples_nacional`` para
manter coerência com o cálculo de alíquota efetiva.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping

import pandas as pd

from processing.faturamento.params import FaturamentoParams, FaturamentoParamsV2
from processing.faturamento.params_regime import find_empresa_faturamento_entry
from processing.faturamento.simples_nacional import (
    SUBLIMITE_RBT12_SIMPLES,
    _nome_para_org,
    _rbt12_janela_meses,
    calcular_rbt12_para_competencia,
    extrair_historico_receita_mensal_por_empresa,
)

LIMITE_SIMPLES_NACIONAL = SUBLIMITE_RBT12_SIMPLES

LIMITE_TRANQUILO = 0.70
LIMITE_ATENCAO = 0.85
LIMITE_CRITICO = 1.00


@dataclass(frozen=True)
class SaudeRegimeEmpresa:
    """Status de saúde do regime SN para uma empresa em uma competência."""

    org_id: str
    nome_empresa: str
    competencia: str
    janela_rbt12_inicio: str
    janela_rbt12_fim: str
    rbt12: float
    limite: float
    percentual_limite: float
    faixa: str
    rbt12_suficiente: bool
    meses_disponiveis: int
    valor_disponivel_ate_limite: float


def classificar_faixa(percentual: float) -> str:
    """Classifica em uma das 4 faixas baseado em fração do limite (0–1+, ex.: 0.75 = 75%)."""
    if percentual < LIMITE_TRANQUILO:
        return "TRANQUILO"
    if percentual < LIMITE_ATENCAO:
        return "ATENCAO"
    if percentual < LIMITE_CRITICO:
        return "CRITICO"
    return "EXCEDIDO"


def _montar_df_historico_rbt12(
    df_fiscal: pd.DataFrame,
    df_fiscal_recorte_periodo: pd.DataFrame | None,
) -> pd.DataFrame:
    """Espelha a montagem de ``df_hist_src`` em ``agregar_simples_nacional_para_painel_fiscal``."""
    df_full = df_fiscal if df_fiscal is not None and not df_fiscal.empty else pd.DataFrame()
    parts: list[pd.DataFrame] = []
    if not df_full.empty:
        parts.append(df_full)
    if df_fiscal_recorte_periodo is not None and not df_fiscal_recorte_periodo.empty:
        parts.append(df_fiscal_recorte_periodo)
    if len(parts) > 1:
        return pd.concat(parts, ignore_index=True)
    return parts[0] if parts else pd.DataFrame()


def _competencia_para_date(competencia: pd.Timestamp) -> date:
    ts = pd.Timestamp(competencia)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(None)
    return date(int(ts.year), int(ts.month), 1)


def _period_label(d: date) -> str:
    return f"{d.year:04d}-{d.month:02d}"


def calcular_saude_regime_sn(
    df_fiscal: pd.DataFrame,
    org_ids_sn: list[str],
    params_regime: FaturamentoParams | FaturamentoParamsV2 | Mapping[str, Any] | None,
    competencia: pd.Timestamp,
    *,
    df_fiscal_recorte_periodo: pd.DataFrame | None = None,
) -> list[SaudeRegimeEmpresa]:
    """
    Calcula saúde do regime SN para cada ``org_id`` listado (já filtrado como SN pelo chamador).

    A janela RBT12 é a mesma de ``calcular_rbt12_para_competencia``: 12 meses anteriores à
    competência (exclui o mês da competência), LC 123/2006.

    ``df_fiscal`` deve ser o conjunto «full» usado no painel; opcionalmente passe também
    ``df_fiscal_recorte_periodo`` (recorte do período), como no agregador SN, para não subestimar o histórico.
    """
    c0 = _competencia_para_date(competencia)
    limite = float(LIMITE_SIMPLES_NACIONAL)
    df_hist = _montar_df_historico_rbt12(df_fiscal, df_fiscal_recorte_periodo)
    historico_global = extrair_historico_receita_mensal_por_empresa(
        df_hist,
        coluna_empresa="empresa_slug",
        coluna_valor="Valor_Liquido_NF",
    )

    janela_meses = _rbt12_janela_meses(c0)
    if janela_meses:
        ini_lab = _period_label(janela_meses[0])
        fim_lab = _period_label(janela_meses[-1])
    else:
        ini_lab = fim_lab = "—"

    out: list[SaudeRegimeEmpresa] = []
    for oid in org_ids_sn:
        oid_s = str(oid).strip()
        if not oid_s:
            continue
        nome = _nome_para_org(params_regime, oid_s)
        hist_emp = dict(historico_global.get(oid_s, {}))
        rbt12, meses_disp = calcular_rbt12_para_competencia(hist_emp, c0)
        rbt12_suficiente = meses_disp >= 12
        pct = (float(rbt12) / limite) if limite > 1e-12 else 0.0
        faixa = classificar_faixa(pct)
        margem = max(0.0, limite - float(rbt12))

        out.append(
            SaudeRegimeEmpresa(
                org_id=oid_s,
                nome_empresa=nome,
                competencia=_period_label(c0),
                janela_rbt12_inicio=ini_lab,
                janela_rbt12_fim=fim_lab,
                rbt12=float(rbt12),
                limite=limite,
                percentual_limite=float(pct),
                faixa=faixa,
                rbt12_suficiente=rbt12_suficiente,
                meses_disponiveis=int(meses_disp),
                valor_disponivel_ate_limite=float(margem),
            )
        )
    return out


def org_ids_simples_nacional_do_params(
    params: FaturamentoParams | FaturamentoParamsV2 | None,
    org_ids_candidatos: list[str],
) -> list[str]:
    """Filtra ``org_ids_candidatos`` mantendo apenas empresas com regime ``simples_nacional`` no JSON."""
    if params is None or isinstance(params, FaturamentoParams):
        return []
    sel: list[str] = []
    for oid in org_ids_candidatos:
        ent = find_empresa_faturamento_entry(params, str(oid).strip())
        if ent is None:
            continue
        r = (ent.regime_tributario or "").strip()
        if r == "simples_nacional":
            sel.append(str(oid).strip())
    return sel
