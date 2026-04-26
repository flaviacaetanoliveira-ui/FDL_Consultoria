"""
Cálculo da alíquota efetiva do Simples Nacional conforme LC 123/2006, art. 18.
Escopo atual: Anexo I (comércio). Outros anexos são trabalho futuro.

Tabelas de alíquotas nominais e parcelas a deduzir: LC 155/2016 (Anexo I).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Mapping, Optional

import pandas as pd

from faturamento_dre_recorte import (
    _fdl_fr_etiquetas_empresa_recorte,
    _fdl_fr_filtrar_por_etiquetas_empresa,
    _fdl_fr_mask_nf_emissao_no_periodo,
    _fdl_fr_ts_nf_emissao_para_dia_civil,
)
from faturamento_dre_recorte_minimo import _nf_fiscal_situacao_invalida
from processing.faturamento.params import FaturamentoParams, FaturamentoParamsV2
from processing.faturamento.params_regime import (
    find_empresa_faturamento_entry,
    get_aliquota_imposto_por_empresa,
    get_regime_tributario_por_empresa,
)

# Tabela oficial Anexo I (LC 155/2016)
# Cada tupla: (rbt12_min, rbt12_max, aliquota_nominal_pct, parcela_deduzir)
TABELA_ANEXO_I = (
    (0.00, 180_000.00, 4.00, 0.00),
    (180_000.01, 360_000.00, 7.30, 5_940.00),
    (360_000.01, 720_000.00, 9.50, 13_860.00),
    (720_000.01, 1_800_000.00, 10.70, 22_500.00),
    (1_800_000.01, 3_600_000.00, 14.30, 87_300.00),
    (3_600_000.01, 4_800_000.00, 19.00, 378_000.00),
)

SUBLIMITE_RBT12_SIMPLES = 4_800_000.00

_MOTIVO_SUBLIMITE = "RBT12 excede sublimite Simples Nacional"
_MOTIVO_HISTORICO = "Histórico inferior a 12 meses para RBT12"
_MOTIVO_SEM_FAIXA = "RBT12 fora das faixas do Anexo I (comércio)"
_MOTIVO_LP = "Lucro Presumido — cálculo específico em desenvolvimento"


@dataclass(frozen=True)
class ResultadoFaixaSimples:
    faixa_numero: int  # 1 a 6
    rbt12_min: float
    rbt12_max: float
    aliquota_nominal_pct: float
    parcela_deduzir: float


@dataclass(frozen=True)
class ResultadoAliquotaEfetivaMes:
    """Resultado completo de um cálculo de alíquota efetiva para um mês/empresa."""

    empresa_slug: str
    competencia: date  # primeiro dia do mês
    rbt12: float
    faixa: Optional[ResultadoFaixaSimples]
    aliquota_efetiva_pct: Optional[float]
    rbt12_suficiente: bool  # False quando faltam meses de histórico
    meses_historico_disponiveis: int
    motivo_indisponivel: Optional[str]  # explicação se efetiva não pôde ser calculada


def _month_start(d: date) -> date:
    return date(int(d.year), int(d.month), 1)


def _add_months_first_day(d: date, delta: int) -> date:
    y, m = d.year, d.month + delta
    while m > 12:
        m -= 12
        y += 1
    while m < 1:
        m += 12
        y -= 1
    return date(y, m, 1)


def texto_periodo_rbt12(competencia: date) -> str:
    """Intervalo de 12 meses (anterior à competência) para legenda na UI."""
    meses = _rbt12_janela_meses(competencia)
    if not meses:
        return "—"
    a, b = meses[0], meses[-1]
    return f"{a.strftime('%m/%Y')} a {b.strftime('%m/%Y')}"


def _rbt12_janela_meses(competencia: date) -> list[date]:
    """
    12 meses imediatamente anteriores à competência (exclusiva da competência).

    Ex.: competência abril/2026 → abr/2025 … mar/2026.
    """
    c0 = _month_start(competencia)
    fim = _add_months_first_day(c0, -1)
    inicio = _add_months_first_day(c0, -12)
    out: list[date] = []
    cur = inicio
    while cur <= fim:
        out.append(cur)
        cur = _add_months_first_day(cur, 1)
    return out


def identificar_faixa_anexo_i(rbt12: float) -> Optional[ResultadoFaixaSimples]:
    """Retorna a faixa do Anexo I correspondente ao RBT12.

    Retorna None se RBT12 > R$ 4.800.000,00 (sublimite do Simples Nacional para o Anexo I).
    """
    if rbt12 != rbt12 or rbt12 < 0:
        return None
    if rbt12 > SUBLIMITE_RBT12_SIMPLES + 1e-9:
        return None
    for idx, (lo, hi, nom, parc) in enumerate(TABELA_ANEXO_I, start=1):
        if rbt12 + 1e-9 >= lo and rbt12 <= hi + 1e-9:
            return ResultadoFaixaSimples(
                faixa_numero=idx,
                rbt12_min=float(lo),
                rbt12_max=float(hi),
                aliquota_nominal_pct=float(nom),
                parcela_deduzir=float(parc),
            )
    return None


def calcular_aliquota_efetiva_formula(rbt12: float, faixa: ResultadoFaixaSimples) -> float:
    """
    Aplica fórmula oficial LC 123/2006 art. 18 §1º:
    Alíq. efetiva = (RBT12 × nominal − dedução) / RBT12

    Retorna em pontos percentuais (ex: 8.47 para 8,47%).
    Precisão interna em float64; retorno arredondado a 2 casas decimais (exibição).
    """
    if rbt12 <= 1e-12:
        return 0.0
    nom = faixa.aliquota_nominal_pct / 100.0
    parcela = float(faixa.parcela_deduzir)
    numerador = float(rbt12) * nom - parcela
    efetiva = (numerador / float(rbt12)) * 100.0
    return round(efetiva + 1e-12, 2)


def calcular_rbt12_para_competencia(
    historico_receita_mensal: dict[date, float],
    competencia: date,
) -> tuple[float, int]:
    """
    Calcula RBT12: soma da receita dos 12 meses ANTERIORES à competência.

    Para competência abril/2026, soma abr/2025 a mar/2026 (12 meses).

    Retorna: (rbt12, meses_disponiveis_no_janela) — meses com chave presente no dicionário.
    """
    janela = _rbt12_janela_meses(competencia)
    if not janela:
        return 0.0, 0
    meses = 0
    soma = 0.0
    for m in janela:
        if m in historico_receita_mensal:
            meses += 1
            soma += float(historico_receita_mensal[m])
    return float(soma), int(meses)


def calcular_aliquota_efetiva_mes(
    empresa_slug: str,
    competencia: date,
    historico_receita_mensal: dict[date, float],
) -> ResultadoAliquotaEfetivaMes:
    """
    Função orquestradora. Para cada empresa/mês:
    1. Calcula RBT12
    2. Verifica suficiência (12 meses disponíveis)
    3. Identifica faixa
    4. Aplica fórmula
    5. Retorna ResultadoAliquotaEfetivaMes completo
    """
    c0 = _month_start(competencia)
    rbt12, meses_disp = calcular_rbt12_para_competencia(historico_receita_mensal, c0)
    if meses_disp < 12:
        return ResultadoAliquotaEfetivaMes(
            empresa_slug=empresa_slug,
            competencia=c0,
            rbt12=float(rbt12),
            faixa=None,
            aliquota_efetiva_pct=None,
            rbt12_suficiente=False,
            meses_historico_disponiveis=meses_disp,
            motivo_indisponivel=_MOTIVO_HISTORICO,
        )
    if rbt12 > SUBLIMITE_RBT12_SIMPLES + 1e-9:
        return ResultadoAliquotaEfetivaMes(
            empresa_slug=empresa_slug,
            competencia=c0,
            rbt12=float(rbt12),
            faixa=None,
            aliquota_efetiva_pct=None,
            rbt12_suficiente=True,
            meses_historico_disponiveis=meses_disp,
            motivo_indisponivel=_MOTIVO_SUBLIMITE,
        )
    faixa = identificar_faixa_anexo_i(rbt12)
    if faixa is None:
        return ResultadoAliquotaEfetivaMes(
            empresa_slug=empresa_slug,
            competencia=c0,
            rbt12=float(rbt12),
            faixa=None,
            aliquota_efetiva_pct=None,
            rbt12_suficiente=True,
            meses_historico_disponiveis=meses_disp,
            motivo_indisponivel=_MOTIVO_SEM_FAIXA,
        )
    efetiva = calcular_aliquota_efetiva_formula(rbt12, faixa)
    return ResultadoAliquotaEfetivaMes(
        empresa_slug=empresa_slug,
        competencia=c0,
        rbt12=float(rbt12),
        faixa=faixa,
        aliquota_efetiva_pct=float(efetiva),
        rbt12_suficiente=True,
        meses_historico_disponiveis=meses_disp,
        motivo_indisponivel=None,
    )


def _resolve_coluna_empresa(df: pd.DataFrame, coluna_empresa: str) -> str:
    # empresa_slug só com NaN/vazio (p.ex. após concat) → usar org_id para não esvaziar o histórico.
    if coluna_empresa == "empresa_slug" and "org_id" in df.columns:
        if coluna_empresa not in df.columns:
            return "org_id"
        s = df[coluna_empresa]
        if s.notna().any() and (s.dropna().astype(str).str.strip() != "").any():
            return coluna_empresa
        return "org_id"
    if coluna_empresa in df.columns:
        return coluna_empresa
    if coluna_empresa == "empresa_slug" and "org_id" in df.columns:
        return "org_id"
    raise KeyError(coluna_empresa)


def extrair_historico_receita_mensal_por_empresa(
    df_nfs: pd.DataFrame,
    coluna_data: str = "Nota_Data_Emissao",
    coluna_empresa: str = "empresa_slug",
    coluna_valor: str = "Valor_Liquido_NF",
    coluna_situacao: str = "Nota_Situacao",
) -> dict[str, dict[date, float]]:
    """
    Agrega receita bruta por mês × empresa.

    Filtros aplicados:
    - Exclui situações inválidas (Cancelada, Denegada, Inutilizada)
    - Considera apenas NFs com data de emissão válida
    - Agrupa por primeiro dia do mês (competência)

    Retorna:
        { "gama_home": { date(2025, 1, 1): 78000.0, ... }, ... }
    """
    if df_nfs.empty:
        return {}
    need = {coluna_data, coluna_valor}
    emp_key = _resolve_coluna_empresa(df_nfs, coluna_empresa)
    need.add(emp_key)
    if not need.issubset(df_nfs.columns):
        return {}

    d = df_nfs.copy()
    if coluna_situacao in d.columns:
        d = d.loc[~_nf_fiscal_situacao_invalida(d[coluna_situacao])].copy()
    if d.empty:
        return {}

    ts = _fdl_fr_ts_nf_emissao_para_dia_civil(d[coluna_data])
    d = d.loc[ts.notna()].copy()
    if d.empty:
        return {}

    d["_per_m"] = ts.dt.to_period("M")
    vl = pd.to_numeric(d[coluna_valor], errors="coerce").fillna(0.0)
    d["_vl"] = vl
    d["_org"] = d[emp_key].fillna("").astype(str).str.strip()

    out: dict[str, dict[date, float]] = {}
    for org, gr in d.groupby("_org", sort=False):
        k = str(org).strip()
        if not k:
            continue
        acc: dict[date, float] = {}
        for per, g2 in gr.groupby("_per_m", sort=True):
            c0 = date(int(per.year), int(per.month), 1)
            acc[c0] = float(g2["_vl"].sum())
        out[k] = acc
    return out


def _regime_para_org(
    params_regime: FaturamentoParams | FaturamentoParamsV2 | Mapping[str, Any] | None,
    org_id: str,
) -> str:
    if params_regime is None:
        return "simples_nacional"
    if isinstance(params_regime, (FaturamentoParams, FaturamentoParamsV2)):
        r = get_regime_tributario_por_empresa(params_regime, org_id)
        if r and str(r).strip():
            return str(r).strip()
        return "simples_nacional"
    if isinstance(params_regime, Mapping):
        raw = params_regime.get(org_id)
        if isinstance(raw, str) and raw.strip():
            return str(raw).strip()
        if isinstance(raw, Mapping):
            rr = raw.get("regime")
            if isinstance(rr, str) and rr.strip():
                return rr.strip()
    return "simples_nacional"


def _aliquota_referencia_json_pct(
    params_regime: FaturamentoParams | FaturamentoParamsV2 | Mapping[str, Any] | None,
    org_id: str,
) -> float:
    """Alíquota de referência em pontos percentuais (0–100) a partir do JSON / params."""
    if params_regime is None:
        return 0.0
    if isinstance(params_regime, (FaturamentoParams, FaturamentoParamsV2)):
        r = get_aliquota_imposto_por_empresa(params_regime, org_id)
        if r is None:
            return 0.0
        x = float(r)
        return x * 100.0 if x <= 1.0 else x
    if isinstance(params_regime, Mapping):
        raw = params_regime.get(org_id)
        if isinstance(raw, Mapping):
            aq = raw.get("aliquota_imposto")
            if isinstance(aq, (int, float)):
                x = float(aq)
                return x * 100.0 if x <= 1.0 else x
    return 0.0


def _aliquota_pct_aplicada_imposto_mes(res_m: ResultadoAliquotaEfetivaMes, json_pct: float) -> float:
    """Alíquota (em %) aplicada ao faturamento bruto do mês para efeito de imposto estimado no painel."""
    if res_m.rbt12_suficiente and res_m.aliquota_efetiva_pct is not None:
        return float(res_m.aliquota_efetiva_pct)
    if not res_m.rbt12_suficiente:
        return float(json_pct)
    return 0.0


def _nome_para_org(
    params_regime: FaturamentoParams | FaturamentoParamsV2 | Mapping[str, Any] | None,
    org_id: str,
) -> str:
    if params_regime is None:
        return org_id
    if isinstance(params_regime, FaturamentoParamsV2):
        ent = find_empresa_faturamento_entry(params_regime, org_id)
        if ent is not None:
            return ent.empresa
        return org_id
    if isinstance(params_regime, Mapping):
        raw = params_regime.get(org_id)
        if isinstance(raw, Mapping):
            n = raw.get("empresa_nome") or raw.get("nome")
            if isinstance(n, str) and n.strip():
                return n.strip()
    return org_id


def _iter_meses_no_periodo(periodo_inicio: date, periodo_fim: date) -> list[date]:
    a = _month_start(periodo_inicio)
    b = _month_start(periodo_fim)
    if b < a:
        return []
    out: list[date] = []
    cur = a
    while cur <= b:
        out.append(cur)
        cur = _add_months_first_day(cur, 1)
    return out


def _receita_bruta_mes_empresa(
    df_fiscal_hist: pd.DataFrame,
    org_id: str,
    competencia: date,
) -> float:
    if df_fiscal_hist.empty:
        return 0.0
    need = {"Nota_Data_Emissao", "Valor_Liquido_NF"}
    if not need.issubset(df_fiscal_hist.columns):
        return 0.0
    key = "org_id" if "org_id" in df_fiscal_hist.columns else _resolve_coluna_empresa(df_fiscal_hist, "empresa_slug")
    d = df_fiscal_hist.loc[df_fiscal_hist[key].astype(str).str.strip() == str(org_id).strip()].copy()
    if d.empty:
        return 0.0
    if "Nota_Situacao" in d.columns:
        d = d.loc[~_nf_fiscal_situacao_invalida(d["Nota_Situacao"])].copy()
    if d.empty:
        return 0.0
    ts = _fdl_fr_ts_nf_emissao_para_dia_civil(d["Nota_Data_Emissao"])
    d = d.loc[ts.notna()].copy()
    if d.empty:
        return 0.0
    c0 = _month_start(competencia)
    mes_ref = ts.dt.to_period("M").apply(lambda p: date(int(p.year), int(p.month), 1))
    sub = d.loc[mes_ref == c0]
    if sub.empty:
        return 0.0
    return float(pd.to_numeric(sub["Valor_Liquido_NF"], errors="coerce").fillna(0.0).sum())


def _devolucoes_empresa_periodo(
    df_devolucoes: pd.DataFrame | None,
    *,
    org_id: str,
    periodo_inicio: date,
    periodo_fim: date,
    ok_nf_dates: bool,
) -> float:
    if df_devolucoes is None or df_devolucoes.empty or not ok_nf_dates or periodo_fim < periodo_inicio:
        return 0.0
    need = {"Nota_Data_Emissao", "Valor_Liquido_Devolucao"}
    if not need.issubset(df_devolucoes.columns):
        return 0.0
    key = "org_id" if "org_id" in df_devolucoes.columns else "empresa"
    if key not in df_devolucoes.columns:
        return 0.0
    d = df_devolucoes.loc[df_devolucoes[key].astype(str).str.strip() == str(org_id).strip()].copy()
    if d.empty:
        return 0.0
    m_period = _fdl_fr_mask_nf_emissao_no_periodo(d["Nota_Data_Emissao"], periodo_inicio, periodo_fim)
    d = d.loc[m_period]
    if d.empty:
        return 0.0
    return float(pd.to_numeric(d["Valor_Liquido_Devolucao"], errors="coerce").fillna(0.0).sum())


def _faturamento_bruto_empresa_periodo(
    df_fiscal_base_periodo: pd.DataFrame,
    org_id: str,
) -> float:
    if df_fiscal_base_periodo.empty:
        return 0.0
    key = "org_id" if "org_id" in df_fiscal_base_periodo.columns else "empresa"
    if key not in df_fiscal_base_periodo.columns or "Valor_Liquido_NF" not in df_fiscal_base_periodo.columns:
        return 0.0
    sub = df_fiscal_base_periodo.loc[df_fiscal_base_periodo[key].astype(str).str.strip() == str(org_id).strip()]
    if sub.empty:
        return 0.0
    return float(pd.to_numeric(sub["Valor_Liquido_NF"], errors="coerce").fillna(0.0).sum())


def _ultima_competencia_com_nf_no_periodo(
    historico_global: dict[str, dict[date, float]],
    org_ids: list[str],
    periodo_inicio: date,
    periodo_fim: date,
) -> date:
    meses = _iter_meses_no_periodo(periodo_inicio, periodo_fim)
    if not meses:
        return _month_start(periodo_fim)
    for m in reversed(meses):
        for oid in org_ids:
            h = historico_global.get(oid, {})
            if h.get(m, 0.0) > 1e-9:
                return m
    return meses[-1]


def agregar_simples_nacional_para_painel_fiscal(
    df_fiscal_base: pd.DataFrame,
    empresas_slugs: list[str],
    params_regime: FaturamentoParams | FaturamentoParamsV2 | Mapping[str, Any] | None,
    periodo_inicio: date,
    periodo_fim: date,
    *,
    df_fiscal_full: pd.DataFrame | None = None,
    df_devolucoes: pd.DataFrame | None = None,
    ok_nf_dates: bool = True,
) -> dict[str, Any]:
    """
    Produz dicionário consumido diretamente pelo painel Fiscal.

    ``df_fiscal_base``: recorte do período (uma linha por NF após agregação), usado para base líquida.
    ``df_fiscal_full``: dataset fiscal completo para RBT12; quando None, usa ``df_fiscal_base`` (pode subestimar RBT12).

    O histórico mensal (RBT12) agrega ``df_fiscal_full`` e ``df_fiscal_base`` para incluir o recorte do período
    quando ainda não materializado no conjunto «full» (em produção o full costuma ser superset).
    """
    df_full = df_fiscal_full if df_fiscal_full is not None and not df_fiscal_full.empty else pd.DataFrame()
    parts: list[pd.DataFrame] = []
    if not df_full.empty:
        parts.append(df_full)
    if df_fiscal_base is not None and not df_fiscal_base.empty:
        parts.append(df_fiscal_base)
    df_hist_src = pd.concat(parts, ignore_index=True) if len(parts) > 1 else (parts[0] if parts else pd.DataFrame())
    if df_hist_src.empty:
        df_hist_src = df_fiscal_base if df_fiscal_base is not None else pd.DataFrame()

    historico_global = extrair_historico_receita_mensal_por_empresa(
        df_hist_src,
        coluna_empresa="empresa_slug",
        coluna_valor="Valor_Liquido_NF",
    )

    org_ids = [str(x).strip() for x in empresas_slugs if str(x).strip()]
    competencia_ref = _ultima_competencia_com_nf_no_periodo(historico_global, org_ids, periodo_inicio, periodo_fim)

    por_empresa: dict[str, Any] = {}
    aliquotas_mensais_por_empresa: dict[str, dict[str, float]] = {}
    tem_fora = False
    base_simples_total = 0.0
    imp_simples_total = 0.0

    for oid in org_ids:
        regime = _regime_para_org(params_regime, oid)
        nome = _nome_para_org(params_regime, oid)
        base_bruta = _faturamento_bruto_empresa_periodo(df_fiscal_base, oid)
        dev = _devolucoes_empresa_periodo(
            df_devolucoes,
            org_id=oid,
            periodo_inicio=periodo_inicio,
            periodo_fim=periodo_fim,
            ok_nf_dates=ok_nf_dates,
        )
        base_liquida = max(0.0, base_bruta - dev)

        if regime != "simples_nacional":
            tem_fora = True
            json_ref_lp = _aliquota_referencia_json_pct(params_regime, oid)
            por_empresa[oid] = {
                "empresa_nome": nome,
                "regime": regime,
                "ultimo_mes": None,
                "historico_mensal_no_periodo": [],
                "base_liquida_periodo": float(base_liquida),
                "imposto_calculado_periodo": None,
                "aliquota_media_periodo_pct": None,
                "aliquota_efetiva_calculada_pct": None,
                "aliquota_referencia_json_pct": float(json_ref_lp) if json_ref_lp > 1e-12 else None,
                "aliquota_efetiva_ponderada_periodo_pct": None,
                "origem_aliquota": "fora_escopo",
                "meses_historico_disponiveis": None,
                "motivo_fallback": None,
                "motivo_fora_escopo": _MOTIVO_LP if regime == "lucro_presumido" else "Regime fora do escopo Simples Nacional neste painel",
            }
            continue

        hist_emp = dict(historico_global.get(oid, {}))
        meses_periodo = _iter_meses_no_periodo(periodo_inicio, periodo_fim)
        historico_mensal: list[ResultadoAliquotaEfetivaMes] = [
            calcular_aliquota_efetiva_mes(oid, m, hist_emp) for m in meses_periodo
        ]
        ultimo = calcular_aliquota_efetiva_mes(oid, competencia_ref, hist_emp)
        json_ref = _aliquota_referencia_json_pct(params_regime, oid)

        al_m: dict[str, float] = {}
        for m, res_m in zip(meses_periodo, historico_mensal, strict=True):
            pct_apl = _aliquota_pct_aplicada_imposto_mes(res_m, json_ref)
            al_m[f"{int(m.year):04d}-{int(m.month):02d}"] = float(pct_apl) / 100.0
        aliquotas_mensais_por_empresa[oid] = al_m

        imposto_periodo = 0.0
        # Imposto do período: receita mensal só do recorte (evita duplicar full+base no concat usado no RBT12).
        df_bruta_mes = (
            df_fiscal_base if df_fiscal_base is not None and not df_fiscal_base.empty else df_hist_src
        )
        for m, res_m in zip(meses_periodo, historico_mensal, strict=True):
            bruta_m = _receita_bruta_mes_empresa(df_bruta_mes, oid, m)
            aliq_apl = _aliquota_pct_aplicada_imposto_mes(res_m, json_ref)
            imposto_periodo += bruta_m * (aliq_apl / 100.0)

        ali_pond = (imposto_periodo / base_liquida * 100.0) if base_liquida > 1e-9 else None
        aliq_calc_ref = ultimo.aliquota_efetiva_pct if ultimo.rbt12_suficiente else None

        if ultimo.rbt12_suficiente and ultimo.aliquota_efetiva_pct is not None:
            origem = "calculada"
            motivo_fb = None
        elif not ultimo.rbt12_suficiente:
            origem = "referencia_json"
            motivo_fb = f"Histórico fiscal incompleto: {ultimo.meses_historico_disponiveis} de 12 meses"
        else:
            origem = "calculada"
            motivo_fb = ultimo.motivo_indisponivel

        por_empresa[oid] = {
            "empresa_nome": nome,
            "regime": regime,
            "ultimo_mes": ultimo,
            "historico_mensal_no_periodo": historico_mensal,
            "base_liquida_periodo": float(base_liquida),
            "imposto_calculado_periodo": float(imposto_periodo),
            "aliquota_media_periodo_pct": float(ali_pond) if ali_pond is not None else None,
            "aliquota_efetiva_calculada_pct": float(aliq_calc_ref) if aliq_calc_ref is not None else None,
            "aliquota_referencia_json_pct": float(json_ref),
            "aliquota_efetiva_ponderada_periodo_pct": float(ali_pond) if ali_pond is not None else None,
            "origem_aliquota": origem,
            "meses_historico_disponiveis": int(ultimo.meses_historico_disponiveis),
            "motivo_fallback": motivo_fb,
            "motivo_fora_escopo": None,
        }
        base_simples_total += float(base_liquida)
        imp_simples_total += float(imposto_periodo)

    ali_pond = (imp_simples_total / base_simples_total * 100.0) if base_simples_total > 1e-9 else None

    empresas_em_warmup: list[str] = []
    empresas_com_calculo_oficial: list[str] = []
    for oid, row in por_empresa.items():
        if not isinstance(row, dict) or row.get("regime") != "simples_nacional":
            continue
        oa = row.get("origem_aliquota")
        if oa == "referencia_json":
            empresas_em_warmup.append(oid)
        elif oa == "calculada" and row.get("aliquota_efetiva_calculada_pct") is not None:
            empresas_com_calculo_oficial.append(oid)

    return {
        "competencia_referencia": competencia_ref,
        "por_empresa": por_empresa,
        "aliquotas_mensais_por_empresa": aliquotas_mensais_por_empresa,
        "total_simples": {
            "base_liquida": float(base_simples_total),
            "imposto_total": float(imp_simples_total),
            "aliquota_media_ponderada_pct": float(ali_pond) if ali_pond is not None else None,
        },
        "tem_empresa_fora_escopo": tem_fora,
        "empresas_em_warmup": empresas_em_warmup,
        "empresas_com_calculo_oficial": empresas_com_calculo_oficial,
        "empresa_com_calculo_oficial": empresas_com_calculo_oficial,
    }
