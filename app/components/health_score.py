"""
Score de saude financeira e diagnosticos (Faturamento / DRE).

Logica pura: pandas apenas; sem Streamlit.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional

import pandas as pd

from faturamento_dre_recorte_minimo import nf_grain_plataforma_match_key

from processing.faturamento.config import STATUS_CUSTO_OK


class HealthLevel(str, Enum):
    SAUDAVEL = "saudavel"
    ATENCAO = "atencao"
    RISCO = "risco"
    CRITICO = "critico"


_HEALTH_LEVEL_META: dict[HealthLevel, tuple[str, str, str]] = {
    HealthLevel.SAUDAVEL: ("Saudável", "#22c55e", "[+]"),
    HealthLevel.ATENCAO: ("Atenção", "#eab308", "[!]"),
    HealthLevel.RISCO: ("Risco", "#f97316", "[!!]"),
    HealthLevel.CRITICO: ("Crítico", "#ef4444", "[X]"),
}


def health_level_meta(level: HealthLevel) -> tuple[str, str, str]:
    return _HEALTH_LEVEL_META[level]


class AlertLevel(str, Enum):
    INFO = "info"
    MEDIUM = "medio"
    HIGH = "alto"
    CRITICAL = "critico"


_ALERT_META: dict[AlertLevel, tuple[str, str, str]] = {
    AlertLevel.INFO: ("info", "#3b82f6", "i"),
    AlertLevel.MEDIUM: ("medio", "#eab308", "!"),
    AlertLevel.HIGH: ("alto", "#f97316", "!!"),
    AlertLevel.CRITICAL: ("critico", "#ef4444", "!!!"),
}


def alert_level_meta(level: AlertLevel) -> tuple[str, str, str]:
    return _ALERT_META[level]


@dataclass
class Diagnostico:
    tipo: str
    nivel: AlertLevel
    titulo: str
    detalhe: str
    acao: Optional[str] = None
    valor: Optional[float] = None
    variacao: Optional[float] = None


@dataclass
class SKURisco:
    sku: str
    receita: float
    margem_pct: float
    custo_pct: float
    resultado: float
    quantidade: float
    preco_medio: float
    custo_unitario: float
    ajuste_breakeven: float
    ajuste_breakeven_pct: float


@dataclass
class HealthScore:
    score: int
    level: HealthLevel
    receita: float
    resultado: float
    margem_pct: float
    custo_pct: float
    margem_anterior: Optional[float] = None
    margem_grupo: Optional[float] = None
    tendencia_pp: Optional[float] = None
    vs_grupo_pp: Optional[float] = None
    diagnosticos: list[Diagnostico] = field(default_factory=list)
    skus_risco: list[SKURisco] = field(default_factory=list)
    periodo: str = ""
    empresa: str = ""
    total_skus: int = 0
    skus_margem_negativa: int = 0


def _num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(0.0)


def calcular_health_score(
    df: pd.DataFrame,
    org_id: str,
    ano: int,
    mes: int,
    df_anterior: Optional[pd.DataFrame] = None,
    df_grupo: Optional[pd.DataFrame] = None,
    config: Optional[dict[str, Any]] = None,
    *,
    periodo_override: Optional[str] = None,
) -> HealthScore:
    cfg: dict[str, Any] = {
        "benchmark_custo_pct": 50.0,
        "benchmark_margem_min": 5.0,
        "threshold_custo_alto": 60.0,
        "threshold_margem_critica": -10.0,
        "threshold_tendencia_alerta": -3.0,
    }
    if config:
        cfg.update(config)

    receita = float(_num(df["Vl_Venda"]).sum()) if "Vl_Venda" in df.columns else 0.0
    resultado = float(_num(df["Resultado"]).sum()) if "Resultado" in df.columns else 0.0
    margem_pct = (resultado / receita * 100.0) if receita > 0 else 0.0

    custo = float(_num(df["Custo_Produto_Total"]).sum()) if "Custo_Produto_Total" in df.columns else 0.0
    custo_pct = (custo / receita * 100.0) if receita > 0 else 0.0

    margem_anterior: Optional[float] = None
    tendencia_pp: Optional[float] = None
    if df_anterior is not None and len(df_anterior) > 0 and "Vl_Venda" in df_anterior.columns:
        ra = float(_num(df_anterior["Vl_Venda"]).sum())
        res_a = float(_num(df_anterior["Resultado"]).sum()) if "Resultado" in df_anterior.columns else 0.0
        if ra > 0:
            margem_anterior = res_a / ra * 100.0
            tendencia_pp = margem_pct - margem_anterior

    margem_grupo: Optional[float] = None
    vs_grupo_pp: Optional[float] = None
    oid = str(org_id).strip()
    skip_bench = oid.casefold() in {"consolidado", "_multi_", ""} or oid.startswith("_multi")
    if (
        not skip_bench
        and df_grupo is not None
        and len(df_grupo) > 0
        and "org_id" in df_grupo.columns
    ):
        outros = df_grupo.loc[df_grupo["org_id"].astype(str).str.strip() != oid].copy()
        if len(outros) > 0 and "Vl_Venda" in outros.columns:
            rg = float(_num(outros["Vl_Venda"]).sum())
            resg = float(_num(outros["Resultado"]).sum()) if "Resultado" in outros.columns else 0.0
            if rg > 0:
                margem_grupo = resg / rg * 100.0
                vs_grupo_pp = margem_pct - margem_grupo

    score = 50
    if margem_pct >= 10:
        score += 25
    elif margem_pct >= 5:
        score += 15
    elif margem_pct >= 0:
        score += 5
    elif margem_pct >= -5:
        score -= 10
    else:
        score -= 25

    if tendencia_pp is not None:
        if tendencia_pp > 2:
            score += 10
        elif tendencia_pp > 0:
            score += 5
        elif tendencia_pp > -3:
            score += 0
        else:
            score -= 10

    if custo_pct <= 45:
        score += 10
    elif custo_pct <= 55:
        score += 0
    elif custo_pct <= 65:
        score -= 5
    else:
        score -= 10

    if vs_grupo_pp is not None:
        if vs_grupo_pp > 3:
            score += 5
        elif vs_grupo_pp < -5:
            score -= 5

    score = max(0, min(100, int(round(score))))

    if score >= 80:
        level = HealthLevel.SAUDAVEL
    elif score >= 60:
        level = HealthLevel.ATENCAO
    elif score >= 40:
        level = HealthLevel.RISCO
    else:
        level = HealthLevel.CRITICO

    diagnosticos: list[Diagnostico] = []

    if resultado < 0:
        diagnosticos.append(
            Diagnostico(
                tipo="ALERTA",
                nivel=AlertLevel.CRITICAL,
                titulo="Resultado negativo no período",
                detalhe=f"Prejuízo de R$ {abs(resultado):,.2f} (margem {margem_pct:.1f}%)",
                acao="Analisar composição de custos e precificação",
                valor=resultado,
            )
        )

    if custo_pct > float(cfg["benchmark_custo_pct"]):
        excesso = custo_pct - float(cfg["benchmark_custo_pct"])
        diagnosticos.append(
            Diagnostico(
                tipo="CAUSA",
                nivel=AlertLevel.HIGH if custo_pct > 60 else AlertLevel.MEDIUM,
                titulo=f"Custo do produto elevado ({custo_pct:.1f}%)",
                detalhe=f"{excesso:.1f}pp acima do benchmark ({cfg['benchmark_custo_pct']:.0f}%)",
                acao="Rever precificação ou renegociar fornecedores",
                valor=custo_pct,
                variacao=excesso,
            )
        )

    if tendencia_pp is not None and tendencia_pp < float(cfg["threshold_tendencia_alerta"]):
        diagnosticos.append(
            Diagnostico(
                tipo="TENDENCIA",
                nivel=AlertLevel.HIGH,
                titulo=f"Margem em queda ({tendencia_pp:+.1f}pp)",
                detalhe=f"Margem passou de {margem_anterior:.1f}% para {margem_pct:.1f}%"
                if margem_anterior is not None
                else f"Variação de margem {tendencia_pp:+.1f} pp vs período anterior",
                acao="Investigar causas da deterioração",
                valor=margem_pct,
                variacao=tendencia_pp,
            )
        )

    if vs_grupo_pp is not None and vs_grupo_pp < -5:
        diagnosticos.append(
            Diagnostico(
                tipo="BENCHMARK",
                nivel=AlertLevel.MEDIUM,
                titulo=f"Abaixo da média do grupo ({vs_grupo_pp:+.1f} pp)",
                detalhe=f"Margem do grupo (outras orgs): {margem_grupo:.1f}%; recorte atual: {margem_pct:.1f}%",
                acao="Analisar práticas das empresas com melhor resultado",
                valor=vs_grupo_pp,
            )
        )

    tendencia_forte_queda = tendencia_pp is not None and tendencia_pp < float(cfg["threshold_tendencia_alerta"])
    abaixo_grupo = vs_grupo_pp is not None and vs_grupo_pp < -5
    if (
        margem_pct >= 10
        and resultado >= 0
        and not tendencia_forte_queda
        and not abaixo_grupo
    ):
        diagnosticos.append(
            Diagnostico(
                tipo="POSITIVO",
                nivel=AlertLevel.INFO,
                titulo=f"Margem saudável ({margem_pct:.1f}%)",
                detalhe="Operação com boa rentabilidade no recorte",
                valor=margem_pct,
            )
        )

    skus_risco: list[SKURisco] = []
    sku_analise = pd.DataFrame()
    n_skus_negativos = 0
    if "SKU_Normalizado" in df.columns and "Quantidade" in df.columns:
        sku_analise = (
            df.groupby("SKU_Normalizado", dropna=False)
            .agg(
                Vl_Venda=("Vl_Venda", "sum"),
                Custo_Produto_Total=("Custo_Produto_Total", "sum"),
                Resultado=("Resultado", "sum"),
                Quantidade=("Quantidade", "sum"),
            )
            .reset_index()
        )
        sku_analise["margem_pct"] = sku_analise.apply(
            lambda r: (r["Resultado"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] else 0.0, axis=1
        )
        sku_analise["custo_pct"] = sku_analise.apply(
            lambda r: (r["Custo_Produto_Total"] / r["Vl_Venda"] * 100.0) if r["Vl_Venda"] else 0.0, axis=1
        )
        sku_analise["preco_medio"] = sku_analise.apply(
            lambda r: (r["Vl_Venda"] / r["Quantidade"]) if r["Quantidade"] else 0.0, axis=1
        )
        sku_analise["custo_unitario"] = sku_analise.apply(
            lambda r: (r["Custo_Produto_Total"] / r["Quantidade"]) if r["Quantidade"] else 0.0, axis=1
        )

        skus_negativos = sku_analise.loc[sku_analise["Resultado"] < 0].sort_values("Resultado")
        for _, row in skus_negativos.head(20).iterrows():
            q = float(row["Quantidade"]) or 1.0
            prejuizo_unit = abs(float(row["Resultado"])) / q
            pm = float(row["preco_medio"])
            ajuste_pct = (prejuizo_unit / pm * 100.0) if pm > 0 else 0.0
            skus_risco.append(
                SKURisco(
                    sku=str(row["SKU_Normalizado"]),
                    receita=float(row["Vl_Venda"]),
                    margem_pct=float(row["margem_pct"]),
                    custo_pct=float(row["custo_pct"]),
                    resultado=float(row["Resultado"]),
                    quantidade=float(row["Quantidade"]),
                    preco_medio=pm,
                    custo_unitario=float(row["custo_unitario"]),
                    ajuste_breakeven=prejuizo_unit,
                    ajuste_breakeven_pct=ajuste_pct,
                )
            )

        n_skus_negativos = int((sku_analise["Resultado"] < 0).sum())
        if n_skus_negativos:
            prejuizo_skus = float(sku_analise.loc[sku_analise["Resultado"] < 0, "Resultado"].sum())
            diagnosticos.append(
                Diagnostico(
                    tipo="CAUSA",
                    nivel=AlertLevel.HIGH if n_skus_negativos > 10 else AlertLevel.MEDIUM,
                    titulo=f"{n_skus_negativos} SKUs com margem negativa",
                    detalhe=f"Prejuízo total desses SKUs: R$ {abs(prejuizo_skus):,.2f}",
                    acao="Rever precificação ou descontinuar produtos",
                    valor=float(n_skus_negativos),
                )
            )

    return HealthScore(
        score=score,
        level=level,
        receita=receita,
        resultado=resultado,
        margem_pct=margem_pct,
        custo_pct=custo_pct,
        margem_anterior=margem_anterior,
        margem_grupo=margem_grupo,
        tendencia_pp=tendencia_pp,
        vs_grupo_pp=vs_grupo_pp,
        diagnosticos=diagnosticos,
        skus_risco=skus_risco,
        periodo=(periodo_override.strip() if periodo_override else f"{mes:02d}/{ano}"),
        empresa=str(org_id),
        total_skus=int(len(sku_analise)),
        skus_margem_negativa=n_skus_negativos,
    )


def _nf_ts_br(s: pd.Series) -> pd.Series:
    t = pd.to_datetime(s, errors="coerce", utc=True)
    try:
        from zoneinfo import ZoneInfo

        br = ZoneInfo("America/Sao_Paulo")
    except Exception:
        return t.dt.tz_localize(None) if t.dt.tz is None else t.dt.tz_convert("UTC").dt.tz_localize(None)
    if t.dt.tz is None:
        t = t.dt.tz_localize("UTC")
    return t.dt.tz_convert(br)


def _series_ano_mes(df: pd.DataFrame, coluna_temporal: str) -> tuple[pd.Series, pd.Series]:
    """Ano e mês civis por linha (mesmo critério que ``build_resultado_gerencial_slice`` para ``Data``)."""
    if coluna_temporal not in df.columns:
        return pd.Series(dtype=int), pd.Series(dtype=int)
    if coluna_temporal == "Data":
        ts = pd.to_datetime(df[coluna_temporal], errors="coerce", dayfirst=True)
        return ts.dt.year, ts.dt.month
    ts = _nf_ts_br(df[coluna_temporal])
    return ts.dt.year, ts.dt.month


def _series_dia_civil_intervalo(df: pd.DataFrame, coluna_temporal: str) -> pd.Series:
    """Datas civis para filtro inclusivo [d_ini, d_fim]."""
    if coluna_temporal not in df.columns:
        return pd.Series(pd.NaT, index=df.index)
    if coluna_temporal == "Data":
        ts = pd.to_datetime(df[coluna_temporal], errors="coerce", dayfirst=True)
        return ts.dt.normalize().dt.date
    ts = _nf_ts_br(df[coluna_temporal])
    return ts.dt.date


def obter_dados_periodo_anterior(
    df_full: pd.DataFrame,
    org_id: str,
    ano: int,
    mes: int,
    *,
    coluna_temporal: str = "Nota_Data_Emissao",
) -> Optional[pd.DataFrame]:
    if mes == 1:
        mes_ant, ano_ant = 12, ano - 1
    else:
        mes_ant, ano_ant = mes - 1, ano
    if df_full.empty or coluna_temporal not in df_full.columns:
        return None
    y, mo = _series_ano_mes(df_full, coluna_temporal)
    m = (
        (df_full["org_id"].astype(str).str.strip() == str(org_id).strip())
        & (y == ano_ant)
        & (mo == mes_ant)
    )
    if "Status_Custo" in df_full.columns:
        m &= df_full["Status_Custo"].astype(str).str.strip().eq(STATUS_CUSTO_OK)
    df_ant = df_full.loc[m].copy()
    return df_ant if len(df_ant) > 0 else None


def obter_dados_grupo(
    df_full: pd.DataFrame,
    ano: int,
    mes: int,
    *,
    coluna_temporal: str = "Nota_Data_Emissao",
) -> Optional[pd.DataFrame]:
    if df_full.empty or coluna_temporal not in df_full.columns:
        return None
    y, mo = _series_ano_mes(df_full, coluna_temporal)
    m = (y == ano) & (mo == mes)
    if "Status_Custo" in df_full.columns:
        m &= df_full["Status_Custo"].astype(str).str.strip().eq(STATUS_CUSTO_OK)
    df_g = df_full.loc[m].copy()
    return df_g if len(df_g) > 0 else None


def slice_linhas_nf_periodo(
    df_full: pd.DataFrame,
    *,
    d_ini: Any,
    d_fim: Any,
    empresas_sel: tuple[str, ...],
    coluna_temporal: str = "Nota_Data_Emissao",
    plataformas_sel: tuple[str, ...] = (),
) -> pd.DataFrame:
    """Recorte comercial: intervalo [d_ini, d_fim] na coluna temporal + CUSTO_OK + filtros opcionais.

    ``coluna_temporal``: ``Nota_Data_Emissao`` (legado, emissão NF) ou ``Data`` (data da venda),
    alinhado ao Resultado Gerencial.
    """
    if df_full.empty or coluna_temporal not in df_full.columns:
        return pd.DataFrame()
    out = df_full.copy()
    dc = _series_dia_civil_intervalo(out, coluna_temporal)
    di = pd.Timestamp(d_ini).date() if hasattr(d_ini, "date") else d_ini
    df_end = pd.Timestamp(d_fim).date() if hasattr(d_fim, "date") else d_fim
    m = dc.notna() & (dc >= di) & (dc <= df_end)
    out = out.loc[m].copy()
    if "Status_Custo" in out.columns:
        out = out.loc[out["Status_Custo"].astype(str).str.strip().eq(STATUS_CUSTO_OK)]
    sel = tuple(str(x).strip() for x in empresas_sel if str(x).strip())
    if sel and "empresa" in out.columns:
        cf = {x.casefold() for x in sel}
        em = out["empresa"].fillna("").astype(str).str.strip().str.casefold()
        out = out.loc[em.isin(cf)].copy()
    if plataformas_sel and "Nome da plataforma" in out.columns:
        want = {nf_grain_plataforma_match_key(x) for x in plataformas_sel}
        want.discard("")
        got = out["Nome da plataforma"].map(nf_grain_plataforma_match_key)
        out = out.loc[got.isin(want)].copy()
    return out


def inferir_org_id_alvo(df_slice: pd.DataFrame, org_sidebar: str) -> str:
    """Uma org no slice -> org_id; varias -> consolidado (sem benchmark vs grupo)."""
    if df_slice.empty or "org_id" not in df_slice.columns:
        return "consolidado"
    u = {str(x).strip() for x in df_slice["org_id"].dropna().unique() if str(x).strip()}
    if len(u) == 1:
        return next(iter(u))
    return "consolidado"


def periodo_mes_de_datas(d_ini: Any, d_fim: Any) -> tuple[int, int, str]:
    """Se inicio e fim no mesmo mes/ano, usa esse mes; senao rotulo intervalo."""
    di = pd.Timestamp(d_ini).date() if hasattr(d_ini, "date") else d_ini
    df_ = pd.Timestamp(d_fim).date() if hasattr(d_fim, "date") else d_fim
    if di.year == df_.year and di.month == df_.month:
        return di.year, di.month, f"{di.month:02d}/{di.year}"
    return di.year, di.month, f"{di.strftime('%d/%m/%Y')} - {df_.strftime('%d/%m/%Y')}"
