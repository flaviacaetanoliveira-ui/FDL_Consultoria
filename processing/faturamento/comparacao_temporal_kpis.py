"""
Comparações temporais MA3 / MoM para KPIs grandes do Resultado Gerencial.

Agregações leves sobre ``df`` linha + mesmo recorte que o slice; não altera pipelines KPI/DRE.
"""

from __future__ import annotations

import html
from calendar import monthrange
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal, Optional

import pandas as pd

from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    build_resultado_gerencial_slice,
)


def _last_day_month(y: int, m: int) -> date:
    return date(y, m, monthrange(y, m)[1])


def _is_calendario_mes_cheio(d0: date, d1: date) -> bool:
    if d0.day != 1:
        return False
    ult = _last_day_month(d0.year, d0.month)
    return d0.year == d1.year and d0.month == d1.month and d1 == ult


def _modo_comparacao(data_inicio: date, data_fim: date) -> Literal[
    "mes_cheio", "recorte_parcial", "multi_mes"
]:
    if data_inicio.year != data_fim.year or data_inicio.month != data_fim.month:
        return "multi_mes"
    if _is_calendario_mes_cheio(data_inicio, data_fim):
        return "mes_cheio"
    return "recorte_parcial"


def _margem_ratio(res: float, rec: float) -> float:
    if rec <= 1e-18:
        return 0.0
    return float(res) / float(rec)


def _slice_metrics(
    df_linha: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    d0: date,
    d1: date,
) -> MetricaMensal:
    """Uma agregação no recorte [d0,d1]."""
    if df_linha.empty or not REQUIRED_LINE_COLUMNS.issubset(df_linha.columns):
        return MetricaMensal(receita=0.0, resultado=0.0, margem=0.0, pedidos=0)
    sl = build_resultado_gerencial_slice(
        df_linha,
        empresas_sel=empresas_sel,
        plataformas_sel=plataformas_sel,
        data_venda_ini=d0,
        data_venda_fim=d1,
    )
    rec = float(sl.stats.receita_total)
    res = float(sl.stats.resultado_linhas_total)
    mg = _margem_ratio(res, rec)
    return MetricaMensal(
        receita=rec,
        resultado=res,
        margem=mg,
        pedidos=int(sl.stats.n_pedidos_unicos),
    )


@dataclass(frozen=True)
class MetricaMensal:
    receita: float
    resultado: float
    margem: float  # 0–1
    pedidos: int


@dataclass(frozen=True)
class ComparacaoKpisTemporal:
    periodo_atual_label: str

    resultado_atual: float
    margem_atual: float
    receita_atual: float

    tem_ma3: bool
    resultado_ma3: Optional[float]
    margem_ma3: Optional[float]
    delta_resultado_ma3_pct: Optional[float]
    delta_margem_ma3_pp: Optional[float]

    tem_mom: bool
    resultado_mom: Optional[float]
    margem_mom: Optional[float]
    delta_resultado_mom_pct: Optional[float]
    delta_margem_mom_pp: Optional[float]

    modo_comparacao: Literal["mes_cheio", "recorte_parcial", "multi_mes", "sem_historico"]


def compute_trailing_monthly_metrics(
    df_linha: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    mes_referencia: tuple[int, int],
    n_meses: int = 6,
) -> dict[str, MetricaMensal]:
    """
    Métricas por **mês civil completo**, dos ``n_meses`` imediatamente anteriores ao mês de referência.

    Chaves ``\"YYYY-MM\"`` em ordem cronológica [mais antigo → mais recente].
    """
    if df_linha.empty:
        return {}
    y0, m0 = mes_referencia
    out: dict[str, MetricaMensal] = {}
    pm = pd.Period(year=y0, month=m0, freq="M")
    for k in range(n_meses, 0, -1):
        p = pm - k
        iy, im = int(p.year), int(p.month)
        dk = f"{iy:04d}-{im:02d}"
        d0 = date(iy, im, 1)
        d1 = _last_day_month(iy, im)
        out[dk] = _slice_metrics(df_linha, empresas_sel=empresas_sel, plataformas_sel=plataformas_sel, d0=d0, d1=d1)
    return out


def _pct_delta_vs_ref(atual: float, referencia: float) -> Optional[float]:
    """Variação percentual (ex.: +11.1 para +11,1%); ``None`` se incomparável."""
    if referencia is None:
        return None
    ar = abs(float(referencia))
    if ar <= 1e-18:
        return None
    return (float(atual) - float(referencia)) / ar * 100.0


def _delta_pp_margem(atual: float, referencia: float) -> Optional[float]:
    """Diferença em pontos percentuais (ex.: margem ratio 0.18 vs 0.166 → 1.4 pp)."""
    return (float(atual) - float(referencia)) * 100.0


def _neutral_pct(delta: Optional[float]) -> bool:
    return delta is None or abs(float(delta)) <= 1.0


def _neutral_pp(delta_pp: Optional[float]) -> bool:
    return delta_pp is None or abs(float(delta_pp)) <= 1.0


def _delta_class_positive_good(delta: Optional[float], *, use_pp: bool) -> str:
    """Verde bom, vermelho ruim, cinza neutro (|delta|≤1 % ou ≤1 pp)."""
    if delta is None:
        return "fdl-fat-kpi-delta--neut"
    if use_pp:
        if _neutral_pp(delta):
            return "fdl-fat-kpi-delta--neut"
    else:
        if _neutral_pct(delta):
            return "fdl-fat-kpi-delta--neut"
    if float(delta) > 0:
        return "fdl-fat-kpi-delta--pos"
    return "fdl-fat-kpi-delta--neg"


def _fmt_delta_caption_line(delta_pct: float, *, is_margin: bool, suffix: str) -> str:
    """Texto com vírgula decimal PT-BR; valores negativos mantêm o sinal no número."""
    neu = _neutral_pp(delta_pct) if is_margin else _neutral_pct(delta_pct)
    if neu:
        num = f"{abs(delta_pct):.1f}".replace(".", ",")
        return f"→ {num}{suffix}"
    if delta_pct > 0:
        num = f"{delta_pct:.1f}".replace(".", ",")
        return f"↑ {num}{suffix}"
    num = f"{delta_pct:.1f}".replace(".", ",")
    return f"↓ {num}{suffix}"


def format_caption_linha_ma3(delta_pct: Optional[float], *, is_margin: bool) -> tuple[str, str]:
    """Seta PT + texto formatado + classe."""
    if delta_pct is None:
        return "", "fdl-fat-kpi-delta--neut"
    suf = "pp vs média 3m" if is_margin else "% vs média 3m"
    txt = _fmt_delta_caption_line(float(delta_pct), is_margin=is_margin, suffix=suf)
    cls = _delta_class_positive_good(delta_pct, use_pp=is_margin)
    return txt, cls


def format_caption_linha_mom(delta_pct: Optional[float], *, is_margin: bool) -> tuple[str, str]:
    if delta_pct is None:
        return "", "fdl-fat-kpi-delta--neut"
    suf = "pp vs mês anterior" if is_margin else "% vs mês anterior"
    txt = _fmt_delta_caption_line(float(delta_pct), is_margin=is_margin, suffix=suf)
    cls = _delta_class_positive_good(delta_pct, use_pp=is_margin)
    return txt, cls


def build_temporal_kpi_captions_html(comp: ComparacaoKpisTemporal | None) -> tuple[str, str]:
    """Fragmentos HTML para Resultado e Margem (cards hero). Vazio se sem comparação."""
    if comp is None or comp.modo_comparacao == "multi_mes":
        return "", ""
    rows_res: list[str] = []
    rows_mg: list[str] = []

    if comp.tem_ma3:
        tr, cr = format_caption_linha_ma3(comp.delta_resultado_ma3_pct, is_margin=False)
        tm, cm = format_caption_linha_ma3(comp.delta_margem_ma3_pp, is_margin=True)
        if tr:
            rows_res.append(f'<span class="fdl-fat-kpi-delta-ma3 {cr}">{html.escape(tr)}</span>')
        if tm:
            rows_mg.append(f'<span class="fdl-fat-kpi-delta-ma3 {cm}">{html.escape(tm)}</span>')
    if comp.tem_mom:
        tr2, cr2 = format_caption_linha_mom(comp.delta_resultado_mom_pct, is_margin=False)
        tm2, cm2 = format_caption_linha_mom(comp.delta_margem_mom_pp, is_margin=True)
        if tr2:
            rows_res.append(f'<span class="fdl-fat-kpi-delta-mom {cr2}">{html.escape(tr2)}</span>')
        if tm2:
            rows_mg.append(f'<span class="fdl-fat-kpi-delta-mom {cm2}">{html.escape(tm2)}</span>')

    def _wrap(parts: list[str]) -> str:
        if not parts:
            return ""
        return '<div class="fdl-fat-kpi-hero-caption-row">' + "".join(parts) + "</div>"

    return _wrap(rows_res), _wrap(rows_mg)


def compute_comparacao_kpis_temporal(
    *,
    slice_rg: object,
    df_linha: pd.DataFrame,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_inicio: date,
    data_fim: date,
    kp_rg: dict[str, float | int],
) -> ComparacaoKpisTemporal:
    """
    ``kp_rg`` deve ser o retorno de ``compute_resultado_gerencial_kpis`` alinhado ao ``slice_rg``.
    """
    modo = _modo_comparacao(data_inicio, data_fim)
    rec_at = float(kp_rg["valor_venda_lista"])
    res_at = float(kp_rg["resultado"])
    mg_at = _margem_ratio(res_at, rec_at)

    lab = f"{data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}"

    empty = ComparacaoKpisTemporal(
        periodo_atual_label=lab,
        resultado_atual=res_at,
        margem_atual=mg_at,
        receita_atual=rec_at,
        tem_ma3=False,
        resultado_ma3=None,
        margem_ma3=None,
        delta_resultado_ma3_pct=None,
        delta_margem_ma3_pp=None,
        tem_mom=False,
        resultado_mom=None,
        margem_mom=None,
        delta_resultado_mom_pct=None,
        delta_margem_mom_pp=None,
        modo_comparacao="sem_historico",
    )

    if modo == "multi_mes":
        return ComparacaoKpisTemporal(
            periodo_atual_label=lab,
            resultado_atual=res_at,
            margem_atual=mg_at,
            receita_atual=rec_at,
            tem_ma3=False,
            resultado_ma3=None,
            margem_ma3=None,
            delta_resultado_ma3_pct=None,
            delta_margem_ma3_pp=None,
            tem_mom=False,
            resultado_mom=None,
            margem_mom=None,
            delta_resultado_mom_pct=None,
            delta_margem_mom_pp=None,
            modo_comparacao="multi_mes",
        )

    if not REQUIRED_LINE_COLUMNS.issubset(df_linha.columns):
        return empty

    _ = slice_rg  # reservado para coerência de assinatura com a app

    yf, mf = data_fim.year, data_fim.month
    mes_ref = (yf, mf)

    # —— mes_cheio ——
    if modo == "mes_cheio":
        trail = compute_trailing_monthly_metrics(
            df_linha,
            empresas_sel=empresas_sel,
            plataformas_sel=plataformas_sel,
            mes_referencia=mes_ref,
            n_meses=6,
        )
        keys_3 = sorted(trail.keys())[-3:]
        if len(keys_3) < 3:
            ma3_res = ma3_mg = None
            tem_ma3 = False
        else:
            mets = [trail[k] for k in keys_3]
            # Mês sem linhas no DF vira receita 0 — não conta como histórico para MA3.
            if any(x.receita <= 1e-18 for x in mets):
                ma3_res = ma3_mg = None
                tem_ma3 = False
            else:
                ma3_res = sum(x.resultado for x in mets) / 3.0
                ma3_mg = sum(x.margem for x in mets) / 3.0
                tem_ma3 = True

        py, pm = (yf, mf - 1) if mf > 1 else (yf - 1, 12)
        d0p = date(py, pm, 1)
        d1p = _last_day_month(py, pm)
        mom_met = _slice_metrics(df_linha, empresas_sel=empresas_sel, plataformas_sel=plataformas_sel, d0=d0p, d1=d1p)
        tem_mom = True

        d_ma3_res = _pct_delta_vs_ref(res_at, ma3_res) if tem_ma3 else None
        d_ma3_mg = (
            _delta_pp_margem(mg_at, ma3_mg)
            if tem_ma3 and ma3_mg is not None
            else None
        )

        d_mom_res = _pct_delta_vs_ref(res_at, mom_met.resultado)
        d_mom_mg = _delta_pp_margem(mg_at, mom_met.margem)

        if not tem_ma3:
            tem_mom = tem_mom and mom_met.receita > 1e-18

        return ComparacaoKpisTemporal(
            periodo_atual_label=lab,
            resultado_atual=res_at,
            margem_atual=mg_at,
            receita_atual=rec_at,
            tem_ma3=tem_ma3,
            resultado_ma3=ma3_res,
            margem_ma3=ma3_mg,
            delta_resultado_ma3_pct=d_ma3_res,
            delta_margem_ma3_pp=d_ma3_mg,
            tem_mom=tem_mom,
            resultado_mom=mom_met.resultado,
            margem_mom=mom_met.margem,
            delta_resultado_mom_pct=d_mom_res,
            delta_margem_mom_pp=d_mom_mg,
            modo_comparacao="mes_cheio",
        )

    # —— recorte_parcial (mesmo mês civil, subperíodo): janelas de N dias desde o dia 1 em M-1, M-2, M-3
    n_days = int((data_fim - data_inicio).days + 1)
    y0, m0 = data_inicio.year, data_inicio.month

    def _sub_month(y: int, m: int, back: int) -> tuple[int, int]:
        mm = m - back
        yy = y
        while mm <= 0:
            mm += 12
            yy -= 1
        return yy, mm

    wins: list[tuple[date, date]] = []
    for back in (1, 2, 3):
        cd, cm = _sub_month(y0, m0, back)
        d0w = date(cd, cm, 1)
        d1w = d0w + timedelta(days=n_days - 1)
        ult_d = _last_day_month(cd, cm)
        if d1w > ult_d:
            d1w = ult_d
        wins.append((d0w, d1w))

    mets_p = [
        _slice_metrics(df_linha, empresas_sel=empresas_sel, plataformas_sel=plataformas_sel, d0=a, d1=b)
        for a, b in wins
    ]

    if any(x.receita <= 1e-18 for x in mets_p):
        ma3_res = ma3_mg = None
        tem_ma3 = False
    else:
        ma3_res = sum(x.resultado for x in mets_p) / 3.0
        ma3_mg = sum(x.margem for x in mets_p) / 3.0
        tem_ma3 = True

    py, pm1 = _sub_month(y0, m0, 1)
    d0mom = date(py, pm1, 1)
    d1mom = d0mom + timedelta(days=n_days - 1)
    ult_mom = _last_day_month(py, pm1)
    if d1mom > ult_mom:
        d1mom = ult_mom

    mom_met = _slice_metrics(df_linha, empresas_sel=empresas_sel, plataformas_sel=plataformas_sel, d0=d0mom, d1=d1mom)

    d_ma3_res = _pct_delta_vs_ref(res_at, ma3_res) if tem_ma3 else None
    d_ma3_mg = _delta_pp_margem(mg_at, ma3_mg) if tem_ma3 and ma3_mg is not None else None
    d_mom_res = _pct_delta_vs_ref(res_at, mom_met.resultado)
    d_mom_mg = _delta_pp_margem(mg_at, mom_met.margem)

    return ComparacaoKpisTemporal(
        periodo_atual_label=lab,
        resultado_atual=res_at,
        margem_atual=mg_at,
        receita_atual=rec_at,
        tem_ma3=tem_ma3,
        resultado_ma3=ma3_res if tem_ma3 else None,
        margem_ma3=ma3_mg if tem_ma3 else None,
        delta_resultado_ma3_pct=d_ma3_res,
        delta_margem_ma3_pp=d_ma3_mg,
        tem_mom=True,
        resultado_mom=mom_met.resultado,
        margem_mom=mom_met.margem,
        delta_resultado_mom_pct=d_mom_res,
        delta_margem_mom_pp=d_mom_mg,
        modo_comparacao="recorte_parcial",
    )
