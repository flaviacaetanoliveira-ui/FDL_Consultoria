"""
Análise comercial sobre a tabela de **pedidos** (grão linha): apenas **atendidos**, sem NF / lógica fiscal.

Usado pela área «Comercial & pedidos» no app operacional; métricas baseadas em
**Preço de lista** e **Quantidade**.
"""

from __future__ import annotations

import math
from datetime import date, timedelta
from typing import Literal

import pandas as pd

from processing.faturamento.config import SKU_NORMALIZADO_COL

TrendLabel = Literal["crescente", "estável", "decrescente", "insuficiente para tendência"]
SuggestionLabel = Literal[
    "priorizar reposição",
    "manter",
    "reduzir compra",
    "testar aumento moderado",
    "evitar reposição automática",
]

ABC_CLASS = Literal["A", "B", "C"]

# Pareto acumulado no recorte
ABC_PCT_A = 0.80
ABC_PCT_B = 0.95

# Tendência: variação mês a mês (último vs penúltimo)
TREND_PCT_THRESHOLD = 0.05
# Volume mínimo (unidades nos 3 meses) para classificar tendência
TREND_MIN_UNITS = 3.0


def atendido_mask(df: pd.DataFrame) -> pd.Series:
    """Situação normalizada == «atendido» (alinhado a ``_faturamento_atendido_mask``)."""
    if "Situação" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    situ = df["Situação"].fillna("").astype(str).str.strip().str.casefold()
    return situ.eq("atendido")


def pedido_id_series(df: pd.DataFrame) -> pd.Series:
    """Chave estável do pedido (multiloja + org), como no painel de faturamento."""
    if "Número do pedido" not in df.columns:
        return pd.Series("", index=df.index, dtype=object)
    ml = (
        df["Número do pedido multiloja"].fillna("").astype(str).str.strip()
        if "Número do pedido multiloja" in df.columns
        else pd.Series("", index=df.index)
    )
    ped = df["Número do pedido"].fillna("").astype(str).str.strip()
    core = ml.mask(ml.eq(""), ped)
    if "org_id" in df.columns:
        oid = df["org_id"].fillna("").astype(str).str.strip()
        return oid + "|" + core
    return core


def sku_key_series(df: pd.DataFrame) -> pd.Series:
    """Dimensão SKU: normalizado se existir; senão ``Código`` trim."""
    if SKU_NORMALIZADO_COL in df.columns:
        s = df[SKU_NORMALIZADO_COL].fillna("").astype(str).str.strip()
        s = s.mask(s.eq(""), df["Código"].fillna("").astype(str).str.strip() if "Código" in df.columns else "")
        return s
    if "Código" in df.columns:
        return df["Código"].fillna("").astype(str).str.strip()
    return pd.Series("", index=df.index, dtype=object)


def produto_label_series(df: pd.DataFrame) -> pd.Series:
    for col in ("Descrição", "Produto", "Nome do produto", "Título", "Título do anúncio", "Nome"):
        if col in df.columns:
            return df[col].fillna("").astype(str).str.strip().replace("", "—")
    return pd.Series("—", index=df.index, dtype=object)


def parse_data_pedido(s: pd.Series) -> pd.Series:
    """Datas comerciais da linha (coluna ``Data`` ou ``OPTIONAL_DATA_COL``)."""
    if s.empty:
        return pd.Series(dtype="datetime64[ns]")
    out = pd.to_datetime(s, errors="coerce", dayfirst=True)
    return out


def data_column(df: pd.DataFrame) -> str | None:
    if "Data" in df.columns:
        return "Data"
    return None


def filter_atendidos(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    m = atendido_mask(df)
    return df.loc[m].copy()


def filter_ui(
    df: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    d_ini: date | None,
    d_fim: date | None,
) -> pd.DataFrame:
    """Recorte comercial: empresa, plataforma, intervalo de datas (inclusivo)."""
    if df.empty:
        return df
    out = df
    emp = [str(x).strip() for x in empresas_sel if str(x).strip()]
    if emp and "empresa" in out.columns:
        out = out.loc[out["empresa"].astype(str).isin(emp)].copy()
    plat = [str(x).strip() for x in plataformas_sel if str(x).strip()]
    if plat and "Nome da plataforma" in out.columns:
        out = out.loc[out["Nome da plataforma"].astype(str).str.strip().isin(plat)].copy()
    dc = data_column(out)
    if dc and d_ini is not None and d_fim is not None and d_fim >= d_ini:
        ts = parse_data_pedido(out[dc])
        d_ini_ts = pd.Timestamp(d_ini)
        d_fim_ts = pd.Timestamp(d_fim) + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
        out = out.loc[(ts >= d_ini_ts) & (ts <= d_fim_ts)].copy()
    return out


def valor_comercial_lista_series(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=float)
    q = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0.0) if "Quantidade" in df.columns else 0.0
    pl = pd.to_numeric(df["Preço de lista"], errors="coerce").fillna(0.0) if "Preço de lista" in df.columns else 0.0
    return q * pl


def compute_kpis(df: pd.DataFrame) -> dict[str, float | int]:
    """KPIs no recorte já filtrado (só atendidos + UI). Pedidos = distintos por ``pedido_id_series``."""
    empty = {
        "valor_comercial_lista": 0.0,
        "quantidade_total": 0.0,
        "pedidos_atendidos_distintos": 0,
        "skus_distintos": 0,
    }
    if df.empty:
        return empty
    vc = valor_comercial_lista_series(df)
    qtd = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0.0) if "Quantidade" in df.columns else 0.0
    pid = pedido_id_series(df).astype(str).str.strip()
    pid = pid[pid.ne("")]
    sku = sku_key_series(df).astype(str).str.strip()
    sku = sku[sku.ne("")]
    return {
        "valor_comercial_lista": float(vc.sum()),
        "quantidade_total": float(qtd.sum()) if hasattr(qtd, "sum") else float(qtd),
        "pedidos_atendidos_distintos": int(pid.nunique()) if len(pid) else 0,
        "skus_distintos": int(sku.nunique()) if len(sku) else 0,
    }


def _abc_classify(cum_pct: pd.Series) -> pd.Series:
    def _one(x: float) -> ABC_CLASS:
        if math.isnan(x):
            return "C"
        if x <= ABC_PCT_A:
            return "A"
        if x <= ABC_PCT_B:
            return "B"
        return "C"

    return cum_pct.map(_one)


def compute_abc_valor(df: pd.DataFrame) -> pd.DataFrame:
    """ABC por Σ (Preço de lista × Quantidade) por SKU."""
    if df.empty or "Código" not in df.columns:
        return pd.DataFrame(
            columns=["SKU", "Produto", "Valor comercial (lista)", "Part %", "Acum %", "Classe"]
        )
    sku = sku_key_series(df)
    lab = produto_label_series(df)
    vc = valor_comercial_lista_series(df)
    g = (
        pd.DataFrame({"_sku": sku, "_lab": lab, "_vc": vc})
        .loc[lambda x: x["_sku"].astype(str).str.strip().ne("")]
        .groupby("_sku", sort=False)
        .agg(_vc=("_vc", "sum"), _lab=("_lab", "first"))
        .reset_index()
    )
    if g.empty:
        return pd.DataFrame(
            columns=["SKU", "Produto", "Valor comercial (lista)", "Part %", "Acum %", "Classe"]
        )
    tot = float(g["_vc"].sum()) or 1.0
    g["Part %"] = g["_vc"] / tot
    g = g.sort_values("_vc", ascending=False).reset_index(drop=True)
    g["Acum %"] = g["Part %"].cumsum()
    g["Classe"] = _abc_classify(g["Acum %"])
    out = g.rename(
        columns={"_sku": "SKU", "_lab": "Produto", "_vc": "Valor comercial (lista)"}
    )
    return out[["SKU", "Produto", "Valor comercial (lista)", "Part %", "Acum %", "Classe"]]


def compute_abc_quantidade(df: pd.DataFrame) -> pd.DataFrame:
    """ABC por Σ Quantidade por SKU (classes independentes da ABC valor)."""
    if df.empty or "Quantidade" not in df.columns:
        return pd.DataFrame(
            columns=["SKU", "Produto", "Quantidade", "Part %", "Acum %", "Classe"]
        )
    sku = sku_key_series(df)
    lab = produto_label_series(df)
    q = pd.to_numeric(df["Quantidade"], errors="coerce").fillna(0.0)
    g = (
        pd.DataFrame({"_sku": sku, "_lab": lab, "_q": q})
        .loc[lambda x: x["_sku"].astype(str).str.strip().ne("")]
        .groupby("_sku", sort=False)
        .agg(_q=("_q", "sum"), _lab=("_lab", "first"))
        .reset_index()
    )
    if g.empty:
        return pd.DataFrame(columns=["SKU", "Produto", "Quantidade", "Part %", "Acum %", "Classe"])
    tot = float(g["_q"].sum()) or 1.0
    g["Part %"] = g["_q"] / tot
    g = g.sort_values("_q", ascending=False).reset_index(drop=True)
    g["Acum %"] = g["Part %"].cumsum()
    g["Classe"] = _abc_classify(g["Acum %"])
    out = g.rename(columns={"_sku": "SKU", "_lab": "Produto", "_q": "Quantidade"})
    return out[["SKU", "Produto", "Quantidade", "Part %", "Acum %", "Classe"]]


def last_completed_calendar_month(as_of: date) -> tuple[int, int]:
    """Último mês calendário **inteiro** já encerrado relativamente a ``as_of`` (nunca o mês civil atual)."""
    first = date(as_of.year, as_of.month, 1)
    prev = first - timedelta(days=1)
    return prev.year, prev.month


def _year_month_before(y: int, m: int) -> tuple[int, int]:
    t = pd.Timestamp(year=y, month=m, day=1) - pd.offsets.MonthBegin(1)
    return int(t.year), int(t.month)


def trend_end_month_closed(period_end: date, as_of: date) -> tuple[int, int]:
    """
    Último mês da janela de tendência: sempre **fechado** (nunca mês parcial / «em aberto»).

    - ``last_closed``: último mês completo relativamente a ``as_of`` (ex.: em abril/2026 → março/2026).
    - Se ``period_end`` cai no **mesmo mês civil** que ``as_of``, esse mês está em aberto → teto do período
      passa a ser o mês **anterior** ao de ``period_end``.
    - O teto final é o **mínimo** (mais restritivo) entre esse teto e ``last_closed``, para não ultrapassar
      o fim do período filtrado nem usar meses futuros em relação a ``as_of``.
    """
    last_closed = last_completed_calendar_month(as_of)
    pe_y, pe_m = period_end.year, period_end.month
    if (pe_y, pe_m) == (as_of.year, as_of.month):
        pe_cap_y, pe_cap_m = _year_month_before(pe_y, pe_m)
    else:
        pe_cap_y, pe_cap_m = pe_y, pe_m
    return min((pe_cap_y, pe_cap_m), last_closed)


def three_closed_months_trend_bounds(
    period_end: date,
    *,
    as_of: date,
) -> tuple[pd.Timestamp, pd.Timestamp, tuple[tuple[int, int], tuple[int, int], tuple[int, int]]]:
    """
    Três meses calendário **fechados** consecutivos terminando em ``trend_end_month_closed(period_end, as_of)``.

    Retorna ``(início_M-2 00:00, fim_M 23:59:59, ((y2,m2),(y1,m1),(y0,m0)))`` com M0 = último mês fechado da janela.
    """
    y0, m0 = trend_end_month_closed(period_end, as_of)
    t_end = pd.Timestamp(year=y0, month=m0, day=1)
    m_minus_1_first = t_end - pd.offsets.MonthBegin(1)
    m_minus_2_first = t_end - pd.offsets.MonthBegin(2)
    start = m_minus_2_first.normalize()
    end = (t_end + pd.offsets.MonthEnd(0)).normalize() + pd.Timedelta(
        hours=23, minutes=59, seconds=59
    )
    triple = (
        (m_minus_2_first.year, m_minus_2_first.month),
        (m_minus_1_first.year, m_minus_1_first.month),
        (y0, m0),
    )
    return start, end, triple


def filter_trend_window(
    df_atendidos: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    period_end: date,
    as_of: date,
) -> pd.DataFrame:
    """Atendidos + empresa/plataforma + datas nos 3 meses **fechados** da tendência (ver ``three_closed_months_trend_bounds``)."""
    if df_atendidos.empty:
        return df_atendidos
    out = df_atendidos
    emp = [str(x).strip() for x in empresas_sel if str(x).strip()]
    if emp and "empresa" in out.columns:
        out = out.loc[out["empresa"].astype(str).isin(emp)].copy()
    plat = [str(x).strip() for x in plataformas_sel if str(x).strip()]
    if plat and "Nome da plataforma" in out.columns:
        out = out.loc[out["Nome da plataforma"].astype(str).str.strip().isin(plat)].copy()
    dc = data_column(out)
    if not dc:
        return pd.DataFrame(columns=out.columns)
    t0, t1, _months = three_closed_months_trend_bounds(period_end, as_of=as_of)
    ts = parse_data_pedido(out[dc])
    out = out.loc[(ts >= t0) & (ts <= t1)].copy()
    return out


def compute_trend_and_suggestion(
    df_trend: pd.DataFrame,
    abc_valor_df: pd.DataFrame,
    *,
    period_end: date,
    as_of: date,
) -> pd.DataFrame:
    """
    Por SKU: qtd e valor nos 3 meses **fechados** (M-2, M-1, último fechado), tendência e sugestão.
    ``as_of`` define o que é «mês em aberto»; o mês civil atual nunca entra na classificação.
    """
    cols = [
        "SKU",
        "Produto",
        "Qtd mês -2",
        "Qtd mês -1",
        "Qtd mês atual",
        "Valor lista mês -2",
        "Valor lista mês -1",
        "Valor lista mês atual",
        "Tendência",
        "Sugestão de compra",
    ]
    if df_trend.empty or "Quantidade" not in df_trend.columns:
        return pd.DataFrame(columns=cols)
    dc = data_column(df_trend)
    if not dc:
        return pd.DataFrame(columns=cols)
    ts = parse_data_pedido(df_trend[dc])
    sku = sku_key_series(df_trend)
    lab = produto_label_series(df_trend)
    q = pd.to_numeric(df_trend["Quantidade"], errors="coerce").fillna(0.0)
    vc = valor_comercial_lista_series(df_trend)
    work = pd.DataFrame({"ts": ts, "sku": sku, "lab": lab, "q": q, "vc": vc})
    work = work.loc[work["sku"].astype(str).str.strip().ne("") & work["ts"].notna()].copy()
    if work.empty:
        return pd.DataFrame(columns=cols)

    _, _, month_triple = three_closed_months_trend_bounds(period_end, as_of=as_of)
    (y2, m2), (y1, m1), (y0, m0) = month_triple

    def _sum_in_month(sub: pd.DataFrame, y: int, m: int) -> tuple[float, float]:
        mask = (sub["ts"].dt.year == y) & (sub["ts"].dt.month == m)
        s = sub.loc[mask]
        return float(s["q"].sum()), float(s["vc"].sum())

    abc_map: dict[str, str] = {}
    if not abc_valor_df.empty and "SKU" in abc_valor_df.columns and "Classe" in abc_valor_df.columns:
        abc_map = dict(zip(abc_valor_df["SKU"].astype(str), abc_valor_df["Classe"].astype(str)))

    rows: list[dict] = []
    for sk in work["sku"].unique():
        sub = work.loc[work["sku"].eq(sk)]
        first_lab = sub["lab"].iloc[0] if len(sub) else "—"
        q2, v2 = _sum_in_month(sub, y2, m2)
        q1, v1 = _sum_in_month(sub, y1, m1)
        q0, v0 = _sum_in_month(sub, y0, m0)
        q_tot = q2 + q1 + q0

        def _pct(a: float, b: float) -> float | None:
            if b == 0 or math.isclose(b, 0.0):
                return None
            return (a - b) / abs(b)

        pq01 = _pct(q0, q1)
        pq12 = _pct(q1, q2)
        pv01 = _pct(v0, v1)
        pv12 = _pct(v1, v2)

        trend: TrendLabel = "insuficiente para tendência"
        if q_tot < TREND_MIN_UNITS:
            trend = "insuficiente para tendência"
        else:
            up_q = (pq01 is not None and pq01 > TREND_PCT_THRESHOLD) or (
                pq12 is not None and pq12 > TREND_PCT_THRESHOLD
            )
            down_q = (pq01 is not None and pq01 < -TREND_PCT_THRESHOLD) or (
                pq12 is not None and pq12 < -TREND_PCT_THRESHOLD
            )
            up_v = (pv01 is not None and pv01 > TREND_PCT_THRESHOLD) or (
                pv12 is not None and pv12 > TREND_PCT_THRESHOLD
            )
            down_v = (pv01 is not None and pv01 < -TREND_PCT_THRESHOLD) or (
                pv12 is not None and pv12 < -TREND_PCT_THRESHOLD
            )
            if up_q and up_v:
                trend = "crescente"
            elif down_q and down_v:
                trend = "decrescente"
            elif (up_q or up_v) and (down_q or down_v):
                trend = "estável"
            else:
                trend = "estável"

        abc_c = abc_map.get(str(sk), "C")
        sug = _suggestion(abc_c, trend)
        rows.append(
            {
                "SKU": sk,
                "Produto": first_lab,
                "Qtd mês -2": q2,
                "Qtd mês -1": q1,
                "Qtd mês atual": q0,
                "Valor lista mês -2": v2,
                "Valor lista mês -1": v1,
                "Valor lista mês atual": v0,
                "Tendência": trend,
                "Sugestão de compra": sug,
            }
        )
    return pd.DataFrame(rows).sort_values("SKU", kind="stable").reset_index(drop=True)


def _suggestion(abc_classe: str, trend: TrendLabel) -> SuggestionLabel:
    a = abc_classe.upper()
    if trend == "insuficiente para tendência":
        return "evitar reposição automática"
    if trend == "crescente":
        if a == "A":
            return "priorizar reposição"
        if a == "B":
            return "testar aumento moderado"
        return "testar aumento moderado"
    if trend == "decrescente":
        if a == "A":
            return "reduzir compra"
        return "evitar reposição automática"
    # estável
    if a == "A":
        return "manter"
    if a == "B":
        return "manter"
    return "evitar reposição automática"


def bounds_dates_atendidos(df: pd.DataFrame) -> tuple[date | None, date | None]:
    """Limites de calendário (min/max) da coluna Data em pedidos atendidos."""
    base = filter_atendidos(df)
    dc = data_column(base)
    if base.empty or not dc:
        return None, None
    ts = parse_data_pedido(base[dc])
    ts = ts.dropna()
    if ts.empty:
        return None, None
    return ts.min().date(), ts.max().date()
