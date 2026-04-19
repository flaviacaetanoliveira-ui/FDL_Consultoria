"""
Helpers de layout do Resultado Gerencial (sem Streamlit) — defaults de filtro e resumos.
"""

from __future__ import annotations

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

_BR_TZ = ZoneInfo("America/Sao_Paulo")


def rg_last_closed_month_civil(d: date) -> tuple[int, int]:
    """Mês civil completo imediatamente anterior a ``d``."""
    if d.month == 1:
        return d.year - 1, 12
    return d.year, d.month - 1


def rg_pick_empresa_maior_receita_mes_fechado(
    df_linha: pd.DataFrame,
    emp_opts: list[str],
    *,
    ref_date: date | None = None,
) -> str | None:
    """Empresa com maior Σ receita (lista) no último mês fechado (eixo Data); fallback alfabético."""
    if df_linha is None or df_linha.empty or not emp_opts:
        return None
    dref = ref_date or datetime.now(_BR_TZ).date()
    y, m = rg_last_closed_month_civil(dref)
    dfc = df_linha.copy()
    if "Data" not in dfc.columns:
        return sorted(emp_opts)[0]
    ec = "empresa" if "empresa" in dfc.columns else None
    if ec is None:
        return sorted(emp_opts)[0]
    ts = pd.to_datetime(dfc["Data"], errors="coerce")
    mask = ts.notna() & (ts.dt.year == y) & (ts.dt.month == m)
    sub = dfc.loc[mask]
    if sub.empty:
        return sorted(emp_opts)[0]
    col_v = "Vl_Venda" if "Vl_Venda" in sub.columns else ("Valor total" if "Valor total" in sub.columns else None)
    if col_v is None:
        return sorted(emp_opts)[0]
    vv = pd.to_numeric(sub[col_v], errors="coerce").fillna(0.0)
    em = sub[ec].astype(str).str.strip()
    best_lab: str | None = None
    best_sum = -1.0
    for lab in emp_opts:
        ssum = float(vv.loc[em == str(lab).strip()].sum())
        if ssum > best_sum:
            best_sum = ssum
            best_lab = lab
    return best_lab or sorted(emp_opts)[0]
