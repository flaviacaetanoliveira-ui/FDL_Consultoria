"""
Recorte mínimo (Etapa 1) — Faturamento & DRE: painel **NF-first**.

Universo: NFs válidas no **período de emissão**; comercial/custos nas **linhas de pedido** ligadas
(``build_nf_grain_dataframe``). ``apply_recorte_minimo`` mantém-se para outros usos / testes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Mapping

import pandas as pd

from faturamento_dre_recorte import (
    _BR_TZ,
    _fdl_fr_etiquetas_empresa_recorte,
    _fdl_fr_filtrar_por_etiquetas_empresa,
    _fdl_fr_faturamento_series_bool_mask,
    _fdl_fr_mask_nf_emissao_no_periodo,
    _fdl_fr_mask_venda_no_periodo,
    _fdl_fr_safe_streamlit_date,
    _fdl_fr_series_datetime_bounds_dates,
    _fdl_fr_ts_nf_emissao_para_dia_civil,
)


def _min_cal_limits(d_min: date, d_max: date) -> tuple[date, date]:
    today = datetime.now(_BR_TZ).date()
    cal_max = max(d_max, today)
    cal_min = min(d_min, today - timedelta(days=3 * 365))
    return cal_min, cal_max


@dataclass(frozen=True)
class FaturamentoRecorteMinState:
    empresas: tuple[str, ...]
    plataformas: tuple[str, ...]
    data_venda_ini: object | None
    data_venda_fim: object | None
    nf_emissao_ini: object | None = None
    nf_emissao_fim: object | None = None


def faturamento_recorte_min_state_from_session(ss: Mapping[str, Any]) -> FaturamentoRecorteMinState:
    def _tup(key: str) -> tuple[str, ...]:
        raw = ss.get(key)
        if not isinstance(raw, list):
            return ()
        return tuple(str(x) for x in raw if str(x).strip())

    return FaturamentoRecorteMinState(
        empresas=_tup("fdl_fat_min_emp"),
        plataformas=_tup("fdl_fat_min_plat"),
        data_venda_ini=ss.get("fdl_fat_min_d_ini"),
        data_venda_fim=ss.get("fdl_fat_min_d_fim"),
        nf_emissao_ini=ss.get("fdl_fat_min_nf_d_ini"),
        nf_emissao_fim=ss.get("fdl_fat_min_nf_d_fim"),
    )


def faturamento_min_series_nf_emissao_bounds_dates(df_raw: pd.DataFrame) -> tuple[date, date, bool]:
    """Retorna (mín, máx, ok) dos dias civis de ``Nota_Data_Emissao`` (ISO / ``dayfirst=False``)."""
    if df_raw.empty or "Nota_Data_Emissao" not in df_raw.columns:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    ts = _fdl_fr_ts_nf_emissao_para_dia_civil(df_raw["Nota_Data_Emissao"])
    t = ts[ts.notna()]
    if t.empty:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    return t.min().date(), t.max().date(), True


def _nf_fiscal_situacao_invalida(series: pd.Series) -> pd.Series:
    ss = series.fillna("").astype(str).str.strip().str.lower()
    return (
        ss.str.contains("cancel", na=False)
        | ss.str.contains("deneg", na=False)
        | ss.str.contains("inutil", na=False)
    )


@dataclass(frozen=True)
class FatMinComercialConferenciaStats:
    """Recorte comercial (``df_recorte``): venda = Qtd × Preço de lista."""

    valor_venda: float
    linhas_pedido: int
    pedidos_multiloja_distintos: int


@dataclass(frozen=True)
class FatMinFiscalConferenciaStats:
    """Eixo fiscal: emissão + NF válida, uma vez por NF (``Nota_Valor_Liquido_Total``)."""

    n_nf_distintas: int
    valor_nota_fiscal: float


def compute_comercial_conferencia_stats(df_recorte: pd.DataFrame) -> FatMinComercialConferenciaStats:
    if df_recorte.empty:
        return FatMinComercialConferenciaStats(0.0, 0, 0)
    qcol, pl_col = "Quantidade", "Preço de lista"
    if qcol not in df_recorte.columns or pl_col not in df_recorte.columns:
        return FatMinComercialConferenciaStats(0.0, int(len(df_recorte)), 0)
    qtd = pd.to_numeric(df_recorte[qcol], errors="coerce").fillna(0.0)
    pl = pd.to_numeric(df_recorte[pl_col], errors="coerce").fillna(0.0)
    valor_venda = float((qtd * pl).sum())
    n_lin = int(len(df_recorte))
    ml_col = "Número do pedido multiloja"
    if ml_col not in df_recorte.columns:
        return FatMinComercialConferenciaStats(valor_venda, n_lin, 0)
    ml = df_recorte[ml_col].fillna("").astype(str).str.strip()
    n_ml = int(ml[ml.ne("")].nunique())
    return FatMinComercialConferenciaStats(valor_venda, n_lin, n_ml)


def compute_fiscal_nf_conferencia_stats(
    df_raw: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
) -> FatMinFiscalConferenciaStats:
    """
    NFs distintas e soma de ``Nota_Valor_Liquido_Total`` (uma vez por NF) com ``Nota_Data_Emissao`` no intervalo,
    após filtro **Empresa**; sem plataforma / sem ``Data`` venda. Exclui cancelada / denegada / inutilizada.
    """
    if df_raw.empty or nf_d_fim < nf_d_ini:
        return FatMinFiscalConferenciaStats(0, 0.0)
    need = {"Nota_Data_Emissao", "Nota_Valor_Liquido_Total", "Nota_Numero_Normalizado"}
    if not need.issubset(df_raw.columns):
        return FatMinFiscalConferenciaStats(0, 0.0)

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(sliced)
    if emp_opts and empresas_sel:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, list(empresas_sel))
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    nn = sliced["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    mask_nf = nn.ne("")
    if "faturamento_nota_vinculada" in sliced.columns:
        mask_nf = mask_nf | _fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])
    sliced = sliced.loc[mask_nf].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    if "Nota_Situacao" in sliced.columns:
        sliced = sliced.loc[~_nf_fiscal_situacao_invalida(sliced["Nota_Situacao"])].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    m_period = _fdl_fr_mask_nf_emissao_no_periodo(sliced["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    sliced = sliced.loc[m_period].copy()
    if sliced.empty:
        return FatMinFiscalConferenciaStats(0, 0.0)

    gb_keys: list[str] = []
    if "org_id" in sliced.columns:
        gb_keys.append("org_id")
    gb_keys.append("Nota_Numero_Normalizado")

    total = 0.0
    n_gr = 0
    for _, gr in sliced.groupby(gb_keys, sort=False):
        n_gr += 1
        vals = pd.to_numeric(gr["Nota_Valor_Liquido_Total"], errors="coerce").dropna()
        total += float(vals.iloc[0]) if not vals.empty else 0.0
    return FatMinFiscalConferenciaStats(n_gr, total)


def _nf_grain_groupby_keys(df: pd.DataFrame) -> list[str]:
    keys: list[str] = []
    if "org_id" in df.columns:
        keys.append("org_id")
    keys.append("Nota_Numero_Normalizado")
    return keys


def _nf_grain_frete_numeric(df: pd.DataFrame) -> pd.Series:
    if "Frete_Plataforma" in df.columns:
        return pd.to_numeric(df["Frete_Plataforma"], errors="coerce").fillna(0.0)
    if "Custo de Frete" in df.columns:
        return pd.to_numeric(df["Custo de Frete"], errors="coerce").fillna(0.0)
    return pd.Series(0.0, index=df.index)


def build_nf_grain_dataframe(
    df_raw: pd.DataFrame,
    state: FaturamentoRecorteMinState,
    *,
    ok_nf_dates: bool,
    nf_d_ini: date,
    nf_d_fim: date,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Um único universo para KPIs e tabela (**só eixo temporal = emissão da NF**):

    1. Filtro **empresa**.
    2. NFs **válidas** (situação) com **emissão** em ``[nf_d_ini, nf_d_fim]`` e vínculo NF.
    3. Todas as **linhas de pedido** com chave ``(org_id, NF)`` nesse conjunto (sem filtro por **Data** de venda).
    4. Filtro opcional: **plataforma**.

    **NF sem linha de pedido no materializado:** não aparece (o dataset é grão pedido; não há nota órfã).
    **Vários pedidos por NF:** agregados na mesma linha NF (somas comerciais; texto ``pedido_resumo``).
    **Várias NFs por pedido:** várias linhas em ``df_nf`` (uma por NF).
    """
    warn: list[str] = []
    cols_out = [
        "org_id",
        "Nota_Numero_Normalizado",
        "Nota_Data_Emissao",
        "Nota_Situacao",
        "empresa",
        "valor_faturado_nf",
        "valor_venda",
        "diferenca",
        "comissao",
        "frete",
        "imposto",
        "resultado",
        "plataforma_resumo",
        "pedido_resumo",
        "n_linhas_pedido",
        "produto_resumo",
    ]
    if df_raw.empty or not ok_nf_dates or nf_d_fim < nf_d_ini:
        return pd.DataFrame(columns=cols_out), tuple(warn)
    need = {"Nota_Data_Emissao", "Nota_Valor_Liquido_Total", "Nota_Numero_Normalizado"}
    if not need.issubset(df_raw.columns):
        warn.append("Materializado sem colunas fiscais mínimas para o painel NF-first.")
        return pd.DataFrame(columns=cols_out), tuple(warn)

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(sliced)
    sel_emp = list(state.empresas)
    if emp_opts and sel_emp:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, sel_emp)
    if sliced.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    nn = sliced["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    mask_nf = nn.ne("")
    if "faturamento_nota_vinculada" in sliced.columns:
        mask_nf = mask_nf | _fdl_fr_faturamento_series_bool_mask(sliced["faturamento_nota_vinculada"])
    sel_emit = sliced.loc[mask_nf].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    if "Nota_Situacao" in sel_emit.columns:
        sel_emit = sel_emit.loc[~_nf_fiscal_situacao_invalida(sel_emit["Nota_Situacao"])].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    m_period = _fdl_fr_mask_nf_emissao_no_periodo(sel_emit["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
    sel_emit = sel_emit.loc[m_period].copy()
    if sel_emit.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    gb_keys = _nf_grain_groupby_keys(sel_emit)
    if "org_id" in sel_emit.columns:
        sel_emit = sel_emit.copy()
        sel_emit["_org_join"] = sel_emit["org_id"].fillna("").astype(str).str.strip()
        unique_nf = sel_emit.drop_duplicates(subset=["_org_join", "Nota_Numero_Normalizado"])[
            ["_org_join", "Nota_Numero_Normalizado"]
        ]
        sliced = sliced.copy()
        sliced["_org_join"] = sliced["org_id"].fillna("").astype(str).str.strip()
        df_linked = sliced.merge(
            unique_nf,
            left_on=["_org_join", "Nota_Numero_Normalizado"],
            right_on=["_org_join", "Nota_Numero_Normalizado"],
            how="inner",
            suffixes=("", "_y"),
        )
        df_linked = df_linked.drop(columns=["_org_join"], errors="ignore")
    else:
        unique_nf = sel_emit[["Nota_Numero_Normalizado"]].drop_duplicates()
        df_linked = sliced.merge(unique_nf, on="Nota_Numero_Normalizado", how="inner")

    sel_plat = list(state.plataformas)
    if sel_plat and "Nome da plataforma" in df_linked.columns:
        df_linked = df_linked[df_linked["Nome da plataforma"].isin(sel_plat)].copy()

    if df_linked.empty:
        return pd.DataFrame(columns=cols_out), tuple(warn)

    gcols = _nf_grain_groupby_keys(df_linked)
    rows: list[dict[str, object]] = []

    qcol, pl_col = "Quantidade", "Preço de lista"
    has_qpl = qcol in df_linked.columns and pl_col in df_linked.columns
    prod_col = "Descrição" if "Descrição" in df_linked.columns else ("Nome" if "Nome" in df_linked.columns else "")
    ml_col = "Número do pedido multiloja"
    ped_col = "Número do pedido"

    for key, gr in df_linked.groupby(gcols, sort=False):
        if len(gcols) == 2:
            oid_s, nfk = str(key[0]).strip(), str(key[1]).strip()
        else:
            oid_s, nfk = "", str(key).strip()
        gr = gr.copy()
        nf_vals = pd.to_numeric(gr["Nota_Valor_Liquido_Total"], errors="coerce").dropna()
        vl_nf = float(nf_vals.iloc[0]) if not nf_vals.empty else 0.0

        if has_qpl:
            qtd = pd.to_numeric(gr[qcol], errors="coerce").fillna(0.0)
            pl = pd.to_numeric(gr[pl_col], errors="coerce").fillna(0.0)
            v_venda = float((qtd * pl).sum())
        else:
            v_venda = 0.0

        com = float(pd.to_numeric(gr["Taxa de Comissão"], errors="coerce").fillna(0.0).sum()) if "Taxa de Comissão" in gr.columns else 0.0
        fre = float(_nf_grain_frete_numeric(gr).sum())
        imp = float(pd.to_numeric(gr["Imposto"], errors="coerce").fillna(0.0).sum()) if "Imposto" in gr.columns else 0.0
        res = float(pd.to_numeric(gr["Resultado"], errors="coerce").fillna(0.0).sum()) if "Resultado" in gr.columns else 0.0

        emi = pd.to_datetime(gr["Nota_Data_Emissao"], errors="coerce", dayfirst=False)
        emi_first = emi.min()
        sit = gr["Nota_Situacao"].dropna().astype(str).str.strip()
        sit_s = str(sit.iloc[0]) if len(sit) else ""

        emp = gr["empresa"].dropna().astype(str).str.strip() if "empresa" in gr.columns else pd.Series(dtype=str)
        emp_s = str(emp.iloc[0]) if len(emp) else ""

        if "Nome da plataforma" in gr.columns:
            if has_qpl:
                w = (pd.to_numeric(gr[qcol], errors="coerce").fillna(0.0) * pd.to_numeric(gr[pl_col], errors="coerce").fillna(0.0)).to_numpy()
                plat_vals = gr["Nome da plataforma"].fillna("").astype(str).str.strip()
                best = ""
                best_w = -1.0
                for p, tw in zip(plat_vals, w, strict=False):
                    if tw > best_w:
                        best_w = tw
                        best = p
                plats_u = plat_vals[plat_vals.ne("")]
                if plats_u.nunique() > 1:
                    plat_res = f"{best or '—'} (+{plats_u.nunique() - 1})" if best else f"{plats_u.nunique()} plataformas"
                else:
                    plat_res = best or (str(plats_u.iloc[0]) if len(plats_u) else "—")
            else:
                plats_u = gr["Nome da plataforma"].dropna().astype(str).str.strip().unique()
                plat_res = str(plats_u[0]) if len(plats_u) == 1 else (f"{plats_u[0]} (+{len(plats_u) - 1})" if len(plats_u) else "—")
        else:
            plat_res = "—"

        ml_set: set[str] = set()
        ped_set: set[str] = set()
        if ml_col in gr.columns:
            for x in gr[ml_col].fillna("").astype(str).str.strip():
                if x:
                    ml_set.add(x)
        if ped_col in gr.columns:
            for x in gr[ped_col].fillna("").astype(str).str.strip():
                if x:
                    ped_set.add(x)

        if len(ml_set) <= 1 and len(ped_set) <= 1:
            ped_res = ""
            if ml_set:
                ped_res = next(iter(ml_set))
            elif ped_set:
                ped_res = next(iter(ped_set))
            else:
                ped_res = "—"
        else:
            parts: list[str] = []
            if len(ml_set) > 1:
                parts.append(f"{len(ml_set)} multiloja")
            elif ml_set:
                parts.append(next(iter(ml_set)))
            if len(ped_set) > 1:
                parts.append(f"{len(ped_set)} pedidos")
            elif ped_set and not ml_set:
                parts.append(next(iter(ped_set)))
            ped_res = " · ".join(parts) if parts else "—"

        if prod_col:
            skus = gr[prod_col].fillna("").astype(str).str.strip()
            skus = skus[skus.ne("")]
            nu = skus.nunique()
            prod_res = str(skus.iloc[0]) if nu == 1 else (f"{skus.iloc[0]} (+{nu - 1} itens)" if nu else "—")
        else:
            prod_res = "—"

        rows.append(
            {
                "org_id": oid_s,
                "Nota_Numero_Normalizado": nfk,
                "Nota_Data_Emissao": emi_first,
                "Nota_Situacao": sit_s,
                "empresa": emp_s,
                "valor_faturado_nf": vl_nf,
                "valor_venda": v_venda,
                "diferenca": v_venda - vl_nf,
                "comissao": com,
                "frete": fre,
                "imposto": imp,
                "resultado": res,
                "plataforma_resumo": plat_res,
                "pedido_resumo": ped_res,
                "n_linhas_pedido": int(len(gr)),
                "produto_resumo": prod_res,
            }
        )

    out = pd.DataFrame(rows)
    if not out.empty and out["Nota_Data_Emissao"].notna().any():
        out = out.sort_values("Nota_Data_Emissao", ascending=False, na_position="last")
    return out.reset_index(drop=True), tuple(warn)


def compute_nf_panel_kpis(df_nf: pd.DataFrame) -> dict[str, float | int]:
    """KPIs do painel NF-first = somas (e contagens) sobre ``df_nf``."""
    empty = {
        "valor_venda": 0.0,
        "valor_faturado_nf": 0.0,
        "diferenca": 0.0,
        "comissao": 0.0,
        "frete": 0.0,
        "imposto": 0.0,
        "resultado": 0.0,
        "n_nf": 0,
    }
    if df_nf.empty:
        return empty
    vv = float(pd.to_numeric(df_nf["valor_venda"], errors="coerce").fillna(0.0).sum())
    vf = float(pd.to_numeric(df_nf["valor_faturado_nf"], errors="coerce").fillna(0.0).sum())
    return {
        "valor_venda": vv,
        "valor_faturado_nf": vf,
        "diferenca": vv - vf,
        "comissao": float(pd.to_numeric(df_nf["comissao"], errors="coerce").fillna(0.0).sum()),
        "frete": float(pd.to_numeric(df_nf["frete"], errors="coerce").fillna(0.0).sum()),
        "imposto": float(pd.to_numeric(df_nf["imposto"], errors="coerce").fillna(0.0).sum()),
        "resultado": float(pd.to_numeric(df_nf["resultado"], errors="coerce").fillna(0.0).sum()),
        "n_nf": int(len(df_nf)),
    }


def compute_vl_nota_fiscal_fiscal_kpi(
    df_raw: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
) -> float:
    """
    Soma do valor líquido **por nota** (``Nota_Valor_Liquido_Total`` uma vez por NF),
    com ``Nota_Data_Emissao`` no intervalo, após filtro **Empresa** (sem plataforma / sem ``Data`` venda).
    Exclui situações cancelada / denegada / inutilizada (mesmo critério textual do pipeline de notas).
    """
    return compute_fiscal_nf_conferencia_stats(
        df_raw, empresas_sel=empresas_sel, nf_d_ini=nf_d_ini, nf_d_fim=nf_d_fim
    ).valor_nota_fiscal


def apply_recorte_minimo(
    df_raw: pd.DataFrame,
    state: FaturamentoRecorteMinState,
) -> tuple[pd.DataFrame, tuple[str, ...]]:
    """
    Ordem: empresa → plataforma → período (**Data** venda).

    Limites de data para ``_safe_streamlit_date`` vêm do ``df_raw`` completo (antes dos filtros),
    para o calendário não encolher só porque se filtrou empresa.
    """
    warn: list[str] = []
    if df_raw.empty:
        return df_raw.copy(), ()

    has_data = "Data" in df_raw.columns
    if has_data:
        d_min, d_max, ok_dates = _fdl_fr_series_datetime_bounds_dates(df_raw["Data"])
    else:
        d_min = d_max = datetime.now(_BR_TZ).date()
        ok_dates = False

    sliced = df_raw.copy()
    emp_opts = _fdl_fr_etiquetas_empresa_recorte(df_raw)
    sel_emp = list(state.empresas)
    if emp_opts and sel_emp:
        sliced = _fdl_fr_filtrar_por_etiquetas_empresa(sliced, sel_emp)

    sel_plat = list(state.plataformas)
    if sel_plat and "Nome da plataforma" in sliced.columns:
        sliced = sliced[sliced["Nome da plataforma"].isin(sel_plat)].copy()

    if ok_dates and not sliced.empty:
        d_ini = _fdl_fr_safe_streamlit_date(state.data_venda_ini, d_min)
        d_fim = _fdl_fr_safe_streamlit_date(state.data_venda_fim, d_max)
        if d_fim < d_ini:
            warn.append("A data final da **venda** não pode ser anterior à inicial.")
            d_fim = d_ini
        m_d = _fdl_fr_mask_venda_no_periodo(sliced["Data"], d_ini, d_fim)
        sliced = sliced.loc[m_d].copy()

    return sliced, tuple(warn)
