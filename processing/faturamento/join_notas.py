"""Join pedidos ↔ notas de saída, rateio do valor líquido e colunas fiscais por linha."""
from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .io_notas_saida import (
    detectar_col_data_emissao,
    detectar_col_valor_total_liquido,
    filtrar_notas_canceladas,
    load_notas_saida_from_dir,
)
from .normalize import normalize_pedido_join_key, to_numeric_br
from .validate import FaturamentoValidationError


def _df_col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Uma série alinhada ao índice de ``df`` para ``col``.

    Exports Excel/CSV por vezes repetem o mesmo nome de coluna; ``df[col]`` passa a ser
    ``DataFrame`` e quebra agregações / ``to_numeric_br``. Aqui usa-se a primeira coluna homónima.
    """
    if col not in df.columns:
        raise KeyError(col)
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def _norm_org_filter(s: str) -> str:
    return str(s).strip().lower().replace(" ", "_")


def _col_status_notas(columns: list[str]) -> str:
    for c in columns:
        n = str(c).lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            return c
    return ""


def _normalize_nota_situacao_cell(val: object) -> str:
    try:
        if pd.isna(val):
            return ""
    except (ValueError, TypeError):
        pass
    if val is None:
        return ""
    xs = str(val).strip()
    if not xs or xs.casefold() in {"nan", "none", "nat", "<na>"}:
        return ""
    low = xs.casefold()
    if "cancel" in low:
        return "Cancelada"
    if "deneg" in low:
        return "Denegada"
    if "inutil" in low:
        return "Inutilizada"
    return xs


def _situacao_por_nf_agregada(prep: pd.DataFrame) -> pd.Series:
    """Uma etiqueta por ``nf_key`` (prioriza cancelamento / denegação / inutilização)."""
    if prep.empty or "nf_key" not in prep.columns or "situacao_norm" not in prep.columns:
        return pd.Series(dtype=object)
    out: dict[str, str] = {}

    def _pick(vals: list[str]) -> str:
        seen = {str(v).strip() for v in vals if str(v).strip()}
        if not seen:
            return ""
        for tag in ("Cancelada", "Denegada", "Inutilizada"):
            if tag in seen:
                return tag
        return sorted(seen)[0]

    for nfk, sub in prep.groupby("nf_key", sort=False):
        k = str(nfk).strip()
        if not k:
            continue
        out[k] = _pick(sub["situacao_norm"].astype(str).tolist())
    return pd.Series(out, dtype=object)


def _prep_notas_dataframe(notas_raw: pd.DataFrame) -> pd.DataFrame:
    """Extrai colunas canónicas a partir do export bruto de notas."""
    if notas_raw.empty:
        return pd.DataFrame(
            columns=[
                "nf_key",
                "vl_liq",
                "frete_linha",
                "dt_emissao",
                "ped_key",
                "ml_key",
                "org_filt",
                "emp_filt",
                "situacao_norm",
            ]
        )
    df = notas_raw.copy()
    col_nf = "Número" if "Número" in df.columns else ""
    if not col_nf:
        for c in df.columns:
            cl = str(c).strip().lower()
            if cl in ("numero", "número", "nr nota", "nr_nota") and "pedido" not in cl:
                col_nf = c
                break
    if not col_nf:
        raise FaturamentoValidationError(
            "Notas de saída: coluna «Número» (número da NF) não encontrada."
        )
    col_ml = "Número do pedido multiloja" if "Número do pedido multiloja" in df.columns else ""
    col_ped = "Número do pedido" if "Número do pedido" in df.columns else ""
    col_vl = detectar_col_valor_total_liquido(list(df.columns))
    if not col_vl and "Valor total" in df.columns:
        col_vl = "Valor total"
    if not col_vl:
        raise FaturamentoValidationError(
            "Notas de saída: coluna «Valor total líquido» (ou similar) não encontrada."
        )
    col_dt = detectar_col_data_emissao(list(df.columns))
    col_org = ""
    for c in df.columns:
        if str(c).strip().lower().replace(" ", "") == "org_id":
            col_org = c
            break
    col_emp = ""
    for c in df.columns:
        if str(c).strip().casefold() == "empresa":
            col_emp = c
            break

    nf_key = normalize_pedido_join_key(_df_col_as_series(df, col_nf).astype(str))
    vl = to_numeric_br(_df_col_as_series(df, col_vl)) if col_vl in df.columns else pd.Series(0.0, index=df.index)
    if col_dt:
        dt = pd.to_datetime(df[col_dt], errors="coerce", dayfirst=True)
    else:
        dt = pd.Series(pd.NaT, index=df.index)
    ped_key = normalize_pedido_join_key(df[col_ped]) if col_ped else pd.Series("", index=df.index)
    ml_key = normalize_pedido_join_key(df[col_ml]) if col_ml else pd.Series("", index=df.index)
    org_f = (
        df[col_org].map(_norm_org_filter)
        if col_org
        else pd.Series("", index=df.index, dtype=object)
    )
    emp_f = (
        df[col_emp].fillna("").astype(str).str.strip().str.casefold()
        if col_emp
        else pd.Series("", index=df.index, dtype=object)
    )
    col_st = _col_status_notas(list(df.columns))
    if col_st:
        situ_norm = df[col_st].map(_normalize_nota_situacao_cell)
    else:
        situ_norm = pd.Series("", index=df.index, dtype=object)

    col_frete = next((c for c in df.columns if str(c).strip().casefold() == "frete"), "")
    frete_v = (
        to_numeric_br(_df_col_as_series(df, col_frete))
        if col_frete and col_frete in df.columns
        else pd.Series(0.0, index=df.index)
    )

    out = pd.DataFrame(
        {
            "nf_key": nf_key.astype(str).str.strip(),
            "vl_liq": vl,
            "frete_linha": pd.to_numeric(frete_v, errors="coerce").fillna(0.0).astype("float64"),
            "dt_emissao": dt,
            "ped_key": ped_key.astype(str).str.strip(),
            "ml_key": ml_key.astype(str).str.strip(),
            "org_filt": org_f.astype(str).str.strip(),
            "emp_filt": emp_f.astype(str).str.strip(),
            "situacao_norm": situ_norm.astype(str).str.strip(),
        }
    )
    out = out[out["nf_key"].ne("")].copy()
    return out


def _filtrar_notas_por_empresa(prep: pd.DataFrame, org_id: str, empresa: str) -> pd.DataFrame:
    if prep.empty:
        return prep
    oid = _norm_org_filter(org_id)
    ename = str(empresa).strip().casefold()
    has_org = prep["org_filt"].ne("").any()
    has_emp = prep["emp_filt"].ne("").any()
    if not has_org and not has_emp:
        return prep
    m = pd.Series(True, index=prep.index)
    if has_org:
        m = m & (prep["org_filt"].eq("") | prep["org_filt"].eq(oid))
    if has_emp:
        m = m & (prep["emp_filt"].eq("") | prep["emp_filt"].eq(ename))
    return prep.loc[m].copy()


def _maps_pedido_para_nf(prep: pd.DataFrame) -> tuple[dict[str, str], dict[str, str]]:
    m_ped: dict[str, str] = {}
    m_ml: dict[str, str] = {}
    for _, r in prep.iterrows():
        nk = str(r["nf_key"]).strip()
        if not nk:
            continue
        pk = str(r["ped_key"]).strip()
        mk = str(r["ml_key"]).strip()
        if pk:
            m_ped[pk] = nk
        if mk:
            m_ml[mk] = nk
    return m_ped, m_ml


def _lookup_nf_para_pedido(k_ped: str, k_ml: str, m_ped: dict[str, str], m_ml: dict[str, str]) -> str:
    if k_ped and k_ped in m_ped:
        return m_ped[k_ped]
    if k_ml and k_ml in m_ml:
        return m_ml[k_ml]
    if k_ped and k_ped in m_ml:
        return m_ml[k_ped]
    if k_ml and k_ml in m_ped:
        return m_ped[k_ml]
    return ""


def enrich_pedidos_com_notas(
    df: pd.DataFrame,
    *,
    notas_dir: Path,
    org_id: str,
    empresa: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Acrescenta colunas de nota e comercial base; **não** calcula imposto/resultado.

    ``Frete_Plataforma`` usa ``Custo de Frete`` (pedido); se no grupo da NF soma CF e soma
    frete-plataforma forem zero, reparte o total da coluna ``Frete`` do export de notas (Bling)
    igualmente entre as linhas do pedido vinculadas à NF.

    Colunas novas principais: ``Vl_Venda``, ``Frete_Plataforma``, ``Nota_Numero_Normalizado``,
    ``Nota_Valor_Liquido_Total``, ``Nota_Valor_Liquido_Rateado``, ``Nota_Rateio_Participacao``,
    ``Nota_Data_Emissao``, ``Nota_Situacao`` (etiqueta derivada do export de notas, por ``nf_key``),
    ``faturamento_nota_vinculada``, ``flag_faturamento_rateio_vl_venda_zero``.
    """
    from .calc import frete_plataforma_series

    meta: dict[str, Any] = {"notas_saida_dir": str(notas_dir.resolve()), "notas_linhas_apos_filtro": 0}

    out = df.copy()
    qtd_col = "Quantidade"
    pl = "Preço de lista"
    out[qtd_col] = to_numeric_br(out[qtd_col])
    out[pl] = to_numeric_br(out[pl])
    out["Vl_Venda"] = out[qtd_col] * out[pl]

    cf = to_numeric_br(out["Custo de Frete"]) if "Custo de Frete" in out.columns else pd.Series(0.0, index=out.index)
    out["Frete_Plataforma"] = frete_plataforma_series(out, cf)

    raw = load_notas_saida_from_dir(notas_dir)
    meta["notas_arquivos_linhas_brutas"] = int(len(raw))
    if raw.empty:
        out["Nota_Numero_Normalizado"] = ""
        out["Nota_Valor_Liquido_Total"] = np.nan
        out["Nota_Valor_Liquido_Rateado"] = 0.0
        out["Nota_Rateio_Participacao"] = 0.0
        out["Nota_Data_Emissao"] = pd.NaT
        out["Nota_Situacao"] = ""
        out["faturamento_nota_vinculada"] = False
        out["flag_faturamento_rateio_vl_venda_zero"] = False
        out["Base_Imposto"] = 0.0
        meta["notas_vazio"] = True
        return out, meta

    prep_all = _prep_notas_dataframe(raw)
    prep_all = _filtrar_notas_por_empresa(prep_all, org_id, empresa)
    if not prep_all.empty:
        prep_all["nf_key"] = prep_all["nf_key"].astype(str).str.strip()
    sit_by_nf = _situacao_por_nf_agregada(prep_all)

    raw = filtrar_notas_canceladas(raw)
    prep = _prep_notas_dataframe(raw)
    prep = _filtrar_notas_por_empresa(prep, org_id, empresa)
    meta["notas_linhas_apos_filtro"] = int(len(prep))
    meta["notas_vazio"] = bool(prep.empty)

    if not prep.empty:
        prep["nf_key"] = prep["nf_key"].astype(str).str.strip()

    if prep.empty:
        out["Nota_Numero_Normalizado"] = ""
        out["Nota_Valor_Liquido_Total"] = np.nan
        out["Nota_Valor_Liquido_Rateado"] = 0.0
        out["Nota_Rateio_Participacao"] = 0.0
        out["Nota_Data_Emissao"] = pd.NaT
        out["Nota_Situacao"] = ""
        out["faturamento_nota_vinculada"] = False
        out["flag_faturamento_rateio_vl_venda_zero"] = False
        out["Base_Imposto"] = 0.0
        return out, meta

    totais = (
        prep.groupby("nf_key", as_index=False)
        .agg(
            Nota_Valor_Liquido_Total=("vl_liq", "sum"),
            Nota_Frete_Total_Export=("frete_linha", "sum"),
            Nota_Data_Emissao=("dt_emissao", "min"),
        )
    )
    totais["nf_key"] = totais["nf_key"].astype(str).str.strip()
    m_ped, m_ml = _maps_pedido_para_nf(prep)

    k_ped = normalize_pedido_join_key(out["Número do pedido"].astype(str))
    k_ml = normalize_pedido_join_key(out["Número do pedido multiloja"].astype(str))
    nf_series = pd.Series(
        [_lookup_nf_para_pedido(a, b, m_ped, m_ml) for a, b in zip(k_ped, k_ml, strict=False)],
        index=out.index,
        dtype=object,
    )
    out["Nota_Numero_Normalizado"] = nf_series.astype(str).str.strip()

    tot_map = totais.set_index("nf_key")["Nota_Valor_Liquido_Total"]
    dt_map = totais.set_index("nf_key")["Nota_Data_Emissao"]
    out["Nota_Valor_Liquido_Total"] = out["Nota_Numero_Normalizado"].map(tot_map)
    out["Nota_Data_Emissao"] = out["Nota_Numero_Normalizado"].map(dt_map)
    out["Nota_Situacao"] = (
        out["Nota_Numero_Normalizado"].map(sit_by_nf).fillna("").astype(str).str.strip()
    )

    out["faturamento_nota_vinculada"] = out["Nota_Numero_Normalizado"].ne("")

    out["Nota_Valor_Liquido_Rateado"] = 0.0
    out["Nota_Rateio_Participacao"] = 0.0
    out["flag_faturamento_rateio_vl_venda_zero"] = False

    bad_groups: list[str] = []
    for nfk, sub in out.groupby("Nota_Numero_Normalizado", sort=False):
        nfk_s = str(nfk).strip()
        if not nfk_s:
            continue
        g_idx = sub.index.tolist()
        w = sub["Vl_Venda"].astype(float)
        s = float(w.sum())
        if s <= 0.0 or math.isnan(s):
            out.loc[g_idx, "flag_faturamento_rateio_vl_venda_zero"] = True
            bad_groups.append(nfk_s)
            continue
        total_nf = sub["Nota_Valor_Liquido_Total"].iloc[0]
        tnf = float(total_nf) if pd.notna(total_nf) else 0.0
        parts = (w / s).astype(float)
        alloc = (parts * tnf).astype(float)
        if len(g_idx) > 1:
            last_i = g_idx[-1]
            rest = tnf - float(alloc.drop(index=last_i).sum())
            alloc.loc[last_i] = rest
        out.loc[g_idx, "Nota_Rateio_Participacao"] = parts
        out.loc[g_idx, "Nota_Valor_Liquido_Rateado"] = alloc

    out["Base_Imposto"] = out["Nota_Valor_Liquido_Rateado"].fillna(0.0)

    frete_por_nf = totais.set_index("nf_key")["Nota_Frete_Total_Export"].fillna(0.0).astype(float)
    _eps = 1e-9
    n_fb = 0
    for nfk, sub in out.groupby("Nota_Numero_Normalizado", sort=False):
        nfk_s = str(nfk).strip()
        if not nfk_s:
            continue
        g_idx = sub.index.tolist()
        sfp = float(sub["Frete_Plataforma"].fillna(0.0).sum())
        scf = float(sub["Custo de Frete"].fillna(0.0).sum()) if "Custo de Frete" in out.columns else 0.0
        if sfp > _eps or scf > _eps:
            continue
        f_nota = float(frete_por_nf.get(nfk_s, 0.0))
        if f_nota <= _eps:
            continue
        n_fb += 1
        share = f_nota / max(len(g_idx), 1)
        out.loc[g_idx, "Frete_Plataforma"] = share

    if n_fb:
        meta["notas_frete_fallback_grupos_nf"] = n_fb

    if bad_groups:
        u = sorted(set(bad_groups))
        msg = ", ".join(u[:20])
        if len(u) > 20:
            msg += f" … (+{len(u) - 20})"
        raise FaturamentoValidationError(
            "Rateio de nota: soma de Vl_Venda é zero para nota(s) com vínculo: " + msg
        )

    meta["notas_nf_distintas"] = int(out.loc[out["faturamento_nota_vinculada"], "Nota_Numero_Normalizado"].nunique())
    return out, meta
