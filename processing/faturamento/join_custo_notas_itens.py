"""
Custo de produto a partir dos itens (SKU + quantidade) do export de notas de saída.

Quando o «Código» do pedido (ex. anúncio MLB) não existe na tabela de custo, mas a NF
lista códigos internos com quantidade, agrega Σ(qtd × custo_unitário) por NF e reparte
pelas linhas de pedido vinculadas à mesma NF (proporcional a Vl_Venda; fallback por quantidade).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .config import CUSTO_SKU_COL, CUSTO_UNITARIO_COL, STATUS_CUSTO_OK
from .custo_por_empresa import build_custo_unitario_map_por_empresa
from .io_notas_saida import filtrar_notas_canceladas, load_notas_saida_from_dir
from .join_notas import _df_col_as_series, _filtrar_notas_por_empresa, _prep_notas_dataframe
from .normalize import normalize_pedido_join_key, normalize_sku_key, to_numeric_br

FLAG_CUSTO_ORIGEM_NF = "Flag_Custo_Origem_NF"


def _detect_col_codigo_item(columns: list) -> str:
    for c in columns:
        cl = str(c).strip().casefold()
        if cl in ("código", "codigo", "código (sku)", "codigo (sku)", "sku"):
            return str(c)
    return ""


def _detect_col_quantidade(columns: list) -> str:
    for c in columns:
        if str(c).strip().casefold() == "quantidade":
            return str(c)
    for c in columns:
        if "quantidade" in str(c).strip().casefold():
            return str(c)
    return ""


def _custo_unitario_por_sku_normalizado(df_custo: pd.DataFrame, empresa: str | None) -> pd.Series:
    s_map, _meta = build_custo_unitario_map_por_empresa(df_custo, empresa)
    return s_map


def compute_custo_total_por_nf_desde_itens_notas(
    notas_dir: Path,
    df_custo: pd.DataFrame,
    org_id: str,
    empresa: str,
) -> tuple[pd.Series, dict[str, Any]]:
    """
    Soma custo (qtd × preço custo) por ``nf_key`` normalizado, usando linhas do export de notas.
    """
    meta: dict[str, Any] = {
        "custo_nf_itens_col_codigo": "",
        "custo_nf_itens_col_quantidade": "",
        "custo_nf_itens_linhas_com_custo": 0,
        "custo_nf_itens_nf_com_total_positivo": 0,
    }
    raw = load_notas_saida_from_dir(notas_dir)
    if raw.empty:
        return pd.Series(dtype="float64"), meta

    raw = filtrar_notas_canceladas(raw)
    col_cod = _detect_col_codigo_item(list(raw.columns))
    col_qtd = _detect_col_quantidade(list(raw.columns))
    meta["custo_nf_itens_col_codigo"] = col_cod
    meta["custo_nf_itens_col_quantidade"] = col_qtd
    if not col_cod or not col_qtd:
        return pd.Series(dtype="float64"), meta

    prep = _prep_notas_dataframe(raw)
    prep["_qtd_item"] = to_numeric_br(_df_col_as_series(raw, col_qtd))
    prep["_sku_item"] = normalize_sku_key(_df_col_as_series(raw, col_cod))
    prep = _filtrar_notas_por_empresa(prep, org_id, empresa)
    if prep.empty:
        return pd.Series(dtype="float64"), meta

    cu_map = _custo_unitario_por_sku_normalizado(df_custo, empresa)
    cu = prep["_sku_item"].map(cu_map)
    line_cost = prep["_qtd_item"].astype(float) * pd.to_numeric(cu, errors="coerce").fillna(0.0)
    meta["custo_nf_itens_linhas_com_custo"] = int((cu.notna() & (cu > 0)).sum())
    cost_by_nf = line_cost.groupby(prep["nf_key"].astype(str).str.strip(), sort=False).sum()
    meta["custo_nf_itens_nf_com_total_positivo"] = int((cost_by_nf > 0).sum())
    return cost_by_nf, meta


def enrich_custo_from_notas_itens(
    df: pd.DataFrame,
    df_custo: pd.DataFrame,
    *,
    notas_dir: Path,
    org_id: str,
    empresa: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Preenche ``Custo_Unitario`` (e flags de auditoria) quando ``Flag_SKU_Sem_Custo`` e há NF
    vinculada com total de custo derivado dos itens da nota.
    """
    out = df.copy()
    if FLAG_CUSTO_ORIGEM_NF not in out.columns:
        out[FLAG_CUSTO_ORIGEM_NF] = False
    else:
        out[FLAG_CUSTO_ORIGEM_NF] = out[FLAG_CUSTO_ORIGEM_NF].fillna(False).astype(bool)

    meta: dict[str, Any] = {"custo_nf_enriquecido_linhas_pedido": 0}

    if "Flag_SKU_Sem_Custo" not in out.columns or "Nota_Numero_Normalizado" not in out.columns:
        return out, meta

    cost_by_nf, meta_agg = compute_custo_total_por_nf_desde_itens_notas(
        notas_dir, df_custo, org_id, empresa
    )
    meta.update(meta_agg)

    if cost_by_nf.empty:
        return out, meta

    nf_norm = normalize_pedido_join_key(out["Nota_Numero_Normalizado"].astype(str))
    need = out["Flag_SKU_Sem_Custo"].fillna(False).astype(bool)
    has_nf = (
        out["faturamento_nota_vinculada"].fillna(False).astype(bool)
        if "faturamento_nota_vinculada" in out.columns
        else pd.Series(False, index=out.index)
    )

    n_enriched = 0
    for nf_key, total_raw in cost_by_nf.items():
        nk = str(nf_key).strip()
        if not nk:
            continue
        try:
            total = float(total_raw)
        except (TypeError, ValueError):
            continue
        if total <= 0 or pd.isna(total):
            continue

        m = need & has_nf & nf_norm.eq(nk)
        if not m.any():
            continue

        idx = out.index[m]
        sub = out.loc[idx]
        vv = to_numeric_br(sub["Vl_Venda"]).fillna(0.0)
        s = float(vv.sum())
        qtd = to_numeric_br(sub["Quantidade"]).fillna(0.0)

        if s > 0:
            for i in idx:
                share = float(vv.loc[i]) / s * total
                q = float(qtd.loc[i])
                out.loc[i, CUSTO_UNITARIO_COL] = share / q if q > 0 else 0.0
        else:
            tq = float(qtd.sum())
            if tq > 0:
                for i in idx:
                    sh = float(qtd.loc[i]) / tq * total
                    q = float(qtd.loc[i])
                    out.loc[i, CUSTO_UNITARIO_COL] = sh / q if q > 0 else 0.0
            else:
                n = max(len(idx), 1)
                share_u = total / n
                for i in idx:
                    out.loc[i, CUSTO_UNITARIO_COL] = share_u

        out.loc[idx, "Status_Custo"] = STATUS_CUSTO_OK
        out.loc[idx, "Flag_SKU_Sem_Custo"] = False
        if "Flag_Produto_Sem_Correspondencia_SKU" in out.columns:
            out.loc[idx, "Flag_Produto_Sem_Correspondencia_SKU"] = False
        out.loc[idx, FLAG_CUSTO_ORIGEM_NF] = True
        n_enriched += int(m.sum())

    meta["custo_nf_enriquecido_linhas_pedido"] = n_enriched
    return out, meta
