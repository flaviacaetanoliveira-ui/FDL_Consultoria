"""
Camada de dados do Resultado Gerencial (grão linha), com âncora temporal em **Data** (venda).

Esta camada não altera a UI nem a Apuração Fiscal; serve de base para migração gradual dos blocos
(KPIs, DRE, Saúde, Tabela) para o recorte por data de venda.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime

import numpy as np
import pandas as pd

from comercial_pedidos_analise import pedido_id_series
from faturamento_dre_recorte import _fdl_fr_filtrar_por_etiquetas_empresa
from faturamento_dre_recorte_minimo import _nf_fiscal_situacao_invalida, nf_grain_plataforma_match_key
from processing.faturamento.calc import _frete_mercado_envios_vs_transportadora
from processing.faturamento.config import SKU_NORMALIZADO_COL

# Colunas mínimas do ``dataset.parquet`` (grão linha) documentadas para o Resultado Gerencial.
REQUIRED_LINE_COLUMNS: frozenset[str] = frozenset(
    {
        "Valor total",
        "Taxa de Comissão",
        "Frete_Plataforma",
        "Custo_Produto_Total",
        "Resultado",
        "Data",
        "Nome da plataforma",
        "empresa",
        "org_id",
        "Número do pedido",
    }
)


def _validate_line_columns(df: pd.DataFrame) -> None:
    missing = REQUIRED_LINE_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(
            "dataset.parquet (grão linha) sem colunas esperadas pelo Resultado Gerencial: "
            + ", ".join(sorted(missing))
        )


def _num_sum(df: pd.DataFrame, col: str) -> float:
    if col not in df.columns:
        return 0.0
    return float(pd.to_numeric(df[col], errors="coerce").fillna(0.0).sum())


def _receita_linha_series(df: pd.DataFrame) -> pd.Series:
    """Receita comercial por linha: ``Vl_Venda`` quando existir; senão ``Valor total``."""
    if "Vl_Venda" in df.columns:
        return pd.to_numeric(df["Vl_Venda"], errors="coerce").fillna(0.0)
    return pd.to_numeric(df["Valor total"], errors="coerce").fillna(0.0)


def _frete_transportadora_propria_sum(df: pd.DataFrame) -> float:
    if "Frete transportadora própria" in df.columns:
        return _num_sum(df, "Frete transportadora própria")
    if "Custo de Frete" not in df.columns:
        return 0.0
    cf = pd.to_numeric(df["Custo de Frete"], errors="coerce").fillna(0.0)
    _me, ftp = _frete_mercado_envios_vs_transportadora(df, cf)
    return float(ftp.fillna(0.0).sum())


def _ads_total(df: pd.DataFrame) -> float:
    if "custo_ads" in df.columns:
        return _num_sum(df, "custo_ads")
    v = 0.0
    if "custo_ads_variavel" in df.columns:
        v += _num_sum(df, "custo_ads_variavel")
    if "custo_ads_fixo" in df.columns:
        v += _num_sum(df, "custo_ads_fixo")
    return v


def _ads_split_totals(df: pd.DataFrame) -> tuple[float, float, bool]:
    """Totais ADS variável / fixo e flag ``custo_ads`` agregado sem colunas de split."""
    has_v = "custo_ads_variavel" in df.columns
    has_f = "custo_ads_fixo" in df.columns
    has_a = "custo_ads" in df.columns
    if has_v or has_f:
        av = _num_sum(df, "custo_ads_variavel") if has_v else 0.0
        af = _num_sum(df, "custo_ads_fixo") if has_f else 0.0
        return av, af, False
    if has_a:
        return 0.0, _num_sum(df, "custo_ads"), True
    return 0.0, 0.0, False


@dataclass(frozen=True)
class ResultadoGerencialSliceMeta:
    """Filtros e período aplicados ao slice."""

    empresas_sel: tuple[str, ...]
    plataformas_sel: tuple[str, ...]
    data_venda_ini: date
    data_venda_fim: date


@dataclass(frozen=True)
class ResultadoGerencialSliceStats:
    """Totais vetorizados sobre o recorte (grão linha)."""

    receita_total: float
    comissao_total: float
    frete_plataforma_total: float
    frete_transportadora_propria_total: float
    cmv_total: float
    resultado_linhas_total: float
    despesa_fixa_total: float
    ads_total: float
    n_linhas: int
    n_pedidos_unicos: int
    ads_variavel_total: float = 0.0
    ads_fixo_total: float = 0.0
    ads_sem_split_agregado: bool = False


@dataclass(frozen=True)
class ResultadoGerencialSlice:
    """Recorte gerencial em grão linha + totais e chave de pedido canónica."""

    df_linha: pd.DataFrame
    pedido_ids: pd.Series  # mesmo índice que ``df_linha``; produto ``comercial_pedidos_analise.pedido_id_series``
    stats: ResultadoGerencialSliceStats
    meta: ResultadoGerencialSliceMeta


def build_resultado_gerencial_slice(
    df_linha: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    data_venda_ini: date,
    data_venda_fim: date,
) -> ResultadoGerencialSlice:
    """Aplica recorte gerencial (empresas, plataformas, data da venda) e devolve slice + stats."""
    _validate_line_columns(df_linha)
    df = df_linha
    if empresas_sel:
        df = _fdl_fr_filtrar_por_etiquetas_empresa(df, list(empresas_sel))
    if plataformas_sel:
        want = {nf_grain_plataforma_match_key(x) for x in plataformas_sel}
        want.discard("")
        got = df["Nome da plataforma"].map(nf_grain_plataforma_match_key)
        df = df.loc[got.isin(want)].copy()

    ts = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    dcal = ts.dt.date
    m_date = ts.notna() & (dcal >= data_venda_ini) & (dcal <= data_venda_fim)
    df = df.loc[m_date].copy()

    receita_total = float(_receita_linha_series(df).sum())
    comissao_total = _num_sum(df, "Taxa de Comissão")
    frete_plataforma_total = _num_sum(df, "Frete_Plataforma")
    frete_tp_total = _frete_transportadora_propria_sum(df)
    cmv_total = _num_sum(df, "Custo_Produto_Total")
    resultado_linhas_total = _num_sum(df, "Resultado")
    despesa_fixa_total = _num_sum(df, "Despesas Fixas")
    ads_total = _ads_total(df)
    av_ads, af_ads, ads_sem_split = _ads_split_totals(df)

    pids = pedido_id_series(df).astype(str).str.strip()
    n_pedidos = int(pids[pids.ne("")].nunique())

    stats = ResultadoGerencialSliceStats(
        receita_total=receita_total,
        comissao_total=comissao_total,
        frete_plataforma_total=frete_plataforma_total,
        frete_transportadora_propria_total=frete_tp_total,
        cmv_total=cmv_total,
        resultado_linhas_total=resultado_linhas_total,
        despesa_fixa_total=despesa_fixa_total,
        ads_total=ads_total,
        n_linhas=int(len(df)),
        n_pedidos_unicos=n_pedidos,
        ads_variavel_total=av_ads,
        ads_fixo_total=af_ads,
        ads_sem_split_agregado=ads_sem_split,
    )
    meta = ResultadoGerencialSliceMeta(
        empresas_sel=empresas_sel,
        plataformas_sel=plataformas_sel,
        data_venda_ini=data_venda_ini,
        data_venda_fim=data_venda_fim,
    )
    return ResultadoGerencialSlice(df_linha=df, pedido_ids=pids.reindex(df.index), stats=stats, meta=meta)


def compute_resultado_gerencial_kpis(
    slice_: ResultadoGerencialSlice,
    *,
    fiscal_imposto_valor: float,
) -> dict[str, float | int]:
    """Calcula KPIs consolidados do Resultado Gerencial a partir do slice gerencial.

    PONTE COM APURAÇÃO FISCAL:
    O imposto desta função NÃO é recalculado — é consumido da Apuração Fiscal via parâmetro
    ``fiscal_imposto_valor``. O imposto é calculado sobre a base fiscal (data de emissão da NF),
    enquanto os demais KPIs usam data de venda.

    Isso pode gerar pequena defasagem temporal: vendas do período filtrado podem ter NFs emitidas
    em períodos adjacentes, e vice-versa. Isso é inerente ao regime de competência fiscal e deve ser
    comunicado ao usuário via tooltip na UI.

    Definições (Etapa 1 — base analítica):
    * **Valor da Venda (lista)** / receita_total: soma da receita por linha no slice (``Vl_Venda`` ou
      ``Valor total``).
    * **Total Receita (DRE):** receita_total + frete_transportadora_própria (repasse TP como receita).
    * **Total Deduções:** comissão + CMV + frete plataforma + frete TP + imposto fiscal + despesa fixa + ADS.
      (Colunas opcionais de ADS ausentes contribuem com 0.)
    * **Resultado:** receita_total − Total Deduções (imposto = valor fiscal externo).
    * **Margem:** resultado ÷ receita_total quando receita_total > 0 (fração 0–1).
    * **Ticket médio:** receita_total ÷ pedidos quando há pedidos.
    """
    s = slice_.stats
    receita = s.receita_total
    imposto = float(fiscal_imposto_valor)
    av = float(s.ads_variavel_total)
    af = float(s.ads_fixo_total)

    ded_diretos_operacional = (
        s.comissao_total
        + s.cmv_total
        + s.frete_plataforma_total
        + s.frete_transportadora_propria_total
        + imposto
        + av
    )
    resultado_operacional = receita - ded_diretos_operacional

    total_deducoes = (
        s.comissao_total
        + s.cmv_total
        + s.frete_plataforma_total
        + s.frete_transportadora_propria_total
        + imposto
        + s.despesa_fixa_total
        + s.ads_total
    )
    resultado = receita - total_deducoes
    resultado_liquido = resultado

    pedidos = s.n_pedidos_unicos
    ticket = receita / pedidos if pedidos else float("nan")
    margem = resultado / receita if receita else float("nan")
    margem_operacional = resultado_operacional / receita if receita else float("nan")
    margem_liquida = margem
    total_receita_dre = receita + s.frete_transportadora_propria_total

    return {
        "resultado": resultado,
        "margem": margem,
        "margem_sobre_venda": margem_liquida,
        "valor_venda_lista": receita,
        "pedidos": pedidos,
        "ticket_medio": ticket,
        "total_receita_dre": total_receita_dre,
        "total_deducoes": total_deducoes,
        "fiscal_imposto_valor": float(fiscal_imposto_valor),
        "n_linhas": s.n_linhas,
        "resultado_linhas_total": s.resultado_linhas_total,
        "resultado_operacional": resultado_operacional,
        "resultado_liquido": resultado_liquido,
        "margem_operacional": margem_operacional,
        "margem_liquida": margem_liquida,
        "total_comissao": s.comissao_total,
        "total_frete_plataforma": s.frete_plataforma_total,
        "total_frete_tp": s.frete_transportadora_propria_total,
        "total_cmv": s.cmv_total,
        "total_imposto": imposto,
        "total_despesa_fixa": s.despesa_fixa_total,
        "total_ads_variavel": av,
        "total_ads_fixo": af,
        "total_ads": s.ads_total,
        "ads_sem_split_agregado": bool(s.ads_sem_split_agregado),
    }


def _frete_tp_linha_series(df: pd.DataFrame) -> pd.Series:
    """Mesmo critério de ``_frete_transportadora_propria_sum``, por linha."""
    if "Frete transportadora própria" in df.columns:
        return pd.to_numeric(df["Frete transportadora própria"], errors="coerce").fillna(0.0)
    if "Custo de Frete" not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    cf = pd.to_numeric(df["Custo de Frete"], errors="coerce").fillna(0.0)
    _me, ftp = _frete_mercado_envios_vs_transportadora(df, cf)
    return ftp.fillna(0.0)


def _ads_var_fix_linha_series(df: pd.DataFrame) -> tuple[pd.Series, pd.Series]:
    """Séries ADS variável e fixo; ``custo_ads`` agregado (sem split) cai 100% no fixo."""
    z = pd.Series(0.0, index=df.index, dtype=float)
    if "custo_ads" in df.columns:
        return z, pd.to_numeric(df["custo_ads"], errors="coerce").fillna(0.0)
    v = z
    f = z
    if "custo_ads_variavel" in df.columns:
        v = pd.to_numeric(df["custo_ads_variavel"], errors="coerce").fillna(0.0)
    if "custo_ads_fixo" in df.columns:
        f = pd.to_numeric(df["custo_ads_fixo"], errors="coerce").fillna(0.0)
    return v, f


def _ads_linha_series(df: pd.DataFrame) -> pd.Series:
    """Espelho linha-a-linha de ``_ads_total`` (soma var+fix)."""
    a, b = _ads_var_fix_linha_series(df)
    return a + b


def _nf_linha_status_series(df: pd.DataFrame) -> pd.Series:
    """Por linha: ``emitida`` | ``cancelada`` | ``sem_nf``."""
    if "Nota_Situacao" not in df.columns:
        return pd.Series("sem_nf", index=df.index, dtype=object)
    raw = df["Nota_Situacao"]
    ss = raw.fillna("").astype(str).str.strip()
    empty = ss.eq("")
    inv = _nf_fiscal_situacao_invalida(raw)
    out = pd.Series("emitida", index=df.index, dtype=object)
    out = out.mask(empty, other="sem_nf")
    out = out.mask(inv & (~empty), other="cancelada")
    return out


def _rollup_status_nf(vals: list[str]) -> str:
    u = {str(x).strip() for x in vals if str(x).strip()}
    if not u:
        return "sem_nf"
    if len(u) == 1:
        return next(iter(u))
    return "parcial"


def _allocate_imposto_total_centavos(keys_sorted: list[str], receitas: dict[str, float], total_imposto: float) -> dict[str, float]:
    """Distribui ``total_imposto`` com fecho ao centavo (maiores restos); chaves ordenadas estáveis."""
    total_cent = int(round(float(total_imposto) * 100))
    out: dict[str, float] = {k: 0.0 for k in keys_sorted}
    if total_cent == 0:
        return out
    rt = sum(max(0.0, receitas.get(k, 0.0)) for k in keys_sorted)
    if rt <= 1e-18:
        return out
    raw = []
    floors: list[int] = []
    for k in keys_sorted:
        rk = max(0.0, receitas.get(k, 0.0))
        q = total_cent * rk / rt
        fl = math.floor(q + 1e-12)
        floors.append(int(fl))
        raw.append(q - fl)
        out[k] = float(fl) / 100.0
    diff = total_cent - sum(floors)
    idx_order = sorted(range(len(keys_sorted)), key=lambda i: (-raw[i], keys_sorted[i]))
    for i in idx_order[: max(0, diff)]:
        k = keys_sorted[i]
        out[k] += 0.01
    return out


def _sku_list_para_pedido(gr: pd.DataFrame) -> list[str]:
    col = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in gr.columns else "Código"
    if col not in gr.columns:
        return []
    s = gr[col].fillna("").astype(str).str.strip()
    uniq = sorted({x for x in s.tolist() if x})
    return uniq


def _skus_tuple_por_grupo_sku_col(s: pd.Series) -> tuple[str, ...]:
    """Único conjunto ordenado de SKUs (mesma semântica que ``_sku_list_para_pedido`` sobre a coluna)."""
    xs = {str(x).strip() for x in s.tolist() if str(x).strip()}
    return tuple(sorted(xs))


def _rollup_nf_por_grupo(s: pd.Series) -> str:
    return _rollup_status_nf([str(x).strip() for x in s.tolist()])


@dataclass(frozen=True)
class PedidoGerencialRow:
    """Uma linha da tabela Resultado Gerencial por pedido (âncora Data da venda)."""

    data_venda: datetime
    plataforma: str
    empresa: str
    pedido_id: str
    numero_pedido_ui: str
    skus: tuple[str, ...]
    qtd_itens: int
    receita: float
    comissao: float
    frete_plataforma: float
    cmv: float
    frete_tp: float
    imposto_rateado: float
    despesa_fixa: float
    ads_variavel: float
    ads_fixo: float
    resultado_operacional: float
    resultado_liquido: float
    margem_operacional_pct: float
    margem_liquida_pct: float
    resultado: float
    margem_pct: float
    status_nf: str


def compute_tabela_por_pedido(
    slice_: ResultadoGerencialSlice,
    *,
    fiscal_imposto_valor: float,
) -> list[PedidoGerencialRow]:
    """Agrega ``slice_.df_linha`` por ``pedido_id_series``.

    Mantém coerência com ``compute_resultado_gerencial_kpis``: somando receita/comissões/CMV/frete TP/
    despesa fixa/ADS por linha e rateando **apenas** o imposto fiscal (mesma ponte dos KPIs) pela receita do pedido,
    obtém-se ``sum(row.resultado)`` igual ao ``kpis[\"resultado\"]`` (ao centavo).

    Granularidade: entrada grão linha — saída grão pedido.
    """
    df = slice_.df_linha
    if df.empty:
        return []

    pid = slice_.pedido_ids.astype(str).str.strip()
    mask_pid = pid.ne("")
    df = df.loc[mask_pid].copy()
    pid = pid.loc[mask_pid]

    rec = _receita_linha_series(df)
    com = pd.to_numeric(df["Taxa de Comissão"], errors="coerce").fillna(0.0)
    fp = pd.to_numeric(df["Frete_Plataforma"], errors="coerce").fillna(0.0)
    cmv = pd.to_numeric(df["Custo_Produto_Total"], errors="coerce").fillna(0.0)
    ftp = _frete_tp_linha_series(df)
    desp = (
        pd.to_numeric(df["Despesas Fixas"], errors="coerce").fillna(0.0)
        if "Despesas Fixas" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    ads_v, ads_f = _ads_var_fix_linha_series(df)
    nf_lin = _nf_linha_status_series(df)
    ts_sale = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)

    n = len(df)
    plat_col = (
        df["Nome da plataforma"].fillna("").astype(str).str.strip().values if "Nome da plataforma" in df.columns else [""] * n
    )
    emp_col = df["empresa"].fillna("").astype(str).str.strip().values if "empresa" in df.columns else [""] * n
    np_col = df["Número do pedido"].fillna("").astype(str).str.strip().values if "Número do pedido" in df.columns else [""] * n

    sku_col_name = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in df.columns else "Código"
    if sku_col_name not in df.columns:
        sku_col = np.array([""] * n, dtype=object)
    else:
        sku_col = df[sku_col_name].fillna("").astype(str).str.strip().values

    work = pd.DataFrame(
        {
            "_pid": pid.astype(str).values,
            "_rec": rec.values,
            "_com": com.values,
            "_fp": fp.values,
            "_cmv": cmv.values,
            "_ftp": ftp.values,
            "_desp": desp.values,
            "_ads_v": ads_v.values,
            "_ads_f": ads_f.values,
            "_nf": nf_lin.values,
            "_dt": ts_sale.values,
            "_plat": plat_col,
            "_emp": emp_col,
            "_np": np_col,
            "_sku": sku_col,
        },
        index=df.index,
    )

    grp = work.groupby("_pid", sort=False)
    sums = grp[["_rec", "_com", "_fp", "_cmv", "_ftp", "_desp", "_ads_v", "_ads_f"]].sum()
    dt_min = grp["_dt"].min()
    plat_first = grp["_plat"].first()
    emp_first = grp["_emp"].first()
    np_first = grp["_np"].first()
    n_linhas_ped = grp.size().rename("_n_linhas")
    skus_por_ped = grp["_sku"].apply(_skus_tuple_por_grupo_sku_col)
    nf_stat_por_ped = grp["_nf"].apply(_rollup_nf_por_grupo)

    base = sums.join(dt_min.rename("_dt_raw")).join(plat_first.rename("_plat")).join(emp_first.rename("_emp")).join(np_first.rename("_np_ui")).join(n_linhas_ped).join(skus_por_ped.rename("_skus")).join(nf_stat_por_ped.rename("_nf_roll"))

    base.index = base.index.map(str)
    receita_por = {str(k): float(base.at[k, "_rec"]) for k in base.index}
    keys_sorted = sorted(receita_por.keys())
    imp_por = _allocate_imposto_total_centavos(keys_sorted, receita_por, float(fiscal_imposto_valor))

    imp_ser = pd.Series(imp_por, dtype="float64").reindex(base.index)
    base["_imp"] = imp_ser.to_numpy(dtype=float)

    rows: list[PedidoGerencialRow] = []
    def_dt = datetime.combine(slice_.meta.data_venda_ini, datetime.min.time())

    for pid_key in keys_sorted:
        row = base.loc[pid_key]
        r = float(row["_rec"])
        c = float(row["_com"])
        fp_ = float(row["_fp"])
        cmv_ = float(row["_cmv"])
        ftp_ = float(row["_ftp"])
        desp_ = float(row["_desp"])
        ads_v_ = float(row["_ads_v"])
        ads_f_ = float(row["_ads_f"])
        imp_ = float(row["_imp"])
        res_op = r - c - fp_ - cmv_ - ftp_ - imp_ - ads_v_
        res_liq = r - c - fp_ - cmv_ - ftp_ - imp_ - desp_ - ads_v_ - ads_f_
        margem_op = (res_op / r * 100.0) if r > 1e-12 else 0.0
        margem_liq = (res_liq / r * 100.0) if r > 1e-12 else 0.0

        raw_dt = row["_dt_raw"]
        if pd.isna(raw_dt):
            dv = def_dt
        else:
            dv = pd.Timestamp(raw_dt).to_pydatetime()

        pf = row["_plat"]
        ef = row["_emp"]
        nf_u = row["_np_ui"]
        _su = row["_skus"]
        sku_u: tuple[str, ...] = _su if isinstance(_su, tuple) else tuple(_su)
        sts = str(row["_nf_roll"])
        n_lin = int(row["_n_linhas"])

        rows.append(
            PedidoGerencialRow(
                data_venda=dv,
                plataforma=str(pf) if pf is not None and str(pf).strip() != "" else "",
                empresa=str(ef) if ef is not None and str(ef).strip() != "" else "",
                pedido_id=str(pid_key),
                numero_pedido_ui=str(nf_u).strip() if nf_u is not None else "",
                skus=sku_u,
                qtd_itens=n_lin,
                receita=r,
                comissao=c,
                frete_plataforma=fp_,
                cmv=cmv_,
                frete_tp=ftp_,
                imposto_rateado=imp_,
                despesa_fixa=desp_,
                ads_variavel=ads_v_,
                ads_fixo=ads_f_,
                resultado_operacional=res_op,
                resultado_liquido=res_liq,
                margem_operacional_pct=margem_op,
                margem_liquida_pct=margem_liq,
                resultado=res_liq,
                margem_pct=margem_liq,
                status_nf=sts,
            )
        )

    rows.sort(key=lambda x: (x.data_venda, x.pedido_id), reverse=True)
    return rows


@dataclass(frozen=True)
class SkuGerencialMargem:
    """Margens operacional/líquida por SKU (rateio fiscal igual aos KPIs por pedido)."""

    sku: str
    receita: float
    resultado_operacional: float
    resultado_liquido: float
    margem_operacional_pct: float
    margem_liquida_pct: float
    resultado_coluna_naive: float


def compute_sku_margens_para_saude(df: pd.DataFrame, *, fiscal_imposto_valor: float) -> list[SkuGerencialMargem]:
    """Agrega linhas por ``SKU_Normalizado`` com os mesmos componentes que ``compute_tabela_por_pedido``."""
    col = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in df.columns else "Código"
    if df.empty or col not in df.columns:
        return []

    rec = _receita_linha_series(df)
    com = pd.to_numeric(df["Taxa de Comissão"], errors="coerce").fillna(0.0)
    fp = pd.to_numeric(df["Frete_Plataforma"], errors="coerce").fillna(0.0)
    cmv = pd.to_numeric(df["Custo_Produto_Total"], errors="coerce").fillna(0.0)
    ftp = _frete_tp_linha_series(df)
    desp = (
        pd.to_numeric(df["Despesas Fixas"], errors="coerce").fillna(0.0)
        if "Despesas Fixas" in df.columns
        else pd.Series(0.0, index=df.index)
    )
    ads_v, ads_f = _ads_var_fix_linha_series(df)
    res_naive = pd.to_numeric(df["Resultado"], errors="coerce").fillna(0.0) if "Resultado" in df.columns else pd.Series(0.0, index=df.index)

    sku_key = df[col].fillna("").astype(str).str.strip()
    work = pd.DataFrame(
        {
            "_sku": sku_key.values,
            "_rec": rec.values,
            "_com": com.values,
            "_fp": fp.values,
            "_cmv": cmv.values,
            "_ftp": ftp.values,
            "_desp": desp.values,
            "_ads_v": ads_v.values,
            "_ads_f": ads_f.values,
            "_naive": res_naive.values,
        },
        index=df.index,
    )
    work = work.loc[work["_sku"].ne("")].copy()
    if work.empty:
        return []

    grp = work.groupby("_sku", sort=False)
    sums = grp[["_rec", "_com", "_fp", "_cmv", "_ftp", "_desp", "_ads_v", "_ads_f", "_naive"]].sum()

    receita_por = {str(k): float(sums.loc[k, "_rec"]) for k in sums.index}
    keys_sorted = sorted(receita_por.keys())
    imp_por = _allocate_imposto_total_centavos(keys_sorted, receita_por, float(fiscal_imposto_valor))

    out: list[SkuGerencialMargem] = []
    for sku_key_s in keys_sorted:
        r = float(receita_por[sku_key_s])
        c = float(sums.loc[sku_key_s, "_com"])
        fp_ = float(sums.loc[sku_key_s, "_fp"])
        cmv_ = float(sums.loc[sku_key_s, "_cmv"])
        ftp_ = float(sums.loc[sku_key_s, "_ftp"])
        desp_ = float(sums.loc[sku_key_s, "_desp"])
        ads_v_ = float(sums.loc[sku_key_s, "_ads_v"])
        ads_f_ = float(sums.loc[sku_key_s, "_ads_f"])
        naive_ = float(sums.loc[sku_key_s, "_naive"])
        imp_ = float(imp_por.get(sku_key_s, 0.0))
        ro = r - c - fp_ - cmv_ - ftp_ - imp_ - ads_v_
        rl = r - c - fp_ - cmv_ - ftp_ - imp_ - desp_ - ads_v_ - ads_f_
        mop = (ro / r * 100.0) if r > 1e-12 else 0.0
        mlq = (rl / r * 100.0) if r > 1e-12 else 0.0
        out.append(
            SkuGerencialMargem(
                sku=str(sku_key_s),
                receita=r,
                resultado_operacional=ro,
                resultado_liquido=rl,
                margem_operacional_pct=mop,
                margem_liquida_pct=mlq,
                resultado_coluna_naive=naive_,
            )
        )

    out.sort(key=lambda x: x.resultado_liquido)
    return out
