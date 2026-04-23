"""
Ficha por pedido (Resultado Gerencial Ciclo C) — composição, benchmarks e diagnóstico.

Consome apenas ``compute_tabela_por_pedido`` / slice já materializado; não altera KPIs nem agregações base.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import pandas as pd

from comercial_pedidos_analise import pedido_id_series
from processing.faturamento.config import CUSTO_UNITARIO_COL, SKU_NORMALIZADO_COL

from processing.faturamento.resultado_gerencial_slice import (
    PedidoGerencialRow,
    ResultadoGerencialSlice,
    _frete_tp_linha_series,
    _receita_linha_series,
    compute_tabela_por_pedido,
)

_logger = logging.getLogger(__name__)

DESC_COL_FALLBACK_ORDER: tuple[str, ...] = (
    "produto_resumo",
    "Descrição",
    "descricao_produto",
    "Nome do produto",
    "Titulo curto ML",
)


@dataclass(frozen=True)
class ItemPedido:
    sku: str
    descricao: str
    quantidade: int
    custo_unitario: float
    preco_unitario: float


@dataclass(frozen=True)
class DiagnosticoItem:
    tipo: str  # saudavel | atencao | risco
    titulo: str
    explicacao: str


@dataclass(frozen=True)
class ComparacaoPedido:
    margem_pedido: float  # líquida % (0–100 escala UI)
    margem_plataforma: float | None
    delta_plataforma_pp: float | None
    margem_sku: float | None
    delta_sku_pp: float | None
    margem_empresa: float | None
    delta_empresa_pp: float | None


@dataclass(frozen=True)
class BenchmarksEmpresa:
    """Referências para diagnóstico (pool exclui o próprio pedido quando possível)."""

    cmv_pct_medio_empresa_outros: float | None  # pooled CMV/rec outras vendas empresa
    margem_operacional_benchmark_frac: float | None


@dataclass(frozen=True)
class FichaPedido:
    pedido_id: str
    plataforma: str
    empresa: str
    data_venda: Any  # datetime
    status_nf: str
    numero_pedido_ui: str

    receita: float
    comissao: float
    frete_plataforma: float
    frete_tp: float
    cmv: float
    imposto_rateado: float
    despesa_fixa: float
    ads_rateado: float
    resultado_operacional: float
    resultado_liquido: float

    comissao_pct: float
    frete_plataforma_pct: float
    frete_tp_pct: float
    cmv_pct: float
    imposto_pct: float
    despesa_fixa_pct: float
    ads_pct: float
    margem_operacional_pct: float
    margem_liquida_pct: float

    itens: tuple[ItemPedido, ...]
    diagnosticos: tuple[DiagnosticoItem, ...]
    comparacao: ComparacaoPedido


def _pct(parte: float, receita: float) -> float:
    if receita <= 1e-18:
        return 0.0
    return parte / receita * 100.0


def _primeira_col_descricao(df: pd.DataFrame) -> str | None:
    for c in DESC_COL_FALLBACK_ORDER:
        if c in df.columns:
            return c
    return None


def _extrair_itens_pedido(gr: pd.DataFrame) -> list[ItemPedido]:
    col_sku = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in gr.columns else "Código"
    if col_sku not in gr.columns:
        return []

    desc_col = _primeira_col_descricao(gr)
    rec = _receita_linha_series(gr).astype(float)
    cmv = pd.to_numeric(gr["Custo_Produto_Total"], errors="coerce").fillna(0.0)
    qtd_raw = pd.to_numeric(gr["Quantidade"], errors="coerce").fillna(0.0) if "Quantidade" in gr.columns else pd.Series(1.0, index=gr.index)

    lista_pl = (
        pd.to_numeric(gr["Preço de lista"], errors="coerce").fillna(0.0) if "Preço de lista" in gr.columns else pd.Series(0.0, index=gr.index)
    )

    custo_u = pd.Series(0.0, index=gr.index)
    if CUSTO_UNITARIO_COL in gr.columns:
        custo_u = pd.to_numeric(gr[CUSTO_UNITARIO_COL], errors="coerce").fillna(0.0)
    else:
        custo_u = (cmv / qtd_raw.replace(0.0, float("nan"))).fillna(0.0)

    out: list[ItemPedido] = []
    for idx in gr.index:
        sku = str(gr.loc[idx, col_sku]).strip() or "—"
        if desc_col:
            d = str(gr.loc[idx, desc_col]).strip()
            if not d or d.casefold() in {"nan", "none"}:
                d = sku
        else:
            d = sku
        q = float(qtd_raw.loc[idx])
        qi = int(round(q)) if q > 0 else 1
        r_lin = float(rec.loc[idx])
        cmv_lin = float(cmv.loc[idx])
        cu = float(custo_u.loc[idx])
        if cu <= 0 and qi > 0:
            cu = cmv_lin / float(qi)
        pl = float(lista_pl.loc[idx])
        if pl <= 0 and qi > 0:
            pl = r_lin / float(qi)
        out.append(ItemPedido(sku=sku, descricao=d, quantidade=qi, custo_unitario=cu, preco_unitario=pl))
    return out


def compute_benchmarks_comparacao(
    *,
    pedidos_contexto: list[PedidoGerencialRow],
    pedido_alvo: PedidoGerencialRow,
    df_linhas: pd.DataFrame,
    fiscal_imposto_valor: float = 0.0,
) -> ComparacaoPedido:
    """Médias no mesmo universo filtrado que a tabela; exclui o próprio pedido."""
    outros = [p for p in pedidos_contexto if p.pedido_id != pedido_alvo.pedido_id]
    mp = pedido_alvo.margem_liquida_pct

    plat = pedido_alvo.plataforma.strip().casefold()
    emp = pedido_alvo.empresa.strip().casefold()

    def _marg_media_acum(rows: list[PedidoGerencialRow]) -> float | None:
        if not rows:
            return None
        sr = sum(p.receita for p in rows)
        sl = sum(p.resultado_liquido for p in rows)
        if sr <= 1e-18:
            return None
        return sl / sr * 100.0

    outros_plat = [p for p in outros if p.plataforma.strip().casefold() == plat]
    outros_emp = [p for p in outros if p.empresa.strip().casefold() == emp]

    m_pl = _marg_media_acum(outros_plat)
    m_em = _marg_media_acum(outros_emp)

    d_pl = (mp - m_pl) if m_pl is not None else None
    d_em = (mp - m_em) if m_em is not None else None

    m_sku: float | None = None
    d_sk: float | None = None
    if df_linhas is not None and len(df_linhas) > 0:
        pid_series = pedido_id_series(df_linhas).astype(str).str.strip()
        col_sku = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in df_linhas.columns else "Código"
        sub = df_linhas.loc[pid_series.eq(pedido_alvo.pedido_id)]
        if col_sku in sub.columns and len(sub) > 0:
            rec_lin = _receita_linha_series(sub)
            receita_skus: dict[str, float] = {}
            for idx in sub.index:
                sk = str(sub.loc[idx, col_sku]).strip()
                if not sk:
                    continue
                receita_skus[sk] = receita_skus.get(sk, 0.0) + float(rec_lin.loc[idx])
            tw = sum(receita_skus.values())
            if tw > 1e-18:
                acc_w = 0.0
                covered = False
                for sk, rv in receita_skus.items():
                    rel = [p for p in outros if sk in p.skus]
                    if not rel:
                        continue
                    sr = sum(p.receita for p in rel)
                    sl = sum(p.resultado_liquido for p in rel)
                    if sr <= 1e-18:
                        continue
                    mref = sl / sr * 100.0
                    acc_w += mref * (rv / tw)
                    covered = True
                if covered:
                    m_sku = acc_w

    if m_sku is not None:
        d_sk = mp - m_sku

    return ComparacaoPedido(
        margem_pedido=mp,
        margem_plataforma=m_pl,
        delta_plataforma_pp=d_pl,
        margem_sku=m_sku,
        delta_sku_pp=d_sk,
        margem_empresa=m_em,
        delta_empresa_pp=d_em,
    )


def compute_benchmarks_empresa(
    *,
    pedidos_contexto: list[PedidoGerencialRow],
    pedido_alvo: PedidoGerencialRow,
    rg_config: dict[str, Any],
) -> BenchmarksEmpresa:
    outros = [p for p in pedidos_contexto if p.pedido_id != pedido_alvo.pedido_id and p.empresa.strip().casefold() == pedido_alvo.empresa.strip().casefold()]
    rec = sum(p.receita for p in outros)
    cmv = sum(p.cmv for p in outros)
    cmv_pct = (cmv / rec * 100.0) if rec > 1e-18 else None
    bench_yaml = rg_config.get("benchmarks") or {}
    m_op_b = bench_yaml.get("margem_operacional_saudavel")
    m_op_bf = float(m_op_b) if m_op_b is not None else None
    return BenchmarksEmpresa(cmv_pct_medio_empresa_outros=cmv_pct, margem_operacional_benchmark_frac=m_op_bf)


def compute_diagnostico_automatico(
    ficha: FichaPedido,
    benchmarks: BenchmarksEmpresa,
    rg_config: dict[str, Any],
    *,
    comissao_esperada_frac: float | None,
    plataforma_slug: str = "",
) -> tuple[DiagnosticoItem, ...]:
    bench_yaml = rg_config.get("benchmarks") or {}
    thr_cmv_pp = float(bench_yaml.get("cmv_alert_above_empresa_pp", 3.0))
    thr_com_pp = float(bench_yaml.get("comissao_alert_above_pp", 1.0))
    thr_fp_pp = float(bench_yaml.get("frete_plat_alert_above_sku_pp", 2.0))
    m_op_ok = bench_yaml.get("margem_operacional_saudavel")
    m_op_ok_f = float(m_op_ok) if m_op_ok is not None else None
    if m_op_ok_f is None and benchmarks.margem_operacional_benchmark_frac is not None:
        m_op_ok_f = float(benchmarks.margem_operacional_benchmark_frac)
    _ = plataforma_slug

    cards: list[DiagnosticoItem] = []

    # Risco operacional
    if ficha.resultado_operacional < -1e-6:
        cards.append(
            DiagnosticoItem(
                tipo="risco",
                titulo="Pedido não cobre os custos diretos",
                explicacao=f"Resultado operacional R$ {ficha.resultado_operacional:,.2f}. Avalie preço ou mix.",
            )
        )
    elif m_op_ok_f is not None and ficha.receita > 1e-18:
        mop = ficha.resultado_operacional / ficha.receita
        if mop >= m_op_ok_f:
            cards.append(
                DiagnosticoItem(
                    tipo="saudavel",
                    titulo=f"Operacional saudável ({mop * 100:.1f}% vs benchmark {m_op_ok_f * 100:.1f}%)",
                    explicacao="Margem operacional acima do mínimo configurado.",
                )
            )

    # Cobertura parcial
    if ficha.resultado_operacional >= -1e-9 and ficha.resultado_liquido < -1e-9:
        cards.append(
            DiagnosticoItem(
                tipo="atencao",
                titulo="Cobertura parcial — não cobre estrutura",
                explicacao="Operacional positivo; líquido negativo por despesa fixa e ADS fixo rateados.",
            )
        )

    # CMV vs empresa
    if benchmarks.cmv_pct_medio_empresa_outros is not None and ficha.receita > 1e-18:
        cmv_p = ficha.cmv / ficha.receita * 100.0
        if cmv_p > benchmarks.cmv_pct_medio_empresa_outros + thr_cmv_pp:
            cards.append(
                DiagnosticoItem(
                    tipo="atencao",
                    titulo="CMV acima da média",
                    explicacao=f"{cmv_p:.1f}% vs média empresa {benchmarks.cmv_pct_medio_empresa_outros:.1f}% (+{thr_cmv_pp:.0f} pp).",
                )
            )

    # Comissão vs esperada categoria
    if comissao_esperada_frac is not None and ficha.receita > 1e-18:
        com_p = ficha.comissao / ficha.receita
        if com_p > comissao_esperada_frac + thr_com_pp / 100.0:
            cards.append(
                DiagnosticoItem(
                    tipo="atencao",
                    titulo="Comissão acima do esperado",
                    explicacao=f"{com_p * 100:.1f}% vs padrão {comissao_esperada_frac * 100:.1f}% (+{thr_com_pp:.0f} pp).",
                )
            )

    # Frete plataforma vs SKU (precisa médias por SKU — simplificado: usa só se rg_config trouxe map pré-calculado)
    fp_map = rg_config.get("_frete_plat_pct_medio_por_sku") or {}
    if isinstance(fp_map, dict) and ficha.receita > 1e-18:
        fp_ped = ficha.frete_plataforma / ficha.receita * 100.0
        for it in ficha.itens:
            ref = fp_map.get(it.sku)
            if ref is not None and fp_ped > float(ref) + thr_fp_pp:
                cards.append(
                    DiagnosticoItem(
                        tipo="atencao",
                        titulo="Frete plataforma acima da média para este SKU",
                        explicacao=f"{fp_ped:.1f}% vs média SKU ~{float(ref):.1f}% (+{thr_fp_pp:.0f} pp).",
                    )
                )
                break

    tem_alerta = any(c.tipo in ("risco", "atencao") for c in cards)
    tem_pos = any(c.tipo == "saudavel" for c in cards)
    if not tem_pos and not tem_alerta and ficha.resultado_liquido >= -1e-9 and ficha.resultado_operacional >= -1e-9:
        cards.append(
            DiagnosticoItem(
                tipo="saudavel",
                titulo="Pedido dentro do esperado",
                explicacao=f"Margem líquida {ficha.margem_liquida_pct:.1f}% no recorte atual.",
            ),
        )

    # Prioridade e limite 5
    prio = {"risco": 0, "atencao": 1, "saudavel": 2}
    cards.sort(key=lambda x: prio.get(x.tipo, 9))
    return tuple(cards[:5])


def compute_ficha_pedido(
    slice_: ResultadoGerencialSlice,
    *,
    pedido_id: str,
    fiscal_imposto_valor: float,
    pedidos_contexto: list[PedidoGerencialRow],
    rg_config: dict[str, Any],
    tab_linhas_full: list[PedidoGerencialRow] | None = None,
) -> FichaPedido | None:
    if tab_linhas_full is not None:
        tab = tab_linhas_full
    else:
        tab = compute_tabela_por_pedido(slice_, fiscal_imposto_valor=float(fiscal_imposto_valor))
    row = next((r for r in tab if r.pedido_id == pedido_id), None)
    if row is None:
        return None

    r = row.receita
    cmpobj = compute_benchmarks_comparacao(
        pedidos_contexto=pedidos_contexto,
        pedido_alvo=row,
        df_linhas=slice_.df_linha,
        fiscal_imposto_valor=float(fiscal_imposto_valor),
    )
    bench_e = compute_benchmarks_empresa(pedidos_contexto=pedidos_contexto, pedido_alvo=row, rg_config=rg_config)

    pid_mask = pedido_id_series(slice_.df_linha).astype(str).str.strip().eq(str(pedido_id))
    gr = slice_.df_linha.loc[pid_mask].copy()
    itens = tuple(_extrair_itens_pedido(gr))

    ads_total = float(row.ads_variavel + row.ads_fixo)

    frete_plat_pct_medio_por_sku = _frete_plat_pct_ref_por_sku(slice_.df_linha, pedido_id)
    cfg2 = dict(rg_config)
    cfg2["_frete_plat_pct_medio_por_sku"] = frete_plat_pct_medio_por_sku

    pl_slug = _slug_plataforma(row.plataforma)
    cat_cfg = rg_config.get("comissao_categoria") or {}
    plat_cat = cat_cfg.get(pl_slug) or cat_cfg.get("padrao") or {}
    comissao_esp = None
    if isinstance(plat_cat, dict):
        comissao_esp = plat_cat.get("padrao")
        if comissao_esp is None and plat_cat:
            comissao_esp = next(iter(plat_cat.values()))
        if comissao_esp is not None:
            comissao_esp = float(comissao_esp)

    ficha_core = FichaPedido(
        pedido_id=row.pedido_id,
        plataforma=row.plataforma,
        empresa=row.empresa,
        data_venda=row.data_venda,
        status_nf=row.status_nf,
        numero_pedido_ui=row.numero_pedido_ui,
        receita=r,
        comissao=row.comissao,
        frete_plataforma=row.frete_plataforma,
        frete_tp=row.frete_tp,
        cmv=row.cmv,
        imposto_rateado=row.imposto_rateado,
        despesa_fixa=row.despesa_fixa,
        ads_rateado=ads_total,
        resultado_operacional=row.resultado_operacional,
        resultado_liquido=row.resultado_liquido,
        comissao_pct=_pct(row.comissao, r),
        frete_plataforma_pct=_pct(row.frete_plataforma, r),
        frete_tp_pct=_pct(row.frete_tp, r),
        cmv_pct=_pct(row.cmv, r),
        imposto_pct=_pct(row.imposto_rateado, r),
        despesa_fixa_pct=_pct(row.despesa_fixa, r),
        ads_pct=_pct(ads_total, r),
        margem_operacional_pct=row.margem_operacional_pct,
        margem_liquida_pct=row.margem_liquida_pct,
        itens=itens,
        diagnosticos=tuple(),
        comparacao=cmpobj,
    )

    diag = compute_diagnostico_automatico(
        ficha_core,
        bench_e,
        cfg2,
        comissao_esperada_frac=comissao_esp,
        plataforma_slug=pl_slug,
    )

    return replace(ficha_core, diagnosticos=diag)


def _slug_plataforma(plataforma: str) -> str:
    p = str(plataforma).strip().casefold()
    if "mercado" in p or p == "ml":
        return "mercado_livre"
    if "shopee" in p:
        return "shopee"
    return "padrao"


def _frete_plat_pct_ref_por_sku(df: pd.DataFrame, pedido_excluir: str) -> dict[str, float]:
    """Média simples de frete_plataforma/receita_linha por SKU (linhas fora do pedido)."""
    if df.empty or "Nome da plataforma" not in df.columns:
        return {}
    pids = pedido_id_series(df).astype(str).str.strip()
    m = pids.ne(pedido_excluir) & pids.ne("")
    sub = df.loc[m].copy()
    if sub.empty:
        return {}
    col_sku = SKU_NORMALIZADO_COL if SKU_NORMALIZADO_COL in sub.columns else "Código"
    if col_sku not in sub.columns:
        return {}
    rec = _receita_linha_series(sub)
    fp = pd.to_numeric(sub["Frete_Plataforma"], errors="coerce").fillna(0.0)
    ratios: dict[str, list[float]] = {}
    for i, sku in enumerate(sub[col_sku].fillna("").astype(str).str.strip()):
        if not sku:
            continue
        rv = float(rec.iloc[i])
        if rv <= 1e-18:
            continue
        ratios.setdefault(sku, []).append(float(fp.iloc[i]) / rv * 100.0)
    return {k: sum(v) / len(v) for k, v in ratios.items()}


def load_resultado_gerencial_config(org_slug: str | None) -> dict[str, Any]:
    """Carrega YAML do cliente + defaults; fallback sem PyYAML usa defaults embutidos."""
    root = Path(__file__).resolve().parents[2]
    defaults_path = root / "config" / "default_resultado_gerencial.yaml"
    merged: dict[str, Any] = {}
    if defaults_path.is_file():
        merged.update(_normalize_rg_yaml(_load_yaml_safe(defaults_path)))
    else:
        merged.update(_embedded_default_rg_config())
        _logger.warning("Arquivo %s não encontrado — usando defaults embutidos", defaults_path)
    if org_slug:
        client_path = root / "config" / f"{str(org_slug).strip()}.yaml"
        if client_path.is_file():
            merged.update(_normalize_rg_yaml(_load_yaml_safe(client_path)))
        else:
            _logger.warning("benchmarks não configurados para %s, usando defaults genéricos", org_slug)
    return merged


def _normalize_rg_yaml(raw: dict[str, Any]) -> dict[str, Any]:
    inner = raw.get("resultado_gerencial")
    if isinstance(inner, dict):
        return dict(inner)
    return dict(raw)


def _embedded_default_rg_config() -> dict[str, Any]:
    return {
        "benchmarks": {
            "margem_operacional_saudavel": 0.20,
            "margem_liquida_saudavel": 0.15,
            "cmv_pct_max_aceitavel": 0.50,
            "cmv_alert_above_empresa_pp": 3.0,
            "comissao_alert_above_pp": 1.0,
            "frete_plat_alert_above_sku_pp": 2.0,
        },
        "comissao_categoria": {
            "mercado_livre": {"padrao": 0.14},
            "shopee": {"padrao": 0.20},
            "padrao": {"padrao": 0.12},
        },
    }


def _load_yaml_safe(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except ImportError:
        _logger.warning("PyYAML não instalado — usando apenas defaults embutidos para Resultado Gerencial")
        return {}
    try:
        raw = path.read_text(encoding="utf-8")
        data = yaml.safe_load(raw)
        return data if isinstance(data, dict) else {}
    except Exception as exc:
        _logger.warning("Falha ao ler %s: %s", path, exc)
        return {}
