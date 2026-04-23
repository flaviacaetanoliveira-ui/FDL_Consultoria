"""
Agregação Resultado Gerencial por plataforma (canal).

Reuso da tabela por pedido já materializada — sem novo Parquet nem ETL.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from typing import Optional

from faturamento_dre_recorte_minimo import nf_grain_plataforma_label_for_ui, nf_grain_plataforma_match_key

from processing.faturamento.formatacao_display_rg import fmt_brl_ptbr_celula, fmt_pct_um_decimal
from processing.faturamento.resultado_gerencial_slice import PedidoGerencialRow, ResultadoGerencialSlice

PLATAFORMA_EXCECAO_LABEL = "Não identificado"


def classifica_nivel_plataforma(plataforma_display: str, margem_liquida_pct: float, benchmark: float) -> str:
    """Nível exibido na coluna «Nível»; «Não identificado» não recebe «Alto» por margem."""
    if str(plataforma_display).strip() == PLATAFORMA_EXCECAO_LABEL:
        return "—"
    if float(margem_liquida_pct) < 0:
        return "Risco"
    if float(margem_liquida_pct) >= float(benchmark):
        return "Alto"
    return "Neutro"


@dataclass(frozen=True)
class LinhaPlataforma:
    plataforma: str
    pedidos: int
    receita: float
    receita_display: str
    resultado_operacional: float
    resultado_liquido: float
    margem_operacional_pct: float
    margem_operacional_display: str
    margem_liquida_pct: float
    margem_liquida_display: str
    pct_da_receita: float  # 0–1


@dataclass(frozen=True)
class AnalisePlataforma:
    linhas: tuple[LinhaPlataforma, ...]
    receita_total: float
    resultado_liquido_total: float
    plataforma_mais_rentavel: Optional[str]
    plataforma_mais_volume: Optional[str]


def _norm_key(raw: object) -> str:
    if raw is None:
        return "__sem_plataforma__"
    xs = str(raw).strip()
    if xs in {"", "-", "—"} or xs.casefold() in {"nan", "none", "<na>"}:
        return "__sem_plataforma__"
    k = nf_grain_plataforma_match_key(raw)
    return k if k else "__sem_plataforma__"


def _normaliza_token_plataforma_bruto(raw: object) -> str | None:
    """Normaliza texto cru antes do agrupamento (nulo / traço → None)."""
    if raw is None:
        return None
    xs = str(raw).strip()
    if xs.casefold() in {"", "-", "—", "nan", "none", "<na>"}:
        return None
    return xs


def _display_label(raw_samples: list[object]) -> str:
    """Um rótulo estável por grupo canônico (preferência por frequência no recorte)."""
    labels = [
        nf_grain_plataforma_label_for_ui(x)
        for x in raw_samples
        if _normaliza_token_plataforma_bruto(x) is not None
    ]
    labels = [x for x in labels if x not in ("—", "")]
    if not labels:
        return "Não identificado"
    c = Counter(labels)
    return sorted(c.keys(), key=lambda lab: (-c[lab], lab))[0]


def _linha_plataforma(
    *,
    plataforma: str,
    pedidos: int,
    rec: float,
    rop: float,
    rliq: float,
    m_op: float,
    m_liq: float,
    pct: float,
) -> LinhaPlataforma:
    return LinhaPlataforma(
        plataforma=plataforma,
        pedidos=pedidos,
        receita=rec,
        receita_display=fmt_brl_ptbr_celula(rec),
        resultado_operacional=rop,
        resultado_liquido=rliq,
        margem_operacional_pct=m_op,
        margem_operacional_display=fmt_pct_um_decimal(m_op),
        margem_liquida_pct=m_liq,
        margem_liquida_display=fmt_pct_um_decimal(m_liq),
        pct_da_receita=pct,
    )


def compute_analise_plataforma(
    *,
    slice_rg: ResultadoGerencialSlice,
    pedidos_tabela: list[PedidoGerencialRow],
    kp_rg: dict[str, float | int],
) -> AnalisePlataforma:
    """
    Agrega pedidos por chave canônica de plataforma (nf_grain_plataforma_match_key).

    Margens são sempre ``sum(resultado*) / sum(receita)`` por canal — nunca média das margens por pedido.
    """
    _ = slice_rg
    receita_kpi = float(kp_rg["valor_venda_lista"])
    res_liq_kpi = float(kp_rg.get("resultado_liquido", kp_rg["resultado"]))

    if not pedidos_tabela:
        return AnalisePlataforma(
            linhas=(),
            receita_total=receita_kpi,
            resultado_liquido_total=res_liq_kpi,
            plataforma_mais_rentavel=None,
            plataforma_mais_volume=None,
        )

    acc: dict[str, dict[str, object]] = defaultdict(
        lambda: {
            "rec": 0.0,
            "rop": 0.0,
            "rliq": 0.0,
            "pids": set(),
            "raw": [],
        }
    )

    for row in pedidos_tabela:
        key = _norm_key(row.plataforma)
        g = acc[key]
        g["rec"] = float(g["rec"]) + float(row.receita)
        g["rop"] = float(g["rop"]) + float(row.resultado_operacional)
        g["rliq"] = float(g["rliq"]) + float(row.resultado_liquido)
        pids: set[str] = g["pids"]  # type: ignore[assignment]
        pids.add(str(row.pedido_id).strip())
        raw_list: list = g["raw"]  # type: ignore[assignment]
        raw_list.append(row.plataforma)

    rows_tmp: list[LinhaPlataforma] = []
    for key, g in acc.items():
        rec = float(g["rec"])
        rop = float(g["rop"])
        rliq = float(g["rliq"])
        pids = g["pids"]
        raw_s = g["raw"]
        assert isinstance(pids, set)
        ped_n = len(pids)
        if ped_n <= 0 or rec <= 1e-18:
            continue
        m_op = (rop / rec * 100.0) if rec > 1e-18 else 0.0
        m_liq = (rliq / rec * 100.0) if rec > 1e-18 else 0.0
        label = _display_label(list(raw_s))
        if key == "__sem_plataforma__" or label in ("—", "Sem plataforma"):
            label = "Não identificado"

        rows_tmp.append(
            _linha_plataforma(
                plataforma=label,
                pedidos=ped_n,
                rec=rec,
                rop=rop,
                rliq=rliq,
                m_op=m_op,
                m_liq=m_liq,
                pct=0.0,
            )
        )

    denom = receita_kpi if receita_kpi > 1e-18 else sum(r.receita for r in rows_tmp)
    final_rows: list[LinhaPlataforma] = []
    if denom > 1e-18 and rows_tmp:
        gross = [r.receita / denom for r in rows_tmp]
        drift = 1.0 - sum(gross)
        adj = list(gross)
        if adj and abs(drift) > 1e-12:
            i_max = max(range(len(adj)), key=lambda i: adj[i])
            adj[i_max] = max(0.0, adj[i_max] + drift)
        for r, p in zip(rows_tmp, adj, strict=True):
            final_rows.append(
                _linha_plataforma(
                    plataforma=r.plataforma,
                    pedidos=r.pedidos,
                    rec=r.receita,
                    rop=r.resultado_operacional,
                    rliq=r.resultado_liquido,
                    m_op=r.margem_operacional_pct,
                    m_liq=r.margem_liquida_pct,
                    pct=float(p),
                )
            )
    else:
        final_rows = [
            _linha_plataforma(
                plataforma=r.plataforma,
                pedidos=r.pedidos,
                rec=r.receita,
                rop=r.resultado_operacional,
                rliq=r.resultado_liquido,
                m_op=r.margem_operacional_pct,
                m_liq=r.margem_liquida_pct,
                pct=r.pct_da_receita,
            )
            for r in rows_tmp
        ]

    final_rows = [r for r in final_rows if r.pedidos > 0 and r.receita > 1e-18]
    final_rows.sort(key=lambda x: x.pct_da_receita, reverse=True)
    linhas_t = tuple(final_rows)

    mais_vol = max(linhas_t, key=lambda x: x.receita).plataforma if linhas_t else None
    mais_rent = (
        max(linhas_t, key=lambda x: (x.margem_liquida_pct, x.receita)).plataforma if linhas_t else None
    )

    return AnalisePlataforma(
        linhas=linhas_t,
        receita_total=receita_kpi,
        resultado_liquido_total=res_liq_kpi,
        plataforma_mais_rentavel=mais_rent,
        plataforma_mais_volume=mais_vol,
    )
