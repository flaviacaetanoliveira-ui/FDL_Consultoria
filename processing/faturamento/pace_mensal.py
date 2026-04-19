"""
Termômetro de pace mensal — receita realizada vs meta (YAML > MA3 > sem meta).

Projeção linear por dias corridos (v1). Sem alteração de pipelines de KPI/DRE.
"""

from __future__ import annotations

import logging
import re
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from typing import Any, Optional

import pandas as pd

from processing.faturamento.resultado_gerencial_slice import (
    ResultadoGerencialSlice,
    _receita_linha_series,
)

_logger = logging.getLogger(__name__)


def _last_day_month(y: int, m: int) -> date:
    return date(y, m, monthrange(y, m)[1])


def _slug_empresa(name: str) -> str:
    s = str(name).strip().lower()
    s = re.sub(r"[^\w]+", "_", s, flags=re.UNICODE)
    return re.sub(r"_+", "_", s).strip("_")


def _is_calendario_mes_cheio(d0: date, d1: date) -> bool:
    if d0.day != 1:
        return False
    ult = _last_day_month(d0.year, d0.month)
    return d0.year == d1.year and d0.month == d1.month and d1 == ult


def determinar_modo(data_inicio: date, data_fim: date, hoje: date) -> str:
    """``mes_corrente`` | ``recorte_parcial`` | ``mes_fechado``."""
    if not _is_calendario_mes_cheio(data_inicio, data_fim):
        return "recorte_parcial"
    if data_fim < hoje:
        return "mes_fechado"
    return "mes_corrente"


def explicar_motivo_pace_none(
    *,
    n_linhas: int,
    data_inicio: date,
    data_fim: date,
    hoje: date,
) -> str:
    """
    Mensagem curta para debug (admin / ``FDL_RG_PACE_DEBUG``) quando ``compute_pace_mensal`` devolve ``None``.
    Mantém texto estável para testes de regressão.
    """
    if int(n_linhas) <= 0:
        return (
            "compute_pace_mensal retornou None · n_linhas=0 (sem linhas no slice — "
            "período/empresa/plataforma sem registros na base linha)"
        )
    modo = determinar_modo(data_inicio, data_fim, hoje)
    yref, mref = data_inicio.year, data_inicio.month
    if modo == "mes_corrente" and (hoje.year != yref or hoje.month != mref):
        return (
            f"compute_pace_mensal retornou None · hoje ({hoje.isoformat()}) fora do mês civil do filtro "
            f"({mref:02d}/{yref}); ritmo só quando o relógio do servidor está nesse mês"
        )
    return (
        f"compute_pace_mensal retornou None · modo={modo} — caso não catalogado "
        "(abrir issue com este texto)"
    )


def compute_trailing_monthly_revenues(
    df_linha: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    mes_referencia: tuple[int, int],
) -> list[float]:
    """
    Últimos 3 meses **fechados** estritamente antes de ``mes_referencia`` (ano, mês).
    Ordem cronológica [mais antigo, ..., mais recente].
    """
    from faturamento_dre_recorte import _fdl_fr_filtrar_por_etiquetas_empresa
    from faturamento_dre_recorte_minimo import nf_grain_plataforma_match_key

    if df_linha.empty or "Data" not in df_linha.columns:
        return []

    df = df_linha
    if empresas_sel:
        df = _fdl_fr_filtrar_por_etiquetas_empresa(df, list(empresas_sel))
    if plataformas_sel:
        want = {nf_grain_plataforma_match_key(x) for x in plataformas_sel}
        want.discard("")
        got = df["Nome da plataforma"].map(nf_grain_plataforma_match_key)
        df = df.loc[got.isin(want)].copy()

    ts = pd.to_datetime(df["Data"], errors="coerce", dayfirst=True)
    df = df.loc[ts.notna()].copy()
    rev = _receita_linha_series(df)
    df["_period"] = ts.dt.to_period("M")
    df["_rev"] = rev
    agg = df.groupby("_period", sort=True)["_rev"].sum()

    y0, m0 = mes_referencia
    # primeiro mês que não entra no MA3 é o mês civil corrente do recorte
    cur = pd.Period(year=y0, month=m0, freq="M")
    perds = [cur - 3, cur - 2, cur - 1]
    out: list[float] = []
    for p in perds:
        out.append(float(agg.get(p, 0.0)))
    return out


def _pace_cfg(cliente_config: dict[str, Any]) -> dict[str, Any]:
    pace = cliente_config.get("pace")
    return pace if isinstance(pace, dict) else {}


def _lookup_yaml_meta(meta_map: dict[str, Any], empresa: str) -> float | None:
    if not meta_map or not isinstance(meta_map, dict):
        return None
    cand = [empresa.strip(), _slug_empresa(empresa)]
    for k in cand:
        if k and k in meta_map:
            try:
                return float(meta_map[k])
            except (TypeError, ValueError):
                return None
        lk = k.lower()
        for mk, mv in meta_map.items():
            if str(mk).strip().lower() == lk:
                try:
                    return float(mv)
                except (TypeError, ValueError):
                    return None
    return None


def _resolver_meta_consolidado(
    *,
    cliente_config: dict[str, Any],
    empresas_sel: tuple[str, ...],
    hist_mensal_trailing: list[float],
    hist_por_empresa: dict[str, list[float]],
) -> tuple[float | None, str]:
    """Soma metas por empresa (YAML > MA3 da empresa); fallback MA3 consolidado."""
    cfg = _pace_cfg(cliente_config)
    meta_map = cfg.get("meta_mensal")
    padrao = cfg.get("meta_mensal_padrao")
    padrao_f: float | None = None
    if padrao is not None:
        try:
            padrao_f = float(padrao)
        except (TypeError, ValueError):
            padrao_f = None

    metas: list[float] = []
    origens: list[str] = []

    if not empresas_sel:
        # todas as empresas — só MA3 consolidado ou padrao
        if isinstance(meta_map, dict) and meta_map:
            pass
        if padrao_f is not None:
            return padrao_f, "yaml"
        if len(hist_mensal_trailing) >= 3:
            return sum(hist_mensal_trailing[-3:]) / 3.0, "ma3"
        return None, "sem_meta"

    for emp in empresas_sel:
        mv = None
        origem = "sem_meta"
        if isinstance(meta_map, dict):
            mv = _lookup_yaml_meta(meta_map, emp)
            if mv is not None:
                origem = "yaml"
        if mv is None and padrao_f is not None:
            mv = padrao_f
            origem = "yaml"
        if mv is None:
            he = hist_por_empresa.get(str(emp).strip()) or hist_por_empresa.get(_slug_empresa(emp))
            if he is not None and len(he) >= 3:
                mv = sum(he[-3:]) / 3.0
                origem = "ma3"
        if mv is not None:
            metas.append(float(mv))
            origens.append(origem)

    if metas:
        total = sum(metas)
        if all(o == "yaml" for o in origens):
            return total, "yaml"
        if all(o == "ma3" for o in origens):
            return total, "ma3"
        return total, "mix"

    if len(hist_mensal_trailing) >= 3:
        return sum(hist_mensal_trailing[-3:]) / 3.0, "ma3"
    return None, "sem_meta"


def _classificar_alerta(desvio: float | None, *, tem_meta: bool) -> tuple[str, str | None]:
    """Retorna (nivel_alerta, mensagem opcional)."""
    if not tem_meta or desvio is None:
        return "ok", None
    d = float(desvio)
    if d > 0.05:
        return "ok_positivo", None
    if d >= -0.03:
        return "ok", None
    if d >= -0.10:
        msg = (
            f"Ritmo {abs(d)*100:.1f}% abaixo da meta projetada. "
            "Investigue pipeline comercial ou revise a meta no YAML."
        )
        return "atencao", msg
    msg = (
        f"Ritmo {abs(d)*100:.1f}% abaixo da meta projetada — risco elevado de não cumprir o mês."
    )
    return "critico", msg


def _mensagem_ritmo_necessario(
    *,
    receita: float,
    meta: float,
    dias_restantes: int,
    ritmo_atual: float,
    ritmo_nec: float,
) -> str | None:
    if dias_restantes <= 0 or meta <= 0:
        return None
    adj = (ritmo_nec / ritmo_atual) - 1.0 if ritmo_atual > 1e-9 else 0.0
    if abs(adj) <= 0.03:
        return None
    return (
        f"Restam {dias_restantes} dias. Para atingir {_fmt_brl_curto(meta)}, o ritmo diário "
        f"precisa subir de {_fmt_brl_curto(ritmo_atual)} para {_fmt_brl_curto(ritmo_nec)} "
        f"({adj*100:+.1f}% vs ritmo atual)."
    )


def _fmt_brl_curto(v: float) -> str:
    av = abs(float(v))
    if av >= 1_000_000:
        return f"R$ {v/1_000_000:.2f}M".replace(".", ",")
    if av >= 1000:
        return f"R$ {v/1000:.1f}k".replace(".", ",")
    return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")


@dataclass(frozen=True)
class PaceMensal:
    mes_referencia: str
    dia_atual: int
    dias_totais_periodo: int
    dias_restantes: int
    modo: str

    receita_realizada: float
    pct_meta_realizada: float

    meta_mensal: Optional[float]
    meta_origem: str

    projecao_linear: Optional[float]
    desvio_projecao_pct: Optional[float]

    ritmo_atual_diario: float
    ritmo_necessario_diario: Optional[float]
    ajuste_ritmo_necessario_pct: Optional[float]

    nivel_alerta: str
    mensagem_alerta: Optional[str]
    titulo_bloco: str = "Ritmo do mês"
    projecao_insuficiente: bool = False
    meta_tooltip_origens: str = ""


def compute_pace_mensal(
    slice_rg: ResultadoGerencialSlice,
    historico_receitas_mensais: list[float],
    cliente_config: dict[str, Any],
    empresas_selecionadas: list[str],
    data_inicio: date,
    data_fim: date,
    hoje: date,
    *,
    historico_por_empresa: dict[str, list[float]] | None = None,
) -> PaceMensal | None:
    """
    ``historico_receitas_mensais`` — série consolidada (últimos meses) para MA3 de fallback.
    ``historico_por_empresa`` — opcional, chaves nome ou slug.
    """
    if int(slice_rg.stats.n_linhas) == 0:
        return None
    rec = float(slice_rg.stats.receita_total)

    modo = determinar_modo(data_inicio, data_fim, hoje)
    mes_rep = f"{data_inicio.month:02d}/{data_inicio.year}"

    yref, mref = data_inicio.year, data_inicio.month
    dias_mes = monthrange(yref, mref)[1]

    emp_tuple = tuple(str(e).strip() for e in empresas_selecionadas if str(e).strip())
    hist_pe = historico_por_empresa or {}
    meta_val, meta_origem = _resolver_meta_consolidado(
        cliente_config=cliente_config,
        empresas_sel=emp_tuple if emp_tuple else tuple(),
        hist_mensal_trailing=list(historico_receitas_mensais),
        hist_por_empresa=hist_pe,
    )
    tem_meta = meta_val is not None and meta_val > 0

    if modo == "recorte_parcial":
        # consumidor não renderiza — ainda retornamos objeto mínimo para testes
        d_periodo = int((data_fim - data_inicio).days + 1)
        dia_equiv = min(max(1, int((min(hoje, data_fim) - data_inicio).days + 1)), d_periodo)
        ritmo = rec / float(dia_equiv) if dia_equiv else 0.0
        pct_m = (rec / meta_val) if tem_meta and meta_val else 0.0
        return PaceMensal(
            mes_referencia=mes_rep,
            dia_atual=dia_equiv,
            dias_totais_periodo=d_periodo,
            dias_restantes=max(0, d_periodo - dia_equiv),
            modo=modo,
            receita_realizada=rec,
            pct_meta_realizada=pct_m,
            meta_mensal=meta_val,
            meta_origem=meta_origem,
            projecao_linear=None,
            desvio_projecao_pct=None,
            ritmo_atual_diario=ritmo,
            ritmo_necessario_diario=None,
            ajuste_ritmo_necessario_pct=None,
            nivel_alerta="leitura",
            mensagem_alerta=None,
        )

    # mês civil completo
    dias_totais = dias_mes

    if modo == "mes_fechado":
        dias_util = dias_totais
        ritmo = rec / float(dias_util) if dias_util else 0.0
        pct_m = (rec / meta_val) if tem_meta and meta_val else 0.0
        nv, msg = _classificar_alerta(None, tem_meta=False)
        _ = nv, msg
        return PaceMensal(
            mes_referencia=mes_rep,
            dia_atual=dias_util,
            dias_totais_periodo=dias_totais,
            dias_restantes=0,
            modo=modo,
            receita_realizada=rec,
            pct_meta_realizada=pct_m,
            meta_mensal=meta_val,
            meta_origem=meta_origem,
            projecao_linear=None,
            desvio_projecao_pct=None,
            ritmo_atual_diario=ritmo,
            ritmo_necessario_diario=None,
            ajuste_ritmo_necessario_pct=None,
            nivel_alerta="leitura",
            mensagem_alerta=None,
            titulo_bloco="Ritmo final do mês",
        )

    # mes_corrente — termômetro ativo só quando o «hoje» calendário está no mesmo mês civil do recorte
    if hoje.year != yref or hoje.month != mref:
        return None

    dia_atual = int(min(hoje.day, dias_mes))
    dias_restantes = int(max(0, dias_mes - hoje.day))

    proj_bloqueia = dia_atual < 3
    proj: float | None = None
    desvio: float | None = None
    if not proj_bloqueia and dia_atual > 0:
        proj = rec * (float(dias_totais) / float(dia_atual))
        if tem_meta and meta_val:
            desvio = proj / float(meta_val) - 1.0

    ritmo_atual = rec / float(dia_atual) if dia_atual else 0.0

    ritmo_nec: float | None = None
    aj_ritmo: float | None = None
    if tem_meta and meta_val and dias_restantes > 0 and rec < float(meta_val) - 1e-6:
        gap = float(meta_val) - rec
        ritmo_nec = gap / float(dias_restantes)
        if ritmo_atual > 1e-9:
            aj_ritmo = (ritmo_nec / ritmo_atual) - 1.0

    pct_m = (rec / float(meta_val)) if tem_meta and meta_val else 0.0

    nivel, msg_alerta = _classificar_alerta(desvio, tem_meta=tem_meta and not proj_bloqueia)
    if proj_bloqueia:
        nivel = "ok"
        msg_alerta = None

    detalhe_mensagem: str | None = msg_alerta
    if (
        not proj_bloqueia
        and nivel in ("atencao", "critico")
        and tem_meta
        and meta_val
        and ritmo_nec is not None
    ):
        extra = _mensagem_ritmo_necessario(
            receita=rec,
            meta=float(meta_val),
            dias_restantes=dias_restantes,
            ritmo_atual=ritmo_atual,
            ritmo_nec=ritmo_nec,
        )
        if extra:
            detalhe_mensagem = (msg_alerta or "").strip()
            detalhe_mensagem = f"{detalhe_mensagem} {extra}".strip()

    tt_parts: list[str] = []
    if meta_origem == "mix":
        tt_parts.append("Meta consolidada: combinação YAML e MA3 por empresa.")
    elif meta_origem:
        tt_parts.append(f"Origem da meta: {meta_origem.upper()}")

    return PaceMensal(
        mes_referencia=mes_rep,
        dia_atual=dia_atual,
        dias_totais_periodo=dias_totais,
        dias_restantes=dias_restantes,
        modo=modo,
        receita_realizada=rec,
        pct_meta_realizada=pct_m,
        meta_mensal=meta_val,
        meta_origem=meta_origem,
        projecao_linear=proj,
        desvio_projecao_pct=desvio,
        ritmo_atual_diario=ritmo_atual,
        ritmo_necessario_diario=ritmo_nec,
        ajuste_ritmo_necessario_pct=aj_ritmo,
        nivel_alerta=nivel if not proj_bloqueia else "ok",
        mensagem_alerta=detalhe_mensagem,
        titulo_bloco="Ritmo do mês",
        projecao_insuficiente=proj_bloqueia,
        meta_tooltip_origens=" · ".join(tt_parts),
    )
