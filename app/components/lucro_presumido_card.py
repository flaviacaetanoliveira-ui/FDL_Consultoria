"""
Componente visual para Apuração Lucro Presumido — premium e detalhado.

Usado no painel Apuração Fiscal para empresas com regime LP.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from processing.faturamento.lucro_presumido import LucroPresumidoBreakdown, calcular_lucro_presumido
from processing.faturamento.lucro_presumido_loader import load_lucro_presumido_params_from_json

_LP_CARD_CSS = """
<style>
.fdl-lp-card-wrap {
  border: 0.5px solid var(--color-border-tertiary, #e5e7eb);
  border-radius: 12px;
  padding: 16px 18px;
  background: var(--color-background-primary, #ffffff);
  margin: 8px 0 14px 0;
}
.fdl-lp-card-title {
  font-size: 16px;
  font-weight: 600;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 4px;
}
.fdl-lp-card-subtitle {
  font-size: 12px;
  color: #6B7280;
  margin-bottom: 10px;
}
.fdl-lp-kpi-value {
  font-variant-numeric: tabular-nums;
}
.fdl-lp-total-imp {
  color: #374151;
}
.fdl-lp-disclaimer {
  font-size: 11px;
  color: #9CA3AF;
  margin-top: 10px;
}
.fdl-lp-break-row {
  display: flex;
  justify-content: space-between;
  gap: 8px;
  padding: 5px 0;
  border-bottom: 0.5px solid var(--color-border-secondary, #f3f4f6);
}
.fdl-lp-break-row:last-child { border-bottom: none; }
.fdl-lp-break-lbl {
  font-size: 12px;
  color: #6B7280;
}
.fdl-lp-break-val {
  font-size: 12px;
  color: var(--color-text-primary, #0f172a);
  font-variant-numeric: tabular-nums;
  text-align: right;
}
</style>
"""


def _fmt_brl(v: float) -> str:
    s = f"{float(v):,.2f}"
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {s}"


def _fmt_pct(v: float) -> str:
    return f"{float(v)*100:.2f}".replace(".", ",") + "%"


def _render_header(empresa: str, dt_ini: pd.Timestamp | date, dt_fim: pd.Timestamp | date) -> None:
    dini = pd.Timestamp(dt_ini).strftime("%d/%m/%Y")
    dfim = pd.Timestamp(dt_fim).strftime("%d/%m/%Y")
    st.markdown(
        (
            '<div class="fdl-lp-card-wrap">'
            f'<div class="fdl-lp-card-title">{empresa} · Lucro Presumido</div>'
            f'<div class="fdl-lp-card-subtitle">Apuração detalhada · {dini} a {dfim}</div>'
        ),
        unsafe_allow_html=True,
    )


def _render_kpis_principais(breakdown: LucroPresumidoBreakdown) -> None:
    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Receita Bruta", _fmt_brl(breakdown.receita_bruta))
        st.caption(f"{breakdown.nfs} NFs · −{_fmt_brl(breakdown.receita_devolucoes)} em devoluções abatidas")
    with c2:
        st.metric("Imposto Total", _fmt_brl(breakdown.total_imposto))
        st.caption(f"Federal {_fmt_brl(breakdown.total_federal)} · Estadual {_fmt_brl(breakdown.total_estadual)}")
    with c3:
        st.metric("Alíquota Efetiva", _fmt_pct(breakdown.aliquota_efetiva))
        st.caption("Carga total")


def _render_breakdown_detalhado(breakdown: LucroPresumidoBreakdown) -> None:
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("**TRIBUTOS FEDERAIS**")
        st.metric("PIS", _fmt_brl(breakdown.pis_valor), help="0,65% sobre receita bruta")
        st.metric("COFINS", _fmt_brl(breakdown.cofins_valor), help="3% sobre receita bruta")
        st.metric(
            "IRPJ",
            _fmt_brl(breakdown.irpj_valor),
            help="15% sobre base presumida (8% até R$ 5M, 8,8% acima — LC 224/2025)",
        )
        st.metric(
            "Adicional IRPJ",
            _fmt_brl(breakdown.irpj_adicional_valor),
            help="10% sobre lucro presumido excedente a R$ 60.000/trimestre (pro-rata mensal)",
        )
        st.metric(
            "CSLL",
            _fmt_brl(breakdown.csll_valor),
            help="9% sobre base presumida (12% até R$ 5M, 13,2% acima — LC 224/2025)",
        )
        st.markdown(f"**Total federal: {_fmt_brl(breakdown.total_federal)}**")
    with c2:
        st.markdown("**TRIBUTOS ESTADUAIS**")
        st.metric(
            "ICMS interno SP",
            _fmt_brl(breakdown.icms_interno_valor),
            help="Móveis 9403 SP: 12% + complemento 1,3% = 13,3% (RICMS-SP)",
        )
        st.metric(
            "ICMS interestadual",
            _fmt_brl(breakdown.icms_interestadual_valor),
            help="12% para sul/sudeste exceto ES, 7% demais",
        )
        st.metric(
            "DIFAL",
            _fmt_brl(breakdown.difal_valor),
            help="Diferença entre alíquota interna do destino (estimada 18%) e alíquota interestadual",
        )
        st.metric(
            "FCP",
            _fmt_brl(breakdown.fcp_valor),
            help="Adicional ao ICMS por UF de destino. RJ 2%, demais 0% (premissa conservadora)",
        )
        st.markdown(f"**Total estadual: {_fmt_brl(breakdown.total_estadual)}**")


def _render_expander_transparencia(breakdown: LucroPresumidoBreakdown) -> None:
    ttl = "Composição e transparência"
    if breakdown.avisos:
        ttl += " · ⚠"
    with st.expander(ttl, expanded=False):
        st.markdown(
            f"- **Majoração LC 224/2025:** {'Aplicada' if breakdown.aplicou_majoracao_lc_224 else 'Não aplicada'} "
            f"(receita anual de referência: {_fmt_brl(breakdown.receita_anual_referencia)})"
        )
        st.markdown(
            f"- **FCP:** base com FCP>0 {_fmt_brl(breakdown.fcp_base_aplicado)} "
            f"| base com FCP=0 {_fmt_brl(breakdown.fcp_base_zero)}"
        )
        st.markdown(f"- **UFs com FCP aplicado:** {list(breakdown.fcp_ufs_aplicadas) or ['—']}")
        st.markdown(f"- **UFs com FCP zerado:** {list(breakdown.fcp_ufs_zeradas) or ['—']}")
        if breakdown.avisos:
            st.markdown("**Avisos do cálculo:**")
            for av in breakdown.avisos:
                st.markdown(f"- {av}")


def _render_disclaimer() -> None:
    st.markdown(
        '<div class="fdl-lp-disclaimer">'
        "Cálculo conforme legislação 2026. Para apuração oficial, consultar contador.<br/>"
        "Pesquisa técnica: docs/pesquisa_fcp_lucro_presumido_2026.md"
        "</div></div>",
        unsafe_allow_html=True,
    )


def render_lucro_presumido_card(
    *,
    df_fiscal: pd.DataFrame,
    df_devolucoes: pd.DataFrame | None,
    org_id: str,
    empresa_nome: str,
    nf_d_ini: pd.Timestamp,
    nf_d_fim: pd.Timestamp,
    json_params_path: Path,
    receita_anual_estimada: float | None = None,
) -> LucroPresumidoBreakdown | None:
    """
    Renderiza bloco de Apuração Lucro Presumido para a empresa indicada.

    Retorna o breakdown quando renderizado com sucesso; retorna ``None`` para
    empresa não-LP, ausência de dados no período ou falha de carregamento.
    """
    try:
        lp_params, icms_params = load_lucro_presumido_params_from_json(json_params_path, org_id)
    except Exception as exc:  # noqa: BLE001
        st.error(f"Não foi possível carregar parâmetros de Lucro Presumido para {empresa_nome}: {exc}")
        return None
    if lp_params is None or icms_params is None:
        return None

    if df_fiscal.empty:
        st.info(f"Sem NFs da {empresa_nome} no período selecionado.")
        return None
    dt = pd.to_datetime(df_fiscal.get("Nota_Data_Emissao"), errors="coerce")
    mask = (
        (df_fiscal.get("org_id", "").astype(str) == str(org_id).strip())
        & (dt >= pd.Timestamp(nf_d_ini))
        & (dt <= pd.Timestamp(nf_d_fim))
    )
    if int(mask.fillna(False).sum()) == 0:
        st.info(f"Sem NFs da {empresa_nome} no período selecionado.")
        return None

    breakdown = calcular_lucro_presumido(
        df_fiscal=df_fiscal,
        df_devolucoes=df_devolucoes,
        org_id=org_id,
        nf_d_ini=nf_d_ini,
        nf_d_fim=nf_d_fim,
        receita_anual_estimada=receita_anual_estimada,
        params=lp_params,
        icms_params=icms_params,
    )

    st.markdown(_LP_CARD_CSS, unsafe_allow_html=True)
    _render_header(empresa_nome, nf_d_ini, nf_d_fim)
    _render_kpis_principais(breakdown)
    _render_breakdown_detalhado(breakdown)
    _render_expander_transparencia(breakdown)
    _render_disclaimer()
    return breakdown

