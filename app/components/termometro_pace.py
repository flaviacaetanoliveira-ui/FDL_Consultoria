"""
Termômetro de pace mensal (Resultado Gerencial) — faixa HTML, sem lógica de negócio.
"""

from __future__ import annotations

import html

import streamlit as st

from processing.faturamento.pace_mensal import PaceMensal

PACE_CSS = """
<style>
.fdl-rg-pace {
  font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  border: 1px solid #e2e8f0;
  border-radius: 12px;
  background: #ffffff;
  box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
  padding: 16px 18px 14px 18px;
  margin: 0 0 16px 0;
  box-sizing: border-box;
}
.fdl-rg-pace-hd {
  display: flex;
  flex-wrap: wrap;
  align-items: baseline;
  justify-content: space-between;
  gap: 8px 16px;
  margin-bottom: 12px;
}
.fdl-rg-pace-tit {
  font-size: 0.6875rem;
  font-weight: 500;
  letter-spacing: 0.08em;
  text-transform: uppercase;
  color: #64748b;
  margin: 0;
}
.fdl-rg-pace-meta-tt {
  font-size: 0.72rem;
  font-weight: 400;
  color: #94a3b8;
}
.fdl-rg-pace-grid {
  display: grid;
  grid-template-columns: repeat(4, minmax(0, 1fr));
  gap: 12px 16px;
}
.fdl-rg-pace-grid--2 {
  grid-template-columns: repeat(2, minmax(0, 1fr));
}
@media (max-width: 960px) {
  .fdl-rg-pace-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 520px) {
  .fdl-rg-pace-grid { grid-template-columns: 1fr; }
}
.fdl-rg-pace-cell-lab {
  font-size: 0.6875rem;
  font-weight: 500;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: #64748b;
  margin: 0 0 6px 0;
}
.fdl-rg-pace-cell-val {
  font-size: 1.75rem;
  font-weight: 500;
  line-height: 1.15;
  font-variant-numeric: tabular-nums;
  color: #0f172a;
  margin: 0;
}
.fdl-rg-pace-cell-sub {
  font-size: 0.75rem;
  font-weight: 400;
  color: #64748b;
  margin: 6px 0 0 0;
  line-height: 1.35;
}
.fdl-rg-pace-tone-pos { color: #0f6e56 !important; }
.fdl-rg-pace-tone-warn { color: #ba7517 !important; }
.fdl-rg-pace-tone-bad { color: #a32d2d !important; }
.fdl-rg-pace-alert {
  margin-top: 14px;
  padding: 12px 14px;
  border-radius: 10px;
  font-size: 0.8125rem;
  line-height: 1.45;
}
.fdl-rg-pace-alert--att {
  background: #fff7ed;
  border: 1px solid #fed7aa;
  color: #9a3412;
}
.fdl-rg-pace-alert--crit {
  background: #fef2f2;
  border: 1px solid #fecaca;
  color: #991b1b;
}
.fdl-rg-pace-alert strong {
  font-weight: 500;
  display: block;
  margin-bottom: 4px;
  font-size: 0.8125rem;
}
.fdl-rg-pace-note {
  margin-top: 10px;
  font-size: 0.75rem;
  color: #64748b;
}
</style>
"""


def _fmt_k(v: float) -> str:
    av = abs(float(v))
    if av >= 1_000_000:
        s = f"{v/1_000_000:.2f}M"
    elif av >= 1000:
        s = f"{v/1000:.1f}k"
    else:
        s = f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    if not s.startswith("R$"):
        s = "R$ " + s
    return s


def _fmt_pct(p: float) -> str:
    return f"{p * 100:.1f}%".replace(".", ",")


def _tone_class(nivel: str) -> str:
    if nivel == "ok_positivo":
        return "fdl-rg-pace-tone-pos"
    if nivel == "atencao":
        return "fdl-rg-pace-tone-warn"
    if nivel == "critico":
        return "fdl-rg-pace-tone-bad"
    return ""


def _html_cell(lab: str, val: str, sub: str | None, *, tone: str = "") -> str:
    tc = f" {tone}".rstrip() if tone else ""
    sub_html = (
        f'<p class="fdl-rg-pace-cell-sub">{html.escape(sub)}</p>'
        if sub
        else ""
    )
    return (
        f'<div><p class="fdl-rg-pace-cell-lab">{html.escape(lab)}</p>'
        f'<p class="fdl-rg-pace-cell-val{tc}">{html.escape(val)}</p>{sub_html}</div>'
    )


def _hdr_row(tit: str, meta_tt: str) -> str:
    right = ""
    if meta_tt:
        right = (
            f'<span class="fdl-rg-pace-meta-tt" title="{html.escape(meta_tt)}">ⓘ origem meta</span>'
        )
    return (
        f'<div class="fdl-rg-pace-hd"><div><p class="fdl-rg-pace-tit">{html.escape(tit)}</p></div>{right}</div>'
    )


def _build_html(pace: PaceMensal) -> str:
    modo = pace.modo
    tone_ritmo = _tone_class(pace.nivel_alerta if modo == "mes_corrente" else "")

    tit = pace.titulo_bloco
    meta_tt = pace.meta_tooltip_origens or ""

    parts: list[str] = []

    if modo == "mes_fechado":
        sub_r = (
            _fmt_pct(pace.pct_meta_realizada) + " da meta"
            if pace.meta_mensal
            else f"Média/dia {_fmt_k(pace.ritmo_atual_diario)}"
        )
        c1_val = _fmt_pct(pace.pct_meta_realizada) if pace.meta_mensal else _fmt_k(pace.receita_realizada)
        grid = (
            '<div class="fdl-rg-pace-grid fdl-rg-pace-grid--2">'
            + _html_cell("Ritmo final", c1_val, sub_r if pace.meta_mensal else "Sem meta configurada")
            + _html_cell("Realizado", _fmt_k(pace.receita_realizada), "Total no período")
            + "</div>"
        )
        parts.append('<section class="fdl-rg-pace">')
        parts.append(_hdr_row(tit, meta_tt))
        parts.append(grid)
        parts.append("</section>")
        return "".join(parts)

    if modo == "mes_corrente" and pace.meta_mensal is None:
        grid = (
            '<div class="fdl-rg-pace-grid fdl-rg-pace-grid--2">'
            + _html_cell("Realizado", _fmt_k(pace.receita_realizada), "Total no período")
            + _html_cell(
                "Ritmo médio diário",
                _fmt_k(pace.ritmo_atual_diario),
                f"Dia {pace.dia_atual} de {pace.dias_totais_periodo}",
            )
            + "</div>"
        )
        parts.append('<section class="fdl-rg-pace">')
        parts.append(_hdr_row(tit, meta_tt))
        parts.append(grid)
        if pace.projecao_insuficiente:
            parts.append(
                '<p class="fdl-rg-pace-note">Dados insuficientes para projeção nos primeiros dias do mês.</p>'
            )
        parts.append("</section>")
        return "".join(parts)

    # mes_corrente com meta (+ projeção / alertas)
    dev_txt = (
        _fmt_pct(pace.desvio_projecao_pct)
        if pace.desvio_projecao_pct is not None
        else "—"
    )
    sub_ritmo = "vs meta"
    if pace.projecao_insuficiente:
        dev_txt = "—"
        sub_ritmo = f"Dia {pace.dia_atual} de {pace.dias_totais_periodo}"

    ritmo_blocks = _html_cell(
        "Ritmo",
        dev_txt,
        sub_ritmo,
        tone=tone_ritmo,
    )
    real_blocks = _html_cell(
        "Realizado",
        _fmt_k(pace.receita_realizada),
        (_fmt_pct(pace.pct_meta_realizada) + " da meta") if pace.meta_mensal else None,
    )
    proj_val = _fmt_k(pace.projecao_linear) if pace.projecao_linear is not None else "—"
    proj_sub = (
        f"meta {_fmt_k(pace.meta_mensal)}"
        if pace.meta_mensal
        else None
    )
    proj_blocks = _html_cell("Projeção", proj_val, proj_sub)

    nec_val = _fmt_k(pace.ritmo_necessario_diario) if pace.ritmo_necessario_diario is not None else "—"
    nec_sub_a = _fmt_k(pace.ritmo_atual_diario) if pace.ritmo_atual_diario else "—"
    nec_sub = f"atual {nec_sub_a}" if nec_sub_a != "—" else None
    nec_blocks = _html_cell("Necessário / dia", nec_val, nec_sub)

    grid = (
        '<div class="fdl-rg-pace-grid">'
        + ritmo_blocks
        + real_blocks
        + proj_blocks
        + nec_blocks
        + "</div>"
    )

    parts.append('<section class="fdl-rg-pace">')
    parts.append(_hdr_row(tit, meta_tt))
    parts.append(grid)

    if pace.projecao_insuficiente:
        parts.append(
            '<p class="fdl-rg-pace-note">Dados insuficientes para projeção nos primeiros dias do mês.</p>'
        )

    if pace.mensagem_alerta and pace.nivel_alerta in ("atencao", "critico"):
        cls = "fdl-rg-pace-alert--crit" if pace.nivel_alerta == "critico" else "fdl-rg-pace-alert--att"
        tit_al = (
            "⚠ Ritmo abaixo do necessário para bater a meta"
            if pace.nivel_alerta == "atencao"
            else "⚠ Ritmo criticamente abaixo da meta"
        )
        parts.append(
            f'<div class="fdl-rg-pace-alert {cls}"><strong>{html.escape(tit_al)}</strong>'
            f"<span>{html.escape(pace.mensagem_alerta)}</span></div>"
        )

    parts.append("</section>")
    return "".join(parts)


def render_termometro_pace(pace: PaceMensal | None) -> None:
    """
    Renderiza faixa horizontal acima dos KPIs (chamar só quando há slice válido).
    """
    if pace is None or pace.modo == "recorte_parcial":
        return

    if st.session_state.get("_fdl_rg_pace_css_injected") is not True:
        st.markdown(PACE_CSS, unsafe_allow_html=True)
        st.session_state["_fdl_rg_pace_css_injected"] = True

    st.html(_build_html(pace))


def pace_html_for_tests(pace: PaceMensal | None) -> str:
    """Expõe HTML para testes sem Streamlit."""
    if pace is None or pace.modo == "recorte_parcial":
        return ""
    return _build_html(pace)
