"""
Painel UI do Indicador Saúde do Regime SN.

Barras de progresso e classificação visual; cores acompanhadas de texto e ícones.
"""

from __future__ import annotations

import html

import streamlit as st

CSS_SAUDE_REGIME = """
<style>
.fdl-saude-wrap {
  margin: 12px 0 20px 0;
}
.fdl-saude-title {
  font-size: 1.05rem;
  font-weight: 600;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 12px;
}
.fdl-saude-bloco {
  background: var(--color-background-primary, #ffffff);
  border: 1px solid var(--color-border-tertiary, #e2e8f0);
  border-radius: 12px;
  padding: 1rem 1.25rem;
  margin-bottom: 12px;
}
.fdl-saude-empresa-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  gap: 12px;
  flex-wrap: wrap;
  margin-bottom: 8px;
}
.fdl-saude-nome {
  font-weight: 600;
  font-size: 14px;
  color: var(--color-text-primary, #0f172a);
}
.fdl-saude-meta {
  font-size: 12px;
  color: var(--color-text-secondary, #475569);
}
.fdl-saude-barra-outer {
  height: 10px;
  border-radius: 6px;
  background: #e2e8f0;
  overflow: hidden;
  margin-top: 6px;
}
.fdl-saude-barra-inner {
  height: 100%;
  border-radius: 6px;
  min-width: 0;
  transition: width 0.2s ease;
}
.fdl-saude-tranquilo .fdl-saude-barra-inner { background: #059669; }
.fdl-saude-atencao .fdl-saude-barra-inner { background: #d97706; }
.fdl-saude-critico .fdl-saude-barra-inner { background: #ea580c; }
.fdl-saude-excedido .fdl-saude-barra-inner { background: #dc2626; }
.fdl-saude-faixa-tranquilo { color: #047857; font-weight: 600; font-size: 13px; }
.fdl-saude-faixa-atencao { color: #b45309; font-weight: 600; font-size: 13px; }
.fdl-saude-faixa-critico { color: #c2410c; font-weight: 600; font-size: 13px; }
.fdl-saude-faixa-excedido { color: #b91c1c; font-weight: 600; font-size: 13px; }
.fdl-saude-aviso-dados {
  font-size: 12px;
  color: #92400e;
  background: #fffbeb;
  border-radius: 8px;
  padding: 8px 10px;
  margin-top: 8px;
}
.fdl-saude-rodape {
  font-size: 11px;
  color: var(--color-text-tertiary, #64748b);
  line-height: 1.45;
  margin-top: 10px;
  padding-top: 10px;
  border-top: 1px solid var(--color-border-tertiary, #e2e8f0);
}
</style>
"""


def _faixa_css_class(faixa: str) -> str:
    f = (faixa or "").strip().upper()
    return {
        "TRANQUILO": "fdl-saude-tranquilo",
        "ATENCAO": "fdl-saude-atencao",
        "CRITICO": "fdl-saude-critico",
        "EXCEDIDO": "fdl-saude-excedido",
    }.get(f, "fdl-saude-tranquilo")


def _faixa_label_publico(faixa: str) -> str:
    return {
        "TRANQUILO": "Tranquilo",
        "ATENCAO": "Atenção",
        "CRITICO": "Crítico",
        "EXCEDIDO": "Excedido",
    }.get((faixa or "").strip().upper(), faixa or "—")


def _fmt_pct(p: float) -> str:
    return f"{p * 100:.1f}%".replace(".", ",")


def render_saude_regime_panel(
    saudes_sn: list,
    *,
    mostrar_lp_aviso: bool = True,
    nome_lp: str | None = None,
) -> None:
    """
    Renderiza bloco de Saúde do Regime SN.

    Args:
        saudes_sn: lista de ``SaudeRegimeEmpresa``
        mostrar_lp_aviso: exibe nota quando há LP no mesmo recorte
        nome_lp: nomes das empresas LP (texto livre)
    """
    if not saudes_sn:
        return

    tem_critico = any(
        getattr(s, "faixa", None) in ("CRITICO", "EXCEDIDO") for s in saudes_sn
    )
    if tem_critico:
        st.warning(
            "Atenção: pelo menos uma empresa do Simples Nacional está próxima ou acima "
            "do sublimite de receita bruta (R$ 4.800.000 / 12 meses). Verifique com seu contador."
        )

    parts: list[str] = ['<div class="fdl-saude-wrap">']
    parts.append('<div class="fdl-saude-title">Saúde do regime — Simples Nacional</div>')
    parts.append(
        '<p class="fdl-saude-meta" style="margin:0 0 12px 0;">RBT12: receita bruta dos 12 meses '
        "anteriores à competência (LC 123/2006). Base: soma mensal de valor líquido de NF no dataset fiscal.</p>"
    )

    for s in saudes_sn:
        nome = html.escape(str(getattr(s, "nome_empresa", "")))
        faixa = str(getattr(s, "faixa", ""))
        classe_wrap = _faixa_css_class(faixa)
        pct = float(getattr(s, "percentual_limite", 0.0))
        pct_vis = min(pct * 100.0, 100.0)
        rbt = float(getattr(s, "rbt12", 0.0))
        lim = float(getattr(s, "limite", 4_800_000.0))
        suf = bool(getattr(s, "rbt12_suficiente", False))
        meses_d = int(getattr(s, "meses_disponiveis", 0))
        ini_j = html.escape(str(getattr(s, "janela_rbt12_inicio", "")))
        fim_j = html.escape(str(getattr(s, "janela_rbt12_fim", "")))
        comp = html.escape(str(getattr(s, "competencia", "")))

        faixa_txt = html.escape(_faixa_label_publico(faixa))
        faixa_span_class = {
            "TRANQUILO": "fdl-saude-faixa-tranquilo",
            "ATENCAO": "fdl-saude-faixa-atencao",
            "CRITICO": "fdl-saude-faixa-critico",
            "EXCEDIDO": "fdl-saude-faixa-excedido",
        }.get(faixa.upper(), "fdl-saude-faixa-tranquilo")

        lbl_pct = _fmt_pct(pct)
        parts.append(f'<div class="fdl-saude-bloco {classe_wrap}">')
        parts.append('<div class="fdl-saude-empresa-row">')
        parts.append(f'<span class="fdl-saude-nome">{nome}</span>')
        parts.append(
            f'<span class="{faixa_span_class}" title="Faixa de proximidade do limite legal">'
            f"{faixa_txt} · {lbl_pct} do limite</span>"
        )
        parts.append("</div>")
        parts.append(
            f'<div class="fdl-saude-meta">Competência {comp} · Janela RBT12: {ini_j} a {fim_j}</div>'
        )
        parts.append(
            f'<div class="fdl-saude-meta">RBT12: R$ {rbt:,.2f} · Limite: R$ {lim:,.2f}</div>'
        )

        parts.append(
            f'<div class="fdl-saude-barra-outer" role="progressbar" '
            f'aria-valuemin="0" aria-valuemax="100" aria-valuenow="{pct_vis:.1f}" '
            f'aria-label="Percentual do limite do Simples Nacional para {nome}">'
            f'<div class="fdl-saude-barra-inner" style="width:{pct_vis:.1f}%"></div></div>'
        )

        if not suf:
            parts.append(
                '<div class="fdl-saude-aviso-dados" role="status">'
                f"<strong>Dados insuficientes para RBT12 completo:</strong> "
                f"{meses_d} de 12 meses com movimento fiscal na janela. "
                "O percentual é parcial — não equivale ao RBT12 oficial até completar o histórico."
                "</div>"
            )

        parts.append("</div>")

    if mostrar_lp_aviso and (nome_lp or "").strip():
        lp_esc = html.escape(str(nome_lp).strip())
        parts.append(
            f'<p class="fdl-saude-meta">Este recorte inclui Lucro Presumido ({lp_esc}). '
            "O indicador acima aplica-se apenas às empresas do Simples Nacional.</p>"
        )

    parts.append(
        '<div class="fdl-saude-rodape">'
        "Referência legal: LC 123/2006 — limite de receita bruta para permanência no Simples Nacional "
        "(comércio/indústria: R$ 4.800.000 em 12 meses). Valores derivados das NF-e materializadas; "
        "podem diferir do PGDAS-D."
        "</div>"
    )
    parts.append("</div>")

    st.markdown(CSS_SAUDE_REGIME + "".join(parts), unsafe_allow_html=True)
