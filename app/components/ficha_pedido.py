"""
Ficha expandida de pedido (Resultado Gerencial — Ciclo C).
"""

from __future__ import annotations

import html
import math

import streamlit as st

from processing.faturamento.ficha_pedido_rg import FichaPedido


def _fmt_brl(v: float) -> str:
    x = float(v)
    body = f"{abs(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("−R$ " if x < 0 else "R$ ") + body


def _fmt_pct(v: float) -> str:
    return f"{float(v):.1f}%"


def _fmt_pp_delta(d: float | None) -> str:
    if d is None or (isinstance(d, float) and math.isnan(d)):
        return "—"
    sign = "+" if d >= 0 else ""
    color = "#047857" if d >= 0 else "#b91c1c"
    arrow = "↑" if d >= 0 else "↓"
    return f'<span style="color:{color};font-weight:600;">{arrow} {sign}{d:.1f} pp</span>'


def _badge_status(status_nf: str) -> str:
    s = str(status_nf).strip().casefold()
    if s == "emitida":
        return '<span style="background:#dcfce7;color:#166534;padding:2px 10px;border-radius:8px;font-size:0.75rem;">NF emitida</span>'
    if s == "cancelada":
        return '<span style="background:#fee2e2;color:#991b1b;padding:2px 10px;border-radius:8px;font-size:0.75rem;">Cancelada</span>'
    if s == "parcial":
        return '<span style="background:#fef3c7;color:#92400e;padding:2px 10px;border-radius:8px;font-size:0.75rem;">Parcial</span>'
    return '<span style="background:#f1f5f9;color:#475569;padding:2px 10px;border-radius:8px;font-size:0.75rem;">Sem NF</span>'


def render_ficha_pedido_html(ficha: FichaPedido) -> str:
    """HTML único (``st.html``) — evita Markdown quebrar com ``>``."""
    comp = ficha.comparacao
    pid_ui = html.escape(ficha.numero_pedido_ui or ficha.pedido_id.split("|")[-1])
    plat = html.escape(ficha.plataforma or "—")
    emp = html.escape(ficha.empresa or "—")
    dt = html.escape(ficha.data_venda.strftime("%d/%m/%Y") if hasattr(ficha.data_venda, "strftime") else str(ficha.data_venda))
    mlq = _fmt_pct(ficha.margem_liquida_pct)
    moq = _fmt_pct(ficha.margem_operacional_pct)

    rows_comp = "".join(
        [
            _row_comp("(−) CMV", _fmt_brl(ficha.cmv), _fmt_pct(ficha.cmv_pct)),
            _row_comp("(−) Comissão", _fmt_brl(ficha.comissao), _fmt_pct(ficha.comissao_pct)),
            _row_comp("(−) Frete plataforma", _fmt_brl(ficha.frete_plataforma), _fmt_pct(ficha.frete_plataforma_pct)),
            _row_comp("(−) Frete TP", _fmt_brl(ficha.frete_tp), _fmt_pct(ficha.frete_tp_pct)),
            _row_comp("(−) Imposto rateado", _fmt_brl(ficha.imposto_rateado), _fmt_pct(ficha.imposto_pct)),
            '<tr style="border-top:1px solid #e2e8f0;"><td colspan="3"></td></tr>',
            _row_destaque_op("(=) Resultado operacional", _fmt_brl(ficha.resultado_operacional), _fmt_pct(ficha.margem_operacional_pct)),
            _row_comp("(−) Despesa fixa rateada", _fmt_brl(ficha.despesa_fixa), _fmt_pct(ficha.despesa_fixa_pct)),
            _row_comp("(−) ADS rateado", _fmt_brl(ficha.ads_rateado), _fmt_pct(ficha.ads_pct)),
            '<tr style="border-top:1px solid #e2e8f0;"><td colspan="3"></td></tr>',
            _row_destaque_liq("(=) Resultado líquido", _fmt_brl(ficha.resultado_liquido), _fmt_pct(ficha.margem_liquida_pct)),
        ]
    )

    diag_html = ""
    for d in ficha.diagnosticos:
        bg = {"saudavel": "#ecfdf5", "atencao": "#fffbeb", "risco": "#fef2f2"}.get(d.tipo, "#f8fafc")
        bord = {"saudavel": "#22c55e", "atencao": "#f59e0b", "risco": "#ef4444"}.get(d.tipo, "#94a3b8")
        ico = {"saudavel": "✓", "atencao": "!", "risco": "⚠"}.get(d.tipo, "i")
        diag_html += (
            f'<div style="background:{bg};border-left:4px solid {bord};padding:12px 14px;margin-bottom:8px;border-radius:8px;">'
            f'<div style="font-weight:600;color:#1e293b;">{ico} {html.escape(d.titulo)}</div>'
            f'<div style="font-size:0.85rem;color:#475569;margin-top:4px;">{html.escape(d.explicacao)}</div>'
            "</div>"
        )

    itens_tbl = ""
    if len(ficha.itens) <= 3:
        for it in ficha.itens:
            itens_tbl += (
                f'<div style="border:1px solid #e2e8f0;border-radius:8px;padding:10px 12px;margin-bottom:8px;background:#fafafa;">'
                f'<strong>{html.escape(it.sku)}</strong> · {html.escape(it.descricao[:80])}<br/>'
                f'<span style="font-size:0.85rem;color:#64748b;">Qtd {it.quantidade} · '
                f"Custo un. {_fmt_brl(it.custo_unitario)} · Preço un. {_fmt_brl(it.preco_unitario)}</span>"
                "</div>"
            )
    else:
        itens_tbl = "<table style='width:100%;font-size:0.9rem;'><tr><th>SKU</th><th>Descrição</th><th>Qtd</th><th>Custo un.</th><th>Preço un.</th></tr>"
        for it in ficha.itens:
            itens_tbl += (
                f"<tr><td>{html.escape(it.sku)}</td><td>{html.escape(it.descricao[:60])}</td>"
                f"<td>{it.quantidade}</td><td>{_fmt_brl(it.custo_unitario)}</td><td>{_fmt_brl(it.preco_unitario)}</td></tr>"
            )
        itens_tbl += "</table>"

    def _linha_comp(label: str, margem_val: float | None, delta: float | None) -> str:
        mv = "—" if margem_val is None else _fmt_pct(margem_val)
        return (
            f"<tr><td>{html.escape(label)}</td><td style='text-align:right;'>{mv}</td>"
            f"<td style='text-align:right;'>{_fmt_pp_delta(delta)}</td></tr>"
        )

    tbl_cmp = (
        "<table style='width:100%;border-collapse:collapse;font-size:0.9rem;'>"
        "<tr style='border-bottom:1px solid #e2e8f0;'><th align='left'>Referência</th><th align='right'>Margem líquida</th><th align='right'>Δ vs pedido</th></tr>"
        f"<tr><td>Este pedido</td><td align='right'>{_fmt_pct(comp.margem_pedido)}</td><td align='right'>—</td></tr>"
        f"{_linha_comp('Média da plataforma', comp.margem_plataforma, comp.delta_plataforma_pp)}"
        f"{_linha_comp('Média ponderada SKUs deste pedido¹', comp.margem_sku, comp.delta_sku_pp)}"
        f"{_linha_comp('Média da empresa', comp.margem_empresa, comp.delta_empresa_pp)}"
        "</table>"
        "<p style='font-size:0.75rem;color:#64748b;margin-top:6px;'>¹ Outros pedidos no mesmo recorte que compartilham cada SKU.</p>"
    )

    receita_row = _row_comp("Receita", _fmt_brl(ficha.receita), "100,0%")

    out = f"""
<section style="font-family:Inter,system-ui,sans-serif;color:#1e293b;margin:12px 0;padding:16px;background:#ffffff;border:1px solid #e2e8f0;border-radius:12px;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;gap:16px;flex-wrap:wrap;">
    <div>
      <div style="font-size:1.15rem;font-weight:700;">Pedido #{pid_ui} &nbsp; {_badge_status(ficha.status_nf)}</div>
      <div style="font-size:0.9rem;color:#64748b;margin-top:6px;">{plat} · {emp} · {dt}</div>
    </div>
    <div style="text-align:right;">
      <div style="font-size:0.7rem;text-transform:uppercase;color:#64748b;font-weight:600;">Margem líquida</div>
      <div style="font-size:1.35rem;font-weight:700;color:#1d4ed8;">{mlq}</div>
      <div style="font-size:0.8rem;color:#64748b;">op. {moq}</div>
    </div>
  </div>
  <h4 style="margin:20px 0 10px 0;font-size:0.85rem;text-transform:uppercase;color:#64748b;">Composição da venda</h4>
  <table style="width:100%;border-collapse:collapse;font-size:0.9rem;">
    <tr style="border-bottom:1px solid #e2e8f0;"><th align='left'>Descrição</th><th align='right'>R$</th><th align='right'>%</th></tr>
    {receita_row}
    {rows_comp}
  </table>
  <h4 style="margin:20px 0 10px 0;font-size:0.85rem;text-transform:uppercase;color:#64748b;">Diagnóstico automático</h4>
  {diag_html}
  <h4 style="margin:20px 0 10px 0;font-size:0.85rem;text-transform:uppercase;color:#64748b;">Itens do pedido ({len(ficha.itens)} SKUs)</h4>
  {itens_tbl}
  <h4 style="margin:20px 0 10px 0;font-size:0.85rem;text-transform:uppercase;color:#64748b;">Comparação</h4>
  {tbl_cmp}
</section>
"""
    return out


def _row_comp(label: str, brl: str, pct: str) -> str:
    return (
        f"<tr><td>{html.escape(label)}</td><td align='right' style='font-variant-numeric:tabular-nums;'>{html.escape(brl)}</td>"
        f"<td align='right'>{html.escape(pct)}</td></tr>"
    )


def _row_destaque_op(label: str, brl: str, pct: str) -> str:
    return (
        f"<tr style='background:#ecfdf5;'><td style='font-weight:600;'>{html.escape(label)}</td>"
        f"<td align='right' style='font-weight:600;'>{html.escape(brl)}</td>"
        f"<td align='right' style='font-weight:600;'>{html.escape(pct)}</td></tr>"
    )


def _row_destaque_liq(label: str, brl: str, pct: str) -> str:
    return (
        f"<tr style='background:#eff6ff;'><td style='font-weight:600;'>{html.escape(label)}</td>"
        f"<td align='right' style='font-weight:600;'>{html.escape(brl)}</td>"
        f"<td align='right' style='font-weight:600;'>{html.escape(pct)}</td></tr>"
    )


def render_ficha_pedido(
    *,
    ficha: FichaPedido,
) -> None:
    """Renderiza ficha na UI Streamlit."""
    st.html(render_ficha_pedido_html(ficha))
