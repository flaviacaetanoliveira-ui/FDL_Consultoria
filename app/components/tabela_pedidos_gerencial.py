"""
Tabela por pedido no Resultado Gerencial (Streamlit).

Agregação pesada só em ``processing/faturamento/resultado_gerencial_slice.compute_tabela_por_pedido``.
"""

from __future__ import annotations

import csv
import io
import re
from typing import Sequence

import pandas as pd
import streamlit as st

from processing.faturamento.resultado_gerencial_slice import PedidoGerencialRow, ResultadoGerencialSlice

_ROWS_PER_PAGE = 50


def _sanitize_filename_part(s: str) -> str:
    x = re.sub(r"[^\w.\-]+", "_", str(s).strip(), flags=re.UNICODE).strip("_")
    return x or "recorte"


def _filtro_pedidos(
    linhas: Sequence[PedidoGerencialRow],
    *,
    plats: Sequence[str],
    statuses: Sequence[str],
    texto: str,
    faixa_resultado: str,
) -> list[PedidoGerencialRow]:
    txt = str(texto).strip().casefold()
    want_p = {str(x).strip().casefold() for x in plats if str(x).strip()}
    want_s = {str(x).strip().casefold() for x in statuses if str(x).strip()}
    out: list[PedidoGerencialRow] = []
    for p in linhas:
        if want_p and p.plataforma.strip().casefold() not in want_p:
            continue
        if want_s and p.status_nf.strip().casefold() not in want_s:
            continue
        if txt:
            hay = " ".join(
                [
                    p.pedido_id,
                    p.numero_pedido_ui,
                    " ".join(p.skus),
                ]
            ).casefold()
            if txt not in hay:
                continue
        if faixa_resultado == "prejuizo" and p.resultado >= -1e-9:
            continue
        if faixa_resultado == "lucro" and p.resultado <= 1e-9:
            continue
        out.append(p)
    return out


def render_tabela_pedidos_rg(
    slice_rg: ResultadoGerencialSlice,
    kp_rg: dict[str, float | int],
    *,
    fiscal_imposto_valor: float,
    export_label: str,
    debug_coerencia: bool = False,
) -> None:
    """Renderiza tabela por pedido: filtros, paginação 50, CSV, colunas opcionais."""
    from processing.faturamento.resultado_gerencial_slice import compute_tabela_por_pedido

    linhas_full = compute_tabela_por_pedido(slice_rg, fiscal_imposto_valor=float(fiscal_imposto_valor))

    soma_rec = sum(p.receita for p in linhas_full)
    soma_res = sum(p.resultado for p in linhas_full)
    if debug_coerencia:
        d_r = abs(soma_rec - float(kp_rg["valor_venda_lista"]))
        d_x = abs(soma_res - float(kp_rg["resultado"]))
        if d_r >= 0.02 or d_x >= 0.02:
            st.warning(
                f"Coerência KPIs/tabela fora da tolerância (Δ receita R$ {d_r:,.4f}, Δ resultado R$ {d_x:,.4f}). "
                "Contacte suporte técnico."
            )
        else:
            st.caption(f"Coerência: Δ receita R$ {d_r:.4f} · Δ resultado R$ {d_x:.4f}")

    total_receita_fmt = f"{soma_rec:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    st.markdown("### Pedidos do período")
    st.caption(
        f"{len(linhas_full)} pedidos · R$ {total_receita_fmt} (receita lista no recorte)"
    )

    plat_opts = sorted({p.plataforma for p in linhas_full if p.plataforma.strip()}, key=lambda t: t.casefold())
    st_opts = sorted({p.status_nf for p in linhas_full if p.status_nf.strip()}, key=lambda t: t.casefold())

    f1, f2, f3, f4 = st.columns((1, 1, 1, 1))
    with f1:
        sel_plat = st.multiselect(
            "Plataforma",
            plat_opts,
            default=[],
            key="fdl_rg_ped_tbl_plat",
            help="Vazio = todas.",
        )
    with f2:
        sel_stat = st.multiselect(
            "Status NF",
            st_opts,
            default=[],
            key="fdl_rg_ped_tbl_st",
            help="Vazio = todos.",
        )
    with f3:
        qtxt = st.text_input(
            "Pedido ou SKU",
            "",
            key="fdl_rg_ped_tbl_q",
            help="Contém texto no ID do pedido, nº ou SKUs.",
        )
    with f4:
        faixa = st.radio(
            "Resultado",
            ("Todos", "Só prejuízo", "Só lucro"),
            horizontal=True,
            key="fdl_rg_ped_tbl_fx",
        )
    fx_map = {"Todos": "todos", "Só prejuízo": "prejuizo", "Só lucro": "lucro"}
    linhas = _filtro_pedidos(
        linhas_full,
        plats=sel_plat,
        statuses=sel_stat,
        texto=qtxt,
        faixa_resultado=fx_map[faixa],
    )

    with st.popover("Colunas opcionais"):
        show_emp = st.checkbox("Empresa", value=False, key="fdl_rg_ped_col_emp")
        show_com = st.checkbox("Comissão", value=False, key="fdl_rg_ped_col_com")
        show_fp = st.checkbox("Frete plataforma", value=False, key="fdl_rg_ped_col_fp")
        show_cmv = st.checkbox("CMV", value=False, key="fdl_rg_ped_col_cmv")
        show_st = st.checkbox("Status NF", value=False, key="fdl_rg_ped_col_st")
        show_q = st.checkbox("Qtd itens", value=False, key="fdl_rg_ped_col_q")

    n_tot = len(linhas)
    n_pages = max(1, (n_tot + _ROWS_PER_PAGE - 1) // _ROWS_PER_PAGE)
    page = st.number_input(
        "Página",
        min_value=1,
        max_value=n_pages,
        value=1,
        step=1,
        key="fdl_rg_ped_tbl_page",
    )
    i0 = (int(page) - 1) * _ROWS_PER_PAGE
    chunk = linhas[i0 : i0 + _ROWS_PER_PAGE]

    cols_default = ["Data venda", "Plataforma", "Nº Pedido", "SKU", "Receita", "Resultado", "Margem %"]
    extra_names: list[str] = []
    if show_emp:
        extra_names.append("Empresa")
    if show_com:
        extra_names.append("Comissão")
    if show_fp:
        extra_names.append("Frete plataforma")
    if show_cmv:
        extra_names.append("CMV")
    if show_st:
        extra_names.append("Status NF")
    if show_q:
        extra_names.append("Qtd itens")

    rows_html: list[dict[str, object]] = []
    for p in chunk:
        sku_disp = p.skus[0] if len(p.skus) == 1 else (f"{len(p.skus)} itens" if p.skus else "—")
        row: dict[str, object] = {
            "Data venda": p.data_venda.strftime("%d/%m/%Y"),
            "Plataforma": p.plataforma or "—",
            "Nº Pedido": p.numero_pedido_ui or p.pedido_id.split("|")[-1],
            "SKU": sku_disp,
            "Receita": p.receita,
            "Resultado": p.resultado,
            "Margem %": p.margem_pct,
        }
        if show_emp:
            row["Empresa"] = p.empresa or "—"
        if show_com:
            row["Comissão"] = p.comissao
        if show_fp:
            row["Frete plataforma"] = p.frete_plataforma
        if show_cmv:
            row["CMV"] = p.cmv
        if show_st:
            row["Status NF"] = p.status_nf
        if show_q:
            row["Qtd itens"] = p.qtd_itens
        rows_html.append(row)

    disp_cols = cols_default + extra_names
    df_show = pd.DataFrame(rows_html)
    if not df_show.empty:
        df_show = df_show[[c for c in disp_cols if c in df_show.columns]]

    st.dataframe(
        df_show,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Receita": st.column_config.NumberColumn("Receita", format="R$ %,.2f"),
            "Resultado": st.column_config.NumberColumn("Resultado", format="R$ %,.2f"),
            "Margem %": st.column_config.NumberColumn("Margem %", format="%.1f"),
            "Comissão": st.column_config.NumberColumn("Comissão", format="R$ %,.2f"),
            "Frete plataforma": st.column_config.NumberColumn("Frete plataforma", format="R$ %,.2f"),
            "CMV": st.column_config.NumberColumn("CMV", format="R$ %,.2f"),
            "Qtd itens": st.column_config.NumberColumn("Qtd itens", format="%d"),
        },
    )
    st.caption(f"Mostrando {len(chunk)} de {n_tot} pedidos filtrados · página {page}/{n_pages}")

    export_rows: list[dict[str, object]] = []
    for p in linhas:
        sku_disp = p.skus[0] if len(p.skus) == 1 else (f"{len(p.skus)} itens" if p.skus else "—")
        er: dict[str, object] = {
            "Data venda": p.data_venda.strftime("%d/%m/%Y"),
            "Plataforma": p.plataforma,
            "Nº Pedido": p.numero_pedido_ui or p.pedido_id.split("|")[-1],
            "SKU": sku_disp,
            "Receita": round(p.receita, 2),
            "Resultado": round(p.resultado, 2),
            "Margem %": round(p.margem_pct, 4),
        }
        if show_emp:
            er["Empresa"] = p.empresa
        if show_com:
            er["Comissão"] = round(p.comissao, 2)
        if show_fp:
            er["Frete plataforma"] = round(p.frete_plataforma, 2)
        if show_cmv:
            er["CMV"] = round(p.cmv, 2)
        if show_st:
            er["Status NF"] = p.status_nf
        if show_q:
            er["Qtd itens"] = p.qtd_itens
        export_rows.append(er)

    buf = io.StringIO()
    if export_rows:
        pd.DataFrame(export_rows).to_csv(buf, index=False, sep=";", quoting=csv.QUOTE_MINIMAL)
    fn = f"pedidos_{_sanitize_filename_part(export_label)}.csv"
    st.download_button(
        "Exportar CSV",
        buf.getvalue().encode("utf-8-sig"),
        file_name=fn,
        mime="text/csv",
        key="fdl_rg_ped_tbl_dl",
        help="Exporta todas as linhas **filtradas**, com as mesmas colunas que escolheu como opcionais.",
    )

