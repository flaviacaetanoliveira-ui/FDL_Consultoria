"""
Tabela por pedido no Resultado Gerencial (Streamlit).

Agregação pesada só em ``processing/faturamento/resultado_gerencial_slice.compute_tabela_por_pedido``.
"""

from __future__ import annotations

import csv
import hashlib
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
        if faixa_resultado == "prejuizo_real" and p.resultado_operacional >= -1e-9:
            continue
        if faixa_resultado == "saudavel_neg_liquido":
            if p.resultado_operacional < -1e-9 or p.resultado_liquido >= -1e-9:
                continue
        if faixa_resultado == "lucro_pleno" and p.resultado_liquido < -1e-9:
            continue
        out.append(p)
    return out


def _hash_pedido_key(pedido_id: str) -> str:
    return hashlib.md5(str(pedido_id).encode("utf-8")).hexdigest()[:16]


def render_tabela_pedidos_rg(
    slice_rg: ResultadoGerencialSlice,
    kp_rg: dict[str, float | int],
    *,
    fiscal_imposto_valor: float,
    export_label: str,
    debug_coerencia: bool = False,
    cliente_slug: str | None = None,
) -> None:
    """Renderiza tabela por pedido: filtros, paginação 50, CSV, colunas opcionais."""
    from app.components.ficha_pedido import render_ficha_pedido
    from processing.faturamento.ficha_pedido_rg import compute_ficha_pedido, load_resultado_gerencial_config
    from processing.faturamento.resultado_gerencial_slice import compute_tabela_por_pedido

    rg_conf = load_resultado_gerencial_config(cliente_slug)
    _sess_k = "fdl_rg_ped_fichas_abertas"
    if _sess_k not in st.session_state:
        st.session_state[_sess_k] = set()
    fichas_abertas: set[str] = st.session_state[_sess_k]

    linhas_full = compute_tabela_por_pedido(slice_rg, fiscal_imposto_valor=float(fiscal_imposto_valor))

    soma_rec = sum(p.receita for p in linhas_full)
    soma_res = sum(p.resultado for p in linhas_full)
    soma_op = sum(p.resultado_operacional for p in linhas_full)
    soma_desp = sum(p.despesa_fixa for p in linhas_full)
    if debug_coerencia:
        d_r = abs(soma_rec - float(kp_rg["valor_venda_lista"]))
        d_x = abs(soma_res - float(kp_rg["resultado"]))
        d_op = abs(soma_op - float(kp_rg.get("resultado_operacional", soma_op)))
        d_de = abs(soma_desp - float(kp_rg.get("total_despesa_fixa", soma_desp)))
        if d_r >= 0.02 or d_x >= 0.02 or d_op >= 0.02 or d_de >= 0.02:
            st.warning(
                f"Coerência KPIs/tabela fora da tolerância (Δ receita R$ {d_r:,.4f}, Δ resultado R$ {d_x:,.4f}, "
                f"Δ op. R$ {d_op:,.4f}, Δ desp. R$ {d_de:,.4f}). "
                "Contacte suporte técnico."
            )
        else:
            st.caption(
                f"Coerência: Δ receita R$ {d_r:.4f} · Δ resultado R$ {d_x:.4f} · Δ op. R$ {d_op:.4f} · Δ desp. R$ {d_de:.4f}"
            )

    total_receita_fmt = f"{soma_rec:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    st.markdown("### Pedidos do período")
    with st.expander("Duas leituras de margem", expanded=False):
        st.markdown(
            """
**Operacional** — após custos diretos do pedido (comissão, CMV, fretes, imposto rateado, ADS variável).  
**Líquida** — após despesa fixa e ADS fixo do período rateados nas linhas.  

Pedidos saudáveis com líquida negativa ajudam a diluir estrutura; descontinuar pode piorar o mês.
"""
        )
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
            "Faixa de resultado",
            (
                "Todos",
                "Prejuízo real (op. < 0)",
                "Saudável mas neg. no líquido",
                "Lucro pleno",
            ),
            horizontal=True,
            key="fdl_rg_ped_tbl_fx",
        )
    fx_map = {
        "Todos": "todos",
        "Prejuízo real (op. < 0)": "prejuizo_real",
        "Saudável mas neg. no líquido": "saudavel_neg_liquido",
        "Lucro pleno": "lucro_pleno",
    }
    linhas = _filtro_pedidos(
        linhas_full,
        plats=sel_plat,
        statuses=sel_stat,
        texto=qtxt,
        faixa_resultado=fx_map[faixa],
    )

    with st.popover("Colunas opcionais"):
        show_res_op = st.checkbox("Resultado operacional (R$)", value=False, key="fdl_rg_ped_col_ro")
        show_res_liq = st.checkbox("Resultado líquido (R$)", value=False, key="fdl_rg_ped_col_rl")
        show_com = st.checkbox("Comissão", value=False, key="fdl_rg_ped_col_com")
        show_fp = st.checkbox("Frete plataforma", value=False, key="fdl_rg_ped_col_fp")
        show_ftp = st.checkbox("Frete TP", value=False, key="fdl_rg_ped_col_ftp")
        show_cmv = st.checkbox("CMV", value=False, key="fdl_rg_ped_col_cmv")
        show_imp = st.checkbox("Imposto (rateado)", value=False, key="fdl_rg_ped_col_imp")
        show_desp = st.checkbox("Despesa fixa", value=False, key="fdl_rg_ped_col_desp")
        show_ads_v = st.checkbox("ADS variável", value=False, key="fdl_rg_ped_col_av")
        show_ads_f = st.checkbox("ADS fixo", value=False, key="fdl_rg_ped_col_af")
        show_emp = st.checkbox("Empresa", value=False, key="fdl_rg_ped_col_emp")
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

    cols_default = [
        "Data venda",
        "Plataforma",
        "Nº Pedido",
        "SKU",
        "Receita",
        "Margem op. %",
        "Margem líquida %",
    ]
    extra_names: list[str] = []
    if show_res_op:
        extra_names.append("Resultado op.")
    if show_res_liq:
        extra_names.append("Resultado líquido")
    if show_com:
        extra_names.append("Comissão")
    if show_fp:
        extra_names.append("Frete plataforma")
    if show_ftp:
        extra_names.append("Frete TP")
    if show_cmv:
        extra_names.append("CMV")
    if show_imp:
        extra_names.append("Imposto")
    if show_desp:
        extra_names.append("Despesa fixa")
    if show_ads_v:
        extra_names.append("ADS var.")
    if show_ads_f:
        extra_names.append("ADS fixo")
    if show_emp:
        extra_names.append("Empresa")
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
            "Margem op. %": p.margem_operacional_pct,
            "Margem líquida %": p.margem_liquida_pct,
        }
        if show_res_op:
            row["Resultado op."] = p.resultado_operacional
        if show_res_liq:
            row["Resultado líquido"] = p.resultado_liquido
        if show_com:
            row["Comissão"] = p.comissao
        if show_fp:
            row["Frete plataforma"] = p.frete_plataforma
        if show_ftp:
            row["Frete TP"] = p.frete_tp
        if show_cmv:
            row["CMV"] = p.cmv
        if show_imp:
            row["Imposto"] = p.imposto_rateado
        if show_desp:
            row["Despesa fixa"] = p.despesa_fixa
        if show_ads_v:
            row["ADS var."] = p.ads_variavel
        if show_ads_f:
            row["ADS fixo"] = p.ads_fixo
        if show_emp:
            row["Empresa"] = p.empresa or "—"
        if show_st:
            row["Status NF"] = p.status_nf
        if show_q:
            row["Qtd itens"] = p.qtd_itens
        rows_html.append(row)

    disp_cols = cols_default + extra_names
    df_show = pd.DataFrame(rows_html)
    if not df_show.empty:
        df_show = df_show[[c for c in disp_cols if c in df_show.columns]]

    cc: dict[str, object] = {
        "Receita": st.column_config.NumberColumn("Receita", format="R$ %,.2f"),
        "Margem op. %": st.column_config.NumberColumn(
            "Margem op. %",
            format="%.1f",
            help="Custos diretos + imposto + ADS variável",
        ),
        "Margem líquida %": st.column_config.NumberColumn(
            "Margem líquida %",
            format="%.1f",
            help="Alinha ao KPI de topo (inclui despesa fixa + ADS fixo rateados)",
        ),
        "Resultado op.": st.column_config.NumberColumn("Resultado op.", format="R$ %,.2f"),
        "Resultado líquido": st.column_config.NumberColumn("Resultado líquido", format="R$ %,.2f"),
        "Comissão": st.column_config.NumberColumn("Comissão", format="R$ %,.2f"),
        "Frete plataforma": st.column_config.NumberColumn("Frete plataforma", format="R$ %,.2f"),
        "Frete TP": st.column_config.NumberColumn("Frete TP", format="R$ %,.2f"),
        "CMV": st.column_config.NumberColumn("CMV", format="R$ %,.2f"),
        "Imposto": st.column_config.NumberColumn("Imposto", format="R$ %,.2f"),
        "Despesa fixa": st.column_config.NumberColumn("Despesa fixa", format="R$ %,.2f"),
        "ADS var.": st.column_config.NumberColumn("ADS var.", format="R$ %,.2f"),
        "ADS fixo": st.column_config.NumberColumn("ADS fixo", format="R$ %,.2f"),
        "Qtd itens": st.column_config.NumberColumn("Qtd itens", format="%d"),
    }

    st.dataframe(
        df_show,
        use_container_width=True,
        hide_index=True,
        column_config=cc,
    )
    st.caption(f"Mostrando {len(chunk)} de {n_tot} pedidos filtrados · página {page}/{n_pages} · use **Ficha** abaixo para detalhar cada linha")

    st.markdown("##### Detalhe por pedido (nesta página)")
    for p in chunk:
        sku_disp = p.skus[0] if len(p.skus) == 1 else (f"{len(p.skus)} itens" if p.skus else "—")
        c0, c1, c2, c3, c4, c5, c6, c7 = st.columns([1.0, 1.0, 1.2, 2.0, 1.0, 1.0, 1.0, 0.52])
        c0.caption(p.data_venda.strftime("%d/%m/%Y"))
        c1.caption(p.plataforma or "—")
        c2.caption(str(p.numero_pedido_ui or p.pedido_id.split("|")[-1])[:24])
        c3.caption(str(sku_disp)[:40])
        c4.caption(f"R$ {p.receita:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        c5.caption(f"{p.margem_operacional_pct:.1f}%")
        c6.caption(f"{p.margem_liquida_pct:.1f}%")
        hk = _hash_pedido_key(p.pedido_id)
        btn_key = f"fdl_fp_btn_{page}_{hk}"
        if c7.button("Ficha", key=btn_key, help="Abre ou fecha a composição e diagnóstico deste pedido"):
            if p.pedido_id in fichas_abertas:
                fichas_abertas.discard(p.pedido_id)
            else:
                fichas_abertas.add(p.pedido_id)
        if p.pedido_id in fichas_abertas:
            fc = compute_ficha_pedido(
                slice_rg,
                pedido_id=p.pedido_id,
                fiscal_imposto_valor=float(fiscal_imposto_valor),
                pedidos_contexto=linhas,
                rg_config=rg_conf,
            )
            if fc is not None:
                render_ficha_pedido(ficha=fc)
            else:
                st.warning("Não foi possível montar a ficha deste pedido.")

    export_rows: list[dict[str, object]] = []
    for p in linhas:
        sku_disp = p.skus[0] if len(p.skus) == 1 else (f"{len(p.skus)} itens" if p.skus else "—")
        er: dict[str, object] = {
            "Data venda": p.data_venda.strftime("%d/%m/%Y"),
            "Plataforma": p.plataforma,
            "Nº Pedido": p.numero_pedido_ui or p.pedido_id.split("|")[-1],
            "SKU": sku_disp,
            "Receita": round(p.receita, 2),
            "Margem op. %": round(p.margem_operacional_pct, 4),
            "Margem líquida %": round(p.margem_liquida_pct, 4),
        }
        if show_res_op:
            er["Resultado op."] = round(p.resultado_operacional, 2)
        if show_res_liq:
            er["Resultado líquido"] = round(p.resultado_liquido, 2)
        if show_emp:
            er["Empresa"] = p.empresa
        if show_com:
            er["Comissão"] = round(p.comissao, 2)
        if show_fp:
            er["Frete plataforma"] = round(p.frete_plataforma, 2)
        if show_ftp:
            er["Frete TP"] = round(p.frete_tp, 2)
        if show_cmv:
            er["CMV"] = round(p.cmv, 2)
        if show_imp:
            er["Imposto"] = round(p.imposto_rateado, 2)
        if show_desp:
            er["Despesa fixa"] = round(p.despesa_fixa, 2)
        if show_ads_v:
            er["ADS var."] = round(p.ads_variavel, 2)
        if show_ads_f:
            er["ADS fixo"] = round(p.ads_fixo, 2)
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

