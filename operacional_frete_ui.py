"""UI para Conciliação de Frete (importada por app_operacional)."""
from __future__ import annotations

import html as html_lib
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st
from streamlit.column_config import TextColumn

from fdl_paths import CLIENTE_BASE_DIR
from operacional_frete import (
    FRETE_UI_ANUNCIO,
    FRETE_UI_DIFERENCA,
    FRETE_UI_FRETE_ESPERADO,
    FRETE_UI_N_VENDA,
    FRETE_UI_QTD_PRECO_ML,
    FRETE_UI_STATUS_CONC,
    FRETE_UI_TITULO_ANUNCIO,
    FRETE_UI_VAL_DIVERGENCIA,
    carregar_base_frete_ml,
    descobrir_fontes_frete,
)


def _dataframe_frete_grid(
    df: pd.DataFrame,
    fmt_brl: Callable[[object], str],
    col_ref: Callable[[pd.Series], pd.Series],
) -> pd.DataFrame:
    if df.empty:
        return df
    g = df.copy()
    for c in (
        "Receita por envio (BRL)",
        "Tarifas de envio (BRL)",
        "Frete ML (receita+tarifa)",
        FRETE_UI_FRETE_ESPERADO,
        FRETE_UI_DIFERENCA,
        FRETE_UI_QTD_PRECO_ML,
    ):
        if c in g.columns:
            g[c] = g[c].map(fmt_brl).astype(object)
    if FRETE_UI_N_VENDA in g.columns:
        g[FRETE_UI_N_VENDA] = col_ref(g[FRETE_UI_N_VENDA])
    if FRETE_UI_ANUNCIO in g.columns:
        g[FRETE_UI_ANUNCIO] = col_ref(g[FRETE_UI_ANUNCIO])
    return g


def _column_config_frete(df: pd.DataFrame) -> dict[str, TextColumn]:
    cfg: dict[str, TextColumn] = {}
    for c in df.columns:
        cl = str(c).lower()
        if c in (FRETE_UI_N_VENDA, FRETE_UI_ANUNCIO):
            cfg[c] = TextColumn(str(c), width="medium")
        elif c in ("Estado", FRETE_UI_STATUS_CONC):
            cfg[c] = TextColumn(str(c), width="small")
        elif "descri" in cl or "titulo" in cl or "título" in cl:
            cfg[c] = TextColumn(str(c), width="large")
    return cfg


def painel_frete_fragment(
    org_id: str,
    *,
    br_tz: object,
    multiselect_stable: Callable[[str, str, list[str]], list[str]],
    render_kpi_card: Callable[[str, str, str, str], None],
    fmt_brl_ptbr_celula: Callable[[object], str],
    col_referencia_como_texto: Callable[[pd.Series], pd.Series],
) -> None:
    try:
        vpath, fpath = descobrir_fontes_frete(CLIENTE_BASE_DIR)
    except Exception as exc:
        st.error("Erro ao localizar ficheiros de frete / vendas ML.")
        st.caption(str(exc))
        with st.expander("Detalhe técnico", expanded=False):
            st.exception(exc)
        return
    if not vpath:
        vendas_dir = Path(CLIENTE_BASE_DIR) / "Vendas - Mercado Livre"
        if not vendas_dir.is_dir():
            st.warning(
                "Não existe a pasta **Vendas - Mercado Livre** na base do cliente. "
                "Crie-a sob a pasta indicada abaixo ou defina **FDL_BASE_DIR** / segredo com a raiz certa."
            )
        else:
            st.warning(
                "A pasta **Vendas - Mercado Livre** existe mas **não há ficheiros .xlsx, .xls ou .csv** "
                "de vendas ML (export do relatório). Copie o export para essa pasta e atualize a página."
            )
        st.caption(str(Path(CLIENTE_BASE_DIR).resolve()))
        return
    v_ns = int(vpath.stat().st_mtime_ns)
    fp = str(fpath.resolve()) if fpath and fpath.is_file() else None
    f_ns = int(fpath.stat().st_mtime_ns) if fpath and fpath.is_file() else None
    try:
        base_df, meta = carregar_base_frete_ml(org_id, str(vpath.resolve()), v_ns, fp, f_ns)
    except Exception as exc:
        st.error("Falha ao ler vendas ML.")
        st.caption(str(exc))
        return
    for w in meta.get("avisos") or []:
        st.info(w)

    try:
        _painel_frete_conteudo(
            org_id=org_id,
            br_tz=br_tz,
            multiselect_stable=multiselect_stable,
            render_kpi_card=render_kpi_card,
            fmt_brl_ptbr_celula=fmt_brl_ptbr_celula,
            col_referencia_como_texto=col_referencia_como_texto,
            base_df=base_df,
            meta=meta,
            vpath=vpath,
        )
    except Exception as exc:
        st.error("Erro ao montar o painel de Frete. Detalhe abaixo.")
        st.exception(exc)


def _painel_frete_conteudo(
    *,
    org_id: str,
    br_tz: object,
    multiselect_stable: Callable[[str, str, list[str]], list[str]],
    render_kpi_card: Callable[[str, str, str, str], None],
    fmt_brl_ptbr_celula: Callable[[object], str],
    col_referencia_como_texto: Callable[[pd.Series], pd.Series],
    base_df: pd.DataFrame,
    meta: dict[str, object],
    vpath: Path,
) -> None:
    # Sufixo único por empresa + ficheiro: evita date_input/multiselect com estado antigo fora de min/max
    # (erro no servidor / ecrã branco na Cloud ao mudar Repasse → Frete ou ao atualizar o .xlsx).
    _sig = f"{org_id}_{vpath.stat().st_mtime_ns}"
    work = base_df.copy()
    if "_data_venda_dt" in work.columns:
        dts = work["_data_venda_dt"].dropna()
        if len(dts):
            d_min = dts.min().date()
            d_max = dts.max().date()
        else:
            d_min = d_max = datetime.now(br_tz).date()
    else:
        d_min = d_max = datetime.now(br_tz).date()
    if d_max < d_min:
        d_min, d_max = d_max, d_min

    estados = []
    if "Estado" in work.columns:
        estados = sorted(
            {str(x).strip() for x in work["Estado"].dropna().unique().tolist() if str(x).strip()}
        )

    st.markdown('<p class="filtros-panel-title">Filtros — Frete ML</p>', unsafe_allow_html=True)
    r1 = st.columns((1.2, 1.2, 1.6))
    with r1[0]:
        sel_est = multiselect_stable(f"frete_ms_estado_{_sig}", "Estado da venda", estados)
    with r1[1]:
        t_busca = st.text_input(
            "Busca (venda ou # anuncio)", "", key=f"frete_busca_{_sig}"
        ).strip().lower()
    with r1[2]:
        data_ini = st.date_input(
            "Data da venda — início",
            value=d_min,
            min_value=d_min,
            max_value=d_max,
            format="DD/MM/YYYY",
            key=f"frete_d_ini_{_sig}",
        )
    r2 = st.columns((1.2, 2.8))
    with r2[0]:
        data_fim = st.date_input(
            "Data da venda — fim",
            value=d_max,
            min_value=d_min,
            max_value=d_max,
            format="DD/MM/YYYY",
            key=f"frete_d_fim_{_sig}",
        )

    st.markdown(
        '<p class="fdl-frete-hint">Frete ML = <strong>Receita por envio + Tarifas de envio</strong> '
        "(soma com sinais do export, alinhado ao total Envios no ML).</p>",
        unsafe_allow_html=True,
    )

    if data_fim < data_ini:
        st.warning("Data final invalida.")
        data_fim = data_ini

    tbl = work
    if sel_est and "Estado" in tbl.columns:
        tbl = tbl[tbl["Estado"].isin(sel_est)]
    if t_busca:
        m = (
            tbl[FRETE_UI_N_VENDA].fillna("").astype(str).str.lower().str.contains(t_busca, regex=False)
            if FRETE_UI_N_VENDA in tbl.columns
            else pd.Series(False, index=tbl.index)
        )
        if FRETE_UI_ANUNCIO in tbl.columns:
            m = m | tbl[FRETE_UI_ANUNCIO].fillna("").astype(str).str.lower().str.contains(
                t_busca, regex=False
            )
        tbl = tbl.loc[m]

    if "_data_venda_dt" in tbl.columns and tbl["_data_venda_dt"].notna().any():
        dd = tbl["_data_venda_dt"].dt.normalize()
        ini = pd.Timestamp(data_ini)
        fim = pd.Timestamp(data_fim) + pd.Timedelta(days=1)
        tbl = tbl.loc[tbl["_data_venda_dt"].notna() & (dd >= ini) & (dd < fim)]

    tbl_show = tbl[[c for c in tbl.columns if not str(c).startswith("_")]].copy()

    ts_v = datetime.fromtimestamp(vpath.stat().st_mtime, tz=br_tz).strftime("%d/%m/%Y %H:%M")
    st.caption(
        f"**Ficheiro vendas:** {meta.get('vendas_arquivo')} · _{ts_v}_ · **Linhas:** {len(tbl_show)}"
    )

    fm = pd.to_numeric(tbl_show.get("Frete ML (receita+tarifa)"), errors="coerce")
    n_com_frete = int(fm.notna().sum())
    soma_frete = float(fm.fillna(0).sum())
    n_sem = int(len(tbl_show) - n_com_frete)

    st.markdown('<div class="section-title">Resumo do recorte</div>', unsafe_allow_html=True)
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("Vendas no recorte", f"{len(tbl_show):,}".replace(",", "."), "â—‡", "kpi-total")
    with k2:
        render_kpi_card("Com frete ML", f"{n_com_frete:,}".replace(",", "."), "âˆš", "kpi-ok")
    with k3:
        render_kpi_card("Sem dados envio", f"{n_sem:,}".replace(",", "."), "â—‹", "kpi-pend")
    with k4:
        render_kpi_card("Soma Frete ML", f"R$ {soma_frete:,.2f}", "â—†", "kpi-acao")

    if meta.get("frete_tabular") and FRETE_UI_STATUS_CONC in tbl_show.columns:
        div = tbl_show[tbl_show[FRETE_UI_STATUS_CONC].eq(FRETE_UI_VAL_DIVERGENCIA)]
        n_div = len(div)
        soma_abs = (
            float(pd.to_numeric(div[FRETE_UI_DIFERENCA], errors="coerce").abs().sum())
            if n_div and FRETE_UI_DIFERENCA in div.columns
            else 0.0
        )
        st.markdown('<div class="section-title">Maior divergencia por anuncio</div>', unsafe_allow_html=True)
        if n_div and FRETE_UI_ANUNCIO in div.columns and FRETE_UI_DIFERENCA in div.columns:
            dnum = div.copy()
            dnum["_ab"] = pd.to_numeric(dnum[FRETE_UI_DIFERENCA], errors="coerce").abs()
            grp = (
                dnum.groupby(FRETE_UI_ANUNCIO, dropna=False)
                .agg(vendas=(FRETE_UI_N_VENDA, "count"), impacto_r=("_ab", "sum"))
                .sort_values("impacto_r", ascending=False)
            )
            if grp.empty:
                st.info("Sem linhas agrupáveis por anúncio para o destaque.")
            else:
                top_id = str(grp.index[0])
                top_imp = float(grp.iloc[0]["impacto_r"])
                top_nv = int(grp.iloc[0]["vendas"])
                tit = ""
                if FRETE_UI_TITULO_ANUNCIO in tbl_show.columns:
                    sub = tbl_show.loc[
                        tbl_show[FRETE_UI_ANUNCIO].astype(str).eq(top_id), FRETE_UI_TITULO_ANUNCIO
                    ]
                    if len(sub):
                        tit = html_lib.escape(str(sub.iloc[0])[:120])
                extra = (
                    f'<br /><span style="font-size:0.82rem;color:#57534e">{tit}</span>' if tit else ""
                )
                top_safe = html_lib.escape(top_id)
                st.markdown(
                    f"""
                <div class="fdl-frete-spotlight">
                  <p class="fdl-fs-title">Anuncio com maior impacto |diferenca|</p>
                  <p class="fdl-fs-an">{top_safe}</p>
                  <p class="fdl-fs-metrics">
                    <strong>{top_nv}</strong> venda(s) · <strong>R$ {top_imp:,.2f}</strong> |Delta| acumulado
                    {extra}
                  </p>
                  <p class="fdl-fs-metrics" style="margin-top:0.6rem;">
                    Linhas em divergencia: <strong>{n_div}</strong> · |Delta| total: <strong>R$ {soma_abs:,.2f}</strong>
                  </p>
                </div>
                """,
                    unsafe_allow_html=True,
                )
                chart_df = grp.head(8).reset_index()
                id_col = chart_df.columns[0]
                chart_df = chart_df[[id_col, "impacto_r"]].rename(
                    columns={id_col: "Anuncio", "impacto_r": "Impacto"}
                )
                chart_df = chart_df.set_index("Anuncio")
                st.markdown('<div class="section-title">Top anuncios</div>', unsafe_allow_html=True)
                try:
                    st.bar_chart(chart_df)
                except Exception:
                    st.dataframe(chart_df.reset_index(), use_container_width=True, hide_index=True)
        elif n_div == 0:
            st.success("Sem divergencias acima da tolerancia.")
    elif not meta.get("frete_tabular"):
        st.markdown('<div class="section-title">Frete por anuncio</div>', unsafe_allow_html=True)
        st.info("Sem tabela MLB+preco reconhecida na pasta do cliente.")

    st.markdown('<div class="section-title">Tabela</div>', unsafe_allow_html=True)
    t_grid = _dataframe_frete_grid(tbl_show, fmt_brl_ptbr_celula, col_referencia_como_texto)
    st.dataframe(
        t_grid,
        column_config=_column_config_frete(t_grid),
        use_container_width=True,
        hide_index=True,
        height=min(520, 120 + 28 * min(len(t_grid), 18)),
    )
    st.download_button(
        "Exportar CSV",
        tbl_show.to_csv(index=False).encode("utf-8-sig"),
        file_name="conciliacao_frete_filtrada.csv",
        mime="text/csv",
        key=f"frete_dl_csv_{_sig}",
    )


