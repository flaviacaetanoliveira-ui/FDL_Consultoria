"""UI para Conciliação de Frete (importada por app_operacional)."""
from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import streamlit as st
from streamlit.column_config import TextColumn

from fdl_paths import CLIENTE_BASE_DIR, resolve_pasta_vendas_ml
from operacional_frete import (
    FRETE_ML_COL,
    FRETE_UI_ANUNCIO,
    FRETE_UI_CLASSIFICACAO,
    FRETE_UI_DIFERENCA,
    FRETE_UI_FRETE_ESPERADO,
    FRETE_UI_N_VENDA,
    FRETE_UI_QTD_PRECO_ML,
    FRETE_UI_STATUS_CONC,
    FRETE_UI_ANALISADO_COBRADO_MAIOR,
    FRETE_UI_ANALISADO_COBRADO_MENOR,
    FRETE_UI_ANALISADO_REPASSE_FRETE,
    FRETE_UI_STATUS_SEM_FRETE_ML,
    FRETE_UI_TITULO_ANUNCIO,
    FRETE_UI_VAL_ACAO_MAIOR,
    FRETE_UI_VAL_ACAO_MENOR,
    FRETE_UI_VAL_ACAO_OK,
    FRETE_UI_VAL_ACAO_REPASSE,
    FRETE_UI_VAL_DIVERGENCIA,
    FRETE_UI_VALOR_FRETE_ANUNCIO,
    FRETE_UI_SITUACAO_FRETE,
    FRETE_UI_ACAO_RECOMENDADA,
    FRETE_UI_RECEBIDO,
    compute_frete_situacao_frete_column,
    carregar_tabela_final_frete_operacional,
    dataframe_frete_conciliacao_principal,
    descobrir_fontes_frete,
    frete_vendas_loader_args,
    frete_series_for_date_filter,
    frete_series_normalize_sale_dt,
    stable_mtime_ns_for_frete_url,
)


def _frete_conciliacao_grid_com_icones(df: pd.DataFrame) -> pd.DataFrame:
    """Prefixa ícones às colunas «Situação do Frete» e «Ação Recomendada» (só grelha; export usa `tbl_show`)."""
    if df.empty:
        return df
    out = df.copy()
    sc, ac = "Situação do Frete", "Ação Recomendada"
    sit_map = {
        "OK": "✅ OK",
        FRETE_UI_ANALISADO_REPASSE_FRETE: f"🚚 {FRETE_UI_ANALISADO_REPASSE_FRETE}",
        FRETE_UI_ANALISADO_COBRADO_MAIOR: f"⬆️ {FRETE_UI_ANALISADO_COBRADO_MAIOR}",
        FRETE_UI_ANALISADO_COBRADO_MENOR: f"⬇️ {FRETE_UI_ANALISADO_COBRADO_MENOR}",
    }
    ac_map = {
        FRETE_UI_VAL_ACAO_OK: f"✅ {FRETE_UI_VAL_ACAO_OK}",
        FRETE_UI_VAL_ACAO_REPASSE: f"📥 {FRETE_UI_VAL_ACAO_REPASSE}",
        FRETE_UI_VAL_ACAO_MAIOR: f"📞 {FRETE_UI_VAL_ACAO_MAIOR}",
        FRETE_UI_VAL_ACAO_MENOR: f"🔍 {FRETE_UI_VAL_ACAO_MENOR}",
    }

    def _ms(x: object) -> object:
        if pd.isna(x):
            return x
        vs = str(x).strip()
        return sit_map.get(vs, x)

    def _ma(x: object) -> object:
        if pd.isna(x):
            return x
        vs = str(x).strip()
        return ac_map.get(vs, x)

    if sc in out.columns:
        out[sc] = out[sc].map(_ms)
    if ac in out.columns:
        out[ac] = out[ac].map(_ma)
    return out


def _cell_style_frete_status(val: object) -> str:
    """Cores semânticas: OK → verde; divergência/cobrado a maior → vermelho; resto → amarelo (atenção)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    s = str(val).strip()
    if not s:
        return ""
    if s == "OK" or s.startswith("✅"):
        return "background-color: #e8f5e9; color: #1b5e20; font-weight: 600"
    if FRETE_UI_VAL_DIVERGENCIA in s or "Diverg" in s or "Cobrado a maior" in s:
        return "background-color: #ffebee; color: #b71c1c; font-weight: 600"
    return "background-color: #fff8e1; color: #e65100; font-weight: 500"


def frete_executivo_display_styled(df: pd.DataFrame) -> Any:
    """
    Grelha executiva com fundo semântico em «Situação do Frete» e «Status conciliação».
    Devolve pandas Styler ou DataFrame se não houver colunas a estilizar.
    """
    if df.empty:
        return df
    cols = [c for c in ("Situação do Frete", FRETE_UI_STATUS_CONC) if c in df.columns]
    if not cols:
        return df
    try:
        return df.style.map(_cell_style_frete_status, subset=cols)
    except AttributeError:
        return df


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
        "Custo do envio (BRL)",
        FRETE_ML_COL,
        FRETE_UI_VALOR_FRETE_ANUNCIO,
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
        if c in (FRETE_UI_N_VENDA, FRETE_UI_ANUNCIO, "Número do anúncio", "Data da venda", "N.º venda"):
            cfg[c] = TextColumn(str(c), width="medium")
        elif c in ("Estado", "Estado da venda", FRETE_UI_SITUACAO_FRETE):
            cfg[c] = TextColumn(str(c), width="small")
        elif c == FRETE_UI_ACAO_RECOMENDADA:
            cfg[c] = TextColumn(str(c), width="medium")
        elif c == FRETE_UI_RECEBIDO:
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
        fontes = descobrir_fontes_frete(CLIENTE_BASE_DIR)
    except Exception as exc:
        st.error("Erro ao localizar ficheiros de frete / vendas ML.")
        st.caption(str(exc))
        with st.expander("Detalhe técnico", expanded=False):
            st.exception(exc)
        return
    vendas_ref, v_ns = frete_vendas_loader_args(fontes)
    if not vendas_ref:
        vendas_dir = resolve_pasta_vendas_ml(CLIENTE_BASE_DIR)
        if not vendas_dir.is_dir():
            st.warning(
                "Não existe fonte de vendas ML: defina **FDL_FRETE_VENDAS_URL** nos Secrets ou a pasta "
                "**Vendas - Mercado Livre** / **Vendas_ML** na base do cliente (**FDL_BASE_DIR**)."
            )
        else:
            st.warning(
                f"A pasta **{vendas_dir.name}** existe mas **não há ficheiros .xlsx, .xls ou .csv** "
                "de vendas ML (export do relatório). Copie o export para essa pasta ou configure **FDL_FRETE_VENDAS_URL**."
            )
        st.caption(str(Path(CLIENTE_BASE_DIR).resolve()))
        return
    frete_ref = (fontes.frete_url or "").strip() or (
        str(fontes.frete_path.resolve())
        if fontes.frete_path and fontes.frete_path.is_file()
        else None
    )
    if (fontes.frete_url or "").strip():
        f_ns = stable_mtime_ns_for_frete_url(fontes.frete_url)
    elif fontes.frete_path and fontes.frete_path.is_file():
        f_ns = int(fontes.frete_path.stat().st_mtime_ns)
    else:
        f_ns = None
    try:
        base_df, meta = carregar_tabela_final_frete_operacional(org_id, vendas_ref, v_ns, frete_ref, f_ns)
    except Exception as exc:
        st.error("Falha ao ler vendas ML.")
        st.caption(str(exc))
        return
    for w in meta.get("avisos") or []:
        st.info(w)

    try:
        # Modo seguro temporário: evita ecrã branco na Cloud em navegadores afetados.
        # Mostra os dados essenciais de frete sem blocos HTML complexos.
        _painel_frete_conteudo_safe(
            fmt_brl_ptbr_celula=fmt_brl_ptbr_celula,
            col_referencia_como_texto=col_referencia_como_texto,
            base_df=base_df,
            meta=meta,
            vpath=fontes.vendas_path,
            vendas_url=(fontes.vendas_url or "").strip(),
            br_tz=br_tz,
        )
    except Exception as exc:
        st.error("Erro ao montar o painel de Frete. Detalhe abaixo.")
        st.exception(exc)


def _painel_frete_conteudo_safe(
    *,
    fmt_brl_ptbr_celula: Callable[[object], str],
    col_referencia_como_texto: Callable[[pd.Series], pd.Series],
    base_df: pd.DataFrame,
    meta: dict[str, object],
    vpath: Path | None,
    vendas_url: str,
    br_tz: object,
) -> None:
    today = datetime.now(br_tz).date()
    ini_30 = today - timedelta(days=29)
    work = base_df
    recorte_30 = False
    fdt = frete_series_normalize_sale_dt(frete_series_for_date_filter(base_df))
    if fdt.notna().any():
        ini_ts = pd.Timestamp(ini_30)
        fim_ts = pd.Timestamp(today) + pd.Timedelta(days=1)
        m = fdt.notna() & (fdt >= ini_ts) & (fdt < fim_ts)
        work = base_df.loc[m]
        recorte_30 = True

    tbl_show = work[[c for c in work.columns if not str(c).startswith("_")]].copy()
    if "data_venda" not in tbl_show.columns and "_data_venda_dt" in work.columns:
        tbl_show["data_venda"] = work["_data_venda_dt"]
    if FRETE_UI_CLASSIFICACAO in tbl_show.columns:
        tbl_show = tbl_show.drop(columns=[FRETE_UI_CLASSIFICACAO])
    if vpath is not None:
        ts_v = datetime.fromtimestamp(vpath.stat().st_mtime, tz=br_tz).strftime("%d/%m/%Y %H:%M")
    elif vendas_url:
        ts_v = "fonte remota (URL)"
    else:
        ts_v = "—"
    if recorte_30:
        st.caption(
            f"Ficheiro vendas: {meta.get('vendas_arquivo')} | {ts_v} | "
            f"**Cenário A (30 dias):** {ini_30.strftime('%d/%m/%Y')} a {today.strftime('%d/%m/%Y')} — "
            f"{len(tbl_show)} linhas"
        )
    else:
        st.caption(
            f"Ficheiro vendas: {meta.get('vendas_arquivo')} | {ts_v} | "
            f"{len(tbl_show)} linhas (sem coluna de data — sem recorte de 30 dias)."
        )

    fm = pd.to_numeric(tbl_show.get(FRETE_ML_COL), errors="coerce")
    soma_frete = float(fm.fillna(0).sum())
    stc = tbl_show[FRETE_UI_STATUS_CONC] if FRETE_UI_STATUS_CONC in tbl_show.columns else None
    n_repasse = int(
        compute_frete_situacao_frete_column(tbl_show).eq(FRETE_UI_ANALISADO_REPASSE_FRETE).sum()
    )
    n_sem_ml = int(stc.eq(FRETE_UI_STATUS_SEM_FRETE_ML).sum()) if stc is not None else 0

    c1, c2, c3 = st.columns(3)
    c1.metric("Vendas", f"{len(tbl_show):,}".replace(",", "."))
    c2.metric("Repasse de frete (situação)", f"{n_repasse:,}".replace(",", "."))
    c3.metric("Soma frete cobrado", f"R$ {soma_frete:,.2f}")
    if n_sem_ml:
        st.info(f"Linhas sem receita e tarifas de envio no ML: {n_sem_ml}")

    if meta.get("frete_tabular") and FRETE_UI_STATUS_CONC in tbl_show.columns:
        n_div = int(tbl_show[FRETE_UI_STATUS_CONC].eq(FRETE_UI_VAL_DIVERGENCIA).sum())
        st.caption(f"Divergências (status técnico): {n_div} · Repasse de frete (situação): {n_repasse}")
    elif not meta.get("frete_tabular"):
        st.info("Sem tabela de frete por anúncio reconhecida na pasta do cliente.")

    t_grid = _dataframe_frete_grid(tbl_show, fmt_brl_ptbr_celula, col_referencia_como_texto)
    t_main = dataframe_frete_conciliacao_principal(t_grid)
    st.dataframe(
        t_main,
        column_config=_column_config_frete(t_main),
        use_container_width=True,
        hide_index=True,
        height=520,
    )
    st.download_button(
        "Exportar CSV",
        tbl_show.to_csv(index=False).encode("utf-8-sig"),
        file_name="conciliacao_frete_filtrada.csv",
        mime="text/csv",
        key="frete_dl_csv_safe",
    )


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
    today = datetime.now(br_tz).date()
    # Janela por omissão: últimos 30 dias corridos até hoje (inclusive).
    default_ini = today - timedelta(days=29)
    default_fim = today

    fdt_bounds = frete_series_normalize_sale_dt(frete_series_for_date_filter(work))
    if fdt_bounds.notna().any():
        d_min_data = fdt_bounds.min().date()
        d_max_data = fdt_bounds.max().date()
    else:
        d_min_data = d_max_data = today

    picker_min = min(d_min_data, default_ini)
    picker_max = max(d_max_data, default_fim, today)
    if picker_max < picker_min:
        picker_min, picker_max = picker_max, picker_min

    d_ini_val = max(picker_min, min(default_ini, picker_max))
    d_fim_val = max(picker_min, min(default_fim, picker_max))
    if d_ini_val > d_fim_val:
        d_ini_val = d_fim_val

    estados = []
    if "Estado" in work.columns:
        estados = sorted(
            {str(x).strip() for x in work["Estado"].dropna().unique().tolist() if str(x).strip()}
        )

    st.subheader("Filtros — Frete ML")
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
            value=d_ini_val,
            min_value=picker_min,
            max_value=picker_max,
            format="DD/MM/YYYY",
            key=f"frete_d_ini_{_sig}",
        )
    r2 = st.columns((1.2, 2.8))
    with r2[0]:
        data_fim = st.date_input(
            "Data da venda — fim",
            value=d_fim_val,
            min_value=picker_min,
            max_value=picker_max,
            format="DD/MM/YYYY",
            key=f"frete_d_fim_{_sig}",
        )

    st.caption(
        "**Cenário A (regra do painel):** últimos **30 dias corridos** incluindo hoje — "
        "primeiro dia = hoje − 29 dias; último dia = hoje (fuso horário da app). "
        f"**Período por omissão agora:** {default_ini.strftime('%d/%m/%Y')} a "
        f"{default_fim.strftime('%d/%m/%Y')}. "
        "*Ex.: se hoje for 28/03/2026 → 27/02/2026 a 28/03/2026.* "
        "Ajuste as datas acima para outro período."
    )
    st.caption(
        "**Frete cobrado** = valor absoluto de **Receita por envio + Tarifas de envio** (sempre ≥ 0). "
        "A coluna **Custo do envio** não entra no cálculo."
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

    if "data_venda" in tbl.columns or "_data_venda_dt" in tbl.columns:
        dd = frete_series_normalize_sale_dt(frete_series_for_date_filter(tbl))
        if dd.notna().any():
            ini = pd.Timestamp(data_ini)
            fim = pd.Timestamp(data_fim) + pd.Timedelta(days=1)
            tbl = tbl.loc[dd.notna() & (dd >= ini) & (dd < fim)]

    tbl_show = tbl[[c for c in tbl.columns if not str(c).startswith("_")]].copy()
    if "data_venda" not in tbl_show.columns and "_data_venda_dt" in tbl.columns:
        tbl_show["data_venda"] = tbl["_data_venda_dt"]
    if FRETE_UI_CLASSIFICACAO in tbl_show.columns:
        tbl_show = tbl_show.drop(columns=[FRETE_UI_CLASSIFICACAO])

    ts_v = datetime.fromtimestamp(vpath.stat().st_mtime, tz=br_tz).strftime("%d/%m/%Y %H:%M")
    st.caption(
        f"**Ficheiro vendas:** {meta.get('vendas_arquivo')} · _{ts_v}_ · **Linhas:** {len(tbl_show)}"
    )

    fm = pd.to_numeric(tbl_show.get(FRETE_ML_COL), errors="coerce")
    soma_frete = float(fm.fillna(0).sum())
    stc = tbl_show[FRETE_UI_STATUS_CONC] if FRETE_UI_STATUS_CONC in tbl_show.columns else None
    n_repasse = int(
        compute_frete_situacao_frete_column(tbl_show).eq(FRETE_UI_ANALISADO_REPASSE_FRETE).sum()
    )
    n_sem_ml = int(stc.eq(FRETE_UI_STATUS_SEM_FRETE_ML).sum()) if stc is not None else 0

    st.subheader("Resumo do recorte")
    k1, k2, k3, k4 = st.columns(4)
    with k1:
        render_kpi_card("Vendas no recorte", f"{len(tbl_show):,}".replace(",", "."), "\u25c6", "kpi-total")
    with k2:
        render_kpi_card("Repasse de frete (situação)", f"{n_repasse:,}".replace(",", "."), "\u2713", "kpi-ok")
    with k3:
        render_kpi_card(
            "Sem info envio (ML)",
            f"{n_sem_ml:,}".replace(",", "."),
            "\u25cb",
            "kpi-pend",
        )
    with k4:
        render_kpi_card("Soma frete cobrado", f"R$ {soma_frete:,.2f}", "\u2605", "kpi-acao")

    if meta.get("frete_tabular") and FRETE_UI_STATUS_CONC in tbl_show.columns:
        div = tbl_show[tbl_show[FRETE_UI_STATUS_CONC].eq(FRETE_UI_VAL_DIVERGENCIA)]
        n_div = len(div)
        soma_abs = (
            float(pd.to_numeric(div[FRETE_UI_DIFERENCA], errors="coerce").abs().sum())
            if n_div and FRETE_UI_DIFERENCA in div.columns
            else 0.0
        )
        st.subheader("Maior divergência por anúncio")
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
                        tit = str(sub.iloc[0])[:120]
                with st.container(border=True):
                    st.caption("Anúncio com maior impacto |Δ|")
                    st.markdown(f"**Identificador:** `{top_id}`")
                    if tit:
                        st.caption(tit)
                    st.write(
                        f"**{top_nv}** venda(s) · **R$ {top_imp:,.2f}** |Δ| acumulado no grupo"
                    )
                    st.caption(
                        f"Linhas em divergência: **{n_div}** · |Δ| total: **R$ {soma_abs:,.2f}**"
                    )
                chart_df = grp.head(8).reset_index()
                id_col = chart_df.columns[0]
                chart_df = chart_df[[id_col, "impacto_r"]].rename(
                    columns={id_col: "Anuncio", "impacto_r": "Impacto"}
                )
                chart_df = chart_df.set_index("Anuncio")
                st.subheader("Top anúncios")
                try:
                    st.bar_chart(chart_df)
                except Exception:
                    st.dataframe(chart_df.reset_index(), use_container_width=True, hide_index=True)
        elif n_div == 0:
            st.success("Sem divergencias acima da tolerancia.")
    elif not meta.get("frete_tabular"):
        st.subheader("Frete por anúncio")
        st.info("Sem tabela MLB+preco reconhecida na pasta do cliente.")

    st.subheader("Tabela")
    t_grid = _dataframe_frete_grid(tbl_show, fmt_brl_ptbr_celula, col_referencia_como_texto)
    t_main = _frete_conciliacao_grid_com_icones(dataframe_frete_conciliacao_principal(t_grid))
    _h_df = min(520, 120 + 28 * min(len(t_main), 18))
    try:
        st.dataframe(
            t_main,
            column_config=_column_config_frete(t_main),
            use_container_width=True,
            hide_index=True,
            height=_h_df,
        )
    except Exception:
        st.dataframe(t_main, use_container_width=True, hide_index=True, height=_h_df)
    st.download_button(
        "Exportar CSV",
        tbl_show.to_csv(index=False).encode("utf-8-sig"),
        file_name="conciliacao_frete_filtrada.csv",
        mime="text/csv",
        key=f"frete_dl_csv_{_sig}",
    )


