"""Painel «Apuração Fiscal» — reutiliza funções de ``app_operacional`` (import tardio)."""

from __future__ import annotations

import html
from datetime import datetime

import pandas as pd
import streamlit as st

from faturamento_dre_recorte import _BR_TZ
from faturamento_dre_recorte_minimo import (
    build_faturamento_fiscal_base_slice,
    build_nf_panel_aligned_to_fiscal_base,
    compute_nf_panel_kpis,
    faturamento_min_series_nf_emissao_bounds_dates,
    faturamento_recorte_min_state_from_session,
    _min_cal_limits,
)
from processing.faturamento.fiscal_materializado import fiscal_contract_dataframe_valid
from processing.faturamento.nf_materializado import nf_first_contract_dataframe_valid
from processing.faturamento.nf_panel_materializado import nf_panel_materializado_dataframe_valid


def render_apuracao_fiscal_panel(
    df: pd.DataFrame,
    load_info: dict[str, object],
    ts_proc: str,
    *,
    org_id: str,
) -> None:
    import app_operacional as ao

    ao._fdl_fat_min_inject_ui_styles()

    use_nf_panel_baked = bool(load_info.get("faturamento_nf_panel_baked"))
    _df_nf_panel = load_info.get("faturamento_nf_panel_df")
    _df_nf_contract = load_info.get("faturamento_nf_df")
    use_nf_panel_baked_effective = (
        use_nf_panel_baked
        and isinstance(_df_nf_panel, pd.DataFrame)
        and nf_panel_materializado_dataframe_valid(_df_nf_panel)
    )
    df_nf_pre = _df_nf_panel if use_nf_panel_baked_effective else _df_nf_contract
    use_nf_materializado = False
    if use_nf_panel_baked_effective:
        use_nf_materializado = isinstance(df_nf_pre, pd.DataFrame) and not df_nf_pre.empty
    elif (
        bool(load_info.get("faturamento_nf_first"))
        and isinstance(df_nf_pre, pd.DataFrame)
        and nf_first_contract_dataframe_valid(df_nf_pre)
    ):
        use_nf_materializado = True
    if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame) and df_nf_pre.empty and not df.empty:
        use_nf_materializado = False

    _nf_panel_ads_ui = bool(load_info.get("faturamento_nf_panel_ads", True))

    df_fiscal_pre = load_info.get("faturamento_fiscal_df")
    df_devolucoes_pre = load_info.get("faturamento_devolucoes_df")
    _df_dev_ok = isinstance(df_devolucoes_pre, pd.DataFrame)
    use_fiscal_parquet = (
        bool(load_info.get("faturamento_fiscal_first"))
        and isinstance(df_fiscal_pre, pd.DataFrame)
        and fiscal_contract_dataframe_valid(df_fiscal_pre)
    )
    use_fiscal_kpi = bool(
        use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame) and fiscal_contract_dataframe_valid(df_fiscal_pre)
    )

    if not use_nf_panel_baked_effective:
        st.error(
            "**Dados por nota fiscal indisponíveis.** "
            "Esta área usa a base consolidada **já publicada** para a sua organização."
        )
        st.caption(
            "Peça a atualização pelo processo habitual de fecho ou aguarde a próxima publicação de dados."
        )
        _pe = load_info.get("faturamento_nf_panel_error")
        if _pe:
            _pe_line = (
                f"Erro técnico: `{html.escape(str(_pe))}`"
                if ao._is_admin_mode()
                else "Não foi possível carregar a base consolidada. Tente novamente mais tarde ou contacte o suporte."
            )
            st.caption(_pe_line)
        elif not use_nf_panel_baked:
            st.caption(
                "A base consolidada ainda não está disponível neste ambiente ou não foi encontrada. "
                "Volte mais tarde ou contacte o suporte."
            )
        elif not isinstance(_df_nf_panel, pd.DataFrame):
            st.caption("Não foi possível preparar a tabela neste momento. Recarregue a página ou tente mais tarde.")
        elif _df_nf_panel.empty:
            st.info(
                "Não há linhas para o período e empresa selecionados. "
                "Verifique filtros ou o escopo na barra lateral."
            )
        elif ao._is_admin_mode():
            st.warning(
                "Contrato do painel incompleto (faltam colunas obrigatórias). "
                "Rematerialize o faturamento com a versão atual do pipeline."
            )
        return

    if df.empty and not use_nf_materializado:
        st.info(
            "Sem dados de faturamento para este escopo. Confirme **materialização**, **slug** do cliente "
            "e o **escopo** (empresa ativa / consolidado) na barra lateral."
        )
        return

    _bounds_parts: list[pd.DataFrame] = []
    _base_bounds = df_nf_pre if use_nf_materializado else df
    if isinstance(_base_bounds, pd.DataFrame) and not _base_bounds.empty:
        _bounds_parts.append(_base_bounds)
    if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame) and not df_fiscal_pre.empty:
        _bounds_parts.append(df_fiscal_pre)
    _df_bounds = (
        pd.concat(_bounds_parts, ignore_index=True)
        if len(_bounds_parts) > 1
        else (_bounds_parts[0] if _bounds_parts else pd.DataFrame())
    )
    if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame) and df_nf_pre.empty:
        st.info(
            "Sem notas fiscais neste recorte. Confirme filtros de data, empresa e consolidado na barra lateral "
            "e que a base consolidada está atualizada."
        )
        return
    nf_min, nf_max, ok_nf_dates = faturamento_min_series_nf_emissao_bounds_dates(_df_bounds)
    _emit_floor = ao._FDL_FAT_DRE_MIN_PANEL_NF_EMISSAO_DESDE
    if ok_nf_dates:
        nf_cal_min, nf_cal_max = _min_cal_limits(nf_min, nf_max)
        nf_cal_min = max(nf_cal_min, _emit_floor)
        if nf_max >= _emit_floor:
            nf_min = max(nf_min, _emit_floor)
        else:
            nf_min, nf_max = _emit_floor, _emit_floor
        nf_cal_max = max(nf_cal_max, nf_max, nf_min, nf_cal_min)
    else:
        nf_cal_min, nf_cal_max = (nf_min, nf_max)

    _nf_sig_k = "fdl_apu_nf_bounds_sig"
    _today = datetime.now(_BR_TZ).date()
    if ok_nf_dates:
        _nf_bs = (nf_min.isoformat(), nf_max.isoformat())
        if st.session_state.get(_nf_sig_k) != _nf_bs:
            st.session_state[_nf_sig_k] = _nf_bs
            _nfi = nf_min
            _nff = min(nf_max, _today)
            _nfi = min(max(_nfi, nf_cal_min), nf_cal_max)
            _nff = min(max(_nff, nf_cal_min), nf_cal_max)
            if _nff < _nfi:
                _nff = _nfi
            st.session_state["fdl_apu_nf_d_ini"] = _nfi
            st.session_state["fdl_apu_nf_d_fim"] = _nff
        if "fdl_apu_nf_d_ini" not in st.session_state:
            _nfi = nf_min
            _nff = min(nf_max, _today)
            _nfi = min(max(_nfi, nf_cal_min), nf_cal_max)
            _nff = min(max(_nff, nf_cal_min), nf_cal_max)
            if _nff < _nfi:
                _nff = _nfi
            st.session_state["fdl_apu_nf_d_ini"] = _nfi
            st.session_state["fdl_apu_nf_d_fim"] = _nff
        st.session_state["fdl_apu_nf_d_ini"] = min(
            max(ao._safe_streamlit_date(st.session_state["fdl_apu_nf_d_ini"], nf_min), nf_cal_min),
            nf_cal_max,
        )
        st.session_state["fdl_apu_nf_d_fim"] = min(
            max(ao._safe_streamlit_date(st.session_state["fdl_apu_nf_d_fim"], nf_max), nf_cal_min),
            nf_cal_max,
        )

    emp_opts = ao._faturamento_dre_etiquetas_empresa_recorte(_df_bounds)

    _FDL_APURACAO_RESET_KEYS = (
        "fdl_apu_emp",
        "fdl_apu_plat",
        "fdl_apu_nf_sit",
        "fdl_apu_nf_d_ini",
        "fdl_apu_nf_d_fim",
        "fdl_apu_nf_bounds_sig",
        "fdl_apu_prod",
        "fdl_apu_venda_sinal",
        "fdl_apu_sinal_resultado",
        "fdl_apu_sinais_resultado",
        "fdl_apu_nf_show_diferenca",
        "fdl_apu_nf_opt_plat",
        "fdl_apu_nf_opt_sit",
        "fdl_apu_nf_opt_ped",
        "fdl_apu_nf_opt_linhas",
        "fdl_apu_nf_opt_qtd",
        "fdl_apu_nf_opt_vf",
        "fdl_apu_nf_opt_rf",
        "fdl_apu_nf_opt_rp",
        "fdl_apu_nf_opt_tar",
        "fdl_apu_nf_opt_df",
        "fdl_apu_nf_opt_ads",
        "fdl_apu_nf_opt_alert",
        "fdl_apu_nf_tbl_plataforma",
        "fdl_apu_nf_tbl_busca",
        "fdl_apu_nf_pg",
    )

    with st.container(border=True):
        _fh_t, _fh_b = st.columns((4, 1))
        with _fh_t:
            st.markdown("**Filtros**")
        with _fh_b:
            if st.button(
                "Limpar filtros",
                key="fdl_apu_reset",
                type="secondary",
                use_container_width=True,
                help="Repor empresa, datas de emissão NF e filtros da tabela ao padrão (independentes do Resultado Gerencial).",
            ):
                for _k in _FDL_APURACAO_RESET_KEYS:
                    st.session_state.pop(_k, None)
                st.rerun()
        _fc1, _fc2 = st.columns(2)
        with _fc1:
            if emp_opts:
                if "fdl_apu_emp" not in st.session_state:
                    st.session_state["fdl_apu_emp"] = []
                else:
                    prev_e = st.session_state["fdl_apu_emp"]
                    if isinstance(prev_e, list):
                        st.session_state["fdl_apu_emp"] = [x for x in prev_e if x in emp_opts]
                    else:
                        st.session_state["fdl_apu_emp"] = []
                st.multiselect(
                    "Empresa",
                    emp_opts,
                    key="fdl_apu_emp",
                    help="**Vazio** = todas as empresas neste carregamento. Estado **independente** do módulo Resultado Gerencial.",
                    placeholder="Todas",
                )
            else:
                st.caption("Empresa: sem opções distintas no recorte atual.")
        with _fc2:
            st.caption(
                "Período de emissão abaixo aplica-se ao **recorte fiscal** e à **tabela**. "
                "Filtros deste módulo não alteram o Resultado Gerencial."
            )
        if ok_nf_dates:
            st.markdown(
                '<p class="fdl-fat-filtros-periodo-tit">Período de emissão</p>',
                unsafe_allow_html=True,
            )
            r_nf = st.columns((1, 1))
            with r_nf[0]:
                st.date_input(
                    "Início",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_apu_nf_d_ini",
                    help=ao._FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
            with r_nf[1]:
                st.date_input(
                    "Fim",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_apu_nf_d_fim",
                    help=ao._FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
        elif "Nota_Data_Emissao" in _df_bounds.columns:
            st.caption("Período de emissão indisponível (datas não utilizáveis em Nota_Data_Emissao).")
        else:
            st.caption("Período de emissão indisponível (sem coluna Nota_Data_Emissao).")

    ao._fdl_ui_gap_section()
    ao._fdl_fat_min_vsp(size="md")

    _min_state = faturamento_recorte_min_state_from_session(
        st.session_state,
        key_emp="fdl_apu_emp",
        key_plat="fdl_apu_plat",
        key_sit="fdl_apu_nf_sit",
    )
    _nf_kpi_ini = ao._safe_streamlit_date(st.session_state.get("fdl_apu_nf_d_ini"), nf_min)
    _nf_kpi_fim = ao._safe_streamlit_date(st.session_state.get("fdl_apu_nf_d_fim"), nf_max)
    if ok_nf_dates:
        _nf_kpi_ini = min(max(_nf_kpi_ini, nf_cal_min), nf_cal_max)
        _nf_kpi_fim = min(max(_nf_kpi_fim, nf_cal_min), nf_cal_max)
        if _nf_kpi_fim < _nf_kpi_ini:
            _nf_kpi_fim = _nf_kpi_ini

    _df_fiscal_base, _fiscal_base_stats = build_faturamento_fiscal_base_slice(
        df_fiscal_pre
        if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame)
        else pd.DataFrame(),
        empresas_sel=_min_state.empresas,
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
        situacoes_sel=_min_state.situacoes_nf,
        df_devolucoes=df_devolucoes_pre if _df_dev_ok else None,
    )
    ao._render_faturamento_dre_fiscal_base_top(
        stats=_fiscal_base_stats,
        ok_nf_dates=ok_nf_dates,
        empresas_sel=_min_state.empresas,
        emp_opts=emp_opts,
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        fiscal_parquet_ok=use_fiscal_parquet,
        situacoes_nf_sel=_min_state.situacoes_nf,
    )

    df_nf_scope_emissao = ao._faturamento_nf_apply_minimal_recorte(
        df_nf_pre,
        empresas_sel=_min_state.empresas,
        plataformas_sel=(),
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
    )
    df_nf_scope_emissao = ao._faturamento_nf_filter_by_situacao(df_nf_scope_emissao, _min_state.situacoes_nf)
    _aligned_fiscal = bool(not _df_fiscal_base.empty)
    if _aligned_fiscal:
        df_nf_commercial_kpi = build_nf_panel_aligned_to_fiscal_base(_df_fiscal_base, df_nf_scope_emissao)
    else:
        df_nf_commercial_kpi = df_nf_scope_emissao.copy()
    if _min_state.plataformas:
        df_nf_commercial_kpi = ao._nf_panel_filter_merged_fiscal_by_plataforma_resumo(
            df_nf_commercial_kpi, _min_state.plataformas
        )
    _kp_cards = compute_nf_panel_kpis(df_nf_commercial_kpi)

    ao._fdl_fat_min_vsp(size="sm")
    _c1, _c2, _c3, _c4 = st.columns(4)
    _base_disp = ao._fmt_brl_ptbr_celula(_fiscal_base_stats.base_fiscal_liquida) if ok_nf_dates else "—"
    _nf_emi = ao._fmt_int_ptbr(_fiscal_base_stats.n_nf)
    _nf_dev_n = ao._fmt_int_ptbr(_fiscal_base_stats.nfs_devolucao)
    _nf_dev_v = ao._fmt_brl_ptbr_celula(_fiscal_base_stats.total_devolvido) or "R$ 0,00"
    _imp_p = ao._fmt_brl_ptbr_celula(_kp_cards.get("imposto", 0.0)) if ok_nf_dates else "—"
    with _c1:
        st.metric("Base Fiscal Líquida", _base_disp)
    with _c2:
        st.metric("NFs emitidas", _nf_emi)
    with _c3:
        st.metric("NFs devolução", f"{_nf_dev_n} · {_nf_dev_v}")
    with _c4:
        st.metric("Imposto do período", _imp_p)

    ao._fdl_fat_min_vsp(size="md")
    ao._fdl_fat_divider_simple()
    ao._fdl_fat_min_vsp(size="sm")

    ao._render_faturamento_dre_nf_table_section(
        df_nf_pre=df_nf_pre,
        df=df,
        df_fiscal_pre=df_fiscal_pre,
        load_info=load_info,
        _min_state=_min_state,
        _nf_kpi_ini=_nf_kpi_ini,
        _nf_kpi_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
        use_fiscal_kpi=use_fiscal_kpi,
        use_nf_materializado=use_nf_materializado,
        use_fiscal_parquet=use_fiscal_parquet,
        _nf_panel_ads_ui=_nf_panel_ads_ui,
        _df_fiscal_base=_df_fiscal_base,
        _fiscal_base_stats=_fiscal_base_stats,
        _kp_cards=_kp_cards,
        org_id=org_id,
        prefix_main="fdl_apu",
        prefix_nf="fdl_apu_nf",
        csv_file_name="apuracao_fiscal_nf.csv",
        table_heading="### Tabela de NFs (visão fiscal)",
    )
