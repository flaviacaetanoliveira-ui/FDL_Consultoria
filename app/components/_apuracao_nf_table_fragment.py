    # Só chegamos aqui com painel materializado válido: recorte = filtrar linhas já agregadas (sem recomputar DRE).
    df_nf_lines = ao._faturamento_nf_apply_minimal_recorte(
        df_nf_pre,
        empresas_sel=_min_state.empresas,
        plataformas_sel=_min_state.plataformas,
        nf_d_ini=_nf_kpi_ini,
        nf_d_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
    )
    df_nf_lines = ao._faturamento_nf_filter_by_situacao(df_nf_lines, _min_state.situacoes_nf)
    df_nf_commercial = df_nf_lines.copy()
    if "plataforma_resumo" not in df_nf_commercial.columns:
        if "plataforma" in df_nf_commercial.columns:
            df_nf_commercial["plataforma_resumo"] = (
                df_nf_commercial["plataforma"].fillna("").astype(str)
            )
        else:
            df_nf_commercial["plataforma_resumo"] = "—"

    df_nf = df_nf_commercial.copy()
    if use_fiscal_kpi and _min_state.plataformas:
        df_nf = ao._nf_panel_filter_merged_fiscal_by_plataforma_resumo(
            df_nf, _min_state.plataformas
        )

    # df_nf = merge fiscal + comercial após empresa / emissão / plataforma; df_nf_panel = idem + produto / sinal.
    _prod_opts: list[str] = []
    if not df_nf.empty and "produto_resumo" in df_nf.columns:
        _prod_opts = sorted(
            {
                str(x).strip()
                for x in df_nf["produto_resumo"].dropna().unique()
                if str(x).strip() and str(x).strip() != "—"
            }
        )

    _k_sinais = "fdl_apu_sinais_resultado"
    if _k_sinais not in st.session_state:
        _leg = st.session_state.get("fdl_apu_sinal_resultado")
        _leg_vs = st.session_state.get("fdl_apu_venda_sinal")
        if isinstance(_leg, str):
            _s = _leg.strip().lower()
            if _s == "lucro":
                st.session_state[_k_sinais] = ["lucro"]
            elif _s == "prejuizo":
                st.session_state[_k_sinais] = ["prejuizo"]
            else:
                st.session_state[_k_sinais] = []
            st.session_state.pop("fdl_apu_sinal_resultado", None)
        elif isinstance(_leg_vs, str):
            _m = {"positiva": "lucro", "negativa": "prejuizo"}
            _one = _m.get(_leg_vs.strip().lower())
            st.session_state[_k_sinais] = (
                [_one] if _one else []
            )
        else:
            st.session_state[_k_sinais] = []
    _prev_s = st.session_state.get(_k_sinais)
    if not isinstance(_prev_s, list):
        st.session_state[_k_sinais] = []
    else:
        _filt = [x for x in _prev_s if x in ("lucro", "prejuizo", "empate")]
        st.session_state[_k_sinais] = _filt

    st.markdown(
        """
<style>
.tabela-nf-contador {
    color: #64748b;
    font-size: 0.85rem;
    margin: 8px 0 16px 0;
}
</style>
""",
        unsafe_allow_html=True,
    )

    _col_tit, _col_acoes = st.columns([3, 1])
    with _col_tit:
        st.markdown("### Tabela de NFs (visão fiscal)")
    with _col_acoes:
        _col_cfg, _col_csv = st.columns(2)
        with _col_cfg:
            with st.popover("⚙️"):
                st.caption(
                    "Por defeito mostram-se receita, deduções principais e resultado. Marque abaixo para acrescentar ao quadro e ao CSV."
                )
                st.checkbox(
                    "Diferença (lista − fiscal)",
                    value=False,
                    key="fdl_apu_nf_show_diferenca",
                    help="Receita de venda (lista) menos valor faturado na NF.",
                )
                st.markdown("**Mais colunas**")
                st.checkbox("Plataforma", key="fdl_apu_nf_opt_plat", value=False)
                st.checkbox("Situação da NF", key="fdl_apu_nf_opt_sit", value=False)
                st.checkbox("Pedido", key="fdl_apu_nf_opt_ped", value=False)
                st.checkbox("Linhas", key="fdl_apu_nf_opt_linhas", value=False)
                st.checkbox("Quantidade", key="fdl_apu_nf_opt_qtd", value=False)
                st.checkbox("Faturado (NF)", key="fdl_apu_nf_opt_vf", value=False)
                st.checkbox("Receita de Frete", key="fdl_apu_nf_opt_rf", value=False)
                st.checkbox("Repasse transp.", key="fdl_apu_nf_opt_rp", value=False)
                st.checkbox("Frete pedido (Σ)", key="fdl_apu_nf_opt_tar", value=False)
                st.checkbox("Despesa fixa", key="fdl_apu_nf_opt_df", value=False)
                if _nf_panel_ads_ui:
                    st.checkbox("ADS (3,5% + fixo)", key="fdl_apu_nf_opt_ads", value=False)
                st.checkbox("Alertas", key="fdl_apu_nf_opt_alert", value=False)
        with _col_csv:
            _nf_dl_hdr_slot = st.empty()

    _f1, _f2, _f3, _f4 = st.columns([1.5, 2, 1.5, 2])
    with _f1:
        st.multiselect(
            "Status",
            options=("lucro", "prejuizo", "empate"),
            format_func=lambda x: {
                "lucro": "Lucro",
                "prejuizo": "Prejuízo",
                "empate": "Neutro",
            }[x],
            key=_k_sinais,
            placeholder="Status…",
            label_visibility="collapsed",
            help=(
                "**Vazio** = todas as NFs. «Neutro» = resultado ~0. "
                "**Lucro** e **Prejuízo** juntos ⇒ sem filtro por sinal; caso contrário união das faixas escolhidas."
            ),
        )
    with _f2:
        if _prod_opts:
            ao._multiselect_stable(
                "fdl_apu_prod",
                "Produto",
                _prod_opts,
                compact_label=False,
                placeholder="Filtrar por produto…",
                label_visibility="collapsed",
                help=(
                    "Vazio = todos. Filtra pela coluna «Produtos» (resumo na NF). "
                    "Não altera o topo fiscal nem os cards/DRE."
                ),
            )
        else:
            st.caption("Sem produto no recorte — filtro indisponível.")
    with _f3:
        _ps_plat = (
            ao._faturamento_nf_platform_display_series(df_nf).fillna("").astype(str).str.strip()
            if not df_nf.empty
            else pd.Series(dtype=str)
        )
        _plat_opts = sorted({x for x in _ps_plat if x and x != "—"})
        if _plat_opts:
            if "fdl_apu_nf_tbl_plataforma" not in st.session_state:
                st.session_state["fdl_apu_nf_tbl_plataforma"] = []
            else:
                _po_prev = st.session_state["fdl_apu_nf_tbl_plataforma"]
                if isinstance(_po_prev, list):
                    st.session_state["fdl_apu_nf_tbl_plataforma"] = [
                        x for x in _po_prev if x in _plat_opts
                    ]
            st.multiselect(
                "Plataforma",
                options=_plat_opts,
                key="fdl_apu_nf_tbl_plataforma",
                placeholder="Plataforma…",
                label_visibility="collapsed",
                help="Vazio = todas as plataformas do recorte. Refina só a tabela.",
            )
        else:
            st.caption("Sem plataforma no recorte.")
    with _f4:
        st.text_input(
            "Buscar",
            key="fdl_apu_nf_tbl_busca",
            placeholder="🔍 Buscar NF ou Pedido…",
            label_visibility="collapsed",
        )

    _prod_sel = tuple(
        str(x).strip()
        for x in (st.session_state.get("fdl_apu_prod") or [])
        if str(x).strip()
    )
    _sinais_ui = st.session_state.get("fdl_apu_sinais_resultado")
    _sinais_tuple = (
        tuple(str(x).strip().lower() for x in _sinais_ui if str(x).strip())
        if isinstance(_sinais_ui, list)
        else ()
    )
    df_nf_panel = ao._faturamento_dre_apply_produto_e_sinal_venda(
        df_nf,
        produtos_sel=_prod_sel,
        sinais_resultado=_sinais_tuple,
    )
    # Cards e DRE: N_base fiscal + situação + enriquecimento; plataforma opcional; tabela = + produto/sinal.
    _kp_table = compute_nf_panel_kpis(df_nf_panel)
    _df_fiscal_kpi_anchor: pd.DataFrame | None = (
        _df_fiscal_base.copy() if use_fiscal_kpi and not _df_fiscal_base.empty else None
    )

    if ao._is_admin_mode():
        with st.expander("Diagnóstico materializado (admin)", expanded=False):
            ao._fdl_fat_min_aside(
                "<strong>Base fiscal</strong>: <code>dataset_faturamento_fiscal.parquet</code> — empresa + emissão + "
                "situação válida; filtro UI <strong>Situação da NF</strong> opcional. <strong>Painel NF</strong>: "
                "<code>dataset_faturamento_nf_panel.parquet</code> — <strong>cards/DRE</strong> = <strong>N_base</strong> "
                "+ situação + <strong>plataforma</strong> (se filtrada); <strong>tabela</strong> = + produto e sinal."
            )
            if len(df_nf_panel) != len(df_nf):
                _kp_pre_produto = compute_nf_panel_kpis(df_nf)
                ao._fdl_fat_min_aside(
                    "<strong>Conferência (tabela)</strong> — após plataforma vs após produto / resultado: "
                    f"venda lista <strong>{float(_kp_table['valor_venda']):.2f}</strong> vs "
                    f"<strong>{float(_kp_pre_produto['valor_venda']):.2f}</strong>; "
                    f"faturado NF <strong>{float(_kp_table['valor_faturado_nf']):.2f}</strong> vs "
                    f"<strong>{float(_kp_pre_produto['valor_faturado_nf']):.2f}</strong>; "
                    f"Σ resultado <strong>{float(_kp_table['resultado']):.2f}</strong> vs "
                    f"<strong>{float(_kp_pre_produto['resultado']):.2f}</strong>.",
                    tight=True,
                )
            if use_fiscal_parquet:
                ao._fdl_fat_min_aside(
                    "<strong>Plataforma</strong> **não** altera o **topo fiscal**; altera <strong>cards/DRE</strong> e "
                    "<strong>tabela</strong> (antes de produto/sinal).",
                    tight=True,
                )
            if load_info.get("faturamento_nf_panel_path"):
                _pp = html.escape(str(load_info.get("faturamento_nf_panel_path")))
                ao._fdl_fat_min_aside(f"Path painel NF: <code>{_pp}</code>", tight=True)
            if use_nf_materializado and load_info.get("faturamento_nf_first_path"):
                _p = html.escape(str(load_info.get("faturamento_nf_first_path")))
                ao._fdl_fat_min_aside(f"Path Parquet NF-first: <code>{_p}</code>", tight=True)
            elif load_info.get("faturamento_nf_first_skip") or load_info.get("faturamento_nf_first_error"):
                _sk = load_info.get("faturamento_nf_first_skip")
                _e = load_info.get("faturamento_nf_first_error")
                _parts = ["NF-first não ativo."]
                if _sk:
                    _parts.append(f"Motivo: <code>{html.escape(str(_sk))}</code>.")
                if _e:
                    _parts.append(f"Erro: <code>{html.escape(str(_e))}</code>.")
                ao._fdl_fat_min_aside(" ".join(_parts), tight=True)
            if load_info.get("faturamento_fiscal_path_resolution"):
                _pr = html.escape(str(load_info.get("faturamento_fiscal_path_resolution")))
                ao._fdl_fat_min_aside(f"Parquet fiscal resolvido via: <code>{_pr}</code>", tight=True)
            if use_fiscal_parquet and load_info.get("faturamento_fiscal_first_path"):
                _pf = html.escape(str(load_info.get("faturamento_fiscal_first_path")))
                ao._fdl_fat_min_aside(f"Path Parquet fiscal: <code>{_pf}</code>", tight=True)
            elif load_info.get("faturamento_fiscal_first_skip") or load_info.get("faturamento_fiscal_first_error"):
                _skf = load_info.get("faturamento_fiscal_first_skip")
                _ef = load_info.get("faturamento_fiscal_first_error")
                _parts_f = ["Parquet fiscal não ativo no carregamento (fallback ao faturado NF comercial)."]
                if _skf:
                    _parts_f.append(f"Motivo: <code>{html.escape(str(_skf))}</code>.")
                if _ef:
                    _parts_f.append(f"Erro: <code>{html.escape(str(_ef))}</code>.")
                ao._fdl_fat_min_aside(" ".join(_parts_f), tight=True)
            if use_nf_materializado and not use_fiscal_parquet:
                _fiscal_why: list[str] = []
                if load_info.get("faturamento_fiscal_user_hint"):
                    _fiscal_why.append(str(load_info["faturamento_fiscal_user_hint"]))
                elif load_info.get("faturamento_fiscal_first_error"):
                    _fiscal_why.append(f"Erro ao ler: {load_info['faturamento_fiscal_first_error']}")
                elif load_info.get("faturamento_fiscal_first_skip"):
                    _sk = str(load_info["faturamento_fiscal_first_skip"])
                    _fiscal_why.append(
                        "ficheiro ausente na pasta do materializado"
                        if _sk == "ficheiro_ausente"
                        else (
                            "materializado só por URL sem pasta local — não dá para ler o Parquet fiscal"
                            if _sk == "sem_path_local"
                            else _sk
                        )
                    )
                else:
                    _fiscal_why.append("Parquet fiscal não validado ou vazio após escopo")
                ao._fdl_fat_min_aside(
                    "<strong>Valor faturado (NF)</strong> neste ecrã está em <strong>NF-first (pedidos ligados)</strong>. "
                    "Para alinhar ao Bling, publique <code>dataset_faturamento_fiscal.parquet</code> junto do materializado. "
                    f"<strong>Estado fiscal:</strong> {' · '.join(html.escape(str(x)) for x in _fiscal_why)}"
                )
            if use_fiscal_kpi and _df_fiscal_kpi_anchor is not None:
                _aud_sum = float(
                    pd.to_numeric(_df_fiscal_kpi_anchor["Valor_Liquido_NF"], errors="coerce")
                    .fillna(0.0)
                    .sum()
                )
                _top_fiscal = float(_fiscal_base_stats.valor_liquido_fiscal_sum)
                _match = abs(_aud_sum - _top_fiscal) < 0.02
                _kp_vf_cards = float(_kp_cards["valor_faturado_nf"])
                _plat_empty = not _min_state.plataformas
                _vf_match_cards = abs(_kp_vf_cards - _top_fiscal) < 0.02 if _plat_empty else None
                _cards_vf_line = (
                    f"Σ <code>valor_faturado_nf</code> nos **cards/DRE** = <strong>{_kp_vf_cards:.2f}</strong> — "
                    f"com **Plataforma** vazia, deve coincidir com o topo: **{'sim' if _vf_match_cards else 'NÃO'}**."
                    if _plat_empty
                    else (
                        f"Σ <code>valor_faturado_nf</code> nos **cards/DRE** = <strong>{_kp_vf_cards:.2f}</strong> — com "
                        "**Plataforma** filtrada, **não** deve igualar o topo fiscal (subconjunto comercial por canal)."
                    )
                )
                ao._fdl_fat_min_aside(
                    "Auditoria fiscal (admin): "
                    f"<code>faturamento_fiscal_first</code>={load_info.get('faturamento_fiscal_first')!s}; "
                    f"Σ <code>Valor_Liquido_NF</code> no <strong>slice base</strong> (topo) = <strong>{_aud_sum:.2f}</strong> "
                    f"(deve coincidir com o card **Total faturado**): **{'sim' if _match else 'NÃO'}**. "
                    + _cards_vf_line,
                    tight=True,
                )
            if use_fiscal_parquet and isinstance(df_fiscal_pre, pd.DataFrame):
                ao._fdl_fat_min_aside(
                    f"Parquet fiscal (escopo org no carregamento): <strong>{len(df_fiscal_pre)}</strong> NF(s).",
                    tight=True,
                )

    _FAT_NF_TABLE_STYLER_MAX_ROWS = 500

    _show_col_diferenca = bool(st.session_state.get("fdl_apu_nf_show_diferenca", False))
    _nf_vis: list[str] = [
        "Emissão",
        "Status",
        "Empresa",
        "NF",
        "Produtos",
        "Receita de Venda",
        "Comissão",
        "Custo produto",
        "Frete plataforma",
        "Imposto",
        "Resultado",
        "Margem %",
    ]
    if _show_col_diferenca:
        _nf_vis.append("Diferença")
    _nf_opt_cols: list[tuple[str, str]] = [
        ("fdl_apu_nf_opt_plat", "Plataforma"),
        ("fdl_apu_nf_opt_sit", "Situação"),
        ("fdl_apu_nf_opt_ped", "Pedido"),
        ("fdl_apu_nf_opt_linhas", "Linhas"),
        ("fdl_apu_nf_opt_qtd", "Quantidade"),
        ("fdl_apu_nf_opt_vf", "Faturado (NF)"),
        ("fdl_apu_nf_opt_rf", "Receita de Frete"),
        ("fdl_apu_nf_opt_rp", "Repasse transp."),
        ("fdl_apu_nf_opt_tar", "Frete pedido (Σ)"),
        ("fdl_apu_nf_opt_df", "Despesa fixa"),
    ]
    for _ok, _colname in _nf_opt_cols:
        if bool(st.session_state.get(_ok, False)):
            _nf_vis.append(_colname)
    if _nf_panel_ads_ui and bool(st.session_state.get("fdl_apu_nf_opt_ads", False)):
        _nf_vis.extend(["ADS 3,5%", "ADS fixo"])
    if bool(st.session_state.get("fdl_apu_nf_opt_alert", False)):
        _nf_vis.append("Alertas")

    _nf_table_cols_order_ui: list[str] = []
    for _c in _nf_vis:
        if _c in ("ADS 3,5%", "ADS fixo") and not _nf_panel_ads_ui:
            continue
        _nf_table_cols_order_ui.append(_c)

    _df_nf_table = df_nf_panel
    if not df_nf_panel.empty and "Nota_Data_Emissao" in df_nf_panel.columns:
        _tmp_sort = df_nf_panel.copy()
        _tmp_sort["_fdl_nf_emi_ord"] = pd.to_datetime(
            ao._df_get_series_column(_tmp_sort, "Nota_Data_Emissao"),
            errors="coerce",
            dayfirst=False,
        )
        _df_nf_table = (
            _tmp_sort.sort_values("_fdl_nf_emi_ord", ascending=False, na_position="last")
            .drop(columns=["_fdl_nf_emi_ord"], errors="ignore")
            .reset_index(drop=True)
        )

    _disp_nf_full = pd.DataFrame()
    _disp_nf_ui = pd.DataFrame()
    if not _df_nf_table.empty:
        _plat_s = ao._faturamento_nf_platform_display_series(_df_nf_table).astype(str)
        _marg_ratio = ao._nf_row_margem_resultado_venda_ratio(
            _df_nf_table["valor_venda"],
            _df_nf_table["resultado"],
        )
        _custo_s = (
            _df_nf_table["custo_produto"]
            if "custo_produto" in _df_nf_table.columns
            else pd.Series(0.0, index=_df_nf_table.index, dtype=float)
        )
        _inc_flag = (
            _df_nf_table["comercial_incompleto"].fillna(False).astype(bool)
            if "comercial_incompleto" in _df_nf_table.columns
            else pd.Series(False, index=_df_nf_table.index, dtype=bool)
        )
        _ads_v_s = (
            pd.to_numeric(_df_nf_table["custo_ads_variavel"], errors="coerce").fillna(0.0)
            if "custo_ads_variavel" in _df_nf_table.columns
            else pd.Series(0.0, index=_df_nf_table.index, dtype=float)
        )
        _ads_f_s = (
            pd.to_numeric(_df_nf_table["custo_ads_fixo"], errors="coerce").fillna(0.0)
            if "custo_ads_fixo" in _df_nf_table.columns
            else pd.Series(0.0, index=_df_nf_table.index, dtype=float)
        )
        _res_line_nf = pd.to_numeric(_df_nf_table["resultado"], errors="coerce")
        _vv_num = pd.to_numeric(_df_nf_table["valor_venda"], errors="coerce").fillna(0.0)
        _cm_num = pd.to_numeric(_df_nf_table["comissao"], errors="coerce").fillna(0.0)
        _imp_num = pd.to_numeric(_df_nf_table["imposto"], errors="coerce").fillna(0.0)
        _cp_num = pd.to_numeric(_custo_s, errors="coerce").fillna(0.0)
        _eps_z = 1e-6
        _sem_mov = (
            (_vv_num.abs() <= _eps_z)
            & (_cp_num.abs() <= _eps_z)
            & (_res_line_nf.fillna(0.0).abs() <= _eps_z)
            & (_cm_num.abs() <= _eps_z)
            & (_imp_num.abs() <= _eps_z)
        )
        if ao._is_admin_mode() and bool(_sem_mov.any()):
            st.caption(f"Admin: {ao._fmt_int_ptbr(int(_sem_mov.sum()))} NF(s) só com zeros nos principais valores comerciais.")

        def _nf_alert_txt(i: int) -> str:
            _parts: list[str] = []
            if bool(_sem_mov.iloc[i]):
                _parts.append("NF sem movimento comercial")
            if bool(_inc_flag.iloc[i]):
                _parts.append("Falta custo / dados")
            return " · ".join(_parts) if _parts else "—"

        _alertas_col = pd.Series(
            [_nf_alert_txt(i) for i in range(len(_df_nf_table))],
            index=_df_nf_table.index,
            dtype=object,
        )
        _qtd_itens = ao._faturamento_nf_quantidade_itens_por_nf(df, _df_nf_table)

        def _nf_status_label_nf(r: object) -> str:
            try:
                x = float(r)
            except (TypeError, ValueError):
                return "—"
            if pd.isna(x):
                return "—"
            if x > 0:
                return "Lucro"
            if x < 0:
                return "Prejuízo"
            return "Neutro"

        _disp_nf_full = pd.DataFrame(
            {
                "Emissão": ao._series_nf_emissao_pt_br(
                    ao._df_get_series_column(_df_nf_table, "Nota_Data_Emissao")
                ),
                "Status": _res_line_nf.map(_nf_status_label_nf),
                "Empresa": ao._series_empty_str_to_dash(ao._df_get_series_column(_df_nf_table, "empresa")),
                "Plataforma": _plat_s,
                "NF": ao._df_get_series_column(_df_nf_table, "Nota_Numero_Normalizado")
                .fillna("")
                .map(lambda v: str(v).strip()),
                "Situação": ao._series_empty_str_to_dash(
                    ao._df_get_series_column(_df_nf_table, "Nota_Situacao")
                ),
                "Pedido": ao._faturamento_disp_texto_sem_none(_df_nf_table["pedido_resumo"]),
                "Produtos": ao._faturamento_disp_texto_sem_none(_df_nf_table["produto_resumo"]),
                "Linhas": _df_nf_table["n_linhas_pedido"].astype(int),
                "Quantidade": _qtd_itens,
                "Receita de Venda": pd.to_numeric(_df_nf_table["valor_venda"], errors="coerce"),
                "Faturado (NF)": pd.to_numeric(_df_nf_table["valor_faturado_nf"], errors="coerce"),
                **(
                    {"Diferença": pd.to_numeric(_df_nf_table["diferenca"], errors="coerce")}
                    if _show_col_diferenca
                    else {}
                ),
                "Comissão": pd.to_numeric(_df_nf_table["comissao"], errors="coerce"),
                "Custo produto": pd.to_numeric(_custo_s, errors="coerce").fillna(0.0),
                "Receita de Frete": pd.to_numeric(
                    _df_nf_table["receita_frete_tp"], errors="coerce"
                )
                if "receita_frete_tp" in _df_nf_table.columns
                else pd.Series(0.0, index=_df_nf_table.index),
                "Frete plataforma": pd.to_numeric(
                    _df_nf_table["custo_frete_plataforma"], errors="coerce"
                )
                if "custo_frete_plataforma" in _df_nf_table.columns
                else pd.Series(0.0, index=_df_nf_table.index),
                "Repasse transp.": pd.to_numeric(
                    _df_nf_table["repasse_frete_transportadora_propria"], errors="coerce"
                )
                if "repasse_frete_transportadora_propria" in _df_nf_table.columns
                else pd.Series(0.0, index=_df_nf_table.index),
                "Frete pedido (Σ)": pd.to_numeric(
                    _df_nf_table["tarifa_custo_envio"], errors="coerce"
                )
                if "tarifa_custo_envio" in _df_nf_table.columns
                else pd.Series(0.0, index=_df_nf_table.index),
                "Imposto": pd.to_numeric(_df_nf_table["imposto"], errors="coerce"),
                "Despesa fixa": pd.to_numeric(_df_nf_table["despesa_fixa"], errors="coerce"),
                "ADS 3,5%": _ads_v_s,
                "ADS fixo": _ads_f_s,
                "Resultado": _res_line_nf,
                "Alertas": _alertas_col.astype(str),
                "Margem %": (_marg_ratio * 100.0),
            }
        )
        _disp_nf_full = _disp_nf_full[_nf_table_cols_order_ui]
        _disp_nf_ui = _disp_nf_full.copy()

        def _fat_min_trunc_text_cell(v: object, max_len: int = 72) -> str:
            t = str(v).strip()
            if t in ("", "—", "nan") or len(t) <= max_len:
                return t if t else "—"
            return t[: max_len - 1] + "…"

        def _nf_tbl_money_str(x: object) -> str:
            if x is None:
                return "—"
            try:
                if pd.isna(x):
                    return "—"
            except TypeError:
                pass
            s = ao._fmt_brl_ptbr_celula(x)
            return s if s else "—"

        def _nf_tbl_linhas_str(x: object) -> str:
            try:
                if pd.isna(x):
                    return "—"
            except TypeError:
                pass
            try:
                n = int(round(float(x)))
            except (TypeError, ValueError):
                return "—"
            return ao._fmt_int_ptbr(n)

        def _nf_tbl_margem_str(ratio_times_100: object) -> str:
            """``Margem %`` no export numérico = ratio×100; reconstrói ratio para o mesmo formato do painel."""
            try:
                if pd.isna(ratio_times_100):
                    return "—"
            except TypeError:
                return "—"
            try:
                pct = float(ratio_times_100)
            except (TypeError, ValueError):
                return "—"
            if math.isnan(pct) or math.isinf(pct):
                return "—"
            return ao._fmt_pct_ptbr_ratio(pct / 100.0, decimals=1)

        for _money_col in (
            "Receita de Venda",
            "Faturado (NF)",
            "Diferença",
            "Comissão",
            "Custo produto",
            "Receita de Frete",
            "Frete plataforma",
            "Repasse transp.",
            "Frete pedido (Σ)",
            "Imposto",
            "Despesa fixa",
            "ADS 3,5%",
            "ADS fixo",
            "Resultado",
        ):
            if _money_col in _disp_nf_ui.columns:
                _disp_nf_ui[_money_col] = _disp_nf_ui[_money_col].map(_nf_tbl_money_str)
        if "Linhas" in _disp_nf_ui.columns:
            _disp_nf_ui["Linhas"] = _disp_nf_ui["Linhas"].map(_nf_tbl_linhas_str)
        if "Quantidade" in _disp_nf_ui.columns:
            _disp_nf_ui["Quantidade"] = _disp_nf_ui["Quantidade"].map(_nf_tbl_linhas_str)
        if "Margem %" in _disp_nf_ui.columns:
            _disp_nf_ui["Margem %"] = _disp_nf_full["Margem %"].map(_nf_tbl_margem_str)
        if "Pedido" in _disp_nf_ui.columns:
            _disp_nf_ui["Pedido"] = _disp_nf_ui["Pedido"].map(
                lambda x: _fat_min_trunc_text_cell(x, 72)
            )
        if "Produtos" in _disp_nf_ui.columns:
            _disp_nf_ui["Produtos"] = _disp_nf_ui["Produtos"].map(
                lambda x: _fat_min_trunc_text_cell(x, 72)
            )

        _plat_filt = st.session_state.get("fdl_apu_nf_tbl_plataforma") or []
        if not isinstance(_plat_filt, list):
            _plat_filt = []
        _ps_pf = (
            ao._faturamento_nf_platform_display_series(df_nf).fillna("").astype(str).str.strip()
            if not df_nf.empty
            else pd.Series(dtype=str)
        )
        _plat_avail_nf = {x for x in _ps_pf if x and x != "—"}
        _plat_filt = [x for x in _plat_filt if x in _plat_avail_nf]
        _busca_filt = str(st.session_state.get("fdl_apu_nf_tbl_busca") or "")
        _nf_tbl_n_antes_extra = len(_disp_nf_full)
        _nf_tbl_mask = nf_table_filter_mask(
            _disp_nf_full,
            plataformas_sel=_plat_filt,
            busca=_busca_filt,
        )
        _disp_nf_full = _disp_nf_full.loc[_nf_tbl_mask].reset_index(drop=True)
        _disp_nf_ui = _disp_nf_ui.loc[_nf_tbl_mask].reset_index(drop=True)
    else:
        _nf_tbl_n_antes_extra = 0

    _cfg_nf: dict[str, NumberColumn | TextColumn] = {}
    _nf_col_help: dict[str, str | None] = {
        "Emissão": None,
        "Status": "Lucro, prejuízo ou neutro (resultado ~0) conforme o resultado consolidado da NF.",
        "Empresa": None,
        "Plataforma": "«—» = NF sem canal comercial associado neste recorte.",
        "NF": None,
        "Situação": None,
        "Pedido": "«—» = sem pedido comercial resolvido para esta NF. Texto completo no CSV.",
        "Produtos": "«—» = sem produto agregado na NF. Texto completo no CSV.",
        "Linhas": "Quantidade de linhas de pedido agregadas nesta NF (comercial).",
        "Quantidade": "Soma de **Quantidade** (unidades) nas linhas de pedido ligadas a esta NF (materializado linha).",
        "Receita de Venda": (
            "Comercial: Σ Quantidade × Preço de lista dos pedidos ligados a esta NF (0 se não houver vínculo)."
            if use_fiscal_kpi
            else "Σ Quantidade × Preço de lista (pedidos ligados à NF)."
        ),
        "Faturado (NF)": (
            "Valor líquido da NF na referência fiscal (Bling / export), 1× por nota no período."
            if use_fiscal_kpi
            else "Valor líquido da NF (uma vez por nota) na base materializada."
        ),
        "Diferença": (
            "Comercial − fiscal nesta linha: Receita de Venda − Faturado (NF). "
            "Interpretar como ponte lista↔nota, não como erro automático."
            if use_fiscal_kpi
            else "Receita de Venda − Faturado (NF)."
        ),
        "Comissão": "Comercial: soma das comissões das linhas de pedido ligadas à NF." if use_fiscal_kpi else None,
        "Custo produto": (
            "Comercial: Σ **Custo_Produto_Total** (ou «Custo do Produto») das linhas de pedido ligadas à NF."
            if use_fiscal_kpi
            else "Σ custo do produto nas linhas de pedido desta NF."
        ),
        "Receita de Frete": (
            "Frete destacado na **nota fiscal** (``Frete_Nota_Export`` no merge fiscal), por NF."
            if use_fiscal_kpi
            else "Receita de frete na NF / gap comercial quando sem fiscal."
        ),
        "Frete plataforma": (
            "Custo de logística da **plataforma** (ME / «Frete_Plataforma»), após separar do repasse TP quando aplicável."
            if use_fiscal_kpi
            else "Parcela plataforma do frete no pedido."
        ),
        "Repasse transp.": (
            "Repasse à **transportadora própria** (parcela TP do «Custo de Frete»); se o pedido não separa modalidade "
            "mas a NF cobra frete, imputa-se pass-through até o teto da tarifa da NF."
            if use_fiscal_kpi
            else "Repasse TP / imputação alinhada à receita NF."
        ),
        "Frete pedido (Σ)": (
            "Σ **Custo de Frete** (ou «Frete_Plataforma») no pedido — conferência (plataforma + repasse após coerência)."
            if use_fiscal_kpi
            else "Σ frete no pedido."
        ),
        "Imposto": "Comercial: soma do imposto das linhas de pedido ligadas à NF." if use_fiscal_kpi else None,
        "Despesa fixa": (
            "Comercial: 5% sobre valor da venda (lista) agregado à NF."
            if use_fiscal_kpi
            else "5% sobre valor da venda agregado à NF."
        ),
        "ADS 3,5%": "3,5% × receita de venda (lista) nesta NF — custo de mídia (materializado).",
        "ADS fixo": "R$ 2,00 quando a receita de venda (lista) > 0 nesta NF (materializado).",
        "Resultado": (
            "Comercial: resultado consolidado por NF **já líquido de ADS** (materializado)."
            if use_fiscal_kpi
            else "Resultado consolidado por NF **já líquido de ADS** (materializado)."
        ),
        "Margem %": (
            "Comercial: Resultado ÷ Receita de Venda nesta NF; não usa valor faturado fiscal."
            if use_fiscal_kpi
            else "Resultado ÷ Receita de Venda; alinhado ao KPI «Margem %» do painel."
        ),
        "Alertas": (
            "«NF sem movimento comercial» quando receita, custo, comissão, imposto e resultado são ~0. "
            "«Falta custo / dados» quando o materializado sinaliza dados incompletos."
        ),
    }
    _nf_col_width: dict[str, str] = {
        "Emissão": "small",
        "Status": "small",
        "Empresa": "medium",
        "Plataforma": "small",
        "NF": "small",
        "Situação": "small",
        "Pedido": "large",
        "Produtos": "large",
        "Linhas": "small",
        "Quantidade": "small",
        "Receita de Venda": "medium",
        "Faturado (NF)": "medium",
        "Diferença": "small",
        "Comissão": "small",
        "Custo produto": "medium",
        "Receita de Frete": "small",
        "Frete plataforma": "small",
        "Repasse transp.": "small",
        "Frete pedido (Σ)": "small",
        "Imposto": "small",
        "Despesa fixa": "small",
        "ADS 3,5%": "small",
        "ADS fixo": "small",
        "Resultado": "medium",
        "Alertas": "medium",
        "Margem %": "small",
    }
    for _cn in _nf_table_cols_order_ui:
        if _cn not in _disp_nf_ui.columns:
            continue
        _w = _nf_col_width.get(_cn, "medium")
        _h = _nf_col_help.get(_cn)
        _cfg_nf[_cn] = (
            TextColumn(_cn, width=_w, help=_h) if _h else TextColumn(_cn, width=_w)
        )

    _nf_dl_n = len(_disp_nf_ui)
    _nf_dl_scope = _nf_tbl_n_antes_extra if _nf_tbl_n_antes_extra else _nf_dl_n
    _nf_dl_hdr_slot.download_button(
        "📥 CSV",
        _disp_nf_full.to_csv(index=False).encode("utf-8-sig") if not _disp_nf_full.empty else b"",
        file_name="apuracao_fiscal_nf.csv",
        mime="text/csv",
        key=f"fdl_apu_dl_hdr_{_oid}",
        disabled=_disp_nf_full.empty,
    )

    with st.container(border=True):
        _nf_cap_txt = ""
        if _nf_dl_scope and _nf_dl_n == _nf_dl_scope:
            _nf_cap_txt = (
                f"{_nf_dl_scope:,} notas · emissão em ordem decrescente · CSV alinhado às colunas visíveis."
                if use_fiscal_kpi
                else f"{_nf_dl_scope:,} notas · emissão decrescente · CSV alinhado às colunas visíveis."
            )
        elif _nf_dl_scope:
            _nf_cap_txt = (
                f"Mostrando {_nf_dl_n:,} de {_nf_dl_scope:,} notas · emissão decrescente."
            )
        else:
            _nf_cap_txt = "Sem linhas para exibir com os filtros atuais."
        st.markdown(f"<p class='tabela-nf-contador'>{html.escape(_nf_cap_txt)}</p>", unsafe_allow_html=True)
        if _disp_nf_ui.empty:
            st.info(
                (
                    "Sem linhas na **tabela** com os filtros atuais (status, produto, plataforma, busca). "
                    "O **topo fiscal** e os **cards/DRE** podem ainda ter **N_base** > 0 — confira período e empresa."
                )
                if use_fiscal_kpi
                else (
                    "Sem linhas no recorte (confirme **período de emissão**, **empresa** e **plataforma**)."
                )
            )
        else:
            _nf_page_sz = 25
            _nf_total_rows = len(_disp_nf_ui)
            _nf_pages = max(1, (_nf_total_rows + _nf_page_sz - 1) // _nf_page_sz)
            _pg_sel = 1
            if _nf_pages > 1:
                _pg_a, _pg_b = st.columns((1, 2))
                with _pg_a:
                    _pg_sel = int(
                        st.number_input(
                            "Página",
                            min_value=1,
                            max_value=int(_nf_pages),
                            value=1,
                            step=1,
                            key="fdl_apu_nf_pg",
                        )
                    )
                with _pg_b:
                    _i0p = (int(_pg_sel) - 1) * _nf_page_sz
                    _i1p = min(_i0p + _nf_page_sz, _nf_total_rows)
                    st.caption(
                        f"Mostrando **{_i0p + 1}**–**{_i1p}** de **{_nf_total_rows}** notas. "
                        "Use o cabeçalho da tabela para ordenar a página atual."
                    )
            else:
                st.caption(f"**{_nf_total_rows}** nota(s) no recorte (ordenar pelo cabeçalho da coluna).")
            _i0 = (int(_pg_sel) - 1) * _nf_page_sz
            _slice_ui = _disp_nf_ui.iloc[_i0 : _i0 + _nf_page_sz].copy()
            _slice_num = _disp_nf_full.iloc[_i0 : _i0 + _nf_page_sz].reset_index(drop=True)
            _slice_ui_r = _slice_ui.reset_index(drop=True)

            def _nf_style_status_col(s: pd.Series) -> list[str]:
                out: list[str] = []
                for v in s.astype(object):
                    vs = str(v).strip()
                    if vs == "Lucro":
                        c = ao.CORES_STATUS["Lucro"]
                        out.append(
                            f"background-color: #dcfce7; color: {c}; font-weight: 500; font-size: 0.75rem"
                        )
                    elif vs == "Prejuízo":
                        c = ao.CORES_STATUS["Prejuízo"]
                        out.append(
                            f"background-color: #fee2e2; color: {c}; font-weight: 500; font-size: 0.75rem"
                        )
                    elif vs == "Neutro":
                        c = ao.CORES_STATUS["Neutro"]
                        out.append(
                            f"background-color: #f3f4f6; color: {c}; font-weight: 500; font-size: 0.75rem"
                        )
                    else:
                        out.append("")
                return out

            def _nf_row_highlight_fat(r: pd.Series) -> list[str]:
                ri = r.name
                try:
                    res = float(pd.to_numeric(_slice_num.loc[ri, "Resultado"], errors="coerce"))
                except Exception:
                    res = 0.0
                c = "background-color: #fef2f2" if res < 0 else ""
                return [c] * len(r)

            _h_tbl = min(440, 132 + 34 * min(len(_slice_ui_r), 14))
            _df_arg: object = _slice_ui_r
            _nf_styler_ok = (not ao._fdl_safe_mode()) and int(_slice_ui_r.size) < 200_000
            if _nf_styler_ok:
                try:
                    _st_obj = _slice_ui_r.style.apply(_nf_style_status_col, subset=["Status"], axis=0)
                    if _nf_total_rows <= _FAT_NF_TABLE_STYLER_MAX_ROWS:
                        _st_obj = _st_obj.apply(_nf_row_highlight_fat, axis=1)
                    _df_arg = _st_obj
                except Exception:
                    _df_arg = _slice_ui_r
            if _nf_total_rows > _FAT_NF_TABLE_STYLER_MAX_ROWS and _nf_styler_ok:
                st.caption("Destaque de linha para prejuízo ativo até **500** notas.")
            st.dataframe(
                _df_arg,
                use_container_width=True,
                hide_index=True,
                height=_h_tbl,
                column_config=_cfg_nf,
            )

    ao._fdl_fat_min_vsp(size="md")