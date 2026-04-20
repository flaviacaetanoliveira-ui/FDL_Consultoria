"""Painel «Apuração Fiscal» — reutiliza funções de ``app_operacional`` (import tardio)."""

from __future__ import annotations

import html
import json
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd
import streamlit as st

from faturamento_dre_recorte import _BR_TZ
from faturamento_dre_recorte_minimo import (
    build_faturamento_fiscal_base_slice,
    build_nf_panel_aligned_to_fiscal_base,
    compute_nf_panel_kpis,
    dre_imposto_para_linha_dre_gerencial,
    enrich_faturamento_fiscal_base_stats,
    faturamento_min_series_nf_emissao_bounds_dates,
    faturamento_recorte_min_state_from_session,
    _min_cal_limits,
)
from processing.faturamento.fiscal_materializado import fiscal_contract_dataframe_valid
from processing.faturamento.nf_materializado import nf_first_contract_dataframe_valid
from processing.faturamento.nf_panel_materializado import nf_panel_materializado_dataframe_valid

_ALIQUOTA_DIVERG_PP = 0.5

FISCAL_KPIS_CSS = """<style>
.fdl-fat-kpi-hero-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 16px;
  margin-bottom: 16px;
}
@media (max-width: 900px) {
  .fdl-fat-kpi-hero-grid { grid-template-columns: 1fr; }
}
.fdl-fat-kpi-hero-card {
  background: var(--color-background-primary, #ffffff);
  border: 0.5px solid var(--color-border-tertiary, #e2e8f0);
  border-radius: 12px;
  padding: 1.25rem 1.5rem;
  position: relative;
}
.fdl-fat-kpi-hero-card--base::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  background: #0F6E56;
  border-top-left-radius: 12px;
  border-bottom-left-radius: 12px;
}
.fdl-fat-kpi-hero-card--imposto::before {
  content: "";
  position: absolute;
  left: 0;
  top: 0;
  bottom: 0;
  width: 4px;
  background: #888780;
  border-top-left-radius: 12px;
  border-bottom-left-radius: 12px;
}
.fdl-fat-kpi-hero-label {
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #64748b);
  margin-bottom: 6px;
}
.fdl-fat-kpi-hero-value {
  font-size: 28px;
  font-weight: 500;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 4px;
}
.fdl-fat-kpi-hero-caption {
  font-size: 12px;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-kpi-secondary-grid {
  display: grid;
  grid-template-columns: repeat(3, 1fr);
  gap: 12px;
}
@media (max-width: 900px) {
  .fdl-fat-kpi-secondary-grid { grid-template-columns: 1fr; }
}
.fdl-fat-kpi-secondary-card {
  background: var(--color-background-secondary, #f8fafc);
  border-radius: 8px;
  padding: 1rem;
}
.fdl-fat-kpi-secondary-label {
  font-size: 11px;
  font-weight: 500;
  letter-spacing: 0.04em;
  color: var(--color-text-tertiary, #64748b);
  margin-bottom: 4px;
}
.fdl-fat-kpi-secondary-value {
  font-size: 18px;
  font-weight: 500;
  color: var(--color-text-primary, #0f172a);
  margin-bottom: 2px;
}
.fdl-fat-kpi-secondary-caption {
  font-size: 11px;
  color: var(--color-text-secondary, #475569);
}
.fdl-fat-kpi-aliquota-divergencia {
  margin-top: 10px;
  padding: 8px 12px;
  background: #FAEEDA;
  border-radius: 8px;
  font-size: 12px;
  color: #854F0B;
}
</style>"""


def _fmt_pct_br(v: float) -> str:
    if v != v:  # NaN
        return "—"
    return f"{v:.1f}".replace(".", ",")


def _aliquota_configurada_pct_from_load_info(load_info: dict[str, object]) -> float:
    raw = load_info.get("faturamento_aliquota_imposto_pct")
    if isinstance(raw, (int, float)) and float(raw) > 0:
        x = float(raw)
        return x * 100.0 if x <= 1.0 else x
    path_final = load_info.get("faturamento_path_final_resolved")
    if not path_final:
        return 0.0
    try:
        meta_path = Path(str(path_final)).expanduser().resolve().parent / "metadata.json"
        if not meta_path.is_file():
            return 0.0
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        v = meta.get("aliquota_imposto_usada")
        if isinstance(v, (int, float)):
            return float(v) * 100.0
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return 0.0


def _diferenca_secondary_caption(valor_cancelado: float) -> str:
    base = "descontos/ajustes"
    if valor_cancelado > 1e-9:
        return f"{base} · cancelamentos fiscais"
    return base


def _build_fiscal_kpis_hero_html(
    *,
    base_liquida: float,
    imposto: float,
    aliquota_efetiva_pct: float,
    aliquota_configurada_pct: float,
    ok_nf_dates: bool,
    fmt_brl: Callable[[float], str],
) -> str:
    """
    Dois cartões hero: Base Tributável Líquida e Imposto Apurado (Hierarquia B, ~28px).
    """
    dash = "—"
    if ok_nf_dates:
        v_base = html.escape(fmt_brl(float(base_liquida)))
        v_imp = html.escape(fmt_brl(float(imposto)))
        cap_ef = f"alíquota efetiva: {_fmt_pct_br(aliquota_efetiva_pct)}%"
        cap_cfg = f"alíquota configurada: {_fmt_pct_br(aliquota_configurada_pct)}%"
    else:
        v_base = dash
        v_imp = dash
        cap_ef = "alíquota efetiva indisponível"
        cap_cfg = "alíquota configurada indisponível"

    alert_block = ""
    if (
        ok_nf_dates
        and aliquota_configurada_pct > 1e-9
        and abs(aliquota_efetiva_pct - aliquota_configurada_pct) > _ALIQUOTA_DIVERG_PP
    ):
        alert_block = (
            '<div class="fdl-fat-kpi-aliquota-divergencia">'
            "⚠ Alíquota efetiva ("
            f"{html.escape(_fmt_pct_br(aliquota_efetiva_pct))}%) diverge da configurada ("
            f"{html.escape(_fmt_pct_br(aliquota_configurada_pct))}%). Verificar composição."
            "</div>"
        )

    return (
        '<div class="fdl-fat-kpi-hero-grid">'
        '<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--base">'
        '<div class="fdl-fat-kpi-hero-label">BASE TRIBUTÁVEL LÍQUIDA</div>'
        f'<div class="fdl-fat-kpi-hero-value">{v_base}</div>'
        f'<div class="fdl-fat-kpi-hero-caption">{html.escape(cap_ef)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-hero-card fdl-fat-kpi-hero-card--imposto">'
        '<div class="fdl-fat-kpi-hero-label">IMPOSTO APURADO</div>'
        f'<div class="fdl-fat-kpi-hero-value">{v_imp}</div>'
        f'<div class="fdl-fat-kpi-hero-caption">{html.escape(cap_cfg)}</div>'
        "</div>"
        "</div>"
        f"{alert_block}"
    )


def _build_fiscal_kpis_secondary_html(
    *,
    valor_faturado_nf: float,
    n_nf: int,
    total_devolvido: float,
    nfs_devolucao: int,
    diferenca_lista_nf: float,
    valor_cancelado: float,
    ok_nf_dates: bool,
    fmt_brl: Callable[[float], str],
    fmt_int: Callable[[int], str],
) -> str:
    """Três cartões secundários (~18px): Valor Faturado (NF), Devoluções, Diferença (lista − NF)."""
    dash = "—"
    if ok_nf_dates:
        vf = html.escape(fmt_brl(float(valor_faturado_nf)))
        dv = html.escape(fmt_brl(float(total_devolvido)))
        df = html.escape(fmt_brl(float(diferenca_lista_nf)))
        cap_nf = f"{fmt_int(int(n_nf))} NFs emitidas"
        cap_dev = f"{fmt_int(int(nfs_devolucao))} NFs de entrada"
    else:
        vf = dv = df = dash
        cap_nf = cap_dev = "período indisponível"

    cap_dif = _diferenca_secondary_caption(float(valor_cancelado))

    return (
        '<div class="fdl-fat-kpi-secondary-grid">'
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">VALOR FATURADO (NF)</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{vf}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_nf)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">DEVOLUÇÕES DO PERÍODO</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{dv}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_dev)}</div>'
        "</div>"
        '<div class="fdl-fat-kpi-secondary-card">'
        '<div class="fdl-fat-kpi-secondary-label">DIFERENÇA (LISTA − NF)</div>'
        f'<div class="fdl-fat-kpi-secondary-value">{df}</div>'
        f'<div class="fdl-fat-kpi-secondary-caption">{html.escape(cap_dif)}</div>'
        "</div>"
        "</div>"
    )


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

    _imp_num = dre_imposto_para_linha_dre_gerencial(
        _kp_cards,
        fiscal_base_stats=_fiscal_base_stats if use_fiscal_parquet else None,
        aplicar_ponte_base_liquida=bool(use_fiscal_kpi),
    )
    _cfg_alq = _aliquota_configurada_pct_from_load_info(load_info)
    _stats_kpi = enrich_faturamento_fiscal_base_stats(
        _fiscal_base_stats,
        imposto_apurado=float(_imp_num),
        df_nf_aligned=df_nf_commercial_kpi,
        aliquota_configurada_pct=float(_cfg_alq),
    )

    ao._fdl_fat_min_vsp(size="sm")
    st.html(
        FISCAL_KPIS_CSS
        + _build_fiscal_kpis_hero_html(
            base_liquida=_stats_kpi.base_fiscal_liquida,
            imposto=float(_stats_kpi.imposto),
            aliquota_efetiva_pct=float(_stats_kpi.aliquota_efetiva_pct),
            aliquota_configurada_pct=float(_stats_kpi.aliquota_configurada_pct),
            ok_nf_dates=ok_nf_dates,
            fmt_brl=ao._fmt_brl_ptbr_celula,
        )
        + _build_fiscal_kpis_secondary_html(
            valor_faturado_nf=float(_stats_kpi.valor_faturado_nf),
            n_nf=int(_stats_kpi.n_nf),
            total_devolvido=float(_stats_kpi.total_devolvido),
            nfs_devolucao=int(_stats_kpi.nfs_devolucao),
            diferenca_lista_nf=float(_stats_kpi.diferenca_lista_nf),
            valor_cancelado=float(_stats_kpi.valor_cancelado),
            ok_nf_dates=ok_nf_dates,
            fmt_brl=ao._fmt_brl_ptbr_celula,
            fmt_int=ao._fmt_int_ptbr,
        )
    )

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
        nf_table_ui_mode="fiscal",
    )
