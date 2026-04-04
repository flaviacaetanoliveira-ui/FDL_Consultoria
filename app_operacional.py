from __future__ import annotations

import html
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from io import BytesIO
import base64
import json
import hashlib
import math
import numbers
import os
from pathlib import Path
import shutil
import time
import unicodedata
from typing import Any, Callable
from textwrap import dedent
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse, urljoin
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st
from streamlit.column_config import DatetimeColumn, NumberColumn, SelectboxColumn, TextColumn
from openpyxl.styles import numbers as oxl_number_formats
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

import comercial_pedidos_analise as cpa

from carregamento_bases import PIPELINE_DATA_REVISION
from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional
from faturamento_dre_recorte import (
    _fdl_fr_mask_nf_emissao_no_periodo,
    apply_recorte_modulo,
    faturamento_recorte_state_from_session,
)
from processing.faturamento.nf_materializado import nf_first_contract_dataframe_valid
from faturamento_dre_recorte_minimo import (
    _min_cal_limits,
    build_nf_grain_dataframe,
    compute_nf_panel_kpis,
    faturamento_min_series_nf_emissao_bounds_dates,
    faturamento_recorte_min_state_from_session,
)
from fdl_paths import resolve_pasta_vendas_ml
from operacional_app_context import (
    SESSION_ACTIVE_ORG_KEY,
    get_active_organization,
    logout_operacional_user,
    nomes_permitidos_com_registro,
    organizacao_por_nome_cadastrado,
    require_app_user,
)

# MVP Faturamento & DRE: área de produto separada de Financeiro (Repasse/Frete).
SESSION_FDL_PRODUCT_AREA_KEY = "fdl_product_area"
FDL_PRODUCT_AREA_FINANCEIRO = "financeiro"
FDL_PRODUCT_AREA_FATURAMENTO_DRE = "faturamento_dre"
FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS = "comercial_pedidos"

# Filtros globais / escopo de carga (MVP)
FAT_DRE_ESCOPO_EMPRESA = "empresa_ativa"
FAT_DRE_ESCOPO_CONSOLIDADO = "consolidado"
# Carga Faturamento & DRE: consolidado não usa org da sidebar; chave de cache estável.
FAT_DRE_CACHE_ACTIVE_ORG_PLACEHOLDER = "_fdl_fat_dre_consolidado_load"

# Select "Visão" no painel Faturamento: rótulo explícito vs. legado em sessão.
_FAT_PAINEL_VISAO_COM_NF = "Com NF (pedido)"
_FAT_PAINEL_VISAO_COM_NF_LEGACY = "Com NF"

from operacional_data_config import DATASET_EMPRESA
from operacional_frete import (
    FRETE_ML_COL,
    FRETE_UI_ANUNCIO,
    FRETE_UI_ANALISADO_COBRADO_MAIOR,
    FRETE_UI_ANALISADO_COBRADO_MENOR,
    FRETE_UI_ANALISADO_REPASSE_FRETE,
    FRETE_UI_CLASSIFICACAO,
    FRETE_UI_DIFERENCA,
    FRETE_UI_N_VENDA,
    FRETE_UI_RECEBIDO,
    FRETE_UI_SITUACAO_FRETE,
    FRETE_UI_STATUS_CONC,
    FRETE_UI_VAL_DIVERGENCIA,
    FRETE_SITUACAO_FRETE_VALORES_FILTRO,
    FRETE_VAL_RECEBIDO_NAO,
    FRETE_VAL_RECEBIDO_SIM,
    FontesFrete,
    carregar_tabela_final_frete_operacional,
    compute_frete_situacao_frete_column,
    dataframe_frete_conciliacao_principal,
    descobrir_fontes_frete,
    frete_vendas_loader_args,
    frete_kpis_executivos,
    frete_series_for_date_filter,
    frete_series_normalize_sale_dt,
    frete_tabela_anuncios_cobrado_maior,
    frete_tabela_anuncios_repasse_frete,
    normalize_frete_status_conc_display,
    stable_mtime_ns_for_frete_url,
    validate_frete_operacional_dataframe,
)
from operacional_frete_ui import (
    _column_config_frete,
    _dataframe_frete_grid,
    _frete_conciliacao_grid_com_icones,
)

_REPO_APP_ROOT = Path(__file__).resolve().parent
BUILD_TAG = "build-20260329-repasse-ui-saas"


def _sidebar_version_display() -> str:
    """Rótulo curto para a sidebar (ex.: v20260329)."""
    for tok in BUILD_TAG.replace("build-", "").split("-"):
        if len(tok) == 8 and tok.isdigit():
            return f"v{tok}"
    return "v—"



try:
    st.set_page_config(page_title="FDL Analytics — Financeiro", layout="wide")
except Exception:
    pass  # já definido por app.py no primeiro arranque


def _fdl_global_trace(msg: str) -> None:
    """Marca etapa do rerun (FDL_DEBUG_BOOTSTRAP). Deve existir antes do restante do módulo."""
    try:
        st.session_state["_fdl_bootstrap_stage"] = msg
        log = st.session_state.setdefault("_fdl_bootstrap_log", [])
        if not isinstance(log, list):
            log = []
            st.session_state["_fdl_bootstrap_log"] = log
        log.append(msg)
        if len(log) > 48:
            del log[:24]
    except Exception:
        pass


def _materialized_path_mode() -> str:
    """
    fixed — FDL_REPASSE_MATERIALIZED_PATH / frete / faturamento nos secrets (legado).
    dynamic — repasse/frete/faturamento derivados de data_products/<cliente>/<org_id>/... pela org ativa.
    """
    raw = os.environ.get("FDL_MATERIALIZED_PATH_MODE", "").strip().lower()
    if raw in {"dynamic", "fixed"}:
        return raw
    try:
        s = str(st.secrets.get("FDL_MATERIALIZED_PATH_MODE", "")).strip().lower()
        if s in {"dynamic", "fixed"}:
            return s
    except Exception:
        pass
    return "fixed"


def _materialized_cliente_slug() -> str:
    """Segmento de pasta do cliente (ex.: cliente_2), alinhado a materialize_financeiro --cliente."""
    raw = os.environ.get("FDL_MATERIALIZED_CLIENTE_SLUG", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_MATERIALIZED_CLIENTE_SLUG", "")).strip()
    except Exception:
        return ""


def _materialized_data_products_root() -> str:
    raw = os.environ.get("FDL_DATA_PRODUCTS_ROOT", "").strip()
    if raw:
        return raw
    try:
        v = str(st.secrets.get("FDL_DATA_PRODUCTS_ROOT", "data_products")).strip()
        return v or "data_products"
    except Exception:
        return "data_products"


def _dynamic_materialized_repasse_rel_path(org_id: str) -> str:
    cliente = _materialized_cliente_slug()
    if not cliente or not (org_id or "").strip():
        return ""
    root = _materialized_data_products_root().strip().strip("/\\")
    oid = org_id.strip()
    return f"{root}/{cliente}/{oid}/repasse/current/dataset_repasse_app.csv"


def _dynamic_materialized_frete_rel_path(org_id: str) -> str:
    """Mesmo layout que materialize_financeiro; não depender do CSV de repasse existir para derivar o path."""
    cliente = _materialized_cliente_slug()
    if not cliente or not (org_id or "").strip():
        return ""
    root = _materialized_data_products_root().strip().strip("/\\")
    oid = org_id.strip()
    return f"{root}/{cliente}/{oid}/frete/current/dataset_frete_app.csv"


_fdl_global_trace("01: início app_operacional (módulo reexecutado)")
_app_ctx = require_app_user()
_fdl_global_trace("02: após autenticação (require_app_user)")
_active_org = get_active_organization(_app_ctx)


def _dataset_empresa_label() -> str:
    """Rótulo da coluna `empresa` quando o dataset não a traz; em dynamic alinha à org ativa."""
    if _materialized_path_mode() == "dynamic":
        return _active_org.display_name
    return DATASET_EMPRESA


def _enabled_finance_modules() -> set[str]:
    """
    Módulos visíveis na sidebar (default: repasse, frete, faturamento).
    ``faturamento`` controla o módulo **Faturamento & DRE** (fora do grupo Financeiro).
    Ex.: FDL_ENABLED_FINANCE_MODULES=repasse,frete para clientes sem faturamento.
    """
    raw = os.environ.get("FDL_ENABLED_FINANCE_MODULES", "").strip()
    if not raw:
        try:
            raw = str(st.secrets.get("FDL_ENABLED_FINANCE_MODULES", "")).strip()
        except Exception:
            raw = ""
    if not raw:
        return {"repasse", "frete", "faturamento"}
    out = {x.strip().lower() for x in raw.split(",") if x.strip()}
    valid = {"repasse", "frete", "faturamento"}
    return out & valid or {"repasse", "frete", "faturamento"}


def _repasse_sem_bling() -> bool:
    """Cliente sem Bling: ações operacionais usam «Baixado» em vez de «Baixar no Bling»."""
    raw = os.environ.get("FDL_REPASSE_SEM_BLING", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_REPASSE_SEM_BLING", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _repasse_vendas_liberacoes_only() -> bool:
    """
    Cenário específico (ex.: Thiago): repasse sem notas/contas, só vendas x liberações.
    """
    raw = os.environ.get("FDL_REPASSE_VENDAS_LIBERACOES_ONLY", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_REPASSE_VENDAS_LIBERACOES_ONLY", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _filtrar_df_col_empresa_por_contexto(df: pd.DataFrame) -> pd.DataFrame:
    """
    Em modo fixed/live, restringe às empresas permitidas ou, em cenários multi-empresa no mesmo ficheiro,
    alinha ao contexto.

    Em **dynamic**, cada artefacto já vive em ``data_products/<cliente>/<org_id>/...`` — não filtrar pela
    coluna ``empresa``: muitas materializações usam ``FDL_DATASET_EMPRESA`` global (ex.: «Antomóveis») e o
    filtro por ``display_name`` esvaziava o repasse/faturamento para todas as orgs.
    """
    if df.empty or "empresa" not in df.columns:
        return df
    if _materialized_path_mode() == "dynamic":
        return df
    empresas = st.session_state["empresas_permitidas"]
    return df[df["empresa"].isin(empresas)].copy()


if "op_financeiro_view" not in st.session_state:
    st.session_state["op_financeiro_view"] = "repasse"
elif st.session_state["op_financeiro_view"] not in ("repasse", "frete", "faturamento"):
    st.session_state["op_financeiro_view"] = "repasse"

_enabled_modules = _enabled_finance_modules()

# Área de produto: Financeiro (repasse/frete) vs Faturamento & DRE (módulo próprio).
if SESSION_FDL_PRODUCT_AREA_KEY not in st.session_state:
    if st.session_state.get("op_financeiro_view") == "faturamento":
        st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FATURAMENTO_DRE
        st.session_state["op_financeiro_view"] = "repasse"
    else:
        st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FINANCEIRO
# Migração de sessões antigas que ainda tinham só op_financeiro_view == faturamento
if st.session_state.get("op_financeiro_view") == "faturamento":
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FATURAMENTO_DRE
    st.session_state["op_financeiro_view"] = "repasse"
if st.session_state["op_financeiro_view"] not in ("repasse", "frete"):
    st.session_state["op_financeiro_view"] = "repasse"

if st.session_state["op_financeiro_view"] not in _enabled_modules:
    st.session_state["op_financeiro_view"] = "repasse" if "repasse" in _enabled_modules else "frete"

if "faturamento" not in _enabled_modules and st.session_state.get(SESSION_FDL_PRODUCT_AREA_KEY) in (
    FDL_PRODUCT_AREA_FATURAMENTO_DRE,
    FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS,
):
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FINANCEIRO

_fdl_global_trace("03: após definir vista financeiro (session_state)")

# Alinhado a PIPELINE_DATA_REVISION (liberações / Valor pago). Subir junto quando mudar o fluxo.
OPERACIONAL_CACHE_REVISION = PIPELINE_DATA_REVISION
REQUIRED_ONEDRIVE_CSV_COLUMNS = {
    "N° de venda",
    "ID do pedido",
    "Número da nota",
    "Plataforma",
    "Situação",
    "Ação sugerida",
    "Valor pago",
    "Valor da nota",
    "Valor a receber",
    "Diferença",
    "Data de pagamento",
}
REQUIRED_ONEDRIVE_SOURCE_FOLDERS = {
    "Vendas - Mercado Livre",
    "Liberações_ML",
    "notas_saida",
    "contas_receber",
}
RETRYABLE_HTTP_CODES = {429, 500, 502, 503, 504}
MAX_HTTP_RETRIES = 4
ONEDRIVE_SYNC_MIN_INTERVAL_SECONDS = 180
# Pedidos curtos na Cloud: evita ecrã em branco ~vários minutos em sequência (SharePoint pode tardar ou pendurar).
PRECOMPUTED_HTTP_TIMEOUT = 35
# Alguns links SharePoint só redirecionam para o binário quando o pedido parece um browser.
_BROWSER_UA_CHROME = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

_BR_TZ = ZoneInfo("America/Sao_Paulo")


def _safe_streamlit_date(value: object, fallback: date) -> date:
    """Evita TypeError ao comparar None com date (`st.date_input` pode devolver None)."""
    if value is None:
        return fallback
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return fallback


def _pd_to_datetime_pedido_br(series: pd.Series) -> pd.Series:
    """
    Datas do export ML / pedidos em **DD/MM/AAAA**. Sem ``dayfirst=True``, o pandas trata
    datas ambíguas como MM/DD (ex.: 05/02/2026 → 2 mai em vez de 5 fev), quebrando filtro por período e totais.
    """
    return pd.to_datetime(series, errors="coerce", dayfirst=True)


def _faturamento_series_bool_mask(series: pd.Series) -> pd.Series:
    """
    Flags booleanas do materializado: Parquet traz ``bool``; o CSV de app pode trazer ``\"True\"`` / ``\"False\"``.
    ``astype(bool)`` em string ``\"False\"`` em pandas devolve **True** (string não vazia) — incorreto para visão NF.
    """
    s = series
    if isinstance(s.dtype, pd.BooleanDtype):
        return s.fillna(False).astype(bool)
    if s.dtype == bool or pd.api.types.is_bool_dtype(s):
        return s.fillna(False).astype(bool)
    if pd.api.types.is_numeric_dtype(s):
        return pd.to_numeric(s, errors="coerce").fillna(0).ne(0)
    x = s.astype(str).str.strip().str.casefold()
    return x.eq("true") | x.eq("1") | x.eq("yes") | x.eq("sim")


def _faturamento_ts_nf_emissao_para_dia_civil(s: pd.Series) -> pd.Series:
    """``Nota_Data_Emissao`` vem em ISO ``YYYY-MM-DD HH:MM:SS`` do join; **não** usar ``dayfirst=True`` em série (pandas infer quebra parte das linhas)."""
    ts = pd.to_datetime(s, errors="coerce", dayfirst=False)
    if ts.empty:
        return ts
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    return ts.dt.normalize()


def _faturamento_mask_nf_emissao_no_periodo(s: pd.Series, d_ini: date, d_fim: date) -> pd.Series:
    """Intervalo inclusive no dia civil BR para data de emissão da NF."""
    ts = _faturamento_ts_nf_emissao_para_dia_civil(s)
    if ts.empty:
        return pd.Series(False, index=ts.index)
    dcal = ts.dt.date
    ok = pd.notna(ts)
    ge = pd.Series(dcal, index=ts.index) >= d_ini
    le = pd.Series(dcal, index=ts.index) <= d_fim
    return ok & ge & le


def _faturamento_ts_pedido_para_dia_civil(s: pd.Series) -> pd.Series:
    """Converte coluna **Data** (texto BR, datetime64 ou tz-aware) para datetime64 naive em dia civil BR."""
    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if ts.empty:
        return ts
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    return ts.dt.normalize()


def _faturamento_mask_venda_no_periodo(s: pd.Series, d_ini: date, d_fim: date) -> pd.Series:
    """
    Linhas cuja **Data** (venda) cai entre ``d_ini`` e ``d_fim`` inclusive, em dia civil.
    Evita comparações ``Timestamp`` vs meia-noite que falham com Parquet tz-aware ou tipos mistos.
    """
    ts = _faturamento_ts_pedido_para_dia_civil(s)
    if ts.empty:
        return pd.Series(False, index=ts.index)
    dcal = ts.dt.date
    ok = pd.notna(ts)
    ge = pd.Series(dcal, index=ts.index) >= d_ini
    le = pd.Series(dcal, index=ts.index) <= d_fim
    return ok & ge & le


def _series_datetime_bounds_dates(series: pd.Series, *, dayfirst: bool = True) -> tuple[date, date, bool]:
    """
    Min/max em dia civil a partir de coluna parseável como datetime.
    Não chama .min().date() sobre série só NaT (evita NaT/erros em limites).
    Devolve (d_min, d_max, tem_alguma_data_parseável).
    """
    ts = pd.to_datetime(series, errors="coerce", dayfirst=dayfirst)
    if getattr(ts.dt, "tz", None) is not None:
        ts = ts.dt.tz_convert(_BR_TZ)
    t = ts[ts.notna()]
    if t.empty:
        d = datetime.now(_BR_TZ).date()
        return d, d, False
    return t.min().date(), t.max().date(), True


def _faturamento_period_calendar_limits(d_min: date, d_max: date) -> tuple[date, date]:
    """
    Limites do ``date_input`` mais amplos que o min/max dos dados carregados.

    Sem isto, o utilizador não consegue escolher janeiro→hoje quando a base só tem março
    (o widget ficava preso a [d_min, d_max] e o estado era re-clampado a cada rerun).
    O filtro continua a usar só linhas cuja **Data** cai no intervalo escolhido.
    """
    today = datetime.now(_BR_TZ).date()
    cal_max = max(d_max, today)
    cal_min = min(d_min, today - timedelta(days=3 * 365))
    return cal_min, cal_max


# Convenção de produto (Faturamento & DRE):
# - Vista mínima: bloco **Conferência venda × NF** usa ``compute_comercial_conferencia_stats`` / ``compute_fiscal_nf_conferencia_stats``.
# - Vista completa / agregados comerciais: «receita líquida» pode usar Σ ``Nota_Valor_Liquido_Rateado`` ou ``Valor total``.
_FATURAMENTO_UI_VALOR_NOTA_FISCAL = "Valor Nota Fiscal"
_FATURAMENTO_HELP_VALOR_NOTA_FISCAL = (
    "Materializado V2 com notas: soma de **Nota_Valor_Liquido_Rateado** (valor líquido da nota de saída, rateado por linha). "
    "Sem join fiscal: fallback à coluna **Valor total** do pedido."
)
_FATURAMENTO_HELP_VL_NF_FISCAL_KPI_MIN = (
    "KPI **fiscal**: soma do **valor líquido total da NF** (``Nota_Valor_Liquido_Total``, uma vez por nota), "
    "com **Nota_Data_Emissao** no período acima. Exclui cancelada / denegada / inutilizada. "
    "Respeita só o filtro **Empresa** (a **Plataforma** não corta este total)."
)
_FATURAMENTO_HELP_VL_NF_COL_MIN_TABLE = (
    "Valor **por linha de venda** (rateio ``Nota_Valor_Liquido_Rateado`` quando existir). "
    "O total fiscal do período de emissão está no KPI **Vl. Nota Fiscal** acima, não na soma desta coluna."
)
_FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN = (
    "Eixo **fiscal**: filtra pela **data de emissão** da nota (``Nota_Data_Emissao``). "
    "Independente do **período da venda** e da **Plataforma**."
)
_FATURAMENTO_HELP_PERIODO_DATA = (
    "Eixo oficial do período: coluna **Data** (pedido / export ML). "
    "**Data do faturamento** na tabela é informativa e pode estar incompleta — não rege este filtro."
)
_FATURAMENTO_HELP_NUMERO_NF_COL = (
    "Com export só de pedidos ML, costuma vazio — **esperado**, não erro. "
    "NF confiável: outra fonte ou join futuro."
)


def _sb_user_initials(display_name: str) -> str:
    """Iniciais para avatar (máx. 2 caracteres)."""
    parts = [p for p in str(display_name).strip().split() if p]
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()[:2]
    if parts:
        return parts[0][:2].upper()
    return "?"


def _sb_nav_set_repasse() -> None:
    st.session_state["op_financeiro_view"] = "repasse"
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FINANCEIRO


def _sb_nav_set_frete() -> None:
    st.session_state["op_financeiro_view"] = "frete"
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FINANCEIRO


def _sb_nav_set_faturamento_dre() -> None:
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_FATURAMENTO_DRE


def _sb_nav_set_comercial_pedidos() -> None:
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS


def _sb_logout_click() -> None:
    logout_operacional_user()
    st.rerun()


def _fdl_sidebar_inject_layout_css() -> None:
    """Tipografia e navegação da sidebar (gerencial vs operacional, estados ativos)."""
    st.markdown(
        dedent(
            """
            <style>
            [data-testid="stSidebar"] .block-container {
              padding-top: 0.5rem;
              padding-bottom: 1.35rem;
            }
            [data-testid="stSidebar"] div[data-testid="stButton"] {
              margin-bottom: 0.48rem !important;
            }
            .fdl-sb-brand-shell {
              margin: 0.1rem 0 0.15rem 0;
              padding: 0.7rem 0.6rem 0.58rem;
              border-radius: 12px;
              background: linear-gradient(
                165deg,
                rgba(248, 250, 252, 0.75) 0%,
                rgba(255, 255, 255, 0.4) 48%,
                rgba(248, 250, 252, 0.35) 100%
              );
              border: 1px solid rgba(226, 232, 240, 0.65);
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.03);
            }
            .fdl-sb-brand {
              margin: 0;
              padding: 0;
            }
            [data-testid="stSidebar"] [data-testid="stImage"] img {
              max-height: 2.72rem;
              width: auto !important;
              object-fit: contain;
              display: block;
              margin: 0 auto 0.52rem auto;
              opacity: 1;
            }
            .fdl-sb-product {
              font-size: 1.14rem;
              font-weight: 700;
              letter-spacing: -0.04em;
              color: #0f172a;
              line-height: 1.15;
              margin: 0 0 0.22rem 0;
            }
            .fdl-sb-tagline {
              font-size: 0.6875rem;
              font-weight: 600;
              letter-spacing: 0.045em;
              color: #5c6675;
              line-height: 1.5;
              margin: 0 0 0.65rem 0;
            }
            .fdl-sb-tagline--after-logo {
              margin-top: 0;
              margin-bottom: 0.62rem;
              padding: 0 0.35rem;
              text-align: center;
              font-weight: 600;
              letter-spacing: 0.062em;
              color: #4a5568;
            }
            .fdl-sb-client-row {
              margin: 0;
              padding-top: 0.52rem;
              border-top: 1px solid rgba(226, 232, 240, 0.65);
            }
            .fdl-sb-client-block {
              display: flex;
              flex-direction: column;
              align-items: center;
              text-align: center;
              gap: 0.3rem;
            }
            .fdl-sb-brand-shell .fdl-sb-client-block {
              align-items: stretch;
              text-align: left;
            }
            .fdl-sb-client-tag {
              font-size: 0.5rem;
              font-weight: 500;
              text-transform: uppercase;
              letter-spacing: 0.14em;
              color: #d8dee6;
              line-height: 1.25;
            }
            .fdl-sb-client-name {
              font-size: 0.875rem;
              font-weight: 600;
              letter-spacing: -0.015em;
              color: #3d4d5c;
              line-height: 1.42;
              word-break: break-word;
            }
            .fdl-sb-divider {
              height: 1px;
              background: linear-gradient(90deg, transparent, #e2e8f0 12%, #e2e8f0 88%, transparent);
              margin: 0.75rem 0 0.15rem 0;
            }
            .fdl-sb-section-label {
              font-size: 0.625rem;
              font-weight: 600;
              text-transform: none;
              letter-spacing: 0.1em;
              color: #8b96a3;
              margin: 1.38rem 0 0.62rem 0;
              padding: 0.32rem 0.45rem 0.32rem 0.5rem;
              border-radius: 8px;
              background: rgba(248, 250, 252, 0.72);
              border: 1px solid rgba(226, 232, 240, 0.55);
              box-shadow: 0 1px 0 rgba(255, 255, 255, 0.65) inset;
              line-height: 1.3;
            }
            .fdl-sb-section-label--first {
              margin-top: 0.35rem;
            }
            .fdl-sb-org-hint {
              font-size: 0.5625rem;
              font-weight: 600;
              letter-spacing: 0.14em;
              text-transform: uppercase;
              color: #d1d9e2;
              line-height: 1.3;
              margin: 0 0 0.38rem 0;
              padding: 0 0.2rem;
              text-align: center;
            }
            [data-testid="stSidebar"] div[data-testid="stButton"] > button[kind="primary"] {
              font-weight: 600 !important;
              letter-spacing: -0.012em !important;
              color: #0f172a !important;
              background: linear-gradient(180deg, #ffffff 0%, #f4f6f9 100%) !important;
              border: 1px solid rgba(226, 232, 240, 0.95) !important;
              border-left: 3px solid #0f172a !important;
              border-radius: 10px !important;
              box-shadow:
                0 1px 0 rgba(255, 255, 255, 0.98) inset,
                0 1px 2px rgba(15, 23, 42, 0.04),
                0 3px 8px rgba(15, 23, 42, 0.05) !important;
              padding-top: 0.56rem !important;
              padding-bottom: 0.56rem !important;
              min-height: 2.65rem !important;
              transition: box-shadow 0.18s ease, border-color 0.18s ease, background 0.18s ease !important;
            }
            [data-testid="stSidebar"] div[data-testid="stButton"] > button[kind="primary"]:hover {
              border-color: rgba(203, 213, 225, 0.98) !important;
              box-shadow:
                0 1px 0 rgba(255, 255, 255, 0.98) inset,
                0 2px 4px rgba(15, 23, 42, 0.05),
                0 5px 14px rgba(15, 23, 42, 0.07) !important;
            }
            [data-testid="stSidebar"] div[data-testid="stButton"] > button[kind="secondary"] {
              font-weight: 500 !important;
              letter-spacing: -0.008em !important;
              color: #64748b !important;
              border: 1px solid rgba(226, 232, 240, 0.55) !important;
              background: linear-gradient(180deg, rgba(255, 255, 255, 0.72) 0%, rgba(248, 250, 252, 0.55) 100%) !important;
              border-radius: 10px !important;
              padding-top: 0.52rem !important;
              padding-bottom: 0.52rem !important;
              min-height: 2.52rem !important;
              box-shadow: 0 1px 0 rgba(255, 255, 255, 0.55) inset !important;
              transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease, box-shadow 0.18s ease !important;
            }
            [data-testid="stSidebar"] div[data-testid="stButton"] > button[kind="secondary"]:hover {
              border-color: rgba(203, 213, 225, 0.85) !important;
              background: linear-gradient(180deg, #ffffff 0%, #f1f5f9 100%) !important;
              color: #475569 !important;
              box-shadow:
                0 1px 0 rgba(255, 255, 255, 0.85) inset,
                0 2px 6px rgba(15, 23, 42, 0.04) !important;
            }
            .fdl-sb-footer-rule {
              height: 1px;
              margin: 1.25rem 0 0;
              background: linear-gradient(90deg, transparent, rgba(226, 232, 240, 0.55) 8%, rgba(226, 232, 240, 0.85) 50%, rgba(226, 232, 240, 0.55) 92%, transparent);
            }
            .fdl-sb-footer {
              margin: 0.62rem 0 0 0;
              padding: 0 0.2rem 0.15rem;
            }
            .fdl-sb-footer-label {
              font-size: 0.5rem;
              font-weight: 500;
              text-transform: uppercase;
              letter-spacing: 0.14em;
              color: #d1d9e2;
              margin: 0 0 0.28rem 0;
              line-height: 1.25;
            }
            .fdl-sb-footer-ts {
              font-size: 0.78125rem;
              font-weight: 500;
              font-variant-numeric: tabular-nums;
              color: #5c6b7a;
              margin: 0 0 0.4rem 0;
              line-height: 1.4;
              letter-spacing: 0.02em;
            }
            .fdl-sb-footer-admin {
              font-size: 0.65rem;
              font-weight: 400;
              color: #b4bcc6;
              margin: 0.1rem 0 0 0;
              line-height: 1.4;
              word-break: break-word;
            }
            .fdl-sb-footer-spacer {
              height: 0.5rem;
            }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )


def _now_ts_br_str() -> str:
    """Carimbo para UI em Brasília (Streamlit Cloud costuma usar UTC — o dia «salta» para o utilizador BR)."""
    return datetime.now(_BR_TZ).strftime("%Y-%m-%d %H:%M:%S")


def _ts_br_from_mtime_ns(mtime_ns: int) -> str:
    dt = datetime.fromtimestamp(mtime_ns / 1e9, tz=timezone.utc).astimezone(_BR_TZ)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _ts_br_from_http_last_modified(header: str | None) -> str | None:
    if not header or not str(header).strip():
        return None
    try:
        dt = parsedate_to_datetime(str(header).strip())
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_BR_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _is_admin_mode() -> bool:
    env_mode = os.environ.get("FDL_APP_MODE", "").strip().lower()
    if env_mode == "admin":
        return True
    try:
        return str(st.secrets.get("FDL_APP_MODE", "")).strip().lower() == "admin"
    except Exception:
        return False


def _expose_load_errors() -> bool:
    """
    Quando True, falhas em _load_data() mostram st.exception (mensagem + traceback).
    Para desligar em produção perante cliente final: FDL_SHOW_LOAD_ERRORS=false nos secrets.
    """
    env = os.environ.get("FDL_SHOW_LOAD_ERRORS", "").strip().lower()
    if env in {"0", "false", "no", "off"}:
        return False
    if env in {"1", "true", "yes", "on"}:
        return True
    try:
        raw = st.secrets.get("FDL_SHOW_LOAD_ERRORS", None)
        if isinstance(raw, bool):
            return raw
        v = str(raw or "").strip().lower()
        if v in {"0", "false", "no", "off"}:
            return False
        if v in {"1", "true", "yes", "on"}:
            return True
    except Exception:
        pass
    return True


def _data_source_mode() -> str:
    env_source = os.environ.get("FDL_DATA_SOURCE", "").strip().lower()
    if env_source:
        return env_source
    try:
        return str(st.secrets.get("FDL_DATA_SOURCE", "onedrive")).strip().lower()
    except Exception:
        return "onedrive"


_STRICT_MATERIALIZED_USER_MSG = (
    "Base de dados não disponível. Contacte o administrador ou tente mais tarde."
)


def _strict_materialized() -> bool:
    """Produção: sem fallback live quando repasse/frete em modo materialized."""
    raw = os.environ.get("FDL_STRICT_MATERIALIZED", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_STRICT_MATERIALIZED", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _frete_debug_ui_enabled() -> bool:
    """Diagnóstico na página Frete — opt-in (env ou secrets). Omisso = desligado (não afeta repasse)."""
    raw = os.environ.get("FDL_DEBUG_FRETE_UI", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_DEBUG_FRETE_UI", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _frete_stage_error(etapa: int, titulo: str, exc: BaseException) -> None:
    """Erro visível com número da etapa (1–4) para diagnóstico na Cloud."""
    try:
        st.error(f"**Conciliação de Frete — falha na etapa {etapa}/4: {titulo}**")
        st.exception(exc)
    except Exception:
        pass


def _frete_stage_trace(etapa: int, titulo: str, detalhe: str) -> None:
    try:
        if _frete_debug_ui_enabled():
            st.caption(f"Frete [etapa {etapa}/4 — {titulo}] {detalhe}")
    except Exception:
        pass


def _bootstrap_debug_enabled() -> bool:
    """Rerun global: etapas visíveis (opt-in). Env/secrets: FDL_DEBUG_BOOTSTRAP=1."""
    raw = os.environ.get("FDL_DEBUG_BOOTSTRAP", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_DEBUG_BOOTSTRAP", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _inject_fdl_professional_theme() -> None:
    """CSS global: reduz ruído de plataforma (menu, footer, toolbar/fork) — não é lógica de negócio."""
    if st.session_state.get("_fdl_ui_theme_applied") is True:
        st.session_state.pop("_fdl_ui_theme_applied", None)
    if st.session_state.get("_fdl_ui_theme_applied") == "v3":
        return
    st.markdown(
        """
        <style>
            #MainMenu {visibility: hidden;}
            footer {visibility: hidden;}
            [data-testid="stToolbar"] {visibility: hidden !important; height: 0 !important; max-height: 0 !important;}
            [data-testid="stDecoration"] {display: none;}
            header[data-testid="stHeader"] {background: rgba(255,255,255,0);}
            header a[href*="fork"] {display: none !important;}
            div[data-testid="stToolbar"] {visibility: hidden !important;}
            section[data-testid="stMain"] {
                max-width: 100%;
            }
            section[data-testid="stMain"] h2 {
                margin-top: 0.45rem;
                margin-bottom: 0.35rem;
                color: #111827;
                font-weight: 600;
                letter-spacing: -0.015em;
                line-height: 1.25;
            }
            section[data-testid="stMain"] h3 {
                margin-top: 0.5rem;
                margin-bottom: 0.35rem;
                color: #1f2937;
                font-weight: 600;
            }
            section[data-testid="stMain"] hr {
                margin: 0.5rem 0 !important;
                border: none;
                border-top: 1px solid #e8ecf1;
            }
            .fdl-ui-gap-section {
                display: block;
                height: 0.55rem;
                min-height: 0.55rem;
            }
            .fdl-ui-gap-section-lg {
                display: block;
                height: 0.75rem;
                min-height: 0.75rem;
            }
            .fdl-ui-gap-tight {
                display: block;
                height: 0.28rem;
                min-height: 0.28rem;
            }
            .fdl-financeiro-header {
                margin: 0 0 0.5rem 0;
                padding: 0 0 0.15rem 0;
            }
            .fdl-financeiro-header .fdl-header-kicker {
                margin: 0 0 0.2rem 0;
                font-size: 0.78rem;
                font-weight: 600;
                letter-spacing: 0.04em;
                text-transform: uppercase;
                color: #6b7280;
            }
            .fdl-financeiro-header .fdl-header-title {
                margin: 0 0 0.35rem 0;
                font-size: 1.65rem;
                font-weight: 700;
                letter-spacing: -0.02em;
                line-height: 1.2;
                color: #111827;
            }
            .fdl-financeiro-header .fdl-header-sub {
                margin: 0;
                font-size: 0.95rem;
                color: #4b5563;
                line-height: 1.5;
            }
            .fdl-financeiro-header--compact .fdl-header-title {
                margin-bottom: 0.2rem;
                font-size: 1.45rem;
            }
            .fdl-financeiro-header--compact .fdl-header-sub {
                font-size: 0.88rem;
                line-height: 1.35;
            }
            /* Cartão de métrica (Streamlit 1.35+: stMetricContainer envolve label + valor) */
            [data-testid="stMetricContainer"] {
                border-radius: 0.5rem !important;
                padding: 0.75rem 0.95rem 0.8rem 0.95rem !important;
                min-height: 4.85rem;
                background: linear-gradient(180deg, #ffffff 0%, #f1f5f9 100%) !important;
                border: 1px solid #cbd5e1 !important;
                box-shadow: 0 1px 3px rgba(15, 23, 42, 0.08) !important;
                box-sizing: border-box !important;
            }
            /* Valor: Streamlit 1.35+ usa Markdown dentro de stMetricValue */
            [data-testid="stMetricValue"] [data-testid="stMarkdownContainer"] p {
                font-weight: 700 !important;
                font-size: 1.62rem !important;
                letter-spacing: -0.04em !important;
                line-height: 1.12 !important;
                color: #0f172a !important;
                margin: 0 !important;
            }
            [data-testid="stMetricValue"] > div {
                font-weight: 700 !important;
                font-size: 1.62rem !important;
                letter-spacing: -0.04em;
                line-height: 1.12 !important;
                color: #0f172a !important;
            }
            [data-testid="stMetricLabel"] {
                opacity: 1 !important;
                margin-bottom: 0.35rem !important;
            }
            [data-testid="stMetricLabel"] label,
            [data-testid="stMetricLabel"] p {
                font-size: 0.78rem !important;
                font-weight: 500 !important;
                color: #64748b !important;
                line-height: 1.3 !important;
                letter-spacing: 0.02em;
            }
            div[data-testid="stVerticalBlockBorderWrapper"] {
                border-radius: 0.5rem;
                border-color: #e2e8f0 !important;
                box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
            }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.session_state["_fdl_ui_theme_applied"] = "v3"


def _fdl_ui_gap_section() -> None:
    """Espaço vertical consistente entre blocos (apenas UI)."""
    st.markdown('<div class="fdl-ui-gap-section" aria-hidden="true"></div>', unsafe_allow_html=True)


def _fdl_ui_gap_section_lg() -> None:
    st.markdown('<div class="fdl-ui-gap-section-lg" aria-hidden="true"></div>', unsafe_allow_html=True)


def _fdl_ui_gap_tight() -> None:
    """Espaço vertical reduzido (MVP Faturamento & DRE — compactação)."""
    st.markdown('<div class="fdl-ui-gap-tight" aria-hidden="true"></div>', unsafe_allow_html=True)


def _render_financeiro_header(
    *,
    segment: str,
    title: str,
    subtitle: str = "",
    kicker_area: str = "Financeiro",
    compact_spacing: bool = False,
) -> None:
    """Topo unificado: evita repetir o nome do cliente (já na barra lateral)."""
    esc_seg = html.escape(segment)
    esc_title = html.escape(title)
    esc_ka = html.escape((kicker_area or "Financeiro").strip() or "Financeiro")
    esc_sub = html.escape((subtitle or "").strip())
    sub_html = ""
    if esc_sub:
        sub_html = f'<p class="fdl-header-sub">{esc_sub}</p>'
    _cls = "fdl-financeiro-header" + (" fdl-financeiro-header--compact" if compact_spacing else "")
    st.markdown(
        f'<div class="{_cls}">'
        f'<p class="fdl-header-kicker">{esc_ka} · {esc_seg}</p>'
        f'<h1 class="fdl-header-title">{esc_title}</h1>'
        f"{sub_html}"
        f"</div>",
        unsafe_allow_html=True,
    )
    if compact_spacing:
        _fdl_ui_gap_tight()
    else:
        _fdl_ui_gap_section()
        st.divider()


def _fdl_safe_mode() -> bool:
    """UI mínima: sem Styler, sem data_editor pesado, menos HTML (FDL_SAFE_MODE=1)."""
    raw = os.environ.get("FDL_SAFE_MODE", "").strip().lower()
    if raw in {"1", "true", "yes", "on"}:
        return True
    if raw in {"0", "false", "no", "off"}:
        return False
    try:
        sec = st.secrets.get("FDL_SAFE_MODE", False)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() in {"1", "true", "yes", "on"}
    except Exception:
        return False


def _fdl_minimal_layout() -> bool:
    """
    Layout nativo Streamlit: sem CSS global injetado, sem HTML customizado nos painéis.
    Omisso ou FDL_MINIMAL_LAYOUT=1 → ativo. Defina FDL_MINIMAL_LAYOUT=0 para restaurar o design.
    """
    raw = os.environ.get("FDL_MINIMAL_LAYOUT", "").strip().lower()
    if raw in {"0", "false", "no", "off"}:
        return False
    if raw in {"1", "true", "yes", "on"}:
        return True
    try:
        sec = st.secrets.get("FDL_MINIMAL_LAYOUT", True)
        if isinstance(sec, bool):
            return sec
        return str(sec).strip().lower() not in {"0", "false", "no", "off"}
    except Exception:
        return True


def _repasse_consume_mode() -> str:
    """Repasse: live = pipeline; materialized = CSV/XLSX. Com FDL_STRICT_MATERIALIZED, sem fallback para live."""
    raw = os.environ.get("FDL_REPASSE_CONSUME_MODE", "").strip().lower()
    if raw in {"materialized", "live"}:
        return raw
    try:
        s = str(st.secrets.get("FDL_REPASSE_CONSUME_MODE", "")).strip().lower()
        if s in {"materialized", "live"}:
            return s
    except Exception:
        pass
    return "live"


def _repasse_materialized_path_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return _dynamic_materialized_repasse_rel_path(_active_org.org_id)
    raw = os.environ.get("FDL_REPASSE_MATERIALIZED_PATH", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_REPASSE_MATERIALIZED_PATH", "")).strip()
    except Exception:
        return ""


def _repasse_materialized_url_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return ""
    raw = os.environ.get("FDL_REPASSE_MATERIALIZED_URL", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_REPASSE_MATERIALIZED_URL", "")).strip()
    except Exception:
        return ""


def _frete_consume_mode() -> str:
    raw = os.environ.get("FDL_FRETE_CONSUME_MODE", "").strip().lower()
    if raw in {"materialized", "live"}:
        return raw
    try:
        s = str(st.secrets.get("FDL_FRETE_CONSUME_MODE", "")).strip().lower()
        if s in {"materialized", "live"}:
            return s
    except Exception:
        pass
    return "live"


def _frete_materialized_path_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return _dynamic_materialized_frete_rel_path(_active_org.org_id)
    raw = os.environ.get("FDL_FRETE_MATERIALIZED_PATH", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_FRETE_MATERIALIZED_PATH", "")).strip()
    except Exception:
        return ""


def _frete_materialized_url_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return ""
    raw = os.environ.get("FDL_FRETE_MATERIALIZED_URL", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_FRETE_MATERIALIZED_URL", "")).strip()
    except Exception:
        return ""


def _faturamento_consume_mode() -> str:
    """Faturamento: nesta fase só materialized é suportado; default materialized."""
    raw = os.environ.get("FDL_FATURAMENTO_CONSUME_MODE", "").strip().lower()
    if raw in {"materialized", "live"}:
        return raw
    try:
        s = str(st.secrets.get("FDL_FATURAMENTO_CONSUME_MODE", "")).strip().lower()
        if s in {"materialized", "live"}:
            return s
    except Exception:
        pass
    return "materialized"


def _faturamento_data_layout() -> str:
    """
    Como interpretar o ficheiro materializado com path/URL explícitos.

    - v1: layout legado (faturamento por pasta de empresa; sem filtro ``org_id``).
    - v2: dataset multi-empresa; filtrar pela org ativa quando existir coluna ``org_id``.
    - auto: inferir (fallback): ``v2`` se existir coluna ``org_id`` com dados, senão ``v1``.

    Descoberta **sem** path explícito: ``v2_canonical`` fixa layout efetivo em v2;
    ``v1_repasse_sibling`` fixa em v1.
    """
    raw = os.environ.get("FDL_FATURAMENTO_DATA_LAYOUT", "").strip().lower()
    if raw in {"v1", "v2", "auto"}:
        return raw
    try:
        s = str(st.secrets.get("FDL_FATURAMENTO_DATA_LAYOUT", "")).strip().lower()
        if s in {"v1", "v2", "auto"}:
            return s
    except Exception:
        pass
    return "auto"


def _faturamento_resolve_disk_path(path_s: str) -> Path:
    p = Path(path_s.strip()).expanduser()
    if not p.is_absolute():
        p = (_REPO_APP_ROOT / p).resolve()
    else:
        p = p.resolve()
    return p


def _faturamento_v2_canonical_dataset_path_str() -> str:
    """``data_products/<cliente_slug>/faturamento/current/dataset_faturamento_app.csv`` (ou Parquet)."""
    slug = _materialized_cliente_slug().strip()
    if not slug:
        return ""
    root = _materialized_data_products_root().strip().strip("/\\")
    base = Path(root) / slug / "faturamento" / "current"
    if not base.is_absolute():
        base = (_REPO_APP_ROOT / base).resolve()
    else:
        base = base.resolve()
    for name in ("dataset_faturamento_app.csv", "dataset.parquet"):
        cand = base / name
        if cand.is_file():
            return str(cand.resolve())
    return ""


def _faturamento_resolve_materialized_target() -> dict[str, str]:
    """
    Ordem: explícito (path/URL) → **V2 canônico** (se existir em disco) → derivado V1 do repasse.

    Sem path explícito, o ficheiro em ``data_products/<slug>/faturamento/current/`` tem prioridade
    sobre o CSV «irmão do repasse» (V1 por empresa), para carregar join fiscal (``Nota_Data_Emissao``, etc.).

    Devolve chaves: path_s, url_s, resolution_source, path_final_resolved, layout_declared.
    """
    declared = _faturamento_data_layout()
    mp = _faturamento_materialized_path_str()
    mu = _faturamento_materialized_url_str()
    if mp or mu:
        final = ""
        if mp:
            try:
                p = _faturamento_resolve_disk_path(mp)
                final = str(p) if p.is_file() else mp.strip()
            except Exception:
                final = mp.strip()
        if not final and mu:
            final = mu.strip()
        return {
            "path_s": mp.strip(),
            "url_s": mu.strip(),
            "resolution_source": "explicit",
            "path_final_resolved": final or mp.strip() or mu.strip(),
            "layout_declared": declared,
        }
    v2s = _faturamento_v2_canonical_dataset_path_str()
    if v2s:
        return {
            "path_s": v2s,
            "url_s": "",
            "resolution_source": "v2_canonical",
            "path_final_resolved": v2s,
            "layout_declared": declared,
        }
    for anchor in (_repasse_materialized_path_str(), _precomputed_path_str()):
        d = _derive_faturamento_materialized_from_repasse_anchor(anchor)
        if d:
            return {
                "path_s": d,
                "url_s": "",
                "resolution_source": "v1_repasse_sibling",
                "path_final_resolved": d,
                "layout_declared": declared,
            }
    return {
        "path_s": "",
        "url_s": "",
        "resolution_source": "none",
        "path_final_resolved": "",
        "layout_declared": declared,
    }


def _faturamento_materialized_path_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return ""
    raw = os.environ.get("FDL_FATURAMENTO_MATERIALIZED_PATH", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_FATURAMENTO_MATERIALIZED_PATH", "")).strip()
    except Exception:
        return ""


def _faturamento_materialized_url_str() -> str:
    if _materialized_path_mode() == "dynamic":
        return ""
    raw = os.environ.get("FDL_FATURAMENTO_MATERIALIZED_URL", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_FATURAMENTO_MATERIALIZED_URL", "")).strip()
    except Exception:
        return ""


def _derive_faturamento_materialized_from_repasse_anchor(anchor: str) -> str:
    """
    Se o repasse/precomputed apontar para .../<cliente>/<empresa>/repasse/current/dataset_repasse_app.csv,
    tenta o irmão .../<empresa>/faturamento/current/dataset_faturamento_app.csv e, se faltar, dataset.parquet.

    Isto alinha ao layout **V1** (faturamento por pasta de empresa). Faturamento **V2** grava em
    ``data_products/<cliente_slug>/faturamento/current/``; esse caminho exige ``FDL_FATURAMENTO_MATERIALIZED_PATH``
    ou evolução desta derivação — ver ``docs/faturamento_pipeline.md``.
    """
    if not (anchor or "").strip():
        return ""
    path = Path(anchor.strip()).expanduser()
    if not path.is_absolute():
        path = (_REPO_APP_ROOT / path).resolve()
    if not path.is_file():
        return ""
    if path.parent.name != "current" or path.parent.parent.name != "repasse":
        return ""
    # materialize_financeiro: .../<cliente>/<empresa>/repasse/current/ e .../<empresa>/faturamento/current/ (irmãos)
    empresa_dir = path.parent.parent.parent
    base = empresa_dir / "faturamento" / "current"
    csv_c = base / "dataset_faturamento_app.csv"
    if csv_c.is_file():
        return str(csv_c.resolve())
    pq = base / "dataset.parquet"
    if pq.is_file():
        return str(pq.resolve())
    return ""


def _faturamento_classify_layout_effective(
    *,
    resolution_source: str,
    layout_declared: str,
    df: pd.DataFrame,
) -> tuple[str, str]:
    """
    Devolve (layout_efetivo v1|v2, nota_curta_para_metadata).
    """
    if resolution_source == "v2_canonical":
        return "v2", "v2_canonical"
    if resolution_source == "v1_repasse_sibling":
        return "v1", "v1_repasse_sibling"
    if resolution_source == "explicit":
        if layout_declared == "v1":
            return "v1", "explicit_declared_v1"
        if layout_declared == "v2":
            return "v2", "explicit_declared_v2"
        if "org_id" in df.columns and df["org_id"].notna().any():
            return "v2", "explicit_auto_org_id"
        return "v1", "explicit_auto_no_org_id"
    return "v1", "fallback_v1"


def _faturamento_apply_layout_scope(
    df: pd.DataFrame, *, layout_effective: str, org_id: str
) -> tuple[pd.DataFrame, str | None]:
    """
    Para layout v2, restringe à org ativa. Devolve (df, nota_warning ou None).
    """
    if layout_effective != "v2":
        return df, None
    oid = str(org_id).strip()
    if "org_id" not in df.columns:
        return df, "Layout v2: coluna org_id ausente — filtro por org não aplicado."
    out = df.loc[df["org_id"].astype(str).str.strip() == oid].copy()
    return out, None


def _faturamento_apply_layout_scope_consolidado_v2(
    df: pd.DataFrame, *, allowed_org_ids: frozenset[str]
) -> tuple[pd.DataFrame, str | None]:
    """V2: todas as orgs permitidas ao utilizador (não a org ativa da sidebar)."""
    if "org_id" not in df.columns:
        return df.copy(), "Layout v2: coluna org_id ausente — consolidado sem filtro por org."
    if not allowed_org_ids:
        return df.iloc[0:0].copy(), None
    oid_s = df["org_id"].astype(str).str.strip()
    out = df.loc[oid_s.isin(allowed_org_ids)].copy()
    return out, None


def _load_faturamento_file_from_disk(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Ficheiro de faturamento não encontrado: {path}")
    suf = path.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    if suf == ".parquet":
        return pd.read_parquet(path, engine="pyarrow")
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(path, engine="openpyxl")
    raise ValueError(f"Formato não suportado para faturamento materializado: {path.name!r}")


def _load_faturamento_materialized_dataframe(path_s: str, url_s: str) -> pd.DataFrame:
    if path_s:
        path = Path(path_s).expanduser()
        if not path.is_absolute():
            path = (_REPO_APP_ROOT / path).resolve()
        return _load_faturamento_file_from_disk(path)
    errs: list[str] = []
    for dl_url, hdr in _precomputed_download_attempts(url_s.strip()):
        try:
            payload, filename, _lm = _download_file_bytes(
                dl_url,
                extra_headers=hdr or None,
                timeout=PRECOMPUTED_HTTP_TIMEOUT,
                http_retries=1,
            )
            return _dataframe_from_frete_materialized_bytes(payload, filename)
        except Exception as exc:  # noqa: BLE001
            hint = dl_url if len(dl_url) < 100 else dl_url[:97] + "..."
            errs.append(f"{hint} → {exc}")
    raise ValueError(
        "Não foi possível ler o faturamento materializado a partir do URL. "
        + " | ".join(errs[:5])
        + (" …" if len(errs) > 5 else "")
    )


def _faturamento_nf_parquet_path_from_materialized_path(path_s: str) -> Path | None:
    """``dataset_faturamento_nf.parquet`` ao lado do CSV/Parquet linha (mesmo ``current/``)."""
    if not (path_s or "").strip():
        return None
    try:
        p = Path(path_s).expanduser()
        if not p.is_absolute():
            p = (_REPO_APP_ROOT / p).resolve()
        if not p.is_file():
            return None
        cand = p.parent / "dataset_faturamento_nf.parquet"
        return cand if cand.is_file() else None
    except OSError:
        return None


def _faturamento_nf_parquet_stat_token(path_s: str) -> str:
    nf = _faturamento_nf_parquet_path_from_materialized_path(path_s)
    if nf is None:
        return "nf_absent"
    try:
        stt = nf.stat()
        return f"{nf.resolve()}|mtime_ns={stt.st_mtime_ns}|size={stt.st_size}"
    except OSError:
        return "nf_unstat"


def _faturamento_ts_for_path(path: Path) -> str:
    try:
        return _ts_br_from_mtime_ns(int(path.stat().st_mtime_ns))
    except OSError:
        return _now_ts_br_str()


def _load_faturamento_data(
    active_org_id: str,
    *,
    scope_consolidado: bool = False,
    allowed_org_ids: frozenset[str] | None = None,
) -> tuple[pd.DataFrame, dict[str, object], str]:
    """
    Carrega dataset materializado (path/URL explícitos → V2 canônico em disco → V1 irmão do repasse),
    classifica layout (v1/v2) e aplica escopo por ``org_id`` (empresa ativa) ou,
    em modo consolidado, todas as orgs permitidas ao utilizador quando o layout é v2.
    """
    if _faturamento_consume_mode() != "materialized":
        return (
            pd.DataFrame(),
            {
                "faturamento_consume": "unsupported",
                "faturamento_note": "Nesta fase só está disponível FDL_FATURAMENTO_CONSUME_MODE=materialized.",
            },
            _now_ts_br_str(),
        )

    resolved = _faturamento_resolve_materialized_target()
    path_s = resolved["path_s"]
    url_s = resolved["url_s"]
    resolution_source = resolved["resolution_source"]
    path_final = resolved["path_final_resolved"]
    layout_declared = resolved["layout_declared"]

    if not path_s and not url_s:
        slug_h = _materialized_cliente_slug().strip() or "(defina FDL_MATERIALIZED_CLIENTE_SLUG)"
        return (
            pd.DataFrame(),
            {
                "faturamento_consume": "missing_config",
                "faturamento_resolution_source": resolution_source,
                "faturamento_layout_declared": layout_declared,
                "faturamento_path_final_resolved": path_final,
                "faturamento_note": (
                    "Faturamento materializado não encontrado. Opções: "
                    "(1) FDL_FATURAMENTO_MATERIALIZED_PATH ou URL; "
                    "(2) V2: ficheiro em data_products/"
                    f"{slug_h}/faturamento/current/ "
                    "(csv ou parquet), com FDL_FATURAMENTO_DATA_LAYOUT=v2 se usar path explícito; "
                    "(3) V1: repasse materializado irmão em .../<org>/faturamento/current/. "
                    "Com path/URL explícitos use FDL_FATURAMENTO_DATA_LAYOUT=v1 ou v2 em produção."
                ),
            },
            _now_ts_br_str(),
        )

    target = (path_s or url_s)[:500]
    try:
        df0 = _load_faturamento_materialized_dataframe(path_s, url_s)
        n_loaded = int(len(df0))
        layout_effective, layout_note = _faturamento_classify_layout_effective(
            resolution_source=resolution_source,
            layout_declared=layout_declared,
            df=df0,
        )
        aids = allowed_org_ids or frozenset()
        if scope_consolidado and layout_effective == "v2":
            df_scoped, scope_warn = _faturamento_apply_layout_scope_consolidado_v2(
                df0, allowed_org_ids=aids
            )
        else:
            df_scoped, scope_warn = _faturamento_apply_layout_scope(
                df0, layout_effective=layout_effective, org_id=active_org_id
            )
        ts = _now_ts_br_str()
        if path_s:
            try:
                p = _faturamento_resolve_disk_path(path_s)
                if p.is_file():
                    ts = _faturamento_ts_for_path(p)
                    path_final = str(p.resolve())
            except Exception:
                pass
        info: dict[str, object] = {
            "faturamento_consume": "materialized",
            "faturamento_materialized_target": target,
            "faturamento_resolution_source": resolution_source,
            "faturamento_layout_declared": layout_declared,
            "faturamento_data_layout": layout_effective,
            "faturamento_layout_classification": layout_note,
            "faturamento_path_final_resolved": path_final or (path_s or url_s).strip(),
            "faturamento_row_count_loaded": n_loaded,
            "linhas": int(len(df_scoped)),
            "faturamento_nf_first": False,
            "faturamento_nf_df": None,
        }
        if path_s:
            nf_p = _faturamento_nf_parquet_path_from_materialized_path(path_s)
            if nf_p is not None:
                try:
                    df_nf0 = pd.read_parquet(nf_p, engine="pyarrow")
                    if nf_first_contract_dataframe_valid(df_nf0):
                        if scope_consolidado and layout_effective == "v2":
                            df_nf_scoped, _nfw = _faturamento_apply_layout_scope_consolidado_v2(
                                df_nf0, allowed_org_ids=aids
                            )
                        else:
                            df_nf_scoped, _nfw = _faturamento_apply_layout_scope(
                                df_nf0, layout_effective=layout_effective, org_id=active_org_id
                            )
                        info["faturamento_nf_df"] = df_nf_scoped
                        info["faturamento_nf_first"] = True
                        info["faturamento_nf_first_path"] = str(nf_p.resolve())
                        info["faturamento_nf_first_row_count_loaded"] = int(len(df_nf0))
                    else:
                        info["faturamento_nf_first_skip"] = "contract_columns_incompletos"
                except Exception as ex_nf:
                    info["faturamento_nf_first_error"] = str(ex_nf).strip() or ex_nf.__class__.__name__
            else:
                info["faturamento_nf_first_skip"] = "ficheiro_ausente"
        else:
            info["faturamento_nf_first_skip"] = "sem_path_local"
        info.update(_faturamento_materialized_fiscal_audit(df_scoped))
        if resolution_source == "explicit" and not bool(info.get("faturamento_fiscal_join_complete")):
            v2_alt = _faturamento_v2_canonical_dataset_path_str()
            if v2_alt:
                info["faturamento_note_v2_canonical_available"] = (
                    "O path/URL explícito atual não inclui colunas completas do join fiscal "
                    "(ex.: Nota_Data_Emissao, Nota_Valor_Liquido_Rateado). "
                    f"Existe materializado em **{v2_alt}** — remova o path explícito ou aponte para esse ficheiro."
                )
        if scope_warn:
            info["faturamento_scope_note"] = scope_warn
        info["faturamento_escopo"] = (
            FAT_DRE_ESCOPO_CONSOLIDADO
            if scope_consolidado
            else FAT_DRE_ESCOPO_EMPRESA
        )
        if "Status_Custo" in df_scoped.columns:
            _vc = (
                df_scoped["Status_Custo"]
                .astype(str)
                .str.strip()
                .value_counts()
                .to_dict()
            )
            info["faturamento_status_custo_counts"] = _vc
        return (df_scoped, info, ts)
    except Exception as exc:
        return (
            pd.DataFrame(),
            {
                "faturamento_consume": "error",
                "faturamento_materialized_error": str(exc).strip() or exc.__class__.__name__,
                "faturamento_materialized_target": target,
                "faturamento_resolution_source": resolution_source,
                "faturamento_layout_declared": layout_declared,
                "faturamento_path_final_resolved": path_final,
            },
            _now_ts_br_str(),
        )


def _faturamento_materialized_source_stat_token() -> str:
    """
    Inclui mtime/tamanho do ficheiro resolvido para invalidar o cache após rematerialização
    (o path costuma ser o mesmo; sem isto o ``@st.cache_data`` pode servir dados antigos até ao TTL).
    """
    resolved = _faturamento_resolve_materialized_target()
    path_s = (resolved.get("path_s") or "").strip()
    if not path_s:
        return "no_path"
    try:
        p = Path(path_s).expanduser()
        if not p.is_absolute():
            p = (_REPO_APP_ROOT / p).resolve()
        if p.is_file():
            st = p.stat()
            return f"{p.resolve()}|mtime_ns={st.st_mtime_ns}|size={st.st_size}"
    except OSError:
        pass
    return f"unstat:{path_s[:240]}"


def _faturamento_load_cache_signature(
    org_id: str, *, consolidado: bool, allowed_org_ids_key: str
) -> str:
    return "|".join(
        [
            str(org_id),
            "1" if consolidado else "0",
            str(allowed_org_ids_key),
            str(OPERACIONAL_CACHE_REVISION),
            _faturamento_consume_mode(),
            _faturamento_data_layout(),
            str(_materialized_cliente_slug()).strip(),
            str(_faturamento_materialized_path_str()).strip(),
            str(_faturamento_materialized_url_str()).strip(),
            str(_repasse_materialized_path_str()).strip(),
            str(_precomputed_path_str()).strip(),
            str(_strict_materialized()),
            _faturamento_materialized_source_stat_token(),
            _faturamento_nf_parquet_stat_token(
                str(_faturamento_resolve_materialized_target().get("path_s") or "").strip()
            ),
        ]
    )


@st.cache_data(show_spinner=False, ttl=900)
def _load_faturamento_dataframe_cached(
    load_signature: str,
    active_org_id: str,
    consolidado: bool,
    allowed_org_ids_key: str,
) -> tuple[pd.DataFrame, dict[str, object], str]:
    _ = load_signature
    aids = frozenset(
        x.strip() for x in str(allowed_org_ids_key).split(",") if x.strip()
    )
    return _load_faturamento_data(
        active_org_id, scope_consolidado=consolidado, allowed_org_ids=aids
    )


def _derive_frete_materialized_path_from_repasse_anchor(anchor: str) -> str:
    """
    Pipeline `materialize_financeiro`: repasse em .../repasse/current/ e frete em .../frete/current/dataset_frete_app.csv.
    `anchor` pode ser FDL_REPASSE_MATERIALIZED_PATH ou FDL_PRECOMPUTED_PATH nesse layout (muitos clientes só definem precomputed).
    """
    if not (anchor or "").strip():
        return ""
    path = Path(anchor.strip()).expanduser()
    if not path.is_absolute():
        path = (_REPO_APP_ROOT / path).resolve()
    if not path.is_file():
        return ""
    if path.parent.name != "current" or path.parent.parent.name != "repasse":
        return ""
    candidate = path.parent.parent / "frete" / "current" / "dataset_frete_app.csv"
    if candidate.is_file():
        return str(candidate.resolve())
    return ""


def _derive_frete_materialized_path_from_repasse() -> str:
    for anchor in (_repasse_materialized_path_str(), _precomputed_path_str()):
        d = _derive_frete_materialized_path_from_repasse_anchor(anchor)
        if d:
            return d
    return ""


def _frete_materialized_targets() -> tuple[str, str]:
    """
    PATH/URL explícitos do frete (com FDL_FRETE_CONSUME_MODE=materialized);
    se vazios, tenta dataset_frete_app.csv ao lado do CSV do repasse em .../repasse/current/
    (via FDL_REPASSE_MATERIALIZED_PATH ou FDL_PRECOMPUTED_PATH nesse layout).

    Sem FDL_FRETE_MATERIALIZED_*: usa o derivado se repasse materializado, ou se FDL_DATA_SOURCE=precomputed|ready|table
    (o repasse costuma ler a tabela final por FDL_PRECOMPUTED_PATH — o frete alinha ao mesmo padrão).
    """
    mp = _frete_materialized_path_str()
    mu = _frete_materialized_url_str()
    derived = _derive_frete_materialized_path_from_repasse()

    if mp or mu:
        if _frete_consume_mode() == "materialized":
            return mp, mu
        return "", ""

    if not derived:
        return "", ""

    if _frete_consume_mode() == "materialized":
        return derived, ""

    if _repasse_consume_mode() == "materialized":
        return derived, ""

    if _data_source_mode() in {"precomputed", "ready", "table"}:
        return derived, ""

    return "", ""


def _validate_frete_materialized_schema(df: pd.DataFrame) -> None:
    validate_frete_operacional_dataframe(df)


def _dataframe_from_frete_materialized_bytes(payload: bytes, filename: str) -> pd.DataFrame:
    head = payload.lstrip()[:800]
    if head.startswith(b"<") or head.upper().startswith(b"<!DOCTYPE"):
        raise ValueError(
            "Resposta HTML em vez do CSV do frete materializado. Confirme URL/partilha do ficheiro."
        )
    lower = (filename or "").lower()
    if lower.endswith(".csv"):
        return pd.read_csv(BytesIO(payload), sep=None, engine="python", encoding="utf-8-sig")
    if lower.endswith(".xlsx") or lower.endswith(".xls") or zipfile.is_zipfile(BytesIO(payload)):
        return pd.read_excel(BytesIO(payload), engine="openpyxl")
    try:
        return pd.read_csv(BytesIO(payload), sep=None, engine="python", encoding="utf-8-sig")
    except Exception as exc:
        raise ValueError(f"Formato não suportado para frete materializado: {filename!r}") from exc


def _frete_materialized_session_signature(path_s: str, url_s: str) -> str:
    if path_s:
        p = Path(path_s).expanduser()
        if not p.is_absolute():
            p = (_REPO_APP_ROOT / p).resolve()
        if p.is_file():
            return f"mat|p|{p.resolve()}|{int(p.stat().st_mtime_ns)}"
        return f"mat|p|missing|{path_s.strip()[:180]}"
    if url_s:
        return f"mat|u|{url_s.strip()[:240]}"
    return "mat|empty"


def _load_frete_materialized_dataframe(path_s: str, url_s: str) -> pd.DataFrame:
    if path_s:
        path = Path(path_s).expanduser()
        if not path.is_absolute():
            path = (_REPO_APP_ROOT / path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"FDL_FRETE_MATERIALIZED_PATH não encontrado: {path}")
        if path.suffix.lower() not in {".csv", ".xlsx", ".xls"}:
            raise ValueError("FDL_FRETE_MATERIALIZED_PATH deve ser .csv, .xlsx ou .xls")
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
        return pd.read_excel(path, engine="openpyxl")
    errs: list[str] = []
    for dl_url, hdr in _precomputed_download_attempts(url_s.strip()):
        try:
            payload, filename, _lm = _download_file_bytes(
                dl_url,
                extra_headers=hdr or None,
                timeout=PRECOMPUTED_HTTP_TIMEOUT,
                http_retries=1,
            )
            return _dataframe_from_frete_materialized_bytes(payload, filename)
        except Exception as exc:  # noqa: BLE001
            hint = dl_url if len(dl_url) < 100 else dl_url[:97] + "..."
            errs.append(f"{hint} → {exc}")
    raise ValueError(
        "Não foi possível ler o frete materializado a partir do URL. "
        + " | ".join(errs[:5])
        + (" …" if len(errs) > 5 else "")
    )


def _frete_session_cache_is_valid(payload: object) -> bool:
    if not isinstance(payload, dict):
        return False
    if "df_frete" not in payload or "meta_frete" not in payload or "source_signature" not in payload:
        return False
    df_obj = payload.get("df_frete")
    if not isinstance(df_obj, pd.DataFrame):
        return False
    meta_obj = payload.get("meta_frete")
    if not isinstance(meta_obj, dict):
        return False
    return True


def _frete_loader_source_signature(org_id: str, fontes: FontesFrete) -> str:
    if (fontes.vendas_url or "").strip():
        u = (fontes.vendas_url or "").strip()
        vendas_origin = u
        vendas_sig = str(stable_mtime_ns_for_frete_url(fontes.vendas_url))
    else:
        paths = [p for p in fontes.vendas_paths if p.is_file()]
        if not paths and fontes.vendas_path and fontes.vendas_path.is_file():
            paths = [fontes.vendas_path]
        if paths:
            vendas_origin = ";".join(str(p.resolve()) for p in paths)
            pieces = "|".join(f"{p.resolve()}:{int(p.stat().st_mtime_ns)}" for p in paths)
            vendas_sig = hashlib.sha256(pieces.encode("utf-8")).hexdigest()[:24]
        else:
            vendas_origin = ""
            vendas_sig = "none"

    frete_origin = (fontes.frete_url or "").strip() or (
        str(fontes.frete_path.resolve()) if fontes.frete_path else ""
    )
    if (fontes.frete_url or "").strip():
        frete_sig = str(stable_mtime_ns_for_frete_url(fontes.frete_url))
    elif fontes.frete_path and fontes.frete_path.is_file():
        frete_sig = str(int(fontes.frete_path.stat().st_mtime_ns))
    else:
        frete_sig = "none"

    return "|".join(
        [
            f"org={org_id}",
            f"v_origin={vendas_origin}",
            f"v_sig={vendas_sig}",
            f"f_origin={frete_origin}",
            f"f_sig={frete_sig}",
        ]
    )


def _frete_merge_info_from_meta(meta: dict[str, object], **extra: object) -> dict[str, object]:
    out: dict[str, object] = {**meta, **extra}
    out.setdefault("origem", "frete_operacional")
    return out


def _frete_ts_for_path(path: Path) -> str:
    try:
        return _ts_br_from_mtime_ns(int(path.stat().st_mtime_ns))
    except OSError:
        return _now_ts_br_str()


def _frete_ts_live_from_fontes(fontes: FontesFrete) -> str:
    paths = [p for p in fontes.vendas_paths if p.is_file()]
    if not paths and fontes.vendas_path and fontes.vendas_path.is_file():
        paths = [fontes.vendas_path]
    if not paths:
        return _now_ts_br_str()
    try:
        m = max(int(p.stat().st_mtime_ns) for p in paths)
        return _ts_br_from_mtime_ns(m)
    except OSError:
        return _now_ts_br_str()


def _frete_local_vendas_caption(fontes: FontesFrete) -> str | None:
    if fontes.vendas_paths and len(fontes.vendas_paths) > 1:
        head = fontes.vendas_paths[:6]
        tail = "; ".join(str(p.resolve()) for p in head)
        return tail + (" …" if len(fontes.vendas_paths) > 6 else "")
    if fontes.vendas_path:
        return str(fontes.vendas_path.resolve())
    return None


def _load_frete_data_strict_materialized_only(
    org_id: str,
) -> tuple[pd.DataFrame, dict[str, object], str]:
    """
    FDL_STRICT_MATERIALIZED + FDL_FRETE_CONSUME_MODE=materialized: só lê artefato (dataset_frete_app.csv
    via path/URL ou derivado); nunca descobrir_fontes_frete nem carregar_tabela_final_frete_operacional.
    """
    _f_mp_exp = _frete_materialized_path_str()
    _f_mu_exp = _frete_materialized_url_str()
    _f_mp, _f_mu = _frete_materialized_targets()
    _mat_try = bool(_f_mp) or bool(_f_mu)
    _frete_mat_from_repasse_sibling = bool(_f_mp) and not _f_mp_exp and not _f_mu_exp
    _frete_ss_key = f"_frete_cache_{org_id}"

    if not _mat_try:
        raise ValueError(
            "Frete em modo materialized: defina FDL_FRETE_MATERIALIZED_PATH ou FDL_FRETE_MATERIALIZED_URL, "
            "ou dataset_frete_app.csv em .../frete/current/ (derivado do repasse em .../repasse/current/). "
            + _STRICT_MATERIALIZED_USER_MSG
        )

    _sig_mat = _frete_materialized_session_signature(_f_mp, _f_mu)
    _cached_top = st.session_state.get(_frete_ss_key)
    if _cached_top is not None and not _frete_session_cache_is_valid(_cached_top):
        st.session_state.pop(_frete_ss_key, None)
        _cached_top = None

    df_mat: pd.DataFrame | None = None
    meta_mat: dict[str, object] = {}

    if _frete_session_cache_is_valid(_cached_top) and str(_cached_top.get("source_signature", "")) == _sig_mat:
        try:
            df_mat = _cached_top.get("df_frete", pd.DataFrame())
            meta_mat = _cached_top.get("meta_frete", {})
            if not isinstance(df_mat, pd.DataFrame):
                raise TypeError("df_frete inválido em cache")
            if not isinstance(meta_mat, dict):
                meta_mat = {}
        except Exception:
            st.session_state.pop(_frete_ss_key, None)
            df_mat = None

    if df_mat is None:
        try:
            df_mat = _load_frete_materialized_dataframe(_f_mp, _f_mu)
            validate_frete_operacional_dataframe(df_mat)
            df_mat = normalize_frete_status_conc_display(df_mat)
            meta_mat = {
                "vendas_arquivo": "dataset_frete_app.csv (materializado)",
                "frete_arquivo": None,
                "frete_tabular": FRETE_UI_STATUS_CONC in df_mat.columns,
                "debug_logs": [],
                "avisos": [],
                "linhas": int(len(df_mat)),
            }
            _loaded_at = _now_ts_br_str()
            st.session_state[_frete_ss_key] = {
                "df_frete": df_mat,
                "meta_frete": meta_mat,
                "debug_logs": [],
                "loaded_at": _loaded_at,
                "source_signature": _sig_mat,
            }
        except Exception as exc:
            raise ValueError(
                f"{_STRICT_MATERIALIZED_USER_MSG} Não foi possível concluir o carregamento dos dados de frete."
            ) from exc

    df_mat = normalize_frete_status_conc_display(df_mat)
    ts_mat = _now_ts_br_str()
    if _f_mp:
        p = Path(_f_mp).expanduser()
        if not p.is_absolute():
            p = (_REPO_APP_ROOT / p).resolve()
        if p.is_file():
            ts_mat = _frete_ts_for_path(p)
    info_ok = _frete_merge_info_from_meta(
        meta_mat,
        frete_consume="materialized",
        frete_materialized_target=(_f_mp or _f_mu)[:500],
        linhas=len(df_mat),
        frete_mat_from_repasse_sibling=_frete_mat_from_repasse_sibling,
    )
    return df_mat, info_ok, ts_mat


def _load_frete_data(org_id: str) -> tuple[pd.DataFrame, dict[str, object], str]:
    """
    Ponto único de carregamento do Frete (materializado primeiro, live com fallback).
    Devolve DataFrame operacional, info (frete_consume, metadados de loader) e timestamp BR.
    """
    if _strict_materialized() and _frete_consume_mode() == "materialized":
        return _load_frete_data_strict_materialized_only(org_id)

    _f_mp_exp = _frete_materialized_path_str()
    _f_mu_exp = _frete_materialized_url_str()
    _f_mp, _f_mu = _frete_materialized_targets()
    _mat_try = bool(_f_mp) or bool(_f_mu)
    _frete_mat_from_repasse_sibling = bool(_f_mp) and not _f_mp_exp and not _f_mu_exp
    _frete_ss_key = f"_frete_cache_{org_id}"
    mat_load_error: Exception | None = None

    def _empty_info(**k: object) -> dict[str, object]:
        base: dict[str, object] = {"origem": "frete_operacional", "linhas": 0}
        base.update(k)
        return base

    if _mat_try:
        _sig_mat = _frete_materialized_session_signature(_f_mp, _f_mu)
        _cached_top = st.session_state.get(_frete_ss_key)
        if _cached_top is not None and not _frete_session_cache_is_valid(_cached_top):
            st.session_state.pop(_frete_ss_key, None)
            _cached_top = None

        df_mat: pd.DataFrame | None = None
        meta_mat: dict[str, object] = {}

        if _frete_session_cache_is_valid(_cached_top) and str(_cached_top.get("source_signature", "")) == _sig_mat:
            try:
                df_mat = _cached_top.get("df_frete", pd.DataFrame())
                meta_mat = _cached_top.get("meta_frete", {})
                if not isinstance(df_mat, pd.DataFrame):
                    raise TypeError("df_frete inválido em cache")
                if not isinstance(meta_mat, dict):
                    meta_mat = {}
            except Exception:
                st.session_state.pop(_frete_ss_key, None)
                df_mat = None

        if df_mat is None:
            try:
                df_mat = _load_frete_materialized_dataframe(_f_mp, _f_mu)
                validate_frete_operacional_dataframe(df_mat)
                df_mat = normalize_frete_status_conc_display(df_mat)
                meta_mat = {
                    "vendas_arquivo": "dataset_frete_app.csv (materializado)",
                    "frete_arquivo": None,
                    "frete_tabular": FRETE_UI_STATUS_CONC in df_mat.columns,
                    "debug_logs": [],
                    "avisos": [],
                    "linhas": int(len(df_mat)),
                }
                _loaded_at = _now_ts_br_str()
                st.session_state[_frete_ss_key] = {
                    "df_frete": df_mat,
                    "meta_frete": meta_mat,
                    "debug_logs": [],
                    "loaded_at": _loaded_at,
                    "source_signature": _sig_mat,
                }
            except Exception as exc:
                mat_load_error = exc
                df_mat = None

        if df_mat is not None:
            df_mat = normalize_frete_status_conc_display(df_mat)
            ts_mat = _now_ts_br_str()
            if _f_mp:
                p = Path(_f_mp).expanduser()
                if not p.is_absolute():
                    p = (_REPO_APP_ROOT / p).resolve()
                if p.is_file():
                    ts_mat = _frete_ts_for_path(p)
            info_ok = _frete_merge_info_from_meta(
                meta_mat,
                frete_consume="materialized",
                frete_materialized_target=(_f_mp or _f_mu)[:500],
                linhas=len(df_mat),
                frete_mat_from_repasse_sibling=_frete_mat_from_repasse_sibling,
            )
            return df_mat, info_ok, ts_mat

    try:
        fontes = descobrir_fontes_frete()
    except Exception as exc:
        return pd.DataFrame(), _empty_info(frete_consume="error", frete_fontes_error=str(exc)), _now_ts_br_str()

    vendas_ref, v_ns = frete_vendas_loader_args(fontes)

    if not _mat_try and not vendas_ref:
        return (
            pd.DataFrame(),
            _empty_info(frete_consume="empty", frete_no_vendas_source=True),
            _now_ts_br_str(),
        )

    _vu = (fontes.vendas_url or "").strip()
    if _vu and "..." in _vu:
        return (
            pd.DataFrame(),
            _empty_info(frete_consume="empty", frete_placeholder_vendas_url=True),
            _now_ts_br_str(),
        )

    # Live local: carregamento automático (sem botão), alinhado ao repasse — vendas ML + frete anúncio
    # descobertos por descobrir_fontes_frete() sob FDL_BASE_DIR.

    _sig_desired = _frete_loader_source_signature(org_id, fontes)
    _cached = st.session_state.get(_frete_ss_key)
    if _cached is not None and not _frete_session_cache_is_valid(_cached):
        st.session_state.pop(_frete_ss_key, None)
        _cached = None

    if _frete_session_cache_is_valid(_cached) and str(_cached.get("source_signature", "")) == _sig_desired:
        try:
            df_frete = _cached.get("df_frete", pd.DataFrame())
            meta_frete = _cached.get("meta_frete", {})
            if not isinstance(meta_frete, dict):
                meta_frete = {}
            ts_live = _frete_ts_live_from_fontes(fontes)
            consume = "live_fallback" if (_mat_try and mat_load_error is not None) else "live"
            fb_err = str(mat_load_error).strip() if mat_load_error else ""
            t_fallback = ((_f_mp or _f_mu)[:500] if _mat_try else "") or ""
            info_fb = _frete_merge_info_from_meta(
                meta_frete,
                frete_consume=consume,
                linhas=len(df_frete),
                frete_materialized_target=t_fallback,
                frete_materialized_error=fb_err,
            )
            if (fontes.vendas_url or "").strip():
                info_fb["frete_vendas_from_url"] = True
            else:
                cap = _frete_local_vendas_caption(fontes)
                if cap:
                    info_fb["frete_fonte_local_path"] = cap
            if (
                _is_admin_mode()
                and _frete_consume_mode() == "materialized"
                and not _f_mp_exp
                and not _f_mu_exp
                and not _derive_frete_materialized_path_from_repasse()
            ):
                info_fb["frete_mat_note"] = (
                    "Frete: modo **ficheiro consolidado** ativo, mas não há caminho dedicado nem ficheiro ao lado do repasse "
                    "(`.../repasse/current/` → `.../frete/current/`) — em uso **fonte em tempo real**."
                )
            return df_frete, info_fb, ts_live
        except Exception:
            st.session_state.pop(_frete_ss_key, None)

    if not vendas_ref:
        return (
            pd.DataFrame(),
            _empty_info(frete_consume="empty", frete_no_vendas_source=True),
            _now_ts_br_str(),
        )

    try:
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
        df_frete, meta_frete = carregar_tabela_final_frete_operacional(
            org_id, vendas_ref, v_ns, frete_ref, f_ns
        )
    except ValueError as exc:
        return (
            pd.DataFrame(),
            _empty_info(
                frete_consume="error",
                frete_loader_error=str(exc),
                frete_ml_validation_failed=True,
            ),
            _now_ts_br_str(),
        )

    _sig_store = _frete_loader_source_signature(org_id, fontes)
    _loaded_at = _now_ts_br_str()
    st.session_state[_frete_ss_key] = {
        "df_frete": df_frete,
        "meta_frete": meta_frete,
        "debug_logs": list(meta_frete.get("debug_logs") or []),
        "loaded_at": _loaded_at,
        "source_signature": _sig_store,
    }
    ts_live = _frete_ts_live_from_fontes(fontes)
    consume = "live_fallback" if (_mat_try and mat_load_error is not None) else "live"
    fb_err = ""
    if mat_load_error:
        fb_err = str(mat_load_error).strip() or mat_load_error.__class__.__name__
    info_out = _frete_merge_info_from_meta(
        meta_frete,
        frete_consume=consume,
        linhas=len(df_frete),
        frete_materialized_target=((_f_mp or _f_mu)[:500] if _mat_try else ""),
        frete_materialized_error=fb_err,
    )
    if (fontes.vendas_url or "").strip():
        info_out["frete_vendas_from_url"] = True
    else:
        cap = _frete_local_vendas_caption(fontes)
        if cap:
            info_out["frete_fonte_local_path"] = cap
    if (
        _is_admin_mode()
        and _frete_consume_mode() == "materialized"
        and not _f_mp_exp
        and not _f_mu_exp
        and not _derive_frete_materialized_path_from_repasse()
    ):
        info_out["frete_mat_note"] = (
            "Frete: modo **ficheiro consolidado** ativo, mas não há caminho dedicado nem ficheiro ao lado do repasse "
            "(`.../repasse/current/` → `.../frete/current/`) — em uso **fonte em tempo real**."
        )
    return df_frete, info_out, ts_live


def _prepare_uploaded_base(zip_bytes: bytes) -> Path:
    """Extrai pacote ZIP de dados para o BASE_DIR esperado pelo pipeline."""
    base_dir = Path(BASE_DIR)
    expected_dirs = {
        "Vendas - Mercado Livre",
        "Vendas_ML",
        "Liberações_ML",
        "notas_saida",
        "contas_receber",
    }

    if base_dir.exists():
        shutil.rmtree(base_dir)
    base_dir.mkdir(parents=True, exist_ok=True)

    tmp_zip = base_dir / "_upload.zip"
    tmp_zip.write_bytes(zip_bytes)
    with zipfile.ZipFile(tmp_zip, "r") as zf:
        zf.extractall(base_dir)
    tmp_zip.unlink(missing_ok=True)

    # Aceita ZIP com uma pasta raiz extra (ex.: dataset/data/...).
    children = [p for p in base_dir.iterdir() if p.is_dir()]
    if len(children) == 1 and expected_dirs.intersection({p.name for p in children[0].iterdir() if p.is_dir()}):
        nested_root = children[0]
        for item in nested_root.iterdir():
            target = base_dir / item.name
            if target.exists():
                if target.is_dir():
                    shutil.rmtree(target)
                else:
                    target.unlink()
            shutil.move(str(item), str(target))
        nested_root.rmdir()

    return base_dir


def _onedrive_public_url() -> str:
    env_url = os.environ.get("FDL_ONEDRIVE_URL", "").strip()
    if env_url:
        return env_url
    try:
        return str(st.secrets.get("FDL_ONEDRIVE_URL", "")).strip()
    except Exception:
        return ""


def _onedrive_root_folder_name() -> str:
    env_name = os.environ.get("FDL_ONEDRIVE_ROOT_FOLDER", "").strip()
    if env_name:
        return env_name
    try:
        value = str(st.secrets.get("FDL_ONEDRIVE_ROOT_FOLDER", "")).strip()
        if value:
            return value
    except Exception:
        pass
    return "cursor"


def _onedrive_client_folder_name() -> str:
    env_name = os.environ.get("FDL_ONEDRIVE_CLIENT_FOLDER", "").strip()
    if env_name:
        return env_name
    try:
        value = str(st.secrets.get("FDL_ONEDRIVE_CLIENT_FOLDER", "")).strip()
        if value:
            return value
    except Exception:
        pass
    return "cliente_1"


def _microsoft_share_token_from_url(url: str) -> str:
    raw_b64 = base64.b64encode(url.strip().encode("utf-8")).decode("ascii")
    return raw_b64.rstrip("=").replace("+", "-").replace("/", "_")


def _is_m365_sharing_url(url: str) -> bool:
    p = urlparse(url.strip())
    host = p.netloc.lower()
    path_l = (p.path or "").lower()
    if "1drv.ms" in host:
        return True
    if "sharepoint.com" in host or "onedrive.live.com" in host:
        return any(
            marker in path_l
            for marker in (":x:/", ":f:/", ":w:/", ":b:/", ":u:/", ":v:/", ":g:/")
        )
    return False


def _build_onedrive_download_url(public_url: str) -> str:
    parsed = urlparse(public_url.strip())
    host = parsed.netloc.lower()
    path_l = (parsed.path or "").lower()

    def _use_onedrive_shares_api() -> bool:
        if "1drv.ms" in host:
            return True
        if "sharepoint.com" not in host and "onedrive.live.com" not in host:
            return False
        # Links partilhados (Excel :x:/, pasta :f:/, etc.) — ?download=1 costuma devolver HTML, não o binário.
        return any(
            marker in path_l
            for marker in (":x:/", ":f:/", ":w:/", ":b:/", ":u:/", ":v:/", ":g:/")
        )

    if _use_onedrive_shares_api():
        encoded = _microsoft_share_token_from_url(public_url.strip())
        return f"https://api.onedrive.com/v1.0/shares/u!{encoded}/root/content"

    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["download"] = "1"
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            parsed.path,
            parsed.params,
            urlencode(query),
            parsed.fragment,
        )
    )


def _encode_share_token(public_url: str) -> str:
    return base64.urlsafe_b64encode(public_url.encode("utf-8")).decode("utf-8").rstrip("=")


def _graph_bearer_token() -> str:
    env_token = os.environ.get("FDL_MS_GRAPH_TOKEN", "").strip()
    if env_token:
        return env_token
    try:
        return str(st.secrets.get("FDL_MS_GRAPH_TOKEN", "")).strip()
    except Exception:
        return ""


def _download_json(url: str, headers: dict[str, str] | None = None) -> dict[str, object]:
    req_headers = {"User-Agent": "FDL-Streamlit-App/1.0"}
    if headers:
        req_headers.update(headers)
    req = Request(url, headers=req_headers)
    try:
        raw = b""
        for attempt in range(MAX_HTTP_RETRIES + 1):
            try:
                with urlopen(req, timeout=60) as resp:
                    raw = resp.read()
                break
            except HTTPError as exc_retry:
                if exc_retry.code not in RETRYABLE_HTTP_CODES or attempt >= MAX_HTTP_RETRIES:
                    raise
                retry_after = str(exc_retry.headers.get("Retry-After", "")).strip()
                sleep_s = float(retry_after) if retry_after.isdigit() else (1.5**attempt)
                time.sleep(min(max(sleep_s, 0.5), 8.0))
    except HTTPError as exc:
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        body = body.strip().replace("\n", " ")
        body_snippet = body[:320] + ("..." if len(body) > 320 else "")
        raise ValueError(
            f"HTTP {exc.code} ao acessar {url}. Resposta: {body_snippet or '(sem corpo)'}"
        ) from exc
    except URLError as exc:
        raise ValueError(f"Erro de rede ao acessar {url}: {exc.reason}") from exc
    except Exception as exc:
        raise ValueError(f"Falha ao acessar {url}: {exc}") from exc
    data = json.loads(raw.decode("utf-8", errors="replace"))
    if not isinstance(data, dict):
        raise ValueError("Resposta JSON inválida ao listar pasta compartilhada.")
    return data


def _fetch_shared_driveitem_metadata(public_url: str) -> tuple[str, dict[str, object]]:
    token = _encode_share_token(public_url)
    bearer = _graph_bearer_token()
    graph_headers = {"Authorization": f"Bearer {bearer}"} if bearer else None

    candidates = [
        (
            f"https://graph.microsoft.com/v1.0/shares/u!{token}/driveItem",
            graph_headers,
        ),
        (
            f"https://api.onedrive.com/v1.0/shares/u!{token}/root",
            None,
        ),
    ]
    errors: list[str] = []
    for url, headers in candidates:
        try:
            payload = _download_json(url, headers=headers)
            return url, payload
        except Exception as exc:  # pragma: no cover - fallback de conectividade/autorizacao
            errors.append(f"- {url}: {exc}")
    raise ValueError(
        "Não foi possível acessar metadados da pasta compartilhada. "
        "Verifique se o link permite leitura pública ou configure FDL_MS_GRAPH_TOKEN. "
        "Tentativas: "
        + " | ".join(errors)
    )


def _download_shared_folder_dataset(public_url: str) -> None:
    base_dir = Path(BASE_DIR)
    mirror_root = base_dir / "_onedrive_shared"
    target_root_name = _onedrive_root_folder_name()
    target_client_name = _onedrive_client_folder_name()
    mirror_root.mkdir(parents=True, exist_ok=True)

    # Evita sincronização remota completa a cada rerun do Streamlit (ex.: troca de filtro).
    sync_meta_file = base_dir / ".onedrive_sync_meta.json"
    sync_context = {
        "url": public_url,
        "root": target_root_name,
        "client": target_client_name,
    }
    if sync_meta_file.exists():
        try:
            meta = json.loads(sync_meta_file.read_text(encoding="utf-8"))
            last_ts = float(meta.get("synced_at_epoch", 0))
            same_context = (
                str(meta.get("url", "")) == sync_context["url"]
                and str(meta.get("root", "")) == sync_context["root"]
                and str(meta.get("client", "")) == sync_context["client"]
            )
            has_local_data = resolve_pasta_vendas_ml(base_dir).is_dir() and all(
                (base_dir / name).is_dir() for name in REQUIRED_ONEDRIVE_SOURCE_FOLDERS if name != "Vendas - Mercado Livre"
            )
            if same_context and has_local_data and (time.time() - last_ts) < ONEDRIVE_SYNC_MIN_INTERVAL_SECONDS:
                return
        except Exception:
            pass

    share_token = _encode_share_token(public_url)
    root_url, root_meta = _fetch_shared_driveitem_metadata(public_url)
    root_is_graph = "graph.microsoft.com" in root_url.lower()
    root_name = str(root_meta.get("name", "")).strip()
    token = _graph_bearer_token()
    graph_headers = {"Authorization": f"Bearer {token}"} if (token and root_is_graph) else None

    def _children_url(item_url: str) -> str:
        return f"{item_url}/children"

    def _encode_rel_path(*parts: str) -> str:
        clean_parts = [p for p in parts if p]
        return "/".join(quote(p, safe="") for p in clean_parts)

    def _graph_item_url_from_child(child: dict[str, object]) -> str:
        item_id = str(child.get("id", "")).strip()
        parent_ref = child.get("parentReference", {})
        drive_id = ""
        if isinstance(parent_ref, dict):
            drive_id = str(parent_ref.get("driveId", "")).strip()
        if item_id and drive_id:
            return f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{item_id}"
        return ""

    def _download_url(item: dict[str, object]) -> str:
        for k in ("@microsoft.graph.downloadUrl", "@content.downloadUrl"):
            v = str(item.get(k, "")).strip()
            if v:
                return v
        return ""

    def _iter_children(item_url: str) -> list[dict[str, object]]:
        entries: list[dict[str, object]] = []
        next_url = _children_url(item_url)
        while next_url:
            payload = _download_json(next_url, headers=graph_headers)
            chunk = payload.get("value", [])
            if isinstance(chunk, list):
                for entry in chunk:
                    if isinstance(entry, dict):
                        entries.append(entry)
            next_url = str(payload.get("@odata.nextLink", "")).strip()
        return entries

    path_prefix = target_root_name

    use_link_root = target_root_name.strip().lower() in {"", ".", "__link_root__", "link_root"}

    def _locate_cliente_1_url() -> str:
        if use_link_root:
            return root_url
        if root_name == target_root_name:
            return root_url
        if root_name == "cliente_1":
            if root_is_graph:
                return f"https://graph.microsoft.com/v1.0/shares/u!{share_token}/driveItem:/cliente_1:"
            return f"https://api.onedrive.com/v1.0/shares/u!{share_token}/root:/cliente_1:"
        for entry in _iter_children(root_url):
            entry_name = str(entry.get("name", "")).strip()
            if entry_name == target_root_name:
                if root_is_graph:
                    return f"https://graph.microsoft.com/v1.0/shares/u!{share_token}/driveItem:/{target_root_name}:"
                return f"https://api.onedrive.com/v1.0/shares/u!{share_token}/root:/{target_root_name}:"
            if entry_name == "cliente_1":
                # Endpoint compatível com Graph e OneDrive API.
                if root_is_graph:
                    return f"https://graph.microsoft.com/v1.0/shares/u!{share_token}/driveItem:/cliente_1:"
                return f"https://api.onedrive.com/v1.0/shares/u!{share_token}/root:/cliente_1:"
        return ""

    cliente_1_url = _locate_cliente_1_url()
    if not cliente_1_url:
        raise ValueError(
            f"Pasta '{target_root_name}' não encontrada no link compartilhado."
        )
    if use_link_root or root_name == target_root_name:
        path_prefix = ""

    def _norm_for_filter(value: str) -> str:
        s = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode().lower().strip()
        s = s.replace("-", " ").replace("_", " ")
        return " ".join(s.split())

    required_norm_keys = {
        "vendas mercado livre",
        "vendas ml",
        "liberacoes ml",
        "notas saida",
        "nota saida",
        "contas receber",
        "contas a receber",
    }
    target_client_norm = _norm_for_filter(target_client_name)

    def _sync_folder(
        item_url: str,
        dest: Path,
        relative_parts: tuple[str, ...] = (),
        *,
        fast_mode: bool = True,
    ) -> None:
        dest.mkdir(parents=True, exist_ok=True)
        next_url = _children_url(item_url)
        while next_url:
            payload = _download_json(next_url, headers=graph_headers)
            children = payload.get("value", [])
            if not isinstance(children, list):
                break
            for child in children:
                if not isinstance(child, dict):
                    continue
                child_name = str(child.get("name", "")).strip()
                if not child_name:
                    continue
                child_rel = relative_parts + (child_name,)
                child_norm = _norm_for_filter(child_name)
                first_norm = _norm_for_filter(child_rel[0]) if child_rel else ""
                child_path = dest / child_name
                if "folder" in child:
                    if fast_mode:
                        if len(child_rel) == 1 and target_client_norm and child_norm not in {target_client_norm, *required_norm_keys}:
                            continue
                        if (
                            len(child_rel) == 2
                            and target_client_norm
                            and first_norm == target_client_norm
                            and child_norm not in required_norm_keys
                        ):
                            continue
                        if len(child_rel) >= 2 and first_norm not in {target_client_norm, *required_norm_keys}:
                            continue
                    if root_is_graph:
                        child_url = _graph_item_url_from_child(child)
                    else:
                        rel_path = _encode_rel_path(*(((path_prefix,) if path_prefix else ()) + child_rel))
                        child_url = f"https://api.onedrive.com/v1.0/shares/u!{share_token}/root:/{rel_path}:"
                    if not child_url:
                        continue
                    _sync_folder(child_url, child_path, child_rel, fast_mode=fast_mode)
                    continue

                if "file" not in child:
                    continue
                dl_url = _download_url(child)
                if not dl_url:
                    continue
                remote_size = child.get("size")
                if child_path.exists() and child_path.is_file() and isinstance(remote_size, int):
                    try:
                        if child_path.stat().st_size == remote_size:
                            continue
                    except Exception:
                        pass
                content, _, _ = _download_file_bytes(dl_url)
                child_path.parent.mkdir(parents=True, exist_ok=True)
                child_path.write_bytes(content)
            next_url = str(payload.get("@odata.nextLink", "")).strip()

    root_label = target_root_name if target_root_name.strip() else "link_root"
    dataset_root = mirror_root / root_label

    required_aliases: dict[str, tuple[str, ...]] = {
        "Vendas - Mercado Livre": ("vendas mercado livre", "vendas ml"),
        "Liberações_ML": ("liberacoes ml", "liberacoes_ml"),
        "notas_saida": ("notas saida", "nota saida", "notas_saida"),
        "contas_receber": ("contas receber", "contas a receber", "contas_receber"),
    }
    norm_to_required: dict[str, str] = {}
    for required_name, aliases in required_aliases.items():
        norm_to_required[_norm_for_filter(required_name)] = required_name
        for alias in aliases:
            norm_to_required[_norm_for_filter(alias)] = required_name

    def _all_candidate_roots(root: Path) -> list[Path]:
        out: list[Path] = []
        if root.exists() and root.is_dir():
            out.append(root)
            for child in root.iterdir():
                if child.is_dir():
                    out.append(child)
                    for grandchild in child.iterdir():
                        if grandchild.is_dir():
                            out.append(grandchild)
        return out

    def _resolve_required_subfolders(base_path: Path) -> dict[str, Path]:
        resolved: dict[str, Path] = {}
        if not base_path.exists() or not base_path.is_dir():
            return resolved
        for child in base_path.iterdir():
            if not child.is_dir():
                continue
            key = norm_to_required.get(_norm_for_filter(child.name))
            if key and key not in resolved:
                resolved[key] = child
        return resolved

    def _resolve_selected_mapping() -> dict[str, Path]:
        candidate_roots = [dataset_root]
        preferred = dataset_root / target_client_name
        if preferred.exists() and preferred.is_dir():
            candidate_roots.insert(0, preferred)
        for candidate in _all_candidate_roots(dataset_root):
            if candidate not in candidate_roots:
                candidate_roots.append(candidate)

        for candidate in candidate_roots:
            mapping = _resolve_required_subfolders(candidate)
            if len(mapping) == len(REQUIRED_ONEDRIVE_SOURCE_FOLDERS):
                return mapping
        return {}

    # 1) Tenta sincronização rápida (menos chamadas e menos arquivos).
    if dataset_root.exists():
        shutil.rmtree(dataset_root)
    _sync_folder(cliente_1_url, dataset_root, fast_mode=True)
    selected_mapping = _resolve_selected_mapping()

    # 2) Fallback automático: sincronização ampla (compatibilidade com estruturas diferentes).
    if not selected_mapping:
        if dataset_root.exists():
            shutil.rmtree(dataset_root)
        _sync_folder(cliente_1_url, dataset_root, fast_mode=False)
        selected_mapping = _resolve_selected_mapping()

    if not selected_mapping:
        raise ValueError(
            "Estrutura de dados não encontrada na pasta compartilhada. "
            "Defina FDL_ONEDRIVE_CLIENT_FOLDER para o cliente correto dentro de "
            f"'{target_root_name}'."
        )

    for required in REQUIRED_ONEDRIVE_SOURCE_FOLDERS:
        dst = base_dir / required
        src = selected_mapping.get(required)
        if src is None:
            continue
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(src, dst)

    missing: list[str] = []
    vdir = resolve_pasta_vendas_ml(base_dir)
    if not vdir.is_dir() or not any(vdir.rglob("*")):
        missing.append("Vendas - Mercado Livre")
    for name in REQUIRED_ONEDRIVE_SOURCE_FOLDERS:
        if name == "Vendas - Mercado Livre":
            continue
        if not any((base_dir / name).rglob("*")):
            missing.append(name)
    if missing:
        raise ValueError(
            "Link de pasta acessado, mas sem arquivos em: " + ", ".join(sorted(missing))
        )
    sync_meta_file.write_text(
        json.dumps(
            {
                **sync_context,
                "synced_at_epoch": time.time(),
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )


@st.cache_data(show_spinner=False, ttl=900)
def _sync_shared_folder_cached(
    public_url: str,
    root_folder: str,
    client_folder: str,
    _revisao: int = OPERACIONAL_CACHE_REVISION,
) -> str:
    del _revisao
    del root_folder
    del client_folder
    _download_shared_folder_dataset(public_url)
    return _now_ts_br_str()


def _http_get_file_follow_redirects(
    url: str, *, timeout: int = 60, extra_headers: dict[str, str] | None = None
) -> tuple[bytes, str, str, str | None]:
    """GET binário seguindo 301–308 (SharePoint / Graph devolve 308 «User migrated»)."""
    headers: dict[str, str] = {"User-Agent": "FDL-Streamlit-App/1.0"}
    if extra_headers:
        headers.update(extra_headers)
    current = url.strip()
    for _ in range(24):
        req = Request(current, headers=headers)
        try:
            with urlopen(req, timeout=timeout) as resp:
                payload = resp.read()
                final_url = resp.geturl()
                filename = ""
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    filename = cd.split("filename=", 1)[1].strip().strip("\"'")
                if not filename:
                    filename = Path(urlparse(final_url).path).name or "download.bin"
                last_mod = resp.headers.get("Last-Modified")
                return payload, filename, final_url, last_mod
        except HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308) and exc.headers.get("Location"):
                current = urljoin(current, exc.headers["Location"])
                continue
            raise
    raise ValueError("Muitos redirecionamentos ao baixar o ficheiro.")


@st.cache_data(show_spinner=False, ttl=900)
def _download_file_bytes(
    url: str,
    _revisao: int = OPERACIONAL_CACHE_REVISION,
    *,
    extra_headers: dict[str, str] | None = None,
    timeout: int = 60,
    http_retries: int | None = None,
) -> tuple[bytes, str, str | None]:
    del _revisao
    url_original = url.strip()
    max_attempts = MAX_HTTP_RETRIES if http_retries is None else http_retries
    for attempt in range(max_attempts + 1):
        try:
            payload, filename, _final_url, last_modified = _http_get_file_follow_redirects(
                url_original, timeout=timeout, extra_headers=extra_headers
            )
            return payload, filename, last_modified
        except HTTPError as exc:
            if exc.code in RETRYABLE_HTTP_CODES and attempt < max_attempts:
                retry_after = str(exc.headers.get("Retry-After", "")).strip()
                sleep_s = float(retry_after) if retry_after.isdigit() else (1.5**attempt)
                time.sleep(min(max(sleep_s, 0.5), 8.0))
                continue
            body = ""
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            body_snippet = body.strip().replace("\n", " ")
            if len(body_snippet) > 400:
                body_snippet = body_snippet[:400] + "..."
            raise ValueError(
                f"HTTP {exc.code} ao baixar ficheiro (URL de destino pode exigir login ou partilha anónima). "
                f"Resposta: {body_snippet or '(sem corpo)'}"
            ) from exc
        except URLError:
            if attempt >= max_attempts:
                raise
            time.sleep(min(1.5**attempt, 8.0))
    raise RuntimeError(f"Falha ao baixar arquivo após tentativas: {url_original}")


def _sync_payload_zip_to_base(payload: bytes) -> None:
    base_dir = Path(BASE_DIR)
    base_dir.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(payload).hexdigest()
    digest_file = base_dir / ".onedrive_payload.sha256"
    if digest_file.exists() and digest_file.read_text(encoding="utf-8").strip() == digest:
        return
    _prepare_uploaded_base(payload)
    digest_file.write_text(digest, encoding="utf-8")


def _validate_onedrive_csv_schema(df: pd.DataFrame) -> None:
    missing = sorted(REQUIRED_ONEDRIVE_CSV_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(
            "CSV do OneDrive sem colunas obrigatórias: "
            + ", ".join(missing)
        )


def _precomputed_path_str() -> str:
    raw = os.environ.get("FDL_PRECOMPUTED_PATH", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_PRECOMPUTED_PATH", "")).strip()
    except Exception:
        return ""


def _precomputed_url_str() -> str:
    raw = os.environ.get("FDL_PRECOMPUTED_URL", "").strip()
    if raw:
        return raw
    try:
        return str(st.secrets.get("FDL_PRECOMPUTED_URL", "")).strip()
    except Exception:
        return ""


def _precomputed_download_attempts(public_url: str) -> list[tuple[str, dict[str, str]]]:
    """Graph (token) → API shares → URL original (browser) → ?download=1 (muitos :x:/ devolvem 404)."""
    u = public_url.strip()
    attempts: list[tuple[str, dict[str, str]]] = []
    if not _is_m365_sharing_url(u):
        attempts.append((_download_url_for_precomputed_table(u), {}))
        return attempts

    tok = _microsoft_share_token_from_url(u)
    bearer = _graph_bearer_token()
    if bearer:
        attempts.append(
            (
                f"https://graph.microsoft.com/v1.0/shares/u!{tok}/driveItem/content",
                {"Authorization": f"Bearer {bearer}"},
            )
        )
    attempts.append((f"https://api.onedrive.com/v1.0/shares/u!{tok}/root/content", {}))

    # Antes de ?download=1: em muitos links :x:/ o parâmetro download=1 responde 404 «resource cannot be found».
    attempts.append((u, {"User-Agent": _BROWSER_UA_CHROME}))
    attempts.append((u, {}))

    parsed = urlparse(u)
    q = dict(parse_qsl(parsed.query, keep_blank_values=True))
    q["download"] = "1"
    dl_page = urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, urlencode(q), parsed.fragment)
    )
    attempts.append((dl_page, {}))
    attempts.append((dl_page, {"User-Agent": _BROWSER_UA_CHROME}))
    return attempts


def _download_url_for_precomputed_table(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "1drv.ms" in host or "sharepoint.com" in host or "onedrive.live.com" in host:
        return _build_onedrive_download_url(url)
    return url


def _dataframe_from_precomputed_bytes(payload: bytes, filename: str) -> pd.DataFrame:
    head = payload.lstrip()[:800]
    if head.startswith(b"<") or head.upper().startswith(b"<!DOCTYPE"):
        raise ValueError(
            "O servidor devolveu HTML (página de login ou erro), não o ficheiro de dados. "
            "No OneDrive, use partilha «Qualquer pessoa com a ligação pode ver» e teste o link em janela anónima."
        )
    lower = (filename or "").lower()
    if lower.endswith(".csv"):
        return pd.read_csv(BytesIO(payload), sep=None, engine="python", encoding="utf-8-sig")
    if lower.endswith(".xlsx") or lower.endswith(".xls"):
        return pd.read_excel(BytesIO(payload), engine="openpyxl")
    # API shares devolve filename «content»; .xlsx é ZIP (OOXML).
    if zipfile.is_zipfile(BytesIO(payload)):
        return pd.read_excel(BytesIO(payload), engine="openpyxl")
    try:
        return pd.read_csv(BytesIO(payload), sep=None, engine="python", encoding="utf-8-sig")
    except Exception:
        pass
    raise ValueError(f"Formato não suportado: {filename!r}. Use .csv ou .xlsx da conciliação.")


def _finalize_precomputed_df(
    tabela: pd.DataFrame,
    origem_arquivo: str,
    base_label: str,
    *,
    ts: str | None = None,
) -> tuple[pd.DataFrame, dict[str, object], str]:
    _validate_onedrive_csv_schema(tabela)
    if "empresa" not in tabela.columns:
        tabela = tabela.copy()
        tabela["empresa"] = _dataset_empresa_label()
    ts_out = ts if ts is not None else _now_ts_br_str()
    info: dict[str, object] = {
        "base_dir": base_label,
        "linhas": int(len(tabela)),
        "origem": "precomputed",
        "arquivo": origem_arquivo,
    }
    return tabela, info, ts_out


@st.cache_data(show_spinner=False, ttl=180)
def _load_precomputed_from_remote(url: str, _revisao: int = OPERACIONAL_CACHE_REVISION) -> tuple[pd.DataFrame, dict[str, object], str]:
    """Tabela já pronta via URL (ex.: ficheiro no OneDrive). Cache TTL evita download a cada filtro."""
    del _revisao
    errs: list[str] = []
    for dl_url, hdr in _precomputed_download_attempts(url.strip()):
        try:
            payload, filename, last_modified = _download_file_bytes(
                dl_url,
                extra_headers=hdr or None,
                timeout=PRECOMPUTED_HTTP_TIMEOUT,
                http_retries=1,
            )
            tabela = _dataframe_from_precomputed_bytes(payload, filename)
            ts = _ts_br_from_http_last_modified(last_modified) or _now_ts_br_str()
            return _finalize_precomputed_df(tabela, filename, "precomputed_url", ts=ts)
        except Exception as exc:  # noqa: BLE001 — agregamos para mensagem única
            hint = dl_url if len(dl_url) < 120 else dl_url[:117] + "..."
            errs.append(f"{hint} → {exc}")
    raise ValueError(
        "Não foi possível ler a tabela a partir do link. "
        "Em links SharePoint longos (:x:/), o servidor muitas vezes não permite download anónimo "
        "(HTTP 403/404 ou HTML em vez do ficheiro). "
        "Use uma destas opções: (1) `FDL_MS_GRAPH_TOKEN` (Bearer Microsoft Graph com acesso ao ficheiro); "
        "(2) link curto **1drv.ms** para o mesmo ficheiro; (3) alojar o .csv/.xlsx noutro URL público. "
        "Confirme partilha «qualquer pessoa com a ligação pode ver». "
        "Detalhes: " + " | ".join(errs[:5])
        + (" …" if len(errs) > 5 else "")
    )


@st.cache_data(show_spinner=True)
def _load_precomputed_from_disk(
    path_str: str, _mtime_ns: int, _revisao: int = OPERACIONAL_CACHE_REVISION
) -> tuple[pd.DataFrame, dict[str, object], str]:
    del _revisao
    path = Path(path_str)
    tabela = (
        pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
        if path.suffix.lower() == ".csv"
        else pd.read_excel(path, engine="openpyxl")
    )
    return _finalize_precomputed_df(
        tabela, path.name, str(path.parent), ts=_ts_br_from_mtime_ns(_mtime_ns)
    )


def load_precomputed_conciliacao() -> tuple[pd.DataFrame, dict[str, object], str]:
    """
    Lê só o ficheiro já gerado (export Power BI / mirror), sem correr o pipeline.
    Configure FDL_PRECOMPUTED_PATH OU URL (ou link direto para .csv/.xlsx em FDL_ONEDRIVE_URL, sem pasta :f:/).
    """
    path_s = _precomputed_path_str()
    url_s = _precomputed_url_str()
    if not url_s:
        od = _onedrive_public_url()
        if od and ":f:/" not in od.lower():
            url_s = od
    if path_s:
        path = Path(path_s).expanduser()
        if not path.is_absolute():
            path = (_REPO_APP_ROOT / path).resolve()
        if not path.is_file():
            raise FileNotFoundError(f"FDL_PRECOMPUTED_PATH não encontrado: {path}")
        if path.suffix.lower() not in {".csv", ".xlsx", ".xls"}:
            raise ValueError("FDL_PRECOMPUTED_PATH deve ser .csv, .xlsx ou .xls")
        mtime_ns = int(path.stat().st_mtime_ns)
        return _load_precomputed_from_disk(str(path.resolve()), mtime_ns)
    if url_s:
        return _load_precomputed_from_remote(url_s.strip())
    raise ValueError(
        "Modo precomputed: defina FDL_PRECOMPUTED_PATH (ficheiro no servidor) ou "
        "FDL_PRECOMPUTED_URL / link direto para .csv ou .xlsx. "
        "Alternativa: FDL_ONEDRIVE_URL apontando só para o ficheiro (não link de pasta :f:/)."
    )


def load_data_from_onedrive() -> tuple[pd.DataFrame, dict[str, object], str]:
    public_url = _onedrive_public_url()
    if not public_url:
        raise ValueError("FDL_ONEDRIVE_URL não configurada.")

    if ":f:/" in public_url.lower():
        _sync_shared_folder_cached(
            public_url=public_url,
            root_folder=_onedrive_root_folder_name(),
            client_folder=_onedrive_client_folder_name(),
        )
        return carregar_tabela_final_operacional_cache()

    download_url = _build_onedrive_download_url(public_url)
    payload, filename, last_modified = _download_file_bytes(download_url)
    lower_name = filename.lower()

    if lower_name.endswith(".zip") or zipfile.is_zipfile(BytesIO(payload)):
        _sync_payload_zip_to_base(payload)
        return carregar_tabela_final_operacional_cache()

    if lower_name.endswith(".csv"):
        tabela = pd.read_csv(BytesIO(payload), sep=None, engine="python", encoding="utf-8-sig")
        _validate_onedrive_csv_schema(tabela)
        if "empresa" not in tabela.columns:
            tabela = tabela.copy()
            tabela["empresa"] = _dataset_empresa_label()
        ts = _ts_br_from_http_last_modified(last_modified) or _now_ts_br_str()
        info = {
            "base_dir": "onedrive",
            "linhas": int(len(tabela)),
            "origem": "onedrive_csv",
            "arquivo": filename,
        }
        return tabela, info, ts

    raise ValueError(f"Formato não suportado no OneDrive: {filename}. Use ZIP/CSV ou link de pasta compartilhada.")


def _render_cloud_data_loader() -> None:
    with st.sidebar.expander("Admin: atualização de dados", expanded=False):
        st.caption("Uso interno. Não disponibilizar para cliente final.")
        uploaded_zip = st.file_uploader("Base de dados (.zip)", type=["zip"], key="fdl_data_zip")
        if uploaded_zip is not None:
            with st.spinner("Processando base enviada..."):
                _prepare_uploaded_base(uploaded_zip.getvalue())
                st.cache_data.clear()
            st.success("Base atualizada. Recarregando app...")
            st.rerun()


def _load_data_live() -> tuple[pd.DataFrame, dict[str, object], str]:
    source = _data_source_mode()
    if source in {"precomputed", "ready", "table"}:
        return load_precomputed_conciliacao()
    if source in {"filesystem", "onedrive"}:
        return load_data_from_onedrive()
    if source == "api":
        raise NotImplementedError(
            "Origem por API ainda não implementada. "
            "Defina FDL_DATA_SOURCE=onedrive até integrar a API (ex.: Bling)."
        )
    if source == "upload_zip":
        return carregar_tabela_final_operacional_cache()
    raise ValueError(f"FDL_DATA_SOURCE inválido: {source}")


def _load_data() -> tuple[pd.DataFrame, dict[str, object], str]:
    if _repasse_consume_mode() != "materialized":
        return _load_data_live()

    path_s = _repasse_materialized_path_str()
    url_s = _repasse_materialized_url_str()
    if _materialized_path_mode() == "dynamic" and not path_s and not url_s:
        msg = (
            "Repasse em modo FDL_MATERIALIZED_PATH_MODE=dynamic: defina FDL_MATERIALIZED_CLIENTE_SLUG "
            f"(ex.: cliente_2). Esperado: {_materialized_data_products_root().strip()}/<cliente>/{_active_org.org_id}/"
            "repasse/current/dataset_repasse_app.csv"
        )
        if _strict_materialized():
            raise ValueError(msg + " " + _STRICT_MATERIALIZED_USER_MSG)
        tabela, info, ts = _load_data_live()
        if _is_admin_mode():
            info = {
                **info,
                "repasse_consume": "live",
                "repasse_materialized_note": msg,
            }
        return tabela, info, ts

    if not path_s and not url_s:
        if _strict_materialized():
            raise ValueError(
                "Repasse em modo materialized: defina FDL_REPASSE_MATERIALIZED_PATH ou "
                "FDL_REPASSE_MATERIALIZED_URL. "
                + _STRICT_MATERIALIZED_USER_MSG
            )
        tabela, info, ts = _load_data_live()
        if _is_admin_mode():
            info = {
                **info,
                "repasse_consume": "live",
                "repasse_materialized_note": (
                    "Repasse em modo **ficheiro consolidado**, mas os caminhos dedicados estão vazios — "
                    "em uso **fonte em tempo real** (configuração FDL_DATA_SOURCE)."
                ),
            }
        return tabela, info, ts

    target_label = path_s or url_s
    try:
        if path_s:
            path = Path(path_s).expanduser()
            if not path.is_absolute():
                path = (_REPO_APP_ROOT / path).resolve()
            if not path.is_file():
                raise FileNotFoundError(f"FDL_REPASSE_MATERIALIZED_PATH não encontrado: {path}")
            if path.suffix.lower() not in {".csv", ".xlsx", ".xls"}:
                raise ValueError("FDL_REPASSE_MATERIALIZED_PATH deve ser .csv, .xlsx ou .xls")
            mtime_ns = int(path.stat().st_mtime_ns)
            tabela, info, ts = _load_precomputed_from_disk(str(path.resolve()), mtime_ns)
        else:
            tabela, info, ts = _load_precomputed_from_remote(url_s.strip())
        return (
            tabela,
            {
                **info,
                "repasse_consume": "materialized",
                "repasse_materialized_target": target_label[:500],
            },
            ts,
        )
    except Exception as exc:
        if _strict_materialized():
            raise ValueError(
                f"{_STRICT_MATERIALIZED_USER_MSG} Não foi possível concluir o carregamento do repasse."
            ) from exc
        tabela, info, ts = _load_data_live()
        if _is_admin_mode():
            info = {
                **info,
                "repasse_consume": "live_fallback",
                "repasse_materialized_attempted": True,
                "repasse_materialized_target": target_label[:500],
                "repasse_materialized_error": str(exc).strip() or exc.__class__.__name__,
            }
        return tabela, info, ts


def _repasse_load_cache_signature(org_id: str) -> str:
    """Chave de cache do carregamento repasse: muda com org, revisão e caminhos de materialização."""
    return "|".join(
        [
            str(org_id),
            str(OPERACIONAL_CACHE_REVISION),
            _repasse_consume_mode(),
            str(_repasse_materialized_path_str()).strip(),
            str(_repasse_materialized_url_str()).strip(),
            _data_source_mode(),
            str(_strict_materialized()),
        ]
    )


@st.cache_data(show_spinner=False, ttl=900)
def _load_repasse_dataframe_cached(load_signature: str) -> tuple[pd.DataFrame, dict[str, object], str]:
    """Evita reler disco/rede a cada interação com filtros (rerun). `load_signature` isola org/config."""
    _ = load_signature
    return _load_data()



@st.cache_data(show_spinner=True)
def carregar_tabela_final_operacional_cache(
    _revisao: int = OPERACIONAL_CACHE_REVISION,
) -> tuple[pd.DataFrame, dict[str, object], str]:
    del _revisao  # só participa da chave de cache do Streamlit
    ts = _now_ts_br_str()
    tabela, info = carregar_tabela_final_operacional(BASE_DIR)
    return tabela, info, ts


def _serie_numero_nota_valida(s: pd.Series) -> pd.Series:
    """True quando há identificador de NF para conciliar (exclui vazio, None literal, NaN)."""
    x = s.fillna("").astype(str).str.strip()
    lower = x.str.lower()
    return x.ne("") & ~lower.isin({"none", "nan", "nat", "<na>", "null"})


def _col_referencia_como_texto(s: pd.Series) -> pd.Series:
    """Venda / pedido / NF como texto literal (incl. numpy.int64; sem «.0» em IDs float)."""
    def _um(v: object) -> str:
        if v is None:
            return ""
        if isinstance(v, str):
            t = v.strip()
            return "" if t.lower() in {"nan", "none", "nat", "<na>", "null"} else t
        try:
            if pd.isna(v):
                return ""
        except TypeError:
            pass
        if isinstance(v, bool):
            return str(v)
        if isinstance(v, numbers.Integral):
            return str(int(v))
        if isinstance(v, numbers.Real):
            fv = float(v)
            if math.isnan(fv):
                return ""
            iv = int(round(fv))
            if abs(fv - iv) < 1e-9:
                return str(iv)
            t = str(v).strip()
            return "" if t.lower() in {"nan", "none"} else t
        t = str(v).strip()
        if t.lower() in {"nan", "none", "nat", "<na>", "null"}:
            return ""
        if t.endswith(".0") and t.replace(".", "", 1).replace("-", "", 1).isdigit():
            return t[:-2]
        return t

    return s.map(_um).astype("string")


def _excluir_linhas_fora_conciliacao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove linhas sem nota fiscal e taxas ML residuais (ex.: 3,62 / 3,54) típicas de encargos sem NF.

    Se **nenhuma** linha tiver número de nota preenchido (ex.: materialização sem pasta notas_saida),
    não elimina o conjunto inteiro — o cliente ainda precisa da fila operacional por venda/pedido.
    """
    if df.empty:
        return df
    out = df
    if "Número da nota" in out.columns:
        mask_nf = _serie_numero_nota_valida(out["Número da nota"])
        if mask_nf.any():
            out = out.loc[mask_nf].copy()
    if out.empty or "Total BRL" not in out.columns:
        return out
    tb = pd.to_numeric(out["Total BRL"], errors="coerce")
    for fee in (3.62, 3.54):
        out = out.loc[~(tb.sub(fee).abs() < 0.005)].copy()
        tb = pd.to_numeric(out["Total BRL"], errors="coerce")
    return out


def _fmt_brl_ptbr_celula(x: object) -> str:
    """Moeda pt-BR para células de grelha (R$ 1.234,56) — evita estilo US do NumberColumn."""
    if x is None:
        return ""
    try:
        if pd.isna(x):
            return ""
    except TypeError:
        pass
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x).strip()
    if math.isnan(v):
        return ""
    neg = v < 0
    v = abs(v)
    cents = int(round(v * 100 + 1e-9))
    inteiro, cent = divmod(cents, 100)
    int_str = f"{inteiro:,}".replace(",", ".")
    corpo = f"{int_str},{cent:02d}"
    if neg:
        return f"R$ -{corpo}"
    return f"R$ {corpo}"


_FRETE_UI_COL_N_VENDA = "N.º venda"


def _fmt_int_ptbr(n: int) -> str:
    """Quantidade com separador de milhar pt-BR."""
    return f"{int(n):,}".replace(",", ".")


def _fmt_pct_ptbr_1(x: object) -> str:
    """Percentual pt-BR com uma casa decimal (ex.: 12,3%). ``x`` já em escala 0–100."""
    if x is None:
        return "—"
    try:
        if pd.isna(x):
            return "—"
    except TypeError:
        pass
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "—"
    if math.isnan(v):
        return "—"
    return f"{v:.1f}".replace(".", ",") + "%"


def _comercial_fmt_qtd_display(x: object) -> str:
    """Quantidade para tabela comercial: inteiro com milhar pt-BR; senão uma casa decimal."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return "—"
    try:
        if pd.isna(v):
            return "—"
    except TypeError:
        pass
    if math.isnan(v):
        return "—"
    if abs(v - round(v)) < 1e-6:
        return _fmt_int_ptbr(int(round(v)))
    return f"{v:.1f}".replace(".", ",")


def _format_frete_anuncio_tabela_display(df: pd.DataFrame) -> pd.DataFrame:
    """Formata moeda pt-BR, quantidades com milhar e mantém «Recebido?» como texto."""
    if df.empty:
        return df
    out = df.copy()
    for c in ("Valor total (R$)", "Impacto (R$)"):
        if c in out.columns:
            out[c] = out[c].map(lambda x: _fmt_brl_ptbr_celula(x) if pd.notna(x) and x != "" else "")
    if "Qtde ocorrências" in out.columns:
        out["Qtde ocorrências"] = out["Qtde ocorrências"].map(
            lambda n: _fmt_int_ptbr(int(n)) if pd.notna(n) else ""
        )
    return out


# Texto exibido na coluna «Ação sugerida» da Fila operacional (UI apenas; export mantém valores canónicos).
_REPASSE_ACAO_SUGERIDA_EXIBICAO: dict[str, str] = {
    "Ok": "✅ OK",
    "Baixar no Bling": "⬇️ Baixar no Bling",
    "Baixado": "✅ Baixado",
    "Analisar diferença": "🔍 Analisar diferença",
    "Verificar recebimento": "📥 Verificar recebimento",
    "Verificar faturamento": "📄 Verificar faturamento",
    "Revisar venda zerada": "⚠️ Revisar venda zerada",
}


def _repasse_format_situacao_exibicao(val: object) -> str:
    """Prefixos visuais para «Situação» na grelha (UI; export permanece canónico)."""
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    s = str(val).strip()
    if s.lower() in {"nan", "none", "<na>", "nat"}:
        return ""
    low = s.lower()
    if "diverg" in low:
        return f"🔍 {s}"
    if "atrasad" in low:
        return f"🔴 {s}"
    if "vencendo" in low and "hoje" in low:
        return f"🟡 {s}"
    if "vencendo" in low:
        return f"🟡 {s}"
    if "em dia" in low or low == "em dia":
        return f"✅ {s}"
    return s


def _repasse_format_acao_sugerida_exibicao(val: object) -> str:
    if val is None:
        return ""
    try:
        if pd.isna(val):
            return ""
    except TypeError:
        pass
    s = str(val).strip()
    if s.lower() in {"nan", "none", "<na>", "nat"}:
        return ""
    return _REPASSE_ACAO_SUGERIDA_EXIBICAO.get(s, s)


def _dataframe_conciliacao_somente_grid(df: pd.DataFrame) -> pd.DataFrame:
    """Cópia para st.dataframe: moeda em texto pt-BR e «Ação sugerida» com ícones (export continua canónico)."""
    if df.empty:
        return df
    g = df.copy()
    for c in ("Valor da nota", "Valor a receber", "Valor pago", "Diferença"):
        if c in g.columns:
            g[c] = g[c].map(_fmt_brl_ptbr_celula).astype(object)
    if "Ação sugerida" in g.columns:
        g["Ação sugerida"] = g["Ação sugerida"].map(_repasse_format_acao_sugerida_exibicao).astype(object)
    if "Situação" in g.columns:
        g["Situação"] = g["Situação"].map(_repasse_format_situacao_exibicao).astype(object)
    return g




def _column_config_conciliacao(
    df: pd.DataFrame, *, moeda_como_texto: bool = False
) -> dict[str, NumberColumn | DatetimeColumn | TextColumn]:
    """Moeda em coluna numérica (export) ou texto pt-BR (grid). Referências sempre TextColumn."""
    cfg: dict[str, NumberColumn | DatetimeColumn | TextColumn] = {}
    for c in ("Valor da nota", "Valor a receber", "Valor pago", "Diferença"):
        if c in df.columns:
            if moeda_como_texto:
                cfg[c] = TextColumn(c, width="medium")
            else:
                cfg[c] = NumberColumn(c, format="R$ %,.2f")
    for c in ("Número da venda", "Número do pedido", "Número da nota"):
        if c in df.columns:
            cfg[c] = TextColumn(c, width="medium")
    if "Data de emissão" in df.columns:
        cfg["Data de emissão"] = DatetimeColumn("Data de emissão", format="DD/MM/YYYY", width="small")
    if "Data de pagamento" in df.columns:
        cfg["Data de pagamento"] = DatetimeColumn("Data de pagamento", format="DD/MM/YYYY HH:mm", width="medium")
    if "Situação" in df.columns:
        cfg["Situação"] = TextColumn("Situação", width="medium")
    if "Ação sugerida" in df.columns:
        cfg["Ação sugerida"] = TextColumn("Ação sugerida", width="large")
    return cfg


def _multiselect_stable(
    key: str, label: str, options: list[str], *, compact_label: bool = False
) -> list[str]:
    """
    Evita `default=` com listas recém-ordenadas a cada rerun (perdia estado / ecrã em branco).
    Estado inicial vazio: o cliente abre a seta e escolhe; vazio = sem filtro nessa dimensão.
    Se o utilizador limpar o último chip, o valor mantém-se vazio — não repor «todas as opções».
    """
    opts = [x for x in options if str(x).strip()]
    if not opts:
        if key not in st.session_state:
            st.session_state[key] = []
        return []
    if key not in st.session_state:
        st.session_state[key] = []
    else:
        prev = st.session_state[key]
        if not isinstance(prev, list):
            st.session_state[key] = []
        else:
            st.session_state[key] = [x for x in prev if x in opts]
    if compact_label:
        st.caption(label)
        return st.multiselect(
            " ",
            opts,
            key=key,
            placeholder="Todos",
            label_visibility="collapsed",
        )
    return st.multiselect(label, opts, key=key, placeholder="Escolher…")


def _faturamento_divergencia_tol() -> float:
    try:
        from processing.faturamento.config import DIVERGENCIA_VALOR_TOL

        return float(DIVERGENCIA_VALOR_TOL)
    except Exception:
        return 0.01


def _faturamento_resolve_produto_column(columns: list[str]) -> str | None:
    for c in ("Descrição", "Produto", "Nome do produto", "Título", "Título do anúncio", "Nome"):
        if c in columns:
            return c
    return None


def _faturamento_painel_custo_produto_col(columns: list[str]) -> str | None:
    """Materializado V2 usa ``Custo_Produto_Total``; legado pode usar ``Custo do Produto``."""
    if "Custo_Produto_Total" in columns:
        return "Custo_Produto_Total"
    if "Custo do Produto" in columns:
        return "Custo do Produto"
    return None


def _faturamento_painel_receita_series(df: pd.DataFrame, pl_col: str) -> pd.Series:
    """Soma de receita para KPIs: ``Vl_Venda`` (Qtd×lista), senão ``Receita_Bruta``, senão preço×qtd."""
    if "Vl_Venda" in df.columns:
        return pd.to_numeric(df["Vl_Venda"], errors="coerce")
    if "Receita_Bruta" in df.columns:
        return pd.to_numeric(df["Receita_Bruta"], errors="coerce")
    if "Quantidade" in df.columns and pl_col in df.columns:
        return pd.to_numeric(df[pl_col], errors="coerce") * pd.to_numeric(df["Quantidade"], errors="coerce")
    if pl_col in df.columns:
        return pd.to_numeric(df[pl_col], errors="coerce")
    return pd.Series(float("nan"), index=df.index, dtype=float)


def _faturamento_painel_missing_schema_columns(df: pd.DataFrame) -> list[str]:
    """Colunas mínimas para o painel; custo aceita alias V2."""
    c = set(df.columns)
    miss: list[str] = []
    for col in (
        "Preço de lista",
        "Valor total",
        "Resultado",
        "Situação",
        "Nome da plataforma",
        "Código",
        "Número do pedido",
        "Número do pedido multiloja",
        "Existe Nota Fiscal gerada",
        "Número da nota",
        "Custo de Frete",
        "Taxa de Comissão",
        "Imposto",
        "Despesas Fixas",
    ):
        if col not in c:
            miss.append(col)
    if _faturamento_painel_custo_produto_col(list(df.columns)) is None:
        miss.append("Custo_Produto_Total ou Custo do Produto")
    return miss


def _faturamento_num_col(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        return pd.Series(0.0, index=df.index, dtype=float)
    return pd.to_numeric(df[col], errors="coerce").fillna(0.0)


def _faturamento_pedido_display_series(df: pd.DataFrame) -> pd.Series:
    """Texto único para coluna «Pedido»: multiloja se preenchido, senão n.º do pedido."""
    ml = df["Número do pedido multiloja"].fillna("").astype(str).str.strip()
    ped = df["Número do pedido"].fillna("").astype(str).str.strip()
    core = ml.mask(ml.eq(""), ped)
    return _faturamento_disp_texto_sem_none(core.astype(str))


def _faturamento_pedido_id_series(df: pd.DataFrame) -> pd.Series:
    """
    Chave estável por linha para contagem distinta de pedidos.
    Usa multiloja quando preenchido; caso contrário ``Número do pedido``.
    Com ``org_id`` (v2 / consolidado), prefixa a org para evitar colisões entre empresas.
    """
    ml = df["Número do pedido multiloja"].fillna("").astype(str).str.strip()
    ped = df["Número do pedido"].fillna("").astype(str).str.strip()
    core = ml.mask(ml.eq(""), ped)
    if "org_id" in df.columns:
        oid = df["org_id"].fillna("").astype(str).str.strip()
        return oid + "|" + core
    return core


def _faturamento_atendido_mask(df: pd.DataFrame) -> pd.Series:
    """Alinhado a ``apply_faturamento_flags``: situação normalizada == «atendido»."""
    if "Situação" not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    situ = df["Situação"].fillna("").astype(str).str.strip().str.casefold()
    return situ.eq("atendido")


def _faturamento_admin_metadata_rowcount_message(path_final: str, row_count_loaded: int) -> str:
    """
    Compara ``faturamento_row_count_loaded`` com ``metadata.json`` na pasta ``current/`` (materializador).
    Ajuda a detetar cache antigo ou ficheiro errado sem hardcodar contagens por cliente.
    """
    raw = (path_final or "").strip()
    if not raw:
        return "Sem path resolvido — nada a contrastar com metadata."
    try:
        p = Path(raw).expanduser()
        if not p.is_absolute():
            p = (_REPO_APP_ROOT / p).resolve()
        else:
            p = p.resolve()
    except Exception:
        return "Path resolvido inválido para leitura de metadata."
    if not p.is_file():
        return f"Ficheiro não existe no disco: `{p}`"
    meta_path = p.parent / "metadata.json"
    if not meta_path.is_file():
        return f"**metadata.json** ausente em `{p.parent}` — confirme manualmente o artefato."
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        rc = meta.get("row_count")
        try:
            rc_i = int(rc) if rc is not None else None
        except (TypeError, ValueError):
            rc_i = None
    except Exception as exc:
        return f"**metadata.json** não legível: `{exc}`"
    if rc_i is None:
        return "**metadata.json** sem `row_count` numérico — confirme manualmente."
    if rc_i == int(row_count_loaded):
        return f"**OK:** `metadata.json` `row_count` = **{rc_i}** coincide com **faturamento_row_count_loaded**."
    return (
        f"**ATENÇÃO:** `metadata.json` indica **row_count={rc_i}** mas o carregamento reportou **{row_count_loaded}** linhas. "
        "Possíveis causas: **cache Streamlit** (até TTL), ficheiro substituído após arranque, ou leitura de outro path."
    )


def _faturamento_materialized_fiscal_audit(df: pd.DataFrame) -> dict[str, object]:
    """Metadados sobre colunas fiscais do join notas (materializado V2)."""
    want = (
        "Nota_Data_Emissao",
        "Nota_Valor_Liquido_Rateado",
        "Nota_Valor_Liquido_Total",
        "Nota_Situacao",
        "Nota_Numero_Normalizado",
        "faturamento_nota_vinculada",
    )
    present = [c for c in want if c in df.columns]
    return {
        "faturamento_fiscal_columns_present": present,
        "faturamento_fiscal_join_complete": bool(
            "Nota_Data_Emissao" in df.columns and "Nota_Valor_Liquido_Rateado" in df.columns
        ),
    }


def _faturamento_agg_recorte(df: pd.DataFrame) -> dict[str, Any]:
    """
    Agregações numéricas do recorte (Visão Geral, KPIs). Independente de Streamlit.

    Definições do módulo (totais no recorte):

    * **Receita bruta (venda comercial)** = Σ ``Vl_Venda`` se existir; senão Σ ``Receita_Bruta`` / preço×qtd.
    * **Valor Nota Fiscal** (KPI) = Σ ``Nota_Valor_Liquido_Rateado`` quando o materializado traz join fiscal; senão
      Σ ``Valor total`` (pedido) ou fallback RB − desconto.
    * **Desconto comercial** = receita bruta − Σ ``Valor total`` quando existe; senão Σ ``Desconto proporcional total``.

    O **resultado** soma só valores numéricos de ``Resultado`` (linhas sem custo OK ficam vazias no materializado).

    ``diag_plug_rb_desc_vt``: Σ RB − Σ Desconto − Σ VT (só diagnóstico; ~0 em dados consistentes).
    """
    out: dict[str, Any] = {
        "n_linhas": int(len(df)),
        "receita_bruta": 0.0,
        "desconto_comercial": 0.0,
        "receita_liquida": 0.0,
        "custo_produto": 0.0,
        "frete": 0.0,
        "frete_me": 0.0,
        "frete_tp": 0.0,
        "comissao_plataforma": 0.0,
        "imposto": 0.0,
        "despesas_fixas": 0.0,
        "outras_despesas": 0.0,
        "resultado": 0.0,
        "margem_principal_pct": float("nan"),
        "pedidos_atendidos_distintos": 0,
        "n_linhas_sem_custo_ok": 0,
        "diag_plug_rb_desc_vt": None,
    }
    if df.empty:
        return out
    pl_col = "Preço de lista"
    rb_s = _faturamento_painel_receita_series(df, pl_col).fillna(0.0)
    out["receita_bruta"] = float(rb_s.sum())
    desc_col = "Desconto proporcional total"
    has_vt = "Valor total" in df.columns
    has_desc = desc_col in df.columns
    has_nvlr = "Nota_Valor_Liquido_Rateado" in df.columns
    vt_sum = float(_faturamento_num_col(df, "Valor total").sum()) if has_vt else None
    desc_sum = float(_faturamento_num_col(df, desc_col).sum()) if has_desc else None

    if vt_sum is not None:
        out["desconto_comercial"] = float(out["receita_bruta"] - vt_sum)
    elif desc_sum is not None:
        out["desconto_comercial"] = desc_sum
    else:
        out["desconto_comercial"] = 0.0

    if has_nvlr:
        out["receita_liquida"] = float(
            pd.to_numeric(df["Nota_Valor_Liquido_Rateado"], errors="coerce").fillna(0.0).sum()
        )
    elif vt_sum is not None:
        out["receita_liquida"] = vt_sum
    elif desc_sum is not None:
        out["receita_liquida"] = float(out["receita_bruta"] - desc_sum)
    else:
        out["receita_liquida"] = float(out["receita_bruta"])

    if has_vt and has_desc and vt_sum is not None and desc_sum is not None:
        out["diag_plug_rb_desc_vt"] = float(out["receita_bruta"] - desc_sum - vt_sum)
    ccol = _faturamento_painel_custo_produto_col(list(df.columns))
    if ccol and ccol in df.columns:
        out["custo_produto"] = float(pd.to_numeric(df[ccol], errors="coerce").fillna(0.0).sum())
    if "Frete_Plataforma" in df.columns:
        out["frete"] = float(_faturamento_num_col(df, "Frete_Plataforma").sum())
    else:
        out["frete"] = float(_faturamento_num_col(df, "Custo de Frete").sum())
    if "Frete Mercado Envios" in df.columns and "Frete transportadora própria" in df.columns:
        out["frete_me"] = float(_faturamento_num_col(df, "Frete Mercado Envios").sum())
        out["frete_tp"] = float(_faturamento_num_col(df, "Frete transportadora própria").sum())
    else:
        out["frete_me"] = float(out["frete"])
        out["frete_tp"] = 0.0
    out["comissao_plataforma"] = float(_faturamento_num_col(df, "Taxa de Comissão").sum())
    out["imposto"] = float(_faturamento_num_col(df, "Imposto").sum())
    out["despesas_fixas"] = float(_faturamento_num_col(df, "Despesas Fixas").sum())
    if "Outras Despesas" in df.columns:
        out["outras_despesas"] = float(_faturamento_num_col(df, "Outras Despesas").sum())
    res_s = pd.to_numeric(df["Resultado"], errors="coerce") if "Resultado" in df.columns else pd.Series(
        dtype=float
    )
    out["resultado"] = float(res_s.sum(skipna=True))
    rb = float(out["receita_bruta"])
    if rb not in (0.0, -0.0) and not math.isnan(rb):
        out["margem_principal_pct"] = float(out["resultado"] / rb)
    m_at = _faturamento_atendido_mask(df)
    if m_at.any():
        pids = _faturamento_pedido_id_series(df.loc[m_at]).astype(str).str.strip()
        pids = pids[pids.ne("")]
        out["pedidos_atendidos_distintos"] = int(pids.nunique()) if len(pids) else 0
    try:
        from processing.faturamento.config import STATUS_CUSTO_OK
    except Exception:
        STATUS_CUSTO_OK = "CUSTO_OK"
    if "Status_Custo" in df.columns:
        sc = df["Status_Custo"].astype(str).str.strip()
        out["n_linhas_sem_custo_ok"] = int((~sc.eq(STATUS_CUSTO_OK)).sum())
    return out


def _faturamento_visao_geral_chart_por_plataforma(df: pd.DataFrame) -> pd.DataFrame:
    """Agregação por plataforma (receita bruta); usada na tabela-resumo da Visão Geral."""
    if df.empty or "Nome da plataforma" not in df.columns:
        return pd.DataFrame()
    pl_col = "Preço de lista"
    rb = _faturamento_painel_receita_series(df, pl_col).fillna(0.0)
    plat = (
        df["Nome da plataforma"]
        .fillna("")
        .astype(str)
        .str.strip()
        .replace("", "(sem plataforma)")
    )
    g = (
        pd.DataFrame({"_rb": rb, "_plat": plat})
        .groupby("_plat", sort=False)["_rb"]
        .sum()
        .sort_values(ascending=False)
        .head(12)
    )
    return pd.DataFrame({"Plataforma": g.index.astype(str), "Receita bruta": g.values})


def _faturamento_visao_geral_tabela_plataforma(df: pd.DataFrame) -> pd.DataFrame:
    """Tabela-resumo por plataforma: receita bruta e participação % (mais legível que barras no MVP)."""
    base = _faturamento_visao_geral_chart_por_plataforma(df)
    if base.empty:
        return base
    tot = float(base["Receita bruta"].sum())
    base = base.copy()
    if tot not in (0.0, -0.0):
        prc = (base["Receita bruta"] / tot * 100.0).round(1)
        base["Part. do recorte"] = prc.map(
            lambda x: f"{x:.1f} %".replace(".", ",") if pd.notna(x) else "—"
        )
    else:
        base["Part. do recorte"] = "—"
    return base


def _render_faturamento_dre_visao_geral(df: pd.DataFrame, load_info: dict[str, object]) -> None:
    """Bloco MVP Visão Geral — mesmo recorte global que o painel detalhado."""
    st.subheader("Visão geral")
    escopo = str(load_info.get("faturamento_escopo", "") or "").strip()
    escopo_lbl = (
        "Consolidado (orgs permitidas)"
        if escopo == FAT_DRE_ESCOPO_CONSOLIDADO
        else "Empresa ativa"
        if escopo == FAT_DRE_ESCOPO_EMPRESA
        else escopo or "—"
    )
    st.caption(
        f"Mesmo **recorte global** que o detalhamento (antes de **Visão** / alertas / busca no bloco abaixo) · "
        f"escopo de carga: **{escopo_lbl}**."
    )
    _fdl_ui_gap_tight()
    if df.empty:
        st.info(
            "Sem dados para o resumo executivo. Confirme a **carga** de faturamento e o **recorte** acima quando existir base."
        )
        return
    if _faturamento_painel_missing_schema_columns(df):
        if _is_admin_mode():
            st.warning(
                "Visão geral indisponível: faltam colunas esperadas no ficheiro de faturamento. "
                "Verifique o materializado (layout V2)."
            )
        else:
            st.warning("Dados insuficientes para a Visão geral. Contacte o suporte.")
        return
    agg = _faturamento_agg_recorte(df)
    n_lin = int(agg["n_linhas"])
    n_sem = int(agg["n_linhas_sem_custo_ok"])
    if n_lin == 0:
        st.info("Sem linhas neste recorte — alargue período, situação ou plataforma.")
        return

    with st.container(border=True):
        st.caption("**Indicadores principais**")
        if "Nota_Valor_Liquido_Rateado" in df.columns:
            st.caption(
                f"**{_FATURAMENTO_UI_VALOR_NOTA_FISCAL}** = soma de **Nota_Valor_Liquido_Rateado** "
                "(valor líquido da nota de saída, rateado por linha; join pedidos↔notas)."
            )
        else:
            st.caption(
                f"**{_FATURAMENTO_UI_VALOR_NOTA_FISCAL}** = soma de **Valor total** do pedido (materializado sem coluna fiscal de nota; reprocesse V2 com notas)."
            )
        rp = st.columns(5)
        with rp[0]:
            st.metric("Receita bruta", _fmt_brl_ptbr_celula(agg["receita_bruta"]))
        with rp[1]:
            st.metric(_FATURAMENTO_UI_VALOR_NOTA_FISCAL, _fmt_brl_ptbr_celula(agg["receita_liquida"]))
        with rp[2]:
            st.metric("Resultado", _fmt_brl_ptbr_celula(agg["resultado"]))
        with rp[3]:
            mp = agg["margem_principal_pct"]
            if isinstance(mp, float) and not math.isnan(mp):
                st.metric("Margem principal", f"{mp * 100:.2f}%".replace(".", ","))
            else:
                st.metric("Margem principal", "—")
        with rp[4]:
            st.metric("Pedidos atendidos", _fmt_int_ptbr(agg["pedidos_atendidos_distintos"]))

    _n_ok_custo_vg = max(0, n_lin - n_sem)
    if n_sem > 0:
        ratio = (n_sem / n_lin) if n_lin else 0.0
        _msg_custo = (
            f"**{_n_ok_custo_vg}** linhas com custo alocado; **{n_sem}** exceções sem custo.\n\n"
            "O resultado total considera apenas linhas com custo."
        )
        if ratio > 0.10:
            st.info(_msg_custo)
        else:
            st.caption(_msg_custo)
    elif n_lin > 0:
        st.caption(
            f"**{_n_ok_custo_vg}** linhas com custo alocado. O resultado total considera apenas linhas com custo."
        )

    with st.expander("Outras métricas e definições", expanded=False):
        _n_ic = (
            int(_faturamento_series_bool_mask(df["faturamento_consolidado"]).sum())
            if "faturamento_consolidado" in df.columns
            else 0
        )
        st.metric("Itens consolidados (visão NF)", _fmt_int_ptbr(_n_ic))
        st.caption("Contagem de linhas em visão consolidada de NF (filtros do detalhamento).")
        o1 = st.columns(4)
        with o1[0]:
            st.metric("Desconto comercial", _fmt_brl_ptbr_celula(agg["desconto_comercial"]))
        with o1[1]:
            st.metric("Linhas no recorte", _fmt_int_ptbr(n_lin))
        with o1[2]:
            st.metric("Linhas sem custo OK", _fmt_int_ptbr(n_sem))
        with o1[3]:
            st.metric("Custo do produto", _fmt_brl_ptbr_celula(agg["custo_produto"]))
        o2 = st.columns(4)
        with o2[0]:
            if "Frete Mercado Envios" in df.columns and "Frete transportadora própria" in df.columns:
                st.metric("Frete (Mercado Envios)", _fmt_brl_ptbr_celula(agg["frete_me"]))
                st.metric("Frete (transp. própria)", _fmt_brl_ptbr_celula(agg["frete_tp"]))
            else:
                st.metric("Frete", _fmt_brl_ptbr_celula(agg["frete"]))
        with o2[1]:
            st.metric("Comissão plataforma", _fmt_brl_ptbr_celula(agg["comissao_plataforma"]))
        with o2[2]:
            st.metric("Imposto", _fmt_brl_ptbr_celula(agg["imposto"]))
        with o2[3]:
            st.metric("Despesas fixas", _fmt_brl_ptbr_celula(agg["despesas_fixas"]))
        o3 = st.columns(2)
        with o3[0]:
            if "Outras Despesas" in df.columns:
                st.metric("Outras despesas", _fmt_brl_ptbr_celula(agg["outras_despesas"]))
            else:
                st.metric("Outras despesas", "—")
        st.caption(
            "**Definições:** receita bruta = Σ **Vl_Venda** / **Receita_Bruta** conforme materializado. "
            "**Desconto comercial** = receita bruta − Σ **Valor total** (quando existir). "
            "**Valor Nota Fiscal** (este bloco) = Σ **Nota_Valor_Liquido_Rateado** com join fiscal; sem essa coluna, o KPI usa Σ **Valor total**."
            " **NF vazia** no fluxo ML: esperado sem vínculo."
        )
        _plug = agg.get("diag_plug_rb_desc_vt")
        if (
            _plug is not None
            and _is_admin_mode()
            and abs(float(_plug)) > max(1.0, 0.001 * max(n_lin, 1))
        ):
            st.caption(
                f"**Admin:** Σ RB − Σ Desconto prop. − Σ Valor total = **{_plug:,.2f}** (esperado ~0)."
            )

    tpm = _faturamento_visao_geral_tabela_plataforma(df)
    if not tpm.empty:
        st.caption("**Por plataforma** (top 12 · receita bruta e peso no recorte).")
        _tc_pl: dict[str, NumberColumn | TextColumn] = {
            "Plataforma": TextColumn("Plataforma", width="medium"),
            "Receita bruta": NumberColumn("Receita bruta", format="R$ %,.2f"),
            "Part. do recorte": TextColumn("Part. do recorte", width="small"),
        }
        _h_tbl = min(300, 52 + 34 * len(tpm))
        st.dataframe(
            tpm,
            use_container_width=True,
            hide_index=True,
            height=_h_tbl,
            column_config=_tc_pl,
        )

    _fdl_ui_gap_tight()


def _faturamento_dre_etiquetas_empresa_recorte(df: pd.DataFrame) -> list[str]:
    """Rótulos únicos de empresa no recorte (coluna ``empresa``; senão ``org_id``)."""
    if df.empty:
        return []
    if "empresa" in df.columns:
        u = sorted({str(x).strip() for x in df["empresa"].dropna().unique() if str(x).strip()})
        if u:
            return u
    if "org_id" in df.columns:
        return sorted({str(x).strip() for x in df["org_id"].dropna().unique() if str(x).strip()})
    return []


def _faturamento_dre_filtrar_por_etiquetas_empresa(df: pd.DataFrame, labels: list[str]) -> pd.DataFrame:
    if df.empty or not labels:
        return df
    if "empresa" in df.columns:
        em = df["empresa"].fillna("").astype(str).str.strip()
        return df.loc[em.isin(labels)].copy()
    if "org_id" in df.columns:
        oid = df["org_id"].astype(str).str.strip()
        return df.loc[oid.isin(labels)].copy()
    return df


def _faturamento_dre_default_empresa_labels(
    df: pd.DataFrame, org_id: str, org_display_name: str
) -> list[str]:
    """Rótulos iniciais do multiselect **Empresa**: vazio = todas quando há várias marcas na base."""
    _ = org_id, org_display_name
    opts = _faturamento_dre_etiquetas_empresa_recorte(df)
    if not opts:
        return []
    if len(opts) == 1:
        return list(opts)
    return []


def _render_faturamento_dre_bloco_por_empresa(
    df_recorte: pd.DataFrame,
    load_info: dict[str, object],
    ts_proc: str,
    org_id: str,
) -> None:
    """Bloco operacional: grelha, export e filtros sobre o mesmo ``df_recorte`` do recorte global."""
    st.subheader("Detalhamento operacional")
    st.caption(
        "Tabela, indicadores deste bloco e **CSV** partilham o **recorte do módulo** (acima). "
        "Aqui só pode refinar por **Visão** (NF comercial do pedido), **alertas** e **busca** — "
        "empresa, datas e fiscal ficam só no **Recorte do módulo**."
    )
    _fdl_ui_gap_tight()

    _painel_faturamento(
        df_recorte,
        load_info,
        ts_proc,
        org_id,
        use_modulo_recorte=True,
        mvp_rotulos_bloco_dre=True,
    )


def _faturamento_nf_platform_display_series(df_nf: pd.DataFrame) -> pd.Series:
    if "plataforma" in df_nf.columns:
        return df_nf["plataforma"].fillna("").astype(str)
    return df_nf["plataforma_resumo"].fillna("").astype(str) if "plataforma_resumo" in df_nf.columns else pd.Series("", index=df_nf.index)


def _faturamento_nf_apply_minimal_recorte(
    df_nf: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
    ok_nf_dates: bool,
) -> pd.DataFrame:
    if df_nf.empty:
        return df_nf
    out = df_nf.copy()
    emp_opts = _faturamento_dre_etiquetas_empresa_recorte(out)
    sel_emp = [str(x).strip() for x in empresas_sel if str(x).strip()]
    if emp_opts and sel_emp:
        out = out.loc[out["empresa"].astype(str).isin(sel_emp)].copy()
    sel_plat = [str(x).strip() for x in plataformas_sel if str(x).strip()]
    if sel_plat and "plataforma" in out.columns:
        out = out.loc[out["plataforma"].astype(str).str.strip().isin(sel_plat)].copy()
    if ok_nf_dates and nf_d_fim >= nf_d_ini and "Nota_Data_Emissao" in out.columns:
        m = _fdl_fr_mask_nf_emissao_no_periodo(out["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
        out = out.loc[m].copy()
    return out


def _fmt_pct_ptbr_ratio(ratio: float, *, decimals: int = 1) -> str:
    """Ex.: 0,123 → «12,3%» (apenas apresentação)."""
    if math.isnan(ratio) or math.isinf(ratio):
        return "—"
    p = ratio * 100.0
    body = f"{p:.{decimals}f}".replace(".", ",")
    return f"{body}%"


def _margem_sobre_venda_str(resultado: float, valor_venda: float) -> str:
    """Margem % = Σ Resultado ÷ Σ Valor da venda (mesmos totais do painel; só exibição)."""
    if (
        valor_venda == 0
        or math.isnan(valor_venda)
        or math.isnan(resultado)
        or math.isinf(resultado)
    ):
        return "—"
    return _fmt_pct_ptbr_ratio(resultado / valor_venda, decimals=1)


def _render_fdl_fat_dre_nf_kpi_cards(
    *,
    kp: dict[str, float | int],
    ok_nf_dates: bool,
    use_nf_materializado: bool,
) -> None:
    """
    Cards executivos NF-first (Faturamento & DRE): duas linhas, hierarquia visual;
    sem alterar totais — apenas exibição.
    """
    vv = float(kp["valor_venda"])
    res = float(kp["resultado"])
    vf_str = (
        _fmt_brl_ptbr_celula(kp["valor_faturado_nf"]) if ok_nf_dates else "—"
    )
    dif_str = _fmt_brl_ptbr_celula(kp["diferenca"]) if ok_nf_dates else "—"
    margem_str = _margem_sobre_venda_str(res, vv)

    _ht_vf = (
        "Soma de Nota_Valor_Liquido_Total uma vez por NF no período de emissão da NF."
    )
    _ht_dif = "Valor da venda total menos valor faturado total (NF) no recorte."
    _ht_df = (
        "5% do Valor da venda (Σ Quantidade × Preço de lista) agregado à NF, por nota."
    )
    _ht_res = (
        "Valores já consolidados no materializado NF-first (Resultado / Despesa fixa)."
        if use_nf_materializado
        else (
            "Soma do Resultado das linhas de pedido da NF; recompõe despesa fixa quando aplicável "
            "para alinhar ao corte único por NF."
        )
    )
    _ht_mg = (
        "Σ Resultado ÷ Σ Valor da venda no recorte. Valor da venda = Quantidade × Preço de lista. "
        "Se Σ Valor da venda = 0, exibe traço."
    )

    def _card(
        label: str,
        value: str,
        *,
        tier: str,
        accent: bool = False,
        title: str | None = None,
    ) -> str:
        classes = f"fdl-fat-kpi-card fdl-fat-kpi-card--{tier}"
        if accent:
            classes += " fdl-fat-kpi-card--accent"
        tattr = ""
        if title:
            tattr = f' title="{html.escape(title, quote=True)}"'
        return (
            f'<div class="{classes}"{tattr}>'
            f'<div class="fdl-fat-kpi-label">{html.escape(label)}</div>'
            f'<div class="fdl-fat-kpi-value">{html.escape(value)}</div>'
            "</div>"
        )

    primary_inner = "".join(
        [
            _card(
                "Valor da venda",
                _fmt_brl_ptbr_celula(kp["valor_venda"]) or "R$ 0,00",
                tier="primary",
                title="Σ Quantidade × Preço de lista no recorte (grão NF).",
            ),
            _card(
                "Valor faturado (NF)",
                vf_str or "—",
                tier="primary",
                title=_ht_vf,
            ),
            _card(
                "Resultado",
                _fmt_brl_ptbr_celula(kp["resultado"]) or "—",
                tier="primary",
                accent=True,
                title=_ht_res,
            ),
            _card(
                "Margem %",
                margem_str,
                tier="primary",
                accent=True,
                title=_ht_mg,
            ),
        ]
    )

    secondary_inner = "".join(
        [
            _card(
                "Diferença (venda − NF)",
                dif_str or "—",
                tier="secondary",
                title=_ht_dif,
            ),
            _card(
                "Comissão",
                _fmt_brl_ptbr_celula(kp["comissao"]) or "R$ 0,00",
                tier="secondary",
            ),
            _card(
                "Frete",
                _fmt_brl_ptbr_celula(kp["frete"]) or "R$ 0,00",
                tier="secondary",
            ),
            _card(
                "Imposto",
                _fmt_brl_ptbr_celula(kp["imposto"]) or "R$ 0,00",
                tier="secondary",
            ),
            _card(
                "Despesa fixa",
                _fmt_brl_ptbr_celula(kp["despesa_fixa"]) or "R$ 0,00",
                tier="secondary",
                title=_ht_df,
            ),
        ]
    )

    st.markdown(
        dedent(
            """
            <style>
            .fdl-fat-kpi-shell {
              font-family: var(--font, "Source Sans Pro", sans-serif);
              margin: 0 0 18px 0;
            }
            .fdl-fat-kpi-row {
              display: flex;
              flex-wrap: wrap;
              gap: 14px;
              margin-bottom: 16px;
            }
            .fdl-fat-kpi-row--secondary {
              gap: 10px;
              margin-bottom: 0;
            }
            .fdl-fat-kpi-card {
              flex: 1 1 0;
              min-width: 148px;
              background: #ffffff;
              border: 1px solid #e5e7eb;
              border-radius: 12px;
              padding: 14px 16px;
              box-sizing: border-box;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
            }
            .fdl-fat-kpi-card--primary {
              padding: 18px 20px;
              min-width: 160px;
            }
            .fdl-fat-kpi-card--primary.fdl-fat-kpi-card--accent {
              background: #f9fafb;
              border-color: #d1d5db;
              box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
            }
            .fdl-fat-kpi-card--secondary {
              padding: 12px 14px;
              min-width: 120px;
            }
            .fdl-fat-kpi-label {
              font-size: 0.72rem;
              font-weight: 500;
              color: #6b7280;
              line-height: 1.35;
              margin: 0 0 10px 0;
              letter-spacing: 0.02em;
              text-transform: none;
            }
            .fdl-fat-kpi-card--secondary .fdl-fat-kpi-label {
              font-size: 0.68rem;
              margin-bottom: 8px;
            }
            .fdl-fat-kpi-value {
              font-size: 1.45rem;
              font-weight: 600;
              color: #111827;
              line-height: 1.15;
              font-variant-numeric: tabular-nums;
              letter-spacing: -0.02em;
            }
            .fdl-fat-kpi-card--primary .fdl-fat-kpi-value {
              font-size: 1.55rem;
              font-weight: 700;
            }
            .fdl-fat-kpi-card--primary.fdl-fat-kpi-card--accent .fdl-fat-kpi-value {
              font-size: 1.68rem;
            }
            .fdl-fat-kpi-card--secondary .fdl-fat-kpi-value {
              font-size: 1.08rem;
              font-weight: 600;
            }
            </style>
            """
        )
        + f'<div class="fdl-fat-kpi-shell">'
        f'<div class="fdl-fat-kpi-row fdl-fat-kpi-row--primary">{primary_inner}</div>'
        f'<div class="fdl-fat-kpi-row fdl-fat-kpi-row--secondary">{secondary_inner}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


def _fmt_brl_ptbr_encargo_dre(v: object) -> str:
    """Total já somado no painel; só exibição como saída (−R$ …) na DRE gerencial."""
    try:
        x = float(v)
    except (TypeError, ValueError):
        return "—"
    if math.isnan(x):
        return "—"
    s = _fmt_brl_ptbr_celula(abs(x))
    if not s:
        return "—"
    if s.startswith("R$ "):
        return "−" + s
    return "−" + s


def _render_fdl_fat_dre_nf_gerencial(
    *,
    kp: dict[str, float | int],
    ok_nf_dates: bool,
) -> None:
    """
    DRE gerencial do mesmo recorte que os KPIs: usa apenas totais de ``compute_nf_panel_kpis``.
    Sem novos agregados; margem = Σ resultado ÷ Σ valor de venda (lista), como no painel.
    """
    vv = float(kp["valor_venda"])
    res = float(kp["resultado"])
    rec_venda = _fmt_brl_ptbr_celula(kp["valor_venda"]) or "R$ 0,00"
    vf_disp = (
        (_fmt_brl_ptbr_celula(kp["valor_faturado_nf"]) or "—")
        if ok_nf_dates
        else "—"
    )
    dif_disp = (
        (_fmt_brl_ptbr_celula(kp["diferenca"]) or "—") if ok_nf_dates else "—"
    )
    margem_s = _margem_sobre_venda_str(res, vv)
    res_disp = _fmt_brl_ptbr_celula(kp["resultado"]) or "—"
    enc_com = _fmt_brl_ptbr_encargo_dre(kp["comissao"])
    enc_fre = _fmt_brl_ptbr_encargo_dre(kp["frete"])
    enc_imp = _fmt_brl_ptbr_encargo_dre(kp["imposto"])
    enc_df = _fmt_brl_ptbr_encargo_dre(kp["despesa_fixa"])

    def _dre_row(
        lab: str,
        val: str,
        *,
        ref: bool = False,
        lead: bool = False,
        bridge: bool = False,
        encargo: bool = False,
        enc_last: bool = False,
        title: str | None = None,
    ) -> str:
        cls = "fdl-fat-dre-row"
        if lead:
            cls += " fdl-fat-dre-row--lead"
        elif ref:
            cls += " fdl-fat-dre-row--ref"
        elif bridge:
            cls += " fdl-fat-dre-row--bridge"
        elif encargo:
            cls += " fdl-fat-dre-row--enc"
            if enc_last:
                cls += " fdl-fat-dre-row--enc-last"
        tattr = f' title="{html.escape(title, quote=True)}"' if title else ""
        vcls = "fdl-fat-dre-val" + (" fdl-fat-dre-val--out" if encargo else "")
        return (
            f'<div class="{cls}"{tattr}>'
            f'<span class="fdl-fat-dre-lab">{html.escape(lab)}</span>'
            f'<span class="{vcls}">{html.escape(val)}</span>'
            "</div>"
        )

    _inner = (
        '<div class="fdl-fat-dre-title">DRE gerencial</div>'
        '<div class="fdl-fat-dre-sub">Totais do painel · leitura gerencial</div>'
        '<div class="fdl-fat-dre-block-h fdl-fat-dre-block-h--a">Ponte comercial × fiscal</div>'
        '<div class="fdl-fat-dre-a-shell">'
        + _dre_row(
            "Receita de venda (lista)",
            rec_venda,
            lead=True,
            title="Σ Quantidade × Preço de lista no recorte.",
        )
        + _dre_row(
            "Faturado NF (ref. fiscal)",
            vf_disp,
            ref=True,
            title=(
                "Nota_Valor_Liquido_Total 1× por NF. Contraste com a receita em lista; não somar como segunda receita."
            ),
        )
        + _dre_row(
            "Diferença (venda − faturado NF)",
            dif_disp,
            bridge=True,
            title="Receita lista − faturado NF (totais do recorte).",
        )
        + '<p class="fdl-fat-dre-foot-a-note">'
        "Ref. fiscal 1× por NF — não soma à receita de lista."
        "</p></div>"
        '<div class="fdl-fat-dre-block-h fdl-fat-dre-block-h--enc">Encargos</div>'
        + _dre_row("Comissão", enc_com, encargo=True, title="Σ comissão no recorte.")
        + _dre_row("Frete", enc_fre, encargo=True, title="Σ frete no recorte.")
        + _dre_row("Imposto", enc_imp, encargo=True, title="Σ imposto no recorte.")
        + _dre_row(
            "Despesa fixa",
            enc_df,
            encargo=True,
            enc_last=True,
            title="Σ despesa fixa (5% sobre valor de venda por NF) no recorte.",
        )
        + '<div class="fdl-fat-dre-block-h">Fechamento</div>'
        '<div class="fdl-fat-dre-close">'
        '<div class="fdl-fat-dre-row--result">'
        '<span class="fdl-fat-dre-lab">Resultado</span>'
        f'<span class="fdl-fat-dre-val">{html.escape(res_disp)}</span>'
        "</div>"
        '<div class="fdl-fat-dre-row--margem">'
        '<span class="fdl-fat-dre-lab">Margem sobre venda</span>'
        f'<span class="fdl-fat-dre-val">{html.escape(margem_s)}</span>'
        "</div></div>"
        '<p class="fdl-fat-dre-foot fdl-fat-dre-foot--final">'
        "Margem = resultado ÷ receita de venda (lista). Sem CMV nesta fase."
        "</p>"
    )
    st.markdown(
        f'<div class="fdl-fat-dre-wrap">{_inner}</div>',
        unsafe_allow_html=True,
    )


def _fdl_fat_min_inject_ui_styles() -> None:
    """Textos auxiliares NF-first mais discretos (apenas UI)."""
    st.markdown(
        dedent(
            """
            <style>
            .fdl-fat-min-aside {
              color: #6b7280;
              font-size: 0.78rem;
              line-height: 1.5;
              margin: 0 0 11px 0;
              max-width: 58rem;
            }
            .fdl-fat-min-aside strong { color: #4b5563; font-weight: 600; }
            .fdl-fat-min-aside--tight { margin-bottom: 5px; }
            .fdl-fat-min-aside--recorte {
              font-size: 0.74rem;
              color: #9ca3af;
              line-height: 1.45;
              margin: 0 0 8px 0;
            }
            .fdl-fat-min-aside--recorte strong { color: #6b7280; font-weight: 600; }
            .fdl-fat-min-table-cap {
              color: #9ca3af;
              font-size: 0.7rem;
              line-height: 1.42;
              margin: 0 0 12px 0;
              max-width: 58rem;
            }
            .fdl-fat-min-vsp-sm {
              display: block;
              height: 0.5rem;
              min-height: 0.5rem;
            }
            .fdl-fat-min-vsp-md {
              display: block;
              height: 1rem;
              min-height: 1rem;
            }
            .fdl-fat-min-vsp-lg {
              display: block;
              height: 1.35rem;
              min-height: 1.35rem;
            }
            .fdl-fat-dre-wrap {
              max-width: min(48rem, 100%);
              width: 100%;
              margin: 0;
              font-family: var(--font, "Source Sans Pro", sans-serif);
            }
            .fdl-fat-dre-title {
              font-size: 0.94rem;
              font-weight: 700;
              color: #111827;
              margin: 0 0 3px 0;
              letter-spacing: -0.02em;
            }
            .fdl-fat-dre-sub {
              font-size: 0.68rem;
              color: #9ca3af;
              margin: 0 0 10px 0;
              line-height: 1.35;
            }
            .fdl-fat-dre-block-h {
              font-size: 0.64rem;
              font-weight: 600;
              text-transform: uppercase;
              letter-spacing: 0.07em;
              color: #9ca3af;
              margin: 14px 0 5px 0;
            }
            .fdl-fat-dre-block-h:first-of-type { margin-top: 0; }
            .fdl-fat-dre-block-h--a {
              margin-bottom: 8px;
            }
            .fdl-fat-dre-a-shell {
              border: 1px solid #e8ecf1;
              border-radius: 10px;
              padding: 6px 14px 8px 14px;
              margin: 0 0 4px 0;
              background: #fefefe;
            }
            .fdl-fat-dre-block-h--enc {
              margin-top: 14px;
              margin-bottom: 8px;
            }
            .fdl-fat-dre-row {
              display: grid;
              grid-template-columns: minmax(0, 1fr) minmax(8rem, max-content);
              column-gap: 1.4rem;
              align-items: baseline;
              padding: 8px 0;
              border-bottom: 1px solid #f0f1f3;
              font-size: 0.875rem;
              color: #374151;
            }
            .fdl-fat-dre-a-shell .fdl-fat-dre-row--lead {
              border-radius: 6px;
            }
            .fdl-fat-dre-row--lead {
              padding: 12px 2px 13px 2px;
              margin-bottom: 0;
              border-bottom: 2px solid #d8dde4;
              background: linear-gradient(180deg, #ffffff 0%, #f7f8fa 100%);
            }
            .fdl-fat-dre-row--lead .fdl-fat-dre-lab {
              font-weight: 600;
              font-size: 0.96rem;
              color: #0f172a;
              letter-spacing: -0.018em;
            }
            .fdl-fat-dre-row--lead .fdl-fat-dre-val {
              font-weight: 700;
              font-size: 1.08rem;
              color: #0f172a;
            }
            .fdl-fat-dre-row--ref {
              margin-top: 0;
              padding-top: 5px;
              padding-bottom: 6px;
              border-top: none;
              border-bottom: 1px dotted #e2e6ec;
              background: transparent;
            }
            .fdl-fat-dre-row--ref .fdl-fat-dre-lab {
              color: #c5ccd6;
              font-weight: 400;
              font-size: 0.625rem;
              font-style: italic;
              letter-spacing: 0.03em;
            }
            .fdl-fat-dre-row--ref .fdl-fat-dre-val {
              color: #c5ccd6;
              font-weight: 400;
              font-size: 0.625rem;
            }
            .fdl-fat-dre-row--bridge {
              margin-top: 5px;
              margin-bottom: 2px;
              padding: 11px 12px 12px 12px;
              border-bottom: none;
              background: #eef0f3;
              border-radius: 8px;
              border: 1px solid #e0e4ea;
            }
            .fdl-fat-dre-row--bridge .fdl-fat-dre-lab {
              font-weight: 600;
              font-size: 0.9rem;
              color: #111827;
            }
            .fdl-fat-dre-row--bridge .fdl-fat-dre-val {
              font-weight: 700;
              font-size: 0.95rem;
              color: #0f172a;
            }
            .fdl-fat-dre-foot-a-note {
              font-size: 0.55rem;
              color: #d8dee6;
              line-height: 1.32;
              margin: 6px 0 0 0;
              padding: 5px 0 2px 10px;
              border-left: 2px solid #eef2f6;
              max-width: none;
            }
            .fdl-fat-dre-row--enc {
              padding: 11px 4px;
              border-bottom: 1px solid #f1f2f4;
            }
            .fdl-fat-dre-row--enc-last {
              border-bottom: 1px solid #e2e6ed;
              padding-bottom: 12px;
            }
            .fdl-fat-dre-lab {
              min-width: 0;
              line-height: 1.38;
            }
            .fdl-fat-dre-val {
              font-variant-numeric: tabular-nums;
              text-align: right;
              white-space: nowrap;
              font-weight: 500;
              color: #111827;
              justify-self: end;
            }
            .fdl-fat-dre-val--out {
              color: #525a63;
              font-weight: 600;
              letter-spacing: 0.02em;
              font-variant-numeric: tabular-nums;
            }
            .fdl-fat-dre-foot {
              font-size: 0.63rem;
              color: #9ca3af;
              line-height: 1.4;
              margin: 3px 0 0 0;
              max-width: 42rem;
            }
            .fdl-fat-dre-foot--inline {
              margin-top: 2px;
              margin-bottom: 2px;
            }
            .fdl-fat-dre-foot--final {
              margin-top: 30px;
              margin-bottom: 0;
              padding-top: 4px;
              font-size: 0.47rem;
              color: #dde2e9;
              line-height: 1.34;
            }
            .fdl-fat-dre-close {
              margin-top: 12px;
              border-radius: 12px;
              border: 1px solid #bfc6d0;
              background: linear-gradient(165deg, #fdfdfd 0%, #f4f5f7 55%, #f0f1f4 100%);
              overflow: hidden;
              box-shadow:
                0 1px 3px rgba(15, 23, 42, 0.035),
                0 6px 18px rgba(15, 23, 42, 0.055);
            }
            .fdl-fat-dre-row--result {
              display: grid;
              grid-template-columns: minmax(0, 1fr) minmax(8rem, max-content);
              column-gap: 1.4rem;
              align-items: baseline;
              padding: 17px 20px;
              border-bottom: 1px solid #cdd2d9;
              background: transparent;
            }
            .fdl-fat-dre-row--result .fdl-fat-dre-lab {
              font-weight: 600;
              font-size: 0.95rem;
              color: #111827;
              letter-spacing: -0.012em;
            }
            .fdl-fat-dre-row--result .fdl-fat-dre-val {
              font-weight: 700;
              font-size: 1.2rem;
              color: #0f172a;
              font-variant-numeric: tabular-nums;
              text-align: right;
              justify-self: end;
            }
            .fdl-fat-dre-row--margem {
              display: grid;
              grid-template-columns: minmax(0, 1fr) minmax(8rem, max-content);
              column-gap: 1.4rem;
              align-items: baseline;
              padding: 13px 20px 15px 20px;
              background: rgba(255, 255, 255, 0.62);
            }
            .fdl-fat-dre-row--margem .fdl-fat-dre-lab {
              font-weight: 500;
              font-size: 0.83rem;
              color: #64748b;
            }
            .fdl-fat-dre-row--margem .fdl-fat-dre-val {
              font-weight: 600;
              font-size: 1rem;
              color: #334155;
              font-variant-numeric: tabular-nums;
              text-align: right;
              justify-self: end;
            }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )


def _fdl_fat_min_vsp(*, size: str = "md") -> None:
    st.markdown(
        f'<div class="fdl-fat-min-vsp-{size}" aria-hidden="true"></div>',
        unsafe_allow_html=True,
    )


def _fdl_fat_min_aside(
    html_body: str, *, tight: bool = False, recorte: bool = False
) -> None:
    cls = "fdl-fat-min-aside"
    if tight:
        cls += " fdl-fat-min-aside--tight"
    if recorte:
        cls += " fdl-fat-min-aside--recorte"
    st.markdown(f'<div class="{cls}">{html_body}</div>', unsafe_allow_html=True)


def _fdl_cp_inject_panel_styles() -> None:
    """CSS compartilhado com KPIs Faturamento (fdl-fat-kpi-*) + refinamento Comercial & pedidos."""
    st.markdown(
        dedent(
            """
            <style>
            .fdl-fat-kpi-shell {
              font-family: var(--font, "Source Sans Pro", sans-serif);
              margin: 0 0 18px 0;
            }
            .fdl-fat-kpi-row {
              display: flex;
              flex-wrap: wrap;
              gap: 14px;
              margin-bottom: 16px;
            }
            .fdl-fat-kpi-row--secondary {
              gap: 10px;
              margin-bottom: 0;
            }
            .fdl-fat-kpi-card {
              flex: 1 1 0;
              min-width: 148px;
              background: #ffffff;
              border: 1px solid #e5e7eb;
              border-radius: 12px;
              padding: 14px 16px;
              box-sizing: border-box;
              box-shadow: 0 1px 2px rgba(15, 23, 42, 0.04);
            }
            .fdl-fat-kpi-card--primary {
              padding: 18px 20px;
              min-width: 160px;
            }
            .fdl-fat-kpi-card--primary.fdl-fat-kpi-card--accent {
              background: #f9fafb;
              border-color: #d1d5db;
              box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
            }
            .fdl-fat-kpi-card--secondary {
              padding: 12px 14px;
              min-width: 120px;
            }
            .fdl-fat-kpi-label {
              font-size: 0.72rem;
              font-weight: 500;
              color: #6b7280;
              line-height: 1.35;
              margin: 0 0 10px 0;
              letter-spacing: 0.02em;
            }
            .fdl-fat-kpi-card--secondary .fdl-fat-kpi-label {
              font-size: 0.68rem;
              margin-bottom: 8px;
            }
            .fdl-fat-kpi-value {
              font-size: 1.45rem;
              font-weight: 600;
              color: #111827;
              line-height: 1.15;
              font-variant-numeric: tabular-nums;
              letter-spacing: -0.02em;
            }
            .fdl-fat-kpi-card--primary .fdl-fat-kpi-value {
              font-size: 1.55rem;
              font-weight: 700;
            }
            .fdl-fat-kpi-card--primary.fdl-fat-kpi-card--accent .fdl-fat-kpi-value {
              font-size: 1.68rem;
            }
            .fdl-fat-kpi-card--secondary .fdl-fat-kpi-value {
              font-size: 1.08rem;
              font-weight: 600;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card {
              display: flex;
              flex-direction: column;
              align-items: stretch;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-label {
              font-size: 0.7rem;
              font-weight: 500;
              color: #9ca3af;
              margin: 0 0 12px 0;
              letter-spacing: 0.01em;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--secondary .fdl-fat-kpi-label {
              font-size: 0.66rem;
              margin-bottom: 10px;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-value {
              text-align: right;
              align-self: stretch;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--primary .fdl-fat-kpi-value {
              font-size: 1.72rem;
              font-weight: 700;
              letter-spacing: -0.03em;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--primary.fdl-fat-kpi-card--accent .fdl-fat-kpi-value {
              font-size: 1.85rem;
              color: #0f172a;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--secondary .fdl-fat-kpi-value {
              font-size: 1.12rem;
              font-weight: 600;
              color: #1e293b;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--primary {
              padding: 20px 22px;
            }
            .fdl-cp-kpi-shell.fdl-fat-kpi-shell .fdl-fat-kpi-card--secondary {
              padding: 13px 15px;
            }
            .fdl-cp-filtros-h {
              font-size: 1.05rem;
              font-weight: 600;
              color: #111827;
              margin: 0 0 6px 0;
              letter-spacing: -0.02em;
            }
            .fdl-cp-caption {
              font-size: 0.76rem;
              font-weight: 400;
              color: #9ca3af;
              line-height: 1.5;
              margin: 0 0 10px 0;
            }
            .fdl-cp-caption strong { color: #6b7280; font-weight: 500; }
            .fdl-cp-title-main {
              font-size: 1.28rem;
              font-weight: 700;
              color: #0f172a;
              margin: 0 0 6px 0;
              letter-spacing: -0.03em;
              line-height: 1.25;
            }
            .fdl-cp-title-sub {
              font-size: 0.76rem;
              font-weight: 400;
              color: #9ca3af;
              margin: 0 0 16px 0;
              line-height: 1.5;
            }
            .fdl-cp-title-sec {
              font-size: 0.98rem;
              font-weight: 600;
              color: #64748b;
              margin: 0 0 4px 0;
              letter-spacing: -0.01em;
            }
            .fdl-cp-title-sec-note {
              font-size: 0.72rem;
              font-weight: 400;
              color: #b4bcc6;
              margin: 0 0 12px 0;
              line-height: 1.45;
            }
            .fdl-cp-title-decision {
              font-size: 1.12rem;
              font-weight: 700;
              color: #0f172a;
              margin: 0 0 4px 0;
              letter-spacing: -0.025em;
              line-height: 1.25;
            }
            .fdl-cp-exec {
              background: #fafafa;
              border: 1px solid #e5e7eb;
              border-radius: 10px;
              padding: 14px 16px 12px 16px;
              margin: 0 0 16px 0;
            }
            .fdl-cp-exec-h {
              font-size: 0.65rem;
              font-weight: 600;
              text-transform: uppercase;
              letter-spacing: 0.07em;
              color: #9ca3af;
              margin: 0 0 10px 0;
            }
            .fdl-cp-exec-row {
              display: flex;
              flex-wrap: wrap;
              gap: 10px 22px;
              margin-bottom: 10px;
              font-size: 0.84rem;
              font-weight: 400;
              color: #64748b;
              line-height: 1.55;
            }
            .fdl-cp-exec-row strong { color: #0f172a; font-weight: 600; font-variant-numeric: tabular-nums; }
            .fdl-cp-exec-top {
              font-size: 0.78rem;
              font-weight: 400;
              color: #64748b;
              margin: 8px 0 0 0;
              padding-top: 10px;
              border-top: 1px solid #e5e7eb;
              line-height: 1.55;
            }
            .fdl-cp-exec-top strong { color: #0f172a; font-weight: 600; }
            .fdl-cp-trend-summary {
              display: flex;
              flex-wrap: wrap;
              gap: 8px 20px;
              align-items: baseline;
              font-size: 0.78rem;
              font-weight: 400;
              color: #64748b;
              margin: 0 0 14px 0;
              padding: 11px 14px;
              background: #f9fafb;
              border-radius: 8px;
              border: 1px solid #e5e7eb;
            }
            .fdl-cp-trend-summary span strong {
              color: #0f172a;
              font-weight: 600;
              font-variant-numeric: tabular-nums;
            }
            </style>
            """
        ),
        unsafe_allow_html=True,
    )


def _render_comercial_pedidos_kpi_cards(kpis: dict[str, float | int]) -> None:
    """Cards de KPI no mesmo espírito visual do painel Faturamento & DRE."""

    def _card(
        label: str,
        value: str,
        *,
        tier: str,
        accent: bool = False,
        title: str | None = None,
    ) -> str:
        classes = f"fdl-fat-kpi-card fdl-fat-kpi-card--{tier}"
        if accent:
            classes += " fdl-fat-kpi-card--accent"
        tattr = ""
        if title:
            tattr = f' title="{html.escape(title, quote=True)}"'
        return (
            f'<div class="{classes}"{tattr}>'
            f'<div class="fdl-fat-kpi-label">{html.escape(label)}</div>'
            f'<div class="fdl-fat-kpi-value">{html.escape(value)}</div>'
            "</div>"
        )

    vcom = _fmt_brl_ptbr_celula(float(kpis["valor_comercial_lista"])) or "R$ 0,00"
    qtd = _fmt_int_ptbr(int(round(float(kpis["quantidade_total"]))))
    ped = _fmt_int_ptbr(int(kpis["pedidos_atendidos_distintos"]))
    skus = _fmt_int_ptbr(int(kpis["skus_distintos"]))
    primary = "".join(
        [
            _card(
                "Valor comercial (lista)",
                vcom,
                tier="primary",
                accent=True,
                title="Soma Preço de lista × Quantidade no período filtrado (pedidos atendidos).",
            ),
        ]
    )
    secondary = "".join(
        [
            _card("Quantidade (unidades)", qtd, tier="secondary"),
            _card(
                "Pedidos atendidos (distintos)",
                ped,
                tier="secondary",
                title="Pedidos únicos (multiloja/org), não linhas.",
            ),
            _card("SKUs distintos", skus, tier="secondary"),
        ]
    )
    st.markdown(
        f'<div class="fdl-fat-kpi-shell fdl-cp-kpi-shell">'
        f'<div class="fdl-fat-kpi-row fdl-fat-kpi-row--primary">{primary}</div>'
        f'<div class="fdl-fat-kpi-row fdl-fat-kpi-row--secondary">{secondary}</div>'
        f"</div>",
        unsafe_allow_html=True,
    )


def _comercial_abc_valor_table_styler(abc_v: pd.DataFrame):
    """Tabela ABC valor só para UI: moeda e % pt-BR, alinhamento, sem alterar ``abc_v`` de origem."""
    if abc_v.empty:
        try:
            return abc_v.style.hide(axis="index")
        except Exception:
            return abc_v.style
    d = abc_v.copy()
    if "Valor comercial (lista)" in d.columns:
        d["Valor comercial (lista)"] = d["Valor comercial (lista)"].map(
            lambda x: _fmt_brl_ptbr_celula(x) or "—"
        )
    if "Part %" in d.columns:
        part_raw = pd.to_numeric(d["Part %"], errors="coerce").fillna(0.0) * 100.0
        d["Part %"] = part_raw.map(_fmt_pct_ptbr_1)
    if "Acum %" in d.columns:
        acum_raw = pd.to_numeric(d["Acum %"], errors="coerce").fillna(0.0) * 100.0
        d["Acum %"] = acum_raw.map(_fmt_pct_ptbr_1)
    sty = d.style.hide(axis="index")
    _right = [c for c in ("Valor comercial (lista)", "Part %", "Acum %") if c in d.columns]
    if _right:
        sty = sty.set_properties(
            subset=_right,
            **{"text-align": "right", "font-variant-numeric": "tabular-nums"},
        )
    if "Produto" in d.columns:
        sty = sty.set_properties(
            subset=["Produto"],
            **{
                "text-align": "left",
                "max-width": "24rem",
                "white-space": "nowrap",
                "overflow": "hidden",
                "text-overflow": "ellipsis",
            },
        )
    if "SKU" in d.columns:
        sty = sty.set_properties(
            subset=["SKU"],
            **{"text-align": "left", "font-variant-numeric": "tabular-nums"},
        )
    if "Classe" in d.columns:
        sty = sty.set_properties(subset=["Classe"], **{"text-align": "center", "font-weight": "600"})
    return sty


def _comercial_abc_quantidade_table_styler(abc_q: pd.DataFrame):
    """Tabela ABC quantidade só para UI: qtd e % pt-BR."""
    if abc_q.empty:
        try:
            return abc_q.style.hide(axis="index")
        except Exception:
            return abc_q.style
    d = abc_q.copy()
    if "Quantidade" in d.columns:
        d["Quantidade"] = d["Quantidade"].map(_comercial_fmt_qtd_display)
    if "Part %" in d.columns:
        pr = pd.to_numeric(d["Part %"], errors="coerce").fillna(0.0) * 100.0
        d["Part %"] = pr.map(_fmt_pct_ptbr_1)
    if "Acum %" in d.columns:
        ar = pd.to_numeric(d["Acum %"], errors="coerce").fillna(0.0) * 100.0
        d["Acum %"] = ar.map(_fmt_pct_ptbr_1)
    sty = d.style.hide(axis="index")
    _right = [c for c in ("Quantidade", "Part %", "Acum %") if c in d.columns]
    if _right:
        sty = sty.set_properties(
            subset=_right,
            **{"text-align": "right", "font-variant-numeric": "tabular-nums"},
        )
    if "Produto" in d.columns:
        sty = sty.set_properties(
            subset=["Produto"],
            **{
                "text-align": "left",
                "max-width": "20rem",
                "white-space": "nowrap",
                "overflow": "hidden",
                "text-overflow": "ellipsis",
            },
        )
    if "SKU" in d.columns:
        sty = sty.set_properties(
            subset=["SKU"],
            **{"text-align": "left", "font-variant-numeric": "tabular-nums"},
        )
    if "Classe" in d.columns:
        sty = sty.set_properties(subset=["Classe"], **{"text-align": "center", "font-weight": "600"})
    return sty


def _comercial_abc_valor_exec_html(abc_v: pd.DataFrame) -> str:
    """Leitura executiva derivada apenas do DataFrame ABC valor (sem alterar lógica)."""
    if abc_v.empty or "Classe" not in abc_v.columns:
        return ""
    cls = abc_v["Classe"].astype(str).str.strip().str.upper()
    part = pd.to_numeric(abc_v.get("Part %"), errors="coerce").fillna(0.0)
    share_a = float(part.loc[cls.eq("A")].sum())
    share_a_pct = share_a * 100.0
    n_a = int(cls.eq("A").sum())
    n_b = int(cls.eq("B").sum())
    n_c = int(cls.eq("C").sum())
    top_lines: list[str] = []
    if "Produto" in abc_v.columns and "Valor comercial (lista)" in abc_v.columns:
        top = abc_v.head(3)
        for _, row in top.iterrows():
            lab = str(row.get("Produto", "—")).strip() or "—"
            if len(lab) > 56:
                lab = lab[:53] + "…"
            vl = row.get("Valor comercial (lista)")
            try:
                vs = _fmt_brl_ptbr_celula(float(vl)) or "—"
            except (TypeError, ValueError):
                vs = "—"
            top_lines.append(f"<strong>{html.escape(lab)}</strong> · {html.escape(vs)}")
    top_html = "<br/>".join(top_lines) if top_lines else "—"
    share_disp = _fmt_pct_ptbr_1(share_a_pct)
    return (
        '<div class="fdl-cp-exec">'
        '<div class="fdl-cp-exec-h">Leitura executiva</div>'
        '<div class="fdl-cp-exec-row">'
        f"<span>Classe <strong>A</strong> concentra <strong>{html.escape(share_disp)}</strong> "
        "da receita comercial (lista) no recorte.</span>"
        "</div>"
        '<div class="fdl-cp-exec-row">'
        f"<span>SKUs <strong>A</strong>: <strong>{html.escape(_fmt_int_ptbr(n_a))}</strong></span>"
        f"<span>· <strong>B</strong>: <strong>{html.escape(_fmt_int_ptbr(n_b))}</strong></span>"
        f"<span>· <strong>C</strong>: <strong>{html.escape(_fmt_int_ptbr(n_c))}</strong></span>"
        "</div>"
        '<div class="fdl-cp-exec-top">'
        "<strong>Top 3 por valor comercial</strong><br/>"
        f"{top_html}"
        "</div></div>"
    )


def _comercial_trend_summary_html(trend_tbl: pd.DataFrame) -> str:
    if trend_tbl.empty or "Tendência" not in trend_tbl.columns:
        return ""
    tnorm = trend_tbl["Tendência"].fillna("").astype(str).str.strip().str.casefold()
    vc = tnorm.value_counts()
    cresc = int(vc.get("crescente", 0))
    decr = int(vc.get("decrescente", 0))
    est = int(vc.get("estável", 0))
    ins = int(vc.get("insuficiente para tendência", 0))
    _ni = _fmt_int_ptbr
    parts = [
        f"<span><strong>{html.escape(_ni(cresc))}</strong> crescente</span>",
        f"<span><strong>{html.escape(_ni(decr))}</strong> decrescente</span>",
        f"<span><strong>{html.escape(_ni(est))}</strong> estável</span>",
        f"<span><strong>{html.escape(_ni(ins))}</strong> volume insuficiente</span>",
    ]
    if "Sugestão de compra" in trend_tbl.columns:
        s = trend_tbl["Sugestão de compra"].fillna("").astype(str).str.strip().str.casefold()
        n_prio = int(s.str.contains("priorizar", na=False).sum())
        n_red = int(s.str.contains("reduzir", na=False).sum())
        n_caut = int(s.str.contains("evitar", na=False).sum())
        parts.append(
            f"<span><strong>{html.escape(_ni(n_prio))}</strong> priorizar · "
            f"<strong>{html.escape(_ni(n_red))}</strong> reduzir · "
            f"<strong>{html.escape(_ni(n_caut))}</strong> cautela</span>"
        )
    return '<div class="fdl-cp-trend-summary">' + "".join(parts) + "</div>"


def _cp_trend_cell_style(v: object) -> str:
    _ta = "text-align: left; font-variant-numeric: tabular-nums; "
    t = str(v).strip().casefold()
    if t == "crescente":
        return _ta + "background-color: #f0fdf4; color: #14532d; font-weight: 600"
    if t == "decrescente":
        return _ta + "background-color: #fef2f2; color: #7f1d1d; font-weight: 600"
    if t == "estável":
        return _ta + "background-color: #f3f4f6; color: #1f2937; font-weight: 600"
    if t == "insuficiente para tendência":
        return _ta + "background-color: #fafafa; color: #6b7280; font-weight: 500"
    return _ta + "font-weight: 500"


def _cp_sug_cell_style(v: object) -> str:
    _ta = "text-align: left; font-weight: 500; "
    s = str(v).strip().casefold()
    if "priorizar" in s:
        return _ta + "background-color: #f0fdf4; color: #14532d; font-weight: 600"
    if "reduzir" in s:
        return _ta + "background-color: #fef2f2; color: #7f1d1d; font-weight: 600"
    if "evitar" in s:
        return _ta + "background-color: #fffbeb; color: #92400e; font-weight: 600"
    if "manter" in s:
        return _ta + "background-color: #f9fafb; color: #374151; font-weight: 600"
    if "testar" in s:
        return _ta + "background-color: #f8fafc; color: #334155; font-weight: 600"
    return _ta + "color: #374151"


def _comercial_trend_styler(trend_tbl: pd.DataFrame):
    """Formatação pt-BR nas colunas numéricas + realce existente em Tendência / Sugestão."""
    if trend_tbl.empty:
        try:
            return trend_tbl.style.hide(axis="index")
        except Exception:
            return trend_tbl.style

    df = trend_tbl.copy()
    qty_cols = [c for c in df.columns if str(c).startswith("Qtd mês")]
    val_cols = [c for c in df.columns if str(c).startswith("Valor lista")]
    for c in qty_cols:
        df[c] = df[c].map(_comercial_fmt_qtd_display)
    for c in val_cols:
        df[c] = df[c].map(lambda x: _fmt_brl_ptbr_celula(x) or "—")

    sty = df.style
    try:
        sty = sty.hide(axis="index")
    except (TypeError, ValueError, AttributeError):
        try:
            sty = sty.hide_index()
        except Exception:
            pass

    def _elem_map(sty_obj: object, fn: object, subset: list[str]) -> object:
        m = getattr(sty_obj, "map", None) or getattr(sty_obj, "applymap", None)
        if m is None:
            return sty_obj
        return m(fn, subset=subset)

    _right = [c for c in qty_cols + val_cols if c in df.columns]
    if _right:
        sty = sty.set_properties(
            subset=_right,
            **{"text-align": "right", "font-variant-numeric": "tabular-nums"},
        )
    if "Produto" in df.columns:
        sty = sty.set_properties(
            subset=["Produto"],
            **{
                "text-align": "left",
                "max-width": "17rem",
                "white-space": "nowrap",
                "overflow": "hidden",
                "text-overflow": "ellipsis",
            },
        )
    if "SKU" in df.columns:
        sty = sty.set_properties(
            subset=["SKU"],
            **{"text-align": "left", "font-variant-numeric": "tabular-nums"},
        )
    if "Tendência" in df.columns:
        sty = _elem_map(sty, _cp_trend_cell_style, ["Tendência"])
    if "Sugestão de compra" in df.columns:
        sty = _elem_map(sty, _cp_sug_cell_style, ["Sugestão de compra"])
    return sty


def _render_comercial_pedidos_analise(
    df: pd.DataFrame,
    load_info: dict[str, object],
    ts_proc: str,
) -> None:
    """Vista comercial: atendidos, coluna Data, sem NF — lógica em ``comercial_pedidos_analise``."""
    _ = load_info, ts_proc
    if df.empty:
        st.info(
            "Sem dados de pedidos para este escopo. Confirme **materialização** e o módulo **faturamento**."
        )
        return
    if not cpa.data_column(df):
        if _is_admin_mode():
            st.warning(
                "Falta a coluna **Data** na base de faturamento — a vista Comercial & pedidos precisa dela para filtros e tendência."
            )
        else:
            st.warning("Dados incompletos para a vista comercial. Contacte o suporte.")
        return

    df_atend = cpa.filter_atendidos(df)
    if df_atend.empty:
        st.info("Não há linhas com **Situação = atendido** neste carregamento.")
        return

    d_min, d_max = cpa.bounds_dates_atendidos(df)
    _today = datetime.now(_BR_TZ).date()
    _sig_k = "fdl_cp_bounds_sig"
    if d_min is not None and d_max is not None:
        _bs = (d_min.isoformat(), d_max.isoformat())
        if st.session_state.get(_sig_k) != _bs:
            st.session_state[_sig_k] = _bs
            _di = d_min
            _df = min(d_max, _today)
            if _df < _di:
                _df = _di
            st.session_state["fdl_cp_d_ini"] = _di
            st.session_state["fdl_cp_d_fim"] = _df
        if "fdl_cp_d_ini" not in st.session_state:
            _di = d_min
            _df = min(d_max, _today)
            if _df < _di:
                _df = _di
            st.session_state["fdl_cp_d_ini"] = _di
            st.session_state["fdl_cp_d_fim"] = _df
        st.session_state["fdl_cp_d_ini"] = _safe_streamlit_date(st.session_state["fdl_cp_d_ini"], d_min)
        st.session_state["fdl_cp_d_fim"] = _safe_streamlit_date(st.session_state["fdl_cp_d_fim"], d_max)
        _d_ini_ui = st.session_state["fdl_cp_d_ini"]
        _d_fim_ui = st.session_state["fdl_cp_d_fim"]
        if _d_fim_ui < _d_ini_ui:
            st.session_state["fdl_cp_d_fim"] = _d_ini_ui
            _d_fim_ui = _d_ini_ui
    else:
        _d_ini_ui = None
        _d_fim_ui = None

    emp_opts = _faturamento_dre_etiquetas_empresa_recorte(df_atend)
    plats = sorted(
        {str(x).strip() for x in df_atend["Nome da plataforma"].dropna().unique() if str(x).strip()}
    ) if "Nome da plataforma" in df_atend.columns else []

    with st.container(border=True):
        st.markdown('<p class="fdl-cp-filtros-h">Filtros</p>', unsafe_allow_html=True)
        st.markdown(
            '<p class="fdl-cp-caption">Universo fixo: <strong>pedidos atendidos</strong> apenas. '
            "Valores comerciais = <strong>Preço de lista × Quantidade</strong>; <strong>sem</strong> nota fiscal.</p>",
            unsafe_allow_html=True,
        )
        if emp_opts:
            if "fdl_cp_emp" not in st.session_state:
                st.session_state["fdl_cp_emp"] = []
            else:
                prev_e = st.session_state["fdl_cp_emp"]
                if isinstance(prev_e, list):
                    st.session_state["fdl_cp_emp"] = [x for x in prev_e if x in emp_opts]
                else:
                    st.session_state["fdl_cp_emp"] = []
            st.multiselect(
                "Empresa",
                emp_opts,
                key="fdl_cp_emp",
                help="**Vazio** = todas. Recorte por marca (mesma coluna que Faturamento & DRE).",
                placeholder="Todas",
            )
        _multiselect_stable("fdl_cp_plat", "Plataforma", plats)
        if d_min is not None and d_max is not None:
            r_d = st.columns((1, 1))
            with r_d[0]:
                st.date_input(
                    "Período — início (Data do pedido)",
                    min_value=d_min,
                    max_value=d_max,
                    format="DD/MM/YYYY",
                    key="fdl_cp_d_ini",
                )
            with r_d[1]:
                st.date_input(
                    "Período — fim (Data do pedido)",
                    min_value=d_min,
                    max_value=d_max,
                    format="DD/MM/YYYY",
                    key="fdl_cp_d_fim",
                )
        else:
            st.markdown(
                '<p class="fdl-cp-caption">Datas de <strong>Data</strong> indisponíveis no recorte atendido.</p>',
                unsafe_allow_html=True,
            )
        if st.button("Limpar filtros desta vista", key="fdl_cp_reset"):
            for _k in ("fdl_cp_emp", "fdl_cp_plat", "fdl_cp_d_ini", "fdl_cp_d_fim", "fdl_cp_bounds_sig"):
                st.session_state.pop(_k, None)
            st.rerun()

    emp_sel = tuple(str(x).strip() for x in (st.session_state.get("fdl_cp_emp") or []) if str(x).strip())
    plat_sel = tuple(str(x).strip() for x in (st.session_state.get("fdl_cp_plat") or []) if str(x).strip())

    if d_min is not None and d_max is not None:
        d_ini_f = _safe_streamlit_date(st.session_state.get("fdl_cp_d_ini"), d_min)
        d_fim_f = _safe_streamlit_date(st.session_state.get("fdl_cp_d_fim"), d_max)
        if d_fim_f < d_ini_f:
            d_fim_f = d_ini_f
    else:
        d_ini_f, d_fim_f = None, None

    filtrado = cpa.filter_ui(
        df_atend,
        empresas_sel=emp_sel,
        plataformas_sel=plat_sel,
        d_ini=d_ini_f,
        d_fim=d_fim_f,
    )
    period_end_trend = d_fim_f if d_fim_f is not None else (_today if d_max is None else min(d_max, _today))

    _fdl_cp_inject_panel_styles()
    _fdl_ui_gap_section()
    kpis = cpa.compute_kpis(filtrado)
    _render_comercial_pedidos_kpi_cards(kpis)

    abc_v = cpa.compute_abc_valor(filtrado)
    abc_q = cpa.compute_abc_quantidade(filtrado)

    _fdl_ui_gap_section()
    with st.container(border=True):
        st.markdown(
            '<p class="fdl-cp-title-main">Receita comercial (ABC)</p>'
            '<p class="fdl-cp-title-sub">Bloco principal · participação no faturamento comercial (Preço de lista × Quantidade) · Pareto 80% / 95%</p>',
            unsafe_allow_html=True,
        )
        if abc_v.empty:
            st.markdown(
                '<p class="fdl-cp-caption">Sem SKU com código no recorte.</p>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(_comercial_abc_valor_exec_html(abc_v), unsafe_allow_html=True)
            _abc_cfg: dict[str, object] = {
                "SKU": TextColumn("SKU", width="small"),
                "Produto": TextColumn("Produto", width="large"),
                "Valor comercial (lista)": TextColumn("Valor (lista)", width="medium"),
                "Part %": TextColumn("Part. %", width="small"),
                "Acum %": TextColumn("Acum. %", width="small"),
                "Classe": TextColumn("Classe", width="small"),
            }
            _abc_cfg = {k: v for k, v in _abc_cfg.items() if k in abc_v.columns}
            st.dataframe(
                _comercial_abc_valor_table_styler(abc_v),
                use_container_width=True,
                height=min(440, 44 + len(abc_v) * 36),
                column_config=_abc_cfg or None,
            )

    _fdl_ui_gap_section()
    st.markdown(
        '<p class="fdl-cp-title-sec">Giro em unidades (ABC)</p>'
        '<p class="fdl-cp-title-sec-note">Complementar · mesmas classes A/B/C por quantidade vendida no período</p>',
        unsafe_allow_html=True,
    )
    if abc_q.empty:
        st.markdown(
            '<p class="fdl-cp-caption">Sem quantidade por SKU no recorte.</p>',
            unsafe_allow_html=True,
        )
    else:
        _q_cfg: dict[str, object] = {
            "SKU": TextColumn("SKU", width="small"),
            "Produto": TextColumn("Produto", width="medium"),
            "Quantidade": TextColumn("Qtd", width="small"),
            "Part %": TextColumn("Part. %", width="small"),
            "Acum %": TextColumn("Acum. %", width="small"),
            "Classe": TextColumn("Classe", width="small"),
        }
        _q_cfg = {k: v for k, v in _q_cfg.items() if k in abc_q.columns}
        st.dataframe(
            _comercial_abc_quantidade_table_styler(abc_q),
            use_container_width=True,
            height=min(320, 36 + min(len(abc_q), 12) * 34),
            column_config=_q_cfg or None,
        )

    _fdl_ui_gap_section()
    with st.container(border=True):
        st.markdown(
            '<p class="fdl-cp-title-decision">Decisão de compra e tendência</p>'
            '<p class="fdl-cp-title-sub">Últimos três meses calendário fechados · mesma classificação e sugestões já calculadas</p>',
            unsafe_allow_html=True,
        )
        _triple = cpa.three_closed_months_trend_bounds(period_end_trend, as_of=_today)[2]
        _tw0 = f"{_triple[0][1]:02d}/{_triple[0][0]}"
        _tw1 = f"{_triple[2][1]:02d}/{_triple[2][0]}"
        _tref = f"{_today.day:02d}/{_today.month:02d}/{_today.year}"
        _tfim = f"{period_end_trend.day:02d}/{period_end_trend.month:02d}/{period_end_trend.year}"
        st.markdown(
            f'<p class="fdl-cp-caption">Janela: <strong>{html.escape(_tw0)}</strong> a <strong>{html.escape(_tw1)}</strong> · '
            f'referência <strong>hoje</strong> {html.escape(_tref)} · fim do filtro {html.escape(_tfim)}. '
            "Sem mês civil em aberto. Mesmos filtros <strong>Empresa</strong> e <strong>Plataforma</strong>.</p>",
            unsafe_allow_html=True,
        )
        df_trend = cpa.filter_trend_window(
            df_atend,
            empresas_sel=emp_sel,
            plataformas_sel=plat_sel,
            period_end=period_end_trend,
            as_of=_today,
        )
        trend_tbl = cpa.compute_trend_and_suggestion(
            df_trend, abc_v, period_end=period_end_trend, as_of=_today
        )
        if trend_tbl.empty:
            st.markdown(
                '<p class="fdl-cp-caption">Sem linhas com SKU e quantidade na janela de tendência.</p>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(_comercial_trend_summary_html(trend_tbl), unsafe_allow_html=True)
            _tr_cfg: dict[str, object] = {}
            if "SKU" in trend_tbl.columns:
                _tr_cfg["SKU"] = TextColumn("SKU", width="small")
            if "Produto" in trend_tbl.columns:
                _tr_cfg["Produto"] = TextColumn("Produto", width="medium")
            for _c in trend_tbl.columns:
                if _c in _tr_cfg:
                    continue
                _cs = str(_c)
                if _cs.startswith("Qtd mês"):
                    _tr_cfg[_c] = TextColumn(_cs.replace("Qtd mês ", "Qtd "), width="small")
                elif _cs.startswith("Valor lista"):
                    _tr_cfg[_c] = TextColumn(_cs.replace("Valor lista ", "R$ "), width="small")
            if "Tendência" in trend_tbl.columns:
                _tr_cfg["Tendência"] = TextColumn("Tendência", width="medium")
            if "Sugestão de compra" in trend_tbl.columns:
                _tr_cfg["Sugestão de compra"] = TextColumn("Sugestão", width="large")
            st.dataframe(
                _comercial_trend_styler(trend_tbl),
                use_container_width=True,
                height=min(540, 48 + min(len(trend_tbl), 20) * 36),
                column_config=_tr_cfg or None,
            )


def _render_faturamento_dre_minimal(
    df: pd.DataFrame,
    load_info: dict[str, object],
    ts_proc: str,
    *,
    org_id: str,
    org_display_name: str,
) -> None:
    """
    Etapa 1 — painel **NF-first**: único período = **emissão da NF**; **plataforma** (opcional) restringe linhas
    de pedido no enriquecimento; KPIs e tabela derivam do mesmo ``df_nf``.
    """
    _fdl_fat_min_inject_ui_styles()
    _oid = str(org_id)
    _ = org_display_name, ts_proc, load_info
    df_nf_pre = load_info.get("faturamento_nf_df")
    use_nf_materializado = (
        bool(load_info.get("faturamento_nf_first"))
        and isinstance(df_nf_pre, pd.DataFrame)
        and nf_first_contract_dataframe_valid(df_nf_pre)
    )
    if use_nf_materializado and df_nf_pre.empty and not df.empty:
        use_nf_materializado = False

    _fdl_fat_min_aside(
        "Universo (Etapa 1): período único = <strong>emissão da NF</strong>. Uma linha por NF válida no intervalo. "
        "Valor faturado = líquido da NF uma vez; valor da venda, comissão, frete, imposto e resultado = soma nas "
        "linhas de pedido ligadas a essas NFs (filtros Empresa e Plataforma). Despesa fixa = 5% do valor da venda "
        "agregado a cada NF. A coluna Data do pedido não filtra o painel. NF só aparece com linha de pedido ligada à nota."
    )
    if use_nf_materializado:
        _fdl_fat_min_aside(
            "Fonte: materializado NF-first (<code>dataset_faturamento_nf.parquet</code>); filtros aplicados em memória."
        )
    else:
        _fdl_fat_min_aside(
            "Fonte: agregação ao vivo a partir do grão linha; com <code>dataset_faturamento_nf.parquet</code> na pasta, "
            "o painel passa a consumir a tabela NF."
        )
    if use_nf_materializado and _is_admin_mode() and load_info.get("faturamento_nf_first_path"):
        _p = html.escape(str(load_info.get("faturamento_nf_first_path")))
        _fdl_fat_min_aside(f"Admin — path Parquet NF-first: <code>{_p}</code>", tight=True)
    elif _is_admin_mode() and (
        load_info.get("faturamento_nf_first_skip") or load_info.get("faturamento_nf_first_error")
    ):
        _sk = load_info.get("faturamento_nf_first_skip")
        _e = load_info.get("faturamento_nf_first_error")
        _parts = ["Admin — NF-first não ativo."]
        if _sk:
            _parts.append(f"Motivo: <code>{html.escape(str(_sk))}</code>.")
        if _e:
            _parts.append(f"Erro: <code>{html.escape(str(_e))}</code>.")
        _fdl_fat_min_aside(" ".join(_parts), tight=True)

    if df.empty and not use_nf_materializado:
        st.info(
            "Sem dados de faturamento para este escopo. Confirme **materialização**, **slug** do cliente "
            "e o **escopo** (empresa ativa / consolidado) na barra lateral."
        )
        return

    if not use_nf_materializado:
        missing = _faturamento_painel_missing_schema_columns(df)
        if missing:
            if _is_admin_mode():
                st.warning(
                    "Estrutura de faturamento incompleta para a vista mínima. "
                    f"Faltam: {', '.join(missing[:12])}{'…' if len(missing) > 12 else ''}."
                )
            else:
                st.warning("Não foi possível apresentar o faturamento. Contacte o suporte.")
            return

    _df_bounds = df_nf_pre if use_nf_materializado else df
    if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame) and df_nf_pre.empty:
        st.info(
            "Sem notas no **materializado NF-first** para este escopo. Confirme materialização "
            "(`dataset_faturamento_nf.parquet`) e **escopo** (org / consolidado)."
        )
        return
    nf_min, nf_max, ok_nf_dates = faturamento_min_series_nf_emissao_bounds_dates(_df_bounds)
    nf_cal_min, nf_cal_max = _min_cal_limits(nf_min, nf_max) if ok_nf_dates else (nf_min, nf_max)
    _nf_sig_k = "fdl_fat_min_nf_bounds_sig"
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
            st.session_state["fdl_fat_min_nf_d_ini"] = _nfi
            st.session_state["fdl_fat_min_nf_d_fim"] = _nff
        if "fdl_fat_min_nf_d_ini" not in st.session_state:
            _nfi = nf_min
            _nff = min(nf_max, _today)
            _nfi = min(max(_nfi, nf_cal_min), nf_cal_max)
            _nff = min(max(_nff, nf_cal_min), nf_cal_max)
            if _nff < _nfi:
                _nff = _nfi
            st.session_state["fdl_fat_min_nf_d_ini"] = _nfi
            st.session_state["fdl_fat_min_nf_d_fim"] = _nff
        st.session_state["fdl_fat_min_nf_d_ini"] = min(
            max(_safe_streamlit_date(st.session_state["fdl_fat_min_nf_d_ini"], nf_min), nf_cal_min),
            nf_cal_max,
        )
        st.session_state["fdl_fat_min_nf_d_fim"] = min(
            max(_safe_streamlit_date(st.session_state["fdl_fat_min_nf_d_fim"], nf_max), nf_cal_min),
            nf_cal_max,
        )

    emp_opts = _faturamento_dre_etiquetas_empresa_recorte(_df_bounds)
    if use_nf_materializado and "plataforma" in _df_bounds.columns:
        plats = sorted(
            {str(x).strip() for x in _df_bounds["plataforma"].dropna().unique() if str(x).strip()}
        )
    elif "Nome da plataforma" in df.columns:
        plats = sorted(
            {str(x).strip() for x in df["Nome da plataforma"].dropna().unique() if str(x).strip()}
        )
    else:
        plats = []

    with st.container(border=True):
        st.subheader("Filtros")
        if emp_opts:
            if "fdl_fat_min_emp" not in st.session_state:
                st.session_state["fdl_fat_min_emp"] = []
            else:
                prev_e = st.session_state["fdl_fat_min_emp"]
                if isinstance(prev_e, list):
                    st.session_state["fdl_fat_min_emp"] = [x for x in prev_e if x in emp_opts]
                else:
                    st.session_state["fdl_fat_min_emp"] = []
            st.multiselect(
                "Empresa",
                emp_opts,
                key="fdl_fat_min_emp",
                help="**Vazio** = todas as empresas neste carregamento. Uma ou mais marcas para refinar.",
                placeholder="Todas",
            )
        _multiselect_stable("fdl_fat_min_plat", "Plataforma", plats)
        _fdl_fat_min_aside(
            "Plataforma: "
            + (
                "filtra notas pela plataforma consolidada no grão NF (materializado)."
                if use_nf_materializado
                else "restringe linhas de pedido no enriquecimento (venda, comissão, frete, etc.) nas NFs já filtradas por emissão."
            ),
            tight=True,
        )
        if ok_nf_dates:
            r_nf = st.columns((1, 1))
            with r_nf[0]:
                st.date_input(
                    "Período emissão NF — início",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_fat_min_nf_d_ini",
                    help=_FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
            with r_nf[1]:
                st.date_input(
                    "Período emissão NF — fim",
                    min_value=nf_cal_min,
                    max_value=nf_cal_max,
                    format="DD/MM/YYYY",
                    key="fdl_fat_min_nf_d_fim",
                    help=_FATURAMENTO_HELP_PERIODO_NF_EMISSAO_MIN,
                )
        elif "Nota_Data_Emissao" in _df_bounds.columns:
            _fdl_fat_min_aside(
                "Nota_Data_Emissao sem datas utilizáveis — período de emissão indisponível.",
                tight=True,
            )
        else:
            _fdl_fat_min_aside("Sem coluna Nota_Data_Emissao — período de emissão indisponível.", tight=True)
        if st.button("Limpar filtros desta vista", key="fdl_fat_min_reset"):
            for _k in (
                "fdl_fat_min_emp",
                "fdl_fat_min_plat",
                "fdl_fat_min_nf_d_ini",
                "fdl_fat_min_nf_d_fim",
                "fdl_fat_min_nf_bounds_sig",
            ):
                st.session_state.pop(_k, None)
            st.rerun()

    _fdl_ui_gap_section()
    _fdl_fat_min_vsp(size="md")

    _min_state = faturamento_recorte_min_state_from_session(st.session_state)
    _nf_kpi_ini = _safe_streamlit_date(st.session_state.get("fdl_fat_min_nf_d_ini"), nf_min)
    _nf_kpi_fim = _safe_streamlit_date(st.session_state.get("fdl_fat_min_nf_d_fim"), nf_max)
    if ok_nf_dates:
        _nf_kpi_ini = min(max(_nf_kpi_ini, nf_cal_min), nf_cal_max)
        _nf_kpi_fim = min(max(_nf_kpi_fim, nf_cal_min), nf_cal_max)
        if _nf_kpi_fim < _nf_kpi_ini:
            _nf_kpi_fim = _nf_kpi_ini

    if use_nf_materializado:
        df_nf = _faturamento_nf_apply_minimal_recorte(
            df_nf_pre,
            empresas_sel=_min_state.empresas,
            plataformas_sel=_min_state.plataformas,
            nf_d_ini=_nf_kpi_ini,
            nf_d_fim=_nf_kpi_fim,
            ok_nf_dates=ok_nf_dates,
        )
        _wrn_nf = ()
    else:
        df_nf, _wrn_nf = build_nf_grain_dataframe(
            df,
            _min_state,
            ok_nf_dates=ok_nf_dates,
            nf_d_ini=_nf_kpi_ini,
            nf_d_fim=_nf_kpi_fim,
        )
    for _m in _wrn_nf:
        st.warning(_m)

    _kp = compute_nf_panel_kpis(df_nf)
    _base_desc_html = (
        f"<strong>{len(df_nf_pre)}</strong> nota(s) no materializado NF-first"
        if use_nf_materializado and isinstance(df_nf_pre, pd.DataFrame)
        else f"<strong>{len(df)}</strong> linhas no carregamento (grão pedido)"
    )
    _fdl_fat_min_aside(
        f"Painel NF-first · recorte: <strong>{_kp['n_nf']}</strong> nota(s) · base: {_base_desc_html}",
        recorte=True,
    )
    _fdl_ui_gap_tight()
    _fdl_fat_min_vsp(size="md")

    _render_fdl_fat_dre_nf_kpi_cards(
        kp=_kp,
        ok_nf_dates=ok_nf_dates,
        use_nf_materializado=use_nf_materializado,
    )

    _fdl_fat_min_vsp(size="md")
    _render_fdl_fat_dre_nf_gerencial(kp=_kp, ok_nf_dates=ok_nf_dates)

    _fdl_fat_min_vsp(size="md")
    _fdl_ui_gap_section()
    _fdl_fat_min_vsp(size="lg")
    st.divider()
    _fdl_fat_min_vsp(size="md")

    _nf_table_cols_order = [
        "Emissão NF",
        "Empresa",
        "Plataforma",
        "NF",
        "Situação",
        "Pedido / multiloja",
        "Produto",
        "Linhas pedido",
        "Valor da venda",
        "Valor faturado NF",
        "Diferença",
        "Comissão",
        "Frete",
        "Imposto",
        "Despesa fixa",
        "Resultado",
    ]

    _df_nf_table = df_nf
    if not df_nf.empty and "Nota_Data_Emissao" in df_nf.columns:
        _df_nf_table = df_nf.sort_values(
            "Nota_Data_Emissao", ascending=False, na_position="last"
        ).reset_index(drop=True)

    _disp_nf_full = pd.DataFrame()
    _disp_nf_ui = pd.DataFrame()
    if not _df_nf_table.empty:
        _plat_s = _faturamento_nf_platform_display_series(_df_nf_table).astype(str)
        _disp_nf_full = pd.DataFrame(
            {
                "Emissão NF": _df_nf_table["Nota_Data_Emissao"].apply(
                    lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "—"
                ),
                "Empresa": _df_nf_table["empresa"].astype(str).replace("", "—"),
                "Plataforma": _plat_s,
                "NF": _df_nf_table["Nota_Numero_Normalizado"].astype(str),
                "Situação": _df_nf_table["Nota_Situacao"].astype(str).replace("", "—"),
                "Pedido / multiloja": _faturamento_disp_texto_sem_none(_df_nf_table["pedido_resumo"]),
                "Produto": _faturamento_disp_texto_sem_none(_df_nf_table["produto_resumo"]),
                "Linhas pedido": _df_nf_table["n_linhas_pedido"].astype(int),
                "Valor da venda": pd.to_numeric(_df_nf_table["valor_venda"], errors="coerce"),
                "Valor faturado NF": pd.to_numeric(_df_nf_table["valor_faturado_nf"], errors="coerce"),
                "Diferença": pd.to_numeric(_df_nf_table["diferenca"], errors="coerce"),
                "Comissão": pd.to_numeric(_df_nf_table["comissao"], errors="coerce"),
                "Frete": pd.to_numeric(_df_nf_table["frete"], errors="coerce"),
                "Imposto": pd.to_numeric(_df_nf_table["imposto"], errors="coerce"),
                "Despesa fixa": pd.to_numeric(_df_nf_table["despesa_fixa"], errors="coerce"),
                "Resultado": pd.to_numeric(_df_nf_table["resultado"], errors="coerce"),
            }
        )
        _disp_nf_full = _disp_nf_full[_nf_table_cols_order]
        _disp_nf_ui = _disp_nf_full.copy()

        def _fat_min_trunc_text_cell(v: object, max_len: int = 72) -> str:
            t = str(v).strip()
            if t in ("", "—", "nan") or len(t) <= max_len:
                return t if t else "—"
            return t[: max_len - 1] + "…"

        _disp_nf_ui["Pedido / multiloja"] = _disp_nf_ui["Pedido / multiloja"].map(
            lambda x: _fat_min_trunc_text_cell(x, 72)
        )
        _disp_nf_ui["Produto"] = _disp_nf_ui["Produto"].map(lambda x: _fat_min_trunc_text_cell(x, 72))

    _cfg_nf: dict[str, NumberColumn | TextColumn] = {}
    if "Valor da venda" in _disp_nf_ui.columns:
        _cfg_nf["Valor da venda"] = NumberColumn(
            "Valor da venda", format="R$ %,.2f", width="medium"
        )
    if "Valor faturado NF" in _disp_nf_ui.columns:
        _cfg_nf["Valor faturado NF"] = NumberColumn(
            "Valor faturado NF", format="R$ %,.2f", width="medium"
        )
    if "Diferença" in _disp_nf_ui.columns:
        _cfg_nf["Diferença"] = NumberColumn(
            "Diferença",
            format="R$ %,.2f",
            width="medium",
            help="Valor da venda menos valor faturado na NF.",
        )
    for _mc in ("Comissão", "Frete", "Imposto", "Despesa fixa"):
        if _mc in _disp_nf_ui.columns:
            _cfg_nf[_mc] = NumberColumn(_mc, format="R$ %,.2f", width="small")
    if "Resultado" in _disp_nf_ui.columns:
        _cfg_nf["Resultado"] = NumberColumn(
            "Resultado",
            format="R$ %,.2f",
            width="medium",
            help="Resultado consolidado por NF (materializado / regra do painel).",
        )
    if "Linhas pedido" in _disp_nf_ui.columns:
        _cfg_nf["Linhas pedido"] = NumberColumn(
            "Linhas pedido",
            format="%d",
            width="small",
            help="Quantidade de linhas de pedido agregadas nesta NF.",
        )

    _txt_nf_specs: tuple[tuple[str, str, str | None], ...] = (
        ("Emissão NF", "small", None),
        ("Empresa", "large", None),
        ("Plataforma", "medium", None),
        ("NF", "small", None),
        ("Situação", "small", None),
        ("Pedido / multiloja", "large", "Resumo de pedido ou multiloja (texto completo no CSV)."),
        ("Produto", "large", "Resumo de produtos/itens (texto completo no CSV)."),
    )
    for _cn, _cw, _ch in _txt_nf_specs:
        if _cn in _disp_nf_ui.columns:
            _cfg_nf[_cn] = TextColumn(_cn, width=_cw, help=_ch) if _ch else TextColumn(_cn, width=_cw)

    _fdl_fat_min_vsp(size="sm")
    st.subheader("Tabela por NF")
    _fdl_fat_min_vsp(size="sm")
    st.markdown(
        f'<div class="fdl-fat-min-table-cap">{len(_disp_nf_ui)} linha(s) · uma por NF · emissão decrescente. '
        f"Export CSV: mesmas colunas; Pedido e Produto sem truncar.</div>",
        unsafe_allow_html=True,
    )
    if _disp_nf_ui.empty:
        st.info(
            "Sem notas no recorte (confirme **período de emissão**, **empresa** e **plataforma**)."
        )
    else:
        st.dataframe(
            _disp_nf_ui,
            use_container_width=True,
            hide_index=True,
            height=min(
                620,
                176 + 42 * min(len(_disp_nf_ui), 17),
            ),
            column_config=_cfg_nf,
        )

    _fdl_ui_gap_section()
    _fdl_fat_min_vsp(size="sm")

    st.download_button(
        "Exportar CSV (recorte atual)",
        _disp_nf_full.to_csv(index=False).encode("utf-8-sig") if not _disp_nf_full.empty else b"",
        file_name="faturamento_recorte_minimo_nf.csv",
        mime="text/csv",
        key=f"fdl_fat_min_dl_{_oid}",
        disabled=_disp_nf_full.empty,
    )


def _faturamento_disp_texto_sem_none(s: pd.Series, *, placeholder: str = "—") -> pd.Series:
    """Evita que None/NaN apareçam como o texto «None» no ``st.dataframe`` (colunas opcionais do CSV de pedidos)."""

    def _cell(v: object) -> str:
        if v is None:
            return placeholder
        if isinstance(v, float) and math.isnan(v):
            return placeholder
        try:
            if pd.isna(v):
                return placeholder
        except (ValueError, TypeError):
            pass
        xs = str(v).strip()
        if not xs or xs.casefold() in {"nan", "none", "nat", "<na>"}:
            return placeholder
        return xs

    return s.map(_cell)


def _faturamento_disp_data_pedidos(s: pd.Series) -> pd.Series:
    """Datas do export ML (dia/mês/ano); placeholders tipo 00/00/0000 → «—»."""

    def _one(v: object) -> str:
        if v is None:
            return "—"
        if isinstance(v, float) and math.isnan(v):
            return "—"
        if isinstance(v, (pd.Timestamp, datetime)):
            if pd.isna(v):
                return "—"
            ts = pd.Timestamp(v)
            if ts.year < 1900:
                return "—"
            return ts.strftime("%d/%m/%Y")
        try:
            if pd.isna(v):
                return "—"
        except (ValueError, TypeError):
            pass
        xs = str(v).strip()
        if not xs or xs.casefold() in {"nan", "none", "nat"}:
            return "—"
        if xs in {"00/00/0000", "0/0/0000"} or xs.replace("0", "").replace("/", "").strip() == "":
            return "—"
        t = pd.to_datetime(xs, errors="coerce", dayfirst=True)
        if pd.isna(t) or t.year < 1900:
            return xs
        return t.strftime("%d/%m/%Y")

    return s.map(_one)


def _faturamento_compute_alert_bools(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas auxiliares _ab_* para KPIs, filtros e texto de alertas."""
    try:
        from processing.faturamento.normalize import to_numeric_br as _fat_to_num
    except Exception:  # noqa: BLE001
        _fat_to_num = None

    def _num(s: pd.Series) -> pd.Series:
        if _fat_to_num is not None:
            return _fat_to_num(s)
        return pd.to_numeric(s, errors="coerce")

    out = df.copy()
    pl, vt = "Preço de lista", "Valor total"
    pln = _num(out[pl]) if pl in out.columns else pd.Series(float("nan"), index=out.index)
    vtn = _num(out[vt]) if vt in out.columns else pd.Series(float("nan"), index=out.index)
    tol = _faturamento_divergencia_tol()
    out["_ab_pl_zero"] = pln.notna() & (pln == 0)
    desc_col = "Desconto proporcional total"
    if "Receita_Bruta" in out.columns:
        rbn = _num(out["Receita_Bruta"])
        base_ok = rbn.notna() & vtn.notna()
        if desc_col in out.columns:
            dcn = _num(out[desc_col])
            residual = (rbn - dcn - vtn).abs()
            out["_ab_div"] = base_ok & (
                dcn.notna() & (residual > tol)
                | dcn.isna() & ((rbn - vtn).abs() > tol)
            )
        else:
            out["_ab_div"] = base_ok & ((rbn - vtn).abs() > tol)
    else:
        out["_ab_div"] = pln.notna() & vtn.notna() & ((pln - vtn).abs() > tol)
    situ = (
        out["Situação"].fillna("").astype(str).str.strip().str.casefold()
        if "Situação" in out.columns
        else pd.Series("", index=out.index, dtype=object)
    )
    nf = (
        out["Existe Nota Fiscal gerada"].fillna("").astype(str).str.strip().str.casefold()
        if "Existe Nota Fiscal gerada" in out.columns
        else pd.Series("", index=out.index, dtype=object)
    )
    atendido = situ == "atendido"
    sem_nf = nf.eq("não") | nf.eq("nao")
    if "faturamento_consolidado" in out.columns:
        fc = _faturamento_series_bool_mask(out["faturamento_consolidado"])
    else:
        fc = pd.Series(False, index=out.index)
    out["_ab_sem_nf_np"] = atendido & sem_nf & ~fc
    return out


def _faturamento_alertas_text(s: pd.Series) -> str:
    parts: list[str] = []
    if bool(s.get("_ab_pl_zero")):
        parts.append("Preço lista zero")
    if bool(s.get("_ab_div")):
        parts.append("Divergência receita × valor (não explicada por desconto)")
    if bool(s.get("_ab_sem_nf_np")):
        parts.append("Sem NF não permitido")
    return " · ".join(parts)


def _faturamento_dre_global_filter_keys() -> list[str]:
    return [
        "fdl_fat_dre_emp",
        "fdl_fat_dre_sit",
        "fdl_fat_dre_d_ini",
        "fdl_fat_dre_d_fim",
        "fdl_fat_dre_plat",
        "fdl_fat_dre_presenca_nf",
        "fdl_fat_dre_nf_emi_use",
        "fdl_fat_dre_nf_emi_ini",
        "fdl_fat_dre_nf_emi_fim",
        "fdl_fat_dre_nf_sit",
        "fdl_fat_dre_data_bounds_sig",
        "fdl_fat_dre_nf_bounds_sig",
    ]


def _render_faturamento_dre_linha_confianca(
    *,
    n_base: int,
    df_recorte: pd.DataFrame,
    ok_dates: bool,
    d_min: date,
    d_max: date,
    has_nf_emi: bool,
    nf_ok_dates: bool,
    nf_d_min: date,
    nf_d_max: date,
) -> None:
    """Resumo curto: linhas no recorte vs. base, eixos comercial/fiscal ativos (sem novo filtro)."""
    n_rec = int(len(df_recorte))
    _pres = str(st.session_state.get("fdl_fat_dre_presenca_nf") or "Todos").strip()
    _emi = bool(st.session_state.get("fdl_fat_dre_nf_emi_use"))
    _nf_sit = st.session_state.get("fdl_fat_dre_nf_sit") or []
    _nf_sit_lbl = ", ".join(str(x) for x in _nf_sit) if _nf_sit else "todas"

    _head = f"**{n_rec}** linha(s) no recorte"
    if n_base != n_rec:
        _head += f" · base neste escopo: **{n_base}** linha(s)"

    if ok_dates:
        _di = _safe_streamlit_date(st.session_state.get("fdl_fat_dre_d_ini"), d_min)
        _df = _safe_streamlit_date(st.session_state.get("fdl_fat_dre_d_fim"), d_max)
        _venda = f"{_di.strftime('%d/%m/%Y')} → {_df.strftime('%d/%m/%Y')}"
    else:
        _venda = "período por **Data** indisponível nesta base"

    if not _emi:
        _emi_txt = "desligado"
    elif has_nf_emi and nf_ok_dates:
        _ni = _safe_streamlit_date(st.session_state.get("fdl_fat_dre_nf_emi_ini"), nf_d_min)
        _nf = _safe_streamlit_date(st.session_state.get("fdl_fat_dre_nf_emi_fim"), nf_d_max)
        _emi_txt = f"ligado · {_ni.strftime('%d/%m/%Y')} → {_nf.strftime('%d/%m/%Y')}"
    else:
        _emi_txt = "ligado (sem datas utilizáveis — recorte pode ficar vazio)"

    st.caption(
        f"**Confiança do recorte:** {_head}. "
        f"Venda: **{_venda}**. Vínculo fiscal (join): **{_pres}**. "
        f"Emissão NF: **{_emi_txt}**. Situação NF: **{_nf_sit_lbl}**."
    )


def _render_faturamento_dre_recorte_global(
    df: pd.DataFrame,
    *,
    org_id: str,
    org_display_name: str,
) -> pd.DataFrame:
    """
    Recorte global (comercial + fiscal) do módulo Faturamento & DRE, alinhado a
    ``docs/faturamento_recorte_modulo_aprovado.md``. Devolve o DataFrame já recortado.
    """
    if df.empty:
        with st.container(border=True):
            st.subheader("Recorte do módulo")
            st.info(
                "Sem linhas na base de faturamento. Confirme **materialização**, **escopo** na barra lateral e permissões. "
                "Com dados carregados, os blocos de recorte comercial e fiscal aparecem aqui."
            )
        return df
    out = df
    has_data = "Data" in out.columns
    if has_data:
        d_min, d_max, ok_dates = _series_datetime_bounds_dates(out["Data"])
    else:
        d_min = d_max = datetime.now(_BR_TZ).date()
        ok_dates = False
    cal_min, cal_max = (
        _faturamento_period_calendar_limits(d_min, d_max) if ok_dates else (d_min, d_max)
    )
    _fat_dre_bounds_sig_key = "fdl_fat_dre_data_bounds_sig"
    if ok_dates:
        _bs = (d_min.isoformat(), d_max.isoformat())
        if st.session_state.get(_fat_dre_bounds_sig_key) != _bs:
            st.session_state[_fat_dre_bounds_sig_key] = _bs
            st.session_state["fdl_fat_dre_d_ini"] = d_min
            st.session_state["fdl_fat_dre_d_fim"] = min(d_max, datetime.now(_BR_TZ).date())
    has_nf_emi = "Nota_Data_Emissao" in out.columns
    if has_nf_emi:
        nf_d_min, nf_d_max, nf_ok_dates = _series_datetime_bounds_dates(
            out["Nota_Data_Emissao"], dayfirst=False
        )
    else:
        nf_d_min = nf_d_max = datetime.now(_BR_TZ).date()
        nf_ok_dates = False
    nf_cal_min, nf_cal_max = (
        _faturamento_period_calendar_limits(nf_d_min, nf_d_max) if nf_ok_dates else (nf_d_min, nf_d_max)
    )
    _fat_dre_nf_bounds_sig_key = "fdl_fat_dre_nf_bounds_sig"
    if nf_ok_dates:
        _nf_bs = (nf_d_min.isoformat(), nf_d_max.isoformat())
        if st.session_state.get(_fat_dre_nf_bounds_sig_key) != _nf_bs:
            st.session_state[_fat_dre_nf_bounds_sig_key] = _nf_bs
            st.session_state["fdl_fat_dre_nf_emi_ini"] = nf_d_min
            st.session_state["fdl_fat_dre_nf_emi_fim"] = min(nf_d_max, datetime.now(_BR_TZ).date())
    sits = sorted({str(x).strip() for x in out["Situação"].dropna().unique() if str(x).strip()})
    plats = sorted(
        {str(x).strip() for x in out["Nome da plataforma"].dropna().unique() if str(x).strip()}
    )
    emp_opts = _faturamento_dre_etiquetas_empresa_recorte(out)
    nf_sit_vals = (
        sorted({str(x).strip() for x in out["Nota_Situacao"].dropna().unique() if str(x).strip()})
        if "Nota_Situacao" in out.columns
        else []
    )
    nf_sit_opts = sorted(set(nf_sit_vals) | {"Cancelada", "Denegada", "Inutilizada"})

    with st.container(border=True):
        st.subheader("Recorte do módulo")
        st.caption(
            "Dois eixos independentes: **comercial** (venda / empresa / plataforma) e **fiscal** (emissão NF / situação da NF). "
            "Com ambos os intervalos de datas preenchidos, o resultado é a **interseção**. "
            "A **Visão geral**, o **detalhamento** e o **CSV exportado** usam o mesmo recorte."
        )
        with st.container(border=True):
            st.caption("**Recorte comercial**")
            if emp_opts:
                st.caption(
                    "A carga do módulo é **consolidada** (orgs permitidas a si); a **Empresa** na barra lateral **não** corta esse universo. "
                    "Use o multiselect **Empresa** abaixo para Esquilo, Wood, ambas ou **vazio** = todas as marcas na base."
                )
            if emp_opts:
                if "fdl_fat_dre_emp" not in st.session_state:
                    st.session_state["fdl_fat_dre_emp"] = _faturamento_dre_default_empresa_labels(
                        out, org_id, org_display_name
                    )
                else:
                    prev_e = st.session_state["fdl_fat_dre_emp"]
                    if isinstance(prev_e, list):
                        st.session_state["fdl_fat_dre_emp"] = [x for x in prev_e if x in emp_opts]
                    else:
                        st.session_state["fdl_fat_dre_emp"] = _faturamento_dre_default_empresa_labels(
                            out, org_id, org_display_name
                        )
                    if not st.session_state["fdl_fat_dre_emp"]:
                        st.session_state["fdl_fat_dre_emp"] = _faturamento_dre_default_empresa_labels(
                            out, org_id, org_display_name
                        )
                st.multiselect(
                    "Empresa",
                    emp_opts,
                    key="fdl_fat_dre_emp",
                    help=(
                        "**Recorte principal** deste módulo. **Vazio** = todas as marcas na base carregada. "
                        "Uma ou mais opções isolam/comparam dentro do consolidado (independente da empresa selecionada na sidebar)."
                    ),
                    placeholder="Todas",
                )
            if "fdl_fat_dre_sit" not in st.session_state:
                st.session_state["fdl_fat_dre_sit"] = (
                    ["Atendido"] if "Atendido" in sits else ([] if not sits else [sits[0]])
                )
            else:
                prev = st.session_state["fdl_fat_dre_sit"]
                if isinstance(prev, list):
                    st.session_state["fdl_fat_dre_sit"] = [x for x in prev if x in sits]
                else:
                    st.session_state["fdl_fat_dre_sit"] = ["Atendido"] if "Atendido" in sits else []
            r_sp = st.columns((1, 1))
            with r_sp[0]:
                st.multiselect(
                    "Situação do pedido",
                    sits,
                    key="fdl_fat_dre_sit",
                    help="Padrão **Atendido**. Vazio = todas as situações.",
                )
            with r_sp[1]:
                _multiselect_stable("fdl_fat_dre_plat", "Plataforma", plats)
            if ok_dates:
                if "fdl_fat_dre_d_ini" not in st.session_state:
                    st.session_state["fdl_fat_dre_d_ini"] = d_min
                if "fdl_fat_dre_d_fim" not in st.session_state:
                    st.session_state["fdl_fat_dre_d_fim"] = min(d_max, datetime.now(_BR_TZ).date())
                st.session_state["fdl_fat_dre_d_ini"] = min(
                    max(_safe_streamlit_date(st.session_state["fdl_fat_dre_d_ini"], d_min), cal_min),
                    cal_max,
                )
                st.session_state["fdl_fat_dre_d_fim"] = min(
                    max(_safe_streamlit_date(st.session_state["fdl_fat_dre_d_fim"], d_max), cal_min),
                    cal_max,
                )
                r0 = st.columns((1, 1))
                with r0[0]:
                    st.date_input(
                        "Data da venda — início",
                        min_value=cal_min,
                        max_value=cal_max,
                        format="DD/MM/YYYY",
                        key="fdl_fat_dre_d_ini",
                        help=_FATURAMENTO_HELP_PERIODO_DATA,
                    )
                with r0[1]:
                    st.date_input(
                        "Data da venda — fim",
                        min_value=cal_min,
                        max_value=cal_max,
                        format="DD/MM/YYYY",
                        key="fdl_fat_dre_d_fim",
                        help=_FATURAMENTO_HELP_PERIODO_DATA,
                    )
                st.caption(
                    "Eixo **Data** do pedido (venda). Pode escolher datas fora do intervalo dos dados; só entram linhas cuja **Data** "
                    "cai entre início (inclusive) e fim (inclusive)."
                )
            elif has_data:
                st.caption("**Data** sem valores utilizáveis — filtro por data da venda desativado.")
            _pres_opts = ("Todos", "Com NF vinculada", "Sem NF vinculada")
            st.selectbox(
                "Com / sem nota (vínculo fiscal na base)",
                options=list(_pres_opts),
                key="fdl_fat_dre_presenca_nf",
                help="Refinamento comercial alinhado a **faturamento_nota_vinculada** (join pedidos↔notas). "
                "A **Visão** da tabela (Consolidado / Com NF (pedido) / …) continua separada, abaixo.",
            )

        with st.container(border=True):
            st.caption("**Recorte fiscal**")
            if "fdl_fat_dre_nf_emi_use" not in st.session_state:
                st.session_state["fdl_fat_dre_nf_emi_use"] = False
            st.checkbox(
                "Filtrar por data de emissão da NF",
                key="fdl_fat_dre_nf_emi_use",
                help="Desligado por omissão (**recorte fiscal inativo**). Ligado: aplica intervalo sobre **Nota_Data_Emissao** e "
                "**exclui linhas sem nota vinculada**.",
            )
            _emi_on = bool(st.session_state.get("fdl_fat_dre_nf_emi_use"))
            if _emi_on:
                if nf_ok_dates:
                    if "fdl_fat_dre_nf_emi_ini" not in st.session_state:
                        st.session_state["fdl_fat_dre_nf_emi_ini"] = nf_d_min
                    if "fdl_fat_dre_nf_emi_fim" not in st.session_state:
                        st.session_state["fdl_fat_dre_nf_emi_fim"] = min(
                            nf_d_max, datetime.now(_BR_TZ).date()
                        )
                    st.session_state["fdl_fat_dre_nf_emi_ini"] = min(
                        max(
                            _safe_streamlit_date(st.session_state["fdl_fat_dre_nf_emi_ini"], nf_d_min),
                            nf_cal_min,
                        ),
                        nf_cal_max,
                    )
                    st.session_state["fdl_fat_dre_nf_emi_fim"] = min(
                        max(
                            _safe_streamlit_date(st.session_state["fdl_fat_dre_nf_emi_fim"], nf_d_max),
                            nf_cal_min,
                        ),
                        nf_cal_max,
                    )
                    r_nf = st.columns((1, 1))
                    with r_nf[0]:
                        st.date_input(
                            "Data de emissão da NF — início",
                            min_value=nf_cal_min,
                            max_value=nf_cal_max,
                            format="DD/MM/YYYY",
                            key="fdl_fat_dre_nf_emi_ini",
                        )
                    with r_nf[1]:
                        st.date_input(
                            "Data de emissão da NF — fim",
                            min_value=nf_cal_min,
                            max_value=nf_cal_max,
                            format="DD/MM/YYYY",
                            key="fdl_fat_dre_nf_emi_fim",
                        )
                elif has_nf_emi:
                    st.caption("**Nota_Data_Emissao** sem datas utilizáveis — não é possível aplicar o intervalo.")
                else:
                    st.caption("Coluna **Nota_Data_Emissao** ausente — filtro fiscal por emissão indisponível.")
            if "fdl_fat_dre_nf_sit" not in st.session_state:
                st.session_state["fdl_fat_dre_nf_sit"] = []
            else:
                prev_nf = st.session_state["fdl_fat_dre_nf_sit"]
                if isinstance(prev_nf, list):
                    st.session_state["fdl_fat_dre_nf_sit"] = [x for x in prev_nf if x in nf_sit_opts]
                else:
                    st.session_state["fdl_fat_dre_nf_sit"] = []
            st.multiselect(
                "Situação da NF",
                nf_sit_opts,
                key="fdl_fat_dre_nf_sit",
                help="Vazio = todas. Valores vêm do export de notas (materialização); **Cancelada** mantém-se como opção mesmo sem linhas.",
                placeholder="Todas",
            )

        if st.button(
            "Redefinir recorte",
            key="fdl_fat_dre_reset_recorte",
            help="Repõe empresa, comercial, fiscal e refinamentos deste bloco",
        ):
            for _k in _faturamento_dre_global_filter_keys():
                st.session_state.pop(_k, None)
            for _rk in list(st.session_state.keys()):
                if isinstance(_rk, str) and _rk.startswith("fdl_fat_dre_painel_emp_"):
                    st.session_state.pop(_rk, None)
            st.rerun()

    _rec_state = faturamento_recorte_state_from_session(st.session_state)
    _rec_res = apply_recorte_modulo(out, _rec_state)
    for _w in _rec_res.warnings:
        st.warning(_w)
    for _a in _rec_res.admin_messages:
        if _is_admin_mode():
            st.warning(_a)
    sliced = _rec_res.df

    _render_faturamento_dre_linha_confianca(
        n_base=len(out),
        df_recorte=sliced,
        ok_dates=ok_dates,
        d_min=d_min,
        d_max=d_max,
        has_nf_emi=has_nf_emi,
        nf_ok_dates=nf_ok_dates,
        nf_d_min=nf_d_min,
        nf_d_max=nf_d_max,
    )

    _emi_active = bool(st.session_state.get("fdl_fat_dre_nf_emi_use"))
    if _emi_active and (not has_nf_emi or not nf_ok_dates):
        _msg_nf = (
            "esta base não inclui a coluna **Nota_Data_Emissao**"
            if not has_nf_emi
            else "a coluna **Nota_Data_Emissao** não tem datas de emissão utilizáveis (apenas vazias ou inválidas)"
        )
        st.info(
            f"O filtro **por data de emissão da NF** está ligado, mas {_msg_nf}. "
            "Por isso o **recorte fica vazio** — a Visão geral e a tabela não mostram linhas até desligar esse filtro "
            "ou corrigir o materializado / export de notas de saída."
        )

    return sliced


def _faturamento_filter_keys(org_id: str) -> list[str]:
    oid = str(org_id)
    return [
        f"fat_visao_{oid}",
        f"fat_d_ini_{oid}",
        f"fat_d_fim_{oid}",
        f"fat_d_bounds_sig_{oid}",
        f"fat_ms_plat_{oid}",
        f"fat_ms_sit_{oid}",
        f"fat_busca_{oid}",
        f"fat_ms_alert_{oid}",
    ]


def _faturamento_dre_vl_venda_series(df: pd.DataFrame, pl_col: str) -> pd.Series:
    """Vl. Venda comercial: coluna ``Vl_Venda`` ou mesma lógica da receita bruta do painel."""
    if "Vl_Venda" in df.columns:
        return pd.to_numeric(df["Vl_Venda"], errors="coerce")
    return _faturamento_painel_receita_series(df, pl_col)


def _faturamento_dre_vl_nota_fiscal_series(df: pd.DataFrame) -> pd.Series:
    """Valor líquido fiscal por linha: nota rateada; senão ``Valor total`` do pedido (legado)."""
    if "Nota_Valor_Liquido_Rateado" in df.columns:
        return pd.to_numeric(df["Nota_Valor_Liquido_Rateado"], errors="coerce")
    if "Valor total" in df.columns:
        return pd.to_numeric(df["Valor total"], errors="coerce")
    return pd.Series(float("nan"), index=df.index)


def _faturamento_dre_nf_coluna_display(df: pd.DataFrame) -> pd.Series:
    """Uma coluna NF: join ``Nota_Numero_Normalizado``, fallback «Número da nota» do pedido."""
    best = pd.Series("", index=df.index, dtype=object)
    if "Nota_Numero_Normalizado" in df.columns:
        best = df["Nota_Numero_Normalizado"].fillna("").astype(str).str.strip()
    if "Número da nota" in df.columns:
        ml = df["Número da nota"].fillna("").astype(str).str.strip()
        best = best.mask(best.eq(""), ml)
    return _faturamento_disp_texto_sem_none(best.astype(str))


def _faturamento_dre_frete_display_series(df: pd.DataFrame) -> pd.Series:
    """Um frete para leitura principal: plataforma; senão custo total de frete."""
    if "Frete_Plataforma" in df.columns:
        return pd.to_numeric(df["Frete_Plataforma"], errors="coerce")
    if "Custo de Frete" in df.columns:
        return pd.to_numeric(df["Custo de Frete"], errors="coerce")
    return pd.Series(float("nan"), index=df.index)


_FATURAMENTO_PAINEL_EM_CONSTRUCAO = False


def _painel_faturamento(
    df: pd.DataFrame,
    _load_info: dict[str, object],
    ts_proc: str,
    org_id: str,
    *,
    use_modulo_recorte: bool = False,
    mvp_rotulos_bloco_dre: bool = False,
) -> None:
    """
    Faturamento: filtros, indicadores, tabela e export CSV.

    Com ``use_modulo_recorte=True``, empresa / período de venda / plataforma / situação / fiscal vêm **só**
    do **Recorte do módulo** (``apply_recorte_modulo``); não se reaplica filtro de **Data** aqui.
    Com ``mvp_rotulos_bloco_dre=True``, rótulos alinham ao **Detalhamento operacional** (MVP DRE).
    """
    if _FATURAMENTO_PAINEL_EM_CONSTRUCAO:
        with st.container(border=True):
            st.caption("Módulo")
            st.info(
                "Em preparação. Em breve, as mesmas funções de análise e exportação da conciliação."
            )
        return
    _oid = str(org_id)
    if df.empty:
        st.info("Sem dados para este recorte. Verifique a empresa ou contacte o suporte.")
        return

    missing = _faturamento_painel_missing_schema_columns(df)
    if missing:
        if _is_admin_mode():
            st.warning(
                "Dados de faturamento sem colunas esperadas pelo painel. "
                f"Faltam: {', '.join(missing[:12])}{'…' if len(missing) > 12 else ''}."
            )
        else:
            st.warning(
                "Não foi possível apresentar o faturamento com a estrutura esperada. "
                "Contacte o administrador."
            )
        return

    work = _faturamento_compute_alert_bools(df)
    for c in (
        "faturamento_consolidado",
        "faturamento_com_nf",
        "faturamento_sem_nf",
    ):
        if c not in work.columns:
            work[c] = False

    if "Status_Custo" in work.columns:
        _vc = work["Status_Custo"].astype(str).str.strip()
        _n_sem = int(_vc.eq("SKU_SEM_CORRESPONDENCIA").sum())
        _n_ok_all = int(_vc.eq("CUSTO_OK").sum())
        _n_tot = len(work)
        _ratio_sem = (_n_sem / _n_tot) if _n_tot else 0.0
        if _n_sem > 0:
            if _ratio_sem <= 0.10:
                st.caption(
                    f"**Custo:** a maioria das linhas já tem custo alocado (**{_n_ok_all}** CUSTO_OK). "
                    f"Restam **{_n_sem}** exceções (SKU ausente na planilha, código inválido ou cadastro divergente). "
                    "Nessas linhas, **Resultado** pode ficar vazio."
                )
            else:
                st.info(
                    f"**Custo:** **{_n_sem}** de **{_n_tot}** linhas ainda sem custo na referência "
                    "(SKU sem correspondência ou cadastro). Onde não há custo, **Resultado** fica vazio."
                )

    pl_col, res_col = "Preço de lista", "Resultado"
    custo_prod_col = _faturamento_painel_custo_produto_col(list(work.columns))
    assert custo_prod_col is not None  # garantido por _faturamento_painel_missing_schema_columns

    has_data_col = "Data" in work.columns
    if use_modulo_recorte:
        has_usable_dates = False
        d_min = d_max = datetime.now(_BR_TZ).date()
        plats: list[str] = []
        sits: list[str] = []
    else:
        if has_data_col:
            d_min, d_max, has_usable_dates = _series_datetime_bounds_dates(work["Data"])
        else:
            d_min = d_max = datetime.now(_BR_TZ).date()
            has_usable_dates = False
        plats = sorted(
            {str(x).strip() for x in work["Nome da plataforma"].dropna().unique() if str(x).strip()}
        )
        sits = sorted({str(x).strip() for x in work["Situação"].dropna().unique() if str(x).strip()})

    _opt_alertas = (
        "Preço lista zero",
        "Divergência receita × valor (não explicada por desconto)",
        "Sem NF não permitido",
    )

    with st.container(border=True):
        st.subheader("Filtros")
        if use_modulo_recorte:
            st.caption(
                "Neste bloco: visão NF, alertas e busca. **Recorte comercial** e **recorte fiscal** (empresa, datas venda/emissão, plataforma, situações) vêm do **Recorte do módulo** acima."
                if not mvp_rotulos_bloco_dre
                else "**Detalhamento:** **Visão** (NF do pedido no materializado), **alertas** e **busca**. "
                "O **recorte** (empresa, datas, fiscal) está **só** no bloco acima — **sem segundo filtro de data** aqui."
            )
        else:
            st.caption("Período, visão, critérios e busca para refinar o recorte.")
        _fdl_ui_gap_section()
        _k_visao = f"fat_visao_{_oid}"
        if st.session_state.get(_k_visao) == _FAT_PAINEL_VISAO_COM_NF_LEGACY:
            st.session_state[_k_visao] = _FAT_PAINEL_VISAO_COM_NF
        _fat_visao_opts = (
            "Todos",
            "Consolidado",
            _FAT_PAINEL_VISAO_COM_NF,
            "Sem NF permitido",
        )
        visao = st.selectbox(
            "Visão",
            _fat_visao_opts,
            key=_k_visao,
            help=(
                "**Consolidado**: linhas com `faturamento_consolidado` (com NF permitido **ou** sem NF permitido). "
                "Não é “soma contábil” entre empresas. "
                f"**{_FAT_PAINEL_VISAO_COM_NF}**: coluna `faturamento_com_nf` — critério do **export de pedidos** "
                "(ex.: “Existe Nota Fiscal gerada”); **não** é o mesmo que **nota vinculada** na base fiscal "
                "(`faturamento_nota_vinculada`), filtrada em “Com / sem nota” acima. "
                "**Sem NF permitido**: `faturamento_sem_nf`."
            ),
        )
        if not use_modulo_recorte:
            r0 = st.columns((1.15, 1.15))
            if has_usable_dates:
                cal_min, cal_max = _faturamento_period_calendar_limits(d_min, d_max)
                k_ini = f"fat_d_ini_{_oid}"
                k_fim = f"fat_d_fim_{_oid}"
                _sig_k = f"fat_d_bounds_sig_{_oid}"
                _sig = (d_min.isoformat(), d_max.isoformat())
                if st.session_state.get(_sig_k) != _sig:
                    st.session_state[_sig_k] = _sig
                    st.session_state[k_ini] = d_min
                    st.session_state[k_fim] = min(d_max, datetime.now(_BR_TZ).date())
                if k_ini not in st.session_state:
                    st.session_state[k_ini] = d_min
                if k_fim not in st.session_state:
                    st.session_state[k_fim] = min(d_max, datetime.now(_BR_TZ).date())
                st.session_state[k_ini] = min(
                    max(_safe_streamlit_date(st.session_state[k_ini], d_min), cal_min),
                    cal_max,
                )
                st.session_state[k_fim] = min(
                    max(_safe_streamlit_date(st.session_state[k_fim], d_max), cal_min),
                    cal_max,
                )
                with r0[0]:
                    d_ini = st.date_input(
                        "Período — início (Data)",
                        min_value=cal_min,
                        max_value=cal_max,
                        format="DD/MM/YYYY",
                        key=k_ini,
                        help=_FATURAMENTO_HELP_PERIODO_DATA,
                    )
                with r0[1]:
                    d_fim = st.date_input(
                        "Período — fim (Data)",
                        min_value=cal_min,
                        max_value=cal_max,
                        format="DD/MM/YYYY",
                        key=k_fim,
                        help=_FATURAMENTO_HELP_PERIODO_DATA,
                    )
                st.caption(
                    "Datas fora do intervalo dos dados são permitidas; só entram linhas com **Data** no período escolhido."
                )
            elif has_data_col:
                st.caption(
                    "A coluna **Data** existe mas não tem valores parseáveis — o filtro por período está desativado."
                )
            r1 = st.columns((1.15, 1.15))
            with r1[0]:
                sel_plat = _multiselect_stable(f"fat_ms_plat_{_oid}", "Plataforma", plats)
            with r1[1]:
                sel_sit = _multiselect_stable(f"fat_ms_sit_{_oid}", "Situação do pedido", sits)
        else:
            sel_plat = []
            sel_sit = []
        busca = st.text_input(
            "Busca (pedido, multiloja, SKU, n.º da nota)",
            key=f"fat_busca_{_oid}",
            placeholder="Texto livre…",
        ).strip().lower()
        sel_alerts = st.multiselect(
            "Alertas",
            list(_opt_alertas),
            key=f"fat_ms_alert_{_oid}",
            placeholder="Nenhum filtro por alerta",
        )
        st.caption(
            "Divergência **receita × valor pago**: só quando a diferença não fecha com "
            "**Receita_Bruta − Desconto proporcional total − Valor total** (desconto comercial da fonte). "
            "Sem coluna de desconto, mantém-se a comparação direta receita de lista vs. valor pago."
        )
        if st.button("Limpar filtros", key=f"fat_clear_{_oid}"):
            for _k in _faturamento_filter_keys(_oid):
                st.session_state.pop(_k, None)
            st.rerun()

    if not use_modulo_recorte and has_usable_dates:
        d_ini = _safe_streamlit_date(d_ini, d_min)
        d_fim = _safe_streamlit_date(d_fim, d_max)

    _fdl_ui_gap_tight() if use_modulo_recorte and mvp_rotulos_bloco_dre else _fdl_ui_gap_section()

    filt = work.copy()
    if visao == "Consolidado":
        filt = filt.loc[_faturamento_series_bool_mask(filt["faturamento_consolidado"])].copy()
    elif visao == _FAT_PAINEL_VISAO_COM_NF:
        filt = filt.loc[_faturamento_series_bool_mask(filt["faturamento_com_nf"])].copy()
    elif visao == "Sem NF permitido":
        filt = filt.loc[_faturamento_series_bool_mask(filt["faturamento_sem_nf"])].copy()

    if not use_modulo_recorte and has_usable_dates:
        if d_fim < d_ini:
            st.warning("A data final não pode ser anterior à inicial.")
            d_fim = d_ini
        filt = filt.loc[_faturamento_mask_venda_no_periodo(filt["Data"], d_ini, d_fim)].copy()

    if not use_modulo_recorte and sel_plat:
        filt = filt[filt["Nome da plataforma"].isin(sel_plat)]
    if not use_modulo_recorte and sel_sit:
        filt = filt[filt["Situação"].isin(sel_sit)]
    if busca:
        m_bus = pd.Series(False, index=filt.index)
        for col in (
            "Número do pedido",
            "Número do pedido multiloja",
            "Código",
            "Número da nota",
            "Número",
            "Data",
            "Data do faturamento",
        ):
            if col in filt.columns:
                m_bus = m_bus | filt[col].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        filt = filt.loc[m_bus].copy()

    if sel_alerts:
        m_a = pd.Series(False, index=filt.index)
        if "Preço lista zero" in sel_alerts:
            m_a = m_a | filt["_ab_pl_zero"]
        if "Divergência receita × valor (não explicada por desconto)" in sel_alerts:
            m_a = m_a | filt["_ab_div"]
        if "Sem NF não permitido" in sel_alerts:
            m_a = m_a | filt["_ab_sem_nf_np"]
        filt = filt.loc[m_a].copy()

    if "Data" in filt.columns:
        _sort_dt = _faturamento_ts_pedido_para_dia_civil(filt["Data"])
        filt = (
            filt.assign(_fdl_sort_dt=_sort_dt)
            .sort_values("_fdl_sort_dt", ascending=False, na_position="last")
            .drop(columns=["_fdl_sort_dt"])
        )
    else:
        filt = filt.sort_values(res_col, ascending=True, na_position="last")

    receita_s = _faturamento_painel_receita_series(filt, pl_col)
    receita_sum = float(receita_s.fillna(0).sum())
    res_sum = float(pd.to_numeric(filt[res_col], errors="coerce").fillna(0).sum())
    margem_total = (res_sum / receita_sum) if receita_sum not in (0.0, -0.0) else float("nan")
    n_cons = (
        int(_faturamento_series_bool_mask(filt["faturamento_consolidado"]).sum())
        if "faturamento_consolidado" in filt.columns
        else 0
    )
    any_alert = filt["_ab_pl_zero"] | filt["_ab_div"] | filt["_ab_sem_nf_np"]
    n_alert = int(any_alert.sum())

    st.subheader("📊 Indicadores")
    if mvp_rotulos_bloco_dre:
        st.caption(
            "Sobre o **recorte do módulo**, depois de **Visão** / **alertas** / **busca** (se usar). "
            "**Visão geral** (topo) resume o recorte **antes** desses três. "
            "**Receita bruta** na grelha = **Receita_Bruta** por linha. **Resultado** e **Margem %** respeitam linhas sem custo."
        )
    else:
        st.caption(
            "Valores sobre o recorte filtrado. **Receita por Produtos** = soma da receita por linha "
            "(Receita_Bruta do materializado, ou preço × quantidade); alinhado à coluna **Receita (produto)** na tabela. "
            "Com custo alocado na maior parte das linhas, **Resultado total** e **Margem %** passam a refletir margem real."
        )
    _fdl_ui_gap_tight() if mvp_rotulos_bloco_dre else _fdl_ui_gap_section()
    if mvp_rotulos_bloco_dre:
        fk1, fk2, fk3, fk4 = st.columns(4)
        _fk = [fk1, fk2, fk3, fk4]
    else:
        fk1, fk2, fk3, fk4, fk5 = st.columns(5)
        _fk = [fk1, fk2, fk3, fk4, fk5]
    with _fk[0]:
        st.metric(
            "Receita bruta" if mvp_rotulos_bloco_dre else "Receita por Produtos",
            _fmt_brl_ptbr_celula(receita_sum),
        )
    with _fk[1]:
        st.metric("Resultado Total", _fmt_brl_ptbr_celula(res_sum))
    with _fk[2]:
        if receita_sum == 0 or (isinstance(margem_total, float) and math.isnan(margem_total)):
            st.metric("Margem Total %", "—")
        else:
            st.metric("Margem Total %", f"{margem_total * 100:.2f}%".replace(".", ","))
    if mvp_rotulos_bloco_dre:
        with _fk[3]:
            st.metric("Alertas Ativos", _fmt_int_ptbr(n_alert))
    else:
        with _fk[3]:
            st.metric("Itens Consolidados", _fmt_int_ptbr(n_cons))
        with _fk[4]:
            st.metric("Alertas Ativos", _fmt_int_ptbr(n_alert))

    if "Status_Custo" in filt.columns:
        _stc = filt["Status_Custo"].astype(str).str.strip()
        _n_ok_f = int(_stc.eq("CUSTO_OK").sum())
        _n_exc_f = int(len(filt) - _n_ok_f)
        if _n_exc_f == 0:
            st.caption(
                f"**{_n_ok_f}** linhas com custo alocado. O resultado total considera apenas linhas com custo."
            )
        else:
            st.caption(
                f"**{_n_ok_f}** linhas com custo alocado; **{_n_exc_f}** exceções sem custo.\n\n"
                "O resultado total considera apenas linhas com custo."
            )
    else:
        st.caption("O mesmo recorte aplica-se à tabela seguinte.")

    prod_col = _faturamento_resolve_produto_column(list(filt.columns))
    rpct = pd.to_numeric(filt["Resultado_Pct"], errors="coerce") if "Resultado_Pct" in filt.columns else pd.Series(float("nan"), index=filt.index)
    receita_linha = _faturamento_painel_receita_series(filt, pl_col)
    _ix = filt.index
    if "empresa" in filt.columns:
        _em = filt["empresa"].fillna("").astype(str).str.strip()
        empresa_s = _em.mask(_em.eq(""), "—")
    elif "org_id" in filt.columns:
        empresa_s = filt["org_id"].astype(str).str.strip()
    else:
        empresa_s = pd.Series("—", index=_ix)
    pedido_s = _faturamento_pedido_display_series(filt)

    if mvp_rotulos_bloco_dre:
        _data_s = (
            _faturamento_disp_data_pedidos(filt["Data"])
            if "Data" in filt.columns
            else pd.Series("—", index=_ix)
        )
        _prod_s = filt[prod_col].astype(str) if prod_col else pd.Series("", index=_ix)
        _qtd_s = (
            pd.to_numeric(filt["Quantidade"], errors="coerce")
            if "Quantidade" in filt.columns
            else pd.Series(float("nan"), index=_ix)
        )
        _vl_v = _faturamento_dre_vl_venda_series(filt, pl_col)
        _vl_nf = _faturamento_dre_vl_nota_fiscal_series(filt)
        _nf_s = _faturamento_dre_nf_coluna_display(filt)
        _fr_s = _faturamento_dre_frete_display_series(filt)
        _main_cols: list[tuple[str, pd.Series]] = [
            ("Data", _data_s),
            ("Empresa", empresa_s),
            ("Plataforma", filt["Nome da plataforma"]),
            ("Pedido", pedido_s),
            ("NF", _nf_s),
            ("Produto", _prod_s),
            ("SKU", filt["Código"]),
            ("Qtd", _qtd_s),
            ("Vl. Venda", _vl_v),
            ("Vl. Nota Fiscal", _vl_nf),
            ("Frete", _fr_s),
            ("Comissão", pd.to_numeric(filt["Taxa de Comissão"], errors="coerce")),
            ("Custo Total", pd.to_numeric(filt[custo_prod_col], errors="coerce")),
            ("Despesa fixa", pd.to_numeric(filt["Despesas Fixas"], errors="coerce")),
            ("Imposto", pd.to_numeric(filt["Imposto"], errors="coerce")),
            ("Resultado", pd.to_numeric(filt[res_col], errors="coerce")),
        ]
        disp = pd.DataFrame(dict(_main_cols))
        disp["Alertas"] = filt.apply(_faturamento_alertas_text, axis=1)

        _extra_export: dict[str, pd.Series] = {}
        if "Resultado_Pct" in filt.columns:
            _extra_export["Resultado %"] = rpct * 100.0
        if "Status_Custo" in filt.columns:
            _extra_export["Status custo"] = filt["Status_Custo"].astype(str)
        _extra_export["Receita bruta (auditoria)"] = receita_linha
        if "Valor total" in filt.columns:
            _extra_export["Valor total (pedido)"] = pd.to_numeric(filt["Valor total"], errors="coerce")
        _extra_export["Situação do pedido"] = filt["Situação"].astype(str)
        _extra_export["N.º do pedido"] = _faturamento_disp_texto_sem_none(filt["Número do pedido"])
        _extra_export["N.º pedido multiloja"] = _faturamento_disp_texto_sem_none(filt["Número do pedido multiloja"])
        if "Data do faturamento" in filt.columns:
            _extra_export["Data do faturamento"] = _faturamento_disp_data_pedidos(filt["Data do faturamento"])
        _extra_export["NF emitida? (pedido)"] = _faturamento_disp_texto_sem_none(filt["Existe Nota Fiscal gerada"])
        if "Frete Mercado Envios" in filt.columns and "Frete transportadora própria" in filt.columns:
            _extra_export["Frete Mercado Envios"] = pd.to_numeric(filt["Frete Mercado Envios"], errors="coerce")
            _extra_export["Frete transp. própria"] = pd.to_numeric(filt["Frete transportadora própria"], errors="coerce")
            _extra_export["Custo frete (total)"] = pd.to_numeric(filt["Custo de Frete"], errors="coerce")
        if "Número" in filt.columns:
            _extra_export["Ref. ML (col. Número)"] = _faturamento_disp_texto_sem_none(filt["Número"])
        if "Base_Imposto" in filt.columns:
            _extra_export["Base imposto"] = pd.to_numeric(filt["Base_Imposto"], errors="coerce")
        if "Nota_Situacao" in filt.columns:
            _extra_export["Situação da NF"] = filt["Nota_Situacao"].fillna("").astype(str)
        if "Nota_Data_Emissao" in filt.columns:
            _extra_export["Data emissão NF"] = _faturamento_disp_data_pedidos(filt["Nota_Data_Emissao"])
        _export = pd.concat([disp, pd.DataFrame(_extra_export)], axis=1)

        _cfg = {}
        for c in (
            "Vl. Venda",
            "Vl. Nota Fiscal",
            "Frete",
            "Comissão",
            "Custo Total",
            "Despesa fixa",
            "Imposto",
            "Resultado",
        ):
            if c in disp.columns:
                _hc = (
                    _FATURAMENTO_HELP_VALOR_NOTA_FISCAL
                    if c == "Vl. Nota Fiscal"
                    else None
                )
                _cfg[c] = (
                    NumberColumn(c, format="R$ %,.2f", help=_hc)
                    if _hc
                    else NumberColumn(c, format="R$ %,.2f")
                )
        if "Qtd" in disp.columns:
            _cfg["Qtd"] = NumberColumn("Qtd", format="%.2f")
        for c in ("Data", "Empresa", "Plataforma", "Pedido", "NF", "Produto", "SKU", "Alertas"):
            if c in disp.columns:
                _tc_kw: dict[str, str | bool] = {"width": "large" if c == "Alertas" else "medium"}
                if c == "Data":
                    _tc_kw["help"] = _FATURAMENTO_HELP_PERIODO_DATA
                elif c == "NF":
                    _tc_kw["help"] = "N.º a partir do join com notas de saída; se vazio, texto do pedido quando existir."
                _cfg[c] = TextColumn(c, **_tc_kw)

        st.subheader("Tabela principal")
        st.caption(
            f"{len(disp)} linhas · ordenação por **Data da venda** (mais recente primeiro). "
            "**CSV** exporta **as mesmas linhas** que a grelha (após **Visão** / alertas / busca). "
            "**Vl. Venda** = comercial (**Vl_Venda** ou equivalente). **Vl. Nota Fiscal** = "
            "**Nota_Valor_Liquido_Rateado** quando há join fiscal; senão **Valor total** do pedido. "
            "**Frete** = **Frete_Plataforma** ou **Custo de Frete**. "
            "O CSV inclui ainda colunas extra (pedido, frete detalhado, base fiscal, etc.)."
        )
    else:
        _core: list[tuple[str, pd.Series]] = [
            (
                "Data",
                _faturamento_disp_data_pedidos(filt["Data"])
                if "Data" in filt.columns
                else pd.Series("—", index=_ix),
            ),
            ("Empresa", empresa_s),
            ("Plataforma", filt["Nome da plataforma"]),
            ("Pedido", pedido_s),
            ("SKU", filt["Código"]),
            ("Produto", filt[prod_col].astype(str) if prod_col else pd.Series("", index=_ix)),
            ("Receita bruta", receita_linha),
            (_FATURAMENTO_UI_VALOR_NOTA_FISCAL, pd.to_numeric(filt["Valor total"], errors="coerce")),
            ("Resultado", pd.to_numeric(filt[res_col], errors="coerce")),
            ("Resultado %", rpct * 100.0),
        ]
        if "Status_Custo" in filt.columns:
            _core.append(("Status custo", filt["Status_Custo"].astype(str)))
        disp = pd.DataFrame(dict(_core))
        disp["Alertas"] = filt.apply(_faturamento_alertas_text, axis=1)

        _tail: list[tuple[str, pd.Series]] = [
            ("Situação do pedido", filt["Situação"].astype(str)),
            ("N.º do pedido", _faturamento_disp_texto_sem_none(filt["Número do pedido"])),
            ("N.º pedido multiloja", _faturamento_disp_texto_sem_none(filt["Número do pedido multiloja"])),
            (
                "Data do faturamento",
                _faturamento_disp_data_pedidos(filt["Data do faturamento"])
                if "Data do faturamento" in filt.columns
                else pd.Series("—", index=_ix),
            ),
            ("NF emitida?", _faturamento_disp_texto_sem_none(filt["Existe Nota Fiscal gerada"])),
            ("N.º da nota", _faturamento_disp_texto_sem_none(filt["Número da nota"])),
        ]
        if "Quantidade" in filt.columns:
            _tail.append(("Quantidade", pd.to_numeric(filt["Quantidade"], errors="coerce")))
        _tail.append(("Custo do produto", pd.to_numeric(filt[custo_prod_col], errors="coerce")))
        if "Frete Mercado Envios" in filt.columns and "Frete transportadora própria" in filt.columns:
            _tail.append(("Frete Mercado Envios", pd.to_numeric(filt["Frete Mercado Envios"], errors="coerce")))
            _tail.append(
                ("Frete transp. própria", pd.to_numeric(filt["Frete transportadora própria"], errors="coerce"))
            )
            _tail.append(("Custo frete (total)", pd.to_numeric(filt["Custo de Frete"], errors="coerce")))
        else:
            _tail.append(("Frete", pd.to_numeric(filt["Custo de Frete"], errors="coerce")))
        _tail.extend(
            [
                ("Comissão Plataforma", pd.to_numeric(filt["Taxa de Comissão"], errors="coerce")),
                ("Imposto", pd.to_numeric(filt["Imposto"], errors="coerce")),
                ("Despesas fixas", pd.to_numeric(filt["Despesas Fixas"], errors="coerce")),
            ]
        )
        _tail.append(
            (
                "Ref. ML (col. Número)",
                _faturamento_disp_texto_sem_none(filt["Número"])
                if "Número" in filt.columns
                else pd.Series("—", index=_ix),
            )
        )
        disp = pd.concat([disp, pd.DataFrame(dict(_tail))], axis=1)
        _export = disp.copy()

        _cfg = {}
        money_cols = (
            "Receita bruta",
            _FATURAMENTO_UI_VALOR_NOTA_FISCAL,
            "Custo do produto",
            "Frete Mercado Envios",
            "Frete transp. própria",
            "Custo frete (total)",
            "Frete",
            "Comissão Plataforma",
            "Imposto",
            "Despesas fixas",
            "Resultado",
        )
        for c in money_cols:
            if c in disp.columns:
                if c == _FATURAMENTO_UI_VALOR_NOTA_FISCAL:
                    _cfg[c] = NumberColumn(
                        c,
                        format="R$ %,.2f",
                        help=_FATURAMENTO_HELP_VALOR_NOTA_FISCAL,
                    )
                elif c == "Custo frete (total)":
                    _cfg[c] = NumberColumn(
                        c,
                        format="R$ %,.2f",
                        help="Soma ME + transportadora própria; continua a entrar no **Resultado**.",
                    )
                else:
                    _cfg[c] = NumberColumn(c, format="R$ %,.2f")
        if "Resultado %" in disp.columns:
            _cfg["Resultado %"] = NumberColumn("Resultado %", format="%.2f%%")
        if "Quantidade" in disp.columns:
            _cfg["Quantidade"] = NumberColumn("Quantidade", format="%.2f")
        if "Status custo" in disp.columns:
            _cfg["Status custo"] = TextColumn("Status custo", width="small")
        for c in (
            "Data",
            "Empresa",
            "Plataforma",
            "Pedido",
            "SKU",
            "Produto",
            "Alertas",
            "Situação do pedido",
            "N.º do pedido",
            "N.º pedido multiloja",
            "Data do faturamento",
            "Ref. ML (col. Número)",
            "NF emitida?",
            "N.º da nota",
        ):
            if c in disp.columns:
                _tc_kw = {"width": "large" if c == "Alertas" else "medium"}
                if c == "Data":
                    _tc_kw["help"] = _FATURAMENTO_HELP_PERIODO_DATA
                elif c == "Data do faturamento":
                    _tc_kw["help"] = (
                        "Campo secundário / futuro (competência fiscal). Pode estar vazio ou inconsistente; "
                        "o período do módulo usa a coluna **Data**."
                    )
                elif c == "N.º da nota":
                    _tc_kw["help"] = _FATURAMENTO_HELP_NUMERO_NF_COL
                _cfg[c] = TextColumn(c, **_tc_kw)

        st.subheader("Tabela principal")
        st.caption(
            f"{len(disp)} linhas · ordenação por **Data da venda** (mais recente primeiro). "
            "À esquerda: operação do dia; à direita: NF, quantidade e composição de custos/taxas (**Ref. ML** no fim). "
            f"**{_FATURAMENTO_UI_VALOR_NOTA_FISCAL}** = **Valor total** (convenção; ver ajuda da coluna — não é NF-e legal). "
            "**N.º da nota** vazio: esperado sem integração fiscal. "
            "Frete repartido (ME / transp. própria) quando o CSV tiver modalidade; total em **Custo frete (total)**. "
            "**Pedido** = multiloja se existir, senão n.º do pedido."
        )
        _export = disp.copy()

    st.dataframe(
        disp,
        use_container_width=True,
        hide_index=True,
        height=440,
        column_config=_cfg,
    )
    if "SKU_Normalizado" in filt.columns:
        _export["SKU_Normalizado"] = filt["SKU_Normalizado"].astype(str)
    if mvp_rotulos_bloco_dre:
        st.caption(
            "O **CSV exportado** inclui as colunas da tabela principal e **campos adicionais** "
            "(Resultado %, pedido, frete detalhado, base imposto, dados fiscais da NF quando existirem"
            + (", **SKU_Normalizado**" if "SKU_Normalizado" in filt.columns else "")
            + ")."
        )
    elif "SKU_Normalizado" in filt.columns:
        st.caption(
            "O **CSV exportado** inclui a coluna **SKU_Normalizado** (chave de join), útil para cruzar com a planilha de custo."
        )
    st.download_button(
        "Exportar CSV (filtrado)",
        _export.to_csv(index=False).encode("utf-8-sig"),
        file_name="faturamento_filtrado.csv",
        mime="text/csv",
        key=f"fat_dl_csv_{_oid}",
    )


def _render_kpi_card(
    label: str,
    value: str,
    icon: str,
    css_class: str,
    *,
    frete_variant: bool = False,
) -> None:
    _ = css_class, frete_variant  # compat. com assinatura antiga — UI só com componentes nativos
    with st.container(border=True):
        st.metric(f"{icon} {label}", value)


def _frete_meta_for_render(load_info: dict[str, object]) -> dict[str, object]:
    keys = (
        "vendas_arquivo",
        "frete_arquivo",
        "frete_tabular",
        "debug_logs",
        "avisos",
        "linhas",
    )
    return {k: load_info[k] for k in keys if k in load_info}


def _frete_org_widget_suffix(org_id: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in org_id)[:80]


def _render_frete_operacional_ui(
    org_id: str,
    df_frete: pd.DataFrame,
    meta_frete: dict[str, object],
    ts_proc: str,
    load_info: dict[str, object],
) -> None:
    """Filtros + contexto + KPIs + tabela + export (alinhado ao painel de repasse)."""
    _is_admin = _is_admin_mode()
    _sig = _frete_org_widget_suffix(org_id)

    try:
            _frete_stage_trace(2, "Filtros", "início")
            work = df_frete.copy()
            today = datetime.now(_BR_TZ).date()
            default_ini = today - timedelta(days=29)
            default_fim = today
        
            if "data_venda" in work.columns or "_data_venda_dt" in work.columns:
                dts = frete_series_normalize_sale_dt(frete_series_for_date_filter(work))
                d_min_data, d_max_data, have_dt = _series_datetime_bounds_dates(dts)
                if not have_dt:
                    d_min_data = d_max_data = today
            else:
                d_min_data = d_max_data = today
        
            picker_min = min(d_min_data, default_ini)
            picker_max = max(d_max_data, default_fim, today)
            if picker_max < picker_min:
                picker_min, picker_max = picker_max, picker_min

            # Janela «últimos 30 dias» ancorada em hoje pode não cruzar as datas reais do CSV
            # (ex.: export só 2025 com app em 2026 → recorte filtrado zerava KPIs e detalhamento).
            d_ini_try = max(d_min_data, default_ini)
            d_fim_try = min(d_max_data, default_fim)
            if d_ini_try > d_fim_try:
                d_ini_val, d_fim_val = d_min_data, d_max_data
            else:
                d_ini_val, d_fim_val = d_ini_try, d_fim_try
        
            estados: list[str] = []
            if "Estado" in work.columns:
                estados = sorted(
                    {str(x).strip() for x in work["Estado"].dropna().unique().tolist() if str(x).strip()}
                )
            situacao_opts: list[str] = list(FRETE_SITUACAO_FRETE_VALORES_FILTRO)
        
            with st.container(border=True):
                st.subheader("Filtros")
                st.caption("Período, critérios e busca para refinar o recorte.")
                st.write("")
                st.markdown("**Período** · data da venda")
                st.caption(
                    "Comparação por dia civil. Por omissão: últimos 30 dias até hoje."
                )
                r2 = st.columns((1.15, 1.15, 2.3))
                with r2[0]:
                    data_ini = st.date_input(
                        "Data da venda — início",
                        value=d_ini_val,
                        min_value=picker_min,
                        max_value=picker_max,
                        format="DD/MM/YYYY",
                        key=f"op_frete_d_ini_{_sig}",
                    )
                with r2[1]:
                    data_fim = st.date_input(
                        "Data da venda — fim",
                        value=d_fim_val,
                        min_value=picker_min,
                        max_value=picker_max,
                        format="DD/MM/YYYY",
                        key=f"op_frete_d_fim_{_sig}",
                    )
                st.write("")
                st.markdown("**Critérios**")
                r1 = st.columns((1.15, 1.15, 1.7))
                with r1[0]:
                    sel_est = _multiselect_stable(f"op_frete_ms_est_{_sig}", "Estado da venda", estados)
                with r1[1]:
                    sel_sit = _multiselect_stable(
                        f"op_frete_ms_situacao_{_sig}", FRETE_UI_SITUACAO_FRETE, situacao_opts
                    )
                with r1[2]:
                    busca = st.text_input("Busca (venda ou # anúncio)", "", key=f"op_frete_busca_{_sig}")
                    busca = busca.strip().lower()

            data_ini = _safe_streamlit_date(data_ini, d_ini_val)
            data_fim = _safe_streamlit_date(data_fim, d_fim_val)

            if data_fim < data_ini:
                st.warning("A data final não pode ser anterior à data inicial. Ajuste o período.")
                data_fim = data_ini
        
            tbl = work
            if sel_est and "Estado" in tbl.columns:
                tbl = tbl[tbl["Estado"].isin(sel_est)]
            if sel_sit:
                sit_tbl = compute_frete_situacao_frete_column(tbl)
                tbl = tbl.loc[sit_tbl.isin(sel_sit)]
            if busca:
                m = (
                    tbl[FRETE_UI_N_VENDA].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
                    if FRETE_UI_N_VENDA in tbl.columns
                    else pd.Series(False, index=tbl.index)
                )
                if FRETE_UI_ANUNCIO in tbl.columns:
                    m = m | tbl[FRETE_UI_ANUNCIO].fillna("").astype(str).str.lower().str.contains(
                        busca, regex=False
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
        
            _miss_req = [c for c in (FRETE_UI_N_VENDA, FRETE_UI_DIFERENCA) if c not in tbl_show.columns]
            if _miss_req:
                st.error(
                    "Colunas obrigatórias ausentes para a Conciliação de Frete: "
                    + ", ".join(repr(c) for c in _miss_req)
                    + ". Verifique o CSV materializado (dataset_frete_app.csv) ou o export ML."
                )
                if _frete_debug_ui_enabled():
                    st.json({"colunas_presentes": list(tbl_show.columns)[:120]})
                return
        
            if _frete_debug_ui_enabled():
                st.caption("Debug Frete: filtros aplicados — a seguir KPIs e tabelas.")
        
            _va = str(meta_frete.get("vendas_arquivo", "—"))
            _ts_esc = str(ts_proc)
            _pl = "Todas"
            if estados and sel_est and len(sel_est) < len(estados):
                _pl = ", ".join(sel_est[:2]) + ("..." if len(sel_est) > 2 else "")
            elif estados and not sel_est:
                _pl = "Todas"
            elif not estados:
                _pl = "—"
            _fdl_ui_gap_section_lg()
            st.caption(
                f"Estado (filtro): **{_pl}** · Atualizado: **{_ts_esc}** · "
                f"Venda: **{data_ini.strftime('%d/%m/%Y')}**–**{data_fim.strftime('%d/%m/%Y')}** · "
                f"Fonte: **{_va}**"
            )

            _rec_key = f"op_frete_recebido_{_sig}"
            if _rec_key not in st.session_state:
                st.session_state[_rec_key] = {}
            rec_map: dict[str, bool] = st.session_state[_rec_key]
        
            nv_s = tbl_show[FRETE_UI_N_VENDA].map(lambda x: str(x).strip() if pd.notna(x) else "")
            recebido_series = nv_s.map(lambda x: FRETE_VAL_RECEBIDO_SIM if rec_map.get(x) else FRETE_VAL_RECEBIDO_NAO)
            recebido_series.index = tbl_show.index
            _frete_stage_trace(2, "Filtros", f"concluída — {len(tbl_show)} linhas após filtros")
    except Exception as exc:
        _frete_stage_error(2, "Filtros e preparação da tabela filtrada", exc)
        return

    try:
        _frete_stage_trace(3, "KPIs e anúncios", "início")
        kpi_ex = frete_kpis_executivos(tbl_show)
        tbl_cob_maior = frete_tabela_anuncios_cobrado_maior(tbl_show)
        tbl_repasse = frete_tabela_anuncios_repasse_frete(tbl_show, recebido_series)

        _fdl_ui_gap_section_lg()
        st.subheader("📊 Indicadores")
        st.caption("Valores sobre o recorte filtrado.")
        _fdl_ui_gap_section()
        ek1, ek2 = st.columns(2)
        with ek1:
            st.metric(
                "Cobrado a maior (valor a recuperar)",
                _fmt_brl_ptbr_celula(kpi_ex["cobrado_maior"]),
            )
        with ek2:
            st.metric(
                "Repasse de frete (valor total)",
                _fmt_brl_ptbr_celula(kpi_ex["repasse"]),
            )

        if _is_admin and FRETE_UI_STATUS_CONC in tbl_show.columns:
            st.caption(
                "Modo técnico: existe **Status conciliação** nos dados; a priorização segue **Situação do Frete**."
            )

        _sem_anuncio = FRETE_UI_ANUNCIO not in tbl_show.columns
        st.divider()
        _fdl_ui_gap_section()
        st.subheader("💸 Problemas de frete (cobrado a maior)")
        st.caption("Anúncios onde o frete cobrado excede o esperado — prioridade para recuperação.")
        if _sem_anuncio:
            st.info("Inclua o **# do anúncio** no export de vendas para agregar por anúncio.")
        elif tbl_cob_maior.empty:
            st.info("Sem anúncios com cobrança a maior neste recorte. Ajuste período ou critérios.")
        else:
            _h1 = min(420, 120 + 36 * max(len(tbl_cob_maior), 1))
            st.dataframe(
                _format_frete_anuncio_tabela_display(tbl_cob_maior),
                use_container_width=True,
                hide_index=True,
                height=_h1,
            )

        st.divider()
        _fdl_ui_gap_section()
        st.subheader("🚚 Controle de repasse de frete")
        st.caption("Anúncios com repasse de frete a validar (inclui marcação «Recebido?» no detalhe).")
        if _sem_anuncio:
            pass
        elif tbl_repasse.empty:
            st.info("Sem repasse de frete a validar neste recorte. Ajuste período ou critérios.")
        else:
            _h2 = min(420, 120 + 36 * max(len(tbl_repasse), 1))
            st.dataframe(
                _format_frete_anuncio_tabela_display(tbl_repasse),
                use_container_width=True,
                hide_index=True,
                height=_h2,
            )
    
        _frete_stage_trace(3, "KPIs e anúncios", "concluída")
    except Exception as exc:
        _frete_stage_error(3, "Indicadores executivos e tabelas por anúncio", exc)
        return
    try:
        _frete_stage_trace(4, "Exportação e detalhe", "início")
        for w in meta_frete.get("avisos") or []:
            st.info(w)

        st.divider()
        _fdl_ui_gap_section()
        st.subheader("📋 Detalhamento das vendas")
        st.caption("Linhas filtradas — exporte o recorte ou ajuste «Recebido?» quando disponível.")
        _fdl_ui_gap_section()

        t_export_view = dataframe_frete_conciliacao_principal(tbl_show, layout="executivo")
        csv_bytes = t_export_view.to_csv(index=False).encode("utf-8-sig")
        t_excel = dataframe_frete_conciliacao_principal(tbl_show, layout="executivo")
        excel_buf = BytesIO()
        with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
            t_excel.to_excel(writer, index=False, sheet_name="Frete")
            ws = writer.sheets["Frete"]
            header_row = [cell.value for cell in ws[1]]
            for c_data in ("Data da venda",):
                if c_data in header_row:
                    col_idx = header_row.index(c_data) + 1
                    for row_idx in range(2, ws.max_row + 1):
                        cell = ws.cell(row=row_idx, column=col_idx)
                        if cell.value is not None:
                            cell.number_format = oxl_number_formats.FORMAT_DATE_DDMMYY
        excel_buf.seek(0)
        with st.container(border=True):
            st.caption("Exportar recorte filtrado")
            btn1, btn2 = st.columns([1, 1])
            with btn1:
                st.download_button(
                    "Exportar CSV",
                    data=csv_bytes,
                    file_name="conciliacao_frete_filtrada.csv",
                    mime="text/csv",
                    use_container_width=True,
                    key=f"op_frete_dl_csv_{_sig}",
                )
            with btn2:
                st.download_button(
                    "Exportar Excel",
                    data=excel_buf.getvalue(),
                    file_name="conciliacao_frete_filtrada.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                    key=f"op_frete_dl_xlsx_{_sig}",
                )

        st.write("")
        st.write("")
        st.write("")

        t_grid = _dataframe_frete_grid(tbl_show, _fmt_brl_ptbr_celula, _col_referencia_como_texto)
        t_main = dataframe_frete_conciliacao_principal(t_grid, layout="executivo")
        t_main = _frete_conciliacao_grid_com_icones(t_main)
        _h_df = 550 if len(t_main) > 8 else 360
    
        if t_main.empty:
            st.info(
                "**Nenhuma venda** com os filtros atuais. Alargue o período de datas ou limpe a busca / multiselects."
            )
        else:
            st.dataframe(
                t_main,
                column_config=_column_config_frete(t_main),
                use_container_width=True,
                hide_index=True,
                height=_h_df,
            )
            st.caption(
                "Use o ícone **olho** na barra da tabela para mostrar ou ocultar colunas. "
                "Em **Situação do Frete**, os ícones indicam o estado (ex.: ✅ OK, ⬆️ cobrado a maior, 🚚 repasse)."
            )
        st.caption(f"{len(t_main)} linhas no filtro atual.")
    
        if _is_admin and load_info.get("frete_consume") in ("live", "live_fallback"):
            st.caption(
                "Modo técnico: **tempo real** — dados calculados diretamente das fontes; ficheiro consolidado indisponível ou falhou."
            )
    
    
        _frete_stage_trace(4, "Exportação e detalhe", "concluída")
    except Exception as exc:
        _frete_stage_error(4, "Exportação, detalhe de vendas, tabela principal e editor", exc)
        return


def _painel_frete_emergencial(
    org_id: str, df_frete: pd.DataFrame, load_info: dict[str, object], ts_proc: str
) -> None:
    """
    Apresentação do painel Frete. O carregamento é feito em _load_frete_data (ponto único).
    """
    _is_admin = _is_admin_mode()

    if load_info.get("frete_fontes_error"):
        st.error("Erro ao localizar fontes de Frete / vendas ML.")
        if _is_admin:
            st.code(str(load_info.get("frete_fontes_error")), language="text")
        return

    if load_info.get("frete_no_vendas_source"):
        if _is_admin:
            st.warning(
                "Sem fonte de vendas ML para Frete. Defina **FDL_FRETE_VENDAS_URL** nos Secrets (Cloud) "
                "ou coloque ficheiros .xlsx/.csv em **Vendas - Mercado Livre** sob **FDL_BASE_DIR**."
            )
            st.caption(str(BASE_DIR))
        else:
            st.warning(
                "Não foi possível localizar o ficheiro de vendas do Mercado Livre para esta conciliação. "
                "Contacte o administrador."
            )
        return

    if load_info.get("frete_placeholder_vendas_url"):
        st.error(
            "O URL de **FDL_FRETE_VENDAS_URL** parece um **placeholder**. "
            "Nos Secrets, substitua por o **link completo** do Excel (Partilhar → copiar ligação do ficheiro)."
        )
        return

    if load_info.get("frete_ml_validation_failed") or load_info.get("frete_loader_error"):
        st.error("O ficheiro não parece ser o **export de vendas ML** (detalhe envios).")
        st.warning(
            "Confirme que **FDL_FRETE_VENDAS_URL** aponta para o mesmo tipo de ficheiro que está em "
            "**Vendas - Mercado Livre** no OneDrive (não use a planilha de **Repasse**)."
        )
        if _is_admin:
            st.code(str(load_info.get("frete_loader_error", "")), language="text")
        return

    if load_info.get("frete_vendas_from_url"):
        if _is_admin:
            st.success("**FDL_FRETE_VENDAS_URL** detetado — a base foi descarregada a partir do SharePoint.")
        else:
            st.success("Dados de vendas carregados a partir da nuvem.")
    elif load_info.get("frete_fonte_local_path") and _is_admin:
        st.caption(f"Fonte local: `{load_info['frete_fonte_local_path']}`")

    if _is_admin and load_info.get("frete_mat_from_repasse_sibling"):
        st.caption(
            "Frete: **dataset_frete_app.csv** ao lado do repasse (`.../repasse/current/` → "
            "`.../frete/current/`), via **FDL_REPASSE_MATERIALIZED_PATH** ou **FDL_PRECOMPUTED_PATH**."
        )

    if load_info.get("frete_consume") == "materialized" and _is_admin:
        for _line in (load_info.get("debug_logs") or []):
            if _line:
                st.caption(f"[frete-debug] {_line}")

    meta = _frete_meta_for_render(load_info)
    if _frete_debug_ui_enabled() and _is_admin:
        try:
            with st.expander("Diagnóstico Frete (opt-in: FDL_DEBUG_FRETE_UI=1)", expanded=True):
                st.write("### Etapa 1/4 — Dataset (carregado antes desta página)")
                st.write("**Estado:**", "carregado" if not df_frete.empty else "DataFrame vazio")
                st.write("**Linhas × colunas:**", df_frete.shape)
                _cols = list(df_frete.columns)
                st.write("**Nº de colunas:**", len(_cols))
                st.write("**Colunas (início):**", _cols[:35])
                if len(_cols) > 35:
                    st.caption(f"… e mais {len(_cols) - 35} colunas.")
                _req_ui = {
                    FRETE_UI_N_VENDA: "N.º venda (filtros / Recebido?)",
                    FRETE_ML_COL: "Frete cobrado (ML)",
                    "Estado": "Estado da venda",
                    FRETE_UI_DIFERENCA: "KPIs e situação",
                }
                st.write("**Colunas esperadas pela UI:**")
                for _k, _desc in _req_ui.items():
                    st.write(f"- `{_k}`: {'OK' if _k in _cols else 'AUSENTE'} — {_desc}")
                st.write("**Fonte (consume):**", load_info.get("frete_consume"))
                st.write("**frete_arquivo:**", load_info.get("frete_arquivo"))
                st.write("**Alvo materializado (path/URL resolvido):**", load_info.get("frete_materialized_target"))
                st.write("**Rótulo vendas (UI):**", meta.get("vendas_arquivo", load_info.get("vendas_arquivo")))
                st.write(
                    "**Próximo passo:**",
                    "etapas 2–4 em _render_frete_operacional_ui (filtros → KPIs → export/detalle)",
                )
        except Exception as exc:
            st.error("Diagnóstico Frete (expander) falhou — o restante da página tenta continuar.")
            st.caption(str(exc))
    if df_frete.empty:
        if load_info.get("frete_consume") == "materialized":
            if _is_admin:
                st.info(
                    "Não há linhas no **frete materializado** (ficheiro com 0 linhas). "
                    "Regere `dataset_frete_app.csv` com `processing/materialize_cliente_5.ps1` a partir das pastas "
                    "Esquilo/Wood e publique os CSV atualizados no repositório (ou aloje o ficheiro e use URL nos Secrets)."
                )
            else:
                st.info("Sem registos de frete para este recorte. Contacte o suporte se precisar de ajuda.")
        elif _is_admin:
            st.info(
                "Não há linhas de frete para exibir. Verifique o export de vendas ML (modo live) "
                "ou o ficheiro consolidado (modo materializado)."
            )
        else:
            st.info("Sem registos de frete para exibir. Tente mais tarde ou contacte o suporte.")
        return
    try:
        _render_frete_operacional_ui(org_id, df_frete, meta, ts_proc, load_info)
    except Exception as exc:
        st.error("Erro ao renderizar a Conciliação de Frete (detalhe abaixo).")
        st.exception(exc)


def _build_pdf_bytes(df: pd.DataFrame) -> bytes:
    buff = BytesIO()
    c = canvas.Canvas(buff, pagesize=A4)
    w, h = A4
    y = h - 36
    c.setFont("Helvetica-Bold", 11)
    c.drawString(30, y, "Conciliação Operacional - Exportação")
    y -= 20
    c.setFont("Helvetica", 8)
    cols = list(df.columns)
    c.drawString(30, y, " | ".join(cols))
    y -= 12
    for row in df.astype(str).itertuples(index=False):
        line = " | ".join(list(row))
        if len(line) > 170:
            line = line[:167] + "..."
        c.drawString(30, y, line)
        y -= 11
        if y < 40:
            c.showPage()
            y = h - 36
            c.setFont("Helvetica", 8)
    c.save()
    return buff.getvalue()


def _pick_col_by_tokens(columns: list[str], tokens: list[str]) -> str:
    for c in columns:
        n = unicodedata.normalize("NFKD", str(c)).encode("ascii", "ignore").decode().lower()
        if all(t in n for t in tokens):
            return c
    return ""


def _resolve_col_data_emissao(columns: list[str]) -> str:
    """Prioriza o nome literal da tabela final; fallback só se encoding divergir."""
    if "Data de emissão" in columns:
        return "Data de emissão"
    return _pick_col_by_tokens(columns, ["data", "emiss"])


def _parse_data_emissao_final(series: pd.Series) -> pd.Series:
    """
    Tabela final persiste emissão como YYYY-MM-DD (integracao_notas + etapa4b).
    Parse fixo — evita ambiguidade de dayfirst na UI.
    """
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    s = s.mask(s.str.lower().isin({"none", "nan", "nat", "<na>", "null"}), "")
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def _parse_data_pagamento_final(series: pd.Series) -> pd.Series:
    """Tabela final: parse robusto (ISO/mixed, com/sem timezone)."""
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    s = s.mask(s.str.lower().isin({"none", "nan", "nat", "<na>", "null"}), "")
    t = pd.to_datetime(s, errors="coerce", format="mixed", utc=True)
    try:
        t = t.dt.tz_convert(_BR_TZ).dt.tz_localize(None)
    except Exception:  # noqa: BLE001
        t = pd.to_datetime(s, errors="coerce", format="mixed")
    return t


def _first_series(df: pd.DataFrame, col: str) -> pd.Series:
    """
    Retorna a primeira série quando há colunas duplicadas com o mesmo nome.
    """
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def _drop_duplicate_columns_keep_first(df: pd.DataFrame) -> pd.DataFrame:
    """Remove colunas com nome duplicado, preservando a primeira ocorrência."""
    if df.empty:
        return df
    return df.loc[:, ~df.columns.duplicated()].copy()


def _repasse_ui_validacao_kpi_saas(contagens: dict[str, int]) -> None:
    """KPIs da base filtrada — `st.metric` dentro de contentores com borda (UI nativa)."""
    if _repasse_vendas_liberacoes_only():
        baixado = int(contagens.get("Baixado", 0))
        diverg = int(contagens.get("Analisar diferença", 0))
        sem_pag = int(contagens.get("Verificar recebimento", 0))
        zero = int(contagens.get("Zerado", 0))
        c1, c2, c3, c4 = st.columns(4)
        specs: list[tuple[Any, str, int]] = [
            (c1, "Baixado", baixado),
            (c2, "Divergências", diverg),
            (c3, "Sem pagamento", sem_pag),
            (c4, "Zerados", zero),
        ]
        for col, label, val in specs:
            with col:
                st.metric(label, _fmt_int_ptbr(val))
        return

    ok = int(contagens.get("Ok", 0))
    bling = int(contagens.get("Baixado", 0)) if _repasse_sem_bling() else int(contagens.get("Baixar no Bling", 0))
    div = int(contagens.get("Analisar diferença", 0))
    zero = int(contagens.get("Zerado", 0))
    c2_label = "Baixado" if _repasse_sem_bling() else "Baixar no Bling"
    c1, c2, c3, c4 = st.columns(4)
    specs: list[tuple[Any, str, int]] = [
        (c1, "OK", ok),
        (c2, c2_label, bling),
        (c3, "Divergências", div),
        (c4, "Zerados", zero),
    ]
    for col, label, val in specs:
        with col:
            st.metric(label, _fmt_int_ptbr(val))


def _painel_conciliacao_fragment(base: pd.DataFrame, ts_proc: str) -> None:
    """
    Filtros + validação de ações + fila/tabela de repasse.

    Não usar @st.fragment aqui: ao mudar para «Frete», o fragment deixava de ser invocado e o Streamlit
    podia mostrar ecrã em branco (desincronização da árvore de widgets entre vistas).
    """
    if base.empty or "Data de pagamento" not in base.columns:
        st.warning("Sem dados de repasse para esta vista. Contacte o suporte se o problema continuar.")
        return

    # Chaves de widget por org: sem isto, ao mudar de empresa o estado do Streamlit podia manter
    # limites/valores de outra org e parecer que o calendário «começa» na data errada.
    _rep_wk = _frete_org_widget_suffix(_active_org.org_id)

    with st.container(border=True):
        st.subheader("Filtros")
        st.caption("Período, critérios e busca para refinar o recorte.")
        st.write("")
        dp_series_full = pd.to_datetime(base["Data de pagamento"], errors="coerce")
        _d_min, _d_max, has_dp_base = _series_datetime_bounds_dates(dp_series_full)
        # O Streamlit mantém data_input no session_state: após rematerializar com datas mais recentes,
        # o «Fim» podia ficar preso (ex.: 23/03) embora a base já vá até 31/03. Repor início/fim quando
        # os limites reais da coluna mudarem.
        _bounds_sig_key = f"op_repasse_dp_bounds_sig_{_rep_wk}"
        _bounds_sig = (_d_min.isoformat(), _d_max.isoformat())
        if st.session_state.get(_bounds_sig_key) != _bounds_sig:
            st.session_state[_bounds_sig_key] = _bounds_sig
            st.session_state[f"op_repasse_d_pag_ini_{_rep_wk}"] = _d_min
            st.session_state[f"op_repasse_d_pag_fim_{_rep_wk}"] = _d_max
        plats = (
            sorted([x for x in base["Plataforma"].dropna().unique().tolist() if str(x).strip()])
            if "Plataforma" in base.columns
            else []
        )
        st.markdown("**Período** · data de pagamento")
        st.caption("Comparação por dia civil (meia-noite a meia-noite).")
        r2 = st.columns((1.15, 1.15))
        with r2[0]:
            st.caption("Início")
            data_pag_ini = st.date_input(
                " ",
                value=_d_min,
                min_value=_d_min,
                max_value=_d_max,
                format="DD/MM/YYYY",
                label_visibility="collapsed",
                key=f"op_repasse_d_pag_ini_{_rep_wk}",
            )
        with r2[1]:
            st.caption("Fim")
            data_pag_fim = st.date_input(
                " ",
                value=_d_max,
                min_value=_d_min,
                max_value=_d_max,
                format="DD/MM/YYYY",
                label_visibility="collapsed",
                key=f"op_repasse_d_pag_fim_{_rep_wk}",
            )
        data_pag_ini = _safe_streamlit_date(data_pag_ini, _d_min)
        data_pag_fim = _safe_streamlit_date(data_pag_fim, _d_max)
        st.write("")
        st.markdown("**Critérios**")
        r1 = st.columns((1.15, 1.15, 1.15))
        with r1[0]:
            sel_plat = _multiselect_stable(
                f"op_ms_plat_{_rep_wk}", "Plataforma", plats, compact_label=True
            )
        # Recalcula opções dependentes da plataforma selecionada para evitar filtros «presos»
        # de outra plataforma (ex.: seleção anterior de ML zerando Shopee).
        base_opts = base.copy()
        if "Plataforma" in base_opts.columns and sel_plat:
            base_opts = base_opts[base_opts["Plataforma"].isin(sel_plat)].copy()
        acoes = sorted(
            [
                x
                for x in base_opts["Ação sugerida operacional"].dropna().unique().tolist()
                if str(x).strip()
            ]
        )
        sit = sorted(
            [x for x in base_opts["Situação"].dropna().unique().tolist() if str(x).strip()]
        )
        with r1[1]:
            sel_acao = _multiselect_stable(
                f"op_ms_acao_{_rep_wk}", "Ação sugerida", acoes, compact_label=True
            )
        with r1[2]:
            sel_sit = _multiselect_stable(f"op_ms_sit_{_rep_wk}", "Situação", sit, compact_label=True)
        st.write("")
        st.markdown("**Busca**")
        busca = st.text_input(
            "Texto (venda, pedido ou nota)",
            placeholder="Venda, pedido ou nota…",
            label_visibility="collapsed",
            key=f"op_repasse_busca_txt_{_rep_wk}",
        ).strip().lower()
        if not has_dp_base:
            st.info(
                "Sem datas de pagamento na base: o período não filtra linhas (todas as vendas aparecem). "
                "Com datas preenchidas, o filtro por período passa a aplicar-se."
            )

    st.divider()
    _fdl_ui_gap_section()

    if data_pag_fim < data_pag_ini:
        st.warning("A data final não pode ser anterior à data inicial. Ajuste o período.")
        data_pag_fim = data_pag_ini
    
    tabela = base.copy()
    if "Plataforma" in tabela.columns and sel_plat:
        tabela = tabela[tabela["Plataforma"].isin(sel_plat)]
    if sel_acao:
        tabela = tabela[tabela["Ação sugerida operacional"].isin(sel_acao)]
    if sel_sit:
        tabela = tabela[tabela["Situação"].isin(sel_sit)]
    if busca:
        m_busca = (
            tabela["N° de venda"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
            | tabela["ID do pedido"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
            | tabela["Número da nota"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        )
        tabela = tabela[m_busca]
    
    _dp_filt = pd.to_datetime(tabela["Data de pagamento"], errors="coerce")
    # Sem nenhuma data parseável (ex.: CSV materializado com coluna vazia): não aplicar filtro por período,
    # senão min=max=hoje em conjunto com .notna() elimina todas as linhas.
    if _dp_filt.notna().any():
        _dd = _dp_filt.dt.normalize()
        _ini_ts = pd.Timestamp(data_pag_ini)
        _fim_ts = pd.Timestamp(data_pag_fim) + pd.Timedelta(days=1)
        m_data = _dp_filt.notna() & (_dd >= _ini_ts) & (_dd < _fim_ts)
        tabela = tabela.loc[m_data].copy()
    tabela = _excluir_linhas_fora_conciliacao(tabela)
    
    if "Plataforma" in base.columns:
        n_plat = len(plats)
        # Multiselect vazio = não filtra por plataforma (mostra todas) — não confundir com «nenhuma linha».
        if not n_plat:
            plataforma_label = "—"
        elif len(sel_plat) == 0 or (n_plat and len(sel_plat) == n_plat):
            plataforma_label = "Todas"
        else:
            plataforma_label = ", ".join(sel_plat[:2]) + ("..." if len(sel_plat) > 2 else "")
    else:
        plataforma_label = "Mercado Livre"
    
    _pag_caption = (
        f"Pagamento: **{data_pag_ini.strftime('%d/%m/%Y')}** a **{data_pag_fim.strftime('%d/%m/%Y')}**"
    )
    if not has_dp_base:
        _pag_caption += " — **filtro por data inativo** (sem datas na base)"
    st.caption(
        f"Plataforma: **{plataforma_label}** · Atualizado: **{ts_proc}** · {_pag_caption}"
    )

    st.divider()

    # Tipos numéricos para a base já filtrada (tabela e totais nas colunas)
    tabela["Valor da nota"] = pd.to_numeric(tabela["Valor da nota"], errors="coerce").fillna(0.0)
    tabela["Total BRL"] = pd.to_numeric(tabela.get("Total BRL"), errors="coerce")
    tabela["Valor a receber"] = pd.to_numeric(tabela.get("Valor a receber"), errors="coerce")
    tabela["Valor pago"] = pd.to_numeric(tabela.get("Valor pago"), errors="coerce")
    tabela["Diferença"] = pd.to_numeric(tabela.get("Diferença"), errors="coerce")

    st.subheader("📊 Resumo por ação")
    st.caption("Contagens sobre o recorte filtrado.")
    if _repasse_vendas_liberacoes_only():
        acoes_validacao = ["Baixado", "Analisar diferença", "Verificar recebimento"]
    else:
        acoes_validacao = ["Ok", "Baixado" if _repasse_sem_bling() else "Baixar no Bling", "Analisar diferença"]
    contagens_acao = {a: int(tabela["Ação sugerida operacional"].eq(a).sum()) for a in acoes_validacao}
    contagens_acao["Zerado"] = int(tabela["Ação sugerida operacional"].eq("Revisar venda zerada").sum())
    _repasse_ui_validacao_kpi_saas(contagens_acao)

    _fdl_ui_gap_section()
    st.divider()

    # Tabela operacional — Data de emissão: mesma coluna da tabela final, parse ISO (sem dayfirst).
    col_data_emissao = _resolve_col_data_emissao(list(tabela.columns))
    if _repasse_vendas_liberacoes_only():
        exibir_cols = [
            "N° de venda",
            "Total BRL",
            "Valor a receber",
            "Valor pago",
            "Diferença",
            "Ação sugerida operacional",
            "Plataforma",
        ]
    else:
        exibir_cols = [
            "N° de venda",
            "ID do pedido",
            "Total BRL",
            "Número da nota",
            "Valor da nota",
            "Valor a receber",
            "Diferença",
            "Situação",
            "Ação sugerida operacional",
        ]
    if "Data de pagamento" in tabela.columns:
        exibir_cols.append("Data de pagamento")
    if "Valor pago" in tabela.columns:
        exibir_cols.append("Valor pago")
    
    exibir_cols = [c for c in exibir_cols if c in tabela.columns]
    if not exibir_cols:
        st.warning("Não foi possível apresentar a tabela com o recorte atual.")
        tabela_exibir = pd.DataFrame()
    else:
        tabela_exibir = tabela[exibir_cols].copy()
        tabela_exibir = _drop_duplicate_columns_keep_first(tabela_exibir)
        if "Valor da nota" in tabela_exibir.columns:
            tabela_exibir["Valor da nota"] = pd.to_numeric(
                _first_series(tabela_exibir, "Valor da nota"), errors="coerce"
            )
        else:
            tabela_exibir["Valor da nota"] = 0.0
        if "Valor a receber" in tabela_exibir.columns:
            tabela_exibir["Valor a receber"] = pd.to_numeric(
                _first_series(tabela_exibir, "Valor a receber"), errors="coerce"
            )
        else:
            tabela_exibir["Valor a receber"] = 0.0
        if "Valor pago" in tabela_exibir.columns:
            tabela_exibir["Valor pago"] = pd.to_numeric(
                _first_series(tabela_exibir, "Valor pago"), errors="coerce"
            )
        else:
            tabela_exibir["Valor pago"] = 0.0
        if "Diferença" in tabela_exibir.columns:
            tabela_exibir["Diferença"] = pd.to_numeric(
                _first_series(tabela_exibir, "Diferença"), errors="coerce"
            )
        else:
            tabela_exibir["Diferença"] = 0.0
        if col_data_emissao:
            tabela_exibir["Data de emissão"] = _parse_data_emissao_final(
                tabela.loc[tabela_exibir.index, col_data_emissao]
            )
        else:
            tabela_exibir["Data de emissão"] = pd.NaT
        tabela_exibir["Data de pagamento"] = _parse_data_pagamento_final(
            tabela.loc[tabela_exibir.index, "Data de pagamento"]
            if "Data de pagamento" in tabela.columns
            else pd.Series("", index=tabela_exibir.index)
        )
        tabela_exibir["Valor da nota"] = tabela_exibir["Valor da nota"].fillna(0.0)
        tabela_exibir["Valor a receber"] = tabela_exibir["Valor a receber"].fillna(0.0)
        tabela_exibir["Valor pago"] = tabela_exibir["Valor pago"].fillna(0.0)
        tabela_exibir["Diferença"] = tabela_exibir["Diferença"].fillna(0.0)
        tabela_exibir = tabela_exibir.rename(
            columns={
                "N° de venda": "Número da venda",
                "ID do pedido": "Número do pedido",
                "Ação sugerida operacional": "Ação sugerida",
            }
        )
        tabela_exibir = _drop_duplicate_columns_keep_first(tabela_exibir)
        tabela_exibir = tabela_exibir.drop(columns=["Total BRL"], errors="ignore")
        _ordem_final = [
            "Número da venda",
            "Número do pedido",
            "Número da nota",
            "Data de emissão",
            "Data de pagamento",
            "Valor da nota",
            "Valor a receber",
            "Valor pago",
            "Diferença",
            "Situação",
            "Ação sugerida",
        ]
        tabela_exibir = tabela_exibir[[c for c in _ordem_final if c in tabela_exibir.columns]]
        # Ordenação padrão operacional: pagamentos mais recentes primeiro.
        if not tabela_exibir.empty and "Data de pagamento" in tabela_exibir.columns:
            tabela_exibir = tabela_exibir.sort_values(
                by="Data de pagamento", ascending=False, na_position="last"
            ).reset_index(drop=True)
        for _dc in ("Data de emissão", "Data de pagamento"):
            if _dc in tabela_exibir.columns:
                tabela_exibir[_dc] = pd.to_datetime(tabela_exibir[_dc], errors="coerce")
        for _ref in ("Número da venda", "Número do pedido", "Número da nota"):
            if _ref in tabela_exibir.columns:
                # object + str puro: o Glide Data Grid formata int/float com separadores de milhar
                _txt = _col_referencia_como_texto(tabela_exibir[_ref])
                _obj: list[str | None] = []
                for x in _txt:
                    if pd.isna(x) or x == "":
                        _obj.append(None)
                    else:
                        _obj.append(str(x))
                tabela_exibir[_ref] = pd.Series(_obj, dtype=object, index=tabela_exibir.index)
    
    if _fdl_safe_mode():
        st.warning(
            "**Modo seguro (FDL_SAFE_MODE=1)** — sem fila HTML, sem exports Excel/PDF, "
            "sem `column_config` na tabela (apenas `st.dataframe` simples)."
        )
        st.metric("Linhas (filtro atual)", len(tabela_exibir))
        _grid_safe = _dataframe_conciliacao_somente_grid(tabela_exibir)
        st.dataframe(
            _grid_safe,
            use_container_width=True,
            height=min(560, 140 + max(18 * min(len(tabela_exibir), 80), 120)),
        )
        st.caption("Desative FDL_SAFE_MODE para voltar à UI completa.")
        return

    st.subheader("📋 Fila operacional")
    st.caption("Analise o recorte na grelha; exporte para partilhar fora do sistema.")
    _fdl_ui_gap_section()

    # Guardrail de estabilidade: evita trabalho pesado em cada troca de filtro
    # para não arriscar ecrã em branco por timeout/memória no Streamlit Cloud.
    _max_rows_heavy_export = 3000
    excel_bytes: bytes | None = None
    pdf_bytes: bytes | None = None
    heavy_exports_enabled = len(tabela_exibir) <= _max_rows_heavy_export
    if heavy_exports_enabled:
        try:
            tabela_excel = tabela_exibir.copy()
            excel_buf = BytesIO()
            with pd.ExcelWriter(excel_buf, engine="openpyxl") as writer:
                tabela_excel.to_excel(writer, index=False, sheet_name="Conciliação")
                ws = writer.sheets["Conciliação"]
                header_row = [cell.value for cell in ws[1]]
                for c_data in ("Data de emissão", "Data de pagamento"):
                    if c_data in header_row:
                        col_idx = header_row.index(c_data) + 1
                        for row_idx in range(2, ws.max_row + 1):
                            cell = ws.cell(row=row_idx, column=col_idx)
                            if cell.value is not None:
                                cell.number_format = oxl_number_formats.FORMAT_DATE_DDMMYY
            excel_buf.seek(0)
            excel_bytes = excel_buf.getvalue()
            pdf_bytes = _build_pdf_bytes(tabela_exibir)
        except Exception:  # noqa: BLE001
            heavy_exports_enabled = False

    with st.container(border=True):
        st.caption("Exportar recorte filtrado")
        btn2, btn3 = st.columns([1, 1])
        with btn2:
            if heavy_exports_enabled and excel_bytes is not None:
                st.download_button(
                    "Exportar Excel",
                    data=excel_bytes,
                    file_name="conciliacao_operacional_filtrada.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True,
                )
            else:
                st.button("Exportar Excel", disabled=True, use_container_width=True)
        with btn3:
            if heavy_exports_enabled and pdf_bytes is not None:
                st.download_button(
                    "Exportar PDF",
                    data=pdf_bytes,
                    file_name="conciliacao_operacional_filtrada.pdf",
                    mime="application/pdf",
                    use_container_width=True,
                )
            else:
                st.button("Exportar PDF", disabled=True, use_container_width=True)
        if not heavy_exports_enabled:
            st.caption(
                f"Excel/PDF desativados para recortes acima de {_max_rows_heavy_export:,} linhas (estabilidade)."
            )

    _fdl_ui_gap_section_lg()

    tabela_grid = _dataframe_conciliacao_somente_grid(tabela_exibir)
    _cfg_grid = (
        _column_config_conciliacao(tabela_grid, moeda_como_texto=True)
        if not tabela_grid.empty
        else None
    )
    _disp_grid: object = tabela_grid

    if tabela_exibir.empty:
        st.info(
            "Nenhum registo corresponde aos filtros. Alargue o período, limpe a busca ou ajuste os critérios."
        )
        st.dataframe(
            _disp_grid,
            use_container_width=True,
            height=160,
            hide_index=True,
            column_config=_cfg_grid,
        )
    else:
        st.dataframe(
            _disp_grid,
            use_container_width=True,
            height=550,
            hide_index=True,
            column_config=_cfg_grid,
        )
    st.caption(f"{len(tabela_exibir)} linhas no filtro atual.")

_admin_mode = _is_admin_mode()

_fv = st.session_state["op_financeiro_view"]
_fdl_product_area = str(st.session_state.get(SESSION_FDL_PRODUCT_AREA_KEY, FDL_PRODUCT_AREA_FINANCEIRO))
if _fdl_product_area not in (
    FDL_PRODUCT_AREA_FINANCEIRO,
    FDL_PRODUCT_AREA_FATURAMENTO_DRE,
    FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS,
):
    _fdl_product_area = FDL_PRODUCT_AREA_FINANCEIRO
    st.session_state[SESSION_FDL_PRODUCT_AREA_KEY] = _fdl_product_area
_fdl_global_trace(f"rerun: area={_fdl_product_area} vista={_fv}")
frete_df = pd.DataFrame()
frete_info: dict[str, object] = {}
faturamento_df = pd.DataFrame()
faturamento_info: dict[str, object] = {}
if _fv == "frete":
    # Ponto único de carga Frete (materializado → live); não carrega repasse/precomputed.
    try:
        _fdl_global_trace("frete: a carregar _load_frete_data")
        with st.spinner("A carregar dados de Frete…"):
            frete_df, frete_info, ts_proc = _load_frete_data(_active_org.org_id)
        if _admin_mode:
            if frete_info.get("frete_consume") == "live_fallback":
                st.warning(
                    "Frete: ficheiro consolidado indisponível ou com erro — em uso **fonte em tempo real** (fallback)."
                )
                st.caption(f"Path/URL tentado: `{frete_info.get('frete_materialized_target', '')}`")
                st.caption(f"Erro: {frete_info.get('frete_materialized_error', '')}")
            elif frete_info.get("frete_consume") == "materialized":
                _t_disp = str(frete_info.get("frete_materialized_target", ""))[:500]
                st.caption(f"Frete: ficheiro consolidado (`{_t_disp}`).")
            elif frete_info.get("frete_mat_note"):
                st.info(str(frete_info["frete_mat_note"]))
    except Exception as exc:
        if _strict_materialized() and isinstance(exc, ValueError):
            st.error(str(exc))
            st.stop()
        err_text = str(exc).strip() or exc.__class__.__name__
        if _expose_load_errors():
            st.error("Erro ao carregar os dados de Frete.")
            st.exception(exc)
        elif _admin_mode:
            st.warning("Dados de Frete indisponíveis no momento.")
            st.caption(f"Detalhe técnico: {exc}")
        else:
            st.warning("Dados indisponíveis no momento. Tente novamente em instantes.")
            with st.expander("Detalhes para suporte", expanded=False):
                st.code(err_text, language="text")
        st.stop()

    tabela_geral = pd.DataFrame()
    info = frete_info
    _fdl_global_trace("frete: dados carregados")
elif _fdl_product_area in (
    FDL_PRODUCT_AREA_FATURAMENTO_DRE,
    FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS,
) and "faturamento" in _enabled_modules:
    _allowed_org_key = ",".join(sorted(o.org_id for o in _app_ctx.organizations))
    _fdl_global_trace("faturamento_dre: a carregar _load_faturamento_dataframe_cached")
    with st.spinner("A carregar dados de Faturamento…"):
        faturamento_df, faturamento_info, ts_proc = _load_faturamento_dataframe_cached(
            _faturamento_load_cache_signature(
                FAT_DRE_CACHE_ACTIVE_ORG_PLACEHOLDER,
                consolidado=True,
                allowed_org_ids_key=_allowed_org_key,
            ),
            FAT_DRE_CACHE_ACTIVE_ORG_PLACEHOLDER,
            True,
            _allowed_org_key,
        )
    fc = str(faturamento_info.get("faturamento_consume", "")).strip()
    if fc == "materialized" and _admin_mode:
        _t_disp = str(faturamento_info.get("faturamento_materialized_target", ""))[:500]
        _lay = str(faturamento_info.get("faturamento_data_layout", ""))
        _src = str(faturamento_info.get("faturamento_resolution_source", ""))
        _pf = str(faturamento_info.get("faturamento_path_final_resolved", ""))[:500]
        _pf_full = str(faturamento_info.get("faturamento_path_final_resolved", "")).strip()
        _n_loaded = faturamento_info.get("faturamento_row_count_loaded")
        st.caption(
            f"Faturamento: layout **{_lay}** · origem `{_src}` · alvo=`{_t_disp}` · path=`{_pf}`"
        )
        with st.expander("Admin — ficheiro lido e validação", expanded=False):
            st.markdown(
                "| Campo | Valor |\n| --- | --- |\n"
                f"| **faturamento_resolution_source** | `{_src}` |\n"
                f"| **faturamento_row_count_loaded** | `{_n_loaded}` |\n"
            )
            st.code(_pf_full or "(vazio)", language="text")
            st.caption("**faturamento_path_final_resolved** (completo, acima).")
            _slug = str(_materialized_cliente_slug()).strip()
            _expect_marker = f"data_products/{_slug}/faturamento/current/dataset_faturamento_app.csv".casefold()
            _norm = _pf_full.replace("\\", "/").casefold()
            _expect_pq = f"data_products/{_slug}/faturamento/current/dataset.parquet".casefold()
            if _slug and (_expect_marker in _norm or _expect_pq in _norm):
                st.success(
                    f"Path alinha ao V2 canónico em `data_products/{_slug}/faturamento/current/` "
                    "(CSV ou Parquet, conforme existir no disco)."
                )
            elif _pf_full:
                st.warning(
                    "O path **não** contém o sufixo canónico esperado para o slug atual — confirme **FDL_MATERIALIZED_CLIENTE_SLUG** e **FDL_FATURAMENTO_MATERIALIZED_PATH**."
                )
            if _n_loaded is not None:
                st.markdown(_faturamento_admin_metadata_rowcount_message(_pf_full, int(_n_loaded)))
        _cnt = faturamento_info.get("faturamento_status_custo_counts")
        if isinstance(_cnt, dict) and _cnt:
            st.caption(
                f"**Status_Custo** após escopo (**{faturamento_info.get('faturamento_escopo', '—')}**): `{_cnt}` · "
                f"linhas carregadas: **{faturamento_info.get('faturamento_row_count_loaded', '—')}** → "
                f"**{faturamento_info.get('linhas', '—')}** após escopo."
            )
        if faturamento_info.get("faturamento_escopo") == FAT_DRE_ESCOPO_CONSOLIDADO:
            st.caption(
                "**Consolidado:** conjunto = todas as organizações permitidas ao utilizador (intersecção com `org_id` no ficheiro). "
                "A **Empresa** na barra lateral não restringe este conjunto; o recorte por marca é o multiselect **Empresa** no módulo."
            )
        st.caption(
            "**V2 canónico:** `data_products/<FDL_MATERIALIZED_CLIENTE_SLUG>/faturamento/current/` — com **CSV e Parquet**, "
            "o app lê o **CSV** primeiro. Sem path explícito, este ficheiro tem **prioridade** sobre o CSV «irmão do repasse» "
            "(join fiscal: Nota_Data_Emissao, etc.). O **slug** tem de bater com a materialização (ex. **cliente_5**)."
        )
        if faturamento_info.get("faturamento_scope_note"):
            st.caption(str(faturamento_info["faturamento_scope_note"]))
        if faturamento_info.get("faturamento_note_v2_canonical_available"):
            st.info(str(faturamento_info["faturamento_note_v2_canonical_available"]))
        with st.expander("Debug — `faturamento_info` (materializado)", expanded=False):
            try:
                st.json(json.loads(json.dumps(faturamento_info, default=str)))
            except (TypeError, ValueError):
                st.write({k: str(v)[:2000] for k, v in faturamento_info.items()})
    elif fc == "missing_config":
        if _admin_mode:
            st.warning(
                str(
                    faturamento_info.get(
                        "faturamento_note",
                        "Dados de faturamento não configurados ou não encontrados.",
                    )
                )
            )
        else:
            st.warning("Dados de faturamento não disponíveis. Contacte o administrador.")
    elif fc == "error":
        st.warning("Não foi possível carregar os dados de **Faturamento**.")
        if _admin_mode:
            st.caption(str(faturamento_info.get("faturamento_materialized_error", "")))
            st.caption(f"Alvo: `{faturamento_info.get('faturamento_materialized_target', '')}`")
    elif fc == "unsupported":
        if _admin_mode:
            st.warning(str(faturamento_info.get("faturamento_note", "Modo de consumo não suportado.")))
        else:
            st.warning("Esta vista não está disponível na configuração atual. Contacte o administrador.")

    if not faturamento_df.empty and "empresa" not in faturamento_df.columns:
        if str(faturamento_info.get("faturamento_data_layout", "")).strip() != "v2":
            faturamento_df = faturamento_df.copy()
            faturamento_df["empresa"] = _dataset_empresa_label()

    if not faturamento_df.empty:
        faturamento_df = _filtrar_df_col_empresa_por_contexto(faturamento_df)

    _nf_ctx = faturamento_info.get("faturamento_nf_df")
    if isinstance(_nf_ctx, pd.DataFrame):
        faturamento_info = {
            **faturamento_info,
            "faturamento_nf_df": _filtrar_df_col_empresa_por_contexto(_nf_ctx),
        }

    faturamento_info = {**faturamento_info, "linhas": int(len(faturamento_df))}
    if not str(faturamento_info.get("faturamento_escopo") or "").strip():
        faturamento_info = {**faturamento_info, "faturamento_escopo": FAT_DRE_ESCOPO_CONSOLIDADO}
    tabela_geral = pd.DataFrame()
    info = faturamento_info
    _fdl_global_trace(f"faturamento_dre: após filtro empresa ({len(faturamento_df)} linhas)")
else:
    try:
        _fdl_global_trace("repasse: a carregar _load_data (cache por org/config)")
        with st.spinner("A carregar dados (a ir buscar o ficheiro à nuvem, se aplicável)…"):
            tabela_geral, info, ts_proc = _load_repasse_dataframe_cached(
                _repasse_load_cache_signature(_active_org.org_id)
            )
            if _admin_mode:
                if info.get("repasse_consume") == "live_fallback":
                    st.warning(
                        "Repasse: ficheiro consolidado indisponível ou com erro — em uso **fonte em tempo real** (fallback)."
                    )
                    st.caption(f"Path/URL tentado: `{info.get('repasse_materialized_target', '')}`")
                    st.caption(f"Erro: {info.get('repasse_materialized_error', '')}")
                elif info.get("repasse_materialized_note"):
                    st.info(str(info["repasse_materialized_note"]))
                elif info.get("repasse_consume") == "materialized":
                    st.caption(
                        f"Repasse: ficheiro consolidado (`{info.get('repasse_materialized_target', '')}`)."
                    )
    except Exception as exc:
        if _strict_materialized() and isinstance(exc, ValueError):
            st.error(str(exc))
            st.stop()
        err_text = str(exc).strip() or exc.__class__.__name__
        if _expose_load_errors():
            st.error("Erro ao carregar os dados. Ajuste Secrets/URL ou use o detalhe abaixo.")
            st.exception(exc)
        elif _admin_mode:
            st.warning("Dados indisponíveis no momento.")
            st.caption(f"Detalhe técnico: {exc}")
        else:
            st.warning("Dados indisponíveis no momento. Tente novamente em instantes.")
            with st.expander("Detalhes para suporte", expanded=False):
                st.code(err_text, language="text")
        od_url = _onedrive_public_url()
        if (
            _admin_mode
            and _data_source_mode() in {"onedrive", "filesystem"}
            and od_url
            and ":f:/" in od_url.lower()
        ):
            st.info(
                "A configuração atual usa **link de pasta** do SharePoint (`:f:/`), que depende de "
                "acesso à API Microsoft e costuma falhar na Streamlit Cloud. "
                "Use **FDL_DATA_SOURCE = \"precomputed\"** com `FDL_PRECOMPUTED_URL` (link direto ao "
                "`.csv` ou `.xlsx`) ou `FDL_PRECOMPUTED_PATH` no servidor."
            )
        st.stop()

    # Cache antigo do Streamlit ou pickle sem a coluna — alinhar ao pipeline atual.
    if "empresa" not in tabela_geral.columns:
        tabela_geral = tabela_geral.copy()
        tabela_geral["empresa"] = _dataset_empresa_label()

    tabela_geral = _filtrar_df_col_empresa_por_contexto(tabela_geral)
    info = {**info, "linhas": int(len(tabela_geral))}
    _fdl_global_trace(f"repasse: após filtro empresa ({len(tabela_geral)} linhas)")

try:
    _ts_raw = str(ts_proc).strip() if ts_proc is not None else ""
    _sb_ts_display = datetime.strptime(_ts_raw, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
except (ValueError, TypeError, OSError):
    _sb_ts_display = str(ts_proc) if ts_proc is not None else "—"

_fdl_global_trace("04: antes da sidebar (dados carregados)")
_inject_fdl_professional_theme()
if _bootstrap_debug_enabled() and _admin_mode:
    with st.expander("Diagnóstico bootstrap (FDL_DEBUG_BOOTSTRAP=1)", expanded=True):
        st.write("**Última etapa:**", st.session_state.get("_fdl_bootstrap_stage", "—"))
        st.write("**Área:**", _fdl_product_area, "· **Vista financeiro:**", _fv)
        st.write("**Modo seguro (FDL_SAFE_MODE):**", _fdl_safe_mode())
        st.write("**Layout mínimo (FDL_MINIMAL_LAYOUT, omisso=on):**", _fdl_minimal_layout())
        _n_linhas_dbg = (
            len(tabela_geral)
            if _fv == "repasse"
            else (
                len(faturamento_df)
                if _fdl_product_area
                in (FDL_PRODUCT_AREA_FATURAMENTO_DRE, FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS)
                else "— (vista frete)"
            )
        )
        st.write("**Linhas tabela_geral (repasse) / faturamento_df:**", _n_linhas_dbg)
        _lg = st.session_state.get("_fdl_bootstrap_log")
        if isinstance(_lg, list) and _lg:
            st.write("**Log de etapas (esta execução):**")
            for _i, _line in enumerate(_lg, 1):
                st.caption(f"{_i}. {_line}")

with st.sidebar:
    _fdl_sidebar_inject_layout_css()
    _sb_view = st.session_state.get("op_financeiro_view", "repasse")
    _sb_area = st.session_state.get(SESSION_FDL_PRODUCT_AREA_KEY, FDL_PRODUCT_AREA_FINANCEIRO)

    st.write("")
    _logo_file = _REPO_APP_ROOT / "assets" / "fdl_analytics_logo.png"
    _has_logo = _logo_file.is_file()
    _sp_l, _sp_c, _sp_r = st.columns([0.35, 3.3, 0.35])
    with _sp_c:
        if _has_logo:
            st.image(str(_logo_file), use_container_width=True)

    _cli_raw = str(st.session_state.get("cliente", "") or _app_ctx.display_name or "").strip()
    if not _cli_raw:
        _u = str(st.session_state.get("usuario", "") or "").strip()
        _cli_raw = _u.split("@", 1)[0] if "@" in _u else (_u or "Conta")
    _cli_nome = html.escape(_cli_raw)
    _sb_tagline = "Da operação ao insight"
    if _has_logo:
        _brand_inner = (
            f'<div class="fdl-sb-tagline fdl-sb-tagline--after-logo">{html.escape(_sb_tagline)}</div>'
            '<div class="fdl-sb-client-row"><div class="fdl-sb-client-block">'
            '<span class="fdl-sb-client-tag">Conta</span>'
            f'<span class="fdl-sb-client-name">{_cli_nome}</span>'
            "</div></div>"
        )
    else:
        _brand_inner = (
            '<div class="fdl-sb-product">FDL Analytics</div>'
            f'<div class="fdl-sb-tagline">{html.escape(_sb_tagline)}</div>'
            '<div class="fdl-sb-client-row"><div class="fdl-sb-client-block">'
            '<span class="fdl-sb-client-tag">Conta</span>'
            f'<span class="fdl-sb-client-name">{_cli_nome}</span>'
            "</div></div>"
        )
    st.markdown(
        '<div class="fdl-sb-brand-shell">'
        f'<div class="fdl-sb-brand">{_brand_inner}</div>'
        '</div>'
        '<div class="fdl-sb-divider" aria-hidden="true"></div>',
        unsafe_allow_html=True,
    )

    _empresas_usuario = list(st.session_state["empresas_permitidas"])
    _nomes_nav = nomes_permitidos_com_registro(_empresas_usuario)

    _has_gerencial = "faturamento" in _enabled_modules
    _has_operacional = "repasse" in _enabled_modules or "frete" in _enabled_modules
    _first_nav_section = True

    _lbl_repasse = "Conciliação de Repasse"
    _lbl_frete = "Conciliação de Frete"
    _lbl_fat_dre = "Faturamento & DRE"

    if _has_gerencial:
        _sec_cls = (
            "fdl-sb-section-label fdl-sb-section-label--first"
            if _first_nav_section
            else "fdl-sb-section-label"
        )
        st.markdown(f'<p class="{_sec_cls}">Gerencial</p>', unsafe_allow_html=True)
        _first_nav_section = False
        st.button(
            _lbl_fat_dre,
            key="fdl_mod_faturamento_dre",
            use_container_width=True,
            type="primary" if _sb_area == FDL_PRODUCT_AREA_FATURAMENTO_DRE else "secondary",
            on_click=_sb_nav_set_faturamento_dre,
            help=(
                "Visão de negócio. Carga consolidada das organizações permitidas; escolha uma ou mais "
                "marcas (ou todas) no filtro **Empresa** dentro do painel."
            ),
        )
        st.button(
            "Comercial & pedidos",
            key="fdl_mod_comercial_pedidos",
            use_container_width=True,
            type="primary" if _sb_area == FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS else "secondary",
            on_click=_sb_nav_set_comercial_pedidos,
            help=(
                "Análise comercial sobre pedidos atendidos (Data, Preço de lista × Quantidade), sem NF. "
                "Filtros no painel; base consolidada como Faturamento & DRE."
            ),
        )

    if _has_operacional:
        _sec_cls = (
            "fdl-sb-section-label fdl-sb-section-label--first"
            if _first_nav_section
            else "fdl-sb-section-label"
        )
        st.markdown(f'<p class="{_sec_cls}">Operacional</p>', unsafe_allow_html=True)
        _first_nav_section = False

        if _sb_area == FDL_PRODUCT_AREA_FINANCEIRO and _nomes_nav:
            st.markdown(
                '<p class="fdl-sb-org-hint">Repasse · Frete</p>',
                unsafe_allow_html=True,
            )
            _org_idx = 0
            for i, n in enumerate(_nomes_nav):
                _o = organizacao_por_nome_cadastrado(n)
                if _o and _o.org_id == _app_ctx.active_org_id:
                    _org_idx = i
                    break
            _sel_nome = st.selectbox(
                "Empresa ativa",
                options=_nomes_nav,
                index=_org_idx,
                key="operacional_empresa_ativa_select",
                label_visibility="visible",
                help=(
                    "Define qual organização carregar para conciliação de Repasse e Frete. "
                    "Em Faturamento e Comercial, o recorte por marca fica no filtro Empresa do painel."
                ),
            )
            _chosen_org = organizacao_por_nome_cadastrado(_sel_nome)
            if _chosen_org and _chosen_org.org_id != _app_ctx.active_org_id:
                st.session_state[SESSION_ACTIVE_ORG_KEY] = _chosen_org.org_id
                st.rerun()

        if "repasse" in _enabled_modules:
            st.button(
                _lbl_repasse,
                key="fdl_mod_repasse",
                use_container_width=True,
                type="primary"
                if _sb_area == FDL_PRODUCT_AREA_FINANCEIRO and _sb_view == "repasse"
                else "secondary",
                on_click=_sb_nav_set_repasse,
            )
        if "frete" in _enabled_modules:
            st.button(
                _lbl_frete,
                key="fdl_mod_frete",
                use_container_width=True,
                type="primary"
                if _sb_area == FDL_PRODUCT_AREA_FINANCEIRO and _sb_view == "frete"
                else "secondary",
                on_click=_sb_nav_set_frete,
            )

    st.markdown('<div class="fdl-sb-footer-rule" aria-hidden="true"></div>', unsafe_allow_html=True)
    _ts_parts = str(_sb_ts_display).strip().split(None, 1)
    _ts_d = _ts_parts[0] if _ts_parts else "—"
    _ts_t = _ts_parts[1] if len(_ts_parts) > 1 else ""
    _ts_line = f"{_ts_d} • {_ts_t}" if _ts_t else _ts_d
    st.markdown(
        '<div class="fdl-sb-footer">'
        '<p class="fdl-sb-footer-label">Atualizado em</p>'
        f'<p class="fdl-sb-footer-ts">{html.escape(_ts_line)}</p>'
        "</div>",
        unsafe_allow_html=True,
    )
    if _admin_mode:
        st.markdown(
            f'<p class="fdl-sb-footer-admin">{html.escape(_sidebar_version_display())}</p>',
            unsafe_allow_html=True,
        )

    if _admin_mode and _data_source_mode() == "upload_zip":
        _render_cloud_data_loader()

    st.markdown('<div class="fdl-sb-footer-spacer"></div>', unsafe_allow_html=True)
    st.divider()

    if _admin_mode and st.button(
        "Atualizar dados",
        use_container_width=True,
        help="Limpa caches e recarrega os dados a partir da fonte configurada.",
        key="fdl_sb_admin_refresh",
        type="primary",
    ):
        st.cache_data.clear()
        for _k in list(st.session_state.keys()):
            if str(_k).startswith("_frete_cache_"):
                st.session_state.pop(_k, None)
        st.rerun()

    _lo1, _lo2, _lo3 = st.columns([1, 2.2, 1])
    with _lo2:
        st.button(
            "Sair",
            use_container_width=True,
            help="Encerra a sessão neste navegador.",
            type="secondary",
            key="fdl_sb_logout",
            on_click=_sb_logout_click,
        )

_fdl_global_trace("05: após sidebar — antes do hero / painel principal")

if _fv == "repasse" and _fdl_product_area == FDL_PRODUCT_AREA_FINANCEIRO:
    try:
        _fdl_global_trace("repasse: a preparar base (map_acao / filtros negócio)")
        _acao_baixa = "Baixado" if _repasse_sem_bling() else "Baixar no Bling"
        map_acao = {
            "Ok": "Ok",
            "Baixar no Bling": _acao_baixa,
            "Baixado": _acao_baixa,
            "Analisar manualmente": "Analisar diferença",
            "Verificar título no Bling": "Verificar recebimento",
            "Revisar venda zerada": "Revisar venda zerada",
            "Verificar faturamento": "Verificar faturamento",
        }
        tabela_geral["Ação sugerida operacional"] = (
            tabela_geral["Ação sugerida"].map(map_acao).fillna(tabela_geral["Ação sugerida"])
        )
        tabela = tabela_geral.copy()

        # Mantém também linhas sem pagamento para não ocultar plataformas
        # em cenários onde o extrato ainda não foi consolidado.
        tabela["Valor pago"] = pd.to_numeric(tabela.get("Valor pago"), errors="coerce")

        # Exibição operacional focada em vendas:
        # mantém somente linhas com N° de venda preenchido.
        tabela["N° de venda"] = tabela["N° de venda"].fillna("").astype(str).str.strip()
        tabela = tabela[tabela["N° de venda"].ne("")].copy()

        # Base operacional antes dos filtros da UI (mesma regra de negócio de sempre).
        tabela_operacional_base = tabela.copy()
        _fdl_global_trace(f"repasse: base pronta ({len(tabela_operacional_base)} linhas)")
    except Exception as exc:
        _fdl_global_trace(f"repasse: ERRO na preparação da base — {exc.__class__.__name__}")
        st.error("Erro ao preparar a base de **Conciliação de Repasse** (colunas ou dados incompatíveis).")
        st.exception(exc)
        tabela_operacional_base = pd.DataFrame()
else:
    tabela_operacional_base = pd.DataFrame()

if _fdl_product_area == FDL_PRODUCT_AREA_FATURAMENTO_DRE and "faturamento" in _enabled_modules:
    _render_financeiro_header(
        segment="Painel",
        title="Faturamento & DRE",
        subtitle="Etapa 1 — filtros mínimos, KPIs e tabela por venda (mesmo recorte no CSV).",
        kicker_area="Faturamento & DRE",
        compact_spacing=True,
    )
elif _fdl_product_area == FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS and "faturamento" in _enabled_modules:
    _render_financeiro_header(
        segment="Comercial",
        title="Comercial & pedidos",
        subtitle="KPIs, ABC e tendência sobre pedidos atendidos (Preço de lista × Quantidade); sem NF.",
        kicker_area="Comercial & pedidos",
        compact_spacing=True,
    )
elif _fv == "repasse":
    _render_financeiro_header(
        segment="Repasse",
        title="Conciliação de Repasse",
        subtitle="Recebimentos, notas e divergências numa única vista.",
    )
else:
    _render_financeiro_header(
        segment="Frete",
        title="Conciliação de Frete",
        subtitle="Frete cobrado na plataforma face ao valor esperado por anúncio.",
    )

if _fv == "repasse" and _fdl_product_area == FDL_PRODUCT_AREA_FINANCEIRO:
    try:
        _fdl_global_trace("repasse: a renderizar _painel_conciliacao_fragment (filtros UI)")
        _painel_conciliacao_fragment(tabela_operacional_base, ts_proc)
        _fdl_global_trace("repasse: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"repasse: ERRO no painel — {exc.__class__.__name__}")
        st.error("Erro ao renderizar a **Conciliação de Repasse** (filtros ou tabela).")
        st.exception(exc)
elif _fdl_product_area == FDL_PRODUCT_AREA_FATURAMENTO_DRE and "faturamento" in _enabled_modules:
    try:
        _fdl_global_trace("faturamento_dre: vista mínima Etapa 1")
        _render_faturamento_dre_minimal(
            faturamento_df,
            faturamento_info,
            ts_proc,
            org_id=_active_org.org_id,
            org_display_name=_active_org.display_name,
        )
        _fdl_global_trace("faturamento_dre: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"faturamento_dre: ERRO no painel — {exc.__class__.__name__}")
        st.error("Erro ao renderizar **Faturamento & DRE**.")
        st.exception(exc)
elif _fdl_product_area == FDL_PRODUCT_AREA_COMERCIAL_PEDIDOS and "faturamento" in _enabled_modules:
    try:
        _fdl_global_trace("comercial_pedidos: painel")
        _render_comercial_pedidos_analise(faturamento_df, faturamento_info, ts_proc)
        _fdl_global_trace("comercial_pedidos: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"comercial_pedidos: ERRO — {exc.__class__.__name__}")
        st.error("Erro ao renderizar **Comercial & pedidos**.")
        st.exception(exc)
elif _fv == "frete":
    try:
        _fdl_global_trace("frete: a renderizar _painel_frete_emergencial")
        _painel_frete_emergencial(_active_org.org_id, frete_df, frete_info, ts_proc)
        _fdl_global_trace("frete: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"frete: ERRO no painel — {exc.__class__.__name__}")
        st.error("Não foi possível carregar o painel de Frete.")
        st.exception(exc)

_fdl_global_trace("99: fim do script app_operacional")

