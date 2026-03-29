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

from carregamento_bases import PIPELINE_DATA_REVISION
from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional
from operacional_app_context import (
    SESSION_ACTIVE_ORG_KEY,
    get_active_organization,
    logout_operacional_user,
    nomes_permitidos_com_registro,
    organizacao_por_nome_cadastrado,
    require_app_user,
)
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
    frete_kpis_executivos,
    frete_series_normalize_sale_dt,
    frete_tabela_anuncios_cobrado_maior,
    frete_tabela_anuncios_repasse_frete,
    normalize_frete_status_conc_display,
    stable_mtime_ns_for_frete_url,
    validate_frete_operacional_dataframe,
)
from operacional_frete_ui import _dataframe_frete_grid

_REPO_APP_ROOT = Path(__file__).resolve().parent
BUILD_TAG = "build-20260329-repasse-ui-saas"

_SB_LOGO_MINI_SVG = """
<svg class="fdl-sb-logo-mini" width="38" height="38" viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <defs><linearGradient id="fdlSbLogoGrad" x1="0" y1="0" x2="1" y2="1">
    <stop offset="0%" stop-color="#38bdf8"/><stop offset="100%" stop-color="#0284c7"/>
  </linearGradient></defs>
  <rect width="40" height="40" rx="11" fill="url(#fdlSbLogoGrad)"/>
  <path d="M9 28h5V16H9v12zm8 0h5V10h-5v18zm8-6h5v6h-5v-6zm8-8h5v14h-5V14z" fill="#fff" fill-opacity="0.95"/>
</svg>"""

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


def _filtrar_df_col_empresa_por_contexto(df: pd.DataFrame) -> pd.DataFrame:
    """Em dynamic, restringe à empresa ativa (evita mistura se o CSV estiver errado)."""
    if df.empty or "empresa" not in df.columns:
        return df
    if _materialized_path_mode() == "dynamic":
        return df[df["empresa"] == _active_org.display_name].copy()
    empresas = st.session_state["empresas_permitidas"]
    return df[df["empresa"].isin(empresas)].copy()


if "op_financeiro_view" not in st.session_state:
    st.session_state["op_financeiro_view"] = "repasse"
elif st.session_state["op_financeiro_view"] not in ("repasse", "frete", "faturamento"):
    st.session_state["op_financeiro_view"] = "repasse"

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


def _sb_user_initials(display_name: str) -> str:
    """Iniciais para avatar (máx. 2 caracteres)."""
    parts = [p for p in str(display_name).strip().split() if p]
    if len(parts) >= 2:
        return (parts[0][0] + parts[1][0]).upper()[:2]
    if parts:
        return parts[0][:2].upper()
    return "?"


def _sidebar_brand_logo_html() -> str:
    """Uma única imagem PNG horizontal (marca + texto); proporção preservada. Fallback: ícone SVG."""
    logo_path = _REPO_APP_ROOT / "assets" / "fdl_analytics_logo.png"
    if logo_path.is_file():
        b64 = base64.b64encode(logo_path.read_bytes()).decode("ascii")
        return (
            f'<img src="data:image/png;base64,{b64}" alt="FDL Analytics" '
            'class="fdl-sb-brand-logo-img" loading="eager" decoding="async" />'
        )
    return f'<div class="fdl-sb-logo-fallback" aria-hidden="true">{_SB_LOGO_MINI_SVG}</div>'


def _sb_nav_set_repasse() -> None:
    st.session_state["op_financeiro_view"] = "repasse"


def _sb_nav_set_frete() -> None:
    st.session_state["op_financeiro_view"] = "frete"


def _sb_nav_set_faturamento() -> None:
    st.session_state["op_financeiro_view"] = "faturamento"


def _sb_logout_click() -> None:
    logout_operacional_user()
    st.rerun()


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


_STRICT_MATERIALIZED_USER_MSG = "Base não encontrada. Execute o processo de materialização."


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


def _faturamento_materialized_targets() -> tuple[str, str]:
    """Path/URL explícitos; se vazios, derivado do CSV do repasse (mesmo layout data_products)."""
    mp = _faturamento_materialized_path_str()
    mu = _faturamento_materialized_url_str()
    if mp or mu:
        return mp, mu
    for anchor in (_repasse_materialized_path_str(), _precomputed_path_str()):
        d = _derive_faturamento_materialized_from_repasse_anchor(anchor)
        if d:
            return d, ""
    return "", ""


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


def _faturamento_ts_for_path(path: Path) -> str:
    try:
        return _ts_br_from_mtime_ns(int(path.stat().st_mtime_ns))
    except OSError:
        return _now_ts_br_str()


def _load_faturamento_data() -> tuple[pd.DataFrame, dict[str, object], str]:
    """
    Carrega apenas dataset materializado (CSV/XLSX/Parquet em path ou URL).
    Sem pipeline live nesta fase.
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

    path_s, url_s = _faturamento_materialized_targets()
    if not path_s and not url_s:
        return (
            pd.DataFrame(),
            {
                "faturamento_consume": "missing_config",
                "faturamento_note": (
                    "Defina FDL_FATURAMENTO_MATERIALIZED_PATH ou URL, ou coloque o dataset em "
                    ".../faturamento/current/ alinhado ao repasse materializado."
                ),
            },
            _now_ts_br_str(),
        )

    target = (path_s or url_s)[:500]
    try:
        df = _load_faturamento_materialized_dataframe(path_s, url_s)
        ts = _now_ts_br_str()
        if path_s:
            p = Path(path_s).expanduser()
            if not p.is_absolute():
                p = (_REPO_APP_ROOT / p).resolve()
            if p.is_file():
                ts = _faturamento_ts_for_path(p)
        return (
            df,
            {
                "faturamento_consume": "materialized",
                "faturamento_materialized_target": target,
                "linhas": int(len(df)),
            },
            ts,
        )
    except Exception as exc:
        return (
            pd.DataFrame(),
            {
                "faturamento_consume": "error",
                "faturamento_materialized_error": str(exc).strip() or exc.__class__.__name__,
                "faturamento_materialized_target": target,
            },
            _now_ts_br_str(),
        )


def _faturamento_load_cache_signature(org_id: str) -> str:
    return "|".join(
        [
            str(org_id),
            str(OPERACIONAL_CACHE_REVISION),
            _faturamento_consume_mode(),
            str(_faturamento_materialized_path_str()).strip(),
            str(_faturamento_materialized_url_str()).strip(),
            str(_repasse_materialized_path_str()).strip(),
            str(_precomputed_path_str()).strip(),
            str(_strict_materialized()),
        ]
    )


@st.cache_data(show_spinner=False, ttl=900)
def _load_faturamento_dataframe_cached(load_signature: str) -> tuple[pd.DataFrame, dict[str, object], str]:
    _ = load_signature
    return _load_faturamento_data()


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
    vendas_origin = (fontes.vendas_url or "").strip() or (
        str(fontes.vendas_path.resolve()) if fontes.vendas_path else ""
    )
    if (fontes.vendas_url or "").strip():
        vendas_sig = str(stable_mtime_ns_for_frete_url(fontes.vendas_url))
    elif fontes.vendas_path and fontes.vendas_path.is_file():
        vendas_sig = str(int(fontes.vendas_path.stat().st_mtime_ns))
    else:
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
                f"{_STRICT_MATERIALIZED_USER_MSG} Frete: não foi possível ler o dataset materializado. "
                f"Detalhe: {exc}"
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

    vendas_ref = (fontes.vendas_url or "").strip() or (
        str(fontes.vendas_path.resolve()) if fontes.vendas_path else ""
    )

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
            ts_live = _now_ts_br_str()
            if fontes.vendas_path and fontes.vendas_path.is_file():
                try:
                    ts_live = _frete_ts_for_path(fontes.vendas_path)
                except OSError:
                    pass
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
            elif fontes.vendas_path:
                info_fb["frete_fonte_local_path"] = str(fontes.vendas_path.resolve())
            if (
                _is_admin_mode()
                and _frete_consume_mode() == "materialized"
                and not _f_mp_exp
                and not _f_mu_exp
                and not _derive_frete_materialized_path_from_repasse()
            ):
                info_fb["frete_mat_note"] = (
                    "Frete: **FDL_FRETE_CONSUME_MODE=materialized** sem **FDL_FRETE_MATERIALIZED_PATH** / "
                    "**FDL_FRETE_MATERIALIZED_URL** e sem **dataset_frete_app.csv** ao lado do repasse em "
                    "`.../repasse/current/` — fluxo **live** em uso."
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
        v_ns = (
            stable_mtime_ns_for_frete_url(fontes.vendas_url)
            if (fontes.vendas_url or "").strip()
            else int(fontes.vendas_path.stat().st_mtime_ns)
        )
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
    ts_live = _now_ts_br_str()
    if fontes.vendas_path and fontes.vendas_path.is_file():
        try:
            ts_live = _frete_ts_for_path(fontes.vendas_path)
        except OSError:
            pass
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
    elif fontes.vendas_path:
        info_out["frete_fonte_local_path"] = str(fontes.vendas_path.resolve())
    if (
        _is_admin_mode()
        and _frete_consume_mode() == "materialized"
        and not _f_mp_exp
        and not _f_mu_exp
        and not _derive_frete_materialized_path_from_repasse()
    ):
        info_out["frete_mat_note"] = (
            "Frete: **FDL_FRETE_CONSUME_MODE=materialized** sem **FDL_FRETE_MATERIALIZED_PATH** / "
            "**FDL_FRETE_MATERIALIZED_URL** e sem **dataset_frete_app.csv** ao lado do repasse em "
            "`.../repasse/current/` — fluxo **live** em uso."
        )
    return df_frete, info_out, ts_live


def _prepare_uploaded_base(zip_bytes: bytes) -> Path:
    """Extrai pacote ZIP de dados para o BASE_DIR esperado pelo pipeline."""
    base_dir = Path(BASE_DIR)
    expected_dirs = {"Vendas - Mercado Livre", "Liberações_ML", "notas_saida", "contas_receber"}

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
            has_local_data = all((base_dir / name).exists() for name in REQUIRED_ONEDRIVE_SOURCE_FOLDERS)
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

    missing = [name for name in REQUIRED_ONEDRIVE_SOURCE_FOLDERS if not any((base_dir / name).rglob("*"))]
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
                    "FDL_REPASSE_CONSUME_MODE=materialized mas FDL_REPASSE_MATERIALIZED_PATH e "
                    "FDL_REPASSE_MATERIALIZED_URL estão vazios — utilizado fluxo live (FDL_DATA_SOURCE)."
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
                f"{_STRICT_MATERIALIZED_USER_MSG} Repasse: não foi possível ler o ficheiro materializado. "
                f"Detalhe: {exc}"
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


if not _fdl_minimal_layout():
    st.markdown(
        """
    <style>
      html, body, [class*="css"] {
        font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      }
      .main .block-container { padding-top: 0.5rem; padding-bottom: 2rem; max-width: 1400px; }

      .fdl-topbar {
        display: flex;
        justify-content: space-between;
        align-items: stretch;
        gap: 1.25rem 2rem;
        flex-wrap: wrap;
        background: linear-gradient(118deg, #0f172a 0%, #1e3a5f 48%, #0f172a 100%);
        color: #f8fafc;
        padding: 1rem 1.45rem 1.05rem 1.45rem;
        margin: 0 0 1.25rem 0;
        border-radius: 14px;
        box-shadow: 0 10px 32px rgba(15, 23, 42, 0.22);
        border: 1px solid rgba(148, 163, 184, 0.18);
      }
      .fdl-topbar-left {
        display: flex;
        align-items: center;
        gap: 0.95rem;
        min-width: 0;
        flex: 1 1 auto;
      }
      .fdl-topbar-logo .fdl-logo-svg {
        display: block;
        width: 44px;
        height: 44px;
        border-radius: 12px;
        flex-shrink: 0;
        box-shadow: 0 4px 14px rgba(14, 165, 233, 0.35);
      }
      .fdl-topbar-titles {
        display: flex;
        flex-direction: column;
        gap: 0.2rem;
        min-width: 0;
      }
      .fdl-topbar-title {
        font-size: 1.48rem;
        font-weight: 800;
        letter-spacing: -0.035em;
        color: #ffffff;
        line-height: 1.12;
        margin: 0;
      }
      .fdl-topbar-tagline {
        font-size: 0.68rem;
        font-weight: 500;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: #94a3b8;
        margin: 0;
        line-height: 1.35;
      }
      .fdl-topbar-right {
        display: flex;
        align-items: center;
        justify-content: flex-end;
        flex: 0 1 auto;
        min-width: min(100%, 14rem);
      }
      .fdl-topbar-client {
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        gap: 0.55rem;
        text-align: right;
      }
      .fdl-topbar-client-block {
        display: flex;
        flex-direction: column;
        align-items: flex-end;
        gap: 0.12rem;
        max-width: 22rem;
      }
      .fdl-topbar-lbl {
        font-size: 0.62rem;
        font-weight: 600;
        letter-spacing: 0.11em;
        text-transform: uppercase;
        color: #64748b;
        line-height: 1.2;
      }
      .fdl-topbar-client-name {
        font-size: 0.86rem;
        font-weight: 500;
        color: #cbd5e1;
        letter-spacing: 0.01em;
        line-height: 1.35;
        word-break: break-word;
      }
      .fdl-topbar-block-org .fdl-topbar-lbl { color: #64748b; }
      .fdl-topbar-org-name {
        font-size: 1.12rem;
        font-weight: 700;
        color: #f8fafc;
        letter-spacing: -0.025em;
        line-height: 1.25;
        word-break: break-word;
      }

      .fdl-breadcrumb {
        display: flex; flex-wrap: wrap; align-items: center; gap: 0.35rem 0.5rem;
        font-size: 0.8rem; font-weight: 500; color: #64748b; margin: 0 0 0.65rem 0;
        letter-spacing: 0.01em;
      }
      .fdl-breadcrumb .fdl-bc-sep { color: #cbd5e1; user-select: none; font-weight: 400; }
      .fdl-breadcrumb .fdl-bc-item { color: #64748b; }
      .fdl-breadcrumb .fdl-bc-item.fdl-bc-current {
        color: #0f172a; font-weight: 700;
      }

      .page-hero {
        margin-bottom: 1.35rem; padding-bottom: 1rem;
        border-bottom: 1px solid #e2e8f0;
      }
      .page-hero h1 {
        font-size: 1.75rem; font-weight: 700; color: #0f172a; margin: 0 0 0.35rem 0;
        letter-spacing: -0.025em;
      }
      .page-hero .page-sub {
        font-size: 0.95rem; color: #64748b; line-height: 1.5; max-width: 52rem; margin: 0;
      }
      .page-hero .page-meta {
        font-size: 0.8rem; color: #94a3b8; margin-top: 0.65rem;
      }
      .page-hero .page-meta strong { color: #475569; }

      .section-title {
        font-size: 0.82rem; font-weight: 600; text-transform: uppercase;
        letter-spacing: 0.06em; color: #64748b; margin: 1.35rem 0 0.65rem 0;
      }

      .money-hero {
        background: linear-gradient(90deg, #f0f9ff 0%, #e0f2fe 45%, #f8fafc 100%);
        border: 1px solid #bae6fd; border-radius: 12px;
        padding: 0.9rem 1.1rem; margin-bottom: 1rem;
        font-size: 1.05rem; color: #0c4a6e; box-shadow: 0 2px 8px rgba(14, 165, 233, 0.08);
      }
      .money-hero b { font-weight: 700; color: #0369a1; }

      .kpi-card {
        border: 1px solid #e2e8f0; border-radius: 12px; padding: 0.85rem 0.95rem;
        background: #fff; box-shadow: 0 1px 3px rgba(15, 23, 42, 0.06);
        min-height: 6.85rem;
        display: flex;
        flex-direction: column;
        justify-content: flex-start;
        gap: 0.35rem;
        box-sizing: border-box;
        transition: box-shadow 0.15s ease;
      }
      .kpi-card:hover { box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08); }
      .kpi-icon { font-size: 1.15rem; margin-right: 0.35rem; opacity: 0.95; flex-shrink: 0; }
      .kpi-label {
        font-size: 0.72rem; font-weight: 600; color: #475569; text-transform: uppercase; letter-spacing: 0.04em;
        line-height: 1.28;
        min-height: 3em;
        display: flex;
        flex-wrap: wrap;
        align-items: flex-start;
        align-content: flex-start;
        gap: 0 0.25rem;
      }
      .kpi-value {
        font-size: 1.28rem; font-weight: 700; margin-top: auto; color: #0f172a;
        letter-spacing: -0.02em;
      }
      .kpi-total { border-left: 4px solid #0284c7; background: linear-gradient(90deg, #f0f9ff 0%, #fff 55%); }
      .kpi-ok { border-left: 4px solid #16a34a; background: linear-gradient(90deg, #f0fdf4 0%, #fff 55%); }
      .kpi-acao { border-left: 4px solid #0891b2; background: linear-gradient(90deg, #ecfeff 0%, #fff 55%); }
      .kpi-div { border-left: 4px solid #ea580c; background: linear-gradient(90deg, #fff7ed 0%, #fff 55%); }
      .kpi-pend { border-left: 4px solid #7c3aed; background: linear-gradient(90deg, #f5f3ff 0%, #fff 55%); }

      .fdl-frete-spotlight {
        border-radius: 14px;
        border: 1px solid #fde68a;
        background: linear-gradient(118deg, #fffbeb 0%, #fef3c7 35%, #ffffff 92%);
        padding: 1.05rem 1.2rem;
        margin: 0 0 1.1rem 0;
        box-shadow: 0 6px 22px rgba(180, 83, 9, 0.1);
      }
      .fdl-frete-spotlight .fdl-fs-title { font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.07em; color: #b45309; margin: 0 0 0.35rem 0; }
      .fdl-frete-spotlight .fdl-fs-an { font-size: 1.05rem; font-weight: 800; color: #0f172a; letter-spacing: -0.02em; margin: 0; word-break: break-all; }
      .fdl-frete-spotlight .fdl-fs-metrics { font-size: 0.88rem; color: #57534e; margin-top: 0.55rem; line-height: 1.55; }
      .fdl-frete-spotlight .fdl-fs-metrics strong { color: #0f172a; }
      .fdl-frete-hint { font-size: 0.8rem; color: #64748b; margin: -0.35rem 0 0.9rem 0; line-height: 1.45; }

      .queue-head { margin-top: 0.25rem; margin-bottom: 0.65rem; }
      .queue-title { font-size: 1.05rem; font-weight: 700; color: #0f172a; }
      .queue-sub { font-size: 0.86rem; color: #64748b; margin-top: 0.2rem; }

      /* Painel Conciliação de Frete — refinamento financeiro */
      .fdl-frete-section-title {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.08em;
        color: #94a3b8;
        margin: 1.85rem 0 0.8rem 0;
      }
      .fdl-frete-section-title.fdl-frete-st-first { margin-top: 0.5rem; }
      .fdl-frete-queue-head {
        margin-top: 2rem;
        margin-bottom: 0.85rem;
      }
      .fdl-frete-kpi-card {
        padding: 1.2rem 1.3rem 1.35rem 1.3rem !important;
        min-height: 7.5rem !important;
        border-radius: 14px !important;
        border: 1px solid #e2e8f0 !important;
        box-shadow: 0 2px 10px rgba(15, 23, 42, 0.06) !important;
      }
      .fdl-frete-kpi-card .kpi-label {
        font-size: 0.62rem !important;
        font-weight: 600 !important;
        color: #94a3b8 !important;
        letter-spacing: 0.07em !important;
        min-height: 2.4em !important;
      }
      .fdl-frete-kpi-card .kpi-value {
        font-size: 1.58rem !important;
        font-weight: 800 !important;
        color: #0f172a !important;
        letter-spacing: -0.025em !important;
      }
      .fdl-frete-meta-line {
        font-size: 0.8rem;
        color: #64748b;
        margin: 0.35rem 0 0 0;
        letter-spacing: 0.01em;
      }

      /* Sidebar — hierarquia Empresa → Módulos → Funcionalidades */
      div[data-testid="stSidebar"] {
        background: #f9fafb !important;
        border-right: 1px solid #e5e7eb !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
      }

      .fdl-sb-header {
        background: transparent;
        border: none;
        border-radius: 0;
        padding: 0.5rem 0 0.65rem 0;
        margin: 0 0 0.6rem 0;
        box-shadow: none;
        border-bottom: 1px solid #eceef2;
      }
      /* Integra a marca ao fundo da sidebar — remove “caixa” branca dos blocos Streamlit */
      div[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] {
        background: transparent !important;
      }
      div[data-testid="stSidebar"] [data-testid="stVerticalBlockBorderWrapper"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stVerticalBlockBorderWrapper"]:has(.fdl-sb-header) {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] img.fdl-sb-brand-logo-img {
        image-rendering: auto;
      }
      /* Faixa horizontal: largura = sidebar; altura vem só da proporção do PNG (sem max-height agressivo). */
      .fdl-sb-brand-logo-wrap {
        display: block;
        width: 100%;
        max-width: 100%;
        margin: 0 0 0.65rem 0;
        padding: 0;
        line-height: 0;
      }
      .fdl-sb-brand-logo-img {
        display: block;
        width: 100%;
        max-width: 100%;
        height: auto;
        object-fit: contain;
        object-position: left top;
      }
      .fdl-sb-brand-logo-wrap .fdl-sb-brand-logo-img ~ .fdl-sb-brand-logo-img {
        display: none !important;
      }
      .fdl-sb-logo-fallback {
        display: flex;
        align-items: center;
      }
      .fdl-sb-logo-fallback svg {
        display: block;
        width: 44px;
        height: 44px;
        border-radius: 8px;
      }
      .fdl-sb-user {
        display: flex;
        align-items: center;
        gap: 0.38rem;
        margin-top: 0;
        padding-top: 0;
        border-top: none;
      }
      .fdl-sb-avatar {
        width: 1.35rem;
        height: 1.35rem;
        border-radius: 999px;
        background: #eef2f6;
        color: #64748b;
        font-size: 0.48rem;
        font-weight: 700;
        letter-spacing: 0.02em;
        display: flex;
        align-items: center;
        justify-content: center;
        flex-shrink: 0;
        box-shadow: none;
        border: 1px solid #e2e8f0;
      }
      .fdl-sb-user-name {
        font-size: 0.76rem;
        font-weight: 500;
        color: #94a3b8;
        line-height: 1.25;
        word-break: break-word;
      }
      /* Seletor de empresa — dropdown alinhado ao restante da sidebar */
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] { margin-top: 0.85rem; margin-bottom: 0.15rem; }
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] label p {
        font-size: 0.68rem !important;
        font-weight: 800 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
        color: #94a3b8 !important;
      }
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div,
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] div[data-baseweb="select"] {
        transition: border-color 0.2s ease, box-shadow 0.2s ease !important;
      }
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div {
        border-radius: 8px !important;
        border-color: #e5e7eb !important;
        background: #ffffff !important;
      }
      div[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"]:focus-within > div {
        border-color: #93c5fd !important;
        box-shadow: 0 0 0 1px rgba(147, 197, 253, 0.45) !important;
      }
      .fdl-sb-system-modules-title {
        font-size: 0.68rem;
        font-weight: 800;
        text-transform: uppercase;
        letter-spacing: 0.11em;
        color: #9ca3af;
        margin: 1rem 0 0.45rem 0.15rem;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] {
        margin: 0 0 0.35rem 0 !important;
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] details {
        border: none !important;
        border-radius: 8px !important;
        background: transparent !important;
        box-shadow: none !important;
        transition: background 0.2s ease !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] details:hover {
        background: rgba(255, 255, 255, 0.45) !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary {
        padding: 0.45rem 0.5rem !important;
        font-weight: 600 !important;
        font-size: 0.8125rem !important;
        color: #374151 !important;
        letter-spacing: 0.01em;
        list-style: none;
        cursor: pointer !important;
        border-radius: 6px !important;
        border: 1px solid #eceef2 !important;
        background: rgba(255, 255, 255, 0.65) !important;
        transition: background 0.2s ease, border-color 0.2s ease !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
        background: #ffffff !important;
        border-color: #e5e7eb !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary::-webkit-details-marker { display: none; }
      div[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpanderDetails"] > div {
        padding: 0.35rem 0.45rem 0.45rem 0.55rem !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton { margin-top: 0.2rem !important; }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button {
        border-radius: 6px !important;
        font-weight: 600 !important;
        font-size: 0.8125rem !important;
        text-align: left !important;
        justify-content: flex-start !important;
        white-space: nowrap !important;
        line-height: 1.35 !important;
        padding: 0.4rem 0.65rem !important;
        min-height: 0 !important;
        cursor: pointer !important;
        transition: background 0.2s ease, border-color 0.2s ease !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button[kind="primary"] {
        background: #eff6ff !important;
        color: #1e3a8a !important;
        border: 1px solid #93c5fd !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button[kind="secondary"] {
        background: transparent !important;
        color: #374151 !important;
        border: 1px solid transparent !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button[kind="secondary"]:hover {
        background: #f3f4f6 !important;
        border-color: #e5e7eb !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] .stButton > button[kind="primary"]:hover {
        background: #dbeafe !important;
        border-color: #60a5fa !important;
      }
      .sb-nav-section-label {
        font-size: 0.68rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em;
        color: #94a3b8; margin: 0 0 0.5rem 0.15rem;
      }

      .sb-divider-soft { height: 1px; background: linear-gradient(90deg, transparent, #cbd5e1 12%, #cbd5e1 88%, transparent); margin: 0 0 1rem 0; border: 0; }

      /* Nível 2 — módulo (indentado, guia vertical) */
      div[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpander"] {
        margin: 0.5rem 0 0 0.65rem !important;
        padding: 0 !important;
        border: none !important;
        box-shadow: none !important;
        background: transparent !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpander"] details {
        border: none !important;
        border-left: 2px solid #cbd5e1 !important;
        border-radius: 0 !important;
        padding-left: 0.5rem !important;
        margin-left: 0.25rem !important;
        background: transparent !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpander"] summary {
        padding: 0.5rem 0.55rem !important;
        font-size: 0.82rem !important;
        font-weight: 700 !important;
        color: #334155 !important;
        border-radius: 6px !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpander"] summary:hover {
        background: rgba(226,232,240,0.6) !important;
      }

      .sb-nav-tree { margin: 0.35rem 0 0.15rem 0; padding: 0.15rem 0 0.25rem 0.15rem; }
      .sb-nav-item {
        display: flex; align-items: center; gap: 0.45rem;
        padding: 0.52rem 0.65rem 0.52rem 0.65rem; margin: 0.2rem 0 0 0.15rem;
        border-radius: 8px; font-size: 0.84rem; font-weight: 500; color: #475569;
        border: 1px solid transparent; position: relative; min-height: 2.25rem;
        padding-left: 2.25rem !important; cursor: default;
        transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease;
      }
      .sb-nav-item .sb-ico { position: absolute; left: 0.62rem; font-size: 1rem; line-height: 1; opacity: 0.92; }
      .sb-nav-item .sb-nav-label { flex: 1; line-height: 1.35; }
      .sb-nav-item:not(.sb-nav-item-active):not(.sb-nav-item-placeholder):hover {
        background: rgba(241,245,249,0.95) !important;
        border-color: #e2e8f0 !important;
      }
      .sb-nav-item-active {
        background: #dbeafe !important;
        color: #0c4a6e !important;
        font-weight: 600 !important;
        border: 1px solid #bfdbfe !important;
        box-shadow: 0 1px 3px rgba(14, 165, 233, 0.1);
      }
      .sb-active-accent {
        position: absolute; left: 0; top: 10%; bottom: 10%; width: 3px;
        background: #0284c7;
        border-radius: 0 3px 3px 0;
        box-shadow: 0 0 0 1px rgba(2,132,199,0.25);
      }
      .sb-nav-item-placeholder {
        opacity: 0.72; cursor: default !important;
        border-style: dashed !important; border-color: #e2e8f0 !important;
        background: rgba(248,250,252,0.8) !important;
      }
      .sb-nav-item-placeholder .sb-soon {
        display: inline-block; margin-left: 0.35rem; font-size: 0.68rem; font-weight: 700;
        text-transform: uppercase; letter-spacing: 0.06em; color: #94a3b8;
      }

      /* Botões da sidebar fora dos cards de módulo (rodapé, expander admin) */
      div[data-testid="stSidebar"] div[data-testid="element-container"] .stButton > button[kind="tertiary"] {
        background: transparent !important;
        color: #64748b !important;
        border: 1px solid #e2e8f0 !important;
        font-weight: 600 !important;
        min-height: unset !important;
        white-space: normal !important;
        padding: 0.65rem 1rem !important;
        box-shadow: none !important;
      }
      div[data-testid="stSidebar"] div[data-testid="element-container"] .stButton > button[kind="tertiary"]:hover {
        background: #f1f5f9 !important;
        border-color: #cbd5e1 !important;
        color: #0f172a !important;
      }

      .sb-sync-block { margin-top: 1.35rem; padding-top: 1.1rem; border-top: 1px solid #d1d9e6; }
      .sb-sync-label {
        font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #94a3b8; margin: 0 0 0.35rem 0;
      }
      .sb-sync-ts { font-size: 0.84rem; font-weight: 600; color: #475569; font-variant-numeric: tabular-nums; line-height: 1.35; }

      .validacao-badges {
        display: flex; flex-wrap: wrap; align-items: center; gap: 0.55rem 0.65rem;
        margin: 0.35rem 0 0.85rem 0;
      }
      .badge-acao {
        display: inline-flex; align-items: center; gap: 0.35rem;
        padding: 0.42rem 0.85rem; border-radius: 999px;
        font-size: 0.8rem; font-weight: 700; letter-spacing: 0.01em;
        border: 1px solid transparent;
        box-shadow: 0 1px 2px rgba(15, 23, 42, 0.06);
      }
      .badge-acao b { font-weight: 800; font-size: 0.92rem; }
      .badge-ok { background: #dcfce7; color: #14532d; border-color: #4ade80; }
      .badge-bling { background: #dbeafe; color: #1e3a8a; border-color: #60a5fa; }
      .badge-analisar { background: #ffedd5; color: #9a3412; border-color: #fb923c; }
      .badge-verificar { background: #f3e8ff; color: #581c87; border-color: #c084fc; }
      .badge-faturamento { background: #ede9fe; color: #4c1d95; border-color: #a78bfa; }
      .badge-revisar { background: #fee2e2; color: #991b1b; border-color: #f87171; }

      .filtros-panel {
        border: 1px solid #e2e8f0; border-radius: 12px; background: #ffffff;
        padding: 1rem 1.1rem 0.85rem 1.1rem; margin-bottom: 1.15rem;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.05);
      }
      .filtros-panel-title {
        font-size: 0.78rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.07em;
        color: #64748b; margin: 0 0 0.75rem 0;
      }

      /* Tabela conciliação: o st.dataframe atual usa Glide Data Grid (canvas) — o cabeçalho
         real vem de theme.dataframeHeaderBackgroundColor em .streamlit/config.toml.
         Regras abaixo sobem o cartão à volta do widget; células HTML stTable só em legacy. */
      section.main div[data-testid="stDataFrame"] {
        border-radius: 14px !important;
        overflow: hidden !important;
        border: 1px solid #d8e2ec !important;
        box-shadow:
          0 1px 2px rgba(15, 23, 42, 0.04),
          0 8px 28px rgba(15, 23, 42, 0.07) !important;
        background: #ffffff !important;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] {
        border-collapse: separate !important;
        border-spacing: 0 !important;
        font-size: 0.9rem !important;
        font-variant-numeric: tabular-nums;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table { width: 100%; }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th {
        background: linear-gradient(165deg, #1e3a5f 0%, #0f172a 48%, #172554 100%) !important;
        color: #f1f5f9 !important;
        -webkit-text-fill-color: #f1f5f9 !important;
        font-weight: 700 !important;
        font-size: 0.76rem !important;
        line-height: 1.25 !important;
        text-transform: uppercase;
        letter-spacing: 0.09em;
        padding: 0.95rem 0.85rem !important;
        border-bottom: none !important;
        border-top: none !important;
        border-left: 1px solid rgba(148, 163, 184, 0.12) !important;
        border-right: none !important;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.07);
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th:first-child {
        border-left: none !important;
        border-top-left-radius: 0;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th:last-child {
        border-right: none !important;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr {
        box-shadow: 0 3px 0 #38bdf8, 0 8px 16px rgba(14, 165, 233, 0.12);
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th * {
        color: #f8fafc !important;
        font-weight: 700 !important;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:nth-child(even) {
        background-color: #f8fafc !important;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:nth-child(odd) {
        background-color: #ffffff !important;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:hover td {
        background-color: #e0f2fe !important;
        transition: background-color 0.12s ease;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody td {
        padding: 0.65rem 0.8rem !important;
        border-bottom: 1px solid #e8eef4 !important;
        color: #1e293b;
        vertical-align: middle;
      }
      section.main div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:last-child td {
        border-bottom: none !important;
      }
    </style>
    """,
        unsafe_allow_html=True,
    )

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


_FRETE_UI_COL_SITUACAO = "Situação do Frete"
_FRETE_UI_COL_DIFERENCA = "Diferença"
_FRETE_UI_COL_N_VENDA = "N.º venda"


def _parse_br_money_display(s: object) -> float:
    """Interpreta célula pt-BR «R$ 1.234,56» ou «R$ -15,00» para float."""
    if s is None:
        return float("nan")
    try:
        if pd.isna(s):
            return float("nan")
    except TypeError:
        pass
    if isinstance(s, (int, float)) and not isinstance(s, bool):
        return float(s)
    raw = str(s).strip().replace("R$", "").replace(" ", "")
    if not raw:
        return float("nan")
    neg = raw.startswith("-")
    raw = raw.lstrip("-")
    raw = raw.replace(".", "").replace(",", ".")
    v = float(pd.to_numeric(raw, errors="coerce"))
    if pd.isna(v):
        return float("nan")
    return -v if neg else v


def _fmt_int_ptbr(n: int) -> str:
    """Quantidade com separador de milhar pt-BR."""
    return f"{int(n):,}".replace(",", ".")


def _styler_frete_conciliacao_principal(df: pd.DataFrame) -> pd.io.formats.style.Styler:
    """Cores por situação e por sinal da diferença (valores já formatados em texto)."""
    sty = df.style.hide(axis="index")
    sty = sty.set_table_styles(
        [
            {
                "selector": "th",
                "props": [
                    ("font-size", "0.72rem"),
                    ("font-weight", "600"),
                    ("color", "#475569"),
                    ("background-color", "#f1f5f9"),
                    ("border-bottom", "2px solid #e2e8f0"),
                    ("padding", "0.55rem 0.65rem"),
                ],
            },
            {
                "selector": "td",
                "props": [
                    ("font-size", "0.84rem"),
                    ("padding", "0.5rem 0.65rem"),
                    ("border-bottom", "1px solid #f1f5f9"),
                ],
            },
        ],
        overwrite=False,
    )

    def _sit_styles(col: pd.Series) -> list[str]:
        out: list[str] = []
        for v in col:
            t = str(v).strip() if pd.notna(v) else ""
            if t == "OK":
                out.append("background-color:#ecfdf5;color:#047857;font-weight:600;")
            elif t == FRETE_UI_ANALISADO_REPASSE_FRETE:
                out.append("background-color:#fef9c3;color:#a16207;font-weight:600;")
            elif t == FRETE_UI_ANALISADO_COBRADO_MAIOR:
                out.append("background-color:#fee2e2;color:#b91c1c;font-weight:600;")
            elif t == FRETE_UI_ANALISADO_COBRADO_MENOR:
                out.append("background-color:#ffedd5;color:#c2410c;font-weight:600;")
            else:
                out.append("")
        return out

    def _diff_styles(col: pd.Series) -> list[str]:
        out: list[str] = []
        for v in col:
            x = _parse_br_money_display(v)
            if pd.isna(x):
                out.append("color:#64748b;background-color:#fafafa;")
            elif abs(x) < 1e-9:
                out.append("color:#334155;background-color:#f8fafc;font-weight:500;")
            elif x > 0:
                out.append("background-color:#ecfdf5;color:#047857;font-weight:600;")
            else:
                out.append("background-color:#fef2f2;color:#b91c1c;font-weight:600;")
        return out

    if _FRETE_UI_COL_SITUACAO in df.columns:
        sty = sty.apply(_sit_styles, subset=[_FRETE_UI_COL_SITUACAO], axis=0)
    if _FRETE_UI_COL_DIFERENCA in df.columns:
        sty = sty.apply(_diff_styles, subset=[_FRETE_UI_COL_DIFERENCA], axis=0)
    return sty


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
    "Ok": "🟢 OK",
    "Baixar no Bling": "🔵 Baixar no Bling",
    "Analisar diferença": "🔴 Analisar diferença",
    "Verificar recebimento": "🟡 Verificar recebimento",
    "Verificar faturamento": "🟡 Verificar faturamento",
    "Revisar venda zerada": "🟡 Revisar venda zerada",
}


def _repasse_format_situacao_exibicao(val: object) -> str:
    """Badge textual para «Situação» (UI; sem alterar dados de negócio)."""
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
    if "atrasad" in low:
        return f"🔴 {s}"
    if "vencendo" in low and "hoje" in low:
        return f"🟡 {s}"
    if "em dia" in low or low == "em dia":
        return f"🟢 {s}"
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


def _repasse_grid_styler(df: pd.DataFrame):
    """Datas pt-BR, alinhamento e destaque dos valores monetários (Pandas Styler)."""
    if df.empty:
        return df
    money_cols = [c for c in ("Valor da nota", "Valor a receber", "Valor pago", "Diferença") if c in df.columns]

    def _fmt_emiss(x: object) -> str:
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return ""
            if pd.isna(x):
                return ""
        except Exception:
            pass
        try:
            return pd.Timestamp(x).strftime("%d/%m/%Y")
        except Exception:
            return str(x) if x is not None else ""

    def _fmt_pag(x: object) -> str:
        try:
            if x is None or (isinstance(x, float) and math.isnan(x)):
                return ""
            if pd.isna(x):
                return ""
        except Exception:
            pass
        try:
            return pd.Timestamp(x).strftime("%d/%m/%Y %H:%M")
        except Exception:
            return str(x) if x is not None else ""

    fmt_map: dict[str, Callable[[object], str]] = {}
    if "Data de emissão" in df.columns:
        fmt_map["Data de emissão"] = _fmt_emiss
    if "Data de pagamento" in df.columns:
        fmt_map["Data de pagamento"] = _fmt_pag

    styler = df.style
    if fmt_map:
        styler = styler.format(fmt_map)
    if money_cols:
        styler = styler.set_properties(
            subset=money_cols,
            **{
                "text-align": "right",
                "font-weight": "600",
                "font-variant-numeric": "tabular-nums",
            },
        )
    return styler


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
    for c in ("Produto", "Nome do produto", "Título", "Título do anúncio", "Nome"):
        if c in columns:
            return c
    return None


def _faturamento_compute_alert_bools(df: pd.DataFrame) -> pd.DataFrame:
    """Colunas auxiliares _ab_* para KPIs, filtros e texto de alertas."""
    out = df.copy()
    pl, vt = "Preço de lista", "Valor total"
    pln = pd.to_numeric(out[pl], errors="coerce") if pl in out.columns else pd.Series(float("nan"), index=out.index)
    vtn = pd.to_numeric(out[vt], errors="coerce") if vt in out.columns else pd.Series(float("nan"), index=out.index)
    tol = _faturamento_divergencia_tol()
    out["_ab_pl_zero"] = pln.notna() & (pln == 0)
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
        fc = out["faturamento_consolidado"].fillna(False).astype(bool)
    else:
        fc = pd.Series(False, index=out.index)
    out["_ab_sem_nf_np"] = atendido & sem_nf & ~fc
    return out


def _faturamento_alertas_text(s: pd.Series) -> str:
    parts: list[str] = []
    if bool(s.get("_ab_pl_zero")):
        parts.append("Preço lista zero")
    if bool(s.get("_ab_div")):
        parts.append("Divergência preço x valor total")
    if bool(s.get("_ab_sem_nf_np")):
        parts.append("Sem NF não permitido")
    return " · ".join(parts)


def _faturamento_filter_keys(org_id: str) -> list[str]:
    oid = str(org_id)
    return [
        f"fat_visao_{oid}",
        f"fat_d_ini_{oid}",
        f"fat_d_fim_{oid}",
        f"fat_ms_plat_{oid}",
        f"fat_ms_sit_{oid}",
        f"fat_busca_{oid}",
        f"fat_ms_alert_{oid}",
    ]


_FATURAMENTO_PAINEL_EM_CONSTRUCAO = True


def _painel_faturamento(df: pd.DataFrame, _load_info: dict[str, object], ts_proc: str, org_id: str) -> None:
    """
    Fase 1 — Faturamento: KPIs, filtros, tabela principal e export CSV (recorte filtrado).
    """
    if _FATURAMENTO_PAINEL_EM_CONSTRUCAO:
        st.markdown("## 🚧 Módulo em construção")
        st.info("Em breve você poderá acompanhar o faturamento completo aqui.")
        return
    _oid = str(org_id)
    if df.empty:
        st.info("Não há linhas de faturamento para exibir. Verifique a materialização ou os filtros de empresa.")
        return

    _req = (
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
        "Custo do Produto",
        "Imposto",
        "Despesas Fixas",
    )
    missing = [c for c in _req if c not in df.columns]
    if missing:
        st.warning(
            "Dataset de faturamento sem colunas esperadas pelo painel. "
            f"Faltam: {', '.join(missing[:12])}{'…' if len(missing) > 12 else ''}."
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

    pl_col, res_col = "Preço de lista", "Resultado"
    pl_sum = float(pd.to_numeric(work[pl_col], errors="coerce").fillna(0).sum())
    res_sum = float(pd.to_numeric(work[res_col], errors="coerce").fillna(0).sum())
    margem_total = (res_sum / pl_sum) if pl_sum not in (0.0, -0.0) else float("nan")
    n_cons = int(work["faturamento_consolidado"].fillna(False).astype(bool).sum()) if "faturamento_consolidado" in work.columns else 0
    any_alert = work["_ab_pl_zero"] | work["_ab_div"] | work["_ab_sem_nf_np"]
    n_alert = int(any_alert.sum())

    k1, k2, k3, k4, k5 = st.columns(5)
    with k1:
        st.metric("Receita por Produtos", _fmt_brl_ptbr_celula(pl_sum))
    with k2:
        st.metric("Resultado Total", _fmt_brl_ptbr_celula(res_sum))
    with k3:
        if pl_sum == 0 or (isinstance(margem_total, float) and math.isnan(margem_total)):
            st.metric("Margem Total %", "—")
        else:
            st.metric("Margem Total %", f"{margem_total * 100:.2f}%".replace(".", ","))
    with k4:
        st.metric("Itens Consolidados", _fmt_int_ptbr(n_cons))
    with k5:
        st.metric("Alertas Ativos", _fmt_int_ptbr(n_alert))

    st.caption(
        "Os indicadores acima referem-se a **todo** o dataset carregado; a tabela abaixo respeita os filtros."
    )

    has_data_col = "Data" in work.columns
    d_series = pd.to_datetime(work["Data"], errors="coerce") if has_data_col else pd.Series(dtype="datetime64[ns]")
    has_usable_dates = bool(has_data_col) and d_series.notna().any()
    if has_usable_dates:
        d_min = d_series.min().date()
        d_max = d_series.max().date()
    else:
        d_min = d_max = datetime.now(_BR_TZ).date()

    plats = sorted({str(x).strip() for x in work["Nome da plataforma"].dropna().unique() if str(x).strip()})
    sits = sorted({str(x).strip() for x in work["Situação"].dropna().unique() if str(x).strip()})

    _opt_alertas = (
        "Preço lista zero",
        "Divergência preço x valor total",
        "Sem NF não permitido",
    )

    with st.expander("Filtros", expanded=True):
        visao = st.selectbox(
            "Visão",
            ("Todos", "Consolidado", "Com NF", "Sem NF permitido"),
            key=f"fat_visao_{_oid}",
        )
        r0 = st.columns((1.15, 1.15))
        if has_usable_dates:
            with r0[0]:
                d_ini = st.date_input(
                    "Período — início (Data)",
                    value=d_min,
                    min_value=d_min,
                    max_value=d_max,
                    format="DD/MM/YYYY",
                    key=f"fat_d_ini_{_oid}",
                )
            with r0[1]:
                d_fim = st.date_input(
                    "Período — fim (Data)",
                    value=d_max,
                    min_value=d_min,
                    max_value=d_max,
                    format="DD/MM/YYYY",
                    key=f"fat_d_fim_{_oid}",
                )
        elif has_data_col:
            st.caption("A coluna **Data** existe mas não tem valores parseáveis — o filtro por período está desativado.")
        r1 = st.columns((1.15, 1.15))
        with r1[0]:
            sel_plat = _multiselect_stable(f"fat_ms_plat_{_oid}", "Plataforma", plats)
        with r1[1]:
            sel_sit = _multiselect_stable(f"fat_ms_sit_{_oid}", "Situação do pedido", sits)
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
        if st.button("Limpar filtros", key=f"fat_clear_{_oid}"):
            for _k in _faturamento_filter_keys(_oid):
                st.session_state.pop(_k, None)
            st.rerun()

    filt = work.copy()
    if visao == "Consolidado":
        filt = filt[filt["faturamento_consolidado"].fillna(False).astype(bool)]
    elif visao == "Com NF":
        filt = filt[filt["faturamento_com_nf"].fillna(False).astype(bool)]
    elif visao == "Sem NF permitido":
        filt = filt[filt["faturamento_sem_nf"].fillna(False).astype(bool)]

    if has_usable_dates:
        if d_fim < d_ini:
            st.warning("A data final não pode ser anterior à inicial.")
            d_fim = d_ini
        d_cmp = pd.to_datetime(filt["Data"], errors="coerce")
        dd = d_cmp.dt.normalize()
        _ini_ts = pd.Timestamp(d_ini)
        _fim_ts = pd.Timestamp(d_fim) + pd.Timedelta(days=1)
        m_d = d_cmp.notna() & (dd >= _ini_ts) & (dd < _fim_ts)
        filt = filt.loc[m_d].copy()

    if sel_plat:
        filt = filt[filt["Nome da plataforma"].isin(sel_plat)]
    if sel_sit:
        filt = filt[filt["Situação"].isin(sel_sit)]
    if busca:
        m_bus = pd.Series(False, index=filt.index)
        for col in ("Número do pedido", "Número do pedido multiloja", "Código", "Número da nota"):
            if col in filt.columns:
                m_bus = m_bus | filt[col].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        filt = filt.loc[m_bus].copy()

    if sel_alerts:
        m_a = pd.Series(False, index=filt.index)
        if "Preço lista zero" in sel_alerts:
            m_a = m_a | filt["_ab_pl_zero"]
        if "Divergência preço x valor total" in sel_alerts:
            m_a = m_a | filt["_ab_div"]
        if "Sem NF não permitido" in sel_alerts:
            m_a = m_a | filt["_ab_sem_nf_np"]
        filt = filt.loc[m_a].copy()

    filt = filt.sort_values(res_col, ascending=True, na_position="last")

    prod_col = _faturamento_resolve_produto_column(list(filt.columns))
    rpct = pd.to_numeric(filt["Resultado_Pct"], errors="coerce") if "Resultado_Pct" in filt.columns else pd.Series(float("nan"), index=filt.index)

    disp = pd.DataFrame(
        {
            "Plataforma": filt["Nome da plataforma"],
            "Situação do pedido": filt["Situação"],
            "N.º do pedido": filt["Número do pedido"],
            "N.º pedido multiloja": filt["Número do pedido multiloja"],
            "SKU": filt["Código"],
            "Produto": filt[prod_col].astype(str) if prod_col else pd.Series("", index=filt.index),
            "NF emitida?": filt["Existe Nota Fiscal gerada"],
            "N.º da nota": filt["Número da nota"],
            "Receita por Produtos": pd.to_numeric(filt[pl_col], errors="coerce"),
            "Valor total": pd.to_numeric(filt["Valor total"], errors="coerce"),
            "Custo do produto": pd.to_numeric(filt["Custo do Produto"], errors="coerce"),
            "Frete": pd.to_numeric(filt["Custo de Frete"], errors="coerce"),
            "Comissão Plataforma": pd.to_numeric(filt["Taxa de Comissão"], errors="coerce"),
            "Imposto": pd.to_numeric(filt["Imposto"], errors="coerce"),
            "Despesas fixas": pd.to_numeric(filt["Despesas Fixas"], errors="coerce"),
            "Resultado": pd.to_numeric(filt[res_col], errors="coerce"),
            "Resultado %": rpct * 100.0,
        }
    )
    disp["Alertas"] = filt.apply(_faturamento_alertas_text, axis=1)

    _cfg: dict[str, NumberColumn | TextColumn] = {}
    money_cols = (
        "Receita por Produtos",
        "Valor total",
        "Custo do produto",
        "Frete",
        "Comissão Plataforma",
        "Imposto",
        "Despesas fixas",
        "Resultado",
    )
    for c in money_cols:
        if c in disp.columns:
            _cfg[c] = NumberColumn(c, format="R$ %,.2f")
    if "Resultado %" in disp.columns:
        _cfg["Resultado %"] = NumberColumn("Resultado %", format="%.2f%%")
    for c in ("Plataforma", "Situação do pedido", "N.º do pedido", "N.º pedido multiloja", "SKU", "Produto", "NF emitida?", "N.º da nota", "Alertas"):
        if c in disp.columns:
            _cfg[c] = TextColumn(c, width="medium" if c != "Alertas" else "large")

    st.subheader("Tabela principal")
    st.caption(f"{len(disp)} registos com os filtros atuais · ordenação: Resultado (ascendente).")
    st.dataframe(
        disp,
        use_container_width=True,
        hide_index=True,
        height=520,
        column_config=_cfg,
    )
    st.download_button(
        "Exportar CSV (filtrado)",
        disp.to_csv(index=False).encode("utf-8-sig"),
        file_name="faturamento_filtrado.csv",
        mime="text/csv",
        key=f"fat_dl_csv_{_oid}",
    )


def _html_fdl_topbar(client_esc: str, org_esc: str) -> str:
    """
    Cabeçalho superior SaaS: logo + marca + cliente/empresa.
    ``client_esc`` e ``org_esc`` devem vir de ``html.escape``.
    """
    _logo = """
<svg class="fdl-logo-svg" width="44" height="44" viewBox="0 0 40 40" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <defs><linearGradient id="fdlLogoGrad" x1="0" y1="0" x2="1" y2="1">
    <stop offset="0%" stop-color="#38bdf8"/><stop offset="100%" stop-color="#0284c7"/>
  </linearGradient></defs>
  <rect width="40" height="40" rx="11" fill="url(#fdlLogoGrad)"/>
  <path d="M9 28h5V16H9v12zm8 0h5V10h-5v18zm8-6h5v6h-5v-6zm8-8h5v14h-5V14z" fill="#fff" fill-opacity="0.95"/>
</svg>"""
    return f"""
<div class="fdl-topbar">
  <div class="fdl-topbar-left">
    <div class="fdl-topbar-logo">{_logo}</div>
    <div class="fdl-topbar-titles">
      <p class="fdl-topbar-title">FDL Analytics</p>
      <p class="fdl-topbar-tagline">Financial Intelligence</p>
    </div>
  </div>
  <div class="fdl-topbar-right">
    <div class="fdl-topbar-client">
      <div class="fdl-topbar-client-block">
        <span class="fdl-topbar-lbl">Cliente</span>
        <span class="fdl-topbar-client-name">{client_esc}</span>
      </div>
      <div class="fdl-topbar-client-block fdl-topbar-block-org">
        <span class="fdl-topbar-lbl">Empresa</span>
        <span class="fdl-topbar-org-name">{org_esc}</span>
      </div>
    </div>
  </div>
</div>
""".strip()


def _render_kpi_card(
    label: str,
    value: str,
    icon: str,
    css_class: str,
    *,
    frete_variant: bool = False,
) -> None:
    if _fdl_minimal_layout():
        st.metric(label, value)
        return
    extra = " fdl-frete-kpi-card" if frete_variant else ""
    st.markdown(
        f"""
        <div class="kpi-card {css_class}{extra}">
          <div class="kpi-label"><span class="kpi-icon" aria-hidden="true">{icon}</span>{label}</div>
          <div class="kpi-value">{html.escape(value)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
        
            if "_data_venda_dt" in work.columns:
                dts = frete_series_normalize_sale_dt(work["_data_venda_dt"])
                if dts.notna().any():
                    d_min_data = dts.min().date()
                    d_max_data = dts.max().date()
                else:
                    d_min_data = d_max_data = today
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
        
            estados: list[str] = []
            if "Estado" in work.columns:
                estados = sorted(
                    {str(x).strip() for x in work["Estado"].dropna().unique().tolist() if str(x).strip()}
                )
            situacao_opts: list[str] = list(FRETE_SITUACAO_FRETE_VALORES_FILTRO)
        
            with st.container(border=True):
                st.subheader("Filtros operacionais")
                st.write("")
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
                st.write("")
                st.write("")
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
                st.caption(
                    "Filtra por **data da venda** (comparação por dia). Por omissão: últimos 30 dias até hoje."
                )

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
        
            if "_data_venda_dt" in tbl.columns:
                dd = frete_series_normalize_sale_dt(tbl["_data_venda_dt"])
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
            st.divider()
            st.caption(
                f"Estado (filtro): **{_pl}** · Dados carregados: **{_ts_esc}** · "
                f"Venda: **{data_ini.strftime('%d/%m/%Y')}** a **{data_fim.strftime('%d/%m/%Y')}** · "
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

        st.divider()
        st.write("")
        ek1, ek2 = st.columns(2)
        with ek1:
            with st.container(border=True):
                st.metric(
                    "💸 Cobrado a maior (valor a recuperar)",
                    _fmt_brl_ptbr_celula(kpi_ex["cobrado_maior"]),
                )
        with ek2:
            with st.container(border=True):
                st.metric(
                    "🚚 Repasse de frete (valor total)",
                    _fmt_brl_ptbr_celula(kpi_ex["repasse"]),
                )

        if _is_admin and FRETE_UI_STATUS_CONC in tbl_show.columns:
            st.caption(
                "Modo técnico: existe **Status conciliação** nos dados; a priorização segue **Situação do Frete**."
            )

        _sem_anuncio = FRETE_UI_ANUNCIO not in tbl_show.columns
        st.divider()
        st.subheader("💸 Problemas de frete (cobrado a maior)")
        st.caption("Anúncios onde o frete cobrado excede o esperado — prioridade para recuperação.")
        if _sem_anuncio:
            st.info("Inclua o **# do anúncio** no export de vendas para agregar por anúncio.")
        elif tbl_cob_maior.empty:
            st.info("Nenhum anúncio com **Cobrado a maior** nos filtros atuais.")
        else:
            _h1 = min(420, 120 + 36 * max(len(tbl_cob_maior), 1))
            st.dataframe(
                _format_frete_anuncio_tabela_display(tbl_cob_maior),
                use_container_width=True,
                hide_index=True,
                height=_h1,
            )

        st.divider()
        st.subheader("🚚 Controle de repasse de frete")
        st.caption("Anúncios com repasse de frete a validar (inclui marcação «Recebido?» no detalhe).")
        if _sem_anuncio:
            pass
        elif tbl_repasse.empty:
            st.info("Nenhum anúncio com **Repasse de frete** nos filtros atuais.")
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
        st.subheader("📋 Detalhamento das vendas")
        st.caption("Linhas filtradas — exporte o recorte ou ajuste «Recebido?» quando disponível.")
        st.write("")

        t_export_view = dataframe_frete_conciliacao_principal(
            tbl_show, recebido=recebido_series, layout="executivo"
        )
        csv_bytes = t_export_view.to_csv(index=False).encode("utf-8-sig")
        t_excel = dataframe_frete_conciliacao_principal(
            tbl_show, recebido=recebido_series, layout="executivo"
        )
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
        t_main = dataframe_frete_conciliacao_principal(
            t_grid, recebido=recebido_series, layout="executivo"
        )
        _h_df = 550 if len(t_main) > 8 else 360
    
        if t_main.empty:
            st.info(
                "**Nenhuma venda** com os filtros atuais. Alargue o período de datas ou limpe a busca / multiselects."
            )
        else:
            if _fdl_safe_mode():
                st.warning("**Modo seguro (FDL_SAFE_MODE)** — sem editor «Recebido?».")
            st.dataframe(t_main, use_container_width=True, height=_h_df)
            if not _fdl_safe_mode() and not _fdl_minimal_layout():
                st.caption(
                    "Ajuste **Recebido?** na grelha abaixo quando aplicável (repasse de frete)."
                )
                _recv = t_main[[_FRETE_UI_COL_N_VENDA, FRETE_UI_RECEBIDO]].copy()
                _cfg_recv: dict[str, object] = {
                    _FRETE_UI_COL_N_VENDA: TextColumn(_FRETE_UI_COL_N_VENDA, width="medium"),
                    FRETE_UI_RECEBIDO: SelectboxColumn(
                        FRETE_UI_RECEBIDO,
                        options=[FRETE_VAL_RECEBIDO_SIM, FRETE_VAL_RECEBIDO_NAO],
                        required=True,
                    ),
                }
                edited_recv = st.data_editor(
                    _recv,
                    column_config=_cfg_recv,
                    disabled=[_FRETE_UI_COL_N_VENDA],
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    key=f"op_frete_editor_{_sig}",
                )
                for _, row in edited_recv.iterrows():
                    vid = str(row[_FRETE_UI_COL_N_VENDA]).strip() if pd.notna(row.get(_FRETE_UI_COL_N_VENDA)) else ""
                    if vid:
                        rec_map[vid] = row[FRETE_UI_RECEBIDO] == FRETE_VAL_RECEBIDO_SIM
                st.session_state[_rec_key] = rec_map
            elif not _fdl_safe_mode() and _fdl_minimal_layout():
                st.caption(
                    "Editor «Recebido?» desativado com FDL_MINIMAL_LAYOUT. Defina FDL_MINIMAL_LAYOUT=0 para ativar."
                )
        st.caption(f"{len(t_main)} linhas no filtro atual.")
    
        if _is_admin and load_info.get("frete_consume") in ("live", "live_fallback"):
            st.caption(
                "Modo técnico: **live** — dados calculados diretamente das fontes; materializado indisponível ou falhou."
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
        st.warning(
            "Sem fonte de vendas ML para Frete. Defina **FDL_FRETE_VENDAS_URL** nos Secrets (Cloud) "
            "ou coloque ficheiros .xlsx/.csv em **Vendas - Mercado Livre** sob **FDL_BASE_DIR**."
        )
        st.caption(str(BASE_DIR))
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
        st.success("**FDL_FRETE_VENDAS_URL** detetado — a base foi descarregada a partir do SharePoint.")
    elif load_info.get("frete_fonte_local_path"):
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
    if _frete_debug_ui_enabled():
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
        st.info("Não há linhas de frete para exibir. Verifique o export de vendas ML ou a materialização.")
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
    """Tabela final: YYYY-MM-DD HH:MM:SS — ISO, sem dayfirst."""
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    s = s.mask(s.str.lower().isin({"none", "nan", "nat", "<na>", "null"}), "")
    return pd.to_datetime(s, errors="coerce")


def _repasse_ui_validacao_kpi_saas(contagens: dict[str, int]) -> None:
    """KPIs da base filtrada — cartões com cores semânticas (UI)."""
    ok = int(contagens.get("Ok", 0))
    bling = int(contagens.get("Baixar no Bling", 0))
    div = int(contagens.get("Analisar diferença", 0))
    rev = int(contagens.get("Revisão", 0))
    st.write("")
    c1, c2, c3, c4 = st.columns(4)
    specs: list[tuple[Any, str, int, str, str]] = [
        (c1, "OK", ok, "#15803d", "Sem pendência operacional neste recorte"),
        (c2, "Baixar no Bling", bling, "#1d4ed8", "Ação: baixa no Bling"),
        (c3, "Divergência", div, "#b91c1c", "Analisar diferença de valores"),
        (c4, "Revisão", rev, "#ca8a04", "Verificar recebimento, faturamento ou venda zerada"),
    ]
    for col, title, val, color, hint in specs:
        with col:
            with st.container(border=True):
                st.markdown(
                    f'<p style="margin:0 0 0.25rem 0;font-size:0.7rem;font-weight:600;color:{color};text-transform:uppercase;letter-spacing:0.04em;">{html.escape(title)}</p>',
                    unsafe_allow_html=True,
                )
                st.metric(
                    "Registos",
                    _fmt_int_ptbr(val),
                    help=hint,
                )


def _painel_conciliacao_fragment(base: pd.DataFrame, ts_proc: str) -> None:
    """
    Filtros + validação de ações + fila/tabela de repasse.

    Não usar @st.fragment aqui: ao mudar para «Frete», o fragment deixava de ser invocado e o Streamlit
    podia mostrar ecrã em branco (desincronização da árvore de widgets entre vistas).
    """
    if base.empty or "Data de pagamento" not in base.columns:
        st.warning("Base de repasse indisponível para esta visualização.")
        return

    with st.container(border=True):
        st.markdown("##### Filtros operacionais")
        st.caption("Refine por plataforma, ação, situação e intervalo de datas de pagamento.")
        st.write("")
        r1 = st.columns((1.15, 1.15, 1.15, 1.55))
        dp_series_full = pd.to_datetime(base["Data de pagamento"], errors="coerce")
        has_dp_base = bool(dp_series_full.notna().any())
        if has_dp_base:
            _d_min: date = dp_series_full.min().date()
            _d_max: date = dp_series_full.max().date()
        else:
            _d_min = _d_max = datetime.now(_BR_TZ).date()
        plats = (
            sorted([x for x in base["Plataforma"].dropna().unique().tolist() if str(x).strip()])
            if "Plataforma" in base.columns
            else []
        )
        acoes = sorted(
            [
                x
                for x in base["Ação sugerida operacional"].dropna().unique().tolist()
                if str(x).strip()
            ]
        )
        sit = sorted(
            [x for x in base["Situação"].dropna().unique().tolist() if str(x).strip()]
        )
        with r1[0]:
            sel_plat = _multiselect_stable("op_ms_plat", "Plataforma", plats, compact_label=True)
        with r1[1]:
            sel_acao = _multiselect_stable("op_ms_acao", "Ação sugerida", acoes, compact_label=True)
        with r1[2]:
            sel_sit = _multiselect_stable("op_ms_sit", "Situação", sit, compact_label=True)
        with r1[3]:
            st.caption("Busca")
            busca = st.text_input(
                " ",
                placeholder="Venda, pedido ou nota…",
                label_visibility="collapsed",
                key="op_repasse_busca_txt",
            ).strip().lower()
        st.write("")
        st.write("")
        r2 = st.columns((1.15, 1.15, 2.3))
        with r2[0]:
            st.caption("Pagamento — início")
            data_pag_ini = st.date_input(
                " ",
                value=_d_min,
                min_value=_d_min,
                max_value=_d_max,
                format="DD/MM/YYYY",
                label_visibility="collapsed",
            )
        with r2[1]:
            st.caption("Pagamento — fim")
            data_pag_fim = st.date_input(
                " ",
                value=_d_max,
                min_value=_d_min,
                max_value=_d_max,
                format="DD/MM/YYYY",
                label_visibility="collapsed",
            )
        with r2[2]:
            st.caption("Período")
            st.caption("Comparação por **dia civil** (meia-noite a meia-noite).")
        if not has_dp_base:
            st.info(
                "Nenhuma **data de pagamento** preenchida nesta base: o intervalo abaixo não filtra linhas "
                "(todas as vendas são listadas). Com datas na materialização, o filtro por período passa a valer."
            )

    st.divider()

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
        f"Plataforma (filtro): **{plataforma_label}** · Dados carregados: **{ts_proc}** · {_pag_caption}"
    )

    st.divider()

    # Tipos numéricos para a base já filtrada (tabela e totais nas colunas)
    tabela["Valor da nota"] = pd.to_numeric(tabela["Valor da nota"], errors="coerce").fillna(0.0)
    tabela["Total BRL"] = pd.to_numeric(tabela.get("Total BRL"), errors="coerce")
    tabela["Valor a receber"] = pd.to_numeric(tabela.get("Valor a receber"), errors="coerce")
    tabela["Valor pago"] = pd.to_numeric(tabela.get("Valor pago"), errors="coerce")
    tabela["Diferença"] = pd.to_numeric(tabela.get("Diferença"), errors="coerce")

    st.markdown("### Validação de ações")
    st.caption("Contagens na **base já filtrada** — priorize divergências e revisões.")
    acoes_validacao = [
        "Ok",
        "Baixar no Bling",
        "Analisar diferença",
    ]
    contagens_acao = {a: int(tabela["Ação sugerida operacional"].eq(a).sum()) for a in acoes_validacao}
    _revisao_acoes = ("Revisar venda zerada", "Verificar recebimento", "Verificar faturamento")
    contagens_acao["Revisão"] = int(tabela["Ação sugerida operacional"].isin(_revisao_acoes).sum())
    _repasse_ui_validacao_kpi_saas(contagens_acao)

    st.write("")
    st.caption(f"Dados carregados: **{ts_proc}**")
    st.divider()
    
    # Tabela operacional — Data de emissão: mesma coluna da tabela final, parse ISO (sem dayfirst).
    col_data_emissao = _resolve_col_data_emissao(list(tabela.columns))
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
        st.warning("Sem colunas para exibir na base filtrada.")
        tabela_exibir = pd.DataFrame()
    else:
        tabela_exibir = tabela[exibir_cols].copy()
        tabela_exibir["Valor da nota"] = pd.to_numeric(tabela_exibir["Valor da nota"], errors="coerce")
        tabela_exibir["Valor a receber"] = pd.to_numeric(tabela_exibir["Valor a receber"], errors="coerce")
        tabela_exibir["Valor pago"] = pd.to_numeric(tabela_exibir.get("Valor pago"), errors="coerce")
        tabela_exibir["Diferença"] = pd.to_numeric(tabela_exibir["Diferença"], errors="coerce")
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

    st.markdown("### Fila operacional")
    st.caption("Analise o recorte na grelha; use a exportação para partilhar fora do sistema.")
    st.write("")
    st.write("")

    csv_bytes = tabela_exibir.to_csv(index=False).encode("utf-8-sig")
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
    pdf_bytes = _build_pdf_bytes(tabela_exibir)

    with st.container(border=True):
        st.caption("Exportar recorte filtrado")
        btn1, btn2, btn3 = st.columns([1, 1, 1])
        with btn1:
            st.download_button(
                "Exportar CSV",
                data=csv_bytes,
                file_name="conciliacao_operacional_filtrada.csv",
                mime="text/csv",
                use_container_width=True,
            )
        with btn2:
            st.download_button(
                "Exportar Excel",
                data=excel_buf.getvalue(),
                file_name="conciliacao_operacional_filtrada.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )
        with btn3:
            st.download_button(
                "Exportar PDF",
                data=pdf_bytes,
                file_name="conciliacao_operacional_filtrada.pdf",
                mime="application/pdf",
                use_container_width=True,
            )

    st.write("")
    st.write("")
    st.write("")

    tabela_grid = _dataframe_conciliacao_somente_grid(tabela_exibir)
    _cfg_grid = None
    _disp_grid: object = tabela_grid
    if not _fdl_minimal_layout() and not tabela_grid.empty:
        try:
            _disp_grid = _repasse_grid_styler(tabela_grid)
        except Exception:
            _disp_grid = tabela_grid
            _cfg_grid = _column_config_conciliacao(tabela_grid, moeda_como_texto=True)
    elif not _fdl_minimal_layout() and tabela_grid.empty:
        _cfg_grid = None
    elif _fdl_minimal_layout() and not tabela_grid.empty:
        _cfg_grid = _column_config_conciliacao(tabela_grid, moeda_como_texto=True)

    if tabela_exibir.empty:
        st.info(
            "**Nenhum registo** com os filtros atuais. Alargue o período de datas ou limpe a busca / multiselects."
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
_fdl_global_trace(f"rerun: vista={_fv}")
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
                    "Frete: tentativa de carregar **materializado** falhou — em uso **fluxo live** (fallback)."
                )
                st.caption(f"Path/URL tentado: `{frete_info.get('frete_materialized_target', '')}`")
                st.caption(f"Erro: {frete_info.get('frete_materialized_error', '')}")
            elif frete_info.get("frete_consume") == "materialized":
                _t_disp = str(frete_info.get("frete_materialized_target", ""))[:500]
                st.caption(f"Frete: dados **materializados** (`{_t_disp}`).")
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
elif _fv == "faturamento":
    _fdl_global_trace("faturamento: a carregar _load_faturamento_dataframe_cached")
    with st.spinner("A carregar dados de Faturamento…"):
        faturamento_df, faturamento_info, ts_proc = _load_faturamento_dataframe_cached(
            _faturamento_load_cache_signature(_active_org.org_id)
        )
    fc = str(faturamento_info.get("faturamento_consume", "")).strip()
    if fc == "materialized" and _admin_mode:
        _t_disp = str(faturamento_info.get("faturamento_materialized_target", ""))[:500]
        st.caption(f"Faturamento: dados **materializados** (`{_t_disp}`).")
    elif fc == "missing_config":
        st.warning(
            str(faturamento_info.get("faturamento_note", "Dataset de faturamento não configurado ou não encontrado."))
        )
    elif fc == "error":
        st.warning(
            "Não foi possível carregar o dataset de **Faturamento** materializado. "
            f"{faturamento_info.get('faturamento_materialized_error', '')}"
        )
        if _admin_mode:
            st.caption(f"Alvo: `{faturamento_info.get('faturamento_materialized_target', '')}`")
    elif fc == "unsupported":
        st.warning(str(faturamento_info.get("faturamento_note", "Modo de consumo não suportado.")))

    if not faturamento_df.empty and "empresa" not in faturamento_df.columns:
        faturamento_df = faturamento_df.copy()
        faturamento_df["empresa"] = _dataset_empresa_label()

    if not faturamento_df.empty:
        faturamento_df = _filtrar_df_col_empresa_por_contexto(faturamento_df)

    faturamento_info = {**faturamento_info, "linhas": int(len(faturamento_df))}
    tabela_geral = pd.DataFrame()
    info = faturamento_info
    _fdl_global_trace(f"faturamento: após filtro empresa ({len(faturamento_df)} linhas)")
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
                        "Repasse: tentativa de carregar **materializado** falhou — em uso **fluxo live** (fallback)."
                    )
                    st.caption(f"Path/URL tentado: `{info.get('repasse_materialized_target', '')}`")
                    st.caption(f"Erro: {info.get('repasse_materialized_error', '')}")
                elif info.get("repasse_materialized_note"):
                    st.info(str(info["repasse_materialized_note"]))
                elif info.get("repasse_consume") == "materialized":
                    st.caption(
                        f"Repasse: dados **materializados** (`{info.get('repasse_materialized_target', '')}`)."
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
            _data_source_mode() in {"onedrive", "filesystem"}
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
if _bootstrap_debug_enabled():
    with st.expander("Diagnóstico bootstrap (FDL_DEBUG_BOOTSTRAP=1)", expanded=True):
        st.write("**Última etapa:**", st.session_state.get("_fdl_bootstrap_stage", "—"))
        st.write("**Vista ativa:**", _fv)
        st.write("**Modo seguro (FDL_SAFE_MODE):**", _fdl_safe_mode())
        st.write("**Layout mínimo (FDL_MINIMAL_LAYOUT, omisso=on):**", _fdl_minimal_layout())
        _n_linhas_dbg = (
            len(tabela_geral)
            if _fv == "repasse"
            else (len(faturamento_df) if _fv == "faturamento" else "— (vista frete)")
        )
        st.write("**Linhas tabela_geral (repasse) / faturamento_df:**", _n_linhas_dbg)
        _lg = st.session_state.get("_fdl_bootstrap_log")
        if isinstance(_lg, list) and _lg:
            st.write("**Log de etapas (esta execução):**")
            for _i, _line in enumerate(_lg, 1):
                st.caption(f"{_i}. {_line}")

with st.sidebar:
    _lc, _mc, _rc = st.columns([1, 2.8, 1])
    with _mc:
        _logo_file = _REPO_APP_ROOT / "assets" / "fdl_analytics_logo.png"
        if _logo_file.is_file():
            st.image(str(_logo_file), width=180)
        else:
            st.caption("Coloque a logo em `assets/fdl_analytics_logo.png`.")
    st.caption(_app_ctx.display_name)
    st.divider()

    _empresas_usuario = list(st.session_state["empresas_permitidas"])
    _nomes_nav = nomes_permitidos_com_registro(_empresas_usuario)

    if _nomes_nav:
        _org_idx = 0
        for i, n in enumerate(_nomes_nav):
            _o = organizacao_por_nome_cadastrado(n)
            if _o and _o.org_id == _app_ctx.active_org_id:
                _org_idx = i
                break
        _sel_nome = st.selectbox(
            "Empresa",
            options=_nomes_nav,
            index=_org_idx,
            key="operacional_empresa_ativa_select",
            label_visibility="visible",
        )
        _chosen_org = organizacao_por_nome_cadastrado(_sel_nome)
        if _chosen_org and _chosen_org.org_id != _app_ctx.active_org_id:
            st.session_state[SESSION_ACTIVE_ORG_KEY] = _chosen_org.org_id
            st.rerun()

    _sb_view = st.session_state.get("op_financeiro_view", "repasse")
    st.caption("Módulos")

    _lbl_repasse = "Conciliação de Repasse"
    _lbl_frete = "Conciliação de Frete"
    _lbl_faturamento = "Faturamento"

    with st.expander("💰 Financeiro", expanded=True):
        st.button(
            _lbl_repasse,
            key="fdl_mod_repasse",
            use_container_width=True,
            type="primary" if _sb_view == "repasse" else "secondary",
            on_click=_sb_nav_set_repasse,
        )
        st.button(
            _lbl_frete,
            key="fdl_mod_frete",
            use_container_width=True,
            type="primary" if _sb_view == "frete" else "secondary",
            on_click=_sb_nav_set_frete,
        )
        st.button(
            _lbl_faturamento,
            key="fdl_mod_faturamento",
            use_container_width=True,
            type="primary" if _sb_view == "faturamento" else "secondary",
            on_click=_sb_nav_set_faturamento,
        )

    with st.expander("📦 Estoque", expanded=False):
        st.caption("Em breve")

    with st.expander("🛒 Comercial", expanded=False):
        st.caption("Em breve")

    st.write("")
    st.caption("Última atualização dos dados")
    st.caption(_sb_ts_display)
    st.caption("Versão")
    st.caption(str(BUILD_TAG))

    if _admin_mode and _data_source_mode() == "upload_zip":
        _render_cloud_data_loader()

    st.divider()

    if _admin_mode and st.button(
        "🔄 Atualizar dados",
        use_container_width=True,
        help="Limpa caches e recarrega (releitura dos artefatos); não executa materialização.",
        key="fdl_sb_admin_refresh",
    ):
        st.cache_data.clear()
        for _k in list(st.session_state.keys()):
            if str(_k).startswith("_frete_cache_"):
                st.session_state.pop(_k, None)
        st.rerun()

    st.button(
        "Sair",
        use_container_width=True,
        help="Encerra a sessão neste navegador.",
        type="tertiary",
        key="fdl_sb_logout",
        on_click=_sb_logout_click,
    )

_fdl_global_trace("05: após sidebar — antes do hero / painel principal")

if _fv == "repasse":
    try:
        _fdl_global_trace("repasse: a preparar base (map_acao / filtros negócio)")
        map_acao = {
            "Ok": "Ok",
            "Baixar no Bling": "Baixar no Bling",
            "Analisar manualmente": "Analisar diferença",
            "Verificar título no Bling": "Verificar recebimento",
            "Revisar venda zerada": "Revisar venda zerada",
            "Verificar faturamento": "Verificar faturamento",
        }
        tabela_geral["Ação sugerida operacional"] = (
            tabela_geral["Ação sugerida"].map(map_acao).fillna(tabela_geral["Ação sugerida"])
        )
        tabela = tabela_geral.copy()

        # Base operacional exclusiva de extrato (liberações/pagamentos):
        # somente linhas com Valor pago > 0.
        tabela["Valor pago"] = pd.to_numeric(tabela.get("Valor pago"), errors="coerce")
        tabela = tabela[tabela["Valor pago"].fillna(0).gt(0)].copy()

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

_fv = st.session_state["op_financeiro_view"]
if _fv == "repasse":
    st.caption(
        f"{_app_ctx.display_name} · {_active_org.display_name} · Financeiro · Repasse"
    )
    if _fdl_minimal_layout():
        st.title("Conciliação de Repasse")
        st.caption(
            "Acompanhe valores recebidos, pendências e ações operacionais do período."
        )
    else:
        st.markdown(
            """
<style>
.fdl-repasse-hero h1 {
  font-weight: 500;
  letter-spacing: -0.02em;
  font-size: 2.1rem;
  color: #0f172a;
  margin: 0 0 0.35rem 0;
  line-height: 1.2;
}
.fdl-repasse-hero p {
  color: #64748b;
  font-size: 1rem;
  margin: 0;
  line-height: 1.45;
  max-width: 48rem;
}
</style>
<div class="fdl-repasse-hero">
  <h1>Conciliação de Repasse</h1>
  <p>Acompanhe valores recebidos, pendências e ações operacionais do período.</p>
</div>
            """,
            unsafe_allow_html=True,
        )
    st.divider()
elif _fv == "faturamento":
    st.caption(
        f"{_app_ctx.display_name} · {_active_org.display_name} · Financeiro · Faturamento"
    )
    st.divider()
else:
    st.caption(
        f"{_app_ctx.display_name} · {_active_org.display_name} · Financeiro · Frete"
    )
    st.title("Conciliação de Frete")
    st.caption(
        "Compare frete cobrado pelo ML com o esperado, priorize cobranças indevidas e repasses a conferir."
    )
    st.divider()

if _fv == "repasse":
    try:
        _fdl_global_trace("repasse: a renderizar _painel_conciliacao_fragment (filtros UI)")
        _painel_conciliacao_fragment(tabela_operacional_base, ts_proc)
        _fdl_global_trace("repasse: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"repasse: ERRO no painel — {exc.__class__.__name__}")
        st.error("Erro ao renderizar a **Conciliação de Repasse** (filtros ou tabela).")
        st.exception(exc)
elif _fv == "faturamento":
    try:
        _fdl_global_trace("faturamento: a renderizar _painel_faturamento")
        _painel_faturamento(faturamento_df, faturamento_info, ts_proc, _active_org.org_id)
        _fdl_global_trace("faturamento: painel concluído")
    except Exception as exc:
        _fdl_global_trace(f"faturamento: ERRO no painel — {exc.__class__.__name__}")
        st.error("Erro ao renderizar o painel de **Faturamento**.")
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

