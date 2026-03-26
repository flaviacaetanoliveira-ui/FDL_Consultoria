from __future__ import annotations

from datetime import date, datetime
from io import BytesIO
import base64
import json
import hashlib
import os
from pathlib import Path
import shutil
import time
import unicodedata
from urllib.parse import parse_qsl, quote, urlencode, urlparse, urlunparse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
import zipfile

import pandas as pd
import streamlit as st
from openpyxl.styles import numbers
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

st.set_page_config(page_title="Conciliação de Repasse", layout="wide")

_app_ctx = require_app_user()
_active_org = get_active_organization(_app_ctx)

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


def _is_admin_mode() -> bool:
    env_mode = os.environ.get("FDL_APP_MODE", "").strip().lower()
    if env_mode == "admin":
        return True
    try:
        return str(st.secrets.get("FDL_APP_MODE", "")).strip().lower() == "admin"
    except Exception:
        return False


def _data_source_mode() -> str:
    env_source = os.environ.get("FDL_DATA_SOURCE", "").strip().lower()
    if env_source:
        return env_source
    try:
        return str(st.secrets.get("FDL_DATA_SOURCE", "onedrive")).strip().lower()
    except Exception:
        return "onedrive"


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


def _build_onedrive_download_url(public_url: str) -> str:
    parsed = urlparse(public_url)
    host = parsed.netloc.lower()
    if "1drv.ms" in host:
        encoded = base64.urlsafe_b64encode(public_url.encode("utf-8")).decode("utf-8").rstrip("=")
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
    if mirror_root.exists():
        shutil.rmtree(mirror_root)
    mirror_root.mkdir(parents=True, exist_ok=True)

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

    def _sync_folder(item_url: str, dest: Path, relative_parts: tuple[str, ...] = ()) -> None:
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
                child_path = dest / child_name
                if "folder" in child:
                    if root_is_graph:
                        child_url = _graph_item_url_from_child(child)
                    else:
                        rel_path = _encode_rel_path(*(((path_prefix,) if path_prefix else ()) + child_rel))
                        child_url = f"https://api.onedrive.com/v1.0/shares/u!{share_token}/root:/{rel_path}:"
                    if not child_url:
                        continue
                    _sync_folder(child_url, child_path, child_rel)
                    continue

                if "file" not in child:
                    continue
                dl_url = _download_url(child)
                if not dl_url:
                    continue
                content, _ = _download_file_bytes(dl_url)
                child_path.parent.mkdir(parents=True, exist_ok=True)
                child_path.write_bytes(content)
            next_url = str(payload.get("@odata.nextLink", "")).strip()

    root_label = target_root_name if target_root_name.strip() else "link_root"
    dataset_root = mirror_root / root_label
    _sync_folder(cliente_1_url, dataset_root)

    def _norm_folder_name(value: str) -> str:
        s = unicodedata.normalize("NFKD", str(value)).encode("ascii", "ignore").decode().lower().strip()
        s = s.replace("-", " ").replace("_", " ")
        s = " ".join(s.split())
        return s

    required_aliases: dict[str, tuple[str, ...]] = {
        "Vendas - Mercado Livre": ("vendas mercado livre", "vendas ml"),
        "Liberações_ML": ("liberacoes ml", "liberacoes_ml"),
        "notas_saida": ("notas saida", "nota saida", "notas_saida"),
        "contas_receber": ("contas receber", "contas a receber", "contas_receber"),
    }
    norm_to_required: dict[str, str] = {}
    for required_name, aliases in required_aliases.items():
        norm_to_required[_norm_folder_name(required_name)] = required_name
        for alias in aliases:
            norm_to_required[_norm_folder_name(alias)] = required_name

    def _resolve_required_subfolders(base_path: Path) -> dict[str, Path]:
        resolved: dict[str, Path] = {}
        if not base_path.exists() or not base_path.is_dir():
            return resolved
        for child in base_path.iterdir():
            if not child.is_dir():
                continue
            key = norm_to_required.get(_norm_folder_name(child.name))
            if key and key not in resolved:
                resolved[key] = child
        return resolved

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

    candidate_roots = [dataset_root]
    preferred = dataset_root / target_client_name
    if preferred.exists() and preferred.is_dir():
        candidate_roots.insert(0, preferred)
    for candidate in _all_candidate_roots(dataset_root):
        if candidate not in candidate_roots:
            candidate_roots.append(candidate)

    selected_root: Path | None = None
    selected_mapping: dict[str, Path] = {}
    for candidate in candidate_roots:
        mapping = _resolve_required_subfolders(candidate)
        if len(mapping) == len(REQUIRED_ONEDRIVE_SOURCE_FOLDERS):
            selected_root = candidate
            selected_mapping = mapping
            break

    if selected_root is None:
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


@st.cache_data(show_spinner=False, ttl=900)
def _download_file_bytes(url: str, _revisao: int = OPERACIONAL_CACHE_REVISION) -> tuple[bytes, str]:
    del _revisao
    req = Request(url, headers={"User-Agent": "FDL-Streamlit-App/1.0"})
    for attempt in range(MAX_HTTP_RETRIES + 1):
        try:
            with urlopen(req, timeout=60) as resp:
                payload = resp.read()
                filename = ""
                cd = resp.headers.get("Content-Disposition", "")
                if "filename=" in cd:
                    filename = cd.split("filename=", 1)[1].strip().strip("\"'")
                if not filename:
                    filename = Path(urlparse(url).path).name or "download.bin"
            return payload, filename
        except HTTPError as exc:
            if exc.code not in RETRYABLE_HTTP_CODES or attempt >= MAX_HTTP_RETRIES:
                raise
            retry_after = str(exc.headers.get("Retry-After", "")).strip()
            sleep_s = float(retry_after) if retry_after.isdigit() else (1.5**attempt)
            time.sleep(min(max(sleep_s, 0.5), 8.0))
        except URLError:
            if attempt >= MAX_HTTP_RETRIES:
                raise
            time.sleep(min(1.5**attempt, 8.0))
    raise RuntimeError(f"Falha ao baixar arquivo após tentativas: {url}")


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


def load_data_from_onedrive() -> tuple[pd.DataFrame, dict[str, object], str]:
    public_url = _onedrive_public_url()
    if not public_url:
        raise ValueError("FDL_ONEDRIVE_URL não configurada.")

    if ":f:/" in public_url.lower():
        _download_shared_folder_dataset(public_url)
        return carregar_tabela_final_operacional_cache()

    download_url = _build_onedrive_download_url(public_url)
    payload, filename = _download_file_bytes(download_url)
    lower_name = filename.lower()

    if lower_name.endswith(".zip") or zipfile.is_zipfile(BytesIO(payload)):
        _sync_payload_zip_to_base(payload)
        return carregar_tabela_final_operacional_cache()

    if lower_name.endswith(".csv"):
        tabela = pd.read_csv(BytesIO(payload), sep=None, engine="python")
        _validate_onedrive_csv_schema(tabela)
        if "empresa" not in tabela.columns:
            tabela = tabela.copy()
            tabela["empresa"] = DATASET_EMPRESA
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
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


def _load_data() -> tuple[pd.DataFrame, dict[str, object], str]:
    source = _data_source_mode()
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


st.markdown(
    """
    <style>
      html, body, [class*="css"] {
        font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      }
      .main .block-container { padding-top: 0.5rem; padding-bottom: 2rem; max-width: 1400px; }

      .fdl-topbar {
        display: flex; justify-content: space-between; align-items: center;
        background: linear-gradient(105deg, #0f172a 0%, #1e3a5f 55%, #0f172a 100%);
        color: #f8fafc; padding: 0.85rem 1.35rem; margin: 0 0 1.35rem 0;
        border-radius: 12px; box-shadow: 0 8px 24px rgba(15, 23, 42, 0.18);
        border: 1px solid rgba(148, 163, 184, 0.15);
      }
      .fdl-topbar-brand { font-size: 1.35rem; font-weight: 700; letter-spacing: -0.02em; color: #fff; }
      .fdl-topbar-brand span { font-weight: 500; opacity: 0.85; font-size: 0.82rem; margin-left: 0.35rem; vertical-align: middle; }
      .fdl-topbar-meta { text-align: right; font-size: 0.88rem; line-height: 1.55; color: #e2e8f0; }
      .fdl-topbar-meta strong { color: #fff; font-weight: 600; }
      .fdl-topbar-meta .fdl-sep { color: #64748b; margin: 0 0.5rem; }

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
        min-height: 5.1rem; transition: box-shadow 0.15s ease;
      }
      .kpi-card:hover { box-shadow: 0 4px 14px rgba(15, 23, 42, 0.08); }
      .kpi-icon { font-size: 1.15rem; margin-right: 0.35rem; opacity: 0.95; }
      .kpi-label { font-size: 0.72rem; font-weight: 600; color: #475569; text-transform: uppercase; letter-spacing: 0.04em; }
      .kpi-value { font-size: 1.28rem; font-weight: 700; margin-top: 0.35rem; color: #0f172a; letter-spacing: -0.02em; }
      .kpi-total { border-left: 4px solid #0284c7; background: linear-gradient(90deg, #f0f9ff 0%, #fff 55%); }
      .kpi-ok { border-left: 4px solid #16a34a; background: linear-gradient(90deg, #f0fdf4 0%, #fff 55%); }
      .kpi-acao { border-left: 4px solid #0891b2; background: linear-gradient(90deg, #ecfeff 0%, #fff 55%); }
      .kpi-div { border-left: 4px solid #ea580c; background: linear-gradient(90deg, #fff7ed 0%, #fff 55%); }
      .kpi-pend { border-left: 4px solid #7c3aed; background: linear-gradient(90deg, #f5f3ff 0%, #fff 55%); }

      .queue-head { margin-top: 0.25rem; margin-bottom: 0.65rem; }
      .queue-title { font-size: 1.05rem; font-weight: 700; color: #0f172a; }
      .queue-sub { font-size: 0.86rem; color: #64748b; margin-top: 0.2rem; }

      /* Sidebar — navegação em árvore (desktop app / SaaS) */
      div[data-testid="stSidebar"] {
        background: linear-gradient(195deg, #f1f5f9 0%, #e8eef4 50%, #f8fafc 100%) !important;
        border-right: 1px solid #c7d2e0 !important;
        box-shadow: inset -1px 0 0 rgba(255,255,255,0.5), 3px 0 20px rgba(15, 23, 42, 0.05);
      }
      div[data-testid="stSidebar"] .block-container {
        padding-top: 1rem !important;
        padding-bottom: 2rem !important;
      }

      .sb-header-shell {
        background: linear-gradient(145deg, #ffffff 0%, #f8fafc 55%, #f1f5f9 100%);
        border: 1px solid #e2e8f0; border-radius: 14px;
        padding: 1.15rem 1.1rem 1rem 1.1rem; margin: 0 0 1.25rem 0;
        box-shadow: 0 2px 8px rgba(15, 23, 42, 0.06), 0 1px 0 rgba(255,255,255,0.8) inset;
      }
      .sb-header-title {
        font-size: 1.28rem; font-weight: 800; letter-spacing: -0.035em; color: #0f172a;
        line-height: 1.15; margin: 0;
      }
      .sb-header-sub {
        font-size: 0.74rem; font-weight: 500; color: #64748b; margin: 0.45rem 0 0 0;
        line-height: 1.4; letter-spacing: 0.03em;
      }
      .sb-client-line {
        font-size: 0.8rem; color: #475569; margin: 1rem 0 0 0; padding-top: 0.85rem;
        border-top: 1px solid #e2e8f0; font-weight: 500;
      }
      .sb-client-line .sb-client-k { font-size: 0.68rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.06em; color: #94a3b8; display: block; margin-bottom: 0.2rem; }
      .sb-client-line strong { color: #0f172a; font-weight: 700; font-size: 0.92rem; }

      .sb-nav-section-label {
        font-size: 0.68rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.1em;
        color: #94a3b8; margin: 0 0 0.5rem 0.15rem;
      }

      .sb-divider-soft { height: 1px; background: linear-gradient(90deg, transparent, #cbd5e1 12%, #cbd5e1 88%, transparent); margin: 0 0 1rem 0; border: 0; }

      /* Nível 1 — empresa (árvore; expander aninhado sobrescreve abaixo) */
      div[data-testid="stSidebar"] [data-testid="stExpander"] {
        background: transparent !important;
        border: none !important;
        box-shadow: none !important;
        margin: 0 0 0.35rem 0 !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] details {
        border: 1px solid #e2e8f0 !important;
        border-radius: 10px !important;
        background: rgba(255,255,255,0.75) !important;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.04) !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary {
        padding: 0.72rem 0.85rem !important;
        font-weight: 700 !important; font-size: 0.9rem !important; color: #0f172a !important;
        background: transparent !important;
        border-radius: 10px !important;
        list-style: none;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
        background: rgba(241,245,249,0.95) !important;
      }
      div[data-testid="stSidebar"] [data-testid="stExpander"] summary::-webkit-details-marker { display: none; }

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

      /* Botão secundário premium */
      div[data-testid="stSidebar"] .stButton { margin-top: 0.5rem; }
      div[data-testid="stSidebar"] .stButton > button {
        background: linear-gradient(180deg, #ffffff 0%, #f8fafc 100%) !important;
        color: #0f172a !important; border: 1px solid #cbd5e1 !important;
        font-weight: 600 !important; font-size: 0.88rem !important;
        padding: 0.65rem 1rem !important; border-radius: 10px !important;
        box-shadow: 0 1px 3px rgba(15, 23, 42, 0.07), 0 1px 0 rgba(255,255,255,0.9) inset !important;
        width: 100%;
        transition: background 0.18s ease, border-color 0.18s ease, box-shadow 0.18s ease;
      }
      div[data-testid="stSidebar"] .stButton > button:hover {
        background: #f1f5f9 !important;
        border-color: #94a3b8 !important;
        box-shadow: 0 3px 10px rgba(15, 23, 42, 0.1) !important;
      }
      div[data-testid="stSidebar"] .stButton > button:focus {
        box-shadow: 0 0 0 3px rgba(148, 163, 184, 0.35) !important;
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

      div[data-testid="stDataFrame"] [data-testid="stTable"] { border-collapse: separate; border-spacing: 0; font-size: 0.9rem; }
      div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:nth-child(even) { background-color: #f8fafc; }
      div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody tr:hover { background-color: #f1f5f9 !important; }
      div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th {
        background: linear-gradient(180deg, #0f172a 0%, #1e293b 55%, #0f172a 100%) !important;
        color: #ffffff !important;
        -webkit-text-fill-color: #ffffff !important;
        font-weight: 800 !important;
        font-size: 0.84rem !important;
        text-transform: uppercase;
        letter-spacing: 0.07em;
        padding: 0.85rem 0.8rem !important;
        border-bottom: 3px solid #0ea5e9 !important;
        border-top: none !important;
        text-shadow: 0 1px 3px rgba(0,0,0,0.45);
      }
      div[data-testid="stDataFrame"] [data-testid="stTable"] table thead tr th * {
        color: #ffffff !important;
      }
      div[data-testid="stDataFrame"] [data-testid="stTable"] table tbody td {
        padding: 0.55rem 0.75rem !important;
        border-bottom: 1px solid #e2e8f0;
        vertical-align: middle;
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
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tabela, info = carregar_tabela_final_operacional(BASE_DIR)
    return tabela, info, ts


def _style_acao(v: str) -> str:
    cores = {
        "Ok": "background-color: #dcfce7; color: #14532d;",
        "Baixar no Bling": "background-color: #e0f2fe; color: #0c4a6e;",
        "Analisar diferença": "background-color: #ffedd5; color: #9a3412;",
        "Verificar recebimento": "background-color: #f3e8ff; color: #581c87;",
        "Verificar faturamento": "background-color: #ede9fe; color: #4c1d95;",
        "Revisar venda zerada": "background-color: #fee2e2; color: #991b1b;",
    }
    return cores.get(str(v), "")


def _style_row(row: pd.Series) -> list[str]:
    cor = _style_acao(row.get("Ação sugerida", ""))
    if not cor:
        return [""] * len(row)
    return [cor] * len(row)


def _render_kpi_card(label: str, value: str, icon: str, css_class: str) -> None:
    st.markdown(
        f"""
        <div class="kpi-card {css_class}">
          <div class="kpi-label"><span class="kpi-icon" aria-hidden="true">{icon}</span>{label}</div>
          <div class="kpi-value">{value}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


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
    s = series.fillna("").astype(str).str.strip().str.replace("NaT", "", regex=False).str.replace(
        "None", "", regex=False
    )
    return pd.to_datetime(s, format="%Y-%m-%d", errors="coerce")


def _parse_data_pagamento_final(series: pd.Series) -> pd.Series:
    """Tabela final: YYYY-MM-DD HH:MM:SS — ISO, sem dayfirst."""
    s = series.fillna("").astype(str).str.strip().str.replace("NaT", "", regex=False).str.replace(
        "None", "", regex=False
    )
    return pd.to_datetime(s, errors="coerce")


_admin_mode = _is_admin_mode()
if _admin_mode and _data_source_mode() == "upload_zip":
    _render_cloud_data_loader()

try:
    tabela_geral, info, ts_proc = _load_data()
except Exception as exc:
    if _admin_mode:
        st.warning("Dados indisponíveis no momento.")
        st.caption(f"Detalhe técnico: {exc}")
    else:
        st.warning("Dados indisponíveis no momento. Tente novamente em instantes.")
    st.stop()

# Cache antigo do Streamlit ou pickle sem a coluna — alinhar ao pipeline atual.
if "empresa" not in tabela_geral.columns:
    tabela_geral = tabela_geral.copy()
    tabela_geral["empresa"] = DATASET_EMPRESA

empresas = st.session_state["empresas_permitidas"]
tabela_geral = tabela_geral[tabela_geral["empresa"].isin(empresas)].copy()
info = {**info, "linhas": int(len(tabela_geral))}

try:
    _sb_ts_display = datetime.strptime(ts_proc, "%Y-%m-%d %H:%M:%S").strftime("%d/%m/%Y %H:%M")
except ValueError:
    _sb_ts_display = ts_proc

with st.sidebar:
    st.markdown(
        f"""
        <div class="sb-header-shell">
          <div class="sb-header-title">FDL Analytics</div>
          <div class="sb-header-sub">Financial Intelligence for E-commerce</div>
          <div class="sb-client-line">
            <span class="sb-client-k">Cliente</span>
            <strong>{_app_ctx.display_name}</strong>
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<hr class="sb-divider-soft" />', unsafe_allow_html=True)
    st.markdown('<p class="sb-nav-section-label">Empresas</p>', unsafe_allow_html=True)

    _empresas_usuario = list(st.session_state["empresas_permitidas"])
    _nomes_nav = nomes_permitidos_com_registro(_empresas_usuario)

    if len(_nomes_nav) > 1:
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
        )
        _chosen_org = organizacao_por_nome_cadastrado(_sel_nome)
        if _chosen_org and _chosen_org.org_id != _app_ctx.active_org_id:
            st.session_state[SESSION_ACTIVE_ORG_KEY] = _chosen_org.org_id
            st.rerun()

    with st.expander(f"🏢 {_active_org.display_name}", expanded=True):
        with st.expander("💰 Financeiro", expanded=True):
            st.markdown(
                """
                <nav class="sb-nav-tree" aria-label="Páginas do módulo Financeiro">
                  <div class="sb-nav-item sb-nav-item-active" aria-current="page">
                    <span class="sb-active-accent" aria-hidden="true"></span>
                    <span class="sb-ico" aria-hidden="true">📊</span>
                    <span class="sb-nav-label">Conciliação de Repasse</span>
                  </div>
                  <div class="sb-nav-item sb-nav-item-placeholder" aria-disabled="true">
                    <span class="sb-ico" aria-hidden="true">🚚</span>
                    <span class="sb-nav-label">Conciliação de Frete <span class="sb-soon">em breve</span></span>
                  </div>
                </nav>
                """,
                unsafe_allow_html=True,
            )

    st.markdown('<hr class="sb-divider-soft" />', unsafe_allow_html=True)
    if st.button("Sair", use_container_width=True, help="Encerra a sessão neste navegador."):
        logout_operacional_user()
        st.rerun()

    if _admin_mode and st.button(
        "🔄 Atualizar dados",
        use_container_width=True,
        help="Atualiza a leitura de dados e limpa cache interno.",
    ):
        st.cache_data.clear()
        st.session_state["_conciliacao_dados_atualizados"] = True
        st.rerun()
    if _admin_mode and st.session_state.pop("_conciliacao_dados_atualizados", False):
        st.success(f"Dados atualizados com sucesso. Última leitura: **{ts_proc}**")

    st.markdown(
        f"""
        <div class="sb-sync-block">
          <div class="sb-sync-label">Última atualização</div>
          <div class="sb-sync-ts">{_sb_ts_display}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

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

st.markdown(
    f"""
    <div class="fdl-topbar">
      <div class="fdl-topbar-brand">FDL Analytics</div>
      <div class="fdl-topbar-meta">
        <span><strong>Cliente:</strong> {_app_ctx.display_name}</span>
        <span class="fdl-sep">|</span>
        <span><strong>Empresa:</strong> {_active_org.display_name}</span>
      </div>
    </div>
    <div class="page-hero">
      <nav class="fdl-breadcrumb" aria-label="Localização no sistema">
        <span class="fdl-bc-item">{_app_ctx.display_name}</span>
        <span class="fdl-bc-sep" aria-hidden="true">›</span>
        <span class="fdl-bc-item">{_active_org.display_name}</span>
        <span class="fdl-bc-sep" aria-hidden="true">›</span>
        <span class="fdl-bc-item">Financeiro</span>
        <span class="fdl-bc-sep" aria-hidden="true">›</span>
        <span class="fdl-bc-item fdl-bc-current">Conciliação de Repasse</span>
      </nav>
      <h1>Conciliação de Repasse</h1>
      <p class="page-sub">
        Painel para acompanhar valores recebidos na plataforma, conferência com notas e fila de ações
        sugeridas — sempre sobre a base já filtrada pelos critérios operacionais do módulo.
      </p>
    </div>
    """,
    unsafe_allow_html=True,
)

with st.container(border=True):
    st.markdown('<p class="filtros-panel-title">Filtros operacionais</p>', unsafe_allow_html=True)
    r1 = st.columns((1.15, 1.15, 1.15, 1.55))
    r2 = st.columns((1.15, 1.15, 2.3))
    dp_series_full = pd.to_datetime(tabela_operacional_base["Data de pagamento"], errors="coerce")
    if dp_series_full.notna().any():
        _d_min: date = dp_series_full.min().date()
        _d_max: date = dp_series_full.max().date()
    else:
        _d_min = _d_max = datetime.now().date()
    plats = (
        sorted([x for x in tabela_operacional_base["Plataforma"].dropna().unique().tolist() if str(x).strip()])
        if "Plataforma" in tabela_operacional_base.columns
        else []
    )
    acoes = sorted(
        [
            x
            for x in tabela_operacional_base["Ação sugerida operacional"].dropna().unique().tolist()
            if str(x).strip()
        ]
    )
    sit = sorted(
        [x for x in tabela_operacional_base["Situação"].dropna().unique().tolist() if str(x).strip()]
    )
    with r1[0]:
        sel_plat = st.multiselect("Plataforma", plats, default=plats)
    with r1[1]:
        sel_acao = st.multiselect("Ação sugerida", acoes, default=acoes)
    with r1[2]:
        sel_sit = st.multiselect("Situação", sit, default=sit)
    with r1[3]:
        busca = st.text_input("Busca (venda / pedido / nota)", "").strip().lower()
    with r2[0]:
        data_pag_ini = st.date_input(
            "Data de pagamento — início",
            value=_d_min,
            min_value=_d_min,
            max_value=_d_max,
            format="DD/MM/YYYY",
        )
    with r2[1]:
        data_pag_fim = st.date_input(
            "Data de pagamento — fim",
            value=_d_max,
            min_value=_d_min,
            max_value=_d_max,
            format="DD/MM/YYYY",
        )
    st.caption(
        "O intervalo de datas restringe as linhas pela **data de pagamento** do registro (comparado por dia)."
    )

if data_pag_fim < data_pag_ini:
    st.warning("A data final não pode ser anterior à data inicial. Ajuste o período.")
    data_pag_fim = data_pag_ini

tabela = tabela_operacional_base.copy()
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
_dd = _dp_filt.dt.normalize()
_ini_ts = pd.Timestamp(data_pag_ini)
_fim_ts = pd.Timestamp(data_pag_fim) + pd.Timedelta(days=1)
m_data = _dp_filt.notna() & (_dd >= _ini_ts) & (_dd < _fim_ts)
tabela = tabela.loc[m_data].copy()

if "Plataforma" in tabela_operacional_base.columns:
    n_plat = len(plats)
    if len(sel_plat) == 0:
        plataforma_label = "Nenhuma"
    elif n_plat and len(sel_plat) == n_plat:
        plataforma_label = "Todas"
    else:
        plataforma_label = ", ".join(sel_plat[:2]) + ("..." if len(sel_plat) > 2 else "")
else:
    plataforma_label = "Mercado Livre"

st.markdown(
    f"""
    <p class="page-meta" style="margin-bottom:1.1rem;">
      <strong>Plataforma (filtro):</strong> {plataforma_label}
      &nbsp;·&nbsp;
      <strong>Dados carregados:</strong> {ts_proc}
      &nbsp;·&nbsp;
      <strong>Pagamento:</strong> {data_pag_ini.strftime("%d/%m/%Y")} a {data_pag_fim.strftime("%d/%m/%Y")}
    </p>
    """,
    unsafe_allow_html=True,
)

# Tipos numéricos para a base já filtrada (mesmo conjunto usado nos KPIs e na tabela)
tabela["Valor da nota"] = pd.to_numeric(tabela["Valor da nota"], errors="coerce").fillna(0.0)
tabela["Total BRL"] = pd.to_numeric(tabela.get("Total BRL"), errors="coerce")
tabela["Valor a receber"] = pd.to_numeric(tabela.get("Valor a receber"), errors="coerce")
tabela["Valor pago"] = pd.to_numeric(tabela.get("Valor pago"), errors="coerce")
tabela["Diferença"] = pd.to_numeric(tabela.get("Diferença"), errors="coerce")

# KPIs — mesma lógica de sempre, sobre a base **após** os filtros operacionais
st.markdown('<div class="section-title">Fluxo financeiro do período</div>', unsafe_allow_html=True)
data_pag_dt = pd.to_datetime(tabela.get("Data de pagamento"), errors="coerce")
mask_recebido = data_pag_dt.notna() & tabela["Valor pago"].fillna(0).gt(0)
mask_baixado = tabela["Ação sugerida operacional"].eq("Ok")
mask_recebido_nao_baixado = tabela["Ação sugerida operacional"].eq("Baixar no Bling")
mask_divergencia = tabela["Diferença"].abs().gt(0.01)
mask_em_aberto = tabela["Valor pago"].fillna(0).le(0)
kpi_valor_recebido = float(tabela.loc[mask_recebido, "Valor pago"].sum())
kpi_baixado = float(tabela.loc[mask_baixado, "Valor pago"].sum())
kpi_recebido_nao_baixado = float(tabela.loc[mask_recebido_nao_baixado, "Valor pago"].sum())
kpi_divergencia = float(tabela.loc[mask_divergencia, "Diferença"].abs().sum())
kpi_em_aberto = float(tabela.loc[mask_em_aberto, "Valor a receber"].sum())
st.markdown(
    f'<div class="money-hero"><b>Valor recebido no período:</b> R$ {kpi_valor_recebido:,.2f}</div>',
    unsafe_allow_html=True,
)
k1, k2, k3, k4, k5 = st.columns(5)
with k1:
    _render_kpi_card(
        "Valor recebido no período", f"R$ {kpi_valor_recebido:,.2f}", "◆", "kpi-total"
    )
with k2:
    _render_kpi_card("Baixado no Bling", f"R$ {kpi_baixado:,.2f}", "✓", "kpi-ok")
with k3:
    _render_kpi_card("Recebido e não baixado", f"R$ {kpi_recebido_nao_baixado:,.2f}", "◇", "kpi-acao")
with k4:
    _render_kpi_card("Divergência de valores", f"R$ {kpi_divergencia:,.2f}", "!", "kpi-div")
with k5:
    _render_kpi_card("Em aberto", f"R$ {kpi_em_aberto:,.2f}", "○", "kpi-pend")

# Validação de consistência dos KPIs (base filtrada)
st.markdown('<div class="section-title">Validação de ações (base filtrada)</div>', unsafe_allow_html=True)
acoes_validacao = [
    "Ok",
    "Baixar no Bling",
    "Analisar diferença",
    "Verificar recebimento",
    "Verificar faturamento",
    "Revisar venda zerada",
]
contagens_acao = {a: int(tabela["Ação sugerida operacional"].eq(a).sum()) for a in acoes_validacao}
st.markdown(
    f"""
    <div class="validacao-badges">
      <span class="badge-acao badge-ok">Ok <b>{contagens_acao["Ok"]}</b></span>
      <span class="badge-acao badge-bling">Baixar no Bling <b>{contagens_acao["Baixar no Bling"]}</b></span>
      <span class="badge-acao badge-analisar">Analisar diferença <b>{contagens_acao["Analisar diferença"]}</b></span>
      <span class="badge-acao badge-verificar">Verificar recebimento <b>{contagens_acao["Verificar recebimento"]}</b></span>
      <span class="badge-acao badge-faturamento">Verificar faturamento <b>{contagens_acao["Verificar faturamento"]}</b></span>
      <span class="badge-acao badge-revisar">Revisar venda zerada <b>{contagens_acao["Revisar venda zerada"]}</b></span>
    </div>
    """,
    unsafe_allow_html=True,
)

# Bloco C - Contexto filtrado
st.markdown('<div class="section-title">Contexto filtrado</div>', unsafe_allow_html=True)
data_pag = pd.to_datetime(tabela.get("Data de pagamento"), errors="coerce")
periodo_filtro = (
    f"{data_pag_ini.strftime('%d/%m/%Y')} a {data_pag_fim.strftime('%d/%m/%Y')} (filtro data pagamento)"
)
if data_pag.notna().any():
    periodo = f"{periodo_filtro} · registros entre {data_pag.min().date()} e {data_pag.max().date()}"
else:
    periodo = f"{periodo_filtro} · sem registros neste recorte"
c1, c2, c3, c4 = st.columns(4)
c1.caption(f"Linhas filtradas: **{len(tabela)}**")
c2.caption(f"Plataforma: **{plataforma_label}**")
c3.caption(f"Período: **{periodo}**")
c4.caption(f"Última atualização: **{ts_proc}**")

st.caption(f"Linhas carregadas: {info.get('linhas',0)}")

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

tabela_exibir = tabela[exibir_cols].copy()
tabela_exibir["Valor da nota"] = pd.to_numeric(tabela_exibir["Valor da nota"], errors="coerce")
tabela_exibir["Valor a receber"] = pd.to_numeric(tabela_exibir["Valor a receber"], errors="coerce")
tabela_exibir["Valor pago"] = pd.to_numeric(tabela_exibir.get("Valor pago"), errors="coerce")
tabela_exibir["Diferença"] = pd.to_numeric(tabela_exibir["Diferença"], errors="coerce")
if col_data_emissao:
    tabela_exibir["Data de emissão"] = _parse_data_emissao_final(tabela.loc[tabela_exibir.index, col_data_emissao])
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
tabela_exibir = tabela_exibir[
    [
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
]
# Ordenação padrão operacional: pagamentos mais recentes primeiro.
tabela_exibir = tabela_exibir.sort_values(
    by="Data de pagamento", ascending=False, na_position="last"
).reset_index(drop=True)

st.markdown(
    """
    <div class="queue-head">
      <div>
        <div class="queue-title">Fila Operacional</div>
        <div class="queue-sub">Casos prontos para tratamento</div>
      </div>
    </div>
    """,
    unsafe_allow_html=True,
)

btn1, btn2, btn3 = st.columns([1, 1, 1])
csv_bytes = tabela_exibir.to_csv(index=False).encode("utf-8-sig")
with btn1:
    st.download_button(
        "Exportar CSV",
        data=csv_bytes,
        file_name="conciliacao_operacional_filtrada.csv",
        mime="text/csv",
        width="stretch",
    )
# Exportação Excel: datas já são datetime (mesmo critério da tela); só formato de célula.
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
                    cell.number_format = numbers.FORMAT_DATE_DDMMYY
excel_buf.seek(0)
with btn2:
    st.download_button(
        "⭐ Exportar Excel",
        data=excel_buf.getvalue(),
        file_name="conciliacao_operacional_filtrada.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
    )
with btn3:
    st.download_button(
        "Exportar PDF",
        data=_build_pdf_bytes(tabela_exibir),
        file_name="conciliacao_operacional_filtrada.pdf",
        mime="application/pdf",
        width="stretch",
    )

fmt = {
    "Valor da nota": "R$ {:,.2f}",
    "Valor a receber": "R$ {:,.2f}",
    "Valor pago": "R$ {:,.2f}",
    "Diferença": "R$ {:,.2f}",
    "Data de emissão": lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "",
    "Data de pagamento": lambda x: x.strftime("%d/%m/%Y") if pd.notna(x) else "",
}
# Cabeçalho explícito no HTML do Styler (reforça contraste no tema Streamlit).
_table_header_css = [
    {
        "selector": "thead th",
        "props": [
            ("background-color", "#0f172a"),
            ("color", "#ffffff"),
            ("font-weight", "800"),
            ("font-size", "0.84rem"),
            ("letter-spacing", "0.06em"),
            ("text-transform", "uppercase"),
            ("padding", "0.75rem 0.65rem"),
            ("border-bottom", "3px solid #0ea5e9"),
            ("vertical-align", "middle"),
        ],
    },
]
sty = (
    tabela_exibir.style.set_table_styles(_table_header_css)
    .format(fmt)
    .apply(_style_row, axis=1)
)
st.dataframe(sty, width="stretch", height=550)
st.write(f"Linhas filtradas: **{len(tabela_exibir)}**")

