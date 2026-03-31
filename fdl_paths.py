"""
Diretório base dos dados do cliente (vendas, liberações, notas, contas a receber).

Ordem de resolução (primeiro que existir):
1. Variável de ambiente `FDL_BASE_DIR`
2. `st.secrets["FDL_BASE_DIR"]` (Streamlit Community Cloud / local)
3. Pasta `./cliente_1` na raiz do repositório, **se existir** (dados originais / mesma árvore que no OneDrive)
4. Senão `./data_cliente`

Não altera regras de negócio — apenas centraliza onde o pipeline lê arquivos.
"""
from __future__ import annotations

import os
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent

# Pasta de export ML (detalhe de vendas). Clientes Antomóveis: "Vendas - Mercado Livre"; outros: "Vendas_ML".
PASTA_VENDAS_ML_CANONICA = "Vendas - Mercado Livre"
PASTA_VENDAS_ML_ALTERNATIVA = "Vendas_ML"


def resolve_pasta_vendas_ml(base_dir: str | Path) -> Path:
    """
    Resolve a pasta de ficheiros de vendas ML sob `base_dir`.
    Preferência: `Vendas - Mercado Livre`; se não existir, `Vendas_ML`.
    Se nenhuma existir, devolve o caminho canónico (para mensagens de erro consistentes).
    """
    base = Path(base_dir)
    canon = base / PASTA_VENDAS_ML_CANONICA
    alt = base / PASTA_VENDAS_ML_ALTERNATIVA
    if canon.is_dir():
        return canon
    if alt.is_dir():
        return alt
    return canon


def _resolve_base_dir() -> Path:
    raw = os.environ.get("FDL_BASE_DIR", "").strip()
    if raw:
        return Path(raw).expanduser().resolve()

    try:
        import streamlit as st  # noqa: WPS433 — import tardio para scripts sem Streamlit

        if hasattr(st, "secrets"):
            sec = st.secrets
            if sec and "FDL_BASE_DIR" in sec:
                return Path(str(sec["FDL_BASE_DIR"])).expanduser().resolve()
    except Exception:
        pass

    cliente1 = _REPO_ROOT / "cliente_1"
    if cliente1.is_dir():
        return cliente1.resolve()

    return (_REPO_ROOT / "data_cliente").resolve()


BASE_DIR = _resolve_base_dir()
CLIENTE_BASE_DIR = BASE_DIR
