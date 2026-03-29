"""
Cadastro local de usuários do app operacional.

Controle de acesso por cliente (rótulo exibido na UI) e lista de empresas permitidas.

Senhas: pode usar texto em `senha` (legado / dev) ou `senha_pbkdf2` (salt + hash, sem plain no repo).
Opcional: defina `FDL_GAMA_HOME_LOGIN_PASSWORD` em variável de ambiente ou em st.secrets
(Streamlit Cloud) para validar em texto — útil para rodar sem redeploy; não commite esse valor.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import os
import secrets
from typing import Any


def normalizar_email(email: str) -> str:
    """E-mail canônico (chave de lookup em `USUARIOS`)."""
    return email.strip().lower()


_norm_email = normalizar_email

# PBKDF2-SHA256 (390k iterações) para a senha «mega fácil» — não armazena plain text no repositório.
_GAMA_PBKDF2 = {
    "iterations": 390_000,
    "salt_b64": "yMTi4JSSCM7Kn0IW/4OSbw==",
    "hash_b64": "ZxNiz4cia3d5U4W1baSGINYe4fPCqmfpGXRj1ijy+mQ=",
}

# Cliente 5 (Flávio) — PBKDF2 (sem armazenar senha em texto plano no repositório).
# Opcional em Cloud: `FDL_FLAVIO_LOGIN_PASSWORD` em secrets/env substitui o hash abaixo.
_FLAVIO_PBKDF2 = {
    "iterations": 390_000,
    "salt_b64": "Lb+fcc6zGdry0Jw7xHAuxA==",
    "hash_b64": "NtHcsWjXG21QscrTJ8x+m0oyGwhBtO7mahr0sChpBqE=",
}


def _verify_pbkdf2_password(
    plain: str, salt_b64: str, hash_b64: str, *, iterations: int
) -> bool:
    try:
        salt = base64.b64decode(salt_b64)
        expected = base64.b64decode(hash_b64)
    except (ValueError, binascii.Error):
        return False
    got = hashlib.pbkdf2_hmac(
        "sha256", plain.encode("utf-8"), salt, iterations, dklen=len(expected)
    )
    return secrets.compare_digest(got, expected)


def _optional_secret_str(key: str) -> str:
    """Lê variável de ambiente ou `st.secrets` (Streamlit Cloud)."""
    raw = os.environ.get(key, "").strip()
    if raw:
        return raw
    try:
        import streamlit as st

        return str(st.secrets.get(key, "")).strip()
    except Exception:
        return ""


def _gama_home_password_override(candidate: str) -> bool | None:
    """
    Se `FDL_GAMA_HOME_LOGIN_PASSWORD` estiver definido (env ou secrets), valida só contra esse valor.
    Retorna None se não houver override (usa PBKDF2 em `senha_pbkdf2`).
    """
    raw = os.environ.get("FDL_GAMA_HOME_LOGIN_PASSWORD", "").strip()
    if not raw:
        try:
            raw = str(st.secrets.get("FDL_GAMA_HOME_LOGIN_PASSWORD", "")).strip()
        except Exception:
            raw = ""
    if not raw:
        return None
    return secrets.compare_digest(candidate, raw)


def _senha_ok(row: dict[str, Any], senha: str) -> bool:
    if "senha_pbkdf2" in row:
        env_override = row.get("senha_env_override")
        if isinstance(env_override, str) and env_override.strip():
            raw = _optional_secret_str(env_override.strip())
            if raw:
                return secrets.compare_digest(senha, raw)
        else:
            ow = _gama_home_password_override(senha)
            if ow is True:
                return True
            if ow is False:
                return False
        cfg = row["senha_pbkdf2"]
        if not isinstance(cfg, dict):
            return False
        return _verify_pbkdf2_password(
            senha,
            str(cfg.get("salt_b64", "")),
            str(cfg.get("hash_b64", "")),
            iterations=int(cfg.get("iterations", 390_000)),
        )
    return str(row.get("senha", "")) == senha


# Chaves devem ser e-mail em minúsculas para lookup direto.
USUARIOS: dict[str, dict[str, Any]] = {
    "flavia.caetanoliveira@gmail.com": {
        "senha": "123456",
        "cliente": "Flávia Admin",
        "empresas": ["Antomóveis"],
    },
    "sac@antomoveis.com.br": {
        "senha": "Anto@moveis",
        "cliente": "Everton",
        "empresas": ["Antomóveis"],
    },
    "megafacilmoveis@gmail.com": {
        "senha_pbkdf2": _GAMA_PBKDF2,
        "cliente": "Grupo Mega Fácil",
        "empresas": ["Gama Home", "Mega Fácil", "Mega Star", "Móveis EAP"],
    },
    "esquilomoveis1@gmail.com": {
        "nome": "Flávio",
        "senha_pbkdf2": _FLAVIO_PBKDF2,
        "senha_env_override": "FDL_FLAVIO_LOGIN_PASSWORD",
        "cliente": "Flávio",
        "empresas": ["Esquilo", "Wood"],
    },
}


def _login_email_allowed(email_norm: str) -> bool:
    """
    Se `FDL_OPERACIONAL_LOGIN_ALLOWLIST` (env ou st.secrets) estiver definida e não vazia,
    só esses e-mails (separados por vírgula) podem autenticar — útil para um app só do cliente 2.
    Se omitida, todos os cadastros em USUARIOS continuam válidos.
    """
    raw = os.environ.get("FDL_OPERACIONAL_LOGIN_ALLOWLIST", "").strip()
    if not raw:
        try:
            import streamlit as st

            raw = str(st.secrets.get("FDL_OPERACIONAL_LOGIN_ALLOWLIST", "")).strip()
        except Exception:
            raw = ""
    if not raw:
        return True
    allowed = {normalizar_email(x) for x in raw.split(",") if x.strip()}
    return email_norm in allowed


def autenticar(email: str, senha: str) -> dict[str, str | list[str]] | None:
    """
    Valida e-mail e senha. Retorna payload para a sessão ou None.
    Campos retornados: email, cliente, empresas.
    """
    email_norm = _norm_email(email)
    if not _login_email_allowed(email_norm):
        return None
    row = USUARIOS.get(email_norm)
    if row is None:
        return None
    if not _senha_ok(row, senha):
        return None
    empresas = row.get("empresas")
    if not isinstance(empresas, list):
        return None
    return {
        "email": _norm_email(email),
        "cliente": str(row.get("cliente", "")),
        "empresas": [str(x) for x in empresas],
    }
