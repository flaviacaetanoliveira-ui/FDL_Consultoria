"""
Cadastro local de usuários do app operacional.

Controle de acesso por cliente (rótulo exibido na UI) e lista de empresas permitidas.
Para produção, prefira hash de senha e segredos fora do código (ex.: st.secrets).
"""
from __future__ import annotations


def normalizar_email(email: str) -> str:
    """E-mail canônico (chave de lookup em `USUARIOS`)."""
    return email.strip().lower()


_norm_email = normalizar_email


# Chaves devem ser e-mail em minúsculas para lookup direto.
USUARIOS: dict[str, dict[str, str | list[str]]] = {
    "flavia.caetanoliveira@gmail.com": {
        "senha": "123456",
        "cliente": "Flávia Admin",
        "empresas": ["Antomóveis"],
    },
    "sac@antomoveis.com.br": {
        "senha": "123456",
        "cliente": "Everton",
        "empresas": ["Antomóveis"],
    },
    # Ambiente cliente 2 (deployment separado): secrets só para Gama Home; trocar e-mail/senha em produção.
    "teste.gama@example.com": {
        "senha": "123456",
        "cliente": "Cliente 2",
        "empresas": ["Gama Home"],
    },
}


def autenticar(email: str, senha: str) -> dict[str, str | list[str]] | None:
    """
    Valida e-mail e senha. Retorna payload para a sessão ou None.
    Campos retornados: email, cliente, empresas.
    """
    row = USUARIOS.get(_norm_email(email))
    if row is None:
        return None
    if str(row.get("senha", "")) != senha:
        return None
    empresas = row.get("empresas")
    if not isinstance(empresas, list):
        return None
    return {
        "email": _norm_email(email),
        "cliente": str(row.get("cliente", "")),
        "empresas": [str(x) for x in empresas],
    }
