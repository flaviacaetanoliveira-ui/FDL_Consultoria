"""
Contexto de usuário / organização / rotas para o app operacional.

`get_app_context()` usa `st.session_state` após login (`operacional_usuarios`).
"""
from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from operacional_usuarios import USUARIOS, autenticar, normalizar_email


@dataclass(frozen=True)
class ModuleRoute:
    """Rota lógica dentro de um módulo (para navegação multipágina futura)."""

    route_id: str
    label: str
    page_module: str  # ex.: nome do módulo Streamlit que renderiza a tela


@dataclass(frozen=True)
class AppOrganization:
    org_id: str
    display_name: str
    module_ids: tuple[str, ...]
    routes: tuple[ModuleRoute, ...]


@dataclass(frozen=True)
class AppUserContext:
    user_id: str
    display_name: str
    organizations: tuple[AppOrganization, ...]
    active_org_id: str
    """Rota em uso nesta página (uma única página hoje)."""

    active_route_id: str


"""
Metadados (rotas, org_id) por nome de empresa — usado só para resolver nomes
 vindos de `empresas_permitidas`, não como lista de navegação global.
"""
_REGISTRO_EMPRESA: dict[str, AppOrganization] = {
    "Antomóveis": AppOrganization(
        org_id="antomoveis",
        display_name="Antomóveis",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
        ),
    ),
}


def organizacao_por_nome_cadastrado(nome: str) -> AppOrganization | None:
    """Resolve nome (como em USUARIOS.empresas) para metadados, se existir registro."""
    return _REGISTRO_EMPRESA.get(nome)


def organizacoes_na_ordem_permitida(nomes_permitidos: list[str]) -> tuple[AppOrganization, ...]:
    """Só empresas cujo nome o usuário tem permissão e que possuem registro técnico."""
    return tuple(
        _REGISTRO_EMPRESA[n]
        for n in nomes_permitidos
        if n in _REGISTRO_EMPRESA
    )


def nomes_permitidos_com_registro(nomes_permitidos: list[str]) -> list[str]:
    """Preserva a ordem de `empresas_permitidas`; exclui nomes sem metadado."""
    return [n for n in nomes_permitidos if n in _REGISTRO_EMPRESA]


SESSION_ACTIVE_ORG_KEY = "_active_org_id"


def require_app_user() -> AppUserContext:
    """
    Garante usuário autenticado: exibe login ou devolve o contexto.
    Encerra a execução da página com st.stop() se não houver sessão.
    """
    if not st.session_state.get("logged_in"):
        # Só widgets nativos — CSS/HTML pesado na página de login causava ecrã em branco na
        # Streamlit Cloud / modo anónimo em alguns browsers.
        st.title("FDL Analytics")
        st.caption("Financial Intelligence for E-commerce")
        st.subheader("Autenticação")
        st.caption("Acesso ao painel operacional.")
        with st.form("operacional_login"):
            email = st.text_input(
                "E-mail",
                placeholder="nome@empresa.com.br",
                autocomplete="email",
            )
            senha = st.text_input(
                "Senha",
                type="password",
                placeholder="Digite sua senha",
                autocomplete="current-password",
            )
            submitted = st.form_submit_button("Entrar")
            if submitted:
                if autenticar(email, senha):
                    email_key = normalizar_email(email)
                    row = USUARIOS[email_key]
                    st.session_state["logged_in"] = True
                    st.session_state["usuario"] = email.strip()
                    st.session_state["cliente"] = row["cliente"]
                    st.session_state["empresas_permitidas"] = list(row["empresas"])
                    st.session_state[SESSION_ACTIVE_ORG_KEY] = None
                    st.rerun()
                else:
                    st.error("E-mail ou senha incorretos. Verifique suas credenciais e tente novamente.")
        st.checkbox(
            "Mantenha-se conectado",
            key="fdl_login_manter_conectado",
            help="Preferência local neste navegador (sem alterar a segurança do servidor).",
        )
        st.caption("Problemas para entrar? Procure o administrador do sistema.")
        st.stop()

    try:
        return get_app_context()
    except ValueError as e:
        st.error(str(e))
        logout_operacional_user()
        if st.button("Tentar novamente"):
            st.rerun()
        st.stop()


def logout_operacional_user() -> None:
    st.session_state["logged_in"] = False
    for _k in ("usuario", "cliente", "empresas_permitidas"):
        st.session_state.pop(_k, None)
    st.session_state[SESSION_ACTIVE_ORG_KEY] = None


def get_app_context() -> AppUserContext:
    if not st.session_state.get("logged_in"):
        raise ValueError("Sessão expirada ou não autenticada.")

    empresas_nomes: list[str] = list(st.session_state.get("empresas_permitidas") or [])
    orgs_t = organizacoes_na_ordem_permitida(empresas_nomes)

    if not orgs_t:
        raise ValueError(
            "Seu usuário não tem empresas válidas no cadastro. "
            "Os nomes em «empresas» devem existir no registro de metadados do sistema."
        )

    allowed_ids = {o.org_id for o in orgs_t}

    active = st.session_state.get(SESSION_ACTIVE_ORG_KEY)
    if active not in allowed_ids:
        active = orgs_t[0].org_id
        st.session_state[SESSION_ACTIVE_ORG_KEY] = active

    return AppUserContext(
        user_id=str(st.session_state.get("usuario", "")),
        display_name=str(st.session_state.get("cliente", "")),
        organizations=orgs_t,
        active_org_id=active,
        active_route_id="conciliacao-repasse",
    )


def get_active_organization(ctx: AppUserContext) -> AppOrganization:
    for o in ctx.organizations:
        if o.org_id == ctx.active_org_id:
            return o
    return ctx.organizations[0]
