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

# Estilos só na rota de login — referência visual tipo portal institucional (ex.: FGV).
_LOGIN_PAGE_STYLES = """
<style>
  .stApp {
    background: #ffffff !important;
  }
  [data-testid="stSidebar"],
  [data-testid="collapsedControl"],
  [data-testid="stHeader"],
  [data-testid="stToolbar"],
  [data-testid="stDecoration"] {
    display: none !important;
  }
  #MainMenu { visibility: hidden; }
  footer { visibility: hidden; }
  /* Não forçar flex no stMain — em Streamlit 1.4x+ pode deixar o ecrã em branco. */
  /* Largura estilo FGV (~350px), centralizado em telas grandes */
  .main .block-container {
    max-width: min(352px, calc(100vw - 2.5rem)) !important;
    width: min(352px, calc(100vw - 2.5rem)) !important;
    padding: 1rem 0.75rem 1.5rem 0.75rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
    flex: 0 0 auto !important;
    box-sizing: border-box !important;
  }
  /* Card branco fino (tipo portal acadêmico/corporativo) */
  div[data-testid="stVerticalBlockBorderWrapper"] {
    background: #ffffff !important;
    border-radius: 4px !important;
    box-shadow: 0 2px 8px rgba(15, 23, 42, 0.06) !important;
    border: 1px solid #e5e7eb !important;
    padding: 1.35rem 1.25rem 1.2rem 1.25rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
    max-width: 100% !important;
    width: 100% !important;
    box-sizing: border-box !important;
  }
  .fdl-login-brand { text-align: center; }
  .fdl-login-topline {
    height: 3px;
    background: #005baa;
    margin: -1.35rem -1.25rem 1.2rem -1.25rem;
    border-radius: 3px 3px 0 0;
  }
  .fdl-login-divider {
    height: 1px;
    background: #e5e7eb;
    margin: 0.85rem 0 1rem 0;
    border: 0;
  }
  div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stElementContainer"] {
    width: 100% !important;
    max-width: 100% !important;
  }
  /* Prateleira do formulário (cinza claro, como no FGV) */
  form[data-testid="stForm"] {
    background: #f3f4f6 !important;
    border: 1px solid #e5e7eb !important;
    border-radius: 6px !important;
    padding: 1rem 1rem 1.1rem 1rem !important;
    margin-top: 0.25rem !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
  }
  form[data-testid="stForm"] label p {
    font-size: 0.8125rem !important;
    font-weight: 600 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
    color: #374151 !important;
  }
  form[data-testid="stForm"] [data-baseweb="input"] > div {
    border-radius: 4px !important;
    border-color: #d1d5db !important;
    background: #ffffff !important;
  }
  form[data-testid="stForm"] [data-baseweb="input"]:focus-within > div {
    border-color: #005baa !important;
    box-shadow: 0 0 0 1px #005baa !important;
  }
  form[data-testid="stForm"] .stFormSubmitButton,
  form[data-testid="stForm"] div.row-widget.stButton {
    width: 100% !important;
  }
  form[data-testid="stForm"] button,
  form[data-testid="stForm"] .stButton > button {
    width: 100% !important;
    margin-top: 1rem !important;
    border-radius: 4px !important;
    padding: 0.72rem 1rem !important;
    font-weight: 700 !important;
    font-size: 0.875rem !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    background: #0086e5 !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: none !important;
    box-shadow: none !important;
    transition: background 0.15s ease !important;
  }
  form[data-testid="stForm"] button:hover,
  form[data-testid="stForm"] .stButton > button:hover {
    background: #0070c2 !important;
    filter: none !important;
    transform: none !important;
  }
  form[data-testid="stForm"] button:focus-visible,
  form[data-testid="stForm"] .stButton > button:focus-visible {
    outline: 2px solid #005baa !important;
    outline-offset: 2px !important;
  }
  /* Checkbox “manter conectado” */
  div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stCheckbox"] label p {
    font-size: 0.8125rem !important;
    color: #1f2937 !important;
  }
  div[data-testid="stAlert"] {
    border-radius: 6px !important;
  }
</style>
"""


def require_app_user() -> AppUserContext:
    """
    Garante usuário autenticado: exibe login ou devolve o contexto.
    Encerra a execução da página com st.stop() se não houver sessão.
    """
    if not st.session_state.get("logged_in"):
        st.title("FDL Analytics")
        st.caption("Acesso ao painel financeiro — se o ecrã abaixo estiver vazio, atualize a página ou use outro navegador.")
        st.markdown(_LOGIN_PAGE_STYLES, unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(
                """
                <div class="fdl-login-brand">
                  <div class="fdl-login-topline" aria-hidden="true"></div>
                  <h1 style="
                    font-family: Georgia, 'Times New Roman', serif;
                    font-size:1.6rem;
                    font-weight:700;
                    letter-spacing:-0.02em;
                    color:#003978;
                    margin:0 0 0.4rem 0;
                    line-height:1.2;
                  ">FDL Analytics</h1>
                  <p style="
                    font-size:0.8125rem;
                    color:#6b7280;
                    margin:0 0 0.5rem 0;
                    line-height:1.5;
                  ">Financial Intelligence for E-commerce</p>
                  <hr class="fdl-login-divider" />
                  <p style="
                    font-size:1rem;
                    font-weight:600;
                    color:#003978;
                    margin:0 0 0.15rem 0;
                    line-height:1.3;
                  ">Autenticação</p>
                  <p style="
                    font-size:0.75rem;
                    color:#6b7280;
                    margin:0;
                    line-height:1.45;
                  ">Acesso ao painel operacional</p>
                </div>
                """,
                unsafe_allow_html=True,
            )
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
                submitted = st.form_submit_button("ENTRAR")
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
