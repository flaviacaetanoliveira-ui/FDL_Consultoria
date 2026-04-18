"""
Contexto de usuário / organização / rotas para o app operacional.

`get_app_context()` usa `st.session_state` após login (`operacional_usuarios`).
"""
from __future__ import annotations

from dataclasses import dataclass

import streamlit as st

from operacional_usuarios import USUARIOS, autenticar, normalizar_email, normalizar_perfil_acesso


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
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Gama Home": AppOrganization(
        org_id="gama_home",
        display_name="Gama Home",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Mega Fácil": AppOrganization(
        org_id="mega_facil",
        display_name="Mega Fácil",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Mega Star": AppOrganization(
        org_id="mega_star",
        display_name="Mega Star",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Móveis EAP": AppOrganization(
        org_id="moveis_eap",
        display_name="Móveis EAP",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Empresa 3": AppOrganization(
        org_id="empresa_3",
        display_name="Empresa 3",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Empresa 4": AppOrganization(
        org_id="empresa_4",
        display_name="Empresa 4",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Esquilo": AppOrganization(
        org_id="esquilo",
        display_name="Esquilo",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Wood": AppOrganization(
        org_id="wood",
        display_name="Wood",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "BP Ramiro": AppOrganization(
        org_id="bp_ramiro",
        display_name="BP Ramiro",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "FMG": AppOrganization(
        org_id="fmg",
        display_name="FMG",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "Let Decor": AppOrganization(
        org_id="let_decor",
        display_name="Let Decor",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
                page_module="app_operacional",
            ),
        ),
    ),
    "TB Paio": AppOrganization(
        org_id="tb_paio",
        display_name="TB Paio",
        module_ids=("financeiro",),
        routes=(
            ModuleRoute(
                route_id="conciliacao-repasse",
                label="Conciliação de Repasse",
                page_module="app_operacional",
            ),
            ModuleRoute(
                route_id="faturamento",
                label="Faturamento",
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

# Login — .login-wrapper / .login-card (via CSS); card max 420px; só com body:has(.fdl-login-brand).
_LOGIN_PAGE_STYLES = """
<style>
  /*
   * Centro na viewport em [data-testid="stMain"] (Streamlit ≥1.55: sem .main entre stMain e .block-container).
   * Largura: [data-testid="stMainBlockContainer"] — em layout wide o tema usa max-width: initial.
   */
  html body:has(.fdl-login-brand),
  body:has(.fdl-login-brand) {
    min-height: 100vh !important;
    background: linear-gradient(135deg, #f0f4ff 0%, #f8fafc 60%, #f0fdf4 100%) !important;
  }
  body:has(.fdl-login-brand) .stApp {
    background: linear-gradient(135deg, #f0f4ff 0%, #f8fafc 60%, #f0fdf4 100%) !important;
    min-height: 100vh !important;
  }
  body:has(.fdl-login-brand) [data-testid="stSidebar"],
  body:has(.fdl-login-brand) [data-testid="collapsedControl"] {
    display: none !important;
  }
  body:has(.fdl-login-brand) #MainMenu { visibility: hidden; }
  body:has(.fdl-login-brand) footer { visibility: hidden; }

  body:has(.fdl-login-brand) [data-testid="stMain"],
  body:has(.fdl-login-brand) section.main {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    min-height: 100vh !important;
    display: flex !important;
    flex-direction: column !important;
    flex: 1 1 auto !important;
    align-items: center !important;
    justify-content: center !important;
    padding: 1rem !important;
    box-sizing: border-box !important;
  }
  body:has(.fdl-login-brand) [data-testid="stAppViewContainer"] {
    width: 100% !important;
    max-width: 100% !important;
    min-height: 100vh !important;
    display: flex !important;
    flex-direction: column !important;
    box-sizing: border-box !important;
  }
  /*
   * CRÍTICO: limitar o bloco principal a 420px (wide mode remove o cap do tema via max-width: initial).
   */
  body:has(.fdl-login-brand) [data-testid="stMainBlockContainer"],
  body:has(.fdl-login-brand) [data-testid="stMain"] .block-container {
    width: 100% !important;
    max-width: min(420px, calc(100vw - 2rem)) !important;
    min-width: 0 !important;
    margin-left: auto !important;
    margin-right: auto !important;
    padding: 0 !important;
    flex: 0 0 auto !important;
    display: flex !important;
    flex-direction: column !important;
    align-items: stretch !important;
    box-sizing: border-box !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
  }
  /* .login-card — card branco; ocupa a largura do block-container (já ≤420px) */
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] {
    background: #ffffff !important;
    border-radius: 16px !important;
    border: 1px solid rgba(255, 255, 255, 0.8) !important;
    box-shadow:
      0 4px 6px rgba(0, 0, 0, 0.04),
      0 12px 32px rgba(0, 0, 0, 0.08) !important;
    padding: 2.5rem 2.25rem !important;
    margin-left: auto !important;
    margin-right: auto !important;
    width: 100% !important;
    max-width: 100% !important;
    min-width: 0 !important;
    flex: 0 0 auto !important;
    align-self: stretch !important;
    box-sizing: border-box !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stVerticalBlockBorderWrapper"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stElementContainer"] {
    width: 100% !important;
    max-width: 100% !important;
  }

  .fdl-login-brand {
    text-align: center;
    margin: 0;
    padding: 0;
  }
  .fdl-login-logo-wrap {
    display: flex;
    flex-direction: column;
    justify-content: center;
    align-items: center;
    margin: 0 auto 1.75rem auto;
    width: 100%;
    max-width: 100%;
  }
  .fdl-login-wordmark-line {
    margin: 0;
    font-family: ui-sans-serif, system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial,
      sans-serif;
    line-height: 1.2;
    text-align: center;
  }
  .fdl-login-wordmark-fdl {
    font-size: 1.35rem;
    font-weight: 800;
    letter-spacing: -0.03125rem;
    color: #1e40af;
  }
  .fdl-login-wordmark-analytics {
    font-size: 1.35rem;
    font-weight: 400;
    letter-spacing: -0.02em;
    color: #475569;
  }
  .fdl-login-tagline {
    font-size: 0.7rem;
    font-weight: 500;
    color: #94a3b8;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    margin: 2px 0 0 0;
    line-height: 1.35;
    text-align: center;
  }
  .fdl-login-title {
    font-family: system-ui, -apple-system, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
    font-size: 1.65rem;
    font-weight: 700;
    letter-spacing: -0.025em;
    color: #0f172a;
    margin: 0 0 6px 0;
    line-height: 1.2;
    text-align: center;
    max-width: 100%;
    white-space: nowrap;
  }
  @media (max-width: 360px) {
    .fdl-login-title {
      white-space: normal;
      text-wrap: balance;
    }
  }
  .fdl-login-sub {
    font-size: 0.9rem;
    font-weight: 400;
    color: #64748b !important;
    margin: 0 0 1.75rem 0;
    line-height: 1.45;
    text-align: center;
  }
  .fdl-login-footer {
    text-align: center;
    margin-top: 24px;
    margin-bottom: 0;
    font-size: 0.72rem;
    color: #94a3b8;
    line-height: 1.4;
  }

  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] .stMarkdown {
    margin-bottom: 0.1rem !important;
  }

  /* Remove painel interno “caixa dentro da caixa” do formulário (tema Streamlit) */
  body:has(.fdl-login-brand) form[data-testid="stForm"] {
    background: transparent !important;
    border: none !important;
    outline: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    margin: 0 !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="stVerticalBlockBorderWrapper"],
  body:has(.fdl-login-brand) form[data-testid="stForm"] div[data-testid="stVerticalBlockBorderWrapper"] {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
    margin: 0 !important;
    border-radius: 0 !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] fieldset {
    border: none !important;
    padding: 0 !important;
    margin: 0 !important;
    min-width: 0 !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="element-container"] {
    width: 100% !important;
    margin-bottom: 0.5rem !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="element-container"]:last-of-type {
    margin-bottom: 0 !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] label p {
    font-size: 0.82rem !important;
    font-weight: 600 !important;
    color: #374151 !important;
    margin-bottom: 6px !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-baseweb="input"] > div {
    border-radius: 10px !important;
    border: 1.5px solid #e2e8f0 !important;
    min-height: 44px !important;
    background: #f8fafc !important;
    transition: border-color 0.2s ease, box-shadow 0.2s ease, background 0.2s ease !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-baseweb="input"] input {
    padding: 12px 14px !important;
    font-size: 0.95rem !important;
    color: #1e293b !important;
    width: 100% !important;
    max-width: 100% !important;
    box-sizing: border-box !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-baseweb="input"] input[type="password"] {
    padding-right: 2.85rem !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-baseweb="input"]:focus-within > div {
    background: #ffffff !important;
    border-color: #3b82f6 !important;
    box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1) !important;
  }

  body:has(.fdl-login-brand) form[data-testid="stForm"] .stFormSubmitButton,
  body:has(.fdl-login-brand) form[data-testid="stForm"] div.row-widget.stButton {
    width: 100% !important;
  }
  /* Azul marca — sobrepõe tema e estilos inline do Base Web */
  body:has(.fdl-login-brand) form[data-testid="stForm"] button[kind="primary"],
  body:has(.fdl-login-brand) form[data-testid="stForm"] button,
  body:has(.fdl-login-brand) form[data-testid="stForm"] .stButton > button,
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="baseButton-primary"],
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="baseButton-secondary"] {
    width: 100% !important;
    margin-top: 0.6rem !important;
    border-radius: 10px !important;
    padding: 14px !important;
    font-weight: 600 !important;
    font-size: 0.95rem !important;
    letter-spacing: 0.02em !important;
    background: linear-gradient(135deg, #1d4ed8 0%, #2563eb 100%) !important;
    background-color: #2563eb !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    border: none !important;
    border-color: transparent !important;
    cursor: pointer !important;
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3) !important;
    transition: transform 0.2s ease, box-shadow 0.2s ease, background 0.2s ease, filter 0.2s ease !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] button:hover,
  body:has(.fdl-login-brand) form[data-testid="stForm"] .stButton > button:hover,
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="baseButton-primary"]:hover,
  body:has(.fdl-login-brand) form[data-testid="stForm"] [data-testid="baseButton-secondary"]:hover {
    transform: translateY(-1px) !important;
    box-shadow: 0 6px 16px rgba(37, 99, 235, 0.4) !important;
    filter: brightness(1.03) !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] button:active,
  body:has(.fdl-login-brand) form[data-testid="stForm"] .stButton > button:active {
    transform: translateY(0) !important;
    filter: brightness(0.98) !important;
    box-shadow: 0 4px 12px rgba(37, 99, 235, 0.28) !important;
  }
  body:has(.fdl-login-brand) form[data-testid="stForm"] button:focus-visible,
  body:has(.fdl-login-brand) form[data-testid="stForm"] .stButton > button:focus-visible {
    outline: 2px solid #93c5fd !important;
    outline-offset: 2px !important;
  }

  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stCheckbox"] {
    margin-top: 0 !important;
    margin-bottom: 0 !important;
    width: 100% !important;
    padding: 0.15rem 0 0 0 !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stCheckbox"] label {
    align-items: center !important;
    gap: 0.35rem !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] div[data-testid="stCheckbox"] label p {
    font-size: 0.72rem !important;
    color: #9ca3af !important;
    font-weight: 400 !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stVerticalBlockBorderWrapper"] [data-testid="stCaption"] {
    color: #94a3b8 !important;
    font-size: 0.6875rem !important;
    text-align: center !important;
    margin-top: 0.35rem !important;
    line-height: 1.4 !important;
    opacity: 1 !important;
  }
  body:has(.fdl-login-brand) div[data-testid="stAlert"] {
    border-radius: 10px !important;
    margin-top: 0.6rem !important;
  }

</style>
"""


def _login_brand_logo_html() -> str:
    """Wordmark tipográfico FDL Analytics (sem PNG; evita ícone embutido em raster)."""
    return (
        '<div class="fdl-login-logo-wrap">'
        '<p class="fdl-login-wordmark-line" role="img" aria-label="FDL Analytics">'
        '<span class="fdl-login-wordmark-fdl">FDL</span>'
        '<span class="fdl-login-wordmark-analytics"> Analytics</span>'
        "</p>"
        '<div class="fdl-login-tagline">Da operação ao insight</div>'
        "</div>"
    )


def require_app_user() -> AppUserContext:
    """
    Garante usuário autenticado: exibe login ou devolve o contexto.
    Encerra a execução da página com st.stop() se não houver sessão.
    """
    if not st.session_state.get("logged_in"):
        st.markdown(_LOGIN_PAGE_STYLES.strip(), unsafe_allow_html=True)
        with st.container(border=True):
            st.markdown(
                f"""
                <div class="fdl-login-brand">
                  {_login_brand_logo_html()}
                  <h1 class="fdl-login-title">Acesse sua conta</h1>
                  <p class="fdl-login-sub">Entre para acessar o sistema</p>
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
                submitted = st.form_submit_button(
                    "Entrar",
                    type="primary",
                    use_container_width=True,
                )
                if submitted:
                    if autenticar(email, senha):
                        email_key = normalizar_email(email)
                        row = USUARIOS[email_key]
                        st.session_state["logged_in"] = True
                        st.session_state["usuario"] = email.strip()
                        st.session_state["cliente"] = row["cliente"]
                        st.session_state["empresas_permitidas"] = list(row["empresas"])
                        st.session_state["fdl_perfil_acesso"] = normalizar_perfil_acesso(
                            row.get("perfil_acesso")
                        )
                        st.session_state[SESSION_ACTIVE_ORG_KEY] = None
                        st.rerun()
                    else:
                        st.error(
                            "E-mail ou senha incorretos. Verifique suas credenciais e tente novamente."
                        )
            st.checkbox(
                "Mantenha-se conectado",
                key="fdl_login_manter_conectado",
                help="Preferência local neste navegador (sem alterar a segurança do servidor).",
            )
            st.caption("Problemas para entrar? Procure o administrador do sistema.")
            st.markdown(
                '<p class="fdl-login-footer">FDL Analytics · FDL Consultoria · v1.0</p>',
                unsafe_allow_html=True,
            )
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
    for _k in ("usuario", "cliente", "empresas_permitidas", "fdl_perfil_acesso"):
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
