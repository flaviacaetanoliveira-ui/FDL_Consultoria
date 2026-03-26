"""Entrypoint para Streamlit Cloud."""

# Importa e executa a aplicação principal.
import app_operacional  # noqa: F401

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional


st.set_page_config(page_title="Conciliação Operacional", layout="wide")
st.title("Conciliação Operacional")


@st.cache_data(show_spinner=True)
def carregar_tabela_final_operacional_cache() -> tuple[pd.DataFrame, dict[str, object], str]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tabela, info = carregar_tabela_final_operacional(BASE_DIR)
    return tabela, info, ts


def _style_acao(v: str) -> str:
    cores = {
        "Ok": "background-color: #d1fae5; color: #065f46;",
        "Analisar manualmente": "background-color: #fde68a; color: #7c2d12;",
        "Verificar título no Bling": "background-color: #fecaca; color: #7f1d1d;",
        "Verificar faturamento": "background-color: #dbeafe; color: #1e3a8a;",
        "Baixar no Bling": "background-color: #fde68a; color: #854d0e;",
    }
    return cores.get(str(v), "")


tabela_base, info, ts_proc = carregar_tabela_final_operacional_cache()
tabela = tabela_base.copy()

with st.sidebar:
    st.subheader("Filtros")
    acoes = sorted([x for x in tabela["Ação sugerida"].dropna().unique().tolist() if str(x).strip()])
    sit = sorted([x for x in tabela["Situação"].dropna().unique().tolist() if str(x).strip()])
    sel_acao = st.multiselect("Ação sugerida", acoes, default=acoes)
    sel_sit = st.multiselect("Situação", sit, default=sit)
    busca = st.text_input("Busca (venda / pedido / nota)").strip().lower()

if sel_acao:
    tabela = tabela[tabela["Ação sugerida"].isin(sel_acao)]
if sel_sit:
    tabela = tabela[tabela["Situação"].isin(sel_sit)]

if busca:
    m = (
        tabela["N° de venda"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        | tabela["ID do pedido"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        | tabela["Número da nota"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
    )
    tabela = tabela[m]

total = len(tabela)
ok = int((tabela["Ação sugerida"] == "Ok").sum())
manual = int((tabela["Ação sugerida"] == "Analisar manualmente").sum())
titulo = int((tabela["Ação sugerida"] == "Verificar título no Bling").sum())
fatur = int((tabela["Ação sugerida"] == "Verificar faturamento").sum())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total de casos", f"{total}")
c2.metric("Ok", f"{ok}")
c3.metric("Analisar manualmente", f"{manual}")
c4.metric("Verificar título no Bling", f"{titulo}")
c5.metric("Verificar faturamento", f"{fatur}")

st.caption(
    f"Base: `{info.get('base_dir','')}` | Linhas carregadas: {info.get('linhas',0)} | Processado em: {ts_proc}"
)

exibir_cols = [
    "N° de venda",
    "ID do pedido",
    "Número da nota",
    "Valor da nota",
    "Situação",
    "Ação sugerida",
]
if "Valor pago" in tabela.columns:
    exibir_cols.append("Valor pago")
if "Data de pagamento" in tabela.columns:
    exibir_cols.append("Data de pagamento")

tabela_exibir = tabela[exibir_cols].copy()
tabela_exibir["Valor da nota"] = pd.to_numeric(tabela_exibir["Valor da nota"], errors="coerce")
if "Valor pago" in tabela_exibir.columns:
    tabela_exibir["Valor pago"] = pd.to_numeric(tabela_exibir["Valor pago"], errors="coerce")

fmt = {"Valor da nota": "R$ {:,.2f}"}
if "Valor pago" in tabela_exibir.columns:
    fmt["Valor pago"] = "R$ {:,.2f}"

sty = tabela_exibir.style.format(fmt).applymap(_style_acao, subset=["Ação sugerida"])
st.dataframe(sty, use_container_width=True, height=520)

st.write(f"Linhas filtradas: **{len(tabela_exibir)}**")

csv_bytes = tabela_exibir.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "Exportar CSV (filtrado)",
    data=csv_bytes,
    file_name="conciliacao_operacional_filtrada.csv",
    mime="text/csv",
)

from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional


st.set_page_config(page_title="Conciliação Operacional", layout="wide")
st.title("Conciliação Operacional")


@st.cache_data(show_spinner=True)
def carregar_tabela_final_operacional_cache() -> tuple[pd.DataFrame, dict[str, object], str]:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    tabela, info = carregar_tabela_final_operacional(BASE_DIR)
    return tabela, info, ts


def _style_acao(v: str) -> str:
    cores = {
        "Ok": "background-color: #d1fae5; color: #065f46;",
        "Analisar manualmente": "background-color: #fde68a; color: #7c2d12;",
        "Verificar título no Bling": "background-color: #fecaca; color: #7f1d1d;",
        "Verificar faturamento": "background-color: #dbeafe; color: #1e3a8a;",
        "Baixar no Bling": "background-color: #fde68a; color: #854d0e;",
    }
    return cores.get(str(v), "")


tabela_base, info, ts_proc = carregar_tabela_final_operacional_cache()
tabela = tabela_base.copy()

with st.sidebar:
    st.subheader("Filtros")
    acoes = sorted([x for x in tabela["Ação sugerida"].dropna().unique().tolist() if str(x).strip()])
    sit = sorted([x for x in tabela["Situação"].dropna().unique().tolist() if str(x).strip()])
    sel_acao = st.multiselect("Ação sugerida", acoes, default=acoes)
    sel_sit = st.multiselect("Situação", sit, default=sit)
    busca = st.text_input("Busca (venda / pedido / nota)").strip().lower()

if sel_acao:
    tabela = tabela[tabela["Ação sugerida"].isin(sel_acao)]
if sel_sit:
    tabela = tabela[tabela["Situação"].isin(sel_sit)]

if busca:
    m = (
        tabela["N° de venda"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        | tabela["ID do pedido"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
        | tabela["Número da nota"].fillna("").astype(str).str.lower().str.contains(busca, regex=False)
    )
    tabela = tabela[m]

total = len(tabela)
ok = int((tabela["Ação sugerida"] == "Ok").sum())
manual = int((tabela["Ação sugerida"] == "Analisar manualmente").sum())
titulo = int((tabela["Ação sugerida"] == "Verificar título no Bling").sum())
fatur = int((tabela["Ação sugerida"] == "Verificar faturamento").sum())

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total de casos", f"{total}")
c2.metric("Ok", f"{ok}")
c3.metric("Analisar manualmente", f"{manual}")
c4.metric("Verificar título no Bling", f"{titulo}")
c5.metric("Verificar faturamento", f"{fatur}")

st.caption(
    f"Base: `{info.get('base_dir','')}` | Linhas carregadas: {info.get('linhas',0)} | Processado em: {ts_proc}"
)

exibir_cols = [
    "N° de venda",
    "ID do pedido",
    "Número da nota",
    "Valor da nota",
    "Situação",
    "Ação sugerida",
]
if "Valor pago" in tabela.columns:
    exibir_cols.append("Valor pago")
if "Data de pagamento" in tabela.columns:
    exibir_cols.append("Data de pagamento")

tabela_exibir = tabela[exibir_cols].copy()
tabela_exibir["Valor da nota"] = pd.to_numeric(tabela_exibir["Valor da nota"], errors="coerce")
if "Valor pago" in tabela_exibir.columns:
    tabela_exibir["Valor pago"] = pd.to_numeric(tabela_exibir["Valor pago"], errors="coerce")

fmt = {"Valor da nota": "R$ {:,.2f}"}
if "Valor pago" in tabela_exibir.columns:
    fmt["Valor pago"] = "R$ {:,.2f}"

sty = tabela_exibir.style.format(fmt).applymap(_style_acao, subset=["Ação sugerida"])
st.dataframe(sty, use_container_width=True, height=520)

st.write(f"Linhas filtradas: **{len(tabela_exibir)}**")

csv_bytes = tabela_exibir.to_csv(index=False).encode("utf-8-sig")
st.download_button(
    "Exportar CSV (filtrado)",
    data=csv_bytes,
    file_name="conciliacao_operacional_filtrada.csv",
    mime="text/csv",
)

from __future__ import annotations

from pathlib import Path

import pandas as pd
import streamlit as st

from src.tabela_final import processar_cliente


st.set_page_config(page_title="Conciliacao ML", layout="wide")
st.title("Conciliacao ML - Tabela Final")
st.caption("Processa automaticamente a pasta base do cliente.")

default_base = r"C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Anto Moveis\cliente_1"
base_dir = st.text_input("Pasta base do cliente", value=default_base)
salvar_csv = st.checkbox("Salvar CSVs tambem", value=True)

if st.button("Processar cliente", type="primary"):
    if not base_dir.strip():
        st.error("Informe a pasta base do cliente.")
    else:
        with st.spinner("Processando arquivos..."):
            try:
                resultado = processar_cliente(base_dir, salvar_csv=salvar_csv)
            except Exception as exc:
                st.exception(exc)
            else:
                st.success("Processamento concluido.")

                st.subheader("Arquivos usados")
                st.write(f"Vendas: `{resultado['arquivo_vendas']}`")
                st.write(f"Liberacoes: `{resultado['arquivo_liberacoes']}`")
                st.write(f"Notas: `{resultado['arquivo_notas']}`")

                st.subheader("Arquivos gerados")
                st.write(f"Tabela Excel: `{resultado['tabela_final_xlsx']}`")
                st.write(f"DE/PARA Excel: `{resultado['de_para_xlsx']}`")
                if "tabela_final_csv" in resultado:
                    st.write(f"Tabela CSV: `{resultado['tabela_final_csv']}`")
                if "de_para_csv" in resultado:
                    st.write(f"DE/PARA CSV: `{resultado['de_para_csv']}`")

                tabela_xlsx = Path(resultado["tabela_final_xlsx"])
                depara_xlsx = Path(resultado["de_para_xlsx"])
                st.info(f"Pasta de saida: `{tabela_xlsx.parent}`")

                st.subheader("Tabela final")
                tabela_final = pd.read_excel(tabela_xlsx)
                st.dataframe(tabela_final, use_container_width=True, hide_index=True)

                qtd_linhas = len(tabela_final)
                total_brl = pd.to_numeric(tabela_final.get("Total BRL"), errors="coerce").sum()
                valor_pago = pd.to_numeric(tabela_final.get("Valor pago"), errors="coerce").sum()
                metricas = resultado.get("metricas_conciliacao", {})

                c1, c2, c3 = st.columns(3)
                c1.metric("Linhas", f"{qtd_linhas:,}".replace(",", "."))
                c2.metric("Soma Total BRL", f"{total_brl:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
                c3.metric("Soma Valor pago", f"{valor_pago:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

                st.subheader("Qualidade da conciliacao")
                q1, q2, q3, q4, q5, q6 = st.columns(6)
                q1.metric("Vendas total", str(int(metricas.get("vendas_total", 0))))
                q2.metric("Match EXTERNAL_REFERENCE", str(int(metricas.get("vendas_match_external_reference", 0))))
                q3.metric("Match ORDER_ID (fallback)", str(int(metricas.get("vendas_match_order_id", 0))))
                q4.metric("Com match liberacao", str(int(metricas.get("vendas_com_match_liberacao", 0))))
                q5.metric("Sem match", str(int(metricas.get("vendas_sem_match_liberacao", 0))))
                q6.metric("Percentual conciliacao", f"{metricas.get('percentual_conciliacao', 0.0):.2f}%")

                # Dica util para abrir a pasta no Windows.
                st.code(f'explorer "{tabela_xlsx.parent}"', language="powershell")

