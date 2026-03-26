# Dados do cliente (pasta base)

Quando `FDL_BASE_DIR` não está definido (variável de ambiente ou `st.secrets`), o app usa esta pasta como raiz.

Estrutura esperada pelo pipeline (mesmos nomes de pastas):

- `Vendas - Mercado Livre/` — arquivos de vendas
- `Liberações_ML/` — liberações
- `notas_saida/` — notas
- `contas_receber/` — contas a receber

Para **Streamlit Community Cloud**, coloque uma cópia mínima dos CSVs no repositório (ou use `FDL_BASE_DIR` apontando para um volume, se no futuro houver suporte), ou defina o segredo `FDL_BASE_DIR` se a plataforma permitir caminho montado.

Veja `DEPLOY_STREAMLIT_CLOUD.md` na raiz do projeto.
