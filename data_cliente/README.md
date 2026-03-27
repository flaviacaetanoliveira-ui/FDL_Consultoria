# Dados do cliente (pasta base — *fallback*)

Quando `FDL_BASE_DIR` não está definido **e não existe** a pasta `cliente_1/` na raiz do projeto, o app usa **`data_cliente/`** como raiz.

Se existir **`cliente_1/`** no repositório (ou junção para a pasta real do cliente), essa pasta tem **prioridade** — é o local recomendado para a planilha original e a mesma árvore do OneDrive.

Estrutura esperada pelo pipeline (mesmos nomes de pastas):

- `Vendas - Mercado Livre/` — arquivos de vendas
- `Liberações_ML/` — liberações
- `notas_saida/` — notas
- `contas_receber/` — contas a receber

Para **Streamlit Community Cloud**, coloque uma cópia mínima dos CSVs no repositório (ou use `FDL_BASE_DIR` apontando para um volume, se no futuro houver suporte), ou defina o segredo `FDL_BASE_DIR` se a plataforma permitir caminho montado.

Veja `DEPLOY_STREAMLIT_CLOUD.md` na raiz do projeto.
