# Frete em produção: consumo materializado

## Comportamento

- **`FDL_FRETE_CONSUME_MODE=materialized`** (em `.streamlit/secrets.toml` ou variáveis de ambiente): o app lê primeiro o ficheiro espelho `dataset_frete_app.csv` gerado pelo pipeline.
- **Caminho padrão alinhado ao Repasse** (segmentos `default` / `antomoveis` de `processing/materialize_financeiro.py`):

  `data_products/default/antomoveis/frete/current/dataset_frete_app.csv`

- **Fallback para live**: se o CSV não existir, estiver corrompido ou falhar a validação de schema, o app recua para o fluxo **live** (export ML + planilha em `FDL_BASE_DIR`), com indicação em modo admin quando aplicável.

## Geração do CSV (cadência)

- O agendador passa a correr **`agendamento/run_rotina_dados.bat`**, que inclui:
  1. `robocopy` de vendas / frete (se `FDL_SYNC_*` definidos);
  2. **`python processing/materialize_financeiro.py --modulo all --base-dir <FDL_BASE_DIR>`** (salvo `SKIP_MATERIALIZE=1`);
  3. `powerbi_mirror/export_powerbi_dataset.py` (salvo `SKIP_POWERBI_EXPORT=1`).

- Registo de tarefas (08:00 e 14:30): `agendamento/register_tarefas_8h_14h30.ps1`.

- **Importante:** `FDL_BASE_DIR` em `config_local.bat` deve ser a **mesma** base onde existem `Vendas - Mercado Livre` (e demais pastas do pipeline), coerente com `secrets.toml`.

## Validação manual

1. **Gerar** `dataset_frete_app.csv` (ou esperar a rotina):
   ```powershell
   cd C:\caminho\para\V2
   $env:FDL_BASE_DIR = "C:\caminho\para\cliente_1"
   python processing/materialize_financeiro.py --modulo frete
   ```
2. Confirmar ficheiro: `data_products/default/antomoveis/frete/current/dataset_frete_app.csv` e `metadata.json`.
3. Subir o app com `FDL_FRETE_CONSUME_MODE=materialized` e `FDL_FRETE_MATERIALIZED_PATH` apontando para esse CSV (relativo à raiz do projeto).
4. No painel **Frete**, na sidebar/info de carregamento, deve aparecer consumo **materialized** (não live). Se o materializado falhar, verifica-se fallback para live.

## Streamlit Cloud

- Incluir nos **Secrets** `FDL_FRETE_CONSUME_MODE` e `FDL_FRETE_MATERIALIZED_PATH` ou `FDL_FRETE_MATERIALIZED_URL` (URL direta ao CSV gerado no PC e publicado no SharePoint/OneDrive), conforme `.streamlit/secrets.toml.example`.
