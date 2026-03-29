# Pipeline de faturamento (Fase 1)

## Fontes

- **Pedidos:** CSV mais recente em `pedidos_dir` (glob `*.csv` por data de modificação).
- **Custo:** Excel, aba `Planilha1`, colunas `Código` e `PREÇO DE CUSTO com IPI`.
- **Parâmetros:** JSON (`faturamento_params.json`) com alíquotas em **decimal com ponto** (ex.: `0.12`).

## Variáveis de ambiente (alternativa aos paths no JSON)

- `FDL_PEDIDOS_DIR` — pasta de pedidos (se omitido no JSON).
- `FDL_TABELA_CUSTO_PATH` — ficheiro `.xlsx` de custo (se omitido no JSON).
- `FDL_FATURAMENTO_PARAMS` — caminho absoluto do JSON (para CLI e agendamento).

## Comando de materialização

Na raiz do repositório:

```text
python processing\materialize_financeiro.py --modulo faturamento --faturamento-params "C:\caminho\faturamento_params.json"
```

Somente faturamento **não** exige `--base-dir`. Para `--modulo all` ou repasse/frete, defina `FDL_BASE_DIR` / `--base-dir`.

Alterou alíquotas no JSON → **volte a executar** a materialização.

## Saída

`data_products/<cliente>/<empresa>/faturamento/current/`

- `dataset.parquet`
- `dataset_faturamento_app.csv`
- `metadata.json`

Ver [operacao_materializacao.md](operacao_materializacao.md) para lock, logs e Task Scheduler.
