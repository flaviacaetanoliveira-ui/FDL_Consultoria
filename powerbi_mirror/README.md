# Power BI Mirror (paralelo ao Streamlit)

Esta pasta cria uma trilha paralela para Power BI **sem alterar** a conciliação existente.

## Objetivo

- Reusar a mesma saída de `carregar_tabela_final_operacional`.
- Gerar arquivos para Power BI (`CSV` e `JSON` de métricas).
- Comparar snapshots para validar que os resultados continuam batendo.

## Como usar

1. Exportar dataset (terminal):

```bash
python powerbi_mirror/export_powerbi_dataset.py
Ou com um clique (Windows):

```bat
powerbi_mirror\run_export_powerbi.bat
```

> Se os dados não estiverem em `data_cliente`, defina `FDL_BASE_DIR` antes de executar.

```

2. (Opcional) Comparar com snapshot anterior:

```bash
python powerbi_mirror/compare_snapshot.py
```

## Saídas

- `powerbi_mirror/output/conciliacao_operacional.csv`
- `powerbi_mirror/output/metrics.json`
- `powerbi_mirror/output/schema.txt`

## Observação

Esta camada é somente de **entrada/saída para BI**.
As regras de conciliação e cálculos permanecem no pipeline atual.
