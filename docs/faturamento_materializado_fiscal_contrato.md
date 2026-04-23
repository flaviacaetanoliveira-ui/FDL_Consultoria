# Contrato — `dataset_faturamento_fiscal.parquet`

Artefato **fiscal** (espelho do export de notas de saída / Bling), **independente** do grão linha de pedidos.

- **Grão:** 1 linha por nota de saída, no âmbito `(org_id, empresa)`.
- **Fonte:** mesmos ficheiros CSV/XLSX que `load_notas_saida_from_dir` + `filtrar_notas_canceladas`, com filtro por empresa/org alinhado a `enrich_pedidos_com_notas`.
- **Não substitui** `dataset_faturamento_nf.parquet` (NF-first comercial); convive na mesma pasta `faturamento/current/`.

## Colunas (schema_version_fiscal = 1)

| Coluna | Tipo | Obrigatória | Descrição |
|--------|------|-------------|-----------|
| `org_id` | string | Sim | Identificador da org (pode ser vazio no legado). |
| `empresa` | string | Sim | Etiqueta de marca (filtro UI). |
| `Nota_Numero_Normalizado` | string | Sim | Chave canónica da NF (`normalize_pedido_join_key` sobre o «Número» do export). |
| `Nota_Data_Emissao` | datetime64 | Sim | Data/hora de emissão (mínimo no grupo de linhas da mesma NF). |
| `Nota_Situacao` | string | Sim | Situação agregada por NF (prioriza cancelada/denegada/inutilizada se existir no grupo). |
| `Valor_Liquido_NF` | float | Sim | Soma do valor líquido por NF (mesma coluna detetada que `Nota_Valor_Liquido_Total` no join). |
| `Valor_Total_NF` | float | Não | Valor total «bruto» se a coluna existir no export; senão `NaN`. |
| `schema_version_fiscal` | int | Sim | Versão deste contrato. |

## Agregação (1 linha por NF)

- Várias linhas no CSV do Bling com o mesmo número de NF → **um** registo: `Valor_Liquido_NF = sum(vl_liq)`, `Nota_Data_Emissao = min(dt_emissao)`.
- Chave única lógica: `(org_id, empresa, Nota_Numero_Normalizado)`.

## Multi-empresa (params V2)

- Uma passagem por entrada em `empresas[]`, com `notas_saida_dir` resolvido como no `build` V2; resultados concatenados.

## Params V1 (legado)

- Sem pasta de notas no build V1 → ficheiro **vazio** (0 linhas) com colunas do contrato, ou não gerado — ver implementação do materialize.
