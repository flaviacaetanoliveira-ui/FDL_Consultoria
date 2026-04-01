# Pipeline de faturamento (Fase 1)

## Política de versão (params / dataset)

| | |
|---|---|
| **Padrão oficial** | `schema_version` **2** — multi-empresa, custo partilhado, saída única em `data_products/<cliente_slug>/faturamento/current/`. |
| **Legado temporário** | `schema_version` **1** ou JSON **sem** `schema_version` — tratado como V1. **Deprecado:** mantido só enquanto existirem consumidores (app, agendamentos, clientes) em transição; **sem novas evoluções**. |
| **Remoção da V1** | Só após a V2 ser consumível no app e a chave custo↔pedido estar minimamente estável — ver [Checklist — remoção futura da V1](#checklist--remoção-futura-da-v1). |

## Parâmetros (`faturamento_params.json`)

### schema_version 1 (**deprecado** / legado — uma pasta de pedidos)

> **Não usar para novos clientes.** Preferir V2. Este modo permanece para não quebrar materializações e secrets que ainda apontam para `data_products/<cliente>/<empresa>/faturamento/...`.

- **Pedidos:** CSV mais recente em `pedidos_dir` (glob `*.csv` por data de modificação).
- **Custo:** Excel, aba `Planilha1`, colunas `Código` e `PREÇO DE CUSTO com IPI`.
- **Colunas obrigatórias no CSV:** incluem `Quantidade`, `Preço de lista`, `Valor total`, frete, comissão, situação, pedidos, SKU, plataforma. O schema de validação ainda exige colunas de NF (`Existe Nota Fiscal gerada`, `Número da nota`); se o export não as trouxer, o normalizador cria-as **vazias** — ver [Pedidos ML e nota fiscal](#pedidos-ml-e-nota-fiscal).
- **Base de imposto (v1):** usa a coluna `Valor total` (comportamento legado alinhado ao cálculo anterior).
- Exemplo: [faturamento_params.example.v1.json](faturamento_params.example.v1.json).

### schema_version 2 (multi-empresa — base única, **padrão oficial**)

- **cliente_root:** pasta raiz do cliente (ex. `Cliente_4`).
- **cliente_slug:** segmento explícito em `data_products` e secrets (ex. `cliente_5`) — **não** inferir só pelo nome da pasta física.
- **custo_xlsx:** ficheiro único partilhado; path relativo a `cliente_root` ou absoluto.
- **empresas[]:** cada entrada com `org_id`, `empresa`, `pedidos_dir` (relativo a `cliente_root`); `permite_faturamento_sem_nf` opcional por empresa (override do default global).
- **coluna_base_imposto:** string ou lista ordenada de candidatos (usa a primeira coluna existente no CSV).
- O pipeline: lê custo uma vez, pedidos por empresa, join por **chave SKU normalizada** (texto, trim, sufixo `.0` tipo Excel, remoção de zeros à esquerda em códigos só numéricos). O valor original do pedido mantém-se na coluna **`Código`**; a chave usada no join aparece como **`SKU_Normalizado`**. Em seguida: auditar custo (SKU sem correspondência / duplicado na tabela), concatenar, aplicar cálculos v2 (`Receita_Bruta`, `Custo_Produto_Total`, etc.), flags NF.

Exemplo: [faturamento_params.example.json](faturamento_params.example.json).

### Pedidos ML e nota fiscal

O export **original de pedidos** do Mercado Livre (lista que costuma incluir `Data`, `Situação`, `Código (SKU)`, `Número do pedido`, `Valor total`, etc.) **não traz, em geral, uma coluna explícita de número de nota fiscal**. A coluna **`Número`** nesse ficheiro **não** é tratada como NF: na fonte não está identificada como tal e o pipeline **não a mapeia** para `Número da nota`.

- **Comportamento esperado:** `Número da nota` (e `Existe Nota Fiscal gerada`, se ausentes) podem existir no dataset materializado **vazios** — não é falha da app.
- **Dado confiável de NF:** outro export ou fluxo (ex. **notas de saída**, **repasse** / tabela operacional após `integracao_notas_pedidos`), com cruzamento explícito, se o produto precisar de número de NF na linha de pedido.
- **Coalesce no código:** só preenche `Número da nota` a partir de colunas **claramente nomeadas** como nota fiscal / NF no mesmo CSV (ex. `Número da nota fiscal`); não inventa valores.

## Variáveis de ambiente (alternativa aos paths no JSON v1)

- `FDL_PEDIDOS_DIR` — pasta de pedidos (se omitido no JSON v1).
- `FDL_TABELA_CUSTO_PATH` — ficheiro `.xlsx` de custo (se omitido no JSON v1).
- `FDL_FATURAMENTO_PARAMS` — caminho absoluto do JSON (para CLI e agendamento).

## Comando de materialização

Na raiz do repositório:

```text
python processing\materialize_financeiro.py --modulo faturamento --faturamento-params "C:\caminho\faturamento_params.json"
```

Somente faturamento **não** exige `--base-dir`. Para `--modulo all` ou repasse/frete, defina `FDL_BASE_DIR` / `--base-dir`.

**Nota:** com `schema_version >= 2`, a saída do faturamento usa só `cliente_slug` do JSON (ignore `--cliente` / `--empresa` da CLI para esse módulo).

Alterou alíquotas ou paths no JSON → **volte a executar** a materialização.

## Saída

- **v1:** `data_products/<cliente>/<empresa>/faturamento/current/` (via `--cliente` / `--empresa` na CLI).
- **v2:** `data_products/<cliente_slug>/faturamento/current/` (único dataset; colunas `empresa` / `org_id` por linha).

Em ambos os casos:

- `dataset.parquet`
- `dataset_faturamento_app.csv`
- `metadata.json`

Ver [operacao_materializacao.md](operacao_materializacao.md) para lock, logs e Task Scheduler.

## App Streamlit — consumo do materializado V2

Definir nos **secrets** do Streamlit (ou variáveis de ambiente equivalentes), alinhado ao `cliente_slug` do `faturamento_params.json`:

| Secret / env | Exemplo (cliente Flávio / `cliente_5`) | Notas |
|----------------|----------------------------------------|--------|
| `FDL_MATERIALIZED_CLIENTE_SLUG` | `cliente_5` | Mesmo segmento que na pasta `data_products/<slug>/faturamento/...`. |
| `FDL_DATA_PRODUCTS_ROOT` | `data_products` ou caminho absoluto à pasta `data_products` | Relativo à raiz do repo onde corre o app. |
| `FDL_FATURAMENTO_DATA_LAYOUT` | `v2` | Em produção: **`v2`** para dataset multi-empresa (filtro por `org_id`). Use `v1` só para layout legado; `auto` só como fallback. |
| `FDL_FATURAMENTO_MATERIALIZED_PATH` | *(opcional)* | Path absoluto ou relativo ao repo para `dataset_faturamento_app.csv` / `dataset.parquet`. Se vazio e `FDL_MATERIALIZED_PATH_MODE` **fixed**: tenta path canónico V2 com slug; em **dynamic**, idem quando não há path explícito. |
| `FDL_MATERIALIZED_PATH_MODE` | `fixed` ou `dynamic` | Com **dynamic**, paths explícitos de faturamento são ignorados pelo código legado; o V2 canónico continua a ser tentado se `DATA_LAYOUT` ≠ `v1`. |

Com **admin**, o app mostra caption com layout, origem da resolução, path e um expander com `faturamento_info` (incl. `faturamento_row_count_loaded` vs linhas após filtro).

### Convenções do produto (Faturamento & DRE na app)

Decisões de regra fechadas para o módulo na interface (sem redesign; textos de ajuda refletem isto):

| Tema | Política |
|------|-----------|
| **Período** | Eixo oficial do filtro: coluna **`Data`** (pedido / export ML). **`Data do faturamento`** no materializado é secundária / futura (competência fiscal), até a fonte ser uniforme. |
| **«Valor Nota Fiscal»** | **Convenção de rótulo** na UI: os valores mostrados são a soma / linha da coluna **`Valor total`** do materializado. **Não** equivalem ao valor fiscal legal da NF-e enquanto não existir integração com documentos fiscais. |
| **Frete** | Regra técnica no pipeline: separação ME vs transportadora própria quando o CSV traz coluna de modalidade; frete de transportadora própria pode integrar **`Receita_Bruta`** — sujeito a validação amostral com financeiro. |
| **NF** | No fluxo só com pedidos ML, **`Número da nota`** (e similares) costuma vazio: **comportamento esperado**, não erro de tela. Número confiável: outra fonte ou join futuro. |

## Checklist — remoção futura da V1

Usar esta lista antes de apagar ramos V1 no código, exemplos e documentação de params legados.

1. **App (`app_operacional`)** lê e filtra corretamente o materializado V2 em `data_products/<cliente_slug>/faturamento/current/` (dataset único com `empresa` / `org_id` por linha), incluindo org ativa e secrets (`FDL_FATURAMENTO_MATERIALIZED_PATH` ou derivação a partir do repasse).
2. **Nenhum cliente em produção** depende de JSON sem `schema_version` ou com `schema_version: 1` para faturamento (migrar params e rotinas de materialização para V2).
3. **Agendamentos / CLI** que materializam faturamento usam sempre JSON V2 onde o multi-empresa for necessário; chamadas `--modulo faturamento` com V1 (`--cliente` / `--empresa` + layout por org) estão extintas ou documentadas como exceção técnica.
4. **Chave custo ↔ pedido** validada em volume real: taxa aceitável de `SKU_SEM_CORRESPONDENCIA` (ou política explícita no produto para linhas sem custo).
5. **Testes** cobrem apenas V2 para build/materialização de produto, ou V1 reduzido a testes mínimos de regressão até o delete.
6. **Documentação e exemplos** (`faturamento_params.example.v1.json`, secções “v1” neste ficheiro) removidos ou arquivados após confirmar (1)–(5).

Até lá, **não remover** `FaturamentoParams`, `_build_faturamento_dataset_v1`, nem o ramo em `materialize_financeiro.py` que grava em `.../<cliente>/<empresa>/faturamento/current/`.
