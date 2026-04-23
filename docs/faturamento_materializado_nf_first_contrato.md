# Contrato — tabela materializada NF-first (Faturamento & DRE)

Documento de **contrato técnico de produto** para o artefacto em grão **nota fiscal (NF)**.  
Implementação no pipeline e consumo na app devem seguir este texto salvo revisão explícita.

**Premissas de negócio (fixas):**

- Grão **NF**: **1 linha por NF** (chave lógica `org_id` + `Nota_Numero_Normalizado` quando `org_id` existir; senão só `Nota_Numero_Normalizado` no âmbito do cliente).
- **Uma plataforma por NF**; vários pedidos na mesma NF são sempre da mesma plataforma.
- Tudo que é **pedido** → **agregado** na linha da NF. Tudo que é **fiscal da nota** → **uma vez por NF** (sem multiplicar por linhas de pedido).

---

## 1. Nome do artefato e localização

| Item | Valor proposto |
|------|----------------|
| **Nome do ficheiro (canónico)** | `dataset_faturamento_nf.parquet` |
| **Formato preferido** | **Parquet** (leitura e tamanho); CSV opcional só para inspeção humana / compatibilidade. |
| **Diretório** | `data_products/<cliente_slug>/faturamento/current/` |
| **Caminho completo (exemplo)** | `data_products/cliente_5/faturamento/current/dataset_faturamento_nf.parquet` |

**Convivência com o grão linha:** o materializado em grão pedido/linha mantém-se como hoje (`dataset_faturamento_app.csv` / `dataset.parquet` na mesma pasta). O NF-first é **derivado** desse dataset (ou da mesma execução de pipeline), não substitui o ficheiro linha.

**Metadados de build (recomendado, fora da tabela ou em colunas técnicas opcionais):**

- Identificador de revisão do pipeline / `schema_version` do contrato NF-first.
- Timestamp UTC de geração.
- Referência ao artefacto linha usado (path ou *hash* do conteúdo), para auditoria e *cache* na app.

---

## 2. Colunas da tabela (por grupo)

### 2.1 Identificação

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `org_id` | string | Organização (vazio permitido se o modelo legado não tiver; chave composta com NF quando preenchido). |
| `empresa` | string | Etiqueta de marca / empresa para filtro e exibição. |
| `Nota_Numero_Normalizado` | string | Identificador normalizado da NF (chave com `org_id`). |

### 2.2 Campos fiscais (nota, uma vez por NF)

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `Nota_Data_Emissao` | datetime (data civil) | Data de emissão usada no filtro temporal do painel. |
| `Nota_Situacao` | string | Situação da NF (ex.: Autorizada); linhas com situação inválida para painel — *cancelada / denegada / inutilizada* — **não entram** no materializado NF-first. |
| `valor_faturado_nf` | float | Valor líquido da nota **uma vez por NF** (ver §4). |

### 2.3 Campo comercial único por NF

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `plataforma` | string | **Única** plataforma da NF (premissa de negócio); mesmo valor em todas as linhas de pedido agregadas. |

### 2.4 Campos comerciais agregados (derivados de linhas de pedido)

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `valor_venda` | float | Σ (`Quantidade` × `Preço de lista`) em todas as linhas ligadas à NF. |
| `n_linhas_pedido` | int | Número de linhas de item/pedido incluídas na agregação. |
| `pedido_resumo` | string | Resumo textual dos pedidos (um id; ou “N pedidos” / lista compacta). |
| `produto_resumo` | string | Resumo textual de produtos/SKUs (um item; ou primeiro (+N itens)). |

### 2.5 Campos económicos (agregados + regras fechadas)

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `comissao` | float | Soma das comissões nas linhas de pedido (`Taxa de Comissão` ou coluna canónica equivalente no grão linha). |
| `frete` | float | Soma do frete nas linhas (`Frete_Plataforma` ou, se ausente, `Custo de Frete` — mesma prioridade que o painel atual). |
| `imposto` | float | Soma de `Imposto` nas linhas de pedido. |
| `despesa_fixa` | float | **5%** de `valor_venda` (alíquota fixa do contrato do painel NF-first). |
| `diferenca` | float | `valor_venda - valor_faturado_nf`. |
| `resultado` | float | Lucro agregado na NF pela **regra já fechada** no materializado (ver §4 e §3). |

### 2.6 Campos de apoio / qualidade (opcionais, recomendados)

| Coluna | Tipo lógico | Descrição |
|--------|-------------|-----------|
| `faturamento_nota_vinculada` | bool | `true` se a NF entrou por vínculo explícito no grão linha (alinhado ao filtro atual). |
| `schema_version_nf` | int | Versão deste contrato (ex.: `1`). |

---

## 3. Regra de agregação por coluna

Legenda: **origem** = coluna(s) no materializado **linha**; **regra** = como obter o valor na linha NF.

| Coluna NF-first | Origem (grão linha) | Agregação / regra | Duplicidade |
|-----------------|---------------------|-------------------|-------------|
| `org_id` | `org_id` | **Valor único** por grupo NF (todos iguais; se conflito, falha de qualidade de dados a tratar no pipeline). | Um valor por linha NF. |
| `empresa` | `empresa` / etiquetas | **Primeiro** não vazio após normalização, ou regra única do pipeline de etiquetas. | Um valor por NF. |
| `Nota_Numero_Normalizado` | `Nota_Numero_Normalizado` | Chave do grupo. | — |
| `Nota_Data_Emissao` | `Nota_Data_Emissao` | **Mínimo** ou **primeiro** não nulo coerente (emissão única por NF); timezone/parsing alinhado ao grão linha. | Uma data por NF. |
| `Nota_Situacao` | `Nota_Situacao` | **Primeiro** não nulo; grupo só inclui NFs **não** canceladas/denegadas/inutilizadas (filtro **antes** da agregação). | Um valor por NF. |
| `valor_faturado_nf` | `Nota_Valor_Liquido_Total` | **Um valor por NF:** tomar o primeiro numérico não nulo do grupo (todas as linhas repetem o mesmo total de nota). **Não** somar nem multiplicar pelo nº de linhas. | Garantido pela regra “first / any consistent”. |
| `plataforma` | `Nome da plataforma` | **Valor único** por NF (premissa); validar unicidade no pipeline; se falhar, registo em *log* / quarentena conforme política. | Escalar. |
| `valor_venda` | `Quantidade`, `Preço de lista` | **Soma** de `Quantidade × Preço de lista` por linha, depois **soma** no grupo NF. | Só soma de linhas. |
| `n_linhas_pedido` | — | **Contagem** de linhas no grupo. | — |
| `pedido_resumo` | `Número do pedido multiloja`, `Número do pedido` | **Resumo textual** (igual semântica ao painel atual: um código; ou “N multiloja” / “N pedidos”). | N/A |
| `produto_resumo` | `Descrição` ou `Nome` | **Resumo textual** (primeiro item; ou “primeiro (+N itens)”). | N/A |
| `comissao` | `Taxa de Comissão` | **Soma** no grupo. | Só soma. |
| `receita_frete_tp` | `Custo de Frete` + modalidade de envio | **Soma** no grupo da parcela **transportadora própria** (mesma regra que `calc._frete_mercado_envios_vs_transportadora`); gap NF×lista (uma linha) quando aplicável. | Só soma. |
| `tarifa_custo_envio` | `Custo de Frete` (ou `Frete_Plataforma` se CF ausente) | **Soma** no grupo = tarifa de envio total do relatório de pedidos na NF. | Só soma. |
| `imposto` | `Imposto` | **Soma** no grupo. | Só soma. |
| `despesa_fixa` | — | **Derivado:** `0.05 × valor_venda` (após calcular `valor_venda`). | Uma vez por NF. |
| `diferenca` | — | **Derivado:** `valor_venda - valor_faturado_nf`. | Uma vez por NF. |
| `resultado` | `Resultado`, opc. `Despesas Fixas` | **Regra fechada (igual ao painel atual):** se existir `Despesas Fixas` no grão linha: `sum(Resultado) + sum(Despesas Fixas) - despesa_fixa`; senão: `sum(Resultado)`. | `despesa_fixa` já reflete 5% sobre `valor_venda` agregado; não duplicar desconto. |

**Nota:** NF sem nenhuma linha de pedido no materializado linha **não** deve aparecer no NF-first (o painel atual também exige join a pedido).

---

## 4. Regras explícitas (obrigatórias no contrato)

1. **`valor_faturado_nf`:** entra **uma vez por NF**, a partir de `Nota_Valor_Liquido_Total` (não somar entre linhas do mesmo grupo).
2. **`valor_venda`:** **soma** de **Quantidade × Preço de lista** em **todas** as linhas de pedido ligadas à NF.
3. **`despesa_fixa`:** **5%** de **`valor_venda`** (alíquota **0,05** fixa neste contrato, salvo futura versão documentada do `schema_version_nf`).
4. **`diferenca`:** **`valor_venda - valor_faturado_nf`**.
5. **`resultado`:** **sem fórmula nova** em relação ao que está fechado hoje no painel NF-first: usar a mesma recomposição com **`Despesas Fixas`** quando a coluna existir no grão linha; caso contrário, soma de **`Resultado`**. O materializado NF-first deve gravar o **valor final já coerente** com esta regra, para a app **não** recalcular lucro.

**Filtro de inclusão:** apenas NFs com emissão válida, situação fiscal válida para o painel, com número normalizado preenchido (e critérios de `faturamento_nota_vinculada` alinhados ao pipeline linha atual).

---

## 5. Consumo pela app (Streamlit)

### 5.1 Filtros disponíveis

- **Período de emissão:** sobre `Nota_Data_Emissao`.
- **Empresa:** sobre `empresa` (multiselect; vazio = todas).
- **Plataforma:** sobre `plataforma` (multiselect; vazio = todas) — **filtro direto** em coluna escalar, alinhado à premissa “uma plataforma por NF”.

### 5.2 O que deixa de ser calculado na app

- Merge linha ↔ conjunto de NFs no período.
- `groupby` por NF e agregações comerciais/fiscais descritas no §3.
- Cálculo de `despesa_fixa`, `diferenca` e `resultado` (passam a ser **lidosp** do ficheiro, eventualmente só *slice* + soma para KPIs).

### 5.3 O que a app ainda faz

- **Carregar** Parquet (e `@st.cache_data` por path + revisão / *mtime* / assinatura do manifest).
- **Aplicar** filtros booleanos (datas, empresa, plataforma).
- **KPIs:** somas e contagens sobre o subconjunto filtrado (`n_nf`, totais de métricas).
- **Tabela e CSV:** exibir/exportar colunas (formatação, rótulos PT).
- **Validação mínima:** presença de colunas obrigatórias; mensagens se o ficheiro NF-first estiver ausente ou desatualizado face ao linha.

### 5.4 Impacto em performance

- Custo por rerun deixa de ser proporcional ao **nº de linhas de pedido** (merge + groupby) e passa a ser proporcional ao **nº de NFs** + leitura de um ficheiro menor e mais “denso”.
- Alinha ao padrão **“ler tabela final”** dos outros módulos: um artefacto principal por vista, filtros em memória sobre esse grão.

---

## 6. Chave de unicidade e evolução

- **Unicidade:** (`org_id`, `Nota_Numero_Normalizado`) quando `org_id` for usado; documentar comportamento quando `org_id` for vazio (único por *slug* / ficheiro).
- **Evolução:** alterações ao contrato incrementam `schema_version_nf` e devem ser descritas em *changelog* do repositório ou neste documento.

---

*Última atualização: contrato inicial para implementação futura (sem código obrigatório nesta fase).*
