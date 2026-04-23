# Recorte do módulo Faturamento & DRE — regras aprovadas (produto)

Documento de **regra de produto aprovada**. Evoluções do módulo (filtros, Visão Geral, tabela principal) devem respeitar este alinhamento, salvo revisão explícita.

## Vista mínima NF-first (Etapa 1) — eixo temporal único

No **painel mínimo** (uma linha por NF, mesma base para KPIs e tabela):

- O **único** filtro de tempo é o **período de emissão da NF** (`Nota_Data_Emissao`).
- **Empresa** e **Plataforma** (opcional) refinam o universo.
- **Não** há filtro por **Data** de venda no painel: as linhas de pedido ligadas às NFs do recorte entram no enriquecimento (venda, comissão, frete, custo, resultado) **sem** interseção com período de venda.
- **Despesa fixa (painel):** **5%** sobre o **valor da venda** agregado à NF — Σ (`Quantidade` × `Preço de lista`) nas linhas de pedido ligadas à nota. Exposta em KPI e tabela; o **Resultado** no painel recompõe o total quando o materializado traz `Despesas Fixas` por linha (ver docstring de `build_nf_grain_dataframe`).
- Implementação: `build_nf_grain_dataframe` em `faturamento_dre_recorte_minimo.py`.

### Premissa para modelagem da tabela final em grão NF (materializado)

**Regra de negócio:** não há cenário em que a **mesma NF** tenha pedidos em **plataformas diferentes**. Pode haver **vários pedidos** (e várias linhas de item) ligados à mesma NF, mas todos na **mesma plataforma**.

**Implicações para a planilha/tabela final única (1 linha por NF):**

- **Plataforma** é um **campo único e confiável por NF** (valor escalar por linha), não um resumo “multi-plataforma” por nota.
- Com esta premissa, um **filtro por plataforma** no consumo do materializado NF-first reduz-se a um **filtro direto nessa coluna** (sem agregação cruzada nem artefacto NF × plataforma só por causa de plataformas mistas na mesma nota).

**O que continua a precisar de tratamento (vários pedidos na mesma NF):**

| Tópico | Tratamento alinhado ao modelo atual |
|--------|-------------------------------------|
| **Representação de vários pedidos** | Campo **resumo** (ex.: um identificador quando há um só pedido; texto do tipo “N pedidos” ou lista compacta quando há vários), à parte das chaves `org_id` + `Nota_Numero_Normalizado`. |
| **Agregação dos campos do pedido** | Métricas **comerciais por linha de pedido**: **somar** sobre todas as linhas ligadas à NF (`Quantidade` × `Preço de lista`, comissão, frete, imposto, `Resultado`, `Despesas Fixas` para recomposição, etc.). |
| **Evitar duplicidade nos valores fiscais** | Campos **da nota** (`Nota_Valor_Liquido_Total`, datas/situação de emissão): usar **um valor por NF** (ex.: primeiro não nulo / dedupe por chave NF), **não** multiplicar pela contagem de linhas de pedido. |

As secções seguintes descrevem o recorte em **grão pedido** e combinações com dois eixos temporais, úteis para **`apply_recorte_minimo`**, testes e evoluções do módulo **fora** deste painel mínimo.

## Organização em dois blocos lógicos

### 1. Recorte comercial

- **Empresa**
- **Data da venda** (eixo da `Data` do pedido)
- **Plataforma**
- Opcionalmente **com nota / sem nota** (refinamento)

### 2. Recorte fiscal

- **Data de emissão da nota fiscal**
- **Situação da NF** (refinamento)

## Hierarquia de filtros

| Prioridade | Filtros |
|------------|---------|
| **Principais** | Empresa, Data da venda, Data de emissão da NF, Plataforma |
| **Secundários / refinamento** | Situação da NF, com nota / sem nota |

## Regras de combinação (datas)

- **Data da venda** e **Data de emissão da NF** são **independentes**: nenhum substitui o outro.
- Quando **ambos** os eixos existem **no mesmo recorte em grão pedido**, o recorte é a **interseção** (a linha deve cumprir os dois). **Isto não se aplica** ao painel mínimo NF-first (Etapa 1), que usa **só** emissão da NF — ver secção no topo.
- Se o filtro de **Data de emissão da NF** estiver **ativo** (intervalo definido), **linhas sem nota não entram** no recorte.
- Se o filtro de **Data de emissão da NF** estiver **vazio / inativo**, linhas **com e sem** nota podem entrar, sujeitas aos demais filtros.

## Impacto no produto

- **Visão Geral** e **tabela principal** devem usar **exatamente** o mesmo recorte derivado destas regras.
- **DRE tradicional** (camada futura) não herda automaticamente esta lógica de tempo; define a sua própria competência quando implementada, mantendo convivência conceitual com a separação **venda vs emissão NF** neste módulo.

## Relação com outros documentos

- Tabela principal (colunas e leitura fiscal vs lucro): ver especificação de produto acordada para a grelha.
- Imposto / nota rateada / `coluna_base_imposto`: `docs/faturamento_imposto_coluna_base_vs_nota.md`.
