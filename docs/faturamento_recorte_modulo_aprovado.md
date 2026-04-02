# Recorte do módulo Faturamento & DRE — regras aprovadas (produto)

Documento de **regra de produto aprovada**. Evoluções do módulo (filtros, Visão Geral, tabela principal) devem respeitar este alinhamento, salvo revisão explícita.

## Vista mínima NF-first (Etapa 1) — eixo temporal único

No **painel mínimo** (uma linha por NF, mesma base para KPIs e tabela):

- O **único** filtro de tempo é o **período de emissão da NF** (`Nota_Data_Emissao`).
- **Empresa** e **Plataforma** (opcional) refinam o universo.
- **Não** há filtro por **Data** de venda no painel: as linhas de pedido ligadas às NFs do recorte entram no enriquecimento (venda, comissão, frete, custo, resultado) **sem** interseção com período de venda.
- Implementação: `build_nf_grain_dataframe` em `faturamento_dre_recorte_minimo.py`.

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
