# Recorte do módulo Faturamento & DRE — regras aprovadas (produto)

Documento de **regra de produto aprovada**. Evoluções do módulo (filtros, Visão Geral, tabela principal) devem respeitar este alinhamento, salvo revisão explícita.

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
- Se **ambos** tiverem intervalo definido, o recorte é a **interseção** (a linha deve cumprir os dois).
- Se o filtro de **Data de emissão da NF** estiver **ativo** (intervalo definido), **linhas sem nota não entram** no recorte.
- Se o filtro de **Data de emissão da NF** estiver **vazio / inativo**, linhas **com e sem** nota podem entrar, sujeitas aos demais filtros.

## Impacto no produto

- **Visão Geral** e **tabela principal** devem usar **exatamente** o mesmo recorte derivado destas regras.
- **DRE tradicional** (camada futura) não herda automaticamente esta lógica de tempo; define a sua própria competência quando implementada, mantendo convivência conceitual com a separação **venda vs emissão NF** neste módulo.

## Relação com outros documentos

- Tabela principal (colunas e leitura fiscal vs lucro): ver especificação de produto acordada para a grelha.
- Imposto / nota rateada / `coluna_base_imposto`: `docs/faturamento_imposto_coluna_base_vs_nota.md`.
