# Ação sugerida: “Baixar no Bling”

## O que é (negócio)

**Não existe integração automática com o Bling** neste projeto. O texto **“Baixar no Bling”** é uma **ação sugerida para o operador financeiro**: indica que o valor **já foi recebido/pago na operação** (conforme os dados de conciliação), mas a **situação do título nas contas a receber** (arquivo em `contas_receber`) **não aparece como quitada/baixada**.

Em termos práticos: a pessoa deve **ir ao Bling** (ou ao processo que vocês usam) e **dar baixa / quitar o título**, para o sistema espelhar o que já aconteceu no dinheiro.

## Como o sistema decide (regra no código)

A classificação está em `etapa4b_integracao_contas_receber.py`, função `_classificar_acao`.

Ordem de avaliação (resumo):

1. **Sem pagamento** (`Valor pago` vazio ou ≤ 0) → **“Verificar recebimento”**.
2. **Valor a receber = 0** e **Valor pago > 0** → **“Revisar venda zerada”**.
3. **Diferença** entre valor a receber e valor pago **maior que R$ 0,01** → **“Analisar diferença”**.
4. Se o campo **Situação** (contas a receber) contém algo como *pago*, *baixado*, *liquidado*, *quitado* → **“Ok”**.
5. **Caso contrário** (houve pagamento, valores batem na tolerância, mas a situação não indica quitado) → **“Baixar no Bling”**.

A coluna **Situação** vem do **join** entre a tabela de conciliação e os arquivos da pasta `contas_receber` (detecção automática de colunas de situação e número da nota).

## Arquivos envolvidos

- Regra da ação: `etapa4b_integracao_contas_receber.py`
- Base de contas a receber: pasta `contas_receber` (CSV/Excel) dentro da base do cliente
- Export para Power BI: `powerbi_mirror/export_powerbi_dataset.py` → CSV com a coluna **Ação sugerida**

## Como “ter” só as linhas “Baixar no Bling”

- **Power BI:** filtre o relatório pela coluna **Ação sugerida** = `Baixar no Bling` e use **Exportar dados** na tabela.
- **Planilha:** após gerar `conciliacao_operacional.csv`, filtre a mesma coluna no Excel.

## Observação

Se no Bling o título já estiver baixado mas o arquivo de `contas_receber` estiver desatualizado ou com outro texto em **Situação**, o sistema pode sugerir “Baixar no Bling” incorretamente até a **próxima exportação** com dados corretos.

## Referência oficial Bling (planilha de contas)

Artigo da central de ajuda sobre **cadastro de novas Contas a Receber/Pagar via planilha** (modelo, colunas obrigatórias, importação CSV e valores aceitos em **Situação**: `aberto`, `pago`, `parcial`, `devolvido`, `cancelada` — sempre em minúsculas no modelo deles):

[https://ajuda.bling.com.br/hc/pt-br/articles/4410469923095-Cadastrar-novas-Contas-a-Receber-Pagar-via-planilha](https://ajuda.bling.com.br/hc/pt-br/articles/4410469923095-Cadastrar-novas-Contas-a-Receber-Pagar-via-planilha)

**Atenção (próprio artigo Bling):** por planilha é possível **cadastrar** contas; **não é possível atualizar** contas existentes só com esse fluxo de importação. Para **dar baixa** em título já cadastrado, o fluxo usual é dentro do Bling (Financeiro → Contas a Receber), ou manter o **export/backup** de contas a receber alinhado à situação real para o seu pipeline de conciliação ler a coluna **Situação** corretamente.
