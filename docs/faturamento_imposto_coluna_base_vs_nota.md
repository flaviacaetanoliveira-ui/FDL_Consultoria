# Imposto no faturamento: `coluna_base_imposto` vs. nota rateada

## Contexto

No `faturamento_params.json` (schema_version 2), o campo **`coluna_base_imposto`** permanece **obrigatório** por **compatibilidade de schema** e para o fluxo **legado** (build V1 e metadados).

## Fluxo novo (V2 com notas de saída + rateio)

A partir da revisão **`faturamento-v3`**:

- A **base de imposto por linha** é **`Nota_Valor_Liquido_Rateado`** (proporcional ao `Vl_Venda` dentro da mesma NF), materializada também como **`Base_Imposto`**.
- O **imposto** é **`Base_Imposto × Aliquota_Imposto_Utilizada`**, com a alíquota resolvida por **empresa + competência = mês da data de emissão da nota** (planilha `params_mensais`, ou fallback do JSON se não houver planilha).
- **Sem nota vinculada:** alíquota de imposto efetiva **0** na linha → **imposto 0** (a base rateada também é 0).

Neste fluxo, o **imposto já não depende** do valor da coluna apontada por `coluna_base_imposto` (ex.: `Valor total` do pedido ou `Base fiscal item`). Esse candidato continua a ser registado em metadados (`coluna_base_imposto_resolvida`) apenas para **auditoria e compatibilidade**, não para o cálculo fiscal novo.

## Resumo

| Aspecto | Papel de `coluna_base_imposto` no V2 novo |
|--------|--------------------------------------------|
| Schema JSON | Continua exigido; evita quebrar configs antigas. |
| Cálculo do imposto (nota rateada) | **Não usado.** |
| Metadados / diagnóstico | Pode ser preenchido para comparações legadas. |

Para validação numérica com dados reais, use o script `scripts/validar_faturamento_notas_rateio.py` apontando para o seu `faturamento_params.json` e pastas reais de pedidos e `notas_saida`.

## Chave de controlo no consolidado (multi-empresa)

Em bases que juntam mais do que uma empresa, o número da nota **sozinho** não identifica de forma única o documento fiscal (podem existir coincidências formais ou ruído entre orgs).

**Regra de controlo:** usar sempre o par **`org_id` + número da nota** (normalizado) para:

- conferir fecho Σ valor líquido rateado = total da nota;
- cruzar com export de saídas;
- auditoria e exemplos linha a linha no consolidado.

O pipeline de rateio corre **por empresa** antes do `concat`; no dataset materializado, agrupações e QA devem repetir essa chave composta.
