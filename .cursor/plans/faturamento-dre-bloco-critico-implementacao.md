# Faturamento & DRE — plano de implementação (bloco crítico)

**Decisão fixa:** Opção A — cards, DRE e tabela usam **o mesmo recorte** (empresa, período emissão NF, plataforma, produto, sinal resultado, etc.). Não manter universo fiscal global silencioso nos cards/DRE.

**Escopo:** alinhamento plataforma, NFs 10031/10030/10027, frete vs tarifa, DRE, auditoria Preço lista × Qtd.

---

## 1. Resumo executivo do plano de implementação

O trabalho divide-se em **cinco PRs pequenos**: (1) unificar a fonte de verdade dos KPIs/DRE com a tabela filtrada; (2) corrigir **visibilidade e busca** de NFs (incl. fiscal-only, resultado 0/NaN, zeros à esquerda); (3) **materializar** duas colunas numéricas (receita frete TP vs custo/tarifa `Custo de Frete`); (4) **consumir** essas colunas em KPIs e DRE; (5) **auditar** apenas as visões onde `Preço de lista` ainda for tratado como total da linha ou comparado incorretamente a `Valor total` sem × Quantidade. O grão NF (`_nf_grain_venda_linha_series`) **permanece** como está (já Σ Quantidade × Preço de lista). Critério de homologação obrigatório: as três NFs Gama Home e totais **coerentes** com o multiselect de plataforma.

---

## 2. Etapas propostas (PRs)

| PR | Nome | Dependência |
|----|------|-------------|
| **PR1** | Um só recorte para cards + DRE + tabela (Opção A) | — |
| **PR2** | Visibilidade, filtro de sinal/neutro, busca NF normalizada, estado fiscal-only | PR1 |
| **PR3** | Pipeline: `receita_frete_tp` + `tarifa_custo_envio` no grão/painel | PR1 (pode paralelizar com PR2 em equipas diferentes; merge depois) |
| **PR4** | KPIs + DRE consumindo as novas colunas | PR3 |
| **PR5** | Auditoria Preço lista × Qtd (UI/alertas/export) | PR1 (parcialmente independente) |

Recomendação de merge: **PR1 → PR2 → PR3 → PR4**; **PR5** após PR1 ou no fim se tocar nos mesmos blocos de UI.

---

## 3. Detalhamento por etapa

### PR1 — Alinhar cards e DRE ao mesmo recorte da tabela

| Campo | Conteúdo |
|-------|----------|
| **Objetivo** | Eliminar o ramo em que `_kp_cards` / DRE usam `_df_fiscal_kpi_anchor` + `_df_nf_so_periodo_fiscal` com `plataformas_sel=()` enquanto a tabela usa `df_nf_panel` filtrado. Totais principais = **soma sobre o mesmo `DataFrame`** que alimenta a grelha (após recorte comercial completo). |
| **Arquivos** | [`app_operacional.py`](app_operacional.py) (`_render_faturamento_dre_minimal`, helpers de KPI), eventualmente extrair função pura para testes. |
| **Funções** | `_render_faturamento_dre_minimal` (~7246–7420+), `compute_nf_panel_kpis`, `_faturamento_nf_apply_minimal_recorte`, `_nf_panel_filter_merged_fiscal_by_plataforma_resumo`. |
| **Camada** | **UI** (orquestração de recorte e KPIs). |
| **Risco** | **Médio:** totais “faturado NF” deixam de bater com soma fiscal **bruta** do período quando há filtro de canal — passa a ser **coerente com o recorte** (comportamento desejado). |
| **Validação** | Com dados Gama + Shopee selecionado: cards `valor_faturado_nf`, `valor_venda`, `n_nf` = agregados de `df_nf_panel` **pós** `_faturamento_dre_apply_produto_e_sinal_venda`. Sem plataforma: coincide com universo atual da tabela. |
| **Dependência** | Nenhuma. |

### PR2 — Visibilidade e busca das NFs (caso 10031, 10030, 10027)

| Campo | Conteúdo |
|-------|----------|
| **Objetivo** | Nenhuma NF presente no `nf_panel` pode “sumir” sem o utilizador perceber: resultado **NaN** ou **0**, fiscal-only (`plataforma` "—"), e busca **10030** = **010030**. |
| **Arquivos** | [`app_operacional.py`](app_operacional.py) (`_faturamento_dre_apply_produto_e_sinal_venda`, multiselect sinal, coluna Info/caption), [`processing/faturamento/normalize.py`](processing/faturamento/normalize.py) (opcional: helper `nf_busca_match_key`), testes em [`tests/`](tests/). |
| **Funções** | `_faturamento_dre_apply_produto_e_sinal_venda`, `_render_faturamento_dre_minimal` (filtro UI + tabela), qualquer `text_input` de busca NF no fluxo minimal. |
| **Camada** | **UI** (+ testes). |
| **Risco** | **Baixo/médio:** tabela pode mostrar mais linhas (neutras); necessário **rótulo** claro (`Info dados` / nova coluna “Estado comercial”). |
| **Validação** | **010031**, **010027**: visíveis com Shopee **ou** aparecem como “canal não identificado” com copy explícito; **010030**: visível com filtro lucro+prejuízo se incluir neutro **ou** linha dedicada no multiselect; busca `10030`/`10031`/`10027` encontra linhas. |
| **Dependência** | **PR1** (para não validar contra KPIs ainda desalinhados). |

### PR3 — Colunas separadas no materializado: Receita de Frete vs Tarifa (Custo de Frete)

| Campo | Conteúdo |
|-------|----------|
| **Objetivo** | Deixar de usar um único `frete` como proxy das duas regras de negócio. **Receita frete** = valor cliente / transportadora própria (reutilizar lógica de [`processing/faturamento/calc.py`](processing/faturamento/calc.py) `frete_mercado_envios_vs_transportadora` / `frete_tp` ao nível **linha**, agregar por NF). **Tarifa** = soma de **`Custo de Frete`** do pedido por NF. |
| **Arquivos** | [`faturamento_dre_recorte_minimo.py`](faturamento_dre_recorte_minimo.py) (`build_nf_grain_dataframe`, `_nf_grain_frete_numeric` refatorar ou substituir por duas séries), [`processing/faturamento/nf_panel_materializado.py`](processing/faturamento/nf_panel_materializado.py) (`NF_PANEL_REQUIRED_COLUMNS`), [`faturamento_dre_recorte_minimo.py`](faturamento_dre_recorte_minimo.py) `apply_nf_panel_frete_gap_fallback`, `apply_nf_panel_resultado_frete_nota_lista`, [`docs/faturamento_materializado_nf_first_contrato.md`](docs/faturamento_materializado_nf_first_contrato.md). |
| **Funções** | `build_nf_grain_dataframe`, `build_nf_materializado_dataframe`, `build_nf_panel_materializado_dataframe`, funções de ajuste de frete/resultado no painel. |
| **Camada** | **Pipeline/materialização** (primário). |
| **Risco** | **Alto** para números: altera `resultado` e possivelmente `frete` legado — exige testes de regressão e `schema_version` / notas no metadata. |
| **Validação** | Testes unitários com fixture mínima (ME vs TP + `Custo de Frete`); reprocessar `cliente_2` e conferir ordens de grandeza; NFs sem pedido: tarifa 0, receita conforme fiscal se aplicável. |
| **Dependência** | **PR1** recomendado (recorte único simplifica debug). Pode desenvolver em branch a partir de main se PR1 já merged. |

### PR4 — KPIs e DRE com duas linhas de frete

| Campo | Conteúdo |
|-------|----------|
| **Objetivo** | `compute_nf_panel_kpis` e blocos DRE no Streamlit mostram **receita de frete** e **custo/tarifa de envio** separadamente; labels e tooltips alinhados à regra aprovada. |
| **Arquivos** | [`faturamento_dre_recorte_minimo.py`](faturamento_dre_recorte_minimo.py) (`compute_nf_panel_kpis`), [`app_operacional.py`](app_operacional.py) (render DRE/cards), testes. |
| **Funções** | `compute_nf_panel_kpis`, trechos de `_render_faturamento_dre_minimal` que montam cards/DRE. |
| **Camada** | **UI** + somas em **Python partilhado** (`compute_nf_panel_kpis`). |
| **Risco** | **Médio:** copy incorreta confunde mais do que hoje — revisão de texto obrigatória. |
| **Validação** | Soma manual no Parquet pós-PR3 = KPIs na UI para um recorte fixo; DRE fecha com `resultado` já materializado. |
| **Dependência** | **PR3** obrigatório. |

### PR5 — Auditoria Preço de lista × Quantidade

| Campo | Conteúdo |
|-------|----------|
| **Objetivo** | Corrigir **apenas** onde o unitário é usado como total ou comparado mal. **Não** alterar `build_nf_grain_dataframe` / `_nf_grain_venda_linha_series` (já corretos). |
| **Arquivos** | [`app_operacional.py`](app_operacional.py) (`_faturamento_compute_alert_bools` ~7972–7973: ramo sem `Receita_Bruta` compara `Preço de lista` a `Valor total` **sem** × `Quantidade`), possivelmente grelha legada `_render_faturamento_painel` / export (~8804+), [`processing/faturamento/build.py`](processing/faturamento/build.py) só se algum cálculo persistido estiver errado (improvável após leitura do grão). |
| **Funções** | `_faturamento_compute_alert_bools`, export CSV do painel linha se aplicável. |
| **Camada** | **Quase só UI/alertas**; pipeline só se auditoria encontrar coluna derivada errada. |
| **Risco** | **Baixo** se limitado a alertas e labels; alterar alertas pode mudar contagens de “divergência”. |
| **Validação** | Caso linha com Qtd>1: alerta de divergência usa `(Quantidade × Preço de lista)` vs `Valor total` (ou `Receita_Bruta` quando existir). Export mostra coluna explícita “Venda lista (linha)” se necessário. |
| **Dependência** | **PR1** mínimo; ideal após PR2–PR4 para conflitos de merge menores. |

---

## 4. Tratamento das NFs 10031, 10030 e 10027

**Chave materializada:** `Nota_Numero_Normalizado` = **`010031`**, **`010030`**, **`010027`** (Gama Home).

| NF | Situação nos artefatos | Tratamento esperado pós-correção |
|----|------------------------|----------------------------------|
| **10031** (`010031`) | Fiscal-only: **0** linhas em `dataset.parquet`; presente em `nf_panel` com `plataforma` "—", `resultado` **0** | **Sempre listável** no recorte empresa+período; com filtro Shopee: **ou** incluir com rótulo “Canal não identificado / só fiscal”, **ou** política explícita na UI (“excluídas do recorte Shopee — sem vínculo comercial”); **não** desaparecer por filtro de sinal. |
| **10030** (`010030`) | Tem pedido Shopee; `comercial_incompleto` True; `resultado` **NaN** | **Visível** na tabela com estado “dados incompletos”; filtro lucro/prejuízo deve incluir opção **neutro/indisponível** ou linha separada; busca `10030` encontra. |
| **10027** (`010027`) | Igual a 10031 (fiscal-only) | Igual a 10031. |

**Regra de produto:** Toda exclusão da grelha deve ter **causa visível** (filtro ativo, legenda ou coluna “Motivo”).

---

## 5. Regra final assumida — Frete / Tarifa / DRE

- **Receita de Frete:** valor pago pelo cliente relativamente a **transportadora própria** (TP), alinhado à separação já existente em `calc.py` para linha de pedido, agregado por NF.
- **Tarifa de Envio:** soma de **`Custo de Frete`** do relatório de pedidos por NF (0 se não houver linhas de pedido).
- **DRE:** duas linhas distintas (e tooltips); `frete` legado documentado como deprecado ou mapeado de forma explícita até remoção.

---

## 6. Regra de Preço de Lista × Quantidade

- **Já correto (não mexer):** agregação `valor_venda` no grão NF via `_nf_grain_venda_linha_series` (`Quantidade × Preço de lista` por linha, depois soma).
- **Auditar e corrigir se necessário:** (1) [`_faturamento_compute_alert_bools`](app_operacional.py) quando **não** há `Receita_Bruta` — hoje pode comparar **unitário** PL a **Valor total**; deve usar **Quantidade × Preço de lista** vs total da linha. (2) Qualquer **export/grelha** do painel **linha** que implique “Preço de lista = total da linha” sem legenda. (3) Drill-downs que mostrem só PL sem Qtd — preferir coluna derivada ou help text.
- **Persistido:** não há evidência de `valor_venda` no `nf_panel` errado pelo grão; PR5 confirma com grep + teste.

---

## 7. Homologação após este bloco

**Ainda não** equivale a “pronto para replicação multi-cliente”: falta bateria de **testes de regressão** (recorte único, frete duplo, NFs fiscais órfãs), revisão de **contrato** e possível bump de **schema** do Parquet. **Sim** para **nova homologação funcional** interna: números alinhados ao recorte, NFs encontráveis e DRE com semântica de frete/tarifa explícita — desde que o checklist das PRs 1–5 e as três NFs estejam verdes.
