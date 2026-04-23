# Padrões técnicos — FDL Analytics Streamlit

Referência para renderização, tabelas, filtros temporais, debug e cache. Ciclo E · consolidado após Ciclo D (mar/2026).

---

## Renderização e CSS

### Regra 1 — CSS inline para `st.html`

**Contexto.** `st.html()` renderiza dentro de iframe isolado (sandbox do Streamlit 1.35+). CSS injetado via `st.markdown` no documento pai **não atravessa** para dentro do iframe.

**Regra.** Todo CSS que estiliza HTML dentro de `st.html()` deve estar na mesma string emitida pelo widget:

```python
st.html(COMPONENTE_CSS + HTML_COMPONENTE)
```

**Bug conhecido.** Ciclo D · termômetro: HTML correto mas invisível porque estilos estavam apenas em `st.markdown`.

**Quando aplicar.** Sempre que usar `st.html()`. Para folhas grandes, usar constante `COMPONENTE_CSS = "<style>...</style>"` e concatenar.

**Alternativa.** `st.markdown(html, unsafe_allow_html=True)` renderiza no DOM principal e pode compartilhar CSS global — porém aumenta risco de conflito com temas do Streamlit.

---

## DataFrames e tabelas

### Regra 2 — Numérico para ordenação e cor, display para exibição

**Contexto.** `st.dataframe` com `pandas.Styler`:

- Ordenação ao clicar no cabeçalho usa valores da célula exibidos após format, mas exige dados numéricos tratáveis.
- Formatação pt-BR (R$, %) via `.format()` do Styler.
- Coloração condicional deve ler o mesmo valor numérico da coluna.

**Regra.** Nos modelos (`dataclass`) de linha, manter paralelos quando necessário:

- `receita` + `receita_display`
- `margem_liquida_pct` + `margem_liquida_display`

Na grade, preferir **colunas numéricas** + `.format(lambda x): fmt_*` no Styler. **Não** usar só `_display` string na coluna ordenada sem format.

**Bug conhecido.** Ciclo D · Curva ABC: margem mostrou `12.117581` porque faltava entrada da coluna no `.format()` do Styler.

---

## Debug e instrumentação

### Regra 3 — Captions admin para debug

**Contexto.** Blocos em `try/except` ou condições de não-renderização podem falhar sem mensagem ao utilizador final.

**Regra.** Para diagnóstico admin, usar variável ou helper que respeita `FDL_RG_PACE_DEBUG` / modo admin (`_fdl_rg_pace_debug_enabled()`):

```python
if _fdl_rg_pace_debug_enabled():
    st.caption(f"🔍 <componente> debug: <motivo>")
```

Em produção o utilizador típico não vê ruído; admin vê causa de skip.

---

## Cache

### Regra 4 — Chave de cache inclui todos os filtros relevantes

**Contexto.** `@st.cache_data` faz hash dos argumentos. DataFrames precisam de função de hash estável (`dataframe_cache_token`).

**Regra.** Parâmetros posicionais/chave devem incluir:

- Recorte temporal (datas como `date`, não objetos mutáveis)
- `empresas_sel` / `plataformas_sel` como `tuple` ordenada (`normalize_sorted_str_tuple`)
- `pipeline_version`, `cliente_slug` para invalidar entre deploys / tenants

**Não fazer.** Passar `list` mutável onde a ordem importa sem normalizar.

---

## Date picker (período da venda / NF) e limites do calendário

### Regra 5 — `max_value` do `st.date_input` segue calendário derivado dos dados + hoje

**Contexto (investigação Ciclo E).** Os filtros de período usam `min_value` / `max_value` derivados de `nf_cal_min` / `nf_cal_max` em `app_operacional.py`, após `_min_cal_limits(nf_min, nf_max)` em `faturamento_dre_recorte_minimo.py`:

```python
def _min_cal_limits(d_min: date, d_max: date) -> tuple[date, date]:
    today = datetime.now(_BR_TZ).date()
    cal_max = max(d_max, today)
    ...
```

Ou seja, o último dia **selecionável** no widget não é “último dia do mês” por defeito — é pelo menos `max(data máxima na base, hoje)`, com clamps adicionais (piso de emissão 2026+, etc.).

**Classificação.** Comportamento **intencional (Caso A)**: evita escolher intervalos cuja data fim ultrapassa o que o calendário considera válido para o contexto atual; o estado da sessão é ainda re-clampado a cada rerun (`min(max(..., nf_cal_min), nf_cal_max)`).

**Sintoma observado.** Filtrar “abril cheio” no meio de abril pode limitar a data fim a **hoje** (ou ao último dia com dados), não a 30/04 — até o calendário permitir (ex.: quando `nf_cal_max` cobre o fim do mês).

**Se no futuro** se quiser sempre permitir o último dia do mês civil corrente independentemente dos dados, isso é decisão de produto (afeta KPIs vazios vs pace).

---

## Termômetro de pace e recortes

### Regra 6 — Mensagem ao utilizador em recorte parcial **dentro do mesmo mês**

O termômetro só rende para mês civil completo (`mes_corrente` / `mes_fechado`). Para intervalos que estão num único mês civil mas não cobrem dia 1 → último dia (ex.: 01/03–15/03), exibe-se uma caption explicativa (não se aplica a filtros **multi-mês**).

Helper: `recorte_parcial_mes_civil_sem_mes_cheio` em `processing/faturamento/pace_mensal.py`.
