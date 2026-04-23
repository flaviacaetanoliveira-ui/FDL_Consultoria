---
name: Tooltips dois Resultados
overview: "Ajustar textos de ajuda no card KPI (HTML premium + fallback `st.metric`) e no métrico do Painel de Saúde para refletir a lógica real: mesmo conceito económico distinto por agregação (NF vs linhas com custo válido) e filtros (cards incluem Plataforma; painel de saúde não)."
todos:
  - id: kpi-html-title
    content: Atualizar `title` do hero Resultado em `build_kpi_nf_premium_shell_html` (faturamento_dre_ui.py) com explicação NF vs linhas + Plataforma.
    status: cancelled
  - id: kpi-fallback-help
    content: Alinhar `help` do `st.metric` Resultado em `_render_fdl_fat_dre_nf_kpi_cards` (app_operacional.py).
    status: cancelled
  - id: health-help
    content: Atualizar `help` do `st.metric` Resultado em `render_health_panel` (health_panel_ui.py).
    status: cancelled
isProject: false
---

# Tooltips para os dois valores de Resultado

## Origem dos números (verificação no código)

| Superfície | Fonte | O que soma |
|------------|--------|------------|
| **Card Resultado (KPI premium)** | [`compute_nf_panel_kpis`](faturamento_dre_recorte_minimo.py) sobre `df_nf_commercial_kpi` | Soma da coluna **`resultado`** com **uma linha por NF** no recorte dos filtros da página (empresa, período emissão NF, situação NF, **Plataforma** quando usada), após `build_nf_panel_aligned_to_fiscal_base` quando há fiscal. |
| **Painel de Saúde — métrico Resultado** | [`calcular_health_score`](app/components/health_score.py) | Soma da coluna **`Resultado`** no slice [`slice_linhas_nf_periodo`](app/components/health_score.py) — **grão linha** com **`Status_Custo` OK**; receita da margem usa **`Vl_Venda`**. |

Chamada em [`app_operacional.py`](app_operacional.py): KPI usa `df_nf_commercial_kpi`; saúde recebe `faturamento_df` (linhas comerciais), sem passar Plataforma para `render_faturamento_health_panel_if_enabled` — logo **os dois totais podem divergir mesmo sem “erro”**.

A narrativa “card = fiscal (NF), saúde = lista” **não corresponde ao código**: o KPI já é resultado **consolidado por NF** na base materializada (deduções já embutidas); o painel soma **linhas** da base operacional. **Não** propor “valor faturado fiscal” como *base do resultado* no tooltip do KPI (isso confunde com “Valor faturado (NF)” como referência de conciliação, não como denominador do resultado do card).

## Alterações propostas (só texto / UX)

### 1. KPI premium (HTML): [`build_kpi_nf_premium_shell_html`](app/components/faturamento_dre_ui.py)

Hoje o “tooltip” é o atributo nativo **`title`** na `div` do hero Resultado (linhas ~390–392), já com texto técnico.

- **Substituir** o `title` por uma frase curta em duas ideias: (i) soma **por NF** no recorte dos filtros (**incluindo Plataforma**); (ii) mesmo materializado que alimenta chips/DRE; (iii) diferente do painel de saúde (**linhas com custo válido**, sem mesmo filtro de Plataforma).
- Manter linguagem para cliente final (sem “dataset”, “materializado” se quiserem máximo polimento — opcional mencionar “base consolidada por NF”).
- Opcional UX: `cursor: help` já costuma aparecer com `title`; não é necessário `st.metric`.

### 2. Fallback KPI com `st.metric`: [`_render_fdl_fat_dre_nf_kpi_cards`](app_operacional.py)

Quando `_FAT_DRE_UI_V2` está desligado, já existe `help=` no métrico “Resultado” (~5260–5265).

- **Alinhar** o texto ao mesmo significado que o `title` do premium (parágrafos curtos consistentes).

### 3. Painel de Saúde: [`render_health_panel`](app/components/health_panel_ui.py)

O métrico “Resultado” já usa `help=` (~98–101).

- **Substituir** por texto que diga explicitamente: soma em **linhas de pedido** com custo válido; margem/score usam **`Vl_Venda`** no período; **não** aplica o filtro **Plataforma** dos cards — por isso pode diferir do Resultado dos KPIs.

## O que não fazer nesta tarefa

- **Não** alterar `calcular_health_score`, `compute_nf_panel_kpis` nem filtros — só documentação na UI.
- **Não** afirmar bug ou igualdade esperada dos dois números sem nova análise de produto.

## Follow-up opcional (fora do escopo pedido)

Se a intenção de produto for **totais idênticos**, seria preciso **alinhar filtros** (ex.: passar Plataforma ao slice do painel de saúde) ou **usar o mesmo dataframe/grão** — isso é mudança de comportamento, não só tooltip.

## Verificação manual

- Modo premium: hover no card **Resultado** mostra o novo `title`.
- Fallback: ícone (?) do `st.metric` “Resultado”.
- Painel de saúde: ícone (?) do métrico “Resultado”.
- Textos não contradizem as linhas “Valor da venda (lista)” / “Valor faturado (NF)” ao lado.
