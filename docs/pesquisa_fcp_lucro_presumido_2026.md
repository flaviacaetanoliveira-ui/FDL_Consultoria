# Pesquisa FCP e Lucro Presumido 2026 — Mega Fácil

**Cliente:** Cliente 2 (Pedro)  
**Empresa:** Mega Fácil  
**Data da pesquisa:** 23/04/2026  
**Objetivo:** Fundamentar implementação do motor fiscal Lucro Presumido (F·T2.5)  
**Status:** Premissas validadas pelo product owner (Flavia)

---

## Resumo Executivo

Dois achados críticos:

**1. LC 224/2025 — Aumento de presunção Lucro Presumido em 2026**
- Aumento de 10% nos percentuais de presunção IRPJ/CSLL
- Apenas para receita anual acima de R$ 5 milhões
- Aplica a parcela excedente, não tudo
- Tributos federais: IRPJ vigente desde 01/01/2026; CSLL/PIS/COFINS desde 01/04/2026

**2. FCP — Panorama para móveis (NCM capítulo 94)**
- SP, SC: 0% confirmado (sem FCP para móveis)
- RJ: 2% aplicável (regime amplo)
- MG, BA, PR, RS: 0% (premissa conservadora — não confirmadas em fontes oficiais como sujeitas a FCP)

---

## Parte 1 — Atualização Federal (LC 224/2025)

### Base legal
- Lei Complementar 224 de 26/12/2025
- Decreto 12.808/2025
- Instrução Normativa RFB 2.305/2025

### Regra
Regimes com base presumida (Lucro Presumido) têm acréscimo de 10% nos percentuais de presunção sobre a parcela da receita bruta anual que exceder R$ 5 milhões.

### Tabela para comércio

| Percentual | Até R$ 5M/ano | Acima de R$ 5M/ano |
|---|---|---|
| Presunção IRPJ | 8% | 8,8% |
| Presunção CSLL | 12% | 13,2% |

### Alíquotas finais aplicadas

| Tributo | Fórmula | Até R$ 5M | Acima R$ 5M |
|---|---|---|---|
| IRPJ | 15% × presunção | 1,20% | 1,32% |
| IRPJ adicional | 10% sobre lucro > R$ 60k/trim | conforme | conforme |
| CSLL | 9% × presunção | 1,08% | 1,19% |
| PIS | alíquota direta | 0,65% | 0,65% |
| COFINS | alíquota direta | 3,00% | 3,00% |
| Total federal | | ~5,93% | ~6,16% |

### Decisão de produto
Motor implementa COM acréscimo (segue lei). Flag configurável `aplicar_majoracao_lc_224` permite desativar caso contribuinte obtenha liminar específica.

**Mega Fácil:** flag mantida em `true` (não pretende buscar liminar — confirmado por Flavia em 24/04/2026).

### Fontes
- EY Tax Alert (dezembro 2025): Decreto 12.808 e IN RFB 2.305
- Contabilidade Scalabrini (janeiro 2026): análise prática
- Conjur (janeiro 2026): artigo acadêmico
- Reforma Tributária (dezembro 2025): cobertura jornalística

---

## Parte 2 — ICMS Interno SP para Móveis

### Descoberta importante

A alíquota interna SP para móveis capítulo 9403 NCM **não é 18%**. É **12% com complemento de 1,3%** conforme RICMS-SP art. 54, inciso XIII, alínea "b" e § 7°.

| Item | Alíquota |
|---|---|
| Móveis 9403 NCM (produtos completos) | 12% + 1,3% = 13,3% |
| Partes e peças de móveis 9403 | 18% |

**Para a Mega Fácil:** 100% das vendas são de móveis completos. Alíquota interna = 13,3%.

### Fontes
- Resposta Consulta RC 26509/2022 SEFAZ-SP
- Resposta Consulta RC 29260/2024 SEFAZ-SP
- Resposta Consulta RC 25757/2022 SEFAZ-SP

---

## Parte 3 — Tabela FCP por UF

### Tabela consolidada (UFs onde Mega Fácil vende)

| UF | % vendas MF | FCP móveis 9403 | Confiança | Fonte |
|---|---|---|---|---|
| SP | 31,2% | 0% | Alta | SEFAZ-SP RC 29586/2024 |
| RJ | 15,0% | 2% | Alta | LC estadual 210/2023 |
| MG | 13,0% | 0% | Conservador | Lei supérfluos ALMG 2023 (móveis fora) |
| BA | 8,4% | 0% | Conservador | Regime seletivo, móveis fora de listas conhecidas |
| PR | 5,9% | 0% | Conservador | Não confirmado em fontes oficiais |
| RS | 3,7% | 0% | Conservador | Não confirmado em fontes oficiais |
| SC | 3,7% | 0% | Alta | Estado não possui FCP |
| Outras UFs | 19,1% | 0% | Conservador | Default |

### Detalhamento por UF

#### São Paulo — 0% para móveis
- **Base:** Lei Estadual 16.006/2015 (FECOEP)
- **Aplicação restrita:** bebidas alcoólicas NCM 22.03 + fumo capítulo 24
- **Móveis capítulo 94:** NÃO aplica
- **Fonte oficial:** Resposta Consulta RC 29586/2024 SEFAZ-SP

#### Rio de Janeiro — 2% aplica
- **Base:** LC estadual 210/2023 (revogou Lei 4.056/2002)
- **Aplicação ampla:** incide sobre praticamente todos os bens, salvo exceções
- **Móveis capítulo 94:** APLICA (não está nas exceções)
- **Alíquota:** 2%
- **Vigência:** até 31/12/2031
- **Fontes:** Lefosse Advogados, Machado Meyer, SEFAZ-RJ

#### Minas Gerais — 0% (premissa conservadora)
- Lei estadual de supérfluos 2023 com lista específica vigente até 31/12/2026
- Produtos na lista: bebidas alcoólicas, cigarros, perfumes, cosméticos, equipamentos de som automotivo
- Móveis capítulo 94: NÃO constam na lista
- Premissa adotada: 0%
- **Fonte:** ALMG (Assembleia Legislativa MG)

#### Bahia — 0% (premissa conservadora)
- Regime FECP seletivo
- Padrão conhecido: bebidas, fumo, energéticos, cosméticos, armas
- Móveis: não aparecem em listas de supérfluos consultadas
- Premissa adotada: 0% — confirmar com contador se houver dúvida

#### Paraná — 0% (premissa conservadora)
- Alíquota genérica FCP PR: 2%
- Aplicação específica em móveis: não confirmada em fontes oficiais
- Premissa adotada: 0% — revisar quando contador confirmar

#### Rio Grande do Sul — 0% (premissa conservadora)
- Alíquota genérica FCP RS: 2%
- Aplicação específica em móveis: não confirmada em fontes oficiais
- Premissa adotada: 0% — revisar quando contador confirmar

#### Santa Catarina — 0% (sem FCP)
- Estado não possui FCP
- Confirmado em múltiplas fontes
- Junto com AP, PA

---

## Parte 4 — Impacto Financeiro Estimado

Baseado em vendas reais Mega Fácil 01/01/2026 a 30/04/2026: R$ 2.550.905 em 9.666 NFs.

### Cenário implementado (RJ 2%, demais 0%)
- Vendas RJ 4 meses: R$ 381.752
- FCP estimado: R$ 7.635 em 4 meses
- **Anualizado: ~R$ 23.000**

### Cenário pior caso (se contador confirmar FCP em MG/BA/PR/RS)
- Vendas combinadas 4 meses: R$ 789.731
- FCP adicional: R$ 15.795 em 4 meses
- **Anualizado adicional: ~R$ 47.000**

---

## Parte 5 — Premissas Aprovadas pelo Product Owner

Confirmadas por Flavia em 24/04/2026:

1. Mega Fácil em SP: sem regime especial, aplicar regra geral 13,3% para móveis 9403
2. FCP MG, BA, PR, RS: aplicar 0% (premissa conservadora)
3. FCP RJ: aplicar 2%
4. LC 224/2025: aplicar majoração conforme lei (não buscar liminar)

---

## Parte 6 — Configuração JSON

Estrutura proposta para `ops/faturamento_params_cliente_2_gama_star_eap.json`:

```json
{
  "lucro_presumido": {
    "ativo": true,
    "limite_receita_majoracao_anual": 5000000.00,
    "presuncao_irpj_ate_limite": 0.08,
    "presuncao_irpj_acima_limite": 0.088,
    "presuncao_csll_ate_limite": 0.12,
    "presuncao_csll_acima_limite": 0.132,
    "aliquota_irpj": 0.15,
    "limite_adicional_irpj_trimestral": 60000.00,
    "aliquota_adicional_irpj": 0.10,
    "aliquota_csll": 0.09,
    "pis": 0.0065,
    "cofins": 0.03,
    "aplicar_majoracao_lc_224": true
  },
  "icms_interno_uf_origem": {
    "SP": {
      "moveis_9403_completos": 0.133,
      "moveis_9403_partes_pecas": 0.18
    }
  },
  "icms_interestadual_origem_sp": {
    "AC": 0.07, "AL": 0.07, "AM": 0.07, "AP": 0.07, "BA": 0.07,
    "CE": 0.07, "DF": 0.07, "ES": 0.07, "GO": 0.07, "MA": 0.07,
    "MG": 0.12, "MS": 0.07, "MT": 0.07, "PA": 0.07, "PB": 0.07,
    "PE": 0.07, "PI": 0.07, "PR": 0.12, "RJ": 0.12, "RN": 0.07,
    "RO": 0.07, "RR": 0.07, "RS": 0.12, "SC": 0.12, "SE": 0.07,
    "TO": 0.07
  },
  "fcp_destino": {
    "RJ": 0.02,
    "default": 0.00
  }
}
```

---

## Parte 7 — Limitações e Recomendações

### Limitações desta pesquisa
- Pesquisa baseada em fontes online (legislação, advogados especializados, software houses)
- Não substitui consulta tributária formal
- Legislação fiscal estadual muda frequentemente
- Algumas premissas conservadoras (MG, BA, PR, RS) precisam validação ideal com contador

### Recomendações
1. Revisar tabela FCP anualmente
2. Quando contador confirmar premissas conservadoras, ajustar JSON
3. Acompanhar evolução jurídica da LC 224/2025 (há contestação em andamento)
4. Documentar cada alteração no JSON em changelog próprio

### Changelog
- 23/04/2026: pesquisa inicial e premissas adotadas
- 24/04/2026: aprovação de premissas por Flavia (product owner)
