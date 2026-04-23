"""
Verifica se o cálculo de imposto do período está usando receita correta.

Objetivo: para Móveis EAP em abril/2026:
- Base líquida do período: ~R$ 675.025,21
- Alíquota efetiva calculada (mês abril): 11,46%
- Imposto esperado do período: R$ 675.025,21 × 11,46% = ~R$ 77.361,89

Output observado (pré-fix): ~R$ 155.586,41 (2x o esperado)

Investigar onde o cálculo mês a mês está buscando a receita e se está duplicando.
"""

from pathlib import Path
from datetime import date

import pandas as pd

from processing.faturamento.params_regime import load_faturamento_params_for_ui
from processing.faturamento.simples_nacional import (
    agregar_simples_nacional_para_painel_fiscal,
    extrair_historico_receita_mensal_por_empresa,
)


def main() -> None:
    params = load_faturamento_params_for_ui(
        load_info={
            "cliente_slug": "cliente_2",
            "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
        }
    )

    parquet_path = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet")
    df_fiscal = pd.read_parquet(parquet_path)

    # Filtrar para recorte 01/01-30/04/2026
    df_fiscal = df_fiscal.copy()
    df_fiscal["Nota_Data_Emissao"] = pd.to_datetime(df_fiscal["Nota_Data_Emissao"])
    mask = (df_fiscal["Nota_Data_Emissao"] >= "2026-01-01") & (df_fiscal["Nota_Data_Emissao"] <= "2026-04-30")
    df_periodo = df_fiscal.loc[mask].copy()

    # Excluir situações inválidas
    sit_invalidas = ["Cancelada", "Denegada", "Inutilizada"]
    df_periodo = df_periodo[~df_periodo["Nota_Situacao"].isin(sit_invalidas)]

    print("=== Receita bruta do PERÍODO por empresa (sem histórico) ===")
    por_emp_periodo = df_periodo.groupby("org_id")["Valor_Liquido_NF"].sum()
    for slug, valor in por_emp_periodo.items():
        print(f"  {slug}: R$ {valor:,.2f}")

    print("\n=== Receita bruta mensal do PERÍODO (sem histórico) ===")
    df_periodo["competencia"] = df_periodo["Nota_Data_Emissao"].dt.to_period("M").dt.to_timestamp()
    por_emp_mes = df_periodo.groupby(["org_id", "competencia"])["Valor_Liquido_NF"].sum().unstack(fill_value=0)
    print(por_emp_mes.to_string())

    # Comparar com o que o extrator retorna
    print("\n=== Receita mensal conforme extrair_historico_receita_mensal_por_empresa (dataset completo) ===")
    historico = extrair_historico_receita_mensal_por_empresa(df_fiscal)
    for slug in ["gama_home", "mega_star", "moveis_eap", "mega_facil"]:
        meses = historico.get(slug, {})
        meses_periodo = {m: v for m, v in meses.items() if date(2026, 1, 1) <= m <= date(2026, 4, 30)}
        total = sum(meses_periodo.values())
        print(f"  {slug}: {len(meses_periodo)} meses no período, total R$ {total:,.2f}")
        for m, v in sorted(meses_periodo.items()):
            print(f"    {m}: R$ {v:,.2f}")

    # Rodar agregador (base = recorte período; full = completo — como o painel)
    print("\n=== Saída do agregador ===")
    resultado = agregar_simples_nacional_para_painel_fiscal(
        df_fiscal_base=df_periodo,
        empresas_slugs=["gama_home", "mega_star", "moveis_eap", "mega_facil"],
        params_regime=params,
        periodo_inicio=date(2026, 1, 1),
        periodo_fim=date(2026, 4, 30),
        df_fiscal_full=df_fiscal,
    )

    print("\n=== Análise de coerência ===")
    for slug in ["gama_home", "mega_star", "moveis_eap"]:
        dados = resultado["por_empresa"][slug]
        base = dados["base_liquida_periodo"]
        imposto = dados["imposto_calculado_periodo"]
        aliq = dados.get("aliquota_efetiva_ponderada_periodo_pct")

        if aliq and aliq > 0:
            aliq_implicita = (imposto / base) * 100 if base > 0 else 0

            aliq_ref = dados.get("aliquota_efetiva_calculada_pct") or dados.get("aliquota_referencia_json_pct")
            imposto_esperado = base * (aliq_ref / 100) if aliq_ref else 0
            razao = imposto / imposto_esperado if imposto_esperado > 0 else 0

            print(f"\n  {slug}:")
            print(f"    Base: R$ {base:,.2f}")
            print(f"    Alíq. simples (JSON ou calculada): {aliq_ref}%")
            print(f"    Imposto esperado (base × alíq): R$ {imposto_esperado:,.2f}")
            print(f"    Imposto do agregador: R$ {imposto:,.2f}")
            print(f"    Razão (atual/esperado): {razao:.2f}x")
            print(f"    Alíquota implícita: {aliq_implicita:.2f}%")
            print(f"    Alíquota ponderada período: {aliq}%")


if __name__ == "__main__":
    main()
