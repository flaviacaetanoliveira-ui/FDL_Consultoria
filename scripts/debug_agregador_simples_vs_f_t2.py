"""
Compara output do agregador atual (F · T2.1) com expectativa baseada em F · T2 original.

Expectativa (F · T2 original, Cliente 2, período 01/01-30/04/2026):
- gama_home: base R$ 285k, alíq 9,0%, imposto R$ 25k (warmup — deve usar JSON)
- mega_star: base R$ 482k, alíq 9,5%, imposto R$ 45k (warmup — deve usar JSON)
- moveis_eap: base R$ 649k, alíq 11,46%, imposto R$ 74k (CALCULADO, 12 meses)

Observado no painel (regressão):
- Todos com R$ 0,00 em base, 0,00% em alíquota, R$ 0,00 em imposto
"""

from pathlib import Path
from datetime import date
import pandas as pd

from faturamento_dre_recorte_minimo import build_faturamento_fiscal_base_slice
from processing.faturamento.fiscal_devolucoes_materializado import build_devolucoes_fiscal_dataframe
from processing.faturamento.params_regime import load_faturamento_params_for_ui
from processing.faturamento.simples_nacional import (
    agregar_simples_nacional_para_painel_fiscal,
    extrair_historico_receita_mensal_por_empresa,
)


def main():
    # 1. Carregar params
    params = load_faturamento_params_for_ui(
        load_info={
            "cliente_slug": "cliente_2",
            "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
        }
    )
    print(f"Params carregado: {params is not None}")
    if params is None:
        print("❌ FALHA: params não carregou")
        return

    print(f"Empresas no params: {[(e.org_id, e.aliquota_imposto, e.regime_tributario) for e in params.empresas]}")

    # 2. Carregar dataset fiscal
    parquet_path = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet")
    df_fiscal = pd.read_parquet(parquet_path)
    print(f"\nParquet fiscal: {len(df_fiscal)} linhas")
    print(f"Colunas: {list(df_fiscal.columns)[:20]}")

    # Se houver coluna de empresa, mostrar distribuição
    if "empresa_slug" in df_fiscal.columns:
        print(f"\nDistribuição por empresa_slug:")
        print(df_fiscal["empresa_slug"].value_counts().to_string())
    elif "empresa" in df_fiscal.columns:
        print(f"\nDistribuição por empresa:")
        print(df_fiscal["empresa"].value_counts().to_string())

    # 3. Testar extração de histórico mensal
    print(f"\n=== HISTÓRICO MENSAL POR EMPRESA ===")
    historico = extrair_historico_receita_mensal_por_empresa(df_fiscal)
    for empresa_slug, meses in historico.items():
        total_meses = len(meses)
        receita_total = sum(meses.values())
        print(f"  {empresa_slug}: {total_meses} meses, total R$ {receita_total:,.2f}")
        if total_meses > 0:
            datas_ordenadas = sorted(meses.keys())
            print(f"    Primeiro mês: {datas_ordenadas[0]}")
            print(f"    Último mês: {datas_ordenadas[-1]}")

    # 4. Testar agregador (base fiscal já recortada ao período — igual ao painel)
    print(f"\n=== AGREGADOR SIMPLES NACIONAL ===")
    empresas_slugs = [e.org_id for e in params.empresas]
    print(f"Empresas passadas: {empresas_slugs}")

    d_ini, d_fim = date(2026, 1, 1), date(2026, 4, 30)
    emp_labels = tuple(e.empresa for e in params.empresas if (e.regime_tributario or "").strip() == "simples_nacional")
    params_json = Path("ops/faturamento_params_cliente_2_gama_star_eap.json")
    df_dev = build_devolucoes_fiscal_dataframe(params_json)
    df_base_periodo, _stats = build_faturamento_fiscal_base_slice(
        df_fiscal,
        empresas_sel=emp_labels,
        nf_d_ini=d_ini,
        nf_d_fim=d_fim,
        ok_nf_dates=True,
        df_devolucoes=df_dev,
    )
    print(f"Base fiscal recorte período: {len(df_base_periodo)} linhas · base líquida stats: {_stats.base_fiscal_liquida:,.2f}")

    resultado = agregar_simples_nacional_para_painel_fiscal(
        df_fiscal_base=df_base_periodo,
        empresas_slugs=empresas_slugs,
        params_regime=params,
        periodo_inicio=d_ini,
        periodo_fim=d_fim,
        df_fiscal_full=df_fiscal,
        df_devolucoes=df_dev,
        ok_nf_dates=True,
    )

    print(f"\nResultado agregador:")
    print(f"  competencia_referencia: {resultado.get('competencia_referencia')}")
    print(f"  empresas_em_warmup: {resultado.get('empresas_em_warmup')}")
    print(f"  empresas_com_calculo_oficial: {resultado.get('empresas_com_calculo_oficial')}")

    print(f"\nPor empresa:")
    for slug, dados in resultado.get("por_empresa", {}).items():
        print(f"  {slug}:")
        print(f"    regime: {dados.get('regime')}")
        print(f"    origem_aliquota: {dados.get('origem_aliquota')}")
        print(f"    meses_historico_disponiveis: {dados.get('meses_historico_disponiveis')}")
        print(f"    aliquota_efetiva_calculada_pct: {dados.get('aliquota_efetiva_calculada_pct')}")
        print(f"    aliquota_referencia_json_pct: {dados.get('aliquota_referencia_json_pct')}")
        print(f"    aliquota_efetiva_ponderada_periodo_pct: {dados.get('aliquota_efetiva_ponderada_periodo_pct')}")
        print(f"    base_liquida_periodo: R$ {dados.get('base_liquida_periodo', 0):,.2f}")
        print(f"    imposto_calculado_periodo: R$ {(dados.get('imposto_calculado_periodo') or 0):,.2f}")

    print(f"\nTotal Simples:")
    total = resultado.get("total_simples", {})
    print(f"  base_liquida: R$ {total.get('base_liquida', 0):,.2f}")
    print(f"  imposto_total: R$ {total.get('imposto_total', 0):,.2f}")
    print(f"  aliquota_media_ponderada_pct: {total.get('aliquota_media_ponderada_pct')}")


if __name__ == "__main__":
    main()
