"""
Simula o fluxo que o painel usa para renderizar o bloco Simples Nacional.
Se o output aqui divergir do painel real, algo externo (cache, branch, env) está a intervir.
"""

from datetime import date
from pathlib import Path

import pandas as pd

from processing.faturamento.params_regime import load_faturamento_params_for_ui
from processing.faturamento.simples_nacional import agregar_simples_nacional_para_painel_fiscal


def main() -> None:
    params = load_faturamento_params_for_ui(
        load_info={
            "cliente_slug": "cliente_2",
            "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
        }
    )

    parquet_path = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet")
    df_fiscal = pd.read_parquet(parquet_path)
    df_fiscal["Nota_Data_Emissao"] = pd.to_datetime(df_fiscal["Nota_Data_Emissao"])

    mask = (df_fiscal["Nota_Data_Emissao"] >= "2026-01-01") & (df_fiscal["Nota_Data_Emissao"] <= "2026-04-30")
    df_periodo = df_fiscal.loc[mask].copy()

    resultado = agregar_simples_nacional_para_painel_fiscal(
        df_fiscal_base=df_periodo,
        empresas_slugs=["gama_home", "mega_star", "moveis_eap", "mega_facil"],
        params_regime=params,
        periodo_inicio=date(2026, 1, 1),
        periodo_fim=date(2026, 4, 30),
        df_fiscal_full=df_fiscal,
    )

    print("=== Valores que o painel DEVERIA renderizar (chamada simplificada) ===")
    for slug in ["gama_home", "mega_star", "moveis_eap", "mega_facil"]:
        dados = resultado["por_empresa"].get(slug, {})
        print(f"\n{slug}:")
        print(f"  regime: {dados.get('regime')}")
        print(f"  origem_aliquota: {dados.get('origem_aliquota')}")
        print(f"  base_liquida_periodo: R$ {dados.get('base_liquida_periodo', 0):,.2f}")
        print(f"  aliquota_efetiva_ponderada_periodo_pct: {dados.get('aliquota_efetiva_ponderada_periodo_pct')}")
        print(f"  imposto_calculado_periodo: R$ {(dados.get('imposto_calculado_periodo') or 0):,.2f}")

    print("\n=== Total Simples ===")
    total = resultado.get("total_simples", {})
    print(f"  base_liquida: R$ {total.get('base_liquida', 0):,.2f}")
    print(f"  imposto_total: R$ {total.get('imposto_total', 0):,.2f}")
    print(f"  aliquota_media_ponderada_pct: {total.get('aliquota_media_ponderada_pct')}")


if __name__ == "__main__":
    main()
