"""
Diagnóstico: Mega Fácil vs outras empresas no materializado Cliente 2.

Usa ``dataset_faturamento_fiscal.parquet`` + devoluções (mesma API que a Apuração Fiscal).
``dataset.parquet`` serve só para Σ ``Imposto`` comercial no período (aproximação ao ``kp["imposto"]``).

Executar na raiz do repositório: python scripts/debug_regime_mega_facil.py
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import pandas as pd

from faturamento_dre_recorte_minimo import (
    build_faturamento_fiscal_base_slice,
    dre_imposto_para_linha_dre_gerencial,
    enrich_faturamento_fiscal_base_stats,
)


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parquet_fiscal = root / "data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet"
    parquet_pedidos = root / "data_products/cliente_2/faturamento/current/dataset.parquet"
    parquet_dev = root / "data_products/cliente_2/faturamento/current/dataset_faturamento_devolucoes.parquet"
    meta_path = root / "data_products/cliente_2/faturamento/current/metadata.json"

    if not parquet_fiscal.is_file():
        print(f"Parquet fiscal em falta: {parquet_fiscal}")
        return

    df_fiscal = pd.read_parquet(parquet_fiscal)
    df_dev = pd.read_parquet(parquet_dev) if parquet_dev.is_file() else None
    df_ped = pd.read_parquet(parquet_pedidos) if parquet_pedidos.is_file() else pd.DataFrame()

    cfg_pct = 0.0
    if meta_path.is_file():
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            v = meta.get("aliquota_imposto_usada")
            if isinstance(v, (int, float)):
                cfg_pct = float(v) * 100.0
        except (OSError, json.JSONDecodeError):
            pass

    empresas_alvo = ["Gama Home", "Mega Star", "Móveis EAP", "Mega Fácil"]
    data_inicio = date(2026, 3, 1)
    data_fim = date(2026, 3, 31)

    print(f"Parquet fiscal: {len(df_fiscal)} linhas (grão NF)")
    print(f"metadata.json aliquota_imposto_usada => {cfg_pct:.2f}% (valor global unico no artefato)")
    print(f"Período NF emissão: {data_inicio} a {data_fim}\n")
    print("=" * 80)

    for empresa in empresas_alvo:
        print(f"\n### EMPRESA: {empresa}\n")
        try:
            _, stats = build_faturamento_fiscal_base_slice(
                df_fiscal,
                empresas_sel=(empresa,),
                nf_d_ini=data_inicio,
                nf_d_fim=data_fim,
                ok_nf_dates=True,
                df_devolucoes=df_dev,
            )

            kp_imposto = 0.0
            if not df_ped.empty and "empresa" in df_ped.columns and "Imposto" in df_ped.columns:
                em = df_ped["empresa"].astype(str).str.strip().eq(empresa)
                if "Data" in df_ped.columns:
                    d = pd.to_datetime(df_ped["Data"], errors="coerce", dayfirst=True)
                    m = em & (d >= pd.Timestamp(data_inicio)) & (d <= pd.Timestamp(data_fim))
                    sl = df_ped.loc[m]
                    kp_imposto = float(pd.to_numeric(sl["Imposto"], errors="coerce").fillna(0.0).sum())
                else:
                    sl = df_ped.loc[em]
                    kp_imposto = float(pd.to_numeric(sl["Imposto"], errors="coerce").fillna(0.0).sum())

            enriched = enrich_faturamento_fiscal_base_stats(
                stats,
                imposto_apurado=dre_imposto_para_linha_dre_gerencial(
                    {"imposto": kp_imposto},
                    fiscal_base_stats=stats,
                    aplicar_ponte_base_liquida=True,
                ),
                df_nf_aligned=None,
                aliquota_configurada_pct=cfg_pct,
            )

            imposto_ponte = dre_imposto_para_linha_dre_gerencial(
                {"imposto": kp_imposto},
                fiscal_base_stats=stats,
                aplicar_ponte_base_liquida=True,
            )

            print(f"  Valor líquido NF (Σ válidas):     R$ {stats.valor_liquido_fiscal_sum:,.2f}")
            print(f"  valor_faturado_nf (same slice):    R$ {stats.valor_faturado_nf:,.2f}")
            print(f"  Valor cancelado (linhas inválidas): R$ {stats.valor_cancelado:,.2f}")
            print(f"  Total devolvido (entradas):       R$ {stats.total_devolvido:,.2f}")
            print(f"  Base fiscal líquida:               R$ {stats.base_fiscal_liquida:,.2f}")
            print(f"  N NFs (fiscal slice):             {stats.n_nf}")
            print(f"  NFs devolução (entradas):         {stats.nfs_devolucao}")
            print(f"  Sum Imposto linhas pedido (mar):   R$ {kp_imposto:,.2f}  [kp imposto]")
            print(f"  Imposto após ponte DRE:           R$ {imposto_ponte:,.2f}")
            print(f"  Alíquota configurada (metadata): {cfg_pct:.2f}%")
            if enriched.base_fiscal_liquida > 1e-9:
                ae = (enriched.imposto / enriched.base_fiscal_liquida) * 100.0
                print(f"  Alíquota efetiva (imposto/base):  {ae:.2f}%")

            if "Aliquota_Imposto_Utilizada" in df_ped.columns and "Data" in df_ped.columns:
                em = df_ped["empresa"].astype(str).str.strip().eq(empresa)
                d = pd.to_datetime(df_ped["Data"], errors="coerce", dayfirst=True)
                m = em & (d >= pd.Timestamp(data_inicio)) & (d <= pd.Timestamp(data_fim))
                sl = df_ped.loc[m]
                ali_u = pd.to_numeric(sl["Aliquota_Imposto_Utilizada"], errors="coerce").dropna()
                if not ali_u.empty:
                    print(f"  Aliquota_Imposto_Utilizada (distinct): {sorted({round(float(x), 4) for x in ali_u.unique()})}")

        except Exception as exc:
            print(f"  ERRO: {type(exc).__name__}: {exc}")

    print("\n" + "=" * 80)
    print(
        "\nNota: O pipeline nao declara regime tributario; aliquota por linha vem de "
        "params/regra documentada em docs/faturamento_imposto_coluna_base_vs_nota.md."
    )


if __name__ == "__main__":
    main()
