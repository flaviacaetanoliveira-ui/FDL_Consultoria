"""
Diagnóstico do termômetro de pace — cenário Gama Home · abril/2026 sem Streamlit.

Uso (na raiz do repo):
  python scripts/debug_pace_gama_abril.py

Para abril cheio + ``hoje`` fixo e simulação de relógio fora do mês (mes_corrente):
  python scripts/debug_pace_abril_corrente.py --empresa "Mega Star"
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from processing.faturamento.pace_mensal import (
    compute_pace_mensal,
    compute_trailing_monthly_revenues,
    determinar_modo,
    explicar_motivo_pace_none,
)
from processing.faturamento.resultado_gerencial_slice import build_resultado_gerencial_slice


def main() -> None:
    parquet_path = ROOT / "data_products" / "cliente_2" / "faturamento" / "current" / "dataset.parquet"
    if not parquet_path.is_file():
        print(f"ERRO: Parquet não encontrado: {parquet_path}")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    print(f"Parquet carregado: {len(df)} linhas totais")

    empresas = ("Gama Home",)
    plataformas: tuple[str, ...] = ()
    data_inicio = date(2026, 4, 1)
    data_fim = date(2026, 4, 30)
    hoje = date.today()

    print("\nCenário:")
    print(f"  empresas={list(empresas)}")
    print(f"  data_inicio={data_inicio}")
    print(f"  data_fim={data_fim}")
    print(f"  hoje={hoje}")

    modo_direto = determinar_modo(data_inicio, data_fim, hoje)
    print(f"\ndeterminar_modo() retornou: {modo_direto}")

    try:
        slice_rg = build_resultado_gerencial_slice(
            df,
            empresas_sel=empresas,
            plataformas_sel=plataformas,
            data_venda_ini=data_inicio,
            data_venda_fim=data_fim,
        )
    except Exception as exc:
        print(f"\nERRO ao construir slice: {type(exc).__name__}: {exc}")
        return

    print("\nSlice construído:")
    print(f"  df_linha: {len(slice_rg.df_linha)} linhas")
    print(f"  n_linhas (stats): {slice_rg.stats.n_linhas}")
    print(f"  receita_total: R$ {slice_rg.stats.receita_total:,.2f}")
    cols = list(slice_rg.df_linha.columns)[:12]
    print(f"  primeiras colunas: {cols}")

    mes_ref = (data_fim.year, data_fim.month)
    try:
        historico = compute_trailing_monthly_revenues(
            df,
            empresas_sel=empresas,
            plataformas_sel=plataformas,
            mes_referencia=mes_ref,
        )
        print("\nHistórico MA3 (3 meses antes do mês de referência):")
        print(f"  lista [antigo .. recente]: {[round(x, 2) for x in historico]}")
    except Exception as exc:
        print(f"\nERRO no histórico: {type(exc).__name__}: {exc}")
        historico = []

    hist_pe = {
        "Gama Home": compute_trailing_monthly_revenues(
            df,
            empresas_sel=("Gama Home",),
            plataformas_sel=plataformas,
            mes_referencia=mes_ref,
        )
    }

    try:
        pace = compute_pace_mensal(
            slice_rg,
            historico,
            {},
            list(empresas),
            data_inicio,
            data_fim,
            hoje,
            historico_por_empresa=hist_pe,
        )
    except Exception as exc:
        import traceback

        print(f"\n[FAIL] EXCEÇÃO em compute_pace_mensal:")
        print(f"   {type(exc).__name__}: {exc}")
        traceback.print_exc()
        return

    if pace is None:
        motivo = explicar_motivo_pace_none(
            n_linhas=int(slice_rg.stats.n_linhas),
            data_inicio=data_inicio,
            data_fim=data_fim,
            hoje=hoje,
        )
        print("\n[FAIL] pace retornou None")
        print(f"   motivo (caption admin): {motivo}")
    elif pace.modo == "recorte_parcial":
        print("\n[WARN] pace.modo == 'recorte_parcial' (render omitido na UI)")
        print(f"   datas: {data_inicio} a {data_fim}")
    else:
        print("\n[OK] pace calculado:")
        print(f"   modo: {pace.modo}")
        print(f"   receita_realizada: R$ {pace.receita_realizada:,.2f}")
        print(f"   meta_mensal: {pace.meta_mensal}")
        print(f"   meta_origem: {pace.meta_origem}")
        print(f"   projecao_linear: {pace.projecao_linear}")
        print(f"   nivel_alerta: {pace.nivel_alerta}")


if __name__ == "__main__":
    main()
