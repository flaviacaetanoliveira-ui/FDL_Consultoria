"""
Diagnóstico do modo mes_corrente do termômetro — reproduz cenário cliente sem Streamlit.

Uso (na raiz do repo):
  python scripts/debug_pace_abril_corrente.py
  python scripts/debug_pace_abril_corrente.py --empresa "Mega Star"
"""

from __future__ import annotations

import argparse
import calendar
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from processing.faturamento.pace_mensal import (
    _is_calendario_mes_cheio,
    compute_pace_mensal,
    compute_trailing_monthly_revenues,
    determinar_modo,
    explicar_motivo_pace_none,
)
from processing.faturamento.resultado_gerencial_slice import build_resultado_gerencial_slice


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--empresa",
        default="Mega Star",
        help="Nome da empresa nas linhas (coluna empresa)",
    )
    args = ap.parse_args()

    parquet_path = ROOT / "data_products" / "cliente_2" / "faturamento" / "current" / "dataset.parquet"
    if not parquet_path.is_file():
        print(f"ERRO: Parquet não encontrado: {parquet_path}")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)
    print(f"Parquet: {len(df)} linhas totais")

    empresas = (args.empresa.strip(),)
    plataformas: tuple[str, ...] = ()
    data_inicio = date(2026, 4, 1)
    data_fim = date(2026, 4, 30)
    hoje = date(2026, 4, 19)

    print("\n=== CENÁRIO ABRIL CORRENTE (fixo) ===")
    print(f"empresas: {list(empresas)}")
    print(f"data_inicio: {data_inicio}")
    print(f"data_fim: {data_fim}")
    print(f"hoje (mock servidor cliente): {hoje}")

    _, ultimo_dia_abril = calendar.monthrange(2026, 4)
    print(f"\n1. Último dia de abril/2026: {ultimo_dia_abril}")
    print(f"   data_fim.day == ultimo_dia_abril? {data_fim.day == ultimo_dia_abril}")

    resultado_mes_cheio = _is_calendario_mes_cheio(data_inicio, data_fim)
    print(f"\n2. _is_calendario_mes_cheio({data_inicio}, {data_fim}): {resultado_mes_cheio}")

    modo = determinar_modo(data_inicio, data_fim, hoje)
    print(f"\n3. determinar_modo(..., hoje={hoje}): {modo}")

    if "empresa" in df.columns:
        df_empresa = df[df["empresa"].astype(str).str.strip() == empresas[0]]
    else:
        df_empresa = df
        print("\n   (aviso: coluna 'empresa' ausente — usando df completo)")

    ts = pd.to_datetime(df_empresa["Data"], errors="coerce", dayfirst=True)
    m_abril = (ts >= pd.Timestamp(data_inicio)) & (ts <= pd.Timestamp(data_fim))
    df_abril = df_empresa.loc[m_abril]
    print(f"\n4. Linhas no período abril/2026 ({empresas[0]}): {len(df_abril)}")
    if len(df_abril) == 0:
        print("   [WARN] SEM DADOS — candidato forte a pace=None por n_linhas=0 no slice")
    else:
        print(f"   Primeira data: {ts[m_abril].min()}")
        print(f"   Última data: {ts[m_abril].max()}")
        col_rec = "Valor total" if "Valor total" in df_abril.columns else "Vl_Venda"
        if col_rec in df_abril.columns:
            print(f"   Soma receita ({col_rec}): {pd.to_numeric(df_abril[col_rec], errors='coerce').fillna(0).sum():,.2f}")

    print("\n5. build_resultado_gerencial_slice(...)")
    try:
        slice_rg = build_resultado_gerencial_slice(
            df,
            empresas_sel=empresas,
            plataformas_sel=plataformas,
            data_venda_ini=data_inicio,
            data_venda_fim=data_fim,
        )
        print(f"   stats.n_linhas: {slice_rg.stats.n_linhas}")
        print(f"   receita_total: R$ {slice_rg.stats.receita_total:,.2f}")
    except Exception as exc:
        print(f"   ERRO: {type(exc).__name__}: {exc}")
        return

    mes_ref = (data_fim.year, data_fim.month)
    print("\n6. compute_trailing_monthly_revenues (consolidado + por empresa)")
    try:
        historico = compute_trailing_monthly_revenues(
            df,
            empresas_sel=empresas,
            plataformas_sel=plataformas,
            mes_referencia=mes_ref,
        )
        print(f"   consolidado [M-3..M-1]: {[round(x, 2) for x in historico]}")
        hist_pe = {
            empresas[0]: compute_trailing_monthly_revenues(
                df,
                empresas_sel=empresas,
                plataformas_sel=plataformas,
                mes_referencia=mes_ref,
            )
        }
        print(f"   por_empresa[{empresas[0]}]: {[round(x, 2) for x in hist_pe[empresas[0]]]}")
    except Exception as exc:
        print(f"   ERRO: {type(exc).__name__}: {exc}")
        historico = []
        hist_pe = {}

    print("\n7. compute_pace_mensal (assinatura real)")
    try:
        pace = compute_pace_mensal(
            slice_rg,
            historico,
            {},
            list(empresas),
            data_inicio,
            data_fim,
            hoje,
            historico_por_empresa=hist_pe or None,
        )
    except Exception as exc:
        import traceback

        print("\n[EXC] Excecao em compute_pace_mensal:")
        traceback.print_exc()
        return

    if pace is None:
        motivo = explicar_motivo_pace_none(
            n_linhas=int(slice_rg.stats.n_linhas),
            data_inicio=data_inicio,
            data_fim=data_fim,
            hoje=hoje,
        )
        print("\n[FAIL] pace == None")
        print(f"   explicar_motivo_pace_none: {motivo}")
    else:
        print("\n[OK] pace calculado:")
        print(f"   modo: {pace.modo}")
        print(f"   receita_realizada: R$ {pace.receita_realizada:,.2f}")
        print(f"   meta_mensal: {pace.meta_mensal}")
        print(f"   meta_origem: {pace.meta_origem}")
        print(f"   projecao_linear: {pace.projecao_linear}")
        print(f"   nivel_alerta: {pace.nivel_alerta}")
        if pace.modo == "recorte_parcial":
            print("\n   [UI] Na app, render do termômetro é omitido quando modo=recorte_parcial")

    print(f"\n8. date.today() nesta maquina: {date.today()} (se fora de abril/2026, a app usa esse relogio em compute_pace_mensal)")

    print("\n9. Simulacao: mesmo slice com hoje em MARCO/2026 (relogio fora do mes do filtro)")
    try:
        pace_mar = compute_pace_mensal(
            slice_rg,
            historico,
            {},
            list(empresas),
            data_inicio,
            data_fim,
            date(2026, 3, 25),
            historico_por_empresa=hist_pe or None,
        )
        assert pace_mar is None
        m = explicar_motivo_pace_none(
            n_linhas=int(slice_rg.stats.n_linhas),
            data_inicio=data_inicio,
            data_fim=data_fim,
            hoje=date(2026, 3, 25),
        )
        print(f"   compute_pace_mensal -> None (esperado)")
        print(f"   explicar_motivo_pace_none: {m}")
    except AssertionError:
        print("   [INESPERADO] pace nao foi None com hoje em marco")


if __name__ == "__main__":
    main()
