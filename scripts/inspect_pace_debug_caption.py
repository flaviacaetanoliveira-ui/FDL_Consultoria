"""
Simula a chamada ao pace como na app (filtro abril cheio, date.today real),
mostrando o texto alinhado aos captions admin (FDL_RG_PACE_DEBUG).

Uso (na raiz do repo):
  python scripts/inspect_pace_debug_caption.py
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd

from processing.faturamento.ficha_pedido_rg import load_resultado_gerencial_config
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
        print(f"ERRO: Parquet nao encontrado: {parquet_path}")
        sys.exit(1)

    df = pd.read_parquet(parquet_path)

    empresas_list = ["Gama Home"]
    emp_tuple = tuple(empresas_list)
    plats: tuple[str, ...] = ()
    data_inicio = date(2026, 4, 1)
    data_fim = date(2026, 4, 30)
    hoje = date.today()

    print("=== SIMULACAO DO CAPTION ADMIN ===\n")
    print(f"Filtro: {empresas_list[0]} | {data_inicio} a {data_fim}")
    print(f"date.today() no servidor: {hoje}")
    print(f"hoje.month == data_inicio.month? {hoje.month == data_inicio.month}")
    print(f"hoje.year == data_inicio.year? {hoje.year == data_inicio.year}")

    modo_pre = determinar_modo(data_inicio, data_fim, hoje)
    print(f"\ndeterminar_modo(): {modo_pre}")

    slice_rg = build_resultado_gerencial_slice(
        df,
        empresas_sel=emp_tuple,
        plataformas_sel=plats,
        data_venda_ini=data_inicio,
        data_venda_fim=data_fim,
    )
    n_linhas = int(slice_rg.stats.n_linhas)
    print(f"\nslice stats.n_linhas: {n_linhas}")

    mes_ref = (data_fim.year, data_fim.month)
    historico = compute_trailing_monthly_revenues(
        df,
        empresas_sel=emp_tuple,
        plataformas_sel=plats,
        mes_referencia=mes_ref,
    )
    hist_pe: dict[str, list[float]] = {}
    for emp in empresas_list:
        hist_pe[str(emp)] = compute_trailing_monthly_revenues(
            df,
            empresas_sel=(str(emp),),
            plataformas_sel=plats,
            mes_referencia=mes_ref,
        )

    rg_conf = load_resultado_gerencial_config(None)

    print("\n--- Chamada compute_pace_mensal (assinatura igual a app_operacional.py) ---")
    try:
        pace = compute_pace_mensal(
            slice_rg,
            historico,
            rg_conf,
            empresas_list,
            data_inicio,
            data_fim,
            hoje,
            historico_por_empresa=hist_pe,
        )
    except Exception as exc:
        print(f"\n>>> EXCECAO: {type(exc).__name__}: {exc}")
        print(f">>> Caption admin exibiria: pace debug: excecao: {type(exc).__name__}: {exc}")
        return

    if pace is None:
        motivo = explicar_motivo_pace_none(
            n_linhas=n_linhas,
            data_inicio=data_inicio,
            data_fim=data_fim,
            hoje=hoje,
        )
        print("\n>>> pace == None")
        print(f">>> Caption admin (_pace_log_motivo): {motivo}")
    elif pace.modo == "recorte_parcial":
        print("\n>>> pace OK mas modo recorte_parcial (render omitido na UI)")
        print(
            ">>> Caption admin exibiria: "
            f"render omitido | modo=recorte_parcial | ini={data_inicio.isoformat()} | "
            f"fim={data_fim.isoformat()}"
        )
    else:
        print(f"\n>>> pace calculado OK, modo={pace.modo}")
        print(f">>> Caption admin exibiria: renderizado | modo={pace.modo}")

    print(
        "\n(notas: captions com emoji no browser dependem do Streamlit; "
        "mensagem acima espelha o texto em _pace_log_motivo.)"
    )


if __name__ == "__main__":
    main()
