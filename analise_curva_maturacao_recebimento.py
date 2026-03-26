from __future__ import annotations

import sys

import pandas as pd

from analise_vendas_maduras import _build_data_venda_por_venda, _normalize_sale_id
from conciliacao_valor_recebido_real import BASE_DIR, build_conciliacao_com_recebido_real


def _faixa_dias(d: pd.Series) -> pd.Series:
    bins = [-1, 7, 15, 30, 60, 10_000]
    labels = ["0-7 dias", "8-15 dias", "16-30 dias", "31-60 dias", "60+ dias"]
    return pd.cut(d, bins=bins, labels=labels)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_com_recebido_real(BASE_DIR).copy()
    conc["N° de venda"] = _normalize_sale_id(conc["N° de venda"])
    map_data, _ = _build_data_venda_por_venda()
    map_data["N° de venda"] = _normalize_sale_id(map_data["N° de venda"])
    conc = conc.merge(map_data, how="left", on="N° de venda")

    hoje = pd.Timestamp.now().normalize()
    conc["Dias desde a venda"] = (hoje - conc["Data da venda"]).dt.days
    conc["Mes da venda"] = conc["Data da venda"].dt.strftime("%Y-%m")
    conc["Faixa dias"] = _faixa_dias(conc["Dias desde a venda"])

    conc["Total BRL"] = pd.to_numeric(conc["Total BRL"], errors="coerce").fillna(0.0)
    conc["Valor recebido real"] = pd.to_numeric(conc["Valor recebido real"], errors="coerce").fillna(0.0)

    # 1) Tabela por mês da venda
    tab_mes = (
        conc[conc["Mes da venda"].notna()]
        .groupby("Mes da venda", as_index=False)
        .agg(
            total_vendido=("Total BRL", "sum"),
            total_recebido_real=("Valor recebido real", "sum"),
            qtd_vendas=("N° de venda", "count"),
        )
        .sort_values("Mes da venda")
        .reset_index(drop=True)
    )
    tab_mes["percentual_recebido"] = (
        tab_mes["total_recebido_real"] / tab_mes["total_vendido"] * 100.0
    ).where(tab_mes["total_vendido"] > 0, 0.0)

    # 2) Tabela por faixa de dias
    ordem = ["0-7 dias", "8-15 dias", "16-30 dias", "31-60 dias", "60+ dias"]
    tab_faixa = (
        conc[conc["Faixa dias"].notna()]
        .groupby("Faixa dias", as_index=False)
        .agg(
            total_vendido=("Total BRL", "sum"),
            total_recebido_real=("Valor recebido real", "sum"),
            qtd_vendas=("N° de venda", "count"),
        )
    )
    tab_faixa["Faixa dias"] = pd.Categorical(tab_faixa["Faixa dias"], categories=ordem, ordered=True)
    tab_faixa = tab_faixa.sort_values("Faixa dias").reset_index(drop=True)
    tab_faixa["percentual_recebido"] = (
        tab_faixa["total_recebido_real"] / tab_faixa["total_vendido"] * 100.0
    ).where(tab_faixa["total_vendido"] > 0, 0.0)

    # 3) Faixa de estabilização (heurística)
    estabiliza_em = "Não identificado"
    if len(tab_faixa) >= 2:
        pct = tab_faixa["percentual_recebido"].tolist()
        faixas = tab_faixa["Faixa dias"].astype(str).tolist()
        # estabiliza quando incremento absoluto passa a ser <= 2 p.p. por faixa
        for i in range(1, len(pct)):
            if abs(pct[i] - pct[i - 1]) <= 2.0:
                estabiliza_em = faixas[i]
                break
        if estabiliza_em == "Não identificado":
            estabiliza_em = faixas[-1]

    print("=== CURVA DE MATURAÇÃO DE RECEBIMENTO ===")
    print("\n[1] Tabela por mês da venda")
    print(tab_mes.to_string(index=False))

    print("\n[2] Tabela por faixa de dias desde a venda")
    print(tab_faixa.to_string(index=False))

    print("\n[3] Faixa onde começa estabilização do recebimento")
    print(f"- Estabiliza em: {estabiliza_em}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

