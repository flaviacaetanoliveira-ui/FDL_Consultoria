from __future__ import annotations

import sys

import pandas as pd

from analise_vendas_maduras import _build_data_venda_por_venda, _normalize_sale_id
from conciliacao_valor_recebido_real import BASE_DIR, build_conciliacao_com_recebido_real


def _status_mes(pct: float) -> str:
    if pct > 90:
        return "Fechado"
    if pct >= 30:
        return "Em maturação"
    return "Recente"


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

    conc["Total BRL"] = pd.to_numeric(conc["Total BRL"], errors="coerce").fillna(0.0)
    conc["Valor recebido real"] = pd.to_numeric(conc["Valor recebido real"], errors="coerce").fillna(0.0)
    conc["Mes da venda"] = conc["Data da venda"].dt.strftime("%Y-%m")

    hoje = pd.Timestamp.now().normalize()
    conc["Dias desde a venda"] = (hoje - conc["Data da venda"]).dt.days

    total_vendido = float(conc["Total BRL"].sum())
    total_recebido = float(conc["Valor recebido real"].sum())
    pct_recebido = (total_recebido / total_vendido * 100.0) if total_vendido else 0.0

    recebidos = conc[(conc["Valor recebido real"] > 0) & conc["Dias desde a venda"].notna()].copy()
    if recebidos.empty:
        prazo_medio = 0.0
    else:
        prazo_medio = float(
            (recebidos["Dias desde a venda"] * recebidos["Valor recebido real"]).sum()
            / recebidos["Valor recebido real"].sum()
        )

    tabela_mes = (
        conc[conc["Mes da venda"].notna()]
        .groupby("Mes da venda", as_index=False)
        .agg(vendido=("Total BRL", "sum"), recebido=("Valor recebido real", "sum"))
        .sort_values("Mes da venda")
        .reset_index(drop=True)
    )
    tabela_mes["% recebido"] = (
        tabela_mes["recebido"] / tabela_mes["vendido"] * 100.0
    ).where(tabela_mes["vendido"] > 0, 0.0)
    tabela_mes["status"] = tabela_mes["% recebido"].apply(_status_mes)

    # Destaques gerenciais
    meses_fechados = tabela_mes[tabela_mes["status"].eq("Fechado")]["Mes da venda"].tolist()
    meses_maturacao = tabela_mes[tabela_mes["status"].eq("Em maturação")]["Mes da venda"].tolist()
    meses_recentes = tabela_mes[tabela_mes["status"].eq("Recente")]["Mes da venda"].tolist()
    meses_risco = tabela_mes[
        tabela_mes["status"].eq("Em maturação") & (tabela_mes["% recebido"] < 60)
    ]["Mes da venda"].tolist()

    print("=== RESUMO EXECUTIVO — CICLO FINANCEIRO ===")
    print("\n[1] KPI principais")
    print(f"- Total vendido: {total_vendido:.2f}")
    print(f"- Total recebido real: {total_recebido:.2f}")
    print(f"- % recebido: {pct_recebido:.2f}%")
    print(f"- Prazo médio de recebimento (estimado): {prazo_medio:.1f} dias")

    print("\n[2] Tabela final por mês da venda")
    print(tabela_mes.to_string(index=False))

    print("\n[3] Destaques gerenciais")
    print(f"- Meses já fechados (>90%): {', '.join(meses_fechados) if meses_fechados else 'Nenhum'}")
    print(
        f"- Meses em risco (Em maturação com <60%): "
        f"{', '.join(meses_risco) if meses_risco else 'Nenhum'}"
    )
    print(
        f"- Meses ainda em maturação (30%–90%): "
        f"{', '.join(meses_maturacao) if meses_maturacao else 'Nenhum'}"
    )
    print(
        f"- Meses recentes (<30%): {', '.join(meses_recentes) if meses_recentes else 'Nenhum'}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

