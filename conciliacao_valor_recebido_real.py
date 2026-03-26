from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _tipo_financeiro_final(cat_somas: dict[str, float]) -> str:
    # escolhe a categoria dominante entre as componentes reconhecidas
    prioridade = ["Receita", "Disputa", "Ajuste financeiro", "Frete repassado", "Comissão"]
    valores = {k: abs(v) for k, v in cat_somas.items() if k in prioridade}
    if not valores or max(valores.values()) == 0:
        return "Não identificado"
    return max(prioridade, key=lambda k: valores.get(k, 0.0))


def build_conciliacao_com_recebido_real(base_dir: str | Path) -> pd.DataFrame:
    conc = build_conciliacao_vendas_liberacoes_validas(base_dir).copy()
    _, lib, _, _ = carregar_bases_consolidadas(base_dir)
    lib = lib.copy()

    for c in ("EXTERNAL_REFERENCE", "PACK_ID", "DESCRIPTION", "RECORD_TYPE"):
        if c not in lib.columns:
            lib[c] = ""
        lib[c] = _norm(lib[c])
    lib["Valor pago"] = pd.to_numeric(lib["Valor pago"], errors="coerce").fillna(0.0)

    conc["N° de venda"] = _norm(conc["N° de venda"])

    detalhes = []
    for _, r in conc.iterrows():
        venda = r["N° de venda"]
        chave = str(r["Chave usada"]) if pd.notna(r["Chave usada"]) else ""
        if chave == "EXTERNAL_REFERENCE":
            s = lib[lib["EXTERNAL_REFERENCE"].eq(venda)]
        elif chave == "PACK_ID":
            s = lib[lib["PACK_ID"].eq(venda)]
        else:
            s = lib.iloc[0:0]

        m_receita = (
            s["DESCRIPTION"].str.lower().eq("payment")
            & s["RECORD_TYPE"].str.lower().eq("release")
            & (s["Valor pago"] > 0)
        )
        valor_recebido_real = float(s.loc[m_receita, "Valor pago"].sum()) if not s.empty else 0.0

        desc_lower = s["DESCRIPTION"].str.lower()
        cat_somas = {
            "Receita": float(s.loc[m_receita, "Valor pago"].sum()) if not s.empty else 0.0,
            "Disputa": float(
                s.loc[desc_lower.str.contains("dispute", na=False), "Valor pago"].sum()
            )
            if not s.empty
            else 0.0,
            "Ajuste financeiro": float(
                s.loc[
                    desc_lower.str.contains("reserve_|payout|refund|mediation", regex=True, na=False),
                    "Valor pago",
                ].sum()
            )
            if not s.empty
            else 0.0,
            "Frete repassado": float(
                s.loc[desc_lower.str.contains("shipping|envio|frete", regex=True, na=False), "Valor pago"].sum()
            )
            if not s.empty
            else 0.0,
            "Comissão": float(
                s.loc[desc_lower.str.contains("fee|comissao|comissão", regex=True, na=False), "Valor pago"].sum()
            )
            if not s.empty
            else 0.0,
        }
        tipo = _tipo_financeiro_final(cat_somas)

        detalhes.append(
            {
                "N° de venda": venda,
                "Valor recebido real": valor_recebido_real if valor_recebido_real > 0 else pd.NA,
                "Tipo financeiro final": tipo,
            }
        )

    det = pd.DataFrame(detalhes)
    conc2 = conc.merge(det, how="left", on="N° de venda")
    conc2["Diferença ajustada"] = conc2["Total BRL"] - pd.to_numeric(
        conc2["Valor recebido real"], errors="coerce"
    )
    conc2.loc[conc2["Valor recebido real"].isna(), "Diferença ajustada"] = pd.NA
    return conc2


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc2 = build_conciliacao_com_recebido_real(BASE_DIR)

    # totais solicitados
    total_receita_real = float(pd.to_numeric(conc2["Valor recebido real"], errors="coerce").sum())
    resumo_tipo = (
        conc2.groupby("Tipo financeiro final", as_index=False)
        .agg(
            Quantidade=("N° de venda", "count"),
            Soma_valor_pago=("Valor pago", "sum"),
            Soma_recebido_real=("Valor recebido real", "sum"),
        )
        .sort_values("Quantidade", ascending=False)
    )
    total_linhas = len(conc2)
    resumo_tipo["Percentual"] = (resumo_tipo["Quantidade"] / total_linhas * 100.0) if total_linhas else 0.0

    total_ajustes = float(
        resumo_tipo.loc[resumo_tipo["Tipo financeiro final"].eq("Ajuste financeiro"), "Soma_valor_pago"].sum()
    )
    total_disputa = float(
        resumo_tipo.loc[resumo_tipo["Tipo financeiro final"].eq("Disputa"), "Soma_valor_pago"].sum()
    )

    soma_venda = float(pd.to_numeric(conc2["Total BRL"], errors="coerce").sum())
    diff_vs_venda = soma_venda - float(pd.to_numeric(conc2["Valor pago"], errors="coerce").sum())
    diff_ajustada = soma_venda - total_receita_real

    print("=== CONCILIAÇÃO COM VALOR RECEBIDO REAL ===")
    print(f"Total vendido (Total BRL): {soma_venda:.2f}")
    print(f"Total recebido real: {total_receita_real:.2f}")
    print(f"Diferença vs venda (usando Valor pago): {diff_vs_venda:.2f}")
    print(f"Diferença ajustada (considerando só receita): {diff_ajustada:.2f}")

    print("\nTotais por tipo (solicitado):")
    print(f"- total de receita real: {float(resumo_tipo.loc[resumo_tipo['Tipo financeiro final'].eq('Receita'), 'Soma_recebido_real'].sum()):.2f}")
    print(f"- total de ajustes: {total_ajustes:.2f}")
    print(f"- total de disputa: {total_disputa:.2f}")

    print("\nDistribuição por Tipo financeiro final:")
    print(resumo_tipo.to_string(index=False))

    print("\nHead com novas colunas derivadas:")
    print(
        conc2[
            [
                "N° de venda",
                "Total BRL",
                "Valor pago",
                "Valor recebido real",
                "Tipo financeiro final",
                "Diferença",
                "Diferença ajustada",
            ]
        ]
        .head(12)
        .to_string(index=False)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

