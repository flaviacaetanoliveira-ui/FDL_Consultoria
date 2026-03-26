from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def _cobertura_por_chave(
    liberacoes: pd.DataFrame, vendas_ids: set[str], chave: str
) -> tuple[pd.DataFrame, dict[str, float]]:
    base = liberacoes.copy()
    key_series = base[chave].fillna("").astype(str).str.strip()
    tem_match = key_series.isin(vendas_ids) & key_series.ne("")

    base["Tem venda"] = tem_match.map({True: "Sim", False: "Não"})

    total = int(len(base))
    com_venda = int(tem_match.sum())
    sem_venda = int(total - com_venda)
    cobertura = float((com_venda / total * 100.0) if total else 0.0)
    resumo = {
        "total_liberacoes": total,
        "liberacoes_com_venda": com_venda,
        "liberacoes_sem_venda": sem_venda,
        "percentual_cobertura": cobertura,
    }
    return base, resumo


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(BASE_DIR)
    vendas_ids = set(vendas_tratadas["N° de venda"].fillna("").astype(str).str.strip())

    # Testes separados
    t1_df, t1 = _cobertura_por_chave(liberacoes_tratadas, vendas_ids, "EXTERNAL_REFERENCE")
    t2_df, t2 = _cobertura_por_chave(liberacoes_tratadas, vendas_ids, "ORDER_ID")
    t3_df, t3 = _cobertura_por_chave(liberacoes_tratadas, vendas_ids, "PACK_ID")

    # Resumo de casamentos exclusivos / múltiplos por linha de liberação
    m1 = t1_df["Tem venda"].eq("Sim")
    m2 = t2_df["Tem venda"].eq("Sim")
    m3 = t3_df["Tem venda"].eq("Sim")
    qtd_matches = m1.astype(int) + m2.astype(int) + m3.astype(int)

    so_external = int((m1 & ~m2 & ~m3).sum())
    so_order = int((~m1 & m2 & ~m3).sum())
    so_pack = int((~m1 & ~m2 & m3).sum())
    mais_de_uma = int((qtd_matches >= 2).sum())

    print("=== DIAGNÓSTICO COMPARATIVO DE CHAVES (LIBERAÇÕES -> VENDAS) ===")

    print("\n[TESTE 1] EXTERNAL_REFERENCE = N° de venda")
    print(f"Total de liberações: {t1['total_liberacoes']}")
    print(f"Liberações com venda: {t1['liberacoes_com_venda']}")
    print(f"Liberações sem venda: {t1['liberacoes_sem_venda']}")
    print(f"Percentual de cobertura: {t1['percentual_cobertura']:.2f}%")

    print("\n[TESTE 2] ORDER_ID = N° de venda")
    print(f"Total de liberações: {t2['total_liberacoes']}")
    print(f"Liberações com venda: {t2['liberacoes_com_venda']}")
    print(f"Liberações sem venda: {t2['liberacoes_sem_venda']}")
    print(f"Percentual de cobertura: {t2['percentual_cobertura']:.2f}%")

    print("\n[TESTE 3] PACK_ID = N° de venda")
    print(f"Total de liberações: {t3['total_liberacoes']}")
    print(f"Liberações com venda: {t3['liberacoes_com_venda']}")
    print(f"Liberações sem venda: {t3['liberacoes_sem_venda']}")
    print(f"Percentual de cobertura: {t3['percentual_cobertura']:.2f}%")

    print("\n[RESUMO ADICIONAL]")
    print(f"Casam só por EXTERNAL_REFERENCE: {so_external}")
    print(f"Casam só por ORDER_ID: {so_order}")
    print(f"Casam só por PACK_ID: {so_pack}")
    print(f"Casam por mais de uma chave: {mais_de_uma}")

    cols = [
        "EXTERNAL_REFERENCE",
        "ORDER_ID",
        "PACK_ID",
        "Data de pagamento",
        "Valor pago",
        "Tem venda",
    ]

    print("\n[AMOSTRA 20 SEM MATCH - TESTE 1 (EXTERNAL_REFERENCE)]")
    print(t1_df.loc[t1_df["Tem venda"].eq("Não"), cols].head(20).to_string(index=False))

    print("\n[AMOSTRA 20 SEM MATCH - TESTE 2 (ORDER_ID)]")
    print(t2_df.loc[t2_df["Tem venda"].eq("Não"), cols].head(20).to_string(index=False))

    print("\n[AMOSTRA 20 SEM MATCH - TESTE 3 (PACK_ID)]")
    print(t3_df.loc[t3_df["Tem venda"].eq("Não"), cols].head(20).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

