from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

from etapa3_conciliacao_multichave import BASE_DIR, build_conciliacao_multichave


KEYWORDS = {
    "Tarifa": ["tarifa", "comissao", "comissão", "fee"],
    "Frete": ["frete", "envio", "shipping"],
    "Ajuste": ["ajuste", "chargeback", "compensacao", "compensação"],
    "Desconhecido": [],
}


def _is_numeric_id(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.fullmatch(r"\d+").fillna(False)


def classificar_tipo_liberacao(df: pd.DataFrame) -> pd.Series:
    ext = df["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    order = df["ORDER_ID"].fillna("").astype(str).str.strip()
    pack = df["PACK_ID"].fillna("").astype(str).str.strip()
    texto = (ext + " " + order + " " + pack).str.lower()

    valor = pd.to_numeric(df["Valor pago"], errors="coerce")
    has_sale_like_id = (
        ext.str.fullmatch(r"\d{16,20}").fillna(False)
        | order.str.fullmatch(r"\d{16,20}").fillna(False)
        | pack.str.fullmatch(r"\d{16,20}").fillna(False)
    )

    tipo = pd.Series("Desconhecido", index=df.index, dtype="object")

    # Palavras-chave explícitas
    tipo[texto.str.contains("|".join(KEYWORDS["Tarifa"]), regex=True, na=False)] = "Tarifa"
    tipo[texto.str.contains("|".join(KEYWORDS["Frete"]), regex=True, na=False)] = "Frete"
    tipo[texto.str.contains("|".join(KEYWORDS["Ajuste"]), regex=True, na=False)] = "Ajuste"
    tipo[texto.str.contains("reembolso|refund", regex=True, na=False)] = "Ajuste"

    # Heurísticas quando não há texto descritivo:
    # - IDs parecidos com venda e valor positivo tendem a ser "Venda"
    tipo[(tipo == "Desconhecido") & has_sale_like_id & (valor > 0)] = "Venda"
    # - valor zero/negativo tende a ser ajuste financeiro
    tipo[(tipo == "Desconhecido") & (valor <= 0)] = "Ajuste"

    return tipo


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conciliacao_multichave = build_conciliacao_multichave(BASE_DIR)
    liberacoes_sem_match = conciliacao_multichave[
        conciliacao_multichave["Match sucesso"].eq("Não")
    ][
        ["EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID", "Valor pago", "Data de pagamento"]
    ].copy()

    # Padrões solicitados
    ext = liberacoes_sem_match["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    order = liberacoes_sem_match["ORDER_ID"].fillna("").astype(str).str.strip()
    pack = liberacoes_sem_match["PACK_ID"].fillna("").astype(str).str.strip()

    total = len(liberacoes_sem_match)
    ext_vazio = int(ext.eq("").sum())
    order_vazio = int(order.eq("").sum())
    pack_vazio = int(pack.eq("").sum())

    ext_nao_num = int((ext.ne("") & ~_is_numeric_id(ext)).sum())
    order_nao_num = int((order.ne("") & ~_is_numeric_id(order)).sum())
    pack_nao_num = int((pack.ne("") & ~_is_numeric_id(pack)).sum())

    formato_diff = int(
        (
            ext.str.len().where(ext.ne(""), 0)
            != order.str.len().where(order.ne(""), 0)
        ).sum()
    )

    texto_busca = (ext + " " + order + " " + pack).str.lower()
    hits = {
        "tarifa": int(texto_busca.str.contains("tarifa", na=False).sum()),
        "comissão/comissao": int(texto_busca.str.contains("comissao|comissão", regex=True, na=False).sum()),
        "envio/frete": int(texto_busca.str.contains("envio|frete", regex=True, na=False).sum()),
        "ajuste": int(texto_busca.str.contains("ajuste", na=False).sum()),
        "reembolso": int(texto_busca.str.contains("reembolso|refund", regex=True, na=False).sum()),
    }

    liberacoes_sem_match["Tipo de liberação"] = classificar_tipo_liberacao(liberacoes_sem_match)
    resumo_tipo = (
        liberacoes_sem_match.groupby("Tipo de liberação", as_index=False)
        .agg(Quantidade=("Tipo de liberação", "count"), Soma_valor_pago=("Valor pago", "sum"))
        .sort_values("Quantidade", ascending=False)
        .reset_index(drop=True)
    )

    print("=== LIBERAÇÕES SEM MATCH ===")
    print(f"Total sem match: {total}")

    print("\n[1] Primeiras 50 linhas")
    print(liberacoes_sem_match.head(50).to_string(index=False))

    print("\n[2] Padrões identificados")
    print(f"- EXTERNAL_REFERENCE vazio: {ext_vazio} ({(ext_vazio/total*100 if total else 0):.2f}%)")
    print(f"- ORDER_ID vazio: {order_vazio} ({(order_vazio/total*100 if total else 0):.2f}%)")
    print(f"- PACK_ID vazio: {pack_vazio} ({(pack_vazio/total*100 if total else 0):.2f}%)")
    print(f"- EXTERNAL_REFERENCE não numérico: {ext_nao_num}")
    print(f"- ORDER_ID não numérico: {order_nao_num}")
    print(f"- PACK_ID não numérico: {pack_nao_num}")
    print(f"- Diferença de formato (len EXTERNAL_REFERENCE vs ORDER_ID): {formato_diff}")

    print("\n- Ocorrência de termos solicitados (campos de chave):")
    for k, v in hits.items():
        print(f"  - {k}: {v}")

    print("\n[3] Quantidade e soma por Tipo de liberação")
    print(resumo_tipo.to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

