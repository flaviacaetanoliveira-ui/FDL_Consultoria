"""Deduplicação ao juntar vários exports de Liberações_ML (evita Valor pago em duplicata)."""

from __future__ import annotations

import pandas as pd

from etapa2_liberacoes import _deduplicar_liberacoes_concatenadas


def test_dedup_remove_linhas_identicas_repetidas() -> None:
    row = {
        "EXTERNAL_REFERENCE": "260402Q2U2K650",
        "ORDER_ID": "2000010334452678",
        "PACK_ID": "",
        "Data de pagamento": pd.Timestamp("2026-04-03 18:00:00"),
        "NET_CREDIT_AMOUNT": 188.91,
        "NET_DEBIT_AMOUNT": 0.0,
        "Valor pago líquido": 188.91,
        "Valor pago": 188.91,
        "RECORD_TYPE": "release",
        "DESCRIPTION": "payment",
    }
    df = pd.DataFrame([row, row])
    out = _deduplicar_liberacoes_concatenadas(df)
    assert len(out) == 1
    assert float(out["Valor pago"].iloc[0]) == 188.91


def test_dedup_mantem_lancamentos_distintos() -> None:
    a = {
        "EXTERNAL_REFERENCE": "A",
        "ORDER_ID": "1",
        "PACK_ID": "",
        "Data de pagamento": pd.Timestamp("2026-04-03 18:00:00"),
        "NET_CREDIT_AMOUNT": 10.0,
        "NET_DEBIT_AMOUNT": 0.0,
        "Valor pago líquido": 10.0,
        "Valor pago": 10.0,
        "RECORD_TYPE": "release",
        "DESCRIPTION": "payment",
    }
    b = {**a, "EXTERNAL_REFERENCE": "B", "Valor pago": 20.0, "Valor pago líquido": 20.0, "NET_CREDIT_AMOUNT": 20.0}
    out = _deduplicar_liberacoes_concatenadas(pd.DataFrame([a, b]))
    assert len(out) == 2
