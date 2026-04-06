"""Deduplicação de linhas brutas na aba Renda antes do groupby Shopee (evita Valor pago 2×)."""

from __future__ import annotations

import pandas as pd

from etapa3_conciliacao_vendas_liberacoes_validas import _dedup_shopee_liberacao_linhas


def test_colapsa_duas_linhas_mesmo_pedido_dia_valor() -> None:
    part = pd.DataFrame(
        {
            "N° de venda": ["P1", "P1"],
            "Data de pagamento": pd.to_datetime(["2026-01-05 18:00", "2026-01-05 12:00"]),
            "Valor pago": [153.44, 153.44],
        }
    )
    out = _dedup_shopee_liberacao_linhas(part)
    assert len(out) == 1
    assert float(out["Valor pago"].iloc[0]) == 153.44


def test_mantem_dois_lancamentos_valores_diferentes() -> None:
    part = pd.DataFrame(
        {
            "N° de venda": ["P1", "P1"],
            "Data de pagamento": pd.to_datetime(["2026-01-05", "2026-01-05"]),
            "Valor pago": [50.0, 30.0],
        }
    )
    out = _dedup_shopee_liberacao_linhas(part)
    assert len(out) == 2
