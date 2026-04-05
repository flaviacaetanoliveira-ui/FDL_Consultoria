"""Coluna materializada Data período repasse (etapa4b)."""
from __future__ import annotations

import pandas as pd

from etapa4b_integracao_contas_receber import _coluna_data_periodo_repasse


def test_periodo_usa_emissao_quando_pagamento_vazio():
    pay = pd.Series([pd.NaT, pd.NaT])
    emi = pd.Series(["2025-06-01", "2026-01-05"])
    out = _coluna_data_periodo_repasse(pay, emi)
    assert "2025-06-01" in out.iloc[0]
    assert "2026-01-05" in out.iloc[1] and "-03:00" in out.iloc[1]


def test_periodo_prioriza_pagamento():
    pay = pd.Series([pd.Timestamp("2026-01-10 12:00:00")])
    emi = pd.Series(["2025-01-01"])
    out = _coluna_data_periodo_repasse(pay, emi)
    assert "2026-01-10" in out.iloc[0]


def test_periodo_vazio_quando_ambos_vazios():
    pay = pd.Series([pd.NaT])
    emi = pd.Series([""])
    out = _coluna_data_periodo_repasse(pay, emi)
    assert out.iloc[0] == ""
