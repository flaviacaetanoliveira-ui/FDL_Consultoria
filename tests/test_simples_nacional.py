"""Motor fiscal Simples Nacional (LC 123/2006, art. 18 — Anexo I)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.simples_nacional import (
    TABELA_ANEXO_I,
    calcular_aliquota_efetiva_formula,
    calcular_aliquota_efetiva_mes,
    calcular_rbt12_para_competencia,
    extrair_historico_receita_mensal_por_empresa,
    identificar_faixa_anexo_i,
)


def _gerar_historico_mock(total_12m: float, competencia: date = date(2026, 4, 1)) -> dict[date, float]:
    from processing.faturamento.simples_nacional import _rbt12_janela_meses

    meses = _rbt12_janela_meses(competencia)
    if not meses:
        return {}
    v = total_12m / len(meses)
    return {m: v for m in meses}


def test_tabela_anexo_i_tem_6_faixas() -> None:
    assert len(TABELA_ANEXO_I) == 6


def test_identifica_faixa_1_rbt12_baixo() -> None:
    faixa = identificar_faixa_anexo_i(50_000.00)
    assert faixa is not None
    assert faixa.faixa_numero == 1
    assert faixa.aliquota_nominal_pct == 4.00


def test_identifica_faixa_4_rbt12_1M() -> None:
    faixa = identificar_faixa_anexo_i(1_010_000.00)
    assert faixa is not None
    assert faixa.faixa_numero == 4
    assert faixa.aliquota_nominal_pct == 10.70
    assert faixa.parcela_deduzir == 22_500.00


def test_faixa_borda_inferior_720001() -> None:
    """R$ 720.000,01 é borda inferior da faixa 4."""
    faixa = identificar_faixa_anexo_i(720_000.01)
    assert faixa is not None
    assert faixa.faixa_numero == 4


def test_faixa_borda_superior_720000() -> None:
    """R$ 720.000,00 é borda superior da faixa 3."""
    faixa = identificar_faixa_anexo_i(720_000.00)
    assert faixa is not None
    assert faixa.faixa_numero == 3


def test_rbt12_excede_sublimite_retorna_none() -> None:
    faixa = identificar_faixa_anexo_i(5_000_000.00)
    assert faixa is None


def test_formula_exemplo_rbt12_1M_faixa_4() -> None:
    """Exemplo calibrado: RBT12 R$ 1.010.000, faixa 4 → efetiva 8,47%."""
    faixa = identificar_faixa_anexo_i(1_010_000.00)
    assert faixa is not None
    efetiva = calcular_aliquota_efetiva_formula(1_010_000.00, faixa)
    assert efetiva == pytest.approx(8.47, abs=0.01)


def test_formula_rbt12_180k_faixa_1_efetiva_igual_nominal() -> None:
    """Faixa 1 tem parcela deduzir = 0, efetiva = nominal."""
    faixa = identificar_faixa_anexo_i(150_000.00)
    assert faixa is not None
    efetiva = calcular_aliquota_efetiva_formula(150_000.00, faixa)
    assert efetiva == pytest.approx(4.00, abs=0.001)


def test_rbt12_12_meses_completos() -> None:
    historico: dict[date, float] = {}
    for mi in range(1, 13):
        historico[date(2025, mi, 1)] = 50_000.0 + mi * 1_000.0
    rbt12, meses = calcular_rbt12_para_competencia(historico, date(2026, 1, 1))
    assert meses == 12
    assert rbt12 > 0


def test_rbt12_insuficiente_retorna_meses_parciais() -> None:
    historico = {
        date(2025, 10, 1): 50_000.0,
        date(2025, 11, 1): 60_000.0,
        date(2025, 12, 1): 80_000.0,
    }
    rbt12, meses = calcular_rbt12_para_competencia(historico, date(2026, 1, 1))
    assert meses == 3
    assert rbt12 == pytest.approx(190_000.0)


def test_rbt12_sem_historico_retorna_zero_zero() -> None:
    rbt12, meses = calcular_rbt12_para_competencia({}, date(2026, 1, 1))
    assert rbt12 == 0.0
    assert meses == 0


def test_calcular_aliquota_efetiva_mes_rbt12_suficiente() -> None:
    historico = _gerar_historico_mock(total_12m=1_010_000.00)
    resultado = calcular_aliquota_efetiva_mes("gama_home", date(2026, 4, 1), historico)
    assert resultado.rbt12_suficiente is True
    assert resultado.faixa is not None
    assert resultado.faixa.faixa_numero == 4
    assert resultado.aliquota_efetiva_pct == pytest.approx(8.47, abs=0.01)


def test_calcular_aliquota_efetiva_mes_rbt12_insuficiente() -> None:
    historico = {date(2025, 10, 1): 50_000.0}
    resultado = calcular_aliquota_efetiva_mes("gama_home", date(2026, 1, 1), historico)
    assert resultado.rbt12_suficiente is False
    assert resultado.aliquota_efetiva_pct is None
    assert resultado.motivo_indisponivel is not None


def test_calcular_aliquota_excede_sublimite() -> None:
    historico = _gerar_historico_mock(total_12m=5_000_000.00)
    resultado = calcular_aliquota_efetiva_mes("gama_home", date(2026, 4, 1), historico)
    assert resultado.aliquota_efetiva_pct is None
    assert resultado.motivo_indisponivel is not None
    assert "sublimite" in resultado.motivo_indisponivel.lower()


def test_extrair_historico_exclui_cancelada() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["a", "a", "a"],
            "Nota_Data_Emissao": [date(2025, 3, 15), date(2025, 3, 20), date(2025, 4, 1)],
            "Valor_Liquido_NF": [100.0, 50.0, 200.0],
            "Nota_Situacao": ["Autorizada", "Cancelada", "Autorizada"],
        }
    )
    h = extrair_historico_receita_mensal_por_empresa(df, coluna_empresa="empresa_slug")
    assert "a" in h
    assert h["a"][date(2025, 3, 1)] == pytest.approx(100.0)
    assert h["a"][date(2025, 4, 1)] == pytest.approx(200.0)
