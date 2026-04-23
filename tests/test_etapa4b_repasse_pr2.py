"""PR2 — contrato repasse no pipeline (etapa4b): pós-montagem e exclusões."""
from __future__ import annotations

import pandas as pd

from etapa4b_integracao_contas_receber import (
    _aplicar_contrato_repasse_pos_montagem,
    _excluir_linhas_fora_conciliacao_repasse,
)
from processing.repasse_contract import REPASSE_ACTION_COLUMN


def _minimal_final_df(**kwargs: object) -> pd.DataFrame:
    row = {
        "N° de venda": "100",
        "ID do pedido": "p1",
        "Total BRL": 10.0,
        "Número da nota": "1",
        "Numero_sem_parcela": "1",
        "Valor da nota": 10.0,
        "Plataforma": "Shopee",
        "Situação": "",
        REPASSE_ACTION_COLUMN: "Ok",
        "Valor a receber": 10.0,
        "Valor pago": 10.0,
        "Diferença": 0.0,
        "Data de pagamento": "2026-01-01 10:00:00",
        "Data de emissão": "2026-01-01",
        "Data período repasse": "2026-01-01T00:00:00-03:00",
    }
    row.update(kwargs)
    return pd.DataFrame([row])


def test_sem_numero_venda_nao_aparece_no_final() -> None:
    df = _minimal_final_df(**{"N° de venda": "  "})
    df = pd.concat([df, _minimal_final_df(**{"N° de venda": "200"})], ignore_index=True)
    out = _aplicar_contrato_repasse_pos_montagem(df, empresa_label=None)
    assert len(out) == 1
    assert str(out.iloc[0]["N° de venda"]).strip() == "200"


def test_data_periodo_repasse_presente_apos_pos_montagem() -> None:
    out = _aplicar_contrato_repasse_pos_montagem(_minimal_final_df(), empresa_label=None)
    assert "Data período repasse" in out.columns
    assert out.iloc[0]["Data período repasse"] != ""


def test_acao_sugerida_coluna_canonica_presente() -> None:
    out = _aplicar_contrato_repasse_pos_montagem(_minimal_final_df(), empresa_label=None)
    assert REPASSE_ACTION_COLUMN in out.columns


def test_excluir_ml_sem_nf_quando_outro_ml_tem_nf() -> None:
    df = pd.DataFrame(
        [
            {
                "N° de venda": "1",
                "Plataforma": "Mercado Livre",
                "Número da nota": "",
                "Total BRL": 50.0,
            },
            {
                "N° de venda": "2",
                "Plataforma": "Mercado Livre",
                "Número da nota": "99",
                "Total BRL": 60.0,
            },
        ]
    )
    out = _excluir_linhas_fora_conciliacao_repasse(df)
    assert len(out) == 1
    assert str(out.iloc[0]["N° de venda"]) == "2"


def test_excluir_taxa_residual_362() -> None:
    df = pd.DataFrame(
        [
            {"N° de venda": "1", "Plataforma": "Mercado Livre", "Número da nota": "1", "Total BRL": 3.62},
            {"N° de venda": "2", "Plataforma": "Mercado Livre", "Número da nota": "2", "Total BRL": 100.0},
        ]
    )
    out = _excluir_linhas_fora_conciliacao_repasse(df)
    assert len(out) == 1
    assert float(out.iloc[0]["Total BRL"]) == 100.0


def test_empresa_label_usado_na_coluna_empresa() -> None:
    out = _aplicar_contrato_repasse_pos_montagem(
        _minimal_final_df(),
        empresa_label="Gama Home",
    )
    assert len(out) == 1
    assert out.iloc[0]["empresa"] == "Gama Home"


def test_normalizar_acao_baixar_no_bling_para_baixado_com_sem_bling(monkeypatch) -> None:
    import etapa4b_integracao_contas_receber as m

    monkeypatch.setenv("FDL_REPASSE_SEM_BLING", "1")
    s = pd.Series(["Baixar no Bling", "Ok"])
    out = m._normalizar_acao_sugerida_canonica(s)
    assert out.tolist() == ["Baixado", "Ok"]
