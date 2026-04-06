"""Painel NF pré-calculado (merge + frete/resultado) na materialização."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import FaturamentoRecorteMinState, build_nf_grain_dataframe
from processing.faturamento.nf_materializado import NF_FIRST_CONTRACT_COLUMNS, build_nf_materializado_dataframe
from processing.faturamento.nf_panel_materializado import (
    build_nf_panel_materializado_dataframe,
    nf_panel_materializado_dataframe_valid,
)


def _line_one_nf() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "empresa": ["A"],
            "org_id": ["o1"],
            "Nota_Numero_Normalizado": ["NF1"],
            "Nota_Valor_Liquido_Total": [752.0],
            "Nota_Data_Emissao": pd.to_datetime(["2025-06-10"]),
            "Nota_Situacao": ["Autorizada"],
            "Quantidade": [1.0],
            "Preço de lista": [630.0],
            "Nome da plataforma": ["ML"],
            "Número do pedido multiloja": ["P1"],
            "Taxa de Comissão": [0.0],
            "Frete_Plataforma": [0.0],
            "Imposto": [0.0],
            "Resultado": [10.0],
            "Descrição": ["X"],
            "faturamento_nota_vinculada": [True],
            "Status_Custo": ["CUSTO_OK"],
        }
    )


def test_build_nf_panel_sem_fiscal_aplica_gap_e_resultado() -> None:
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    assert not df_nf.empty
    panel = build_nf_panel_materializado_dataframe(df_nf, pd.DataFrame())
    assert nf_panel_materializado_dataframe_valid(panel)
    assert abs(float(panel.iloc[0]["frete"]) - 122.0) < 1e-6
    assert abs(float(panel.iloc[0]["resultado"]) - 132.0) < 1e-6
    assert bool(panel.iloc[0]["comercial_incompleto"]) is False


def test_build_nf_panel_contract_columns_present() -> None:
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    panel = build_nf_panel_materializado_dataframe(df_nf, pd.DataFrame())
    exp = sorted(
        {
            "org_id",
            "Nota_Numero_Normalizado",
            "Nota_Data_Emissao",
            "Nota_Situacao",
            "empresa",
            "valor_faturado_nf",
            "valor_venda",
            "diferenca",
            "comissao",
            "custo_produto",
            "frete",
            "imposto",
            "despesa_fixa",
            "resultado",
            "plataforma_resumo",
            "plataforma",
            "pedido_resumo",
            "n_linhas_pedido",
            "produto_resumo",
            "faturamento_nota_vinculada",
            "comercial_incompleto",
        }
    )
    assert list(panel.columns) == exp


def test_nf_materializado_invariante_com_grain() -> None:
    """Colunas numéricas alinhadas ao grão NF (``build_nf_grain_dataframe``)."""
    line = _line_one_nf()
    got = build_nf_materializado_dataframe(line)
    assert list(got.columns) == list(NF_FIRST_CONTRACT_COLUMNS)
    st = FaturamentoRecorteMinState((), ())
    ref, _ = build_nf_grain_dataframe(
        line,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert float(ref.iloc[0]["valor_venda"]) == float(got.iloc[0]["valor_venda"])
