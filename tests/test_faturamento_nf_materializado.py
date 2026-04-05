"""Geração do materializado NF-first (contrato)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import FaturamentoRecorteMinState, build_nf_grain_dataframe
from processing.faturamento.nf_materializado import (
    NF_FIRST_CONTRACT_COLUMNS,
    SCHEMA_VERSION_NF_FIRST,
    build_nf_materializado_dataframe,
    nf_first_contract_dataframe_valid,
)


def _line_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF1"],
            "Nota_Valor_Liquido_Total": [100.0, 100.0],
            "Nota_Data_Emissao": pd.to_datetime(["2025-06-10", "2025-06-10"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Quantidade": [2.0, 1.0],
            "Preço de lista": [10.0, 5.0],
            "Nome da plataforma": ["ML", "ML"],
            "Número do pedido multiloja": ["P1", "P1"],
            "Taxa de Comissão": [1.0, 2.0],
            "Custo_Produto_Total": [4.0, 6.0],
            "Frete_Plataforma": [0.5, 0.5],
            "Imposto": [3.0, 3.0],
            "Resultado": [10.0, -5.0],
            "Descrição": ["X", "Y"],
            "faturamento_nota_vinculada": [True, True],
        }
    )


def test_build_nf_materializado_matches_grain_and_contract() -> None:
    line = _line_df()
    got = build_nf_materializado_dataframe(line)
    assert nf_first_contract_dataframe_valid(got)
    assert list(got.columns) == list(NF_FIRST_CONTRACT_COLUMNS)
    assert int(got.iloc[0]["schema_version_nf"]) == SCHEMA_VERSION_NF_FIRST
    assert got.iloc[0]["plataforma"] == "ML"
    assert float(got.iloc[0]["valor_venda"]) == 25.0
    assert float(got.iloc[0]["custo_produto"]) == 10.0
    assert float(got.iloc[0]["despesa_fixa"]) == 1.25
    st = FaturamentoRecorteMinState((), ())
    ref, _ = build_nf_grain_dataframe(
        line,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert float(ref.iloc[0]["valor_venda"]) == float(got.iloc[0]["valor_venda"])
    assert float(ref.iloc[0]["resultado"]) == float(got.iloc[0]["resultado"])


def test_nf_first_contract_rejects_empty() -> None:
    empty = pd.DataFrame(columns=list(NF_FIRST_CONTRACT_COLUMNS))
    assert not nf_first_contract_dataframe_valid(empty)
