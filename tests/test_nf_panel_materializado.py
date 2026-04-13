"""Painel NF pré-calculado (merge + frete/resultado) na materialização."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import FaturamentoRecorteMinState, build_nf_grain_dataframe
from processing.faturamento.fiscal_materializado import SCHEMA_VERSION_FISCAL
from processing.faturamento.nf_materializado import NF_FIRST_CONTRACT_COLUMNS, build_nf_materializado_dataframe
from processing.faturamento.nf_panel_materializado import (
    NF_PANEL_REQUIRED_COLUMNS,
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


def _fiscal_one_nf(*, frete_nota: float = 122.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "org_id": "o1",
                "empresa": "A",
                "Nota_Numero_Normalizado": "NF1",
                "Nota_Data_Emissao": pd.Timestamp("2025-06-10"),
                "Nota_Situacao": "Autorizada",
                "Valor_Liquido_NF": 752.0,
                "Frete_Nota_Export": frete_nota,
                "Valor_Total_NF": 752.0,
                "schema_version_fiscal": SCHEMA_VERSION_FISCAL,
            }
        ]
    )


def test_build_nf_panel_com_fiscal_receita_igual_frete_nota_export() -> None:
    """Com fiscal, receita = ``Frete_Nota_Export``; repasse = receita (coincide com gap NF×lista) → frete neutro."""
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    panel = build_nf_panel_materializado_dataframe(df_nf, _fiscal_one_nf(frete_nota=122.0))
    assert nf_panel_materializado_dataframe_valid(panel)
    assert abs(float(panel.iloc[0]["receita_frete_tp"]) - 122.0) < 1e-5
    assert abs(float(panel.iloc[0]["repasse_frete_transportadora_propria"]) - 122.0) < 1e-4
    assert abs(float(panel.iloc[0]["custo_frete_plataforma"])) < 1e-4
    assert abs(float(panel.iloc[0]["resultado"]) - (-14.05)) < 1e-4


def test_build_nf_panel_com_fiscal_nao_substitui_receita_pelo_gap_nf_lista() -> None:
    """Gap NF×lista (122) não sobrescreve quando o fiscal traz outro ``Frete_Nota_Export``."""
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    panel = build_nf_panel_materializado_dataframe(df_nf, _fiscal_one_nf(frete_nota=90.0))
    assert abs(float(panel.iloc[0]["receita_frete_tp"]) - 90.0) < 1e-6
    # Receita fiscal (90) ≠ perfil gap (vf−vv=122): coerência imputa repasse=90 → resultado += 90−90;
    # 10 − (630*0.035 + 2) = −14,05
    assert abs(float(panel.iloc[0]["repasse_frete_transportadora_propria"]) - 90.0) < 1e-6
    assert abs(float(panel.iloc[0]["resultado"]) - (-14.05)) < 1e-4


def test_build_nf_panel_sem_fiscal_aplica_gap_e_resultado() -> None:
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    assert not df_nf.empty
    panel = build_nf_panel_materializado_dataframe(df_nf, pd.DataFrame())
    assert nf_panel_materializado_dataframe_valid(panel)
    assert abs(float(panel.iloc[0]["receita_frete_tp"]) - 122.0) < 1e-6
    assert abs(float(panel.iloc[0]["tarifa_custo_envio"])) < 1e-6
    assert abs(float(panel.iloc[0]["repasse_frete_transportadora_propria"]) - 122.0) < 1e-4
    assert abs(float(panel.iloc[0]["custo_frete_plataforma"])) < 1e-6
    # Resultado: base 10 + receita − repasse = 10; ADS = 3,5%×630 + 2 = 24,05 → −14,05
    assert abs(float(panel.iloc[0]["resultado"]) - (-14.05)) < 1e-4
    assert abs(float(panel.iloc[0]["custo_ads_variavel"]) - 22.05) < 1e-5
    assert abs(float(panel.iloc[0]["custo_ads_fixo"]) - 2.0) < 1e-6
    assert abs(float(panel.iloc[0]["custo_ads"]) - 24.05) < 1e-5
    assert bool(panel.iloc[0]["comercial_incompleto"]) is False


def test_build_nf_panel_contract_columns_present() -> None:
    line = _line_one_nf()
    df_nf = build_nf_materializado_dataframe(line)
    panel = build_nf_panel_materializado_dataframe(df_nf, pd.DataFrame())
    assert list(panel.columns) == sorted(NF_PANEL_REQUIRED_COLUMNS)


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
