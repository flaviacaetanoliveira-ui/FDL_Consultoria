"""Testes de ``enriquecer_nfs_com_imposto_calculado``."""

from __future__ import annotations

import logging

import pandas as pd
import pytest

from processing.faturamento.imposto_por_nf import enriquecer_nfs_com_imposto_calculado
from processing.faturamento.lucro_presumido import LucroPresumidoBreakdown


def _minimal_breakdown(tributos: pd.DataFrame) -> LucroPresumidoBreakdown:
    """Breakdown mínimo só com ``tributos_por_nf`` para testes."""
    tot = float(tributos["imposto_total_nf"].sum()) if not tributos.empty else 0.0
    return LucroPresumidoBreakdown(
        receita_bruta=0.0,
        nfs=int(len(tributos)),
        receita_devolucoes=0.0,
        pis_aliquota=0.0,
        pis_valor=0.0,
        cofins_aliquota=0.0,
        cofins_valor=0.0,
        irpj_base=0.0,
        irpj_valor=0.0,
        irpj_adicional_valor=0.0,
        csll_base=0.0,
        csll_valor=0.0,
        total_federal=0.0,
        icms_interno_base=0.0,
        icms_interno_valor=0.0,
        icms_interestadual_base=0.0,
        icms_interestadual_valor=0.0,
        difal_valor=0.0,
        fcp_valor=0.0,
        total_estadual=0.0,
        fcp_base_zero=0.0,
        fcp_base_aplicado=0.0,
        fcp_ufs_aplicadas=(),
        fcp_ufs_zeradas=(),
        total_imposto=tot,
        aliquota_efetiva=0.0,
        aplicou_majoracao_lc_224=False,
        receita_anual_referencia=0.0,
        cfops_outros_base=0.0,
        tributos_por_nf=tributos,
        avisos=(),
    )


def test_nf_sn_recebe_aliquota_do_mes_de_emissao() -> None:
    df = pd.DataFrame(
        [
            {
                "org_id": "acme",
                "Nota_Data_Emissao": pd.Timestamp("2026-03-15"),
                "Valor_Liquido_NF": 10_000.0,
                "Nota_Numero_Normalizado": "NF1",
                "Nota_Situacao": "Emitida",
            }
        ]
    )
    aliq = {"acme": {"2026-03": 0.04}}
    out = enriquecer_nfs_com_imposto_calculado(
        df,
        aliquotas_mensais_sn=aliq,
        breakdowns_lp={},
        org_ids_lp=set(),
    )
    assert out["regime_nf"].iloc[0] == "SN"
    assert float(out["imposto_estimado_nf"].iloc[0]) == pytest.approx(400.0)
    assert float(out["aliquota_mensal_nf"].iloc[0]) == pytest.approx(0.04)


def test_nf_lp_recebe_tributos_individuais_do_breakdown() -> None:
    trib = pd.DataFrame(
        [
            {
                "Nota_Numero_Normalizado": "NF-LP-1",
                "Nota_Data_Emissao": pd.Timestamp("2026-02-01"),
                "Valor_Liquido_NF": 1000.0,
                "pis_nf": 1.0,
                "cofins_nf": 2.0,
                "irpj_nf": 3.0,
                "csll_nf": 4.0,
                "icms_interno_nf": 5.0,
                "icms_interestadual_nf": 6.0,
                "difal_nf": 7.0,
                "fcp_nf": 8.0,
                "imposto_total_nf": 36.0,
            }
        ]
    )
    bd = _minimal_breakdown(trib)
    df = pd.DataFrame(
        [
            {
                "org_id": "mega_facil",
                "Nota_Data_Emissao": pd.Timestamp("2026-02-10"),
                "Valor_Liquido_NF": 1000.0,
                "Nota_Numero_Normalizado": "NF-LP-1",
                "Nota_Situacao": "Emitida DANFE",
            }
        ]
    )
    out = enriquecer_nfs_com_imposto_calculado(
        df,
        aliquotas_mensais_sn={},
        breakdowns_lp={"mega_facil": bd},
        org_ids_lp={"mega_facil"},
    )
    assert out["regime_nf"].iloc[0] == "LP"
    assert float(out["imposto_estimado_nf"].iloc[0]) == pytest.approx(36.0)
    assert float(out["pis_nf"].iloc[0]) == pytest.approx(1.0)
    assert pd.isna(out["aliquota_mensal_nf"].iloc[0])


def test_nf_cancelada_imposto_nao_calculavel() -> None:
    df = pd.DataFrame(
        [
            {
                "org_id": "acme",
                "Nota_Data_Emissao": pd.Timestamp("2026-01-10"),
                "Valor_Liquido_NF": 5000.0,
                "Nota_Numero_Normalizado": "X1",
                "Nota_Situacao": "Cancelada",
            }
        ]
    )
    out = enriquecer_nfs_com_imposto_calculado(
        df,
        aliquotas_mensais_sn={"acme": {"2026-01": 0.1}},
        breakdowns_lp={},
        org_ids_lp=set(),
    )
    assert not bool(out["imposto_calculavel_nf"].iloc[0])
    assert pd.isna(out["imposto_estimado_nf"].iloc[0]) or out["imposto_estimado_nf"].isna().iloc[0]


def test_nf_sn_sem_aliquota_no_mes_recebe_zero_com_warning(caplog: pytest.LogCaptureFixture) -> None:
    caplog.set_level(logging.WARNING)
    df = pd.DataFrame(
        [
            {
                "org_id": "acme",
                "Nota_Data_Emissao": pd.Timestamp("2026-05-01"),
                "Valor_Liquido_NF": 100.0,
                "Nota_Numero_Normalizado": "N1",
                "Nota_Situacao": "Emitida",
            }
        ]
    )
    out = enriquecer_nfs_com_imposto_calculado(
        df,
        aliquotas_mensais_sn={"acme": {"2026-04": 0.05}},
        breakdowns_lp={},
        org_ids_lp=set(),
    )
    assert float(out["imposto_estimado_nf"].iloc[0]) == 0.0
    assert any("alíquota mensal não encontrada" in r.message for r in caplog.records)


def test_dataframe_resultado_preserva_index_e_ordem_original() -> None:
    df = pd.DataFrame(
        [
            {
                "org_id": "b",
                "Nota_Data_Emissao": pd.Timestamp("2026-01-20"),
                "Valor_Liquido_NF": 1.0,
                "Nota_Numero_Normalizado": "B",
                "Nota_Situacao": "Emitida",
            },
            {
                "org_id": "a",
                "Nota_Data_Emissao": pd.Timestamp("2026-01-10"),
                "Valor_Liquido_NF": 2.0,
                "Nota_Numero_Normalizado": "A",
                "Nota_Situacao": "Emitida",
            },
        ],
        index=[31, 7],
    )
    aliq = {
        "a": {"2026-01": 0.1},
        "b": {"2026-01": 0.2},
    }
    out = enriquecer_nfs_com_imposto_calculado(
        df,
        aliquotas_mensais_sn=aliq,
        breakdowns_lp={},
        org_ids_lp=set(),
    )
    assert list(out.index) == [31, 7]
    assert list(out["Nota_Numero_Normalizado"]) == ["B", "A"]
