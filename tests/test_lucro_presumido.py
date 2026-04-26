"""Testes do motor de Lucro Presumido (federal + ICMS + DIFAL + FCP)."""

from __future__ import annotations

import pandas as pd
import pytest

from processing.faturamento.lucro_presumido import (
    CFOP_INTERESTADUAL_NAO_CONTRIBUINTE,
    CFOP_INTERNO_VENDA,
    IcmsParams,
    LucroPresumidoParams,
    calcular_lucro_presumido,
)


def _df_fiscal(rows: list[dict]) -> pd.DataFrame:
    base = pd.DataFrame(rows)
    if "Nota_Data_Emissao" in base.columns:
        base["Nota_Data_Emissao"] = pd.to_datetime(base["Nota_Data_Emissao"])
    return base


def _base_row(**kwargs) -> dict:
    row = {
        "org_id": "mega_facil",
        "empresa": "Mega Fácil",
        "Nota_Numero_Normalizado": "NF1",
        "Nota_Data_Emissao": "2026-01-10",
        "Nota_Situacao": "Autorizada",
        "Valor_Liquido_NF": 1000.0,
        "Valor_Total_NF": 1000.0,
        "Frete_Nota_Export": 0.0,
        "Nota_UF_Destino": "SP",
        "Nota_CFOP": CFOP_INTERNO_VENDA,
        "Nota_NCM": "9403.30.00",
        "schema_version_fiscal": 3,
    }
    row.update(kwargs)
    return row


def _run(df: pd.DataFrame, **kwargs):
    return calcular_lucro_presumido(
        df,
        org_id="mega_facil",
        nf_d_ini=pd.Timestamp("2026-01-01"),
        nf_d_fim=pd.Timestamp("2026-03-31"),
        **kwargs,
    )


def test_calculo_basico_federal_sem_majoracao() -> None:
    df = _df_fiscal([_base_row(Valor_Liquido_NF=1_000_000.0)])
    out = _run(df, receita_anual_estimada=4_000_000.0)
    assert out.aplicou_majoracao_lc_224 is False
    assert out.receita_bruta == pytest.approx(1_000_000.0)
    assert out.pis_valor == pytest.approx(6_500.0)
    assert out.cofins_valor == pytest.approx(30_000.0)
    assert out.irpj_base == pytest.approx(80_000.0)
    assert out.irpj_valor == pytest.approx(12_000.0)
    assert out.csll_base == pytest.approx(120_000.0)
    assert out.csll_valor == pytest.approx(10_800.0)


def test_calculo_federal_com_majoracao_lc_224() -> None:
    df = _df_fiscal([_base_row(Valor_Liquido_NF=1_000_000.0)])
    out = _run(df, receita_anual_estimada=6_000_000.0)
    # 1/6 da receita no bucket majorado (receita anual acima de 5M sobre 6M)
    irpj_base_esperada = (1_000_000.0 * (5.0 / 6.0) * 0.08) + (1_000_000.0 * (1.0 / 6.0) * 0.088)
    csll_base_esperada = (1_000_000.0 * (5.0 / 6.0) * 0.12) + (1_000_000.0 * (1.0 / 6.0) * 0.132)
    assert out.aplicou_majoracao_lc_224 is True
    assert out.irpj_base == pytest.approx(irpj_base_esperada)
    assert out.csll_base == pytest.approx(csll_base_esperada)


def test_calculo_federal_sem_majoracao_quando_flag_off() -> None:
    df = _df_fiscal([_base_row(Valor_Liquido_NF=1_000_000.0)])
    p = LucroPresumidoParams(aplicar_majoracao_lc_224=False)
    out = _run(df, receita_anual_estimada=10_000_000.0, params=p)
    assert out.aplicou_majoracao_lc_224 is False
    assert out.irpj_base == pytest.approx(80_000.0)
    assert out.csll_base == pytest.approx(120_000.0)


def test_adicional_irpj_acima_60k_trimestre() -> None:
    df = _df_fiscal([_base_row(Valor_Liquido_NF=2_000_000.0)])
    out = _run(df, receita_anual_estimada=4_000_000.0)
    # IRPJ base = 160k; limite trimestre = 60k; excedente=100k; adicional=10k
    assert out.irpj_base == pytest.approx(160_000.0)
    assert out.irpj_adicional_valor == pytest.approx(10_000.0)


def test_icms_interno_sp_moveis_9403() -> None:
    df = _df_fiscal([_base_row(Nota_CFOP=CFOP_INTERNO_VENDA, Valor_Liquido_NF=1_000.0, Nota_NCM="9403.30.00")])
    out = _run(df)
    assert out.icms_interno_base == pytest.approx(1_000.0)
    assert out.icms_interno_valor == pytest.approx(133.0)


def test_icms_interestadual_destino_sul_sudeste() -> None:
    df = _df_fiscal(
        [_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="MG", Valor_Liquido_NF=1_000.0)]
    )
    out = _run(df)
    assert out.icms_interestadual_valor == pytest.approx(120.0)


def test_icms_interestadual_destino_outros() -> None:
    df = _df_fiscal(
        [_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="BA", Valor_Liquido_NF=1_000.0)]
    )
    out = _run(df)
    assert out.icms_interestadual_valor == pytest.approx(70.0)


def test_difal_calculo_basico() -> None:
    df = _df_fiscal(
        [_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="BA", Valor_Liquido_NF=1_000.0)]
    )
    out = _run(df)
    assert out.difal_valor == pytest.approx(110.0)  # 18% - 7%


def test_fcp_rj_aplica_2pct() -> None:
    df = _df_fiscal(
        [_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="RJ", Valor_Liquido_NF=1_000.0)]
    )
    out = _run(df)
    assert out.fcp_valor == pytest.approx(20.0)
    assert out.fcp_ufs_aplicadas == ("RJ",)


def test_fcp_outros_destinos_zero() -> None:
    df = _df_fiscal(
        [_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="MG", Valor_Liquido_NF=1_000.0)]
    )
    out = _run(df)
    assert out.fcp_valor == pytest.approx(0.0)
    assert "MG" in out.fcp_ufs_zeradas


def test_aviso_cfop_desconhecido() -> None:
    df = _df_fiscal([_base_row(Nota_CFOP="6202", Nota_UF_Destino="BA", Valor_Liquido_NF=1_000.0)])
    out = _run(df)
    joined = " | ".join(out.avisos)
    assert "CFOPs não classificados encontrados" in joined
    assert "6202" in joined


def test_aviso_uf_destino_nula() -> None:
    df = _df_fiscal([_base_row(Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="", Valor_Liquido_NF=1000.0)])
    out = _run(df)
    joined = " | ".join(out.avisos)
    assert "UF destino não preenchida" in joined


def test_breakdown_completo_mega_facil_simulado() -> None:
    df = _df_fiscal(
        [
            _base_row(Nota_Numero_Normalizado="NF1", Nota_CFOP=CFOP_INTERNO_VENDA, Nota_NCM="9403.30.00", Valor_Liquido_NF=2_000.0),
            _base_row(
                Nota_Numero_Normalizado="NF2",
                Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE,
                Nota_UF_Destino="RJ",
                Valor_Liquido_NF=3_000.0,
            ),
            _base_row(Nota_Numero_Normalizado="NF3", Nota_CFOP="6202", Nota_UF_Destino="BA", Valor_Liquido_NF=1_000.0),
        ]
    )
    out = _run(df, receita_anual_estimada=6_000_000.0)
    assert out.nfs == 3
    assert out.total_federal > 0
    assert out.total_estadual > 0
    assert out.total_imposto == pytest.approx(out.total_federal + out.total_estadual)
    assert out.aliquota_efetiva > 0


def test_devolucoes_abatem_receita_bruta() -> None:
    df = _df_fiscal([_base_row(Valor_Liquido_NF=1_000_000.0)])
    devol = pd.DataFrame(
        {
            "org_id": ["mega_facil"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-02-15")],
            "Valor_Liquido_Devolucao": [100_000.0],
        }
    )
    out = _run(df, df_devolucoes=devol, receita_anual_estimada=4_000_000.0)
    assert out.receita_devolucoes == pytest.approx(100_000.0)
    assert out.receita_bruta == pytest.approx(900_000.0)


def test_devolucoes_filtradas_por_data_emissao_nao_data_entrada() -> None:
    """Emissão da NF de devolução define o período; Data_Entrada não substitui."""
    df = _df_fiscal([_base_row(Valor_Liquido_NF=1_000_000.0)])
    devol = pd.DataFrame(
        {
            "org_id": ["mega_facil", "mega_facil"],
            "Nota_Data_Emissao": [
                pd.Timestamp("2026-01-15"),
                pd.Timestamp("2026-03-10"),
            ],
            "Data_Entrada": [
                pd.Timestamp("2026-03-20"),
                pd.Timestamp("2026-01-20"),
            ],
            "Valor_Liquido_Devolucao": [1000.0, 2000.0],
        }
    )
    out = calcular_lucro_presumido(
        df,
        df_devolucoes=devol,
        org_id="mega_facil",
        nf_d_ini=pd.Timestamp("2026-01-01"),
        nf_d_fim=pd.Timestamp("2026-01-31"),
        receita_anual_estimada=4_000_000.0,
    )
    assert out.receita_devolucoes == pytest.approx(1000.0)
    assert out.receita_bruta == pytest.approx(999_000.0)


def test_nfs_no_ultimo_dia_apos_meia_noite_sao_incluidas() -> None:
    """NFs do último dia do período com hora > 00:00 entram (recorte por dia civil)."""
    df_fiscal = pd.DataFrame(
        {
            "org_id": ["mega_facil"] * 3,
            "empresa": ["Mega Fácil"] * 3,
            "Nota_Numero_Normalizado": ["001", "002", "003"],
            "Nota_Data_Emissao": [
                pd.Timestamp("2026-03-15 09:00:00"),
                pd.Timestamp("2026-03-31 14:30:00"),
                pd.Timestamp("2026-04-01 08:00:00"),
            ],
            "Nota_Situacao": ["Emitida DANFE"] * 3,
            "Valor_Liquido_NF": [1000.0, 2000.0, 3000.0],
            "Valor_Total_NF": [1000.0, 2000.0, 3000.0],
            "Frete_Nota_Export": [0.0] * 3,
            "Nota_CFOP": ["5102"] * 3,
            "Nota_NCM": ["9403.30.00"] * 3,
            "Nota_UF_Destino": ["SP"] * 3,
            "schema_version_fiscal": [3] * 3,
        }
    )
    breakdown = calcular_lucro_presumido(
        df_fiscal,
        df_devolucoes=None,
        org_id="mega_facil",
        nf_d_ini=pd.Timestamp("2026-03-01"),
        nf_d_fim=pd.Timestamp("2026-03-31"),
        params=LucroPresumidoParams(),
        icms_params=IcmsParams(),
    )
    assert breakdown.nfs == 2, f"Esperava 2 NFs, recebeu {breakdown.nfs}"
    assert abs(breakdown.receita_bruta - 3000.0) < 0.01, (
        f"Esperava receita 3000, recebeu {breakdown.receita_bruta}"
    )


def test_breakdown_inclui_fcp_base_zero_e_aplicado() -> None:
    df = _df_fiscal(
        [
            _base_row(Nota_Numero_Normalizado="NF1", Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="RJ", Valor_Liquido_NF=1000.0),
            _base_row(Nota_Numero_Normalizado="NF2", Nota_CFOP=CFOP_INTERESTADUAL_NAO_CONTRIBUINTE, Nota_UF_Destino="MG", Valor_Liquido_NF=500.0),
        ]
    )
    out = _run(df)
    assert out.fcp_base_aplicado == pytest.approx(1000.0)
    assert out.fcp_base_zero == pytest.approx(500.0)
    assert out.fcp_ufs_aplicadas == ("RJ",)
    assert "MG" in out.fcp_ufs_zeradas

