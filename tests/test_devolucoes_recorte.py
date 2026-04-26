"""Testes de ``calcular_devolucoes_fiscais_no_periodo`` (faturamento_dre_recorte)."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from faturamento_dre_recorte import calcular_devolucoes_fiscais_no_periodo


def test_helper_filtra_por_data_emissao_corretamente() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["a", "a"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-01-15"), pd.Timestamp("2026-05-01")],
            "Valor_Liquido_Devolucao": [100.0, 200.0],
        }
    )
    got = calcular_devolucoes_fiscais_no_periodo(
        df,
        chave_empresa="a",
        periodo_inicio=date(2026, 1, 1),
        periodo_fim=date(2026, 1, 31),
    )
    assert got == pytest.approx(100.0)


def test_helper_aceita_org_id_ou_empresa_como_chave() -> None:
    df_org = pd.DataFrame(
        {
            "org_id": ["slug_x"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-02-01")],
            "Valor_Liquido_Devolucao": [50.0],
        }
    )
    assert calcular_devolucoes_fiscais_no_periodo(
        df_org,
        chave_empresa="slug_x",
        periodo_inicio=date(2026, 1, 1),
        periodo_fim=date(2026, 3, 31),
    ) == pytest.approx(50.0)

    df_emp = pd.DataFrame(
        {
            "empresa": ["Nome Bonito"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-02-10")],
            "Valor_Liquido_Devolucao": [77.0],
        }
    )
    assert calcular_devolucoes_fiscais_no_periodo(
        df_emp,
        chave_empresa="Nome Bonito",
        periodo_inicio=date(2026, 1, 1),
        periodo_fim=date(2026, 3, 31),
    ) == pytest.approx(77.0)


def test_helper_retorna_zero_para_empresa_inexistente() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["only_me"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-01-10")],
            "Valor_Liquido_Devolucao": [999.0],
        }
    )
    assert (
        calcular_devolucoes_fiscais_no_periodo(
            df,
            chave_empresa="ghost",
            periodo_inicio=date(2026, 1, 1),
            periodo_fim=date(2026, 1, 31),
        )
        == 0.0
    )


def test_helper_retorna_zero_para_dataframe_vazio() -> None:
    assert (
        calcular_devolucoes_fiscais_no_periodo(
            None,
            chave_empresa="x",
            periodo_inicio=date(2026, 1, 1),
            periodo_fim=date(2026, 1, 31),
        )
        == 0.0
    )
    empty = pd.DataFrame(columns=["org_id", "Nota_Data_Emissao", "Valor_Liquido_Devolucao"])
    assert (
        calcular_devolucoes_fiscais_no_periodo(
            empty,
            chave_empresa="x",
            periodo_inicio=date(2026, 1, 1),
            periodo_fim=date(2026, 1, 31),
        )
        == 0.0
    )


def test_helper_retorna_zero_para_periodo_invalido() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["a"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-01-15")],
            "Valor_Liquido_Devolucao": [100.0],
        }
    )
    assert (
        calcular_devolucoes_fiscais_no_periodo(
            df,
            chave_empresa="a",
            periodo_inicio=date(2026, 2, 1),
            periodo_fim=date(2026, 1, 1),
        )
        == 0.0
    )


def test_helper_respeita_ok_nf_dates_false() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["a"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-01-15")],
            "Valor_Liquido_Devolucao": [100.0],
        }
    )
    assert (
        calcular_devolucoes_fiscais_no_periodo(
            df,
            chave_empresa="a",
            periodo_inicio=date(2026, 1, 1),
            periodo_fim=date(2026, 1, 31),
            ok_nf_dates=False,
        )
        == 0.0
    )
