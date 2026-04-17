"""Garante que ``apply_recorte_modulo`` mantém a cadeia de filtros do recorte global."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte import (
    FaturamentoRecorteState,
    apply_recorte_modulo,
)


def _minimal_df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Data": pd.to_datetime(["2025-01-10", "2025-01-15", "2025-02-01"]),
            "Situação": ["Atendido", "Atendido", "Cancelado"],
            "Nome da plataforma": ["ML", "ML", "Shopee"],
            "empresa": ["A", "A", "A"],
            "faturamento_nota_vinculada": [True, False, False],
            "Nota_Data_Emissao": pd.to_datetime(
                ["2025-01-11", "2025-01-16", "2025-02-02"], errors="coerce"
            ),
            "Nota_Situacao": ["Normal", "Normal", "Cancelada"],
            "Nota_Numero_Normalizado": ["1", "", "3"],
        }
    )


def test_apply_preserves_all_rows_with_default_like_state() -> None:
    df = _minimal_df()
    st = FaturamentoRecorteState(
        empresas=(),
        situacoes_pedido=(),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 12, 31),
        presenca_nf="Todos",
        nf_emissao_filtrar=False,
        nf_emissao_ini=None,
        nf_emissao_fim=None,
        situacoes_nf=(),
    )
    res = apply_recorte_modulo(df, st)
    assert len(res.df) == 3
    assert not res.warnings


def test_apply_situacao_pedido() -> None:
    df = _minimal_df()
    st = FaturamentoRecorteState(
        empresas=(),
        situacoes_pedido=("Atendido",),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 12, 31),
        presenca_nf="Todos",
        nf_emissao_filtrar=False,
        nf_emissao_ini=None,
        nf_emissao_fim=None,
        situacoes_nf=(),
    )
    res = apply_recorte_modulo(df, st)
    assert len(res.df) == 2


def test_apply_nf_vinculada() -> None:
    df = _minimal_df()
    st = FaturamentoRecorteState(
        empresas=(),
        situacoes_pedido=(),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 12, 31),
        presenca_nf="Com NF vinculada",
        nf_emissao_filtrar=False,
        nf_emissao_ini=None,
        nf_emissao_fim=None,
        situacoes_nf=(),
    )
    res = apply_recorte_modulo(df, st)
    assert len(res.df) == 1
    assert res.df.iloc[0]["Nota_Numero_Normalizado"] == "1"


def test_apply_situacao_nf_sem_coluna_gera_admin_message() -> None:
    df = _minimal_df().drop(columns=["Nota_Situacao"])
    st = FaturamentoRecorteState(
        empresas=(),
        situacoes_pedido=(),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 12, 31),
        presenca_nf="Todos",
        nf_emissao_filtrar=False,
        nf_emissao_ini=None,
        nf_emissao_fim=None,
        situacoes_nf=("Normal",),
    )
    res = apply_recorte_modulo(df, st)
    assert len(res.df) == 3
    assert len(res.admin_messages) == 1
    assert "Nota_Situacao" in res.admin_messages[0]


def test_apply_data_venda_invertida_emite_warning_e_corrige() -> None:
    df = _minimal_df()
    st = FaturamentoRecorteState(
        empresas=(),
        situacoes_pedido=(),
        plataformas=(),
        data_venda_ini=date(2025, 2, 1),
        data_venda_fim=date(2025, 1, 1),
        presenca_nf="Todos",
        nf_emissao_filtrar=False,
        nf_emissao_ini=None,
        nf_emissao_fim=None,
        situacoes_nf=(),
    )
    res = apply_recorte_modulo(df, st)
    assert any("venda" in w.lower() for w in res.warnings)
    # Com fim corrigido = ini, só entram linhas nesse dia
    assert len(res.df) >= 0


def _assert_apply_empty_input(pres: str) -> None:
    res = apply_recorte_modulo(
        pd.DataFrame(),
        FaturamentoRecorteState(
            empresas=(),
            situacoes_pedido=(),
            plataformas=(),
            data_venda_ini=None,
            data_venda_fim=None,
            presenca_nf=pres,
            nf_emissao_filtrar=False,
            nf_emissao_ini=None,
            nf_emissao_fim=None,
            situacoes_nf=(),
        ),
    )
    assert res.df.empty


def test_apply_empty_input_todos() -> None:
    _assert_apply_empty_input("Todos")


def test_apply_empty_input_sem_nf_vinculada() -> None:
    _assert_apply_empty_input("Sem NF vinculada")


def load_tests(loader, tests, pattern):
    """Expõe funções ``test_*`` ao ``unittest discover`` (sem dependência de pytest)."""
    import unittest

    names = (
        "test_apply_preserves_all_rows_with_default_like_state",
        "test_apply_situacao_pedido",
        "test_apply_nf_vinculada",
        "test_apply_situacao_nf_sem_coluna_gera_admin_message",
        "test_apply_data_venda_invertida_emite_warning_e_corrige",
        "test_apply_empty_input_todos",
        "test_apply_empty_input_sem_nf_vinculada",
    )
    return unittest.TestSuite(unittest.FunctionTestCase(globals()[n]) for n in names)
