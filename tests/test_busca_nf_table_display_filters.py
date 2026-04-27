"""Regressão da busca na tabela NF (nf_table_filter_mask)."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.nf_table_display_filters import nf_table_filter_mask


def test_modo_legacy_so_nf_busca_match() -> None:
    df = pd.DataFrame(
        {
            "NF": ["12345"],
            "Plataforma": ["—"],
        }
    )
    m = nf_table_filter_mask(df, plataformas_sel=[], busca="123")
    assert bool(m.iloc[0])


def test_modo_gerencial_nf_ou_pedido() -> None:
    df = pd.DataFrame(
        {
            "NF": ["999"],
            "Pedido": ["PED-ZZ"],
            "Plataforma": ["ML"],
        }
    )
    m_nf = nf_table_filter_mask(df, plataformas_sel=[], busca="999")
    m_ped = nf_table_filter_mask(df, plataformas_sel=[], busca="PED-ZZ")
    assert bool(m_nf.iloc[0]) and bool(m_ped.iloc[0])


def test_modo_devolucao_num_nome_documento() -> None:
    df = pd.DataFrame(
        {
            "Nº NF entrada": ["001234"],
            "Nome destinatário": ["Fulano Silva"],
            "CPF/CNPJ destinatário": ["12345678901"],
            "Plataforma": ["—"],
        }
    )
    assert bool(
        nf_table_filter_mask(df, plataformas_sel=[], busca="001234").iloc[0]
    )
    assert bool(
        nf_table_filter_mask(df, plataformas_sel=[], busca="Fulano").iloc[0]
    )
    assert bool(
        nf_table_filter_mask(df, plataformas_sel=[], busca="78901").iloc[0]
    )


def test_busca_vazio_mascara_toda_true() -> None:
    df = pd.DataFrame(
        {
            "NF": ["a", "b"],
            "Pedido": ["", ""],
            "Plataforma": ["x", "y"],
        }
    )
    m = nf_table_filter_mask(df, plataformas_sel=[], busca="")
    assert m.all()


def test_nf_ausente_usa_num_nf_entrada() -> None:
    df = pd.DataFrame(
        {
            "Nº NF entrada": ["5566"],
            "CPF/CNPJ destinatário": ["—"],
            "Nome destinatário": ["—"],
            "Plataforma": ["—"],
        }
    )
    m = nf_table_filter_mask(df, plataformas_sel=[], busca="5566")
    assert bool(m.iloc[0])
