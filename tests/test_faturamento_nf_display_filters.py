"""Testes dos filtros plataforma/busca na Tabela por NF (exibição)."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.nf_table_display_filters import nf_table_filter_mask


def test_nf_filter_mask_plataforma_e_busca():
    df = pd.DataFrame(
        {
            "Plataforma": ["ML", "Shopee", "ML"],
            "NF": ["010031", "010032", "010033"],
            "Pedido": ["P1524", "P99", "P1525"],
        }
    )
    m = nf_table_filter_mask(df, plataformas_sel=("ML",), busca="")
    assert m.sum() == 2

    m2 = nf_table_filter_mask(df, plataformas_sel=(), busca="032")
    assert m2.sum() == 1
    assert df.loc[m2, "NF"].iloc[0] == "010032"

    m3 = nf_table_filter_mask(df, plataformas_sel=(), busca="1524")
    assert m3.sum() == 1


def test_nf_filter_mask_busca_escapa_regex():
    df = pd.DataFrame(
        {
            "Plataforma": ["ML"],
            "NF": ["001.02"],
            "Pedido": ["x"],
        }
    )
    m = nf_table_filter_mask(df, plataformas_sel=(), busca="001.02")
    assert m.sum() == 1
