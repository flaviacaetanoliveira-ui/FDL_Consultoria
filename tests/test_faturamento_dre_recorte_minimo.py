"""Recorte mínimo Etapa 1 — empresa, plataforma, período venda."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoRecorteMinState,
    apply_recorte_minimo,
    compute_vl_nota_fiscal_fiscal_kpi,
    faturamento_min_series_nf_emissao_bounds_dates,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Data": pd.to_datetime(["2025-01-10", "2025-01-20", "2025-02-05"]),
            "Nome da plataforma": ["ML", "Shopee", "ML"],
            "empresa": ["A", "A", "B"],
            "Situação": ["Atendido", "Cancelado", "Atendido"],
        }
    )


def test_empty_empresa_and_plat_is_all_rows() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, w = apply_recorte_minimo(df, st)
    assert len(out) == 3 and not w


def test_empresa_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=("B",),
        plataformas=(),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1 and out.iloc[0]["empresa"] == "B"


def test_plat_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=("Shopee",),
        data_venda_ini=date(2024, 1, 1),
        data_venda_fim=date(2030, 1, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1


def test_date_window() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(
        empresas=(),
        plataformas=(),
        data_venda_ini=date(2025, 1, 15),
        data_venda_fim=date(2025, 2, 1),
    )
    out, _ = apply_recorte_minimo(df, st)
    assert len(out) == 1


def test_all_situacoes_preserved_without_situacao_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState((), (), date(2024, 1, 1), date(2030, 1, 1))
    out, _ = apply_recorte_minimo(df, st)
    assert set(out["Situação"].astype(str)) == {"Atendido", "Cancelado"}


def _df_fiscal() -> pd.DataFrame:
    """Duas linhas mesma NF / org: total 100; uma linha NF cancelada no período."""
    return pd.DataFrame(
        {
            "empresa": ["A", "A", "A"],
            "org_id": ["o1", "o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF1", "NF2"],
            "Nota_Valor_Liquido_Total": [100.0, 100.0, 50.0],
            "Nota_Data_Emissao": pd.to_datetime(
                ["2025-06-10", "2025-06-10", "2025-06-15"]
            ),
            "Nota_Situacao": ["Autorizada", "Autorizada", "Cancelada"],
            "faturamento_nota_vinculada": [True, True, True],
        }
    )


def test_compute_vl_nota_fiscal_dedupes_nf_and_excludes_cancelada() -> None:
    df = _df_fiscal()
    got = compute_vl_nota_fiscal_fiscal_kpi(
        df,
        empresas_sel=(),
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert got == 100.0


def test_compute_vl_nota_fiscal_respects_empresa_only() -> None:
    df = pd.concat(
        [
            _df_fiscal(),
            pd.DataFrame(
                {
                    "empresa": ["B"],
                    "org_id": ["o2"],
                    "Nota_Numero_Normalizado": ["NF9"],
                    "Nota_Valor_Liquido_Total": [999.0],
                    "Nota_Data_Emissao": pd.to_datetime(["2025-06-20"]),
                    "Nota_Situacao": ["Autorizada"],
                    "faturamento_nota_vinculada": [True],
                }
            ),
        ],
        ignore_index=True,
    )
    got = compute_vl_nota_fiscal_fiscal_kpi(
        df,
        empresas_sel=("A",),
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert got == 100.0


def test_faturamento_min_nf_bounds() -> None:
    df = _df_fiscal()
    lo, hi, ok = faturamento_min_series_nf_emissao_bounds_dates(df)
    assert ok is True
    assert lo == date(2025, 6, 10)
    assert hi == date(2025, 6, 15)
