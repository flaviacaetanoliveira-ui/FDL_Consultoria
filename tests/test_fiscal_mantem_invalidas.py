"""Contrato fiscal schema ≥2: Parquet mantém inválidas; base tributável exclui no slice."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from app.components.apuracao_fiscal_panel import (
    _agregar_invalidas_por_tipo_no_periodo,
    _classificar_nf_invalida_por_situacao,
    _count_nf_canceladas_periodo,
)
from faturamento_dre_recorte_minimo import build_faturamento_fiscal_base_slice
from processing.faturamento.fiscal_materializado import (
    SCHEMA_VERSION_FISCAL,
    build_fiscal_notas_from_directory,
)


def test_classificar_invalida_por_substring() -> None:
    assert _classificar_nf_invalida_por_situacao("Cancelada pelo emitente") == "cancel"
    assert _classificar_nf_invalida_por_situacao("Denegada") == "deneg"
    assert _classificar_nf_invalida_por_situacao("Inutilizada") == "inutil"


def test_build_fiscal_mantem_cancelada_quando_flag_padrao(tmp_path) -> None:
    (tmp_path / "n.csv").write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "77;02/01/2026;123,45;Cancelada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o1", empresa="MarcaA")
    assert len(df) == 1
    assert "cancel" in str(df.iloc[0]["Nota_Situacao"]).lower()
    assert int(df.iloc[0]["schema_version_fiscal"]) == SCHEMA_VERSION_FISCAL


def test_build_fiscal_manter_invalidas_false_remove_cancelada(tmp_path) -> None:
    (tmp_path / "n.csv").write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "77;02/01/2026;123,45;Cancelada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(
        tmp_path, org_id="o1", empresa="MarcaA", manter_invalidas=False
    )
    assert df.empty


def test_build_fiscal_mantem_denegada(tmp_path) -> None:
    (tmp_path / "n.csv").write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "88;03/01/2026;50,00;Denegada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o1", empresa="MarcaA")
    assert len(df) == 1
    assert "deneg" in str(df.iloc[0]["Nota_Situacao"]).lower()


def test_slice_exclui_invalidas_do_total_mantendo_base() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["o1", "o1"],
            "empresa": ["Acme", "Acme"],
            "Nota_Numero_Normalizado": ["A", "B"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-03-10"), pd.Timestamp("2026-03-11")],
            "Nota_Situacao": ["Cancelada", "Autorizada"],
            "Valor_Liquido_NF": [200.0, 100.0],
        }
    )
    _, st = build_faturamento_fiscal_base_slice(
        df,
        empresas_sel=("Acme",),
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
        ok_nf_dates=True,
    )
    assert st.valor_liquido_fiscal_sum == 100.0
    assert st.valor_cancelado == 200.0
    assert st.valor_liquido_nf_periodo_todas_situacoes == 300.0
    assert st.valor_liquido_nf_periodo_todas_situacoes == st.valor_faturado_nf + st.valor_cancelado
    assert st.base_fiscal_liquida == st.valor_faturado_nf


def test_contagem_canceladas_periodo_maior_zero() -> None:
    df = pd.DataFrame(
        {
            "empresa": ["Acme"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-03-10")],
            "Nota_Situacao": ["Cancelada"],
            "Nota_Numero_Normalizado": ["Z1"],
            "Valor_Liquido_NF": [1.0],
        }
    )
    n = _count_nf_canceladas_periodo(
        df,
        empresas_sel=("Acme",),
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
        ok_nf_dates=True,
    )
    assert n == 1


def test_agregar_invalidas_tres_tipos() -> None:
    df = pd.DataFrame(
        {
            "org_id": ["o1", "o1", "o1"],
            "empresa": ["Acme", "Acme", "Acme"],
            "Nota_Numero_Normalizado": ["C1", "D1", "I1"],
            "Nota_Data_Emissao": [
                pd.Timestamp("2026-03-05"),
                pd.Timestamp("2026-03-06"),
                pd.Timestamp("2026-03-07"),
            ],
            "Nota_Situacao": ["Cancelada", "Denegada", "Inutilizada"],
            "Valor_Liquido_NF": [10.0, 20.0, 0.0],
        }
    )
    (nc, vc), (nd, vd), ni = _agregar_invalidas_por_tipo_no_periodo(
        df,
        empresas_sel=("Acme",),
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
        ok_nf_dates=True,
    )
    assert nc == 1 and vc == pytest.approx(10.0)
    assert nd == 1 and vd == pytest.approx(20.0)
    assert ni == 1


def test_composicao_base_fecha_com_devolucoes() -> None:
    df_f = pd.DataFrame(
        {
            "org_id": ["o1", "o1"],
            "empresa": ["Acme", "Acme"],
            "Nota_Numero_Normalizado": ["N1", "N2"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-03-15"), pd.Timestamp("2026-03-16")],
            "Nota_Situacao": ["Autorizada", "Cancelada"],
            "Valor_Liquido_NF": [1000.0, 100.0],
        }
    )
    df_d = pd.DataFrame(
        {
            "empresa": ["Acme"],
            "Nota_Data_Emissao": [pd.Timestamp("2026-03-20")],
            "Valor_Liquido_Devolucao": [50.0],
        }
    )
    _, st = build_faturamento_fiscal_base_slice(
        df_f,
        empresas_sel=("Acme",),
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
        ok_nf_dates=True,
        df_devolucoes=df_d,
    )
    esperado = st.valor_liquido_nf_periodo_todas_situacoes - st.valor_cancelado - st.total_devolvido
    assert pytest.approx(st.base_fiscal_liquida) == esperado
