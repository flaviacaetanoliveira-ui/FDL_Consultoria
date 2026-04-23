"""Parquet fiscal: Nota_UF_Destino, Nota_CFOP, Nota_NCM a partir do export Bling."""

from __future__ import annotations

import pytest

from processing.faturamento.fiscal_materializado import (
    FISCAL_CONTRACT_COLUMNS,
    SCHEMA_VERSION_FISCAL,
    build_fiscal_notas_from_directory,
    fiscal_contract_dataframe_valid,
)


def test_fiscal_parquet_contract_tem_doze_colunas() -> None:
    assert len(FISCAL_CONTRACT_COLUMNS) == 12


def test_csv_com_uf_cfop_ncm_materializa_campos(tmp_path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Situação;UF;CFOP;NCM\n"
        "1;02/01/2026;100,00;Autorizada;SP;5102;1234.56.78\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o1", empresa="MarcaA")
    assert fiscal_contract_dataframe_valid(df)
    assert list(df.columns) == list(FISCAL_CONTRACT_COLUMNS)
    assert df.iloc[0]["Nota_UF_Destino"] == "SP"
    assert df.iloc[0]["Nota_CFOP"] == "5102"
    assert df.iloc[0]["Nota_NCM"] == "1234.56.78"
    assert int(df.iloc[0]["schema_version_fiscal"]) == SCHEMA_VERSION_FISCAL


def test_csv_sem_uf_cfop_ncm_nao_quebra(tmp_path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "9;03/01/2026;50,00;Autorizada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o1", empresa="MarcaA")
    assert fiscal_contract_dataframe_valid(df)
    assert df.iloc[0]["Nota_UF_Destino"] == ""
    assert df.iloc[0]["Nota_CFOP"] == ""
    assert df.iloc[0]["Nota_NCM"] == ""


def test_mesma_nf_cfop_distinto_emite_warning_e_usa_first(tmp_path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Situação;UF;CFOP;NCM\n"
        "1;02/01/2026;50,00;Autorizada;SP;5102;1111.11.11\n"
        "1;02/01/2026;50,00;Autorizada;SP;6108;1111.11.11\n",
        encoding="utf-8",
    )
    with pytest.warns(UserWarning, match="FDL fiscal"):
        df = build_fiscal_notas_from_directory(tmp_path, org_id="o1", empresa="MarcaA")
    assert len(df) == 1
    assert df.iloc[0]["Nota_CFOP"] in ("5102", "6108")
