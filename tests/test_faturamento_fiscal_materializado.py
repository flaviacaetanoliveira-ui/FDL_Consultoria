"""Materializado fiscal (grão NF a partir de notas de saída)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from processing.faturamento.fiscal_materializado import (
    SCHEMA_VERSION_FISCAL,
    build_fiscal_notas_from_directory,
    fiscal_contract_dataframe_valid,
)


def test_build_fiscal_one_nf(tmp_path: Path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "42;02/01/2026;250,50;Autorizada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="org1", empresa="MarcaA")
    assert fiscal_contract_dataframe_valid(df)
    assert len(df) == 1
    assert df.iloc[0]["Nota_Numero_Normalizado"] == "42"
    assert abs(float(df.iloc[0]["Valor_Liquido_NF"]) - 250.5) < 0.01
    assert abs(float(df.iloc[0]["Frete_Nota_Export"])) < 1e-9
    assert int(df.iloc[0]["schema_version_fiscal"]) == SCHEMA_VERSION_FISCAL


def test_build_fiscal_soma_frete_coluna_export(tmp_path: Path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Frete;Situação\n"
        "99;05/01/2026;339,80;98,80;Autorizada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o", empresa="E")
    assert len(df) == 1
    assert abs(float(df.iloc[0]["Valor_Liquido_NF"]) - 339.80) < 0.02
    assert abs(float(df.iloc[0]["Frete_Nota_Export"]) - 98.80) < 0.02


def test_build_fiscal_groups_duplicate_nf_rows(tmp_path: Path) -> None:
    p = tmp_path / "n.csv"
    p.write_text(
        "Número;Data de emissão;Valor total líquido;Situação\n"
        "10;03/01/2026;100,00;Autorizada\n"
        "10;03/01/2026;50,00;Autorizada\n",
        encoding="utf-8",
    )
    df = build_fiscal_notas_from_directory(tmp_path, org_id="o", empresa="E")
    assert len(df) == 1
    assert abs(float(df.iloc[0]["Valor_Liquido_NF"]) - 150.0) < 0.01


def test_fiscal_contract_rejects_empty() -> None:
    assert not fiscal_contract_dataframe_valid(pd.DataFrame())
