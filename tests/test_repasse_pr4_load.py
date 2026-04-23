"""PR4 — leitura Parquet repasse: flag, contrato, identidade (sem Streamlit)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from processing.repasse_contract import REPASSE_ARTIFACT_FILENAME, REPASSE_REQUIRED_COLUMNS
from processing.repasse_load import (
    coerce_numero_nota_fiscal_text,
    postprocess_repasse_parquet_dataframe,
    read_repasse_parquet,
    repasse_use_parquet_flag,
    validate_repasse_contract_columns,
    validate_repasse_empresa_id_column,
)


def test_flag_off_default() -> None:
    env = {k: v for k, v in {}.items()}
    assert repasse_use_parquet_flag(env, secret_raw="") is False


def test_flag_on_env() -> None:
    assert repasse_use_parquet_flag({"FDL_REPASSE_USE_PARQUET": "1"}) is True
    assert repasse_use_parquet_flag({"FDL_REPASSE_USE_PARQUET": "true"}) is True
    assert repasse_use_parquet_flag({"FDL_REPASSE_USE_PARQUET": "0"}) is False


def test_flag_secret_fallback() -> None:
    assert repasse_use_parquet_flag({}, secret_raw="on") is True


def test_parquet_artifact_filename_matches_contract() -> None:
    assert REPASSE_ARTIFACT_FILENAME == "dataset.parquet"


def test_dynamic_path_segment_convention() -> None:
    """Mesmo padrão que app + materializador: data_products/<slug>/<org>/repasse/current/dataset.parquet"""
    root = "data_products"
    assert f"{root}/cliente_2/gama_home/repasse/current/{REPASSE_ARTIFACT_FILENAME}".endswith(
        "repasse/current/dataset.parquet"
    )


def _minimal_repasse_df(*, org_id: str, drop: str | None = None) -> pd.DataFrame:
    row = {
        "N° de venda": "1",
        "ID do pedido": "p",
        "Total BRL": 1.0,
        "Número da nota": "008821",
        "Valor da nota": 1.0,
        "Plataforma": "ML",
        "Situação": "Pago",
        "Ação sugerida": "Ok",
        "Valor a receber": 1.0,
        "Valor pago": 1.0,
        "Diferença": 0.0,
        "Data de pagamento": "2026-01-01 00:00:00",
        "Data de emissão": "2026-01-01",
        "Data período repasse": "2026-01-01T00:00:00-03:00",
        "empresa": "Gama Home",
        "empresa_id": org_id,
        "cliente_id": "cliente_2",
        "cnpj": pd.NA,
    }
    if drop:
        del row[drop]
    return pd.DataFrame([row])


def test_postprocess_loads_minimal_valid(tmp_path: Path) -> None:
    try:
        import pyarrow  # noqa: F401
    except ImportError:
        pytest.skip("pyarrow não instalado")
    df = _minimal_repasse_df(org_id="gama_home")
    p = tmp_path / "dataset.parquet"
    df.to_parquet(p, index=False, engine="pyarrow")
    loaded = read_repasse_parquet(p)
    out = postprocess_repasse_parquet_dataframe(loaded, "gama_home")
    assert len(out) == 1
    assert out.iloc[0]["Número da nota"] == "008821"


def test_contract_fails_missing_column() -> None:
    df = _minimal_repasse_df(org_id="x", drop="Data período repasse")
    with pytest.raises(ValueError, match="Data período repasse|contrato"):
        validate_repasse_contract_columns(df, REPASSE_REQUIRED_COLUMNS)


def test_identity_fails_wrong_org() -> None:
    df = _minimal_repasse_df(org_id="mega_star")
    with pytest.raises(ValueError, match="empresa_id"):
        validate_repasse_empresa_id_column(df, "gama_home")


def test_identity_fails_missing_empresa_id_column() -> None:
    df = _minimal_repasse_df(org_id="gama_home")
    df = df.drop(columns=["empresa_id"])
    with pytest.raises(ValueError, match="empresa_id"):
        validate_repasse_empresa_id_column(df, "gama_home")


def test_numero_nota_preserves_leading_zeros_string() -> None:
    df = pd.DataFrame([{"Número da nota": "008821"}])
    out = coerce_numero_nota_fiscal_text(df)
    assert out.iloc[0]["Número da nota"] == "008821"


def test_empty_dataframe_still_requires_empresa_id_column() -> None:
    df = pd.DataFrame(columns=list(_minimal_repasse_df(org_id="gama_home").columns))
    validate_repasse_empresa_id_column(df, "gama_home")  # no rows — só coluna
