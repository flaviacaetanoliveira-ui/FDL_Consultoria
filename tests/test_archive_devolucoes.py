"""Archive parcial de ``dataset_faturamento_devolucoes.parquet`` antes de rematerializar."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from processing.materialize_financeiro import _archive_current_se_existe


def _write_min_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, engine="pyarrow", index=False)


@pytest.fixture
def faturamento_layout(tmp_path: Path) -> Path:
    """``base/faturamento/current`` como em produção (filho de ``faturamento``)."""
    base = tmp_path / "data_products" / "cliente_x" / "faturamento"
    cur = base / "current"
    cur.mkdir(parents=True)
    return base


def test_archive_move_parquet_para_archive_version_ts(faturamento_layout: Path) -> None:
    base = faturamento_layout
    cur = base / "current"
    dev_path = cur / "dataset_faturamento_devolucoes.parquet"
    _write_min_parquet(
        dev_path,
        pd.DataFrame({"org_id": ["a"], "Valor_Liquido_Devolucao": [1.0]}),
    )
    meta_path = cur / "metadata.json"
    meta_path.write_text(
        json.dumps(
            {
                "schema_version_devolucoes": 3,
                "pipeline_revision_devolucoes": "devolucoes-fiscais-v3",
                "filtros_aplicados": {"x": 1},
                "extra_global": "should_not_expect_in_archive_partial_logic",
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _archive_current_se_existe(base)

    assert not dev_path.is_file()
    arch_roots = list((base / "archive").glob("v3_*"))
    assert len(arch_roots) == 1
    moved = arch_roots[0] / "dataset_faturamento_devolucoes.parquet"
    assert moved.is_file()


def test_archive_metadata_json_apenas_campos_devolucoes(faturamento_layout: Path) -> None:
    base = faturamento_layout
    cur = base / "current"
    _write_min_parquet(
        cur / "dataset_faturamento_devolucoes.parquet",
        pd.DataFrame({"x": [1]}),
    )
    cur.joinpath("metadata.json").write_text(
        json.dumps(
            {
                "schema_version_devolucoes": 2,
                "pipeline_revision_devolucoes": "pr",
                "filtros_aplicados": {},
                "cobertura": {},
                "warnings": [],
                "total_devolvido": 10.0,
                "nfs_devolucao": 2,
                "base_fiscal_composicao": "emitidas - canceladas - devolucoes",
                "pipeline_revision": "fat-global",
                "row_count": 99999,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    _archive_current_se_existe(base)

    arch_meta = next((base / "archive").glob("v2_*")) / "metadata.json"
    payload = json.loads(arch_meta.read_text(encoding="utf-8"))
    assert payload.get("pipeline_revision") is None
    assert payload.get("row_count") is None
    assert "dataset_faturamento_devolucoes_parquet" in payload
    assert "archived_at" in payload
    assert payload.get("schema_version_devolucoes") == 2


def test_outros_parquets_em_current_permanecem(faturamento_layout: Path) -> None:
    base = faturamento_layout
    cur = base / "current"
    _write_min_parquet(
        cur / "dataset_faturamento_devolucoes.parquet",
        pd.DataFrame({"a": [1]}),
    )
    nf_path = cur / "dataset_faturamento_nf.parquet"
    _write_min_parquet(nf_path, pd.DataFrame({"nf": [1]}))
    cur.joinpath("metadata.json").write_text(
        '{"schema_version_devolucoes": 1}', encoding="utf-8"
    )

    _archive_current_se_existe(base)

    assert nf_path.is_file()


def test_idempotente_sem_parquet_devolucoes_nao_cria_archive(faturamento_layout: Path) -> None:
    base = faturamento_layout
    cur = base / "current"
    cur.mkdir(parents=True, exist_ok=True)

    _archive_current_se_existe(base)

    archive_root = base / "archive"
    assert not archive_root.exists() or not any(archive_root.iterdir())


def test_sem_schema_version_devolucoes_usa_unknown_no_nome_pasta(faturamento_layout: Path) -> None:
    base = faturamento_layout
    cur = base / "current"
    _write_min_parquet(
        cur / "dataset_faturamento_devolucoes.parquet",
        pd.DataFrame({"z": [1]}),
    )
    cur.joinpath("metadata.json").write_text(
        json.dumps({"pipeline_revision": "global_only"}, ensure_ascii=False),
        encoding="utf-8",
    )

    _archive_current_se_existe(base)

    dirs = list((base / "archive").glob("vunknown_*"))
    assert len(dirs) == 1
    assert (dirs[0] / "dataset_faturamento_devolucoes.parquet").is_file()
