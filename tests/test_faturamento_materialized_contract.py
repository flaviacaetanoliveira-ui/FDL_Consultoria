"""
Contrato técnico de leitura do faturamento materializado (CSV/Parquet em disco),
espelhando o comportamento de `_load_faturamento_file_from_disk` em app_operacional.py.
Não importa o app Streamlit (exige sessão).

Executar: python -m unittest tests.test_faturamento_materialized_contract
"""
from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

import pandas as pd


def _load_faturamento_file_from_disk(path: Path) -> pd.DataFrame:
    if not path.is_file():
        raise FileNotFoundError(f"Ficheiro de faturamento não encontrado: {path}")
    suf = path.suffix.lower()
    if suf == ".csv":
        return pd.read_csv(path, sep=None, engine="python", encoding="utf-8-sig")
    if suf == ".parquet":
        return pd.read_parquet(path, engine="pyarrow")
    raise ValueError(f"Formato não suportado: {path.name!r}")


class TestFaturamentoMaterializedContract(unittest.TestCase):
    def test_csv_reads_utf8_sig(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "dataset_faturamento_app.csv"
            p.write_text("col_a,col_b\n1,foo\n2,bar\n", encoding="utf-8-sig")
            df = _load_faturamento_file_from_disk(p)
            self.assertEqual(len(df), 2)
            self.assertEqual(list(df.columns), ["col_a", "col_b"])

    def test_parquet_roundtrip(self) -> None:
        try:
            import pyarrow  # noqa: F401
        except ImportError:
            self.skipTest("pyarrow não instalado")
        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "dataset.parquet"
            pd.DataFrame({"x": [1, 2]}).to_parquet(p, engine="pyarrow")
            df = _load_faturamento_file_from_disk(p)
            self.assertEqual(len(df), 2)

    def test_derive_faturamento_path_from_repasse_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            tmp_path = Path(d)
            repasse_csv = (
                tmp_path
                / "data_products"
                / "cli"
                / "emp"
                / "repasse"
                / "current"
                / "dataset_repasse_app.csv"
            )
            repasse_csv.parent.mkdir(parents=True)
            repasse_csv.write_text("stub", encoding="utf-8")
            fat_csv = (
                tmp_path
                / "data_products"
                / "cli"
                / "emp"
                / "faturamento"
                / "current"
                / "dataset_faturamento_app.csv"
            )
            fat_csv.parent.mkdir(parents=True)
            fat_csv.write_text("a\n1\n", encoding="utf-8")

            anchor = str(repasse_csv.resolve())
            path = Path(anchor.strip()).expanduser().resolve()
            self.assertTrue(path.is_file())
            self.assertEqual(path.parent.name, "current")
            self.assertEqual(path.parent.parent.name, "repasse")
            empresa_dir = path.parent.parent.parent
            csv_c = empresa_dir / "faturamento" / "current" / "dataset_faturamento_app.csv"
            self.assertTrue(csv_c.is_file())
            df = _load_faturamento_file_from_disk(csv_c)
            self.assertEqual(len(df), 1)


if __name__ == "__main__":
    unittest.main()
