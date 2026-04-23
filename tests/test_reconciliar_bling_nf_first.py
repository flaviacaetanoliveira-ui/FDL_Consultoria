"""Smoke test do script de reconciliação Bling x NF-first (fixtures locais)."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
FIX = ROOT / "tests" / "fixtures" / "reconciliacao"
SCRIPT = ROOT / "scripts" / "reconciliar_bling_nf_first.py"


@pytest.mark.skipif(not FIX.is_dir(), reason="fixtures ausentes")
def test_reconciliar_script_runs_on_fixtures() -> None:
    r = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--bling",
            str(FIX / "bling_saida_sample.csv"),
            "--parquet",
            str(FIX / "dataset_faturamento_nf_sample.parquet"),
            "--line",
            str(FIX / "dataset_faturamento_app_sample.csv"),
            "--d-ini",
            "2026-01-01",
            "--d-fim",
            "2026-01-05",
            "--empresa",
            "Esquilo",
        ],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    assert r.returncode == 0, r.stderr + r.stdout
    out = r.stdout
    assert "NFs no Bling (recorte):       4" in out
    assert "NFs no app Parquet (recorte): 2" in out
    assert "33.000,00" in out or "33000" in out.replace(".", "")
    assert "12.000,00" in out or "12000" in out.replace(".", "")
