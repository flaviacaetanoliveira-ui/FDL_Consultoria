#!/usr/bin/env python3
"""
Simulação do painel de Frete (Cenário A: últimos 30 dias) com os mesmos dados que o materializador usa.

Uso (PowerShell, na raiz do repositório V2):
  $env:FDL_BASE_DIR = "C:\\caminho\\para\\pasta\\cliente"
  python processing/simular_frete_dados_reais.py

Ou:
  python processing/simular_frete_dados_reais.py --base-dir "C:\\...\\cliente_1"

Requisitos na pasta base:
  - Ficheiro(s) .xlsx/.csv em "Vendas - Mercado Livre" (export ML detalhe envios), OU
  - FDL_FRETE_VENDAS_URL definido no ambiente (não coberto aqui).

O CSV `powerbi_mirror/output/conciliacao_operacional.csv` é do REPASSE — não serve como fonte de frete ML.
"""
from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _ensure_repo_on_path() -> None:
    r = str(REPO)
    if r not in sys.path:
        sys.path.insert(0, r)


def main() -> int:
    parser = argparse.ArgumentParser(description="Simula materialização + recorte 30 dias do frete")
    parser.add_argument(
        "--base-dir",
        default=os.environ.get("FDL_BASE_DIR", "").strip(),
        help="Pasta raiz do cliente (vendas ML, liberações, …). Obrigatório se FDL_BASE_DIR não estiver definido.",
    )
    parser.add_argument(
        "--org-id",
        default=os.environ.get("FDL_MATERIALIZE_ORG_ID", "antomoveis"),
    )
    parser.add_argument(
        "--cliente",
        default=os.environ.get("FDL_MATERIALIZE_CLIENTE", "default"),
    )
    parser.add_argument(
        "--empresa",
        default=os.environ.get("FDL_MATERIALIZE_EMPRESA", "").strip(),
    )
    parser.add_argument(
        "--skip-materialize",
        action="store_true",
        help="Só lê o dataset_frete_app.csv já existente em data_products (não regera).",
    )
    args = parser.parse_args()

    if not args.base_dir:
        print(
            "Defina --base-dir ou a variável de ambiente FDL_BASE_DIR (pasta com "
            "'Vendas - Mercado Livre' e export ML).",
            file=sys.stderr,
        )
        return 1

    base = Path(args.base_dir).expanduser().resolve()
    if not base.is_dir():
        print(f"Pasta não encontrada: {base}", file=sys.stderr)
        return 1

    vendas_dir = base / "Vendas - Mercado Livre"
    if not vendas_dir.is_dir():
        print(f"Pasta esperada em falta: {vendas_dir}", file=sys.stderr)
        return 1

    sales = list(vendas_dir.glob("*.xlsx")) + list(vendas_dir.glob("*.xls")) + list(vendas_dir.glob("*.csv"))
    sales = [p for p in sales if p.is_file() and p.name.lower() != "leia-me.txt"]
    if not sales:
        print(
            f"Nenhum .xlsx/.csv de vendas ML em:\n  {vendas_dir}\n"
            "Copie para aqui o export do Mercado Livre (detalhe de envios) e volte a correr.",
            file=sys.stderr,
        )
        return 1

    _ensure_repo_on_path()

    empresa = args.empresa
    if not empresa:
        _ensure_repo_on_path()
        from operacional_data_config import DATASET_EMPRESA

        def _slug_empresa_folder(name: str) -> str:
            s = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode("ascii")
            s = re.sub(r"[^a-zA-Z0-9_-]+", "_", s).strip("_").lower()
            return s or "default"

        empresa = _slug_empresa_folder(DATASET_EMPRESA)

    out_csv = (
        REPO
        / "data_products"
        / args.cliente
        / empresa
        / "frete"
        / "current"
        / "dataset_frete_app.csv"
    )

    if not args.skip_materialize:
        cmd = [
            sys.executable,
            str(REPO / "processing" / "materialize_financeiro.py"),
            "--base-dir",
            str(base),
            "--cliente",
            args.cliente,
            "--empresa",
            empresa,
            "--modulo",
            "frete",
            "--org-id",
            str(args.org_id),
        ]
        print("A executar:", " ".join(cmd))
        r = subprocess.run(cmd, cwd=str(REPO))
        if r.returncode != 0:
            return r.returncode

    if not out_csv.is_file():
        print(f"Ficheiro materializado em falta: {out_csv}", file=sys.stderr)
        return 1

    import pandas as pd

    from operacional_frete import frete_series_normalize_sale_dt

    df = pd.read_csv(out_csv, sep=None, engine="python", encoding="utf-8-sig")
    n_total = len(df)
    print()
    print("=== Ficheiro materializado ===")
    print(f"  Caminho: {out_csv}")
    print(f"  Linhas: {n_total}")

    br_tz = None
    try:
        from zoneinfo import ZoneInfo

        br_tz = ZoneInfo("America/Sao_Paulo")
    except Exception:
        pass
    today = datetime.now(br_tz).date() if br_tz else datetime.now().date()
    ini_30 = today - timedelta(days=29)
    print()
    print("=== Cenário A (igual à app) ===")
    print(f"  Hoje (fuso SP): {today.strftime('%d/%m/%Y')}")
    print(f"  Janela: {ini_30.strftime('%d/%m/%Y')} a {today.strftime('%d/%m/%Y')} (30 dias corridos, inclusive)")

    if "_data_venda_dt" not in df.columns and "data_venda" in df.columns:
        df = df.copy()
        df["_data_venda_dt"] = frete_series_normalize_sale_dt(df["data_venda"])
    elif "_data_venda_dt" in df.columns:
        df = df.copy()
        df["_data_venda_dt"] = frete_series_normalize_sale_dt(df["_data_venda_dt"])

    if "_data_venda_dt" not in df.columns:
        print("  Sem coluna de data — recorte 30 dias não aplicável (app mostra tudo).")
        return 0

    dts = frete_series_normalize_sale_dt(df["_data_venda_dt"])
    ini_ts = pd.Timestamp(ini_30)
    fim_ts = pd.Timestamp(today) + pd.Timedelta(days=1)
    m = dts.notna() & (dts >= ini_ts) & (dts < fim_ts)
    n_recorte = int(m.sum())
    if dts.notna().any():
        d_lo = dts.min()
        d_hi = dts.max()
        print(f"  Datas no ficheiro: {d_lo.strftime('%d/%m/%Y')} a {d_hi.strftime('%d/%m/%Y')}")
    print(f"  Linhas no recorte (30 dias): {n_recorte}")
    if n_total > 0 and n_recorte == 0 and dts.notna().any():
        print()
        print(
            "  AVISO: há linhas no ficheiro, mas nenhuma dentro dos últimos 30 dias. "
            "Atualize o export ML ou alargue o período no painel completo."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
