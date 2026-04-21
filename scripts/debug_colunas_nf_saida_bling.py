"""
Lista colunas disponíveis nos CSVs de notas de saída das 4 empresas.
Objetivo: confirmar se há campo de imposto destacado (ICMS, PIS, COFINS)
diretamente do Bling.

Estende com estatísticas (Tarefa 3): % zeros vs >0 nas colunas de imposto detectadas.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

BASE = Path(r"C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Pedro\Cliente_2")
EMPRESAS = ["Gama Home", "Mega Star", "Móveis EAP", "Mega Facil"]

CANDIDATOS_PASTA = (
    "notas_saida",
    "Notas",
    "Saidas",
    "NF Saida",
    "Vendas",
)

def _col_looks_tax_related(name: str) -> bool:
    """Evita falso positivo «iss» dentro de «emissão»."""
    n = str(name).lower()
    if any(x in n for x in ("icms", "pis", "cofins", "ipi", "imposto", "tributo", "irpj", "csll")):
        return True
    if "aliquota" in n or "alíquota" in n:
        return True
    if "valor_imp" in n or "vl_imp" in n:
        return True
    return False


def _read_sample_csv(arquivo: Path, nrows: int = 5000) -> pd.DataFrame:
    if arquivo.suffix.lower() == ".xlsx":
        return pd.read_excel(arquivo, nrows=nrows)
    for enc in ("utf-8", "latin-1", "cp1252"):
        for sep in (None, ";", ","):
            try:
                if sep is None:
                    return pd.read_csv(arquivo, nrows=nrows, sep=None, engine="python", encoding=enc)
                return pd.read_csv(arquivo, nrows=nrows, sep=sep, encoding=enc)
            except Exception:
                continue
    raise OSError(f"Não foi possível ler {arquivo}")


def _stats_col(df: pd.DataFrame, col: str) -> str:
    s = pd.to_numeric(df[col], errors="coerce")
    n = int(s.notna().sum())
    if n == 0:
        return "sem numéricos válidos"
    zeros = int((s.fillna(0.0).abs() <= 1e-12).sum())
    pos = n - zeros
    nz = s[s.abs() > 1e-12]
    mean_nz = float(nz.mean()) if len(nz) else 0.0
    pct_zero = 100.0 * zeros / n if n else 0.0
    return f"n={n}, zeros={zeros} ({pct_zero:.1f}%), >0={pos}, média (>0)={mean_nz:.4f}"


def main() -> None:
    if not BASE.is_dir():
        print(f"ERRO: base não encontrada: {BASE}")
        return

    for empresa in EMPRESAS:
        print(f"\n### {empresa}")

        pasta_saida: Path | None = None
        for nome in CANDIDATOS_PASTA:
            c = BASE / empresa / nome
            if c.is_dir():
                pasta_saida = c
                print(f"  Pasta usada: {pasta_saida}")
                break

        if not pasta_saida:
            print(f"  ⚠ Pasta de saída não localizada (tentados: {list(CANDIDATOS_PASTA)})")
            continue

        arquivos = sorted(pasta_saida.rglob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not arquivos:
            arquivos = sorted(pasta_saida.rglob("*.xlsx"), key=lambda p: p.stat().st_mtime, reverse=True)

        if not arquivos:
            print("  ⚠ Sem arquivos")
            continue

        arquivo = arquivos[0]
        print(f"  Arquivo analisado (amostra): {arquivo.name}")

        try:
            df = _read_sample_csv(arquivo, nrows=5000)
        except Exception as exc:
            print(f"  ❌ Erro leitura: {exc}")
            continue

        colunas = list(df.columns)
        print(f"  Total de colunas: {len(colunas)}")

        relevantes = [c for c in colunas if _col_looks_tax_related(str(c))]

        if relevantes:
            print("  ✓ Colunas relacionadas a imposto/tributo:")
            for c in relevantes:
                vals = df[c].dropna().head(3).tolist()
                print(f"    - {c}: amostra={vals}")
                print(f"      {_stats_col(df, c)}")
        else:
            print("  ⚠ Nenhuma coluna de imposto detectada pelo filtro de palavras-chave")
            print(f"  Primeiras 25 colunas: {colunas[:25]}")


if __name__ == "__main__":
    main()
