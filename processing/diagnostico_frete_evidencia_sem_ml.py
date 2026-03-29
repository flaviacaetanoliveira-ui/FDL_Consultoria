"""
Diagnóstico objetivo: linhas com status «Sem frete da plataforma nesta venda» onde
valor frete por anúncio e frete esperado estão preenchidos — mostra colunas brutas
do export ML e o motivo pelo qual o frete cobrado não foi calculado.

Uso (base real, mesmos ficheiros do app):
  python processing/diagnostico_frete_evidencia_sem_ml.py ^
    --vendas "C:\\...\\Vendas - Mercado Livre\\vendas.xlsx" ^
    --frete "C:\\...\\Frete_Anuncio.xlsx" ^
    --limit 3

Cenário de demonstração (3 linhas sintéticas, ficheiros temporários):
  python processing/diagnostico_frete_evidencia_sem_ml.py --demo

Exportar todas as linhas do critério (evidência + medição) para CSV:
  python processing/diagnostico_frete_evidencia_sem_ml.py --vendas ... --frete ... --csv-out evidencia_frete_sem_ml.csv
"""
from __future__ import annotations

import argparse
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from operacional_frete import (
    FRETE_ML_COL,
    FRETE_UI_ANUNCIO,
    FRETE_UI_FRETE_ESPERADO,
    FRETE_UI_N_VENDA,
    FRETE_UI_STATUS_CONC,
    FRETE_UI_STATUS_SEM_FRETE_ML,
    FRETE_UI_VALOR_FRETE_ANUNCIO,
    carregar_base_frete_ml,
)


COL_REC = "Receita por envio (BRL)"
COL_CUSTO = "Custo do envio (BRL)"
COL_TAR = "Tarifas de envio (BRL)"


def _motivo_frete_cobrado_ausente(row: pd.Series, _modo: str) -> str:
    """Explica linhas com status «sem info ML» (receita e tarifas ausentes; custo não entra no motor)."""
    re = pd.to_numeric(row.get(COL_REC), errors="coerce")
    ta = pd.to_numeric(row.get(COL_TAR), errors="coerce")
    if pd.isna(re) and pd.isna(ta):
        return (
            "Motor receita + tarifas: receita e tarifas de envio ausentes — frete cobrado = 0; "
            "status sem informação ML."
        )
    return "Caso inesperado para status sem informação ML; verificar colunas."


def _build_demo_files() -> tuple[Path, Path]:
    """Três vendas com planilha de frete preenchida e export ML sem dados para cobrado (modo tarifas).

    Inclui uma linha sentinela (n.º 999999) só para que «Receita» e «Tarifas» não sejam removidas
    por `dropna(axis=1, how='all')` quando todas as células de exemplo estão vazias no Excel.
    """
    vendas = pd.DataFrame(
        {
            "Data da venda": ["2026-03-01", "2026-03-02", "2026-03-03", "2026-01-01"],
            "N.º venda": [90001, 90002, 90003, 999999],
            "Estado": ["Entregue"] * 4,
            "Descrição do status": ["Entregue"] * 4,
            "Unidades": [1, 2, 1, 1],
            COL_REC: ["", "", "", 0.0],
            COL_TAR: ["", "", "", 0.0],
            "# do anúncio": [
                "MLB9000000001",
                "MLB9000000002",
                "MLB9000000003",
                "MLB0000000000",
            ],
        }
    )
    frete = pd.DataFrame(
        {
            "# Anuncio MLB": ["MLB9000000001", "MLB9000000002", "MLB9000000003"],
            "Preco frete unit (BRL)": [10.0, 4.5, 7.0],
        }
    )
    td = tempfile.mkdtemp(prefix="fdl_frete_demo_")
    v_path = Path(td) / "vendas_demo_sem_frete_ml.xlsx"
    f_path = Path(td) / "frete_demo.xlsx"
    vendas.to_excel(v_path, index=False)
    frete.to_excel(f_path, index=False)
    return v_path, f_path


def main() -> None:
    ap = argparse.ArgumentParser(description="Evidência para status «sem frete no ML» com esperado preenchido.")
    ap.add_argument("--demo", action="store_true", help="Usa 3 linhas sintéticas (ficheiros temporários).")
    ap.add_argument("--vendas", type=Path, help="Ficheiro de vendas ML (.csv/.xlsx).")
    ap.add_argument("--frete", type=Path, help="Planilha frete por anúncio (.xlsx).")
    ap.add_argument("--limit", type=int, default=3, help="Máximo de linhas a listar no ecrã (não limita o CSV).")
    ap.add_argument(
        "--csv-out",
        type=Path,
        default=None,
        help="Gravar todas as linhas do critério em CSV (UTF-8 com BOM), com coluna de motivo.",
    )
    args = ap.parse_args()

    if args.demo:
        v_path, f_path = _build_demo_files()
    else:
        if not args.vendas or not args.vendas.is_file():
            ap.error("--vendas obrigatório e ficheiro existente (ou use --demo).")
        v_path = args.vendas
        f_path = args.frete if args.frete and args.frete.is_file() else None

    frete_str = str(f_path) if f_path else ""
    df, meta = carregar_base_frete_ml(
        "_diag",
        str(v_path),
        int(Path(v_path).stat().st_mtime_ns),
        frete_str or None,
        int(Path(f_path).stat().st_mtime_ns) if f_path else None,
    )
    modo = str(meta.get("frete_cobrado_modo", ""))

    need = (FRETE_UI_STATUS_CONC, FRETE_UI_FRETE_ESPERADO, FRETE_UI_VALOR_FRETE_ANUNCIO)
    missing = [c for c in need if c not in df.columns]
    if missing:
        print(
            "Colunas em falta no dataset após carregar: "
            f"{missing}. Confirme que a planilha de frete por anúncio foi lida como tabela "
            f"(merge aplicado). Total linhas: {len(df)}."
        )
        return

    mask = (
        (df[FRETE_UI_STATUS_CONC] == FRETE_UI_STATUS_SEM_FRETE_ML)
        & df[FRETE_UI_FRETE_ESPERADO].notna()
        & df[FRETE_UI_VALOR_FRETE_ANUNCIO].notna()
    )
    n_total = int(len(df))
    n_hit = int(mask.sum())
    sub = df.loc[mask].head(int(args.limit)).copy()

    print("=== Resumo (evidência operacional + medição) ===")
    print(f"Linhas no export de vendas (após pipeline): {n_total}")
    print(f"Linhas no critério — sem frete ML + valor anúncio + esperado preenchidos: {n_hit}")
    if n_total:
        print(f"Percentual sobre o export: {100.0 * n_hit / n_total:.4f}%")
    print(f"frete_cobrado_modo (meta): {modo}")
    print(f"Vendas (ficheiro): {meta.get('vendas_arquivo', '')}")
    print(f"Frete anúncio (ficheiro): {meta.get('frete_arquivo', '')}")
    print()

    if n_hit == 0:
        print(
            "Nenhuma linha no critério acima — nada a listar nem a exportar.\n"
            "Se esperava casos, confirme caminho da planilha e colunas do export."
        )
        return

    hit = df.loc[mask].copy()
    hit["_motivo_frete_cobrado_ausente"] = hit.apply(lambda r: _motivo_frete_cobrado_ausente(r, modo), axis=1)
    if args.csv_out is not None:
        out_cols = [
            FRETE_UI_N_VENDA,
            FRETE_UI_ANUNCIO,
            FRETE_UI_VALOR_FRETE_ANUNCIO,
            FRETE_UI_FRETE_ESPERADO,
            COL_REC,
            COL_CUSTO,
            COL_TAR,
            FRETE_ML_COL,
            FRETE_UI_STATUS_CONC,
            "_motivo_frete_cobrado_ausente",
        ]
        for c in out_cols:
            if c not in hit.columns and c != "_motivo_frete_cobrado_ausente":
                hit[c] = np.nan
        hit[[c for c in out_cols if c in hit.columns]].to_csv(
            args.csv_out.expanduser().resolve(),
            index=False,
            encoding="utf-8-sig",
        )
        print(f"CSV gravado: {args.csv_out.resolve()} ({n_hit} linhas)\n")

    if sub.empty:
        return

    cols_show = [
        FRETE_UI_N_VENDA,
        FRETE_UI_ANUNCIO,
        FRETE_UI_VALOR_FRETE_ANUNCIO,
        FRETE_UI_FRETE_ESPERADO,
        COL_REC,
        COL_CUSTO,
        COL_TAR,
        FRETE_ML_COL,
        FRETE_UI_STATUS_CONC,
    ]
    for c in cols_show:
        if c not in sub.columns:
            sub[c] = np.nan

    sub["_motivo_calculo"] = sub.apply(lambda r: _motivo_frete_cobrado_ausente(r, modo), axis=1)

    print(f"--- Detalhe no ecrã (até {args.limit} linhas) ---\n")
    for i, (_, r) in enumerate(sub.iterrows(), 1):
        print(f"--- Exemplo {i} ---")
        print(f"1. N.º venda: {r[FRETE_UI_N_VENDA]}")
        print(f"2. # de anúncio: {r[FRETE_UI_ANUNCIO]}")
        print(f"3. Valor frete por anúncio: {r[FRETE_UI_VALOR_FRETE_ANUNCIO]}")
        print(f"4. Frete esperado: {r[FRETE_UI_FRETE_ESPERADO]}")
        print(f"5. ML — Receita por envio (BRL): {r[COL_REC]}")
        print(f"   ML — Custo do envio (BRL): {r[COL_CUSTO]}")
        print(f"   ML — Tarifas de envio (BRL): {r[COL_TAR]}")
        print(f"6. Frete cobrado (final): {r[FRETE_ML_COL]}")
        print(f"7. Status conciliação: {r[FRETE_UI_STATUS_CONC]}")
        print(f"   Evidência: {r['_motivo_calculo']}")
        print()


if __name__ == "__main__":
    main()
