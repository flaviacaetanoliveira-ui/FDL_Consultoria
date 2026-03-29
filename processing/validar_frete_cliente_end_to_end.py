"""
Validacao ponta a ponta do pipeline Frete com ficheiros sob FDL_BASE_DIR
(mesma funcao que o app usa em modo live: carregar_tabela_final_frete_operacional).

Uso:
  set FDL_BASE_DIR=C:\\caminho\\cliente
  python processing/validar_frete_cliente_end_to_end.py

Ou:
  python processing/validar_frete_cliente_end_to_end.py --base-dir cliente_1

Saida: relatorio em processing/output/validacao_frete_e2e_report.txt (UTF-8)
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
OUT_DIR = REPO / "processing" / "output"
TOL = 1e-6


def _nome_sugere_demo(path: Path | None) -> bool:
    if path is None:
        return False
    return "demo" in path.name.lower()


def _relatorio_identificacao_fontes(
    fontes: object,
    base: Path,
    lines: list[str],
) -> tuple[bool, bool]:
    """
    Detalha o que descobrir_fontes_frete resolveu. Devolve (tem_url_vendas, parece_apenas_demo).
    """
    from datetime import datetime

    vendas_url = bool((getattr(fontes, "vendas_url", None) or "").strip())
    frete_url = bool((getattr(fontes, "frete_url", None) or "").strip())
    vp = getattr(fontes, "vendas_path", None)
    fp = getattr(fontes, "frete_path", None)

    lines.append("=== IDENTIFICACAO DAS FONTES (descobrir_fontes_frete) ===")
    lines.append(f"Base analisada: {base}")
    lines.append(f"Vendas via URL (FDL_FRETE_VENDAS_URL / PRECOMPUTED): {vendas_url}")
    lines.append(f"Frete anuncio via URL (FDL_FRETE_ANUNCIO_URL): {frete_url}")
    if vp and vp.is_file():
        try:
            st = vp.stat()
            mtime = datetime.fromtimestamp(st.st_mtime).isoformat(sep=" ", timespec="seconds")
            lines.append(f"Vendas local (mais recente em Vendas - Mercado Livre): {vp}")
            lines.append(f"  mtime: {mtime}  tamanho_bytes: {st.st_size}")
        except OSError as e:
            lines.append(f"Vendas local: {vp} (stat erro: {e})")
    else:
        lines.append("Vendas local: (nenhum ficheiro .csv/.xlsx/.xls encontrado)")
    if fp and fp.is_file():
        try:
            st = fp.stat()
            mtime = datetime.fromtimestamp(st.st_mtime).isoformat(sep=" ", timespec="seconds")
            lines.append(f"Frete por anuncio local: {fp}")
            lines.append(f"  mtime: {mtime}  tamanho_bytes: {st.st_size}")
        except OSError as e:
            lines.append(f"Frete por anuncio: {fp} (stat erro: {e})")
    else:
        lines.append("Frete por anuncio local: (nenhum .xlsx encontrado)")

    demo_vp = _nome_sugere_demo(vp)
    demo_fp = _nome_sugere_demo(fp)
    parece_demo = demo_vp or demo_fp
    lines.append("")
    if parece_demo:
        lines.append(
            "AVISO: nome de ficheiro contem 'demo' — isto NAO e um export completo de producao "
            "para validacao comercial final."
        )
        if demo_vp:
            lines.append("  -> vendas: " + vp.name)
        if demo_fp:
            lines.append("  -> frete: " + fp.name)
    else:
        lines.append(
            "Nomes de ficheiro nao contem 'demo' (heuristica simples). "
            "Confirme manualmente que sao os exports oficiais do ML e a planilha vigente."
        )

    lines.append("")
    return vendas_url, parece_demo


def _count_linhas_vendas(path: Path) -> int | None:
    try:
        from etapa1_vendas import read_sales_file

        df = read_sales_file(path)
        return int(len(df))
    except Exception as exc:  # noqa: BLE001
        return None


def _set_base_and_import(base: Path) -> tuple[object, object]:
    os.environ["FDL_BASE_DIR"] = str(base.resolve())
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from operacional_frete import (
        FRETE_ML_COL,
        FRETE_UI_ANUNCIO,
        FRETE_UI_DIFERENCA,
        FRETE_UI_FRETE_ESPERADO,
        FRETE_UI_N_VENDA,
        FRETE_UI_STATUS_CONC,
        FRETE_UI_VALOR_FRETE_ANUNCIO,
        carregar_tabela_final_frete_operacional,
        descobrir_fontes_frete,
        stable_mtime_ns_for_frete_url,
    )

    return (
        (
            FRETE_ML_COL,
            FRETE_UI_ANUNCIO,
            FRETE_UI_DIFERENCA,
            FRETE_UI_FRETE_ESPERADO,
            FRETE_UI_N_VENDA,
            FRETE_UI_STATUS_CONC,
            FRETE_UI_VALOR_FRETE_ANUNCIO,
            carregar_tabela_final_frete_operacional,
            descobrir_fontes_frete,
            stable_mtime_ns_for_frete_url,
        ),
        base,
    )


def _recalc_checks(df: pd.DataFrame, meta: dict, lines: list[str]) -> bool:
    from operacional_frete import (
        FRETE_ML_COL,
        FRETE_UI_DIFERENCA,
        FRETE_UI_FRETE_ESPERADO,
        FRETE_UI_VALOR_FRETE_ANUNCIO,
    )

    REC = "Receita por envio (BRL)"
    TAR = "Tarifas de envio (BRL)"
    QTD = "Unidades"

    mode = str(meta.get("frete_cobrado_modo", "") or "")
    lines.append(f"Meta frete_cobrado_modo: {mode or '(ausente)'}")
    lines.append(f"Meta frete_tabular: {meta.get('frete_tabular')}")

    rec = pd.to_numeric(df[REC], errors="coerce") if REC in df.columns else None
    if rec is None:
        lines.append("ERRO: falta coluna receita.")
        return False

    if mode == "receita_mais_tarifas_sinal" or mode == "receita_custo_tarifas_prioridade":
        # Modo legado em CSVs antigos; motor atual só usa receita + tarifas (custo não entra).
        ta = pd.to_numeric(df[TAR], errors="coerce") if TAR in df.columns else pd.Series(np.nan, index=df.index)
        both_na = rec.isna() & ta.isna()
        raw = (rec.fillna(0.0) + ta.fillna(0.0)).where(~both_na, 0.0)
        fc_expected = raw.abs()
        lines.append("Motor esperado: Frete cobrado = |receita + tarifas|; custo ignorado")
    else:
        lines.append(f"ERRO: frete_cobrado_modo desconhecido: {mode!r}")
        return False

    ok_fc = np.allclose(
        pd.to_numeric(df[FRETE_ML_COL], errors="coerce"),
        fc_expected,
        rtol=0,
        atol=TOL,
        equal_nan=True,
    )
    lines.append(f"Check frete cobrado (vs motor): {'OK' if ok_fc else 'FALHA'}")
    if not ok_fc:
        bad = ~(np.isclose(pd.to_numeric(df[FRETE_ML_COL], errors="coerce"), fc_expected, rtol=0, atol=TOL, equal_nan=True))
        lines.append(f"  Linhas divergentes no check: {bad.sum()}")

    if FRETE_UI_FRETE_ESPERADO not in df.columns:
        lines.append("Sem frete esperado (sem planilha tabular?) — skip qtd x preco.")
        return ok_fc

    qtd = pd.to_numeric(df[QTD], errors="coerce").fillna(0.0)
    pu = pd.to_numeric(df[FRETE_UI_VALOR_FRETE_ANUNCIO], errors="coerce")
    mask_esperado = pu.notna() & qtd.notna()
    esp_calc = qtd * pu
    esp_col = pd.to_numeric(df[FRETE_UI_FRETE_ESPERADO], errors="coerce")
    ok_esp = True
    if mask_esperado.any():
        ok_esp = np.allclose(
            esp_col.loc[mask_esperado],
            esp_calc.loc[mask_esperado],
            rtol=0,
            atol=TOL,
            equal_nan=True,
        )
    lines.append(f"Check frete esperado = qtd x valor frete anuncio: {'OK' if ok_esp else 'FALHA'}")

    diff_col = pd.to_numeric(df[FRETE_UI_DIFERENCA], errors="coerce")
    fc_col = pd.to_numeric(df[FRETE_ML_COL], errors="coerce")
    diff_calc = esp_col - fc_col
    mask_diff = mask_esperado & fc_col.notna() & esp_col.notna()
    ok_diff = True
    if mask_diff.any():
        ok_diff = np.allclose(
            diff_col.loc[mask_diff],
            diff_calc.loc[mask_diff],
            rtol=0,
            atol=TOL,
            equal_nan=True,
        )
    lines.append(f"Check diferenca = frete esperado - frete cobrado: {'OK' if ok_diff else 'FALHA'}")

    return bool(ok_fc and ok_esp and ok_diff)


def _kpi_ranking(df: pd.DataFrame, lines: list[str]) -> None:
    sys.path.insert(0, str(REPO))
    from processing.simulacao_frete_logica import divergencia_mask, ranking_anuncios

    from operacional_frete import (
        FRETE_ML_COL,
        FRETE_UI_DIFERENCA,
        FRETE_UI_STATUS_CONC,
        FRETE_UI_VAL_DIVERGENCIA,
    )

    fm = pd.to_numeric(df.get(FRETE_ML_COL), errors="coerce")
    n_com = int(fm.notna().sum())
    soma_fc = float(fm.fillna(0).sum())
    lines.append(f"KPI soma frete cobrado (todas linhas, NaN=0): R$ {soma_fc:.2f}")
    lines.append(f"KPI linhas com frete cobrado numerico: {n_com}")

    if FRETE_UI_STATUS_CONC in df.columns:
        div = df[df[FRETE_UI_STATUS_CONC].eq(FRETE_UI_VAL_DIVERGENCIA)]
        n_div = len(div)
        soma_abs = (
            float(pd.to_numeric(div[FRETE_UI_DIFERENCA], errors="coerce").abs().sum())
            if n_div and FRETE_UI_DIFERENCA in div.columns
            else 0.0
        )
        lines.append(f"KPI linhas Divergencia (status): {n_div}")
        lines.append(f"KPI soma |diferenca| nas linhas Divergencia: R$ {soma_abs:.2f}")
    else:
        div_m = divergencia_mask(df)
        lines.append(f"KPI linhas divergentes (fallback |diff|>0.02): {int(div_m.sum())}")

    rank_df, meta = ranking_anuncios(df)
    lines.append(f"Ranking meta: {json.dumps(meta, ensure_ascii=True)}")
    lines.append("Ranking (top):")
    lines.append(rank_df.to_string(index=False) if not rank_df.empty else "  (vazio)")


def main() -> int:
    ap = argparse.ArgumentParser(description="Validacao E2E Frete com dados do cliente.")
    ap.add_argument(
        "--base-dir",
        type=Path,
        default=None,
        help="Pasta cliente (Vendas - Mercado Livre, Frete por Anuncio). Default: cliente_1 no repo.",
    )
    args = ap.parse_args()
    base = (args.base_dir or (REPO / "cliente_1")).resolve()

    if not base.is_dir():
        print(f"Base dir nao existe: {base}", file=sys.stderr)
        return 2

    pack, _ = _set_base_and_import(base)
    (
        FRETE_ML_COL,
        _A,
        _D,
        _E,
        _N,
        _S,
        _V,
        carregar_tabela_final_frete_operacional,
        descobrir_fontes_frete,
        stable_mtime_ns_for_frete_url,
    ) = pack

    lines: list[str] = []
    lines.append("=== VALIDACAO E2E FRETE ===")
    lines.append(f"FDL_BASE_DIR: {base}")
    lines.append("")

    fontes = descobrir_fontes_frete(base)
    vendas_url_on, parece_demo = _relatorio_identificacao_fontes(fontes, base, lines)

    vendas_ref = (fontes.vendas_url or "").strip() or (
        str(fontes.vendas_path.resolve()) if fontes.vendas_path else ""
    )
    if not vendas_ref:
        lines.append("ERRO: sem ficheiro de vendas ML (pasta Vendas - Mercado Livre vazia ou so .gitkeep).")
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / "validacao_frete_e2e_report.txt").write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return 1

    frete_ref = (fontes.frete_url or "").strip() or (
        str(fontes.frete_path.resolve()) if fontes.frete_path and fontes.frete_path.is_file() else None
    )

    if not vendas_url_on and fontes.vendas_path and fontes.vendas_path.is_file():
        n_raw = _count_linhas_vendas(fontes.vendas_path)
        if n_raw is not None:
            lines.append(f"Linhas no ficheiro de vendas (read_sales_file): {n_raw}")
        lines.append("")

    if (fontes.vendas_url or "").strip():
        v_ns = stable_mtime_ns_for_frete_url(fontes.vendas_url)
    else:
        v_ns = int(fontes.vendas_path.stat().st_mtime_ns)

    if (fontes.frete_url or "").strip():
        f_ns = stable_mtime_ns_for_frete_url(fontes.frete_url)
    elif fontes.frete_path and fontes.frete_path.is_file():
        f_ns = int(fontes.frete_path.stat().st_mtime_ns)
    else:
        f_ns = None

    lines.append("--- Referencias usadas no loader ---")
    lines.append(f"Vendas: {vendas_ref}")
    lines.append(f"Frete anuncio: {frete_ref or '(nenhum)'}")
    lines.append("")

    try:
        df, meta = carregar_tabela_final_frete_operacional(
            "validacao_e2e", vendas_ref, v_ns, frete_ref, f_ns
        )
    except Exception as exc:  # noqa: BLE001
        lines.append(f"ERRO ao carregar pipeline: {exc}")
        OUT_DIR.mkdir(parents=True, exist_ok=True)
        (OUT_DIR / "validacao_frete_e2e_report.txt").write_text("\n".join(lines), encoding="utf-8")
        print("\n".join(lines))
        return 1

    lines.append(f"Linhas carregadas: {len(df)}")
    lines.append(f"Colunas: {list(df.columns)}")
    lines.append("")
    lines.append("--- Checks de formulas ---")
    ok = _recalc_checks(df, meta, lines)
    lines.append("")
    lines.append("--- KPIs e ranking (mesma logica simulacao_frete_logica) ---")
    _kpi_ranking(df, lines)
    lines.append("")
    lines.append("--- Amostra tabela operacional (primeiras 15 linhas) ---")
    lines.append(df.head(15).to_string())
    lines.append("")
    lines.append("=== RESULTADO GLOBAL ===")
    lines.append(f"Formulas e diffs conferidos: {'PASS' if ok else 'FAIL'}")
    lines.append("")
    lines.append("=== VEREDITO: DADOS REAIS vs EXEMPLO (neste ambiente) ===")
    if parece_demo or (not vendas_url_on and fontes.vendas_path and _nome_sugere_demo(fontes.vendas_path)):
        lines.append(
            "VALIDACAO EXECUTADA SOBRE FICHEIROS DE EXEMPLO (demo) OU NOMES COMPATIVEIS COM "
            "TEMPLATE — NAO substitui um export completo do Mercado Livre + planilha real de frete."
        )
        lines.append(
            "Para validar com dados reais: copie o export oficial ML para "
            "'<FDL_BASE_DIR>/Vendas - Mercado Livre/' (substitua ou remova o *demo*.csv), "
            "coloque a planilha de Frete por Anuncio na raiz do cliente, e execute de novo com "
            "--base-dir apontando para essa pasta (ou junction para o OneDrive do cliente)."
        )
    else:
        lines.append(
            "Heuristica: ficheiros locais nao parecem ser o template 'demo' pelo nome. "
            "Revise linhas e negocio com o cliente antes da apresentacao final."
        )
    lines.append("")
    lines.append("NOTA APP:")
    lines.append(
        "O Streamlit usa carregar_tabela_final_frete_operacional em modo live quando ha vendas "
        "em FDL_BASE_DIR (sem depender de dados de exemplo embutidos no codigo). "
        "Se FDL_FRETE_MATERIALIZED_PATH/URL estiver definido e o ficheiro existir, "
        "o app le o CSV materializado primeiro — regenere o materializado apos trocar os exports."
    )
    lines.append("")
    if parece_demo or (not vendas_url_on and fontes.vendas_path and _nome_sugere_demo(fontes.vendas_path)):
        lines.append(
            "NOTA: no repo V2 existe cliente_1 com ficheiros *demo* — com FDL_BASE_DIR absoluto para "
            "a base real, esses ficheiros deixam de ser usados pela app."
        )
    if not meta.get("frete_tabular"):
        lines.append(
            "NOTA FRETE POR ANUNCIO: frete_tabular=false — o ficheiro encontrado nao foi reconhecido "
            "como tabela (MLB + preco unit.). Export de ecran do ML nao alimenta Frete esperado/diferenca. "
            "Use uma planilha tabular (como Frete_Anuncio_demo.xlsx no repo) ou ajuste o formato."
        )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    report_path = OUT_DIR / "validacao_frete_e2e_report.txt"
    report_path.write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))
    print(f"\nRelatorio gravado: {report_path}")
    return 0 if ok else 3


if __name__ == "__main__":
    raise SystemExit(main())
