"""
Simulação em memória da lógica de divergência e ranking do painel Frete
(paridade histórica: divergência = apenas **Cobrado a maior**; repasse não conta).

Regras de negócio simuladas (alinhadas a operacional_frete.carregar_base_frete_ml):
- Frete esperado = valor frete por anúncio × quantidade
- Frete cobrado = |receita por envio + tarifas de envio| (sempre ≥ 0); custo do envio não entra no cálculo
- Diferença = frete esperado − frete cobrado
- Status conciliação: OK se |diferença| ≤ tolerância (0,02), senão Divergência

Uso (na raiz do repo):
  python processing/simulacao_frete_logica.py
  python processing/simulacao_frete_logica.py --regras

Não depende do Streamlit nem de ficheiros — apenas pandas + operacional_frete (constantes).
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from operacional_frete import (
    FRETE_ML_COL,
    FRETE_UI_ANALISADO_COBRADO_MAIOR,
    FRETE_UI_ANUNCIO,
    FRETE_UI_DIFERENCA,
    FRETE_UI_FRETE_ESPERADO,
    FRETE_UI_N_VENDA,
    FRETE_UI_STATUS_CONC,
    FRETE_UI_VALOR_FRETE_ANUNCIO,
    FRETE_UI_VAL_DIVERGENCIA,
    _compute_frete_cobrado_ml,
    compute_frete_situacao_frete_column,
    dataframe_frete_conciliacao_principal,
)

_FRETE_DIV_TOL = 0.02


def frete_cobrado_motor(
    receita: pd.Series,
    *,
    tarifas: pd.Series | None = None,
) -> tuple[pd.Series, str]:
    """Espelha `_compute_frete_cobrado_ml` em `carregar_base_frete_ml`."""
    out, _ = _compute_frete_cobrado_ml(receita, tarifas)
    return out, "receita_mais_tarifas_sinal"


def montar_tabela_operacional_com_planilha(
    base: pd.DataFrame,
    *,
    receita_col: str,
    tarifas_col: str | None,
    qtd_col: str = "Unidades",
    preco_arquivo_col: str = FRETE_UI_VALOR_FRETE_ANUNCIO,
) -> pd.DataFrame:
    """Constroi linhas finais com frete esperado, cobrado, diferença e status (planilha frete por anúncio)."""
    qtd = pd.to_numeric(base[qtd_col], errors="coerce").fillna(0.0)
    pu = pd.to_numeric(base[preco_arquivo_col], errors="coerce")
    frete_esperado = qtd * pu
    re_s = base[receita_col]
    ta_s = base[tarifas_col] if tarifas_col and tarifas_col in base.columns else None
    fc, _ = frete_cobrado_motor(re_s, tarifas=ta_s)
    diff = frete_esperado - fc
    st = np.where(
        frete_esperado.notna() & fc.notna(),
        np.where(diff.abs() <= _FRETE_DIV_TOL, "OK", FRETE_UI_VAL_DIVERGENCIA),
        pd.Series(np.nan, index=base.index),
    )
    out = base.copy()
    out[FRETE_UI_FRETE_ESPERADO] = frete_esperado
    out[FRETE_ML_COL] = fc
    out[FRETE_UI_DIFERENCA] = diff
    out[FRETE_UI_STATUS_CONC] = st
    if "Estado" not in out.columns:
        out["Estado"] = "Entregue"
    return out


REC_ENV = "Receita por envio (BRL)"
CUSTO_ENV = "Custo do envio (BRL)"
TAR_ENV = "Tarifas de envio (BRL)"


def simulacao_regras_negocio_completa() -> None:
    """
    Simulação explícita das regras finais: tabela, cálculo do frete cobrado, |Δ| e ranking.
    Inclui (1) sem divergência, (2) Δ+ e Δ−, (3) mesmo anúncio várias vendas,
    (4) receita+tarifas (único motor de frete cobrado).
    """
    print("\n" + "=" * 78)
    print("SIMULACAO - Regras finais de negocio (motor explicito)")
    print("=" * 78)
    print(
        f"Tolerancia conciliacao: |diferenca| <= {_FRETE_DIV_TOL} -> OK; senao Divergencia.\n"
    )

    # --- (4) Motor único: receita + tarifas (custo do envio no export não entra no cálculo) ---
    print("-" * 78)
    print("CENARIO A - Frete cobrado = |receita por envio + tarifas de envio|")
    print("-" * 78)
    # V0: frete na planilha = 0 → Frete esperado 0; plataforma cobra → Situação «Repasse de frete».
    base_custo = pd.DataFrame(
        {
            FRETE_UI_N_VENDA: ["V0", "V1", "V2", "V3", "V4", "V5"],
            FRETE_UI_ANUNCIO: ["MLB000", "MLB111", "MLB222", "MLB222", "MLB333", "MLB444"],
            "Unidades": [1, 1, 2, 1, 1, 1],
            FRETE_UI_VALOR_FRETE_ANUNCIO: [0.0, 50.0, 10.0, 10.0, 100.0, 50.0],
            REC_ENV: [25.0, 100.0, 30.0, 12.0, 100.0, 48.0],
            # Tarifas típicas ML (negativas); coluna Custo só informativa — não usada no motor.
            TAR_ENV: [-10.0, -50.0, -5.0, -5.0, -0.01, -2.0],
        }
    )
    tbl_c = montar_tabela_operacional_com_planilha(
        base_custo,
        receita_col=REC_ENV,
        tarifas_col=TAR_ENV,
    )
    print("\nDetalhe do frete cobrado (cada linha):")
    for _, r in base_custo.iterrows():
        re_v = r[REC_ENV]
        ta_v = r[TAR_ENV]
        fc_v = abs(re_v + ta_v)
        print(
            f"  {r[FRETE_UI_N_VENDA]}: |{REC_ENV} {re_v:.2f} + {TAR_ENV} ({ta_v:.2f})| "
            f"= Frete cobrado {fc_v:.2f}"
        )
    print("\nTabela final (grid conciliacao):")
    print(dataframe_frete_conciliacao_principal(tbl_c).to_string(index=False))
    print("\nResumo numerico (operacional):")
    cols_show = [
        FRETE_UI_N_VENDA,
        FRETE_UI_ANUNCIO,
        "Unidades",
        FRETE_UI_VALOR_FRETE_ANUNCIO,
        FRETE_UI_FRETE_ESPERADO,
        FRETE_ML_COL,
        FRETE_UI_DIFERENCA,
        FRETE_UI_STATUS_CONC,
    ]
    print(tbl_c[cols_show].to_string(index=False))

    div_c = divergencia_mask(tbl_c)
    imp_c = pd.to_numeric(tbl_c.loc[div_c, FRETE_UI_DIFERENCA], errors="coerce").abs().sum()
    print(f"\nLinhas em divergencia: {list(tbl_c.loc[div_c, FRETE_UI_N_VENDA])}")
    print(f"Impacto total |diff| (so linhas divergentes): R$ {imp_c:.2f}")
    rank_c, meta_c = ranking_anuncios(tbl_c)
    print("Meta ranking:", meta_c)
    print("\nRanking por anuncio (impacto |diff| nas linhas divergentes):\n")
    print(rank_c.to_string(index=False) if not rank_c.empty else "  (vazio)")

    # --- (5) Sem custo: TARIFAS com sinal (típico ML negativo) ---
    print("\n" + "-" * 78)
    print("CENARIO B - Mesmo motor receita + tarifas (tabela só com colunas de receita/tarifas)")
    print("-" * 78)
    base_tar = pd.DataFrame(
        {
            FRETE_UI_N_VENDA: ["V1", "V2", "V3"],
            FRETE_UI_ANUNCIO: ["MLB111", "MLB222", "MLB222"],
            "Unidades": [1, 2, 1],
            FRETE_UI_VALOR_FRETE_ANUNCIO: [50.0, 10.0, 10.0],
            REC_ENV: [100.0, 30.0, 12.0],
            TAR_ENV: [-50.0, -5.0, -5.0],
        }
    )
    tbl_t = montar_tabela_operacional_com_planilha(
        base_tar,
        receita_col=REC_ENV,
        tarifas_col=TAR_ENV,
    )
    print("\nDetalhe do frete cobrado (cada linha):")
    for _, r in base_tar.iterrows():
        re_v = r[REC_ENV]
        ta_v = r[TAR_ENV]
        fc_v = abs(re_v + ta_v)
        print(
            f"  {r[FRETE_UI_N_VENDA]}: |{REC_ENV} {re_v:.2f} + {TAR_ENV} ({ta_v:.2f})| "
            f"= Frete cobrado {fc_v:.2f}"
        )
    print("\nTabela final (grid conciliacao):")
    print(dataframe_frete_conciliacao_principal(tbl_t).to_string(index=False))
    print("\nResumo numerico:")
    print(tbl_t[cols_show].to_string(index=False))

    div_t = divergencia_mask(tbl_t)
    imp_t = pd.to_numeric(tbl_t.loc[div_t, FRETE_UI_DIFERENCA], errors="coerce").abs().sum()
    print(f"\nLinhas em divergencia: {list(tbl_t.loc[div_t, FRETE_UI_N_VENDA])}")
    print(f"Impacto total |diff|: R$ {imp_t:.2f}")
    print("(Mesmas vendas V1-V3 que no Cenario A: mesma fórmula receita + tarifas.)")

    rank_t, meta_t = ranking_anuncios(tbl_t)
    print("\nRanking por anuncio:\n")
    print(rank_t.to_string(index=False) if not rank_t.empty else "  (vazio)")
    print(
        "\nNota: V0 frete esperado 0 (planilha) → Repasse de frete; V1 OK; V2 cobrado a maior; "
        "V3 cobrado a menor; duas vendas MLB222 no mesmo anuncio."
    )

    print("\n" + "=" * 78)
    print("Fim - regras de negocio simuladas.")
    print("=" * 78 + "\n")


def divergencia_mask(df: pd.DataFrame) -> pd.Series:
    """Divergência de negócio = apenas situação «Cobrado a maior» (repasse não entra)."""
    sit = compute_frete_situacao_frete_column(df)
    return sit.eq(FRETE_UI_ANALISADO_COBRADO_MAIOR)


def ranking_anuncios(tbl_show: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """Espelho de app_operacional._frete_ranking_anuncios_divergencia."""
    if FRETE_UI_ANUNCIO not in tbl_show.columns:
        return pd.DataFrame(), {"aplicavel": False, "motivo": "sem_coluna_anuncio"}
    if tbl_show.empty:
        return pd.DataFrame(), {"aplicavel": False, "motivo": "vazio"}

    div_m = divergencia_mask(tbl_show)
    if not div_m.any():
        return pd.DataFrame(), {
            "aplicavel": True,
            "motivo": "sem_divergencias",
            "n_anuncios_com_div": 0,
            "impacto_total_abs": 0.0,
            "pct_impacto_top10": 0.0,
        }

    sub = tbl_show.loc[div_m].copy()
    sub["_sd"] = pd.to_numeric(sub.get(FRETE_UI_DIFERENCA), errors="coerce").fillna(0.0)
    sub["_ab"] = sub["_sd"].abs()

    grp_full = (
        sub.groupby(FRETE_UI_ANUNCIO, dropna=False)
        .agg(
            linhas_div=(FRETE_UI_N_VENDA, "count"),
            soma_diferenca=("_sd", "sum"),
            impacto_abs=("_ab", "sum"),
        )
        .reset_index()
    )
    vendas_periodo = (
        tbl_show.groupby(FRETE_UI_ANUNCIO, dropna=False).size().reset_index(name="vendas_periodo")
    )
    grp_full = grp_full.merge(vendas_periodo, on=FRETE_UI_ANUNCIO, how="left")
    grp_full["vendas_periodo"] = grp_full["vendas_periodo"].fillna(0).astype(int)
    grp_full["pct_linhas_div"] = (
        (grp_full["linhas_div"] / grp_full["vendas_periodo"].replace(0, pd.NA)).astype(float) * 100.0
    ).round(1)

    impacto_total_abs = float(sub["_ab"].sum())
    n_anuncios_com_div = int(len(grp_full))

    grp_top = grp_full.sort_values("impacto_abs", ascending=False).head(10).copy()
    impacto_top10 = float(grp_top["impacto_abs"].sum())
    pct_conc = (impacto_top10 / impacto_total_abs * 100.0) if impacto_total_abs > 1e-9 else 0.0

    out = pd.DataFrame(
        {
            "Anúncio": grp_top[FRETE_UI_ANUNCIO].map(lambda x: "" if pd.isna(x) else str(x).strip()),
            "Vendas (período)": grp_top["vendas_periodo"],
            "Linhas em divergência": grp_top["linhas_div"].astype(int),
            "% linhas divergentes": grp_top["pct_linhas_div"],
            "Soma diferença (R$)": grp_top["soma_diferenca"].round(2),
            "Impacto |diff| (R$)": grp_top["impacto_abs"].round(2),
        }
    )

    meta = {
        "aplicavel": True,
        "motivo": "ok",
        "n_anuncios_com_div": n_anuncios_com_div,
        "impacto_total_abs": impacto_total_abs,
        "pct_impacto_top10": round(pct_conc, 1),
        "impacto_top10": impacto_top10,
    }
    return out, meta


def _tbl_base_com_anuncio() -> pd.DataFrame:
    """
    Simula saída de carregar_base_frete_ml com planilha de frete por anúncio:
    - frete cobrado (FRETE_ML_COL) = valor já calculado no loader
    - frete_esperado = qtd * preço arquivo
    - diferenca = frete_esperado - frete cobrado
    - status OK / Divergência com tolerância 0.02 no motor (replicamos os valores finais).
    """
    rows = [
        # 1) Sem divergência: diff = esperado − cobrado = 0
        ("V1", "MLB111", 50.0, 50.0, 0.0, "OK", "Entregue"),
        # 2) diff = 55 − 60 = −5 (|Δ| > tol → Divergência)
        ("V2", "MLB222", 60.0, 55.0, -5.0, FRETE_UI_VAL_DIVERGENCIA, "Entregue"),
        # 3) diff = 45 − 40 = 5
        ("V3", "MLB222", 40.0, 45.0, 5.0, FRETE_UI_VAL_DIVERGENCIA, "Entregue"),
        # 4) diff = 99.99 − 100 = −0.01 → |Δ| ≤ 0.02 → OK
        ("V4", "MLB333", 100.0, 99.99, -0.01, "OK", "Entregue"),
        # 5) diff = 9.97 − 10 = −0.03 — com coluna status pré-fixada a OK (cenário legado)
        ("V5", "MLB444", 10.0, 9.97, -0.03, "OK", "Entregue"),
        # 6) diff = 180 − 200 = −20
        ("V6", "MLB555", 200.0, 180.0, -20.0, FRETE_UI_VAL_DIVERGENCIA, "Entregue"),
    ]
    df = pd.DataFrame(
        rows,
        columns=[
            FRETE_UI_N_VENDA,
            FRETE_UI_ANUNCIO,
            FRETE_ML_COL,
            FRETE_UI_FRETE_ESPERADO,
            FRETE_UI_DIFERENCA,
            FRETE_UI_STATUS_CONC,
            "Estado",
        ],
    )
    return df


def _tbl_sem_status_so_diff() -> pd.DataFrame:
    """Sem coluna Status: divergência só por |diferença| > 0.02 (fallback)."""
    df = _tbl_base_com_anuncio().drop(columns=[FRETE_UI_STATUS_CONC])
    # Ajustar linha V5: 0.03 > 0.02 → passa a contar como divergência sem status
    return df


def _tbl_sem_coluna_anuncio() -> pd.DataFrame:
    df = _tbl_base_com_anuncio()
    return df.drop(columns=[FRETE_UI_ANUNCIO])


def main() -> None:
    parser = argparse.ArgumentParser(description="Simulação lógica Frete (regras + paridade UI).")
    parser.add_argument(
        "--regras-only",
        action="store_true",
        help="Só a simulação explícita das regras de negócio (sem cenários legados A–D).",
    )
    parser.add_argument(
        "--legacy-only",
        action="store_true",
        help="Só os cenários legados de paridade (tabela pré-calculada).",
    )
    args = parser.parse_args()

    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", 200)

    if not args.legacy_only:
        simulacao_regras_negocio_completa()

    if args.regras_only:
        return

    print("=" * 72)
    print("SIMULAÇÃO — Lógica Frete (paridade app_operacional)")
    print("=" * 72)
    print(
        "\nMotor real (carregar_base_frete_ml): frete cobrado = |receita + tarifas| (sempre ≥ 0); "
        "custo do envio não entra; "
        "frete_esperado = qtd × preço unit. arquivo; diferenca = frete_esperado − frete cobrado; "
        f"status com tolerância { _FRETE_DIV_TOL }.\n"
        "Abaixo usamos a **tabela já calculada** (como após o loader).\n"
    )

    # --- Com planilha (coluna Status) ---
    tbl = _tbl_base_com_anuncio()
    print("--- Cenário A: COM planilha frete por anúncio (colunas Status + Diferença + # anúncio) ---\n")
    print("Tabela operacional (colunas internas / grid conciliação):")
    ui = dataframe_frete_conciliacao_principal(tbl)
    print(ui.to_string())
    print()

    div_m = divergencia_mask(tbl)
    print("Mascara divergencia (linha -> True):")
    print(div_m.tolist(), "linhas divergentes:", list(tbl.loc[div_m, FRETE_UI_N_VENDA]))
    print(
        "Nota: V5 tem |diff|=0.03 mas status OK -- nao e divergencia quando existe coluna Status "
        "(regra do app).\n"
    )

    impacto_total = pd.to_numeric(tbl.loc[div_m, FRETE_UI_DIFERENCA], errors="coerce").abs().sum()
    print(f"Impacto total |diff| (linhas em divergencia): R$ {impacto_total:.2f} (esperado: 5+5+20 = 30)\n")

    rank_df, meta = ranking_anuncios(tbl)
    print("Meta ranking:", meta)
    print("\nRanking Top 10 (por impacto |diff|):\n", rank_df.to_string(index=False))
    # MLB222: |5|+|-5| = 10; MLB555: 20
    print("\nVerificacao manual:")
    print("  MLB222: 2 linhas div, soma |diff| = 10.0, 2/2 vendas no periodo para MLB222 -> 100%")
    print("  MLB555: impacto 20.0")
    print(f"  Concentração Top10: {meta['pct_impacto_top10']}% (deve ser 100% com 2 anúncios)\n")

    # --- Sem planilha (sem Status): fallback |Δ|>0.02 ---
    tbl2 = _tbl_sem_status_so_diff()
    print("--- Cenário B: SEM coluna Status (fallback |diferença| > 0.02) ---\n")
    div2 = divergencia_mask(tbl2)
    print("Divergencia por linha (V4 diff 0.01 nao; V5 diff 0.03 sim):", div2.tolist())
    r2, m2 = ranking_anuncios(tbl2)
    print("\nRanking:\n", r2.to_string(index=False))
    print("Meta:", m2)
    print()

    # --- Sem coluna anúncio ---
    tbl3 = _tbl_sem_coluna_anuncio()
    r3, m3 = ranking_anuncios(tbl3)
    print("--- Cenário C: sem coluna # anúncio ---\n", m3)
    print()

    # --- Só OK (sem divergência) ---
    tbl_ok = tbl[tbl[FRETE_UI_N_VENDA].isin(["V1", "V4"])].copy()
    tbl_ok[FRETE_UI_DIFERENCA] = [0.0, 0.0]
    tbl_ok[FRETE_UI_STATUS_CONC] = ["OK", "OK"]
    r_ok, m_ok = ranking_anuncios(tbl_ok)
    print("--- Cenário D: apenas vendas sem divergência ---\n", m_ok)
    print()

    print("=" * 72)
    print("Fim da simulação.")
    print("=" * 72)


if __name__ == "__main__":
    main()
