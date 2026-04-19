"""
Validação offline do checklist Etapa 5 — Gama Home · mar/2026 · todas plataformas.
Usa os Parquets em data_products/cliente_2/faturamento/current/ (mesma fonte típica do app).

Executar na raiz do repo:
  python scripts/validate_rg_checklist_gama_mar2026.py
"""

from __future__ import annotations

import time
from datetime import date
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
_ROOT = _REPO / "data_products/cliente_2/faturamento/current"
_DF_LINE = _ROOT / "dataset.parquet"
_DF_FISCAL = _ROOT / "dataset_faturamento_fiscal.parquet"
_DF_NF_PANEL = _ROOT / "dataset_faturamento_nf_panel.parquet"


def _nf_apply_minimal_recorte(
    df_nf: pd.DataFrame,
    *,
    empresas_sel: tuple[str, ...],
    plataformas_sel: tuple[str, ...],
    nf_d_ini: date,
    nf_d_fim: date,
    ok_nf_dates: bool,
) -> pd.DataFrame:
    """Espelho de ``_faturamento_nf_apply_minimal_recorte`` sem importar ``app_operacional``."""
    from faturamento_dre_recorte import _fdl_fr_mask_nf_emissao_no_periodo
    from faturamento_dre_recorte_minimo import nf_grain_plataforma_match_key

    if df_nf.empty:
        return df_nf
    out = df_nf.copy()
    sel_emp = [str(x).strip() for x in empresas_sel if str(x).strip()]
    if sel_emp and "empresa" in out.columns:
        sel_cf = {x.casefold() for x in sel_emp}
        em_cf = out["empresa"].fillna("").astype(str).str.strip().str.casefold()
        out = out.loc[em_cf.isin(sel_cf)].copy()
    sel_plat = [str(x).strip() for x in plataformas_sel if str(x).strip()]
    _plat_col = (
        "plataforma"
        if "plataforma" in out.columns
        else (
            "plataforma_resumo"
            if "plataforma_resumo" in out.columns
            else ("Nome da plataforma" if "Nome da plataforma" in out.columns else "")
        )
    )
    if sel_plat and _plat_col:
        want = {nf_grain_plataforma_match_key(x) for x in sel_plat}
        want.discard("")
        if want:
            got = out[_plat_col].map(nf_grain_plataforma_match_key)
            out = out.loc[got.isin(want)].copy()
    if ok_nf_dates and nf_d_fim >= nf_d_ini and "Nota_Data_Emissao" in out.columns:
        m = _fdl_fr_mask_nf_emissao_no_periodo(out["Nota_Data_Emissao"], nf_d_ini, nf_d_fim)
        out = out.loc[m].copy()
    return out


def _nf_filter_by_situacao(df_nf: pd.DataFrame, situacoes_sel: tuple[str, ...]) -> pd.DataFrame:
    if df_nf.empty or not situacoes_sel:
        return df_nf
    want = {str(x).strip().casefold() for x in situacoes_sel if str(x).strip()}
    if not want or "Nota_Situacao" not in df_nf.columns:
        return df_nf
    s = df_nf["Nota_Situacao"].fillna("").astype(str).str.strip()
    return df_nf.loc[s.str.casefold().isin(want)].copy()


def main() -> int:
    from comercial_pedidos_analise import pedido_id_series
    from faturamento_dre_recorte_minimo import (
        build_faturamento_fiscal_base_slice,
        build_nf_panel_aligned_to_fiscal_base,
        compute_nf_panel_kpis,
        dre_imposto_para_linha_dre_gerencial,
    )
    from processing.faturamento.resultado_gerencial_slice import (
        REQUIRED_LINE_COLUMNS,
        build_resultado_gerencial_slice,
        compute_resultado_gerencial_kpis,
        compute_tabela_por_pedido,
    )

    d_ini, d_fim = date(2026, 3, 1), date(2026, 3, 31)
    emp = ("Gama Home",)
    plats: tuple[str, ...] = ()

    if not _DF_LINE.is_file():
        print(f"ERRO: não encontrado {_DF_LINE}")
        return 1

    df = pd.read_parquet(_DF_LINE, engine="pyarrow")
    miss = REQUIRED_LINE_COLUMNS - set(df.columns)
    if miss:
        print(f"ERRO: faltam colunas RG no dataset: {sorted(miss)}")
        return 1

    use_fiscal = _DF_FISCAL.is_file()
    use_panel = _DF_NF_PANEL.is_file()
    df_fiscal = pd.read_parquet(_DF_FISCAL, engine="pyarrow") if use_fiscal else pd.DataFrame()
    df_nf_pre = pd.read_parquet(_DF_NF_PANEL, engine="pyarrow") if use_panel else pd.DataFrame()

    ok_nf_dates = True
    situacoes_nf: tuple[str, ...] = ()

    _df_fb, fst = build_faturamento_fiscal_base_slice(
        df_fiscal,
        empresas_sel=emp,
        nf_d_ini=d_ini,
        nf_d_fim=d_fim,
        ok_nf_dates=ok_nf_dates,
        situacoes_sel=situacoes_nf,
        df_devolucoes=None,
    )
    df_nf_scope = _nf_apply_minimal_recorte(
        df_nf_pre,
        empresas_sel=emp,
        plataformas_sel=(),
        nf_d_ini=d_ini,
        nf_d_fim=d_fim,
        ok_nf_dates=ok_nf_dates,
    )
    df_nf_scope = _nf_filter_by_situacao(df_nf_scope, situacoes_nf)
    aligned_kpi = (
        build_nf_panel_aligned_to_fiscal_base(_df_fb, df_nf_scope)
        if not _df_fb.empty
        else df_nf_scope.copy()
    )
    kp_cards = compute_nf_panel_kpis(aligned_kpi)
    use_fiscal_kpi = bool(use_fiscal and fst is not None and fst.n_nf > 0)
    imp_rg = dre_imposto_para_linha_dre_gerencial(
        kp_cards,
        fiscal_base_stats=fst if use_fiscal else None,
        aplicar_ponte_base_liquida=(fst is not None and use_fiscal_kpi),
    )

    sl = build_resultado_gerencial_slice(
        df,
        empresas_sel=emp,
        plataformas_sel=plats,
        data_venda_ini=d_ini,
        data_venda_fim=d_fim,
    )
    kp = compute_resultado_gerencial_kpis(sl, fiscal_imposto_valor=float(imp_rg))

    t0 = time.perf_counter()
    tab = compute_tabela_por_pedido(sl, fiscal_imposto_valor=float(imp_rg))
    t_tbl = time.perf_counter() - t0

    soma_rec = sum(x.receita for x in tab)
    soma_res = sum(x.resultado for x in tab)
    margem_pond = (soma_res / soma_rec * 100.0) if soma_rec else 0.0
    d_rec = abs(soma_rec - float(kp["valor_venda_lista"]))
    d_res = abs(soma_res - float(kp["resultado"]))

    print("=== Checklist 1 - Coerencia (dataset local) ===")
    print(f"Linhas na tabela (pedidos): {len(tab)} (KPI pedidos={kp['pedidos']})")
    print(f"Soma Receita: {soma_rec:,.2f} (KPI valor_venda_lista={float(kp['valor_venda_lista']):,.2f}) d_rec={d_rec:.6f}")
    print(f"Soma Resultado: {soma_res:,.2f} (KPI resultado={float(kp['resultado']):,.2f}) d_res={d_res:.6f}")
    print(f"Margem ponderada: {margem_pond:.2f}% (KPI margem={float(kp['margem'])*100:.2f}%)")
    print(f"Divergencia OK (<0.02): receita {'SIM' if d_rec < 0.02 else 'NAO'}, resultado {'SIM' if d_res < 0.02 else 'NAO'}")
    print(f"compute_tabela_por_pedido: {t_tbl*1000:.1f} ms")

    print("\n=== Checklist 2 - Nota_Situacao no slice (df_linha) ===")
    if "Nota_Situacao" in sl.df_linha.columns:
        vc = sl.df_linha["Nota_Situacao"].value_counts(dropna=False)
        print(vc.to_string())
    else:
        print("(coluna Nota_Situacao ausente)")

    st_counts: dict[str, int] = {}
    for p in tab:
        st_counts[p.status_nf] = st_counts.get(p.status_nf, 0) + 1
    print("\nDistribuicao por rotulo (tabela por pedido):")
    for k in sorted(st_counts.keys()):
        print(f"  {k}: {st_counts[k]}")

    parciais = [p for p in tab if p.status_nf == "parcial"]
    print(f"\nPedidos 'parcial': {len(parciais)}")
    if parciais:
        ex = parciais[0]
        pid = ex.pedido_id
        m = pedido_id_series(sl.df_linha).astype(str).str.strip().eq(pid)
        sub = sl.df_linha.loc[m, ["Número do pedido", "Nota_Situacao"] if "Nota_Situacao" in sl.df_linha.columns else ["Número do pedido"]]
        print(f"Exemplo pedido_id={pid}")
        print(sub.to_string())

    print("\n=== Checklist 3 - Filtros (logica espelhada em Python) ===")
    lucro = [p for p in tab if p.resultado > 1e-9]
    prej = [p for p in tab if p.resultado < -1e-9]
    print(f"So lucro (simulado): {len(lucro)} / So prejuizo: {len(prej)} / Total: {len(tab)}")

    dig = "001"
    qdig = [p for p in tab if dig in (p.numero_pedido_ui or "") or dig in p.pedido_id]
    print(f"Busca substring '{dig}' em n pedido/id: {len(qdig)} pedidos")

    print("\n=== Checklist 4 - Paginacao / CSV ===")
    per_page = 50
    n_pages = max(1, (len(tab) + per_page - 1) // per_page)
    print(f"Paginas (50/pagina): {n_pages} para {len(tab)} pedidos")
    print("Nome ficheiro esperado na UI: pedidos_<org>_<ano>-<mes>.csv -> pedidos_gama_home_2026-03.csv")

    print("\n=== Checklist 5 - Tempos (so backend agregacao) ===")
    print(f"compute_tabela_por_pedido: {t_tbl*1000:.0f} ms (Streamlit nao medido aqui)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
