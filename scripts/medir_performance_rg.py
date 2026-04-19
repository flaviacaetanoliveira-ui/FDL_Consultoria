#!/usr/bin/env python3
"""
Medição de performance do Resultado Gerencial (funções Python; sem UI Streamlit).

Uso:
    python scripts/medir_performance_rg.py ^
        --parquet data_products/cliente_2/faturamento/current/dataset.parquet ^
        --empresa "Gama Home" ^
        --data-inicio 2026-01-01 ^
        --data-fim 2026-04-17
"""

from __future__ import annotations

import argparse
import io
import logging
import os
import pickle
import pstats
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date
from pathlib import Path

# Repositório na import path
_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import pandas as pd  # noqa: E402
import streamlit as st  # noqa: E402

# Suprime aviso de cache sem runtime (MemoryCacheStorageManager)
os.environ.setdefault("STREAMLIT_BROWSER_GATHER_USAGE_STATS", "false")
logging.getLogger("streamlit").disabled = True
logging.getLogger("streamlit.runtime.caching.cache_data_api").disabled = True


@dataclass
class RunStats:
    mn: float
    mx: float
    avg: float


def _stats_from_runs(times: list[float]) -> RunStats:
    return RunStats(mn=min(times), mx=max(times), avg=sum(times) / len(times))


def _fmt_s(x: float) -> str:
    return f"{x:.2f}".replace(".", ",")


def _mb_nbytes(obj: object) -> float:
    b = len(pickle.dumps(obj, protocol=pickle.HIGHEST_PROTOCOL))
    return b / (1024 * 1024)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Mede performance RG com Parquet real.")
    p.add_argument("--parquet", required=True, type=Path, help="Caminho dataset.parquet (grão linha)")
    p.add_argument("--empresa", default="Gama Home", help="Empresa (recorte RG)")
    p.add_argument("--empresa-alt", default="Mega Star", help="Outra empresa para cenário cache miss")
    p.add_argument("--data-inicio", default="2026-01-01")
    p.add_argument("--data-fim", default="2026-04-17")
    p.add_argument(
        "--data-fim-periodo-alt",
        default="2026-06-30",
        help="Data fim alternativa para cenário mudança de período (cache miss)",
    )
    p.add_argument("--cliente-slug", default="cliente_2", help="Slug YAML ficha / chave cache")
    p.add_argument("--fiscal-imposto", type=float, default=0.0, help="Imposto fiscal rateado (ponte), como no app")
    p.add_argument("--pipeline-version", default=None, help="Override FDL_RG_PIPELINE_VERSION")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    if args.pipeline_version:
        os.environ["FDL_RG_PIPELINE_VERSION"] = args.pipeline_version

    from app.components.rg_cached_compute import cached_rg_slice_kpis_tabela, pipeline_version as rg_pv
    from processing.faturamento.ficha_pedido_rg import compute_ficha_pedido, load_resultado_gerencial_config
    from processing.faturamento.resultado_gerencial_slice import (
        build_resultado_gerencial_slice,
        compute_resultado_gerencial_kpis,
        compute_tabela_por_pedido,
    )
    from processing.faturamento.rg_cache_keys import normalize_sorted_str_tuple

    d_ini = date.fromisoformat(args.data_inicio)
    d_fim = date.fromisoformat(args.data_fim)
    d_fim_alt = date.fromisoformat(args.data_fim_periodo_alt)

    emp = normalize_sorted_str_tuple([args.empresa])
    emp_alt = normalize_sorted_str_tuple([args.empresa_alt])
    plat: tuple[str, ...] = ()
    pv = rg_pv()
    slug = str(args.cliente_slug).strip()
    fiscal = float(args.fiscal_imposto)

    rg_conf = load_resultado_gerencial_config(slug)

    print(f"MEDIÇÃO DE PERFORMANCE — RG · {args.empresa} · {args.data_inicio} a {args.data_fim}")
    print("=" * 70)

    # --- Cenário 1: cold (parquet + primeira chamada cacheada = compute completo)
    times_c1: list[float] = []
    df_shape = (0, 0)
    n_pedidos = 0
    bundle_ref: tuple | None = None

    for _ in range(3):
        st.cache_data.clear()
        t0 = time.perf_counter()
        df = pd.read_parquet(args.parquet)
        bundle_ref = cached_rg_slice_kpis_tabela(
            df,
            emp,
            plat,
            d_ini,
            d_fim,
            fiscal,
            pv,
            slug,
        )
        t1 = time.perf_counter()
        times_c1.append(t1 - t0)
        df_shape = df.shape
        n_pedidos = len(bundle_ref[2])

    st1 = _stats_from_runs(times_c1)

    # --- Cenário 2: cache hit (segunda chamada; df já em memória; mesma chave)
    times_c2: list[float] = []
    df_mem = pd.read_parquet(args.parquet)
    # aquece uma vez
    st.cache_data.clear()
    cached_rg_slice_kpis_tabela(df_mem, emp, plat, d_ini, d_fim, fiscal, pv, slug)
    for _ in range(3):
        t0 = time.perf_counter()
        _ = cached_rg_slice_kpis_tabela(df_mem, emp, plat, d_ini, d_fim, fiscal, pv, slug)
        t1 = time.perf_counter()
        times_c2.append(t1 - t0)
    st2 = _stats_from_runs(times_c2)

    _sl, _kp, tab = bundle_ref  # type: ignore[misc]
    pedidos_tab = list(tab)
    if len(pedidos_tab) < 6:
        print("ERRO: menos de 6 pedidos no recorte — não dá para cenário 6.", file=sys.stderr)
        return 2
    pid1 = pedidos_tab[0].pedido_id
    pids5 = [p.pedido_id for p in pedidos_tab[:5]]

    # --- Cenário 3: uma ficha (mede só compute_ficha_pedido; bundle via cache hit)
    times_c3: list[float] = []
    for _ in range(3):
        st.cache_data.clear()
        cached_rg_slice_kpis_tabela(df_mem, emp, plat, d_ini, d_fim, fiscal, pv, slug)
        sl_c3, _, tab_c3 = cached_rg_slice_kpis_tabela(df_mem, emp, plat, d_ini, d_fim, fiscal, pv, slug)
        linhas_full = list(tab_c3)
        t0 = time.perf_counter()
        _ = compute_ficha_pedido(
            sl_c3,
            pedido_id=pid1,
            fiscal_imposto_valor=fiscal,
            pedidos_contexto=linhas_full,
            rg_config=rg_conf,
            tab_linhas_full=linhas_full,
        )
        t1 = time.perf_counter()
        times_c3.append(t1 - t0)
    st3 = _stats_from_runs(times_c3)

    # --- Cenário 4: mudança empresa (miss após clear)
    times_c4: list[float] = []
    for _ in range(3):
        st.cache_data.clear()
        t0 = time.perf_counter()
        df4 = pd.read_parquet(args.parquet)
        _ = cached_rg_slice_kpis_tabela(df4, emp_alt, plat, d_ini, d_fim, fiscal, pv, slug)
        t1 = time.perf_counter()
        times_c4.append(t1 - t0)
    st4 = _stats_from_runs(times_c4)

    # --- Cenário 5: mesmo empresa base, período diferente (miss após clear)
    times_c5: list[float] = []
    for _ in range(3):
        st.cache_data.clear()
        t0 = time.perf_counter()
        df5 = pd.read_parquet(args.parquet)
        _ = cached_rg_slice_kpis_tabela(df5, emp, plat, d_ini, d_fim_alt, fiscal, pv, slug)
        t1 = time.perf_counter()
        times_c5.append(t1 - t0)
    st5 = _stats_from_runs(times_c5)

    # --- Cenário 6: 5 fichas sequenciais
    st.cache_data.clear()
    b6 = cached_rg_slice_kpis_tabela(
        pd.read_parquet(args.parquet),
        emp,
        plat,
        d_ini,
        d_fim,
        fiscal,
        pv,
        slug,
    )
    sl6, _, tab6 = b6
    lin6 = list(tab6)
    times_each: list[float] = []
    for pid in pids5:
        t0 = time.perf_counter()
        _ = compute_ficha_pedido(
            sl6,
            pedido_id=pid,
            fiscal_imposto_valor=fiscal,
            pedidos_contexto=lin6,
            rg_config=rg_conf,
            tab_linhas_full=lin6,
        )
        t1 = time.perf_counter()
        times_each.append(t1 - t0)
    total_c6 = sum(times_each)
    st6_each = _stats_from_runs(times_each)  # min/máx/médio por ficha entre as 5

    # Tamanho aproximado do objeto cacheado
    mb_bundle = _mb_nbytes(bundle_ref)

    print()
    print(f"Dataset (após leitura): {df_shape[0]} linhas no grão linha")
    print(f"Pedidos no recorte ({args.empresa}): {n_pedidos}")
    print(f"Versão pipeline (chave cache): {pv}")
    print(f"Tamanho aprox. bundle (slice+KPIs+tab) pickle: {_fmt_s(mb_bundle)} MB")
    print()

    meta = {
        "c1": 5.0,
        "c2": 0.5,
        "c3": 0.8,
        "c4": 3.0,
        "c5": 3.0,
        "c6": 3.0,
    }

    def line(title: str | None, stx: RunStats, m: float, note: str = "") -> None:
        ok = stx.mx <= m
        status = "[OK] atingida" if ok else "[FALHA] acima do alvo"
        if title:
            print(title)
        print(f"  Mínimo: {_fmt_s(stx.mn)} s")
        print(f"  Máximo: {_fmt_s(stx.mx)} s")
        print(f"  Médio:  {_fmt_s(stx.avg)} s")
        print(f"  Meta:   < {_fmt_s(m)} s")
        print(f"  Status: {status} {note}".rstrip())
        print()

    line("Cenário 1 — Abertura inicial (cold: parquet + 1ª chamada agregação)", st1, meta["c1"])

    line("Cenário 2 — Paginar (cache hit: 2ª chamada, df em RAM, mesma chave)", st2, meta["c2"])

    line("Cenário 3 — Abrir 1 ficha (compute_ficha_pedido + tab_linhas_full)", st3, meta["c3"])

    line(f"Cenário 4 — Mudar empresa (cold: parquet + miss: {args.empresa_alt})", st4, meta["c4"])

    line(f"Cenário 5 — Mudar período (cold: parquet + miss: até {d_fim_alt.isoformat()})", st5, meta["c5"])

    print("Cenário 6 — 5 fichas sequenciais (tempos individuais + total)")
    print(f"  Por ficha — Mínimo: {_fmt_s(st6_each.mn)} s · Máximo: {_fmt_s(st6_each.mx)} s · Médio: {_fmt_s(st6_each.avg)} s")
    print(f"  Total 5 fichas: {_fmt_s(total_c6)} s")
    print(f"  Meta total: < {_fmt_s(meta['c6'])} s")
    ok6 = total_c6 <= meta["c6"]
    print(f"  Status: {'[OK] atingida' if ok6 else '[FALHA] acima do alvo'}")
    print()

    failures: list[str] = []
    if st1.mx > meta["c1"]:
        failures.append("c1")
    if st2.mx > meta["c2"]:
        failures.append("c2")
    if st3.mx > meta["c3"]:
        failures.append("c3")
    if st4.mx > meta["c4"]:
        failures.append("c4")
    if st5.mx > meta["c5"]:
        failures.append("c5")
    if total_c6 > meta["c6"]:
        failures.append("c6")

    if failures:
        print("=" * 70)
        print("DIAGNÓSTICO (metas não atingidas):", ", ".join(failures))
        _run_diagnostic(
            args.parquet,
            emp,
            plat,
            d_ini,
            d_fim,
            fiscal,
            pv,
            slug,
            build_resultado_gerencial_slice,
            compute_resultado_gerencial_kpis,
            compute_tabela_por_pedido,
        )

    return 0


def _run_diagnostic(
    parquet: Path,
    emp: tuple[str, ...],
    plat: tuple[str, ...],
    d_ini: date,
    d_fim: date,
    fiscal: float,
    pv: str,
    slug: str,
    build_slice: Callable,
    compute_kpis: Callable,
    compute_tab: Callable,
) -> None:
    import cProfile

    st.cache_data.clear()
    pr = cProfile.Profile()
    pr.enable()
    t0 = time.perf_counter()
    df = pd.read_parquet(parquet)
    t_r = time.perf_counter()
    sl = build_slice(df, empresas_sel=emp, plataformas_sel=plat, data_venda_ini=d_ini, data_venda_fim=d_fim)
    t_s = time.perf_counter()
    kp = compute_kpis(sl, fiscal_imposto_valor=fiscal)
    t_k = time.perf_counter()
    tab = compute_tab(sl, fiscal_imposto_valor=fiscal)
    t_t = time.perf_counter()
    pr.disable()

    print("Tempo aninhado (uma amostra, sem cache decorator):")
    print(f"  read_parquet: {t_r - t0:,.4f}s".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  build_resultado_gerencial_slice: {t_s - t_r:,.4f}s".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  compute_resultado_gerencial_kpis: {t_k - t_s:,.4f}s".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  compute_tabela_por_pedido: {t_t - t_k:,.4f}s".replace(",", "X").replace(".", ",").replace("X", "."))
    print()

    s = io.StringIO()
    pstats.Stats(pr, stream=s).sort_stats("cumtime").print_stats(25)
    print(s.getvalue())

    bundle = (sl, kp, tab)
    mb = len(pickle.dumps(bundle, protocol=pickle.HIGHEST_PROTOCOL)) / (1024 * 1024)
    print(f"Tamanho pickle bundle (slice+kp+tab): {mb:,.2f} MB".replace(",", "X").replace(".", ",").replace("X", "."))
    print()
    print("Hipóteses: leitura Parquet e/ou compute_tabela_por_pedido costumam dominar em bases grandes;")
    print("cache hit lento sugere objeto grande ao desserializar; fichas repetidas — perfil compute_benchmarks_*.")
    print()


if __name__ == "__main__":
    raise SystemExit(main())
