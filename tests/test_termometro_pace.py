"""Ciclo D · termômetro de pace mensal — cálculo e modos."""

from __future__ import annotations

from datetime import date

import pandas as pd

from processing.faturamento.pace_mensal import (
    PaceMensal,
    compute_pace_mensal,
    compute_trailing_monthly_revenues,
    determinar_modo,
)
from processing.faturamento.resultado_gerencial_slice import (
    ResultadoGerencialSlice,
    ResultadoGerencialSliceMeta,
    ResultadoGerencialSliceStats,
)


def _slice(
    rec: float,
    d0: date,
    d1: date,
    *,
    n_linhas: int = 5,
    empresas: tuple[str, ...] = ("Gama Home",),
) -> ResultadoGerencialSlice:
    st = ResultadoGerencialSliceStats(
        receita_total=rec,
        comissao_total=0.0,
        frete_plataforma_total=0.0,
        frete_transportadora_propria_total=0.0,
        cmv_total=0.0,
        resultado_linhas_total=0.0,
        despesa_fixa_total=0.0,
        ads_total=0.0,
        n_linhas=n_linhas,
        n_pedidos_unicos=1,
    )
    meta = ResultadoGerencialSliceMeta(
        empresas_sel=empresas,
        plataformas_sel=(),
        data_venda_ini=d0,
        data_venda_fim=d1,
    )
    return ResultadoGerencialSlice(
        df_linha=pd.DataFrame({"Data": [d0]}),
        pedido_ids=pd.Series(["1"]),
        stats=st,
        meta=meta,
    )


def test_calculo_projecao_linear_simples() -> None:
    sl = _slice(94_120.0, date(2026, 3, 1), date(2026, 3, 31))
    hoje = date(2026, 3, 18)
    pace = compute_pace_mensal(
        sl,
        [80_000.0, 90_000.0, 88_000.0],
        {},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        hoje,
    )
    assert pace is not None
    assert pace.modo == "mes_corrente"
    assert pace.projecao_linear is not None
    assert abs(pace.projecao_linear - (94_120.0 * 31.0 / 18.0)) < 1.0


def test_desvio_vs_meta() -> None:
    sl = _slice(94_120.0, date(2026, 3, 1), date(2026, 3, 31))
    hoje = date(2026, 3, 18)
    proj = 94_120.0 * 31.0 / 18.0
    pace = compute_pace_mensal(
        sl,
        [80_000.0, 90_000.0, 88_000.0],
        {
            "pace": {
                "meta_mensal": {"gama_home": 170_000.0},
            }
        },
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        hoje,
    )
    assert pace is not None and pace.desvio_projecao_pct is not None
    assert abs(pace.desvio_projecao_pct - (proj / 170_000.0 - 1.0)) < 1e-6


def test_ritmo_necessario_quando_abaixo_da_meta() -> None:
    sl = _slice(94_120.0, date(2026, 3, 1), date(2026, 3, 31))
    hoje = date(2026, 3, 18)
    pace = compute_pace_mensal(
        sl,
        [80_000.0, 90_000.0, 88_000.0],
        {
            "pace": {
                "meta_mensal": {"gama_home": 170_000.0},
            }
        },
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        hoje,
    )
    assert pace is not None and pace.ritmo_necessario_diario is not None
    assert abs(pace.ritmo_necessario_diario - (170_000.0 - 94_120.0) / 13.0) < 0.02


def test_modo_mes_corrente_vs_recorte_parcial() -> None:
    assert determinar_modo(date(2026, 3, 1), date(2026, 3, 31), date(2026, 3, 18)) == "mes_corrente"
    assert determinar_modo(date(2026, 3, 1), date(2026, 3, 15), date(2026, 3, 20)) == "recorte_parcial"


def test_modo_mes_fechado() -> None:
    assert determinar_modo(date(2026, 1, 1), date(2026, 1, 31), date(2026, 2, 5)) == "mes_fechado"


def test_meta_origem_yaml() -> None:
    sl = _slice(50_000.0, date(2026, 3, 1), date(2026, 3, 31), empresas=("Acme",))
    pace = compute_pace_mensal(
        sl,
        [10_000.0, 10_000.0, 10_000.0],
        {"pace": {"meta_mensal": {"acme": 99_000.0}}},
        ["Acme"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 10),
    )
    assert pace is not None
    assert pace.meta_mensal == 99_000.0
    assert pace.meta_origem == "yaml"


def test_meta_origem_ma3() -> None:
    sl = _slice(40_000.0, date(2026, 3, 1), date(2026, 3, 31))
    hist = [90_000.0, 96_000.0, 93_000.0]
    pace = compute_pace_mensal(
        sl,
        hist,
        {},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 10),
    )
    assert pace is not None
    assert pace.meta_origem == "ma3"
    assert abs(float(pace.meta_mensal or 0) - sum(hist[-3:]) / 3.0) < 1e-6


def test_meta_origem_sem_meta() -> None:
    sl = _slice(40_000.0, date(2026, 3, 1), date(2026, 3, 31))
    pace = compute_pace_mensal(
        sl,
        [90_000.0, 96_000.0],
        {},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 10),
    )
    assert pace is not None
    assert pace.meta_mensal is None
    assert pace.meta_origem == "sem_meta"


def test_nivel_alerta_critico_queda_10_pp() -> None:
    meta = 100_000.0
    dia = 10
    desvio_needed = -0.11
    rec = (1.0 + desvio_needed) * meta * dia / 31.0
    sl = _slice(rec, date(2026, 3, 1), date(2026, 3, 31))
    pace = compute_pace_mensal(
        sl,
        [80_000.0, 80_000.0, 80_000.0],
        {"pace": {"meta_mensal": {"gama_home": meta}}},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, dia),
    )
    assert pace is not None
    assert pace.desvio_projecao_pct is not None and pace.desvio_projecao_pct < -0.10
    assert pace.nivel_alerta == "critico"


def test_nivel_alerta_atencao_queda_4pp() -> None:
    meta = 170_000.0
    proj_target = meta * (1.0 - 0.047)
    rec = proj_target * 18.0 / 31.0
    sl = _slice(rec, date(2026, 3, 1), date(2026, 3, 31))
    pace = compute_pace_mensal(
        sl,
        [160_000.0, 160_000.0, 160_000.0],
        {"pace": {"meta_mensal": {"gama_home": meta}}},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 18),
    )
    assert pace is not None
    assert pace.nivel_alerta == "atencao"


def test_nivel_alerta_ok_positivo_acima_5pct() -> None:
    meta = 100_000.0
    proj_target = meta * 1.08
    rec = proj_target * 10.0 / 31.0
    sl = _slice(rec, date(2026, 3, 1), date(2026, 3, 31))
    pace = compute_pace_mensal(
        sl,
        [90_000.0, 90_000.0, 90_000.0],
        {"pace": {"meta_mensal": {"gama_home": meta}}},
        ["Gama Home"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 10),
    )
    assert pace is not None
    assert pace.nivel_alerta == "ok_positivo"


def test_consolidado_multiplas_empresas() -> None:
    sl = _slice(
        10_000.0,
        date(2026, 3, 1),
        date(2026, 3, 31),
        empresas=("Gama Home", "Mega Star"),
    )
    pace = compute_pace_mensal(
        sl,
        [50_000.0, 50_000.0, 50_000.0],
        {
            "pace": {
                "meta_mensal": {
                    "gama_home": 100_000.0,
                    "mega_star": 50_000.0,
                },
            }
        },
        ["Gama Home", "Mega Star"],
        date(2026, 3, 1),
        date(2026, 3, 31),
        date(2026, 3, 20),
    )
    assert pace is not None
    assert pace.meta_mensal == 150_000.0
    assert pace.meta_origem == "yaml"


def test_compute_trailing_smoke_from_frame() -> None:
    df = pd.DataFrame(
        {
            "Data": pd.to_datetime(
                ["2025-12-15", "2026-01-10", "2026-02-05", "2026-03-02"],
            ),
            "Valor total": [10_000.0, 20_000.0, 30_000.0, 5_000.0],
            "Taxa de Comissão": [0, 0, 0, 0],
            "Frete_Plataforma": [0, 0, 0, 0],
            "Custo_Produto_Total": [0, 0, 0, 0],
            "Resultado": [0, 0, 0, 0],
            "Nome da plataforma": ["ml", "ml", "ml", "ml"],
            "empresa": ["a", "a", "a", "a"],
            "org_id": ["1", "1", "1", "1"],
            "Número do pedido": [1, 2, 3, 4],
        }
    )
    s = compute_trailing_monthly_revenues(
        df,
        empresas_sel=(),
        plataformas_sel=(),
        mes_referencia=(2026, 3),
    )
    assert len(s) == 3


def test_render_recorte_parcial_vazio_em_html_aux() -> None:
    from app.components.termometro_pace import pace_html_for_tests

    p = PaceMensal(
        mes_referencia="03/2026",
        dia_atual=15,
        dias_totais_periodo=15,
        dias_restantes=0,
        modo="recorte_parcial",
        receita_realizada=1.0,
        pct_meta_realizada=0.0,
        meta_mensal=None,
        meta_origem="sem_meta",
        projecao_linear=None,
        desvio_projecao_pct=None,
        ritmo_atual_diario=1.0,
        ritmo_necessario_diario=None,
        ajuste_ritmo_necessario_pct=None,
        nivel_alerta="leitura",
        mensagem_alerta=None,
    )
    assert pace_html_for_tests(p) == ""
