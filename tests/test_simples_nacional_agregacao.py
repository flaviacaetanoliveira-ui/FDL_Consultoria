"""Agregação Simples Nacional para o painel Fiscal."""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd
import pytest

from processing.faturamento.simples_nacional import _rbt12_janela_meses, agregar_simples_nacional_para_painel_fiscal


def _df_nf_linha(
    org: str,
    emissao: date,
    valor: float,
    nn: str,
    situacao: str = "Autorizada",
) -> dict:
    return {
        "org_id": org,
        "empresa": org,
        "Nota_Data_Emissao": emissao,
        "Valor_Liquido_NF": valor,
        "Nota_Numero_Normalizado": nn,
        "Nota_Situacao": situacao,
    }


def _montar_df_full_rbt12_faixa1() -> pd.DataFrame:
    """12 meses antes de fev/2026 com 15k cada → RBT12 180k (faixa 1, 4%)."""
    rows: list[dict] = []
    nn = 0
    # janela RBT12 para competência fev/2026: fev/2025 … jan/2026
    for y, m in (
        (2025, 2),
        (2025, 3),
        (2025, 4),
        (2025, 5),
        (2025, 6),
        (2025, 7),
        (2025, 8),
        (2025, 9),
        (2025, 10),
        (2025, 11),
        (2025, 12),
        (2026, 1),
    ):
        nn += 1
        rows.append(_df_nf_linha("gama_home", date(y, m, 10), 15_000.0, f"NF{y}{m:02d}"))
    return pd.DataFrame(rows)


def test_agregar_filtro_so_simples_retorna_3_empresas() -> None:
    full = _montar_df_full_rbt12_faixa1()
    rows_periodo = [
        _df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "P1"),
        _df_nf_linha("mega_star", date(2026, 2, 5), 10_000.0, "P2"),
        _df_nf_linha("moveis_eap", date(2026, 2, 5), 20_000.0, "P3"),
    ]
    base = pd.DataFrame(rows_periodo)
    params = {
        "gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
        "mega_star": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
        "moveis_eap": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
    }
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home", "mega_star", "moveis_eap"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    assert len(r["por_empresa"]) == 3
    assert r["tem_empresa_fora_escopo"] is False


def test_agregar_filtro_inclui_mega_facil_marca_fora_escopo() -> None:
    full = _montar_df_full_rbt12_faixa1()
    rows_periodo = [
        _df_nf_linha("gama_home", date(2026, 2, 5), 50_000.0, "P1"),
        _df_nf_linha("mega_facil", date(2026, 2, 5), 30_000.0, "P2"),
    ]
    base = pd.DataFrame(rows_periodo)
    params = {
        "gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
        "mega_facil": "lucro_presumido",
    }
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home", "mega_facil"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    mega = resultado["por_empresa"]["mega_facil"]
    assert mega["regime"] == "lucro_presumido"
    assert mega["ultimo_mes"] is None
    assert resultado["tem_empresa_fora_escopo"] is True


def test_total_simples_exclui_mega_facil() -> None:
    full = _montar_df_full_rbt12_faixa1()
    rows_periodo = [
        _df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "P1"),
        _df_nf_linha("mega_facil", date(2026, 2, 5), 999_999.0, "P2"),
    ]
    base = pd.DataFrame(rows_periodo)
    params = {
        "gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
        "mega_facil": "lucro_presumido",
    }
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home", "mega_facil"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    assert resultado["total_simples"]["base_liquida"] == pytest.approx(100_000.0)
    assert resultado["total_simples"]["imposto_total"] < 10_000.0


def test_historico_com_rbt12_insuficiente_em_primeiro_mes() -> None:
    """Histórico curto: primeiro mês do dataset não tem 12 meses anteriores."""
    rows_hist = [_df_nf_linha("gama_home", date(2025, 6, 10), 20_000.0, "H1")]
    full = pd.DataFrame(rows_hist)
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2025, 6, 15), 10_000.0, "B1")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.09}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2025, 6, 1),
        date(2025, 6, 30),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    gh = resultado["por_empresa"]["gama_home"]
    ult = gh["ultimo_mes"]
    assert ult is not None
    assert ult.rbt12_suficiente is False
    assert ult.aliquota_efetiva_pct is None
    assert gh["origem_aliquota"] == "referencia_json"
    assert gh["aliquota_referencia_json_pct"] == pytest.approx(9.0)
    assert gh["motivo_fallback"] is not None


def test_aliquota_media_ponderada_do_periodo() -> None:
    """Um mês no filtro com RBT12 na faixa 1 (4%): média do período ≈ alíquota efetiva."""
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "Fev")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.99}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    gh = resultado["por_empresa"]["gama_home"]
    assert gh["origem_aliquota"] == "calculada"
    ali = gh["aliquota_media_periodo_pct"]
    assert ali is not None
    assert ali == pytest.approx(4.0, abs=0.01)


def test_devolucoes_reduzem_base_liquida() -> None:
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "P1")])
    dev = pd.DataFrame(
        [
            {
                "org_id": "gama_home",
                "empresa": "gama_home",
                "Nota_Data_Emissao": date(2026, 2, 20),
                "Valor_Liquido_Devolucao": 10_000.0,
            }
        ]
    )
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        df_devolucoes=dev,
        ok_nf_dates=True,
    )
    assert resultado["por_empresa"]["gama_home"]["base_liquida_periodo"] == pytest.approx(90_000.0)


def test_competencia_referencia_ultimo_mes_com_nf() -> None:
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame(
        [
            _df_nf_linha("gama_home", date(2026, 2, 5), 10_000.0, "A"),
            _df_nf_linha("gama_home", date(2026, 3, 8), 20_000.0, "B"),
        ]
    )
    extra = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 10), 15_000.0, "Hfeb26")])
    full2 = pd.concat([full, extra], ignore_index=True)
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 3, 31),
        df_fiscal_full=full2,
        ok_nf_dates=True,
    )
    assert resultado["competencia_referencia"] == date(2026, 3, 1)


def test_agregar_resolve_regime_via_params_dict() -> None:
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 10_000.0, "P1")])
    params = {"gama_home": {"regime": "simples_nacional", "empresa_nome": "Gama Home"}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    assert r["por_empresa"]["gama_home"]["empresa_nome"] == "Gama Home"


def test_df_full_none_usa_base_para_extracao() -> None:
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 50_000.0, "P1")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=None,
        ok_nf_dates=True,
    )
    assert resultado["por_empresa"]["gama_home"]["ultimo_mes"] is not None
    assert resultado["por_empresa"]["gama_home"]["ultimo_mes"].rbt12_suficiente is False


def test_agregar_empresa_warmup_usa_json_como_fallback() -> None:
    """Empresa com poucos meses de histórico tem origem_aliquota='referencia_json'."""
    base = pd.DataFrame([_df_nf_linha("warm_co", date(2025, 6, 15), 100_000.0, "B1")])
    params = {"warm_co": {"regime": "simples_nacional", "aliquota_imposto": 0.09}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["warm_co"],
        params,
        date(2025, 6, 1),
        date(2025, 6, 30),
        df_fiscal_full=None,
        ok_nf_dates=True,
    )
    row = r["por_empresa"]["warm_co"]
    assert row["origem_aliquota"] == "referencia_json"
    assert row["imposto_calculado_periodo"] == pytest.approx(9_000.0)
    assert r["empresas_em_warmup"] == ["warm_co"]


def test_agregar_empresa_12_meses_usa_calculada() -> None:
    """Empresa com histórico completo tem origem_aliquota='calculada'."""
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "Fev")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.99}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    row = r["por_empresa"]["gama_home"]
    assert row["origem_aliquota"] == "calculada"
    assert row["aliquota_efetiva_calculada_pct"] == pytest.approx(4.0, abs=0.02)
    assert row["imposto_calculado_periodo"] == pytest.approx(4_000.0, abs=1.0)
    assert r["empresas_com_calculo_oficial"] == ["gama_home"]


def test_agregar_lista_empresas_em_warmup() -> None:
    full_a = _montar_df_full_rbt12_faixa1()
    rows_short = [_df_nf_linha("short_co", date(2025, 8, 10), 10_000.0, "S1")]
    full = pd.concat([pd.DataFrame(rows_short), full_a], ignore_index=True)
    base = pd.DataFrame(
        [
            _df_nf_linha("short_co", date(2026, 2, 5), 5_000.0, "P0"),
            _df_nf_linha("gama_home", date(2026, 2, 5), 10_000.0, "P1"),
        ]
    )
    params = {
        "short_co": {"regime": "simples_nacional", "aliquota_imposto": 0.08},
        "gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.04},
    }
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["short_co", "gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    assert set(r["empresas_em_warmup"]) == {"short_co"}
    assert set(r["empresas_com_calculo_oficial"]) == {"gama_home"}


def test_agregar_imposto_mes_a_mes_com_fallback_misto() -> None:
    """Fev/2026 com janela RBT12 incompleta (falta fev/2025) usa JSON; Mar/2026 com janela completa usa fórmula."""
    rows: list[dict] = []
    feb25 = date(2025, 2, 1)
    for m in _rbt12_janela_meses(date(2026, 3, 1)):
        if m == feb25:
            continue
        rows.append(_df_nf_linha("mix_org", date(m.year, m.month, 9), 5_000.0, f"Hm-{m.isoformat()}"))
    full = pd.DataFrame(rows)
    base = pd.DataFrame(
        [
            _df_nf_linha("mix_org", date(2026, 2, 10), 100_000.0, "NFfeb"),
            _df_nf_linha("mix_org", date(2026, 3, 10), 50_000.0, "NFmar"),
        ]
    )
    params = {"mix_org": {"regime": "simples_nacional", "aliquota_imposto": 0.10}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["mix_org"],
        params,
        date(2026, 2, 1),
        date(2026, 3, 31),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    imp = float(r["por_empresa"]["mix_org"]["imposto_calculado_periodo"])
    # Fev: JSON 10% só sobre o recorte (100k); Mar: fórmula 4% sobre 50k. Histórico no full não duplica a receita do mês.
    assert imp == pytest.approx(12_000.0, abs=1.0)


def test_agregar_df_full_com_empresa_slug_sem_dados_usa_org_id_para_rbt12() -> None:
    """
    Regression: após concat, coluna empresa_slug pode existir só com NaN; o agregador não deve
    perder o histórico (0 meses / warm-up indevido) nem zerar a base quando org_id está correto.
    """
    full = _montar_df_full_rbt12_faixa1()
    full_bad = full.copy()
    full_bad["empresa_slug"] = np.nan
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "Fev")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.99}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full_bad,
        ok_nf_dates=True,
    )
    gh = r["por_empresa"]["gama_home"]
    assert int(gh["meses_historico_disponiveis"]) == 12
    assert gh["origem_aliquota"] == "calculada"
    assert gh["base_liquida_periodo"] == pytest.approx(100_000.0)


def test_imposto_calculado_periodo_usa_apenas_receita_do_periodo() -> None:
    """
    Imposto do período = Σ (receita_mês no recorte base × alíquota_mês).
    Linhas extra no full no mesmo mês não podem inflar a receita usada no imposto (regressão F · T2.2).
    """
    full = _montar_df_full_rbt12_faixa1()
    ghost_same_month = pd.DataFrame(
        [_df_nf_linha("gama_home", date(2026, 2, 15), 25_000.0, "GhostFeb")],
    )
    full_extra = pd.concat([full, ghost_same_month], ignore_index=True)
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 8), 100_000.0, "PeriodFeb")])
    params = {"gama_home": {"regime": "simples_nacional", "aliquota_imposto": 0.10}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full_extra,
        ok_nf_dates=True,
    )
    imp = float(r["por_empresa"]["gama_home"]["imposto_calculado_periodo"])
    # Só 100k no recorte; com bug antigo seria ~125k × ~4% ≈ 5000.
    assert imp == pytest.approx(4_000.0, rel=0.03)


def test_aliquota_ponderada_periodo_proxima_da_aliquota_de_referencia() -> None:
    """Alíquota ponderada do período alinha à de referência quando não há distorção de receita (regressão imposto 2×)."""
    base = pd.DataFrame([_df_nf_linha("warm_co", date(2025, 6, 15), 100_000.0, "B1")])
    params = {"warm_co": {"regime": "simples_nacional", "aliquota_imposto": 0.09}}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["warm_co"],
        params,
        date(2025, 6, 1),
        date(2025, 6, 30),
        df_fiscal_full=None,
        ok_nf_dates=True,
    )
    dados = resultado["por_empresa"]["warm_co"]
    aliq_ref = dados.get("aliquota_efetiva_calculada_pct") or dados["aliquota_referencia_json_pct"]
    aliq_pond = dados["aliquota_efetiva_ponderada_periodo_pct"]
    assert aliq_ref is not None and aliq_pond is not None
    assert abs(float(aliq_pond) - float(aliq_ref)) < 2.0


def test_agregador_json_lookup_preserva_decimal_para_pct() -> None:
    """params em mapping com aliquota_imposto=0.09 (decimal) → aliquota_referencia_json_pct=9,0 (%)."""
    base = pd.DataFrame([_df_nf_linha("x", date(2026, 2, 5), 10_000.0, "P")])
    params = {"x": {"regime": "simples_nacional", "aliquota_imposto": 0.09}}
    r = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["x"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=None,
        ok_nf_dates=True,
    )
    row = r["por_empresa"]["x"]
    assert row["aliquota_referencia_json_pct"] == pytest.approx(9.0)
    assert row["origem_aliquota"] == "referencia_json"
