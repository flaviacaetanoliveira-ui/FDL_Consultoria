"""Agregação Simples Nacional para o painel Fiscal."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from processing.faturamento.simples_nacional import agregar_simples_nacional_para_painel_fiscal


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
        "gama_home": "simples_nacional",
        "mega_star": "simples_nacional",
        "moveis_eap": "simples_nacional",
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
    params = {"gama_home": "simples_nacional", "mega_facil": "lucro_presumido"}
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
    params = {"gama_home": "simples_nacional", "mega_facil": "lucro_presumido"}
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
    params = {"gama_home": "simples_nacional"}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2025, 6, 1),
        date(2025, 6, 30),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    ult = resultado["por_empresa"]["gama_home"]["ultimo_mes"]
    assert ult is not None
    assert ult.rbt12_suficiente is False
    assert ult.aliquota_efetiva_pct is None


def test_aliquota_media_ponderada_do_periodo() -> None:
    """Um mês no filtro com RBT12 na faixa 1 (4%): média do período ≈ alíquota efetiva."""
    full = _montar_df_full_rbt12_faixa1()
    base = pd.DataFrame([_df_nf_linha("gama_home", date(2026, 2, 5), 100_000.0, "Fev")])
    params = {"gama_home": "simples_nacional"}
    resultado = agregar_simples_nacional_para_painel_fiscal(
        base,
        ["gama_home"],
        params,
        date(2026, 2, 1),
        date(2026, 2, 28),
        df_fiscal_full=full,
        ok_nf_dates=True,
    )
    ali = resultado["por_empresa"]["gama_home"]["aliquota_media_periodo_pct"]
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
    params = {"gama_home": "simples_nacional"}
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
    params = {"gama_home": "simples_nacional"}
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
    params = {"gama_home": "simples_nacional"}
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
