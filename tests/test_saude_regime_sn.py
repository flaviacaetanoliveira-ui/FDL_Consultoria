"""Indicador Saúde do Regime SN (RBT12 + faixas de limite)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pandas as pd
import pytest

from processing.faturamento.params import EmpresaFaturamentoEntry
from processing.faturamento.saude_regime_sn import (
    LIMITE_SIMPLES_NACIONAL,
    classificar_faixa,
    calcular_saude_regime_sn,
    org_ids_simples_nacional_do_params,
)
from processing.faturamento.simples_nacional import _rbt12_janela_meses
from tests._helpers_fiscal import v2_min_params as _v2_min


def _df_mensal(org: str, por_mes: dict[date, float]) -> pd.DataFrame:
    rows: list[dict] = []
    for m, val in por_mes.items():
        rows.append(
            {
                "org_id": org,
                "Nota_Data_Emissao": m,
                "Valor_Liquido_NF": val,
                "Nota_Situacao": "Autorizada",
            }
        )
    return pd.DataFrame(rows)


def _hist_no_window(competencia: date, total: float) -> dict[date, float]:
    meses = _rbt12_janela_meses(competencia)
    assert len(meses) == 12
    v = total / 12.0
    return {m: v for m in meses}


def test_saude_tranquilo_quando_rbt12_baixo() -> None:
    comp = date(2026, 4, 1)
    lim = float(LIMITE_SIMPLES_NACIONAL)
    total = lim * 0.20
    h = _hist_no_window(comp, total)
    df = _df_mensal("sn_x", h)
    out = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))
    assert len(out) == 1
    s = out[0]
    assert s.faixa == "TRANQUILO"
    assert s.rbt12_suficiente is True
    assert s.meses_disponiveis == 12
    assert s.percentual_limite == pytest.approx(0.20, abs=1e-9)


def test_saude_atencao_quando_acima_70_pct() -> None:
    comp = date(2026, 4, 1)
    lim = float(LIMITE_SIMPLES_NACIONAL)
    total = lim * 0.75
    df = _df_mensal("sn_x", _hist_no_window(comp, total))
    s = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))[0]
    assert s.faixa == "ATENCAO"
    assert s.percentual_limite == pytest.approx(0.75, abs=1e-6)


def test_saude_critico_quando_acima_85_pct() -> None:
    comp = date(2026, 4, 1)
    lim = float(LIMITE_SIMPLES_NACIONAL)
    total = lim * 0.90
    df = _df_mensal("sn_x", _hist_no_window(comp, total))
    s = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))[0]
    assert s.faixa == "CRITICO"


def test_saude_excedido_quando_acima_100_pct() -> None:
    comp = date(2026, 4, 1)
    lim = float(LIMITE_SIMPLES_NACIONAL)
    total = lim * 1.05
    df = _df_mensal("sn_x", _hist_no_window(comp, total))
    s = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))[0]
    assert s.faixa == "EXCEDIDO"
    assert s.valor_disponivel_ate_limite == pytest.approx(0.0, abs=1e-6)


def test_rbt12_suficiente_false_se_menos_12_meses() -> None:
    comp = date(2026, 4, 1)
    meses = _rbt12_janela_meses(comp)
    # apenas 6 primeiros meses da janela
    h = {meses[i]: 100_000.0 for i in range(6)}
    df = _df_mensal("sn_x", h)
    s = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))[0]
    assert s.rbt12_suficiente is False
    assert s.meses_disponiveis == 6


def test_classificacao_de_faixa() -> None:
    assert classificar_faixa(0.69) == "TRANQUILO"
    assert classificar_faixa(0.70) == "ATENCAO"
    assert classificar_faixa(0.84) == "ATENCAO"
    assert classificar_faixa(0.85) == "CRITICO"
    assert classificar_faixa(0.999) == "CRITICO"
    assert classificar_faixa(1.0) == "EXCEDIDO"
    assert classificar_faixa(1.5) == "EXCEDIDO"


def test_apenas_empresas_sn_aparecem_via_filter_params() -> None:
    emp_sn = EmpresaFaturamentoEntry(
        org_id="sn1",
        empresa="SN 1",
        pedidos_dir="p",
        permite_faturamento_sem_nf=None,
        regime_tributario="simples_nacional",
    )
    emp_lp = EmpresaFaturamentoEntry(
        org_id="lp1",
        empresa="LP 1",
        pedidos_dir="p",
        permite_faturamento_sem_nf=None,
        regime_tributario="lucro_presumido",
    )
    p = _v2_min((emp_sn, emp_lp))
    assert org_ids_simples_nacional_do_params(p, ["sn1", "lp1"]) == ["sn1"]


def test_competencia_usa_12_meses_anteriores() -> None:
    comp = date(2026, 4, 1)
    esperados = _rbt12_janela_meses(comp)
    assert esperados[0] == date(2025, 4, 1)
    assert esperados[-1] == date(2026, 3, 1)
    total = float(LIMITE_SIMPLES_NACIONAL) * 0.1
    df = _df_mensal("sn_x", _hist_no_window(comp, total))
    s = calcular_saude_regime_sn(df, ["sn_x"], None, pd.Timestamp(comp))[0]
    assert s.janela_rbt12_inicio == "2025-04"
    assert s.janela_rbt12_fim == "2026-03"
    assert s.competencia == "2026-04"


@pytest.mark.skipif(
    not Path("data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet").exists(),
    reason="dataset cliente_2 opcional",
)
def test_validacao_numerica_cliente2_abril_2026_smoke() -> None:
    """Sanidade: função roda no parquet real quando presente (valores variam com dados)."""
    from processing.faturamento.params import load_faturamento_params

    p = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_fiscal.parquet")
    df = pd.read_parquet(p)
    params = load_faturamento_params(
        Path("ops/faturamento_params_cliente_2_gama_star_eap.json"),
        validate_fs_layout=False,
    )
    org_ids_sn = ["gama_home", "mega_star", "moveis_eap"]
    saudes = calcular_saude_regime_sn(
        df_fiscal=df,
        org_ids_sn=org_ids_sn,
        params_regime=params,
        competencia=pd.Timestamp("2026-04-01"),
    )
    assert len(saudes) == 3
    for s in saudes:
        assert s.rbt12 >= 0
        assert 0 <= s.percentual_limite < 2.0 or s.percentual_limite >= 0
