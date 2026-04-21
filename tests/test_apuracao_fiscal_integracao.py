"""Integração da Apuração Fiscal com params reais do Cliente 2."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import pytest

from app.components.apuracao_fiscal_panel import (
    _aliquota_imposto_caption_safe_html_and_divergencia_ref,
    _build_aliquota_efetiva_simples_html,
    _build_calculo_detalhado_expander_html,
    _build_composicao_base_tributavel_html,
    _tem_alguma_empresa_simples_no_filtro,
)
from processing.faturamento.fiscal_devolucoes_materializado import build_devolucoes_fiscal_dataframe
from processing.faturamento.params import FaturamentoParamsV2
from processing.faturamento.params_regime import (
    aliquota_configurada_para_empresas_filtradas,
    detectar_regimes_tributarios,
    load_faturamento_params_for_ui,
)

_FAT_DATASET = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_app.csv")
_PARAMS_JSON = Path("ops/faturamento_params_cliente_2_gama_star_eap.json")


def _fmt_brl_ptbr_test(x: object) -> str:
    """Espelho mínimo de ``app_operacional._fmt_brl_ptbr_celula`` para testes sem Streamlit."""
    try:
        v = float(x)
    except (TypeError, ValueError):
        return str(x).strip()
    neg = v < 0
    v = abs(v)
    cents = int(round(v * 100 + 1e-9))
    inteiro, cent = divmod(cents, 100)
    int_str = f"{inteiro:,}".replace(",", ".")
    corpo = f"{int_str},{cent:02d}"
    if neg:
        return f"R$ -{corpo}"
    return f"R$ {corpo}"


def _fmt_int_ptbr_test(n: int) -> str:
    return f"{int(n):,}".replace(",", ".")


def _load_info_cliente2() -> dict[str, object]:
    return {
        "cliente_slug": "cliente_2",
        "params_path": str(_PARAMS_JSON),
        "faturamento_path_final_resolved": str(_FAT_DATASET),
    }


def _params_cliente2() -> FaturamentoParamsV2:
    params = load_faturamento_params_for_ui(_load_info_cliente2())
    if not isinstance(params, FaturamentoParamsV2):
        raise AssertionError("Esperado params V2 do cliente_2")
    return params


@pytest.mark.skipif(not _FAT_DATASET.is_file(), reason="sem dataset_faturamento_app.csv cliente_2")
def test_filtro_todas_empresas_inclui_mega_facil_no_detectar_regimes() -> None:
    params = _params_cliente2()
    empresas_efetivas = [e.org_id for e in params.empresas]
    assert "mega_facil" in empresas_efetivas

    info = detectar_regimes_tributarios(params, empresas_efetivas)
    assert info["tem_regime_fora_escopo"] is True
    assert "Mega Fácil" in info["empresas_fora_escopo"]


@pytest.mark.skipif(not _FAT_DATASET.is_file(), reason="sem dataset_faturamento_app.csv cliente_2")
def test_caption_multiplas_aliquotas_com_filtro_todas() -> None:
    params = _params_cliente2()
    todos_slugs = [e.org_id for e in params.empresas]
    info = aliquota_configurada_para_empresas_filtradas(params, todos_slugs)

    assert info["modo"] == "multipla"
    assert info["min_pct"] == pytest.approx(9.0)
    assert info["max_pct"] == pytest.approx(11.0)

    caption_html, divergencia_ref = _aliquota_imposto_caption_safe_html_and_divergencia_ref(
        params_union=params,
        aliquotas_info=info,
        empresas_efetivas=todos_slugs,
        fallback_metadata_pct=11.0,
        ok_nf_dates=True,
    )
    assert "múltiplas" in caption_html.lower()
    assert "ℹ" in caption_html
    assert divergencia_ref is None


@pytest.mark.skipif(not _PARAMS_JSON.is_file(), reason="sem ops/faturamento_params cliente_2")
def test_devolucoes_lidas_de_todas_empresas_configuradas() -> None:
    params = _params_cliente2()
    if not params.cliente_root.is_dir():
        pytest.skip(f"cliente_root indisponível: {params.cliente_root}")

    df_dev = build_devolucoes_fiscal_dataframe(_PARAMS_JSON)
    assert not df_dev.empty
    empresas_presentes = set(df_dev["org_id"].dropna().astype(str).tolist()) if "org_id" in df_dev.columns else set()
    assert len(empresas_presentes) >= 2, f"Só encontrou devoluções para: {sorted(empresas_presentes)}"
    assert "mega_facil" in empresas_presentes, (
        "Mega Fácil deve contribuir com devoluções (natureza Bling sem «de»). "
        f"Encontradas: {sorted(empresas_presentes)}"
    )


@pytest.mark.skipif(not _PARAMS_JSON.is_file(), reason="sem ops/faturamento_params cliente_2")
def test_painel_com_filtro_so_simples_nao_tem_badge_regime() -> None:
    params = _params_cliente2()
    simples_only = [e.org_id for e in params.empresas if (e.regime_tributario or "").strip() == "simples_nacional"]
    assert len(simples_only) >= 1
    info = detectar_regimes_tributarios(params, simples_only)
    assert info["tem_regime_fora_escopo"] is False


def test_painel_composicao_base_renderiza_com_valores_corretos() -> None:
    html = _build_composicao_base_tributavel_html(
        valor_faturado=3_888_758.72,
        nfs_emitidas=17_124,
        valor_cancelado=0.0,
        nfs_canceladas=0,
        valor_devolucoes=80_747.65,
        nfs_devolucoes=355,
        base_liquida=3_808_011.07,
        fmt_brl=_fmt_brl_ptbr_test,
        fmt_int=_fmt_int_ptbr_test,
    )
    assert "3.888.758,72" in html
    assert "80.747,65" in html
    assert "3.808.011,07" in html
    assert "(+)" in html
    assert "(−)" in html


def test_bloco_aliquota_efetiva_nao_aparece_se_filtro_so_mega_facil() -> None:
    simples_agregado = {
        "por_empresa": {
            "mega_facil": {
                "regime": "lucro_presumido",
                "ultimo_mes": None,
            }
        },
        "tem_empresa_fora_escopo": True,
    }
    assert not _tem_alguma_empresa_simples_no_filtro(simples_agregado)


def test_tem_alguma_empresa_simples_false_para_dict_vazio() -> None:
    assert not _tem_alguma_empresa_simples_no_filtro({})


def test_expander_calculo_detalhado_contem_rbt12_e_formula() -> None:
    from processing.faturamento.simples_nacional import ResultadoAliquotaEfetivaMes, ResultadoFaixaSimples

    resultado = ResultadoAliquotaEfetivaMes(
        empresa_slug="gama_home",
        competencia=date(2026, 4, 1),
        rbt12=1_010_000.00,
        faixa=ResultadoFaixaSimples(
            faixa_numero=4,
            rbt12_min=720_000.01,
            rbt12_max=1_800_000.00,
            aliquota_nominal_pct=10.70,
            parcela_deduzir=22_500.00,
        ),
        aliquota_efetiva_pct=8.47,
        rbt12_suficiente=True,
        meses_historico_disponiveis=12,
        motivo_indisponivel=None,
    )
    html = _build_calculo_detalhado_expander_html("gama_home", "Gama Home", resultado, fmt_brl=_fmt_brl_ptbr_test)
    assert "1.010.000" in html
    assert "10,70%" in html
    assert "22.500" in html
    assert "8,47" in html
    assert "LC 123" in html or "art. 18" in html


def test_aliquota_simples_html_contem_tabela_e_total() -> None:
    from processing.faturamento.simples_nacional import ResultadoAliquotaEfetivaMes, ResultadoFaixaSimples

    fx = ResultadoFaixaSimples(1, 0.0, 180_000.0, 4.0, 0.0)
    ult = ResultadoAliquotaEfetivaMes(
        "gama_home",
        date(2026, 4, 1),
        100_000.0,
        fx,
        4.0,
        True,
        12,
        None,
    )
    ag = {
        "por_empresa": {
            "gama_home": {
                "empresa_nome": "Gama Home",
                "regime": "simples_nacional",
                "ultimo_mes": ult,
                "historico_mensal_no_periodo": [],
                "base_liquida_periodo": 50_000.0,
                "imposto_calculado_periodo": 2_000.0,
                "aliquota_media_periodo_pct": 4.0,
            }
        },
        "total_simples": {"base_liquida": 50_000.0, "imposto_total": 2_000.0, "aliquota_media_ponderada_pct": 4.0},
        "tem_empresa_fora_escopo": False,
    }
    html = _build_aliquota_efetiva_simples_html(ag, fmt_brl=_fmt_brl_ptbr_test)
    assert "fdl-fat-sn-table" in html
    assert "Total Simples" in html
    assert "Gama Home" in html
