"""Integração da Apuração Fiscal com params reais do Cliente 2."""

from __future__ import annotations

from pathlib import Path

import pytest

from app.components.apuracao_fiscal_panel import _aliquota_imposto_caption_safe_html_and_divergencia_ref
from processing.faturamento.fiscal_devolucoes_materializado import build_devolucoes_fiscal_dataframe
from processing.faturamento.params import FaturamentoParamsV2
from processing.faturamento.params_regime import (
    aliquota_configurada_para_empresas_filtradas,
    detectar_regimes_tributarios,
    load_faturamento_params_for_ui,
)

_FAT_DATASET = Path("data_products/cliente_2/faturamento/current/dataset_faturamento_app.csv")
_PARAMS_JSON = Path("ops/faturamento_params_cliente_2_gama_star_eap.json")


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
