"""Testes de alíquota/regime agregados a partir de ``faturamento_params`` (UI / legenda)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from processing.faturamento.params import (
    EmpresaFaturamentoEntry,
    FaturamentoParams,
    FaturamentoParamsV2,
)
from processing.faturamento.params_regime import (
    aliquota_configurada_para_empresas_filtradas,
    detectar_regimes_tributarios,
    enrich_aliquota_ref_pct_for_stats,
    find_empresa_faturamento_entry,
    get_aliquota_imposto_por_empresa,
    get_regime_tributario_por_empresa,
    load_faturamento_params_for_ui,
    resolve_faturamento_params_path_for_ui,
)

_REPO = Path(__file__).resolve().parents[1]


def _v2_min(
    empresas: tuple[EmpresaFaturamentoEntry, ...],
    default_ali: float = 0.11,
) -> FaturamentoParamsV2:
    r = _REPO
    return FaturamentoParamsV2(
        cliente_root=r,
        cliente_slug="t",
        custo_xlsx_resolved=r / "ops" / "faturamento_params_cliente_2_gama_star_eap.json",
        empresas=empresas,
        aliquota_imposto=default_ali,
        aliquota_despesas_fixas=0.05,
        permite_faturamento_sem_nf_default=False,
        coluna_base_imposto=("Base fiscal item",),
        params_mensais_resolved=None,
        notas_saida_dir="notas_saida",
        notas_entrada_dir=None,
        nf_panel_ads=True,
    )


def test_aliquota_none_params_lista_vazia_modo_desconhecido() -> None:
    empty = aliquota_configurada_para_empresas_filtradas(None, [])
    assert empty["modo"] == "desconhecida"
    empty2 = aliquota_configurada_para_empresas_filtradas(None, ["x"])
    assert empty2["modo"] == "desconhecida"


def test_aliquota_v1_unica_replica_para_cada_chave() -> None:
    p = FaturamentoParams(
        aliquota_imposto=0.06,
        aliquota_despesas_fixas=0.05,
        pedidos_dir=None,
        custo_xlsx=None,
        permite_faturamento_sem_nf=False,
    )
    info = aliquota_configurada_para_empresas_filtradas(p, ["a", "b"])
    assert info["modo"] == "unica"
    assert info["valor_unico_pct"] == pytest.approx(6.0)
    assert info["min_pct"] == pytest.approx(6.0)
    assert info["max_pct"] == pytest.approx(6.0)


def test_aliquota_v2_multipla_gama_vs_mega_star() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="gama_home",
            empresa="Gama Home",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.09,
            regime_tributario="simples_nacional",
        ),
        EmpresaFaturamentoEntry(
            org_id="mega_star",
            empresa="Mega Star",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.095,
            regime_tributario="simples_nacional",
        ),
    )
    p = _v2_min(emp)
    info = aliquota_configurada_para_empresas_filtradas(p, ["gama_home", "mega_star"])
    assert info["modo"] == "multipla"
    assert info["valor_unico_pct"] is None
    assert info["min_pct"] == pytest.approx(9.0)
    assert info["max_pct"] == pytest.approx(9.5)
    assert info["valores_por_empresa"]["gama_home"] == pytest.approx(9.0)
    assert info["valores_por_empresa"]["mega_star"] == pytest.approx(9.5)


def test_aliquota_v2_unica_duas_empresas_mesma_aliquota() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="a",
            empresa="A",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.11,
            regime_tributario="simples_nacional",
        ),
        EmpresaFaturamentoEntry(
            org_id="b",
            empresa="B",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.11,
            regime_tributario="simples_nacional",
        ),
    )
    p = _v2_min(emp)
    info = aliquota_configurada_para_empresas_filtradas(p, ["a", "b"])
    assert info["modo"] == "unica"
    assert info["valor_unico_pct"] == pytest.approx(11.0)


def test_enrich_ref_media_multipla() -> None:
    info = {
        "modo": "multipla",
        "valor_unico_pct": None,
        "valores_por_empresa": {"a": 9.0, "b": 11.0},
        "min_pct": 9.0,
        "max_pct": 11.0,
    }
    assert enrich_aliquota_ref_pct_for_stats(info) == pytest.approx(10.0)


def test_enrich_ref_unica() -> None:
    info = {
        "modo": "unica",
        "valor_unico_pct": 7.5,
        "valores_por_empresa": {"x": 7.5},
        "min_pct": 7.5,
        "max_pct": 7.5,
    }
    assert enrich_aliquota_ref_pct_for_stats(info) == pytest.approx(7.5)


def test_detectar_lp_mega_facil_fora_escopo() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="mega_facil",
            empresa="Mega Fácil",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.11,
            regime_tributario="lucro_presumido",
        ),
    )
    p = _v2_min(emp)
    d = detectar_regimes_tributarios(p, ["mega_facil"])
    assert d["tem_regime_fora_escopo"] is True
    assert "Mega Fácil" in d["empresas_fora_escopo"]
    assert "lucro_presumido" in d["regimes_presentes"]


def test_detectar_simples_sem_alerta() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="gama_home",
            empresa="Gama Home",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.09,
            regime_tributario="simples_nacional",
        ),
    )
    p = _v2_min(emp)
    d = detectar_regimes_tributarios(p, ["gama_home"])
    assert d["tem_regime_fora_escopo"] is False
    assert d["empresas_fora_escopo"] == []


def test_detectar_regime_ausente_nao_conta_fora_escopo() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="z",
            empresa="Zeta",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            aliquota_imposto=0.10,
            regime_tributario=None,
        ),
    )
    p = _v2_min(emp)
    d = detectar_regimes_tributarios(p, ["z"])
    assert d["tem_regime_fora_escopo"] is False
    assert d["regimes_presentes"] == frozenset()


def test_detectar_v1_retorna_vazio() -> None:
    p = FaturamentoParams(
        aliquota_imposto=0.06,
        aliquota_despesas_fixas=0.05,
        pedidos_dir=None,
        custo_xlsx=None,
        permite_faturamento_sem_nf=False,
    )
    d = detectar_regimes_tributarios(p, ["x"])
    assert d["tem_regime_fora_escopo"] is False


def test_find_entry_por_org_id_ou_nome() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="mega_star",
            empresa="Mega Star",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            regime_tributario="simples_nacional",
        ),
    )
    p = _v2_min(emp)
    assert find_empresa_faturamento_entry(p, "mega_star") is not None
    assert find_empresa_faturamento_entry(p, "Mega Star") is not None
    assert find_empresa_faturamento_entry(p, "outro") is None


def test_get_aliquota_regime_none_params() -> None:
    assert get_aliquota_imposto_por_empresa(None, "x") is None
    assert get_regime_tributario_por_empresa(None, "x") is None


def test_resolve_params_path_via_metadata(tmp_path: Path) -> None:
    params_json = tmp_path / "fp.json"
    params_json.write_text(
        json.dumps({"schema_version": 1, "note": "stub"}),
        encoding="utf-8",
    )
    fat_root = tmp_path / "fat"
    fat_root.mkdir()
    meta = fat_root / "metadata.json"
    meta.write_text(
        json.dumps({"params_path": str(params_json.resolve())}),
        encoding="utf-8",
    )
    li = {"faturamento_path_final_resolved": str(fat_root / "x.parquet")}
    assert resolve_faturamento_params_path_for_ui(li) == params_json.resolve()


def test_load_faturamento_params_for_ui_missing_returns_none(tmp_path: Path) -> None:
    fat_root = tmp_path / "fat2"
    fat_root.mkdir()
    meta = fat_root / "metadata.json"
    meta.write_text(json.dumps({}), encoding="utf-8")
    li = {"faturamento_path_final_resolved": str(fat_root / "y.parquet")}
    assert load_faturamento_params_for_ui(li) is None
