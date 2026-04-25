"""Testes do loader de parâmetros de Lucro Presumido."""

from __future__ import annotations

import json
import warnings
from pathlib import Path

import pytest

from processing.faturamento.lucro_presumido_loader import load_lucro_presumido_params_from_json


def _write_json(tmp_path: Path, payload: dict) -> Path:
    p = tmp_path / "params.json"
    p.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return p


def _base_json() -> dict:
    return {
        "schema_version": 2,
        "cliente_root": "C:/tmp",
        "cliente_slug": "cliente_2",
        "custo_xlsx": "Custos.xlsx",
        "aliquota_imposto": 0.11,
        "aliquota_despesas_fixas": 0.05,
        "permite_faturamento_sem_nf": False,
        "coluna_base_imposto": ["Base fiscal item", "Valor total"],
        "notas_saida_dir": "notas_saida",
        "empresas": [],
    }


def test_load_params_mega_facil_existente(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
            "lucro_presumido_params": {"pis": 0.0065, "cofins": 0.03, "aplicar_majoracao_lc_224": True},
            "icms_params": {"icms_interno_moveis_9403_completos": 0.133, "fcp_destino": {"RJ": 0.02}},
        }
    ]
    p = _write_json(tmp_path, data)
    lp, icms = load_lucro_presumido_params_from_json(p, "mega_facil")
    assert lp is not None
    assert icms is not None
    assert lp.cofins == pytest.approx(0.03)
    assert lp.aplicar_majoracao_lc_224 is True
    assert dict(icms.fcp_destino) == {"RJ": 0.02}
    assert icms.icms_interno_moveis_9403_completos == pytest.approx(0.133)


def test_load_params_empresa_simples_retorna_none(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "gama_home",
            "empresa": "Gama Home",
            "regime_tributario": "simples_nacional",
            "pedidos_dir": "Gama Home/Pedidos",
        }
    ]
    p = _write_json(tmp_path, data)
    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")
        lp, icms = load_lucro_presumido_params_from_json(p, "gama_home")
    assert (lp, icms) == (None, None)
    assert len(w) == 0


def test_load_params_chave_ausente_usa_defaults(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
            "lucro_presumido_params": {"pis": 0.01},
        }
    ]
    p = _write_json(tmp_path, data)
    lp, icms = load_lucro_presumido_params_from_json(p, "mega_facil")
    assert lp is not None and icms is not None
    assert lp.pis == pytest.approx(0.01)
    assert lp.cofins == pytest.approx(0.03)
    assert lp.aliquota_irpj == pytest.approx(0.15)


def test_load_params_json_malformado_levanta_excecao(tmp_path: Path) -> None:
    p = tmp_path / "bad.json"
    p.write_text("{ invalid json", encoding="utf-8")
    with pytest.raises(ValueError, match="JSON malformado"):
        load_lucro_presumido_params_from_json(p, "mega_facil")


def test_load_params_fcp_destino_carrega_corretamente(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
            "lucro_presumido_params": {},
            "icms_params": {"fcp_destino": {"RJ": 0.02, "MG": 0.02}, "fcp_default": 0.0},
        }
    ]
    p = _write_json(tmp_path, data)
    lp, icms = load_lucro_presumido_params_from_json(p, "mega_facil")
    assert lp is not None and icms is not None
    assert icms.fcp_destino["RJ"] == pytest.approx(0.02)
    assert icms.fcp_destino["MG"] == pytest.approx(0.02)
    assert icms.fcp_destino.get("BA", icms.fcp_default) == pytest.approx(0.00)


def test_load_params_empresa_lp_sem_bloco_usa_defaults_com_warning(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
        }
    ]
    p = _write_json(tmp_path, data)
    with pytest.warns(UserWarning, match="é LP mas não tem lucro_presumido_params"):
        lp, icms = load_lucro_presumido_params_from_json(p, "mega_facil")
    assert lp is not None and icms is not None
    assert lp.pis == pytest.approx(0.0065)
    assert icms.icms_interno_moveis_9403_completos == pytest.approx(0.133)


def test_load_params_empresa_org_id_inexistente_retorna_none(tmp_path: Path) -> None:
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
        }
    ]
    p = _write_json(tmp_path, data)
    lp, icms = load_lucro_presumido_params_from_json(p, "nao_existe")
    assert lp is None and icms is None


def test_load_params_icms_interestadual_origem_sp_completo(tmp_path: Path) -> None:
    full_map = {
        "AC": 0.07,
        "AL": 0.07,
        "AM": 0.07,
        "AP": 0.07,
        "BA": 0.07,
        "CE": 0.07,
        "DF": 0.07,
        "ES": 0.07,
        "GO": 0.07,
        "MA": 0.07,
        "MG": 0.12,
        "MS": 0.07,
        "MT": 0.07,
        "PA": 0.07,
        "PB": 0.07,
        "PE": 0.07,
        "PI": 0.07,
        "PR": 0.12,
        "RJ": 0.12,
        "RN": 0.07,
        "RO": 0.07,
        "RR": 0.07,
        "RS": 0.12,
        "SC": 0.12,
        "SE": 0.07,
        "TO": 0.07,
    }
    data = _base_json()
    data["empresas"] = [
        {
            "org_id": "mega_facil",
            "empresa": "Mega Fácil",
            "regime_tributario": "lucro_presumido",
            "pedidos_dir": "Mega Facil/Pedidos",
            "lucro_presumido_params": {},
            "icms_params": {"icms_interestadual_origem_sp": full_map},
        }
    ]
    p = _write_json(tmp_path, data)
    lp, icms = load_lucro_presumido_params_from_json(p, "mega_facil")
    assert lp is not None and icms is not None
    assert len(icms.icms_interestadual_origem_sp) == 26
    assert icms.icms_interestadual_origem_sp["MG"] == pytest.approx(0.12)
    assert icms.icms_interestadual_origem_sp["BA"] == pytest.approx(0.07)

