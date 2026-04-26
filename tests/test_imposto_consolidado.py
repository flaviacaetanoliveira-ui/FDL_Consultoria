"""Testes de ``calcular_imposto_total_painel_fiscal`` (imposto consolidado SN + LP).

O argumento ``imposto_simples_total`` representa o imposto **só** das empresas SN
(``agregar_simples_nacional_para_painel_fiscal`` → ``total_simples["imposto_total"]``),
não a ponte fiscal DRE.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from processing.faturamento.imposto_consolidado import (
    calcular_imposto_total_painel_fiscal,
    resolver_org_ids_para_consolidacao_imposto,
)
from processing.faturamento.lucro_presumido import CFOP_INTERNO_VENDA
from processing.faturamento.lucro_presumido_loader import load_lucro_presumido_params_from_json
from processing.faturamento.params import load_faturamento_params

_REPO = Path(__file__).resolve().parents[1]
_JSON_C2 = _REPO / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"


def _row_mega(**kwargs) -> dict:
    r = {
        "org_id": "mega_facil",
        "empresa": "Mega Fácil",
        "Nota_Numero_Normalizado": "NF1",
        "Nota_Data_Emissao": pd.Timestamp("2026-02-10"),
        "Nota_Situacao": "Emitida DANFE",
        "Valor_Liquido_NF": 50_000.0,
        "Valor_Total_NF": 50_000.0,
        "Frete_Nota_Export": 0.0,
        "Nota_UF_Destino": "SP",
        "Nota_CFOP": CFOP_INTERNO_VENDA,
        "Nota_NCM": "9403.30.00",
        "schema_version_fiscal": 3,
    }
    r.update(kwargs)
    return r


def test_consolidacao_so_simples_nacional() -> None:
    df = pd.DataFrame([_row_mega(org_id="gama_home", empresa="Gama Home", Nota_Numero_Normalizado="G1")])
    p = load_faturamento_params(_JSON_C2)
    oids = resolver_org_ids_para_consolidacao_imposto(df, p, ["Gama Home"])
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=oids or None,
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=100.0,
        json_params_path=_JSON_C2,
    )
    assert r.imposto_lucro_presumido == 0.0
    assert r.imposto_total == pytest.approx(100.0)
    assert r.empresas_lp_calculadas == ()


def test_consolidacao_inclui_lucro_presumido() -> None:
    df = pd.DataFrame([_row_mega()])
    p = load_faturamento_params(_JSON_C2)
    oids = resolver_org_ids_para_consolidacao_imposto(df, p, ["Mega Fácil"])
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=oids or None,
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=10_000.0,
        json_params_path=_JSON_C2,
    )
    assert r.imposto_simples_total == pytest.approx(10_000.0)
    assert r.imposto_lucro_presumido > 0.0
    assert r.imposto_total == pytest.approx(10_000.0 + r.imposto_lucro_presumido)
    assert "mega_facil" in r.breakdown_lp_por_empresa


def test_consolidacao_filtro_so_lp() -> None:
    df = pd.DataFrame(
        [
            _row_mega(),
            _row_mega(
                org_id="gama_home",
                empresa="Gama Home",
                Nota_Numero_Normalizado="G2",
                Valor_Liquido_NF=999.0,
            ),
        ]
    )
    p = load_faturamento_params(_JSON_C2)
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=["mega_facil"],
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=0.0,
        json_params_path=_JSON_C2,
    )
    assert r.imposto_simples_total == 0.0
    assert r.imposto_lucro_presumido > 0.0
    assert r.imposto_total == pytest.approx(r.imposto_lucro_presumido)
    assert r.empresas_lp_calculadas == ("mega_facil",)


def test_consolidacao_lp_sem_dados_no_periodo() -> None:
    """LP no filtro mas NF fora do período → motor zera imposto LP."""
    df = pd.DataFrame([_row_mega(Nota_Data_Emissao=pd.Timestamp("2025-06-01"))])
    p = load_faturamento_params(_JSON_C2)
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=["mega_facil"],
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=50.0,
        json_params_path=_JSON_C2,
    )
    assert r.imposto_lucro_presumido == 0.0
    assert r.imposto_total == pytest.approx(50.0)


def test_consolidacao_lp_sem_params_no_json(monkeypatch: pytest.MonkeyPatch) -> None:
    df = pd.DataFrame([_row_mega()])

    def _no_params(_path: Path, _oid: str):
        return None, None

    monkeypatch.setattr(
        "processing.faturamento.imposto_consolidado.load_lucro_presumido_params_from_json",
        _no_params,
    )
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=["mega_facil"],
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=77.0,
        json_params_path=_JSON_C2,
    )
    assert r.imposto_lucro_presumido == 0.0
    assert r.imposto_total == pytest.approx(77.0)
    assert "mega_facil" in r.empresas_lp_sem_params


def test_consolidacao_filtro_none_pega_todas_empresas() -> None:
    df = pd.DataFrame(
        [
            _row_mega(),
            _row_mega(
                org_id="gama_home",
                empresa="Gama Home",
                Nota_Numero_Normalizado="G9",
                Nota_Data_Emissao=pd.Timestamp("2026-02-11"),
                Valor_Liquido_NF=1_000.0,
                Valor_Total_NF=1_000.0,
            ),
        ]
    )
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=None,
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=1.0,
        json_params_path=_JSON_C2,
    )
    assert "mega_facil" in r.empresas_lp_calculadas
    assert r.imposto_total >= r.imposto_simples_total + r.imposto_lucro_presumido - 1e-6


def test_consolidador_funciona_sem_cliente_root_valido_no_fs(tmp_path: Path) -> None:
    """
    Regressão Cloud/Linux: JSON com cliente_root inexistente não deve impedir o consolidador.

    Antes: ``load_faturamento_params`` validava ``cliente_root`` no disco e levantava
    ``FaturamentoParamsError``; o painel capturava e o hero ficava com defaults.
    """
    raw = json.loads(_JSON_C2.read_text(encoding="utf-8"))
    mega = next(e for e in raw["empresas"] if e.get("org_id") == "mega_facil")
    stub = {
        "schema_version": 2,
        "cliente_root": "/path/que/nao/existe/em/lugar/nenhum",
        "cliente_slug": "cliente_2",
        "empresas": [mega],
    }
    json_path = tmp_path / "params_lp_only.json"
    json_path.write_text(json.dumps(stub), encoding="utf-8")

    df = pd.DataFrame([_row_mega()])
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=None,
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=1_000.0,
        json_params_path=json_path,
    )
    assert "mega_facil" in r.empresas_lp_calculadas
    assert r.imposto_lucro_presumido > 0.0
    assert r.imposto_total == pytest.approx(1_000.0 + r.imposto_lucro_presumido)


def test_consolidacao_breakdown_por_empresa() -> None:
    df = pd.DataFrame([_row_mega()])
    r = calcular_imposto_total_painel_fiscal(
        df_fiscal=df,
        df_devolucoes=None,
        org_ids_filtro=["mega_facil"],
        periodo_inicio=pd.Timestamp("2026-01-01"),
        periodo_fim=pd.Timestamp("2026-03-31"),
        imposto_simples_total=0.0,
        json_params_path=_JSON_C2,
    )
    bd = r.breakdown_lp_por_empresa["mega_facil"]
    assert bd.total_imposto == pytest.approx(r.imposto_lucro_presumido)
    lp, _icms = load_lucro_presumido_params_from_json(_JSON_C2, "mega_facil")
    assert lp is not None
