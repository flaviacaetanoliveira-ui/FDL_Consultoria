"""Resolve org_id técnico para agregador Simples (rótulos UI vs coluna org_id)."""

from __future__ import annotations

import pandas as pd
import pytest

from app.components.apuracao_fiscal_panel import (
    _apuracao_org_ids_do_filtro,
    _apuracao_org_ids_resolvidos_para_df,
)
from processing.faturamento.params import EmpresaFaturamentoEntry, FaturamentoParamsV2
from tests._helpers_fiscal import v2_min_params as _v2_min


def _df_fiscal_stub() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "org_id": ["gama_home", "gama_home", "mega_star"],
            "empresa": ["Gama Home", "Gama Home", "Mega Star"],
            "Valor_Liquido_NF": [1.0, 2.0, 3.0],
        }
    )


def test_com_params_v2_igual_do_filtro_quando_ja_slugs() -> None:
    emp = (
        EmpresaFaturamentoEntry(
            org_id="gama_home",
            empresa="Gama Home",
            pedidos_dir="p",
            permite_faturamento_sem_nf=None,
            regime_tributario="simples_nacional",
        ),
    )
    p = _v2_min(emp)
    df = _df_fiscal_stub()
    ch = ["gama_home"]
    assert _apuracao_org_ids_resolvidos_para_df(df, p, ch) == _apuracao_org_ids_do_filtro(p, ch)


def test_sem_params_mas_df_com_empresa_resolve_labels() -> None:
    df = _df_fiscal_stub()
    out = _apuracao_org_ids_resolvidos_para_df(df, None, ["Gama Home", "Mega Star"])
    assert out == ["gama_home", "mega_star"]


def test_sem_params_sem_coluna_empresa_fallback_conservador() -> None:
    df = pd.DataFrame({"org_id": ["gama_home"], "Valor_Liquido_NF": [1.0]})
    out = _apuracao_org_ids_resolvidos_para_df(df, None, ["Gama Home"])
    assert out == ["Gama Home"]


def test_nome_inexistente_no_df_mantem_original() -> None:
    df = _df_fiscal_stub()
    out = _apuracao_org_ids_resolvidos_para_df(df, None, ["Fantasma Corp"])
    assert out == ["Fantasma Corp"]
