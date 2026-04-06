"""Merge fiscal ↔ comercial NF-first (zeros à esquerda na NF; fallback org_id vazio)."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.fiscal_commercial_nf_merge import merge_fiscal_base_with_commercial_nf_dataframe


def _fiscal_row(**kwargs: object) -> pd.DataFrame:
    base = {
        "org_id": "o1",
        "empresa": "Esquilo",
        "Nota_Numero_Normalizado": "042480",
        "Nota_Data_Emissao": pd.Timestamp("2026-04-02"),
        "Nota_Situacao": "Autorizada",
        "Valor_Liquido_NF": 100.0,
    }
    base.update(kwargs)
    return pd.DataFrame([base])


def _comm_row(**kwargs: object) -> pd.DataFrame:
    base = {
        "org_id": "o1",
        "empresa": "Esquilo",
        "Nota_Numero_Normalizado": "42480",
        "valor_venda": 80.0,
        "comissao": 1.0,
        "custo_produto": 0.0,
        "frete": 2.0,
        "imposto": 0.0,
        "despesa_fixa": 0.0,
        "resultado": 10.0,
        "plataforma_resumo": "ML",
        "pedido_resumo": "2000015600000000",
        "n_linhas_pedido": 1,
        "produto_resumo": "Prod A",
        "faturamento_nota_vinculada": True,
        "comercial_incompleto": False,
    }
    base.update(kwargs)
    return pd.DataFrame([base])


def test_merge_nf_leading_zero_aligns_with_short_form() -> None:
    """042480 (fiscal) e 42480 (comercial) com mesmo org → mesmo ``normalize_nf``."""
    out = merge_fiscal_base_with_commercial_nf_dataframe(_fiscal_row(), _comm_row())
    assert len(out) == 1
    assert str(out.iloc[0]["Nota_Numero_Normalizado"]) == "042480"
    assert out.iloc[0]["pedido_resumo"] == "2000015600000000"
    assert abs(float(out.iloc[0]["valor_venda"]) - 80.0) < 1e-6


def test_merge_fallback_quando_comercial_sem_org_id() -> None:
    """Fiscal com org preenchida e Parquet comercial com org vazio: ainda casa por empresa+NF."""
    fiscal = _fiscal_row(org_id="org-esquilo")
    comm = _comm_row(org_id="", Nota_Numero_Normalizado="042480")
    out = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm)
    assert out.iloc[0]["pedido_resumo"] == "2000015600000000"
    assert out.iloc[0]["plataforma_resumo"] == "ML"


def test_merge_strict_org_only_ignores_fallback() -> None:
    fiscal = _fiscal_row(org_id="org-esquilo")
    comm = _comm_row(org_id="", Nota_Numero_Normalizado="042480")
    out_strict = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm, strict_org_only=True)
    out_full = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm, strict_org_only=False)
    assert str(out_strict.iloc[0]["pedido_resumo"]) == "—"
    assert out_full.iloc[0]["pedido_resumo"] == "2000015600000000"


def test_merge_frete_nota_export_quando_comercial_zero() -> None:
    """Coluna Frete do CSV de notas no fiscal preenche «frete» do painel se o comercial veio 0."""
    fiscal = _fiscal_row(Valor_Liquido_NF=339.80, Frete_Nota_Export=98.80)
    comm = _comm_row(
        valor_venda=241.0,
        frete=0.0,
        Nota_Numero_Normalizado="042480",
    )
    out = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm)
    assert abs(float(out.iloc[0]["frete"]) - 98.80) < 1e-6


def test_merge_frete_comercial_prevalece_sobre_frete_nota() -> None:
    fiscal = _fiscal_row(Frete_Nota_Export=50.0)
    comm = _comm_row(frete=2.0, Nota_Numero_Normalizado="042480")
    out = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm)
    assert abs(float(out.iloc[0]["frete"]) - 2.0) < 1e-6


def test_merge_prioriza_linha_com_org_quando_ambas_existem() -> None:
    """Com linha com org correta e linha sem org, não sobrescrever com fallback errado."""
    fiscal = _fiscal_row(org_id="o1")
    comm = pd.concat(
        [
            _comm_row(org_id="", pedido_resumo="SEM_ORG"),
            _comm_row(org_id="o1", pedido_resumo="COM_ORG"),
        ],
        ignore_index=True,
    )
    out = merge_fiscal_base_with_commercial_nf_dataframe(fiscal, comm)
    assert out.iloc[0]["pedido_resumo"] == "COM_ORG"
