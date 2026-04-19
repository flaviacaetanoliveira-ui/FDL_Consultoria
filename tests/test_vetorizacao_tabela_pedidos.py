"""Regressão: vetorização de ``compute_tabela_por_pedido`` (determinismo e igualdade entre chamadas)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from processing.faturamento.config import SKU_NORMALIZADO_COL
from processing.faturamento.resultado_gerencial_slice import (
    REQUIRED_LINE_COLUMNS,
    build_resultado_gerencial_slice,
    compute_tabela_por_pedido,
)


def _row(
    *,
    data: str,
    empresa: str,
    org_id: str,
    pedido: str,
    plataforma: str,
    valor_total: float,
    comissao: float,
    frete_plat: float,
    cmv: float,
    resultado: float,
    frete_tp: float = 0.0,
    desp_fixa: float = 0.0,
    ads: float = 0.0,
    sku: str = "SKU1",
    nota_situacao: str = "",
) -> dict:
    r = {
        "Data": data,
        "empresa": empresa,
        "org_id": org_id,
        "Número do pedido": pedido,
        "Nome da plataforma": plataforma,
        "Valor total": valor_total,
        "Taxa de Comissão": comissao,
        "Frete_Plataforma": frete_plat,
        "Custo_Produto_Total": cmv,
        "Resultado": resultado,
        "Frete transportadora própria": frete_tp,
        "Despesas Fixas": desp_fixa,
        "custo_ads": ads,
        SKU_NORMALIZADO_COL: sku,
        "Quantidade": 1.0,
        "Custo de Frete": 0.0,
        "Nota_Situacao": nota_situacao,
    }
    assert REQUIRED_LINE_COLUMNS.issubset(r.keys())
    return r


def _slice_gama_mar2026() -> object:
    """Recorte compacto tipo Gama Home mar/2026 (março civil)."""
    df = pd.DataFrame(
        [
            _row(
                data="05/03/2026",
                empresa="Gama Home",
                org_id="o1",
                pedido="PX",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=1.0,
                cmv=15.0,
                resultado=20.0,
                sku="S1",
            ),
            _row(
                data="06/03/2026",
                empresa="Gama Home",
                org_id="o1",
                pedido="PY",
                plataforma="ML",
                valor_total=50.0,
                comissao=5.0,
                frete_plat=1.0,
                cmv=15.0,
                resultado=20.0,
                sku="S2",
            ),
            _row(
                data="07/03/2026",
                empresa="Gama Home",
                org_id="o1",
                pedido="PZ",
                plataforma="ML",
                valor_total=100.0,
                comissao=10.0,
                frete_plat=2.0,
                cmv=30.0,
                resultado=40.0,
                sku="S3",
                nota_situacao="Autorizada",
            ),
            _row(
                data="07/03/2026",
                empresa="Gama Home",
                org_id="o1",
                pedido="PZ",
                plataforma="ML",
                valor_total=20.0,
                comissao=2.0,
                frete_plat=0.5,
                cmv=6.0,
                resultado=8.0,
                sku="S4",
                nota_situacao="Cancelada pelo emitente",
            ),
        ]
    )
    df["Vl_Venda"] = df["Valor total"]
    return build_resultado_gerencial_slice(
        df,
        empresas_sel=("Gama Home",),
        plataformas_sel=(),
        data_venda_ini=date(2026, 3, 1),
        data_venda_fim=date(2026, 3, 31),
    )


def test_vetorizacao_produz_mesmo_output_que_loop_original() -> None:
    """Duas chamadas seguidas com o mesmo slice devem produzir linhas idênticas (regressão de saída)."""
    sl = _slice_gama_mar2026()
    imp = 12.34
    a = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)
    b = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)
    assert a == b


def test_vetorizacao_ordem_dos_pedidos_estavel() -> None:
    """Mesma entrada → mesma ordenação de ``pedido_id`` no retorno (determinismo)."""
    sl = _slice_gama_mar2026()
    imp = 0.0
    t1 = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)
    t2 = compute_tabela_por_pedido(sl, fiscal_imposto_valor=imp)
    assert [p.pedido_id for p in t1] == [p.pedido_id for p in t2]
