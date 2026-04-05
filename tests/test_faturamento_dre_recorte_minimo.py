"""Recorte mínimo Etapa 1 — ``apply_recorte_minimo`` (venda); ``build_nf_grain`` (só emissão NF no painel)."""

from __future__ import annotations

from datetime import date

import pandas as pd

from faturamento_dre_recorte_minimo import (
    FaturamentoRecorteMinState,
    apply_nf_panel_frete_gap_fallback,
    apply_recorte_minimo,
    build_nf_grain_dataframe,
    nf_grain_plataforma_label_for_ui,
    nf_grain_plataforma_match_key,
    nf_grain_plataforma_ui_options,
    compute_comercial_conferencia_stats,
    compute_fiscal_nf_conferencia_stats,
    compute_nf_panel_kpis,
    compute_vl_nota_fiscal_fiscal_kpi,
    faturamento_min_series_nf_emissao_bounds_dates,
)


def _df() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "Data": pd.to_datetime(["2025-01-10", "2025-01-20", "2025-02-05"]),
            "Nome da plataforma": ["ML", "Shopee", "ML"],
            "empresa": ["A", "A", "B"],
            "Situação": ["Atendido", "Cancelado", "Atendido"],
        }
    )


def test_empty_empresa_and_plat_is_all_rows() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(empresas=(), plataformas=())
    out, w = apply_recorte_minimo(
        df, st, data_venda_ini=date(2024, 1, 1), data_venda_fim=date(2030, 1, 1)
    )
    assert len(out) == 3 and not w


def test_empresa_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(empresas=("B",), plataformas=())
    out, _ = apply_recorte_minimo(
        df, st, data_venda_ini=date(2024, 1, 1), data_venda_fim=date(2030, 1, 1)
    )
    assert len(out) == 1 and out.iloc[0]["empresa"] == "B"


def test_plat_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(empresas=(), plataformas=("Shopee",))
    out, _ = apply_recorte_minimo(
        df, st, data_venda_ini=date(2024, 1, 1), data_venda_fim=date(2030, 1, 1)
    )
    assert len(out) == 1


def test_date_window() -> None:
    df = _df()
    st = FaturamentoRecorteMinState(empresas=(), plataformas=())
    out, _ = apply_recorte_minimo(
        df, st, data_venda_ini=date(2025, 1, 15), data_venda_fim=date(2025, 2, 1)
    )
    assert len(out) == 1


def test_all_situacoes_preserved_without_situacao_filter() -> None:
    df = _df()
    st = FaturamentoRecorteMinState((), ())
    out, _ = apply_recorte_minimo(
        df, st, data_venda_ini=date(2024, 1, 1), data_venda_fim=date(2030, 1, 1)
    )
    assert set(out["Situação"].astype(str)) == {"Atendido", "Cancelado"}


def _df_fiscal() -> pd.DataFrame:
    """Duas linhas mesma NF / org: total 100; uma linha NF cancelada no período."""
    return pd.DataFrame(
        {
            "empresa": ["A", "A", "A"],
            "org_id": ["o1", "o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF1", "NF2"],
            "Nota_Valor_Liquido_Total": [100.0, 100.0, 50.0],
            "Nota_Data_Emissao": pd.to_datetime(
                ["2025-06-10", "2025-06-10", "2025-06-15"]
            ),
            "Nota_Situacao": ["Autorizada", "Autorizada", "Cancelada"],
            "faturamento_nota_vinculada": [True, True, True],
        }
    )


def test_compute_vl_nota_fiscal_dedupes_nf_and_excludes_cancelada() -> None:
    df = _df_fiscal()
    got = compute_vl_nota_fiscal_fiscal_kpi(
        df,
        empresas_sel=(),
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert got == 100.0


def test_compute_vl_nota_fiscal_respects_empresa_only() -> None:
    df = pd.concat(
        [
            _df_fiscal(),
            pd.DataFrame(
                {
                    "empresa": ["B"],
                    "org_id": ["o2"],
                    "Nota_Numero_Normalizado": ["NF9"],
                    "Nota_Valor_Liquido_Total": [999.0],
                    "Nota_Data_Emissao": pd.to_datetime(["2025-06-20"]),
                    "Nota_Situacao": ["Autorizada"],
                    "faturamento_nota_vinculada": [True],
                }
            ),
        ],
        ignore_index=True,
    )
    got = compute_vl_nota_fiscal_fiscal_kpi(
        df,
        empresas_sel=("A",),
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert got == 100.0


def test_faturamento_min_nf_bounds() -> None:
    df = _df_fiscal()
    lo, hi, ok = faturamento_min_series_nf_emissao_bounds_dates(df)
    assert ok is True
    assert lo == date(2025, 6, 10)
    assert hi == date(2025, 6, 15)


def test_compute_fiscal_nf_conferencia_counts_distinct_nf() -> None:
    stt = compute_fiscal_nf_conferencia_stats(
        _df_fiscal(),
        empresas_sel=(),
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert stt.n_nf_distintas == 1
    assert stt.valor_nota_fiscal == 100.0


def test_build_nf_grain_one_nf_two_order_lines() -> None:
    df = pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF1"],
            "Nota_Valor_Liquido_Total": [100.0, 100.0],
            "Nota_Data_Emissao": pd.to_datetime(["2025-06-10", "2025-06-10"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Data": pd.to_datetime(["2025-06-01", "2025-06-02"]),
            "Quantidade": [2.0, 1.0],
            "Preço de lista": [10.0, 5.0],
            "Nome da plataforma": ["ML", "ML"],
            "Número do pedido multiloja": ["P1", "P1"],
            "Taxa de Comissão": [1.0, 2.0],
            "Frete_Plataforma": [0.5, 0.5],
            "Imposto": [3.0, 3.0],
            "Resultado": [10.0, -5.0],
            "Descrição": ["X", "Y"],
            "faturamento_nota_vinculada": [True, True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert not w
    assert len(out) == 1
    assert float(out.iloc[0]["valor_faturado_nf"]) == 100.0
    assert float(out.iloc[0]["valor_venda"]) == 25.0
    assert float(out.iloc[0]["despesa_fixa"]) == 1.25
    assert float(out.iloc[0]["comissao"]) == 3.0
    assert float(out.iloc[0]["custo_produto"]) == 0.0
    assert float(out.iloc[0]["frete"]) == 1.0
    assert float(out.iloc[0]["resultado"]) == 5.0
    assert int(out.iloc[0]["n_linhas_pedido"]) == 2
    assert bool(out.iloc[0]["faturamento_nota_vinculada"])
    kp = compute_nf_panel_kpis(out)
    assert kp["n_nf"] == 1
    assert kp["valor_faturado_nf"] == 100.0
    assert kp["valor_venda"] == 25.0
    assert kp["custo_produto"] == 0.0
    assert kp["despesa_fixa"] == 1.25
    assert kp["resultado"] == 5.0


def test_build_nf_grain_recomposes_resultado_when_despesas_fixas_present() -> None:
    """Σ Resultado + Σ Despesas Fixas (linhas) − 5% × valor_venda NF."""
    df = pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF1"],
            "Nota_Valor_Liquido_Total": [100.0, 100.0],
            "Nota_Data_Emissao": pd.to_datetime(["2025-06-10", "2025-06-10"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Quantidade": [2.0, 1.0],
            "Preço de lista": [10.0, 5.0],
            "Nome da plataforma": ["ML", "ML"],
            "Taxa de Comissão": [0.0, 0.0],
            "Frete_Plataforma": [0.0, 0.0],
            "Imposto": [0.0, 0.0],
            "Despesas Fixas": [2.0, 0.5],
            "Resultado": [4.0, 3.0],
            "Descrição": ["X", "Y"],
            "faturamento_nota_vinculada": [True, True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert not w and len(out) == 1
    assert float(out.iloc[0]["valor_venda"]) == 25.0
    assert float(out.iloc[0]["despesa_fixa"]) == 1.25
    assert float(out.iloc[0]["resultado"]) == 4.0 + 3.0 + 2.0 + 0.5 - 1.25


def test_build_nf_grain_valor_venda_usa_somente_preco_lista() -> None:
    """Mesmo com «Valor total» no export, o grão NF usa Quantidade × Preço de lista."""
    df = pd.DataFrame(
        {
            "empresa": ["A"],
            "org_id": ["o1"],
            "Nota_Numero_Normalizado": ["NFZ"],
            "Nota_Valor_Liquido_Total": [98.4],
            "Nota_Data_Emissao": pd.to_datetime(["2026-03-31"]),
            "Nota_Situacao": ["Autorizada"],
            "Quantidade": [1.0],
            "Preço de lista": [196.8],
            "Valor total": [98.4],
            "Nome da plataforma": ["Shopee"],
            "Número do pedido multiloja": ["260331K9EX896U"],
            "Taxa de Comissão": [10.0],
            "Frete_Plataforma": [0.0],
            "Imposto": [0.0],
            "Resultado": [20.0],
            "Descrição": ["Mesa"],
            "faturamento_nota_vinculada": [True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
    )
    assert not w and len(out) == 1
    assert abs(float(out.iloc[0]["valor_venda"]) - 196.8) < 1e-6


def test_build_nf_grain_soma_comissao_frete_por_linha() -> None:
    """Dois itens na mesma NF: comissão e frete somam todas as linhas."""
    df = pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NFK", "NFK"],
            "Nota_Valor_Liquido_Total": [642.75, 642.75],
            "Nota_Data_Emissao": pd.to_datetime(["2026-03-27", "2026-03-27"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Quantidade": [1.0, 1.0],
            "Preço de lista": [300.0, 342.75],
            "Valor total": [300.0, 342.75],
            "Nome da plataforma": ["MercadoLivre", "MercadoLivre"],
            "Número do pedido multiloja": ["2000015598945394", "2000015598945394"],
            "Taxa de Comissão": [109.27, 109.27],
            "Frete_Plataforma": [91.15, 91.15],
            "Imposto": [0.0, 0.0],
            "Resultado": [0.0, 0.0],
            "Descrição": ["A", "B"],
            "faturamento_nota_vinculada": [True, True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2026, 3, 1),
        nf_d_fim=date(2026, 3, 31),
    )
    assert not w and len(out) == 1
    row = out.iloc[0]
    assert abs(float(row["comissao"]) - 218.54) < 1e-6
    assert abs(float(row["frete"]) - 182.30) < 1e-6
    assert abs(float(row["valor_venda"]) - 642.75) < 1e-6


def test_build_nf_grain_integracommerce_usa_taxa_bling_sem_override() -> None:
    df = pd.DataFrame(
        {
            "empresa": ["A"],
            "org_id": ["o1"],
            "Nota_Numero_Normalizado": ["NFI"],
            "Nota_Valor_Liquido_Total": [200.0],
            "Nota_Data_Emissao": pd.to_datetime(["2026-01-05"]),
            "Nota_Situacao": ["Autorizada"],
            "Quantidade": [1.0],
            "Preço de lista": [100.0],
            "Valor total": [100.0],
            "Nome da plataforma": ["IntegraCommerce"],
            "Número do pedido multiloja": ["ML1"],
            "Taxa de Comissão": [50.0],
            "Frete_Plataforma": [0.0],
            "Imposto": [0.0],
            "Resultado": [40.0],
            "Descrição": ["X"],
            "faturamento_nota_vinculada": [True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2026, 1, 1),
        nf_d_fim=date(2026, 1, 31),
    )
    assert not w and len(out) == 1
    row = out.iloc[0]
    assert abs(float(row["valor_venda"]) - 100.0) < 1e-6
    assert abs(float(row["comissao"]) - 50.0) < 1e-6
    assert abs(float(row["resultado"]) - 40.0) < 1e-6


def test_build_nf_grain_plataforma_resumo_linha_sem_nome_maior_peso_cai_no_fallback() -> None:
    """Linha com maior Q×lista sem «Nome da plataforma»: rótulo cai no 1.º nome válido do grupo."""
    df = pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NFX", "NFX"],
            "Nota_Valor_Liquido_Total": [300.0, 300.0],
            "Nota_Data_Emissao": pd.to_datetime(["2026-01-10", "2026-01-10"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Quantidade": [1.0, 1.0],
            "Preço de lista": [50.0, 999.0],
            "Valor total": [50.0, 999.0],
            "Nome da plataforma": ["MadeiraMadeira", ""],
            "Número do pedido multiloja": ["P1", "P1"],
            "Taxa de Comissão": [0.0, 0.0],
            "Frete_Plataforma": [0.0, 0.0],
            "Imposto": [0.0, 0.0],
            "Resultado": [0.0, 0.0],
            "Descrição": ["A", "B"],
            "faturamento_nota_vinculada": [True, True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2026, 1, 1),
        nf_d_fim=date(2026, 1, 31),
    )
    assert not w and len(out) == 1
    assert "MadeiraMadeira" in str(out.iloc[0]["plataforma_resumo"])


def test_build_nf_grain_madeiramadeira_usa_taxa_bling_sem_override() -> None:
    df = pd.DataFrame(
        {
            "empresa": ["A"],
            "org_id": ["o1"],
            "Nota_Numero_Normalizado": ["NFM"],
            "Nota_Valor_Liquido_Total": [500.0],
            "Nota_Data_Emissao": pd.to_datetime(["2026-01-05"]),
            "Nota_Situacao": ["Autorizada"],
            "Quantidade": [1.0],
            "Preço de lista": [200.0],
            "Valor total": [200.0],
            "Nome da plataforma": ["MadeiraMadeira"],
            "Número do pedido multiloja": ["MM1"],
            "Taxa de Comissão": [80.0],
            "Frete_Plataforma": [0.0],
            "Imposto": [0.0],
            "Resultado": [50.0],
            "Descrição": ["Y"],
            "faturamento_nota_vinculada": [True],
        }
    )
    st = FaturamentoRecorteMinState((), ())
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2026, 1, 1),
        nf_d_fim=date(2026, 1, 31),
    )
    assert not w and len(out) == 1
    row = out.iloc[0]
    assert abs(float(row["comissao"]) - 80.0) < 1e-6
    assert abs(float(row["resultado"]) - 50.0) < 1e-6


def test_nf_grain_plataforma_match_key_unifies_labels() -> None:
    assert nf_grain_plataforma_match_key("MADEIRA MADEIRA") == nf_grain_plataforma_match_key("MadeiraMadeira")
    assert nf_grain_plataforma_match_key("Mercado Livre") == "mercadolivre"


def test_nf_grain_plataforma_ui_options_excludes_bling() -> None:
    s = pd.Series(["Bling", "MercadoLivre", "bling", "Shopee"])
    assert nf_grain_plataforma_ui_options(s) == ["MercadoLivre", "Shopee"]


def test_nf_grain_plataforma_label_for_ui_maps_bling() -> None:
    assert nf_grain_plataforma_label_for_ui("Bling") == "Loja direta"
    assert nf_grain_plataforma_label_for_ui("MercadoLivre") == "MercadoLivre"
    assert nf_grain_plataforma_label_for_ui("") == "—"


def test_build_nf_grain_platform_filter_accepts_alias() -> None:
    df = pd.DataFrame(
        {
            "empresa": ["A", "A"],
            "org_id": ["o1", "o1"],
            "Nota_Numero_Normalizado": ["NF1", "NF2"],
            "Nota_Valor_Liquido_Total": [10.0, 20.0],
            "Nota_Data_Emissao": pd.to_datetime(["2025-06-10", "2025-06-11"]),
            "Nota_Situacao": ["Autorizada", "Autorizada"],
            "Quantidade": [1.0, 1.0],
            "Preço de lista": [5.0, 7.0],
            "Nome da plataforma": ["MadeiraMadeira", "MercadoLivre"],
            "Número do pedido multiloja": ["P1", "P2"],
            "Taxa de Comissão": [0.0, 0.0],
            "Frete_Plataforma": [0.0, 0.0],
            "Imposto": [0.0, 0.0],
            "Resultado": [1.0, 2.0],
            "Descrição": ["X", "Y"],
            "faturamento_nota_vinculada": [True, True],
        }
    )
    st = FaturamentoRecorteMinState((), ("MADEIRA MADEIRA",))
    out, w = build_nf_grain_dataframe(
        df,
        st,
        ok_nf_dates=True,
        nf_d_ini=date(2025, 6, 1),
        nf_d_fim=date(2025, 6, 30),
    )
    assert not w and len(out) == 1
    assert str(out.iloc[0]["Nota_Numero_Normalizado"]) == "NF1"


def test_compute_comercial_conferencia_qtd_x_pl() -> None:
    df = pd.DataFrame(
        {
            "Quantidade": [2, 1],
            "Preço de lista": [10.5, 3.0],
            "Número do pedido multiloja": ["ML1", "ML1"],
        }
    )
    stt = compute_comercial_conferencia_stats(df)
    assert stt.valor_venda == 24.0
    assert stt.linhas_pedido == 2
    assert stt.pedidos_multiloja_distintos == 1


def test_apply_nf_panel_frete_gap_fallback_um_pedido() -> None:
    df = pd.DataFrame(
        {
            "frete": [0.0],
            "valor_faturado_nf": [339.80],
            "valor_venda": [241.0],
            "n_linhas_pedido": [1],
        }
    )
    out = apply_nf_panel_frete_gap_fallback(df)
    assert abs(float(out.iloc[0]["frete"]) - 98.80) < 1e-6


def test_apply_nf_panel_frete_gap_fallback_ignora_varias_linhas_pedido() -> None:
    df = pd.DataFrame(
        {
            "frete": [0.0],
            "valor_faturado_nf": [339.80],
            "valor_venda": [241.0],
            "n_linhas_pedido": [2],
        }
    )
    out = apply_nf_panel_frete_gap_fallback(df)
    assert abs(float(out.iloc[0]["frete"])) < 1e-9


def test_apply_nf_panel_frete_gap_fallback_nao_sobrescreve_frete_comercial() -> None:
    df = pd.DataFrame(
        {
            "frete": [10.0],
            "valor_faturado_nf": [339.80],
            "valor_venda": [241.0],
            "n_linhas_pedido": [1],
        }
    )
    out = apply_nf_panel_frete_gap_fallback(df)
    assert abs(float(out.iloc[0]["frete"]) - 10.0) < 1e-9
