"""
Conciliação de Frete — vendas Mercado Livre + arquivo opcional «frete por anúncio» tabular.

Regras:
- Frete esperado = valor do frete por anúncio (planilha) × quantidade da venda.
- Frete cobrado (ML): **|Receita por envio + Tarifas de envio|** (sempre ≥ 0 na UI); ambos ausentes → 0.
  A coluna **Custo do envio** existe só no export para consulta — **não entra** no cálculo.
- Diferença = **Frete esperado − Frete cobrado** (planilha vs plataforma).
- **Status conciliação** (interno / KPI; não na grelha principal): só estados técnicos.
- **Situação do Frete** (grelha): leitura operacional (OK, Repasse de frete, Cobrado a maior/menor).
- **Ação recomendada** (grelha): texto derivado da situação.
"""
from __future__ import annotations

import hashlib
import os
import re
import tempfile
import time
import unicodedata
from io import BytesIO
from pathlib import Path
from typing import NamedTuple

import numpy as np
import pandas as pd
import streamlit as st

from etapa1_vendas import detect_excel_header_row, list_sales_files, read_sales_file
from fdl_paths import CLIENTE_BASE_DIR, resolve_pasta_vendas_ml

# Colunas e valores na UI (UTF-8). operacional_frete_ui deve importar estes nomes — evita mojibake no .py.
FRETE_UI_N_VENDA = "N.º venda"
FRETE_UI_ANUNCIO = "# de anúncio"
FRETE_UI_STATUS_CONC = "Status conciliação"
FRETE_UI_DIFERENCA = "Diferença"
FRETE_UI_VAL_DIVERGENCIA = "Divergência"
# Situações operacionais (merge mantém todas as vendas ML; planilha só enriquece)
FRETE_UI_STATUS_SEM_FRETE_ML = "Sem frete da plataforma nesta venda"
# Rótulo antigo em CSVs materializados gerados antes da troca de texto na UI.
FRETE_LEGACY_STATUS_SEM_FRETE_ML = "Venda sem informação de frete no ML"
FRETE_UI_STATUS_SEM_PRECO_PLANILHA = "Frete não cadastrado no anúncio"
FRETE_UI_TITULO_ANUNCIO = "Título do anúncio"
FRETE_UI_FRETE_ESPERADO = "Frete esperado"
FRETE_UI_QTD_PRECO_ML = "Qtd × preço unit. produto (ML)"
FRETE_UI_VALOR_FRETE_ANUNCIO = "Valor frete por anúncio"
FRETE_ML_COL = "Frete cobrado"
# Legado em CSVs materializados antigos — não entra na grelha principal.
FRETE_UI_CLASSIFICACAO = "Classificação frete"
FRETE_UI_SITUACAO_FRETE = "Situação do Frete"
FRETE_UI_ACAO_RECOMENDADA = "Ação Recomendada"
# Valores da situação (mesma semântica que antes em «Analisado»).
FRETE_UI_ANALISADO_REPASSE_FRETE = "Repasse de frete"
FRETE_UI_ANALISADO_COBRADO_MAIOR = "Cobrado a maior"
FRETE_UI_ANALISADO_COBRADO_MENOR = "Cobrado a menor"
# Textos da coluna «Ação Recomendada».
FRETE_UI_VAL_ACAO_OK = "Nenhuma ação necessária"
FRETE_UI_VAL_ACAO_REPASSE = "Validar recebimento no extrato"
FRETE_UI_VAL_ACAO_MAIOR = "Entrar em contato com a plataforma"
FRETE_UI_VAL_ACAO_MENOR = "Avaliar impacto / acompanhar"
# Valores fixos do multiselect «Situação do Frete» (mesma ordem da tabela).
FRETE_SITUACAO_FRETE_VALORES_FILTRO: tuple[str, ...] = (
    "OK",
    FRETE_UI_ANALISADO_REPASSE_FRETE,
    FRETE_UI_ANALISADO_COBRADO_MAIOR,
    FRETE_UI_ANALISADO_COBRADO_MENOR,
)
FRETE_UI_RECEBIDO = "Recebido?"
FRETE_VAL_RECEBIDO_SIM = "Sim"
FRETE_VAL_RECEBIDO_NAO = "Não"
# Tolerância para tratar «frete grátis» na planilha (alinhado à conciliação |diferença| ≤ 0,02).
_FRETE_ANALISADO_FE_ZERO_ATOL = 0.02


def _coerce_br_money_series(s: pd.Series | None, index: pd.Index) -> pd.Series:
    """Converte coluna numérica ou texto tipo «R$ 1.234,56» para float."""
    if s is None:
        return pd.Series(np.nan, index=index)
    s = s.reindex(index)
    n = pd.to_numeric(s, errors="coerce")
    if n.notna().any():
        return n
    t = s.astype(str).str.strip().str.replace(r"R\$\s*", "", regex=True)
    t = t.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(t, errors="coerce")


def compute_frete_situacao_frete_column(df: pd.DataFrame) -> pd.Series:
    """
    **Situação do Frete** — leitura de negócio com **Diferença = Frete esperado − Frete cobrado** (colunas na tabela).

    Cenários (tolerância «~0» no esperado: ``_FRETE_ANALISADO_FE_ZERO_ATOL``; cobrado ~0: ``1e-6``):
    1. Esperado ~0 e cobrado ~0 → OK
    2. Esperado ~0 e cobrado > 0 → Repasse de frete
    3. Esperado > 0 e cobrado = esperado (diff ~0) → OK
    4. Esperado > 0 e cobrado ~0 (plataforma não cobrou frete do lojista) → OK — **não** é «Cobrado a menor»
    5. Esperado > 0 e cobrado > esperado → Cobrado a maior
    6. Esperado > 0 e 0 < cobrado < esperado → Cobrado a menor

    Sem frete esperado ou frete cobrado numérico → situação vazia.
    """
    idx = df.index
    diff = _coerce_br_money_series(df.get(FRETE_UI_DIFERENCA), idx)
    fe = _coerce_br_money_series(df.get(FRETE_UI_FRETE_ESPERADO), idx)
    fc = _coerce_br_money_series(df.get(FRETE_ML_COL), idx)
    out = pd.Series(pd.NA, index=idx, dtype=object)
    fe_num = pd.to_numeric(fe.reindex(idx), errors="coerce")
    fc_num = pd.to_numeric(fc.reindex(idx), errors="coerce")
    has_both = fe_num.notna() & fc_num.notna() & diff.notna()
    if not has_both.any():
        return out
    atol_z = 1e-6
    fe_z = pd.Series(
        np.isclose(
            fe_num.to_numpy(dtype=float, copy=False),
            0.0,
            rtol=0,
            atol=_FRETE_ANALISADO_FE_ZERO_ATOL,
        ),
        index=idx,
    )
    fc_z = pd.Series(
        np.isclose(fc_num.to_numpy(dtype=float, copy=False), 0.0, rtol=0, atol=atol_z),
        index=idx,
    )
    d_ok = pd.Series(
        np.isclose(
            diff.reindex(idx).to_numpy(dtype=float, copy=False),
            0.0,
            rtol=0,
            atol=atol_z,
        ),
        index=idx,
    )
    # «Esperado > 0» em termos operacionais: não é frete ~0 na planilha e acima da tolerância mínima.
    fe_pos = ~fe_z & (fe_num > _FRETE_ANALISADO_FE_ZERO_ATOL)

    ok_mask = has_both & (d_ok | (fe_z & fc_z) | (fe_pos & fc_z))
    repasse = has_both & ~ok_mask & fe_z & ~fc_z & (diff < -atol_z)
    maior = has_both & ~ok_mask & fe_pos & ~fc_z & (fc_num > fe_num + atol_z)
    menor = has_both & ~ok_mask & fe_pos & ~fc_z & (fc_num < fe_num - atol_z) & (fc_num > atol_z)
    out.loc[ok_mask] = "OK"
    out.loc[repasse] = FRETE_UI_ANALISADO_REPASSE_FRETE
    out.loc[maior] = FRETE_UI_ANALISADO_COBRADO_MAIOR
    out.loc[menor] = FRETE_UI_ANALISADO_COBRADO_MENOR
    return out


# Compat: nome antigo da função.
compute_frete_analisado_column = compute_frete_situacao_frete_column


def compute_frete_acao_recomendada_column(situacao: pd.Series) -> pd.Series:
    """Deriva **Ação Recomendada** a partir dos valores de **Situação do Frete**."""
    m: dict[str, str] = {
        "OK": FRETE_UI_VAL_ACAO_OK,
        FRETE_UI_ANALISADO_REPASSE_FRETE: FRETE_UI_VAL_ACAO_REPASSE,
        FRETE_UI_ANALISADO_COBRADO_MAIOR: FRETE_UI_VAL_ACAO_MAIOR,
        FRETE_UI_ANALISADO_COBRADO_MENOR: FRETE_UI_VAL_ACAO_MENOR,
    }
    return situacao.map(m).astype(object)


def _compute_frete_cobrado_ml(
    receita: pd.Series,
    tarifas: pd.Series | None,
) -> tuple[pd.Series, pd.Series]:
    """
    Devolve (frete_cobrado, série não usada — reservada para compat).

    Soma receita + tarifas (como no ML) e devolve **valor absoluto** — frete cobrado sempre ≥ 0.
    Ambos ausentes → 0. **Custo do envio não entra** (coluna só informativa no export).
    """
    re = pd.to_numeric(receita, errors="coerce")
    ta = pd.to_numeric(tarifas, errors="coerce") if tarifas is not None else pd.Series(np.nan, index=re.index)
    both_na = re.isna() & ta.isna()
    raw = (re.fillna(0.0) + ta.fillna(0.0)).where(~both_na, 0.0)
    fc = raw.abs()
    return fc, pd.Series(False, index=re.index)


_ML_PT_VENDA_RE = re.compile(
    r"^(\d{1,2})\s+de\s+(.+?)\s+de\s+(\d{4})\s+(\d{1,2}):(\d{2})",
    flags=re.IGNORECASE,
)
# Meses do export ML em português (chave sem acentos — ver _normalize_pt_month_token).
_PT_MONTH_MAP_ASCII: dict[str, int] = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


def _normalize_pt_month_token(raw: str) -> str:
    s = unicodedata.normalize("NFKD", raw.casefold().strip())
    return "".join(c for c in s if not unicodedata.combining(c))


def _parse_ml_pt_br_datetime_string(val: object) -> pd.Timestamp:
    """Export ML em PT-BR: «30 de março de 2026 18:53 hs.»."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return pd.NaT  # type: ignore[return-value]
    s = str(val).strip()
    if not s:
        return pd.NaT  # type: ignore[return-value]
    s_low = s.lower().replace(" hs.", " ").replace("hs.", " ").strip().rstrip(".")
    m = _ML_PT_VENDA_RE.match(s_low)
    if not m:
        return pd.NaT  # type: ignore[return-value]
    d_, mon_raw, y_, h_, mi_ = m.groups()
    mon_key = _normalize_pt_month_token(mon_raw)
    mon = _PT_MONTH_MAP_ASCII.get(mon_key)
    if mon is None:
        return pd.NaT  # type: ignore[return-value]
    try:
        return pd.Timestamp(int(y_), mon, int(d_), int(h_), int(mi_))
    except (ValueError, OverflowError):
        return pd.NaT  # type: ignore[return-value]


def frete_parse_data_venda_series(s: pd.Series) -> pd.Series:
    """
    Converte coluna de data de venda. ISO YYYY-MM-DD primeiro — com dayfirst=True o pandas
    interpreta mal strings como 2026-03-01 (vira 2026-01-03).
    """
    if pd.api.types.is_datetime64_any_dtype(s):
        return s
    if s.dtype == object or str(s.dtype) == "string":
        # Export ML em PT-BR: evita `to_datetime` (avisos dateutil) quando o formato bate.
        pt = s.map(_parse_ml_pt_br_datetime_string)
        if pt.notna().any():
            t = pt.copy()
            need = pt.isna() & s.notna()
            if need.any():
                rest = s.loc[need]
                t.loc[need] = pd.to_datetime(rest, errors="coerce", dayfirst=False)
                still = t.isna() & need
                if still.any():
                    t.loc[still] = pd.to_datetime(rest.loc[still], errors="coerce", dayfirst=True)
            return t
    t = pd.to_datetime(s, errors="coerce", dayfirst=False)
    need = t.isna() & s.notna()
    if need.any():
        t2 = pd.to_datetime(s.loc[need], errors="coerce", dayfirst=True)
        t.loc[need] = t2
    need_pt = t.isna() & s.notna()
    if need_pt.any():
        t.loc[need_pt] = s.loc[need_pt].map(_parse_ml_pt_br_datetime_string)
    return t


def frete_format_data_venda_display(s: pd.Series) -> pd.Series:
    """
    Texto curto para a grelha / export: DD/MM/AAAA HH:MM (pt-BR).
    Se o parse falhar, mantém o valor original como string.
    """
    if s.empty:
        return pd.Series(dtype=object, index=s.index)
    t = s if pd.api.types.is_datetime64_any_dtype(s) else frete_parse_data_venda_series(s)
    fmt = t.dt.strftime("%d/%m/%Y %H:%M")
    fallback = s.map(lambda x: "" if x is None or (isinstance(x, float) and pd.isna(x)) else str(x).strip())
    return fmt.where(t.notna(), fallback)


def frete_series_normalize_sale_dt(s: pd.Series) -> pd.Series:
    """Normaliza data de venda ao dia; materializado/CSV pode trazer object em vez de datetime64."""
    if pd.api.types.is_datetime64_any_dtype(s):
        return s.dt.normalize()
    return frete_parse_data_venda_series(s).dt.normalize()


def frete_series_for_date_filter(df: pd.DataFrame) -> pd.Series:
    """
    Datetimes alinhados a `df.index` para filtro «data da venda» e limites do date_input.
    Prefere `_data_venda_dt` quando preenchida; caso contrário faz parse de `data_venda`
    (incl. texto em português do export ML).
    """
    idx = df.index
    empty = pd.Series(pd.NaT, index=idx, dtype="datetime64[ns]")
    if "data_venda" not in df.columns and "_data_venda_dt" not in df.columns:
        return empty
    parsed: pd.Series | None = None
    if "data_venda" in df.columns:
        parsed = frete_parse_data_venda_series(df["data_venda"])
    if "_data_venda_dt" not in df.columns:
        return parsed if parsed is not None else empty
    from_file = pd.to_datetime(df["_data_venda_dt"], errors="coerce")
    if parsed is None:
        return from_file
    return from_file.where(from_file.notna(), parsed)


def dataframe_frete_conciliacao_principal(
    df: pd.DataFrame,
    *,
    recebido: pd.Series | None = None,
    layout: str = "completo",
) -> pd.DataFrame:
    """
    Colunas da tabela principal de conciliação (ordem + rótulos de exibição).

    ``layout``:
    - ``completo`` — inclui valor frete por anúncio e quantidade (simulação / export detalhado).
    - ``executivo`` — foco gestão: data, venda, anúncio, esperado, cobrado, diferença, situação, ação, recebido.
    """
    work = df.copy()
    if "data_venda" not in work.columns and "_data_venda_dt" in work.columns:
        work["data_venda"] = work["_data_venda_dt"]
    if FRETE_UI_CLASSIFICACAO in work.columns:
        work = work.drop(columns=[FRETE_UI_CLASSIFICACAO])

    situacao = compute_frete_situacao_frete_column(work)
    acao = compute_frete_acao_recomendada_column(situacao)

    if layout == "executivo":
        order: list[tuple[str, str]] = [
            ("data_venda", "Data da venda"),
            (FRETE_UI_N_VENDA, "N.º venda"),
            ("Estado", "Estado da venda"),
            (FRETE_UI_ANUNCIO, "Número do anúncio"),
            (FRETE_UI_FRETE_ESPERADO, "Frete esperado"),
            (FRETE_ML_COL, "Frete cobrado"),
            (FRETE_UI_DIFERENCA, "Diferença"),
            (FRETE_UI_SITUACAO_FRETE, "Situação do Frete"),
            (FRETE_UI_ACAO_RECOMENDADA, "Ação Recomendada"),
        ]
    else:
        order = [
            ("data_venda", "Data da venda"),
            (FRETE_UI_N_VENDA, "N.º venda"),
            ("Estado", "Estado da venda"),
            (FRETE_UI_ANUNCIO, "Número do anúncio"),
            (FRETE_UI_VALOR_FRETE_ANUNCIO, "Valor frete por anúncio"),
            ("Unidades", "Quantidade"),
            (FRETE_UI_FRETE_ESPERADO, "Frete esperado"),
            (FRETE_ML_COL, "Frete cobrado"),
            (FRETE_UI_DIFERENCA, "Diferença"),
            (FRETE_UI_SITUACAO_FRETE, "Situação do Frete"),
            (FRETE_UI_ACAO_RECOMENDADA, "Ação Recomendada"),
        ]
    out = pd.DataFrame(index=work.index)
    for col_key, label in order:
        if col_key == FRETE_UI_SITUACAO_FRETE:
            out[label] = situacao
        elif col_key == FRETE_UI_ACAO_RECOMENDADA:
            out[label] = acao
        elif col_key == "data_venda" and col_key in work.columns:
            out[label] = frete_format_data_venda_display(work[col_key])
        else:
            out[label] = work[col_key] if col_key in work.columns else pd.NA
    if recebido is not None:
        out[FRETE_UI_RECEBIDO] = recebido.reindex(work.index)
    return out


def frete_impacto_financeiro_por_situacao(df: pd.DataFrame) -> dict[str, float]:
    """Soma |Diferença| (R$) por situação operacional (três categorias de impacto)."""
    sit = compute_frete_situacao_frete_column(df)
    d = pd.to_numeric(df.get(FRETE_UI_DIFERENCA), errors="coerce").fillna(0.0)
    ad = d.abs()
    return {
        "repasse": float(ad[sit.eq(FRETE_UI_ANALISADO_REPASSE_FRETE)].sum()),
        "cobrado_menor": float(ad[sit.eq(FRETE_UI_ANALISADO_COBRADO_MENOR)].sum()),
        "cobrado_maior": float(ad[sit.eq(FRETE_UI_ANALISADO_COBRADO_MAIOR)].sum()),
    }


def frete_kpis_executivos(df: pd.DataFrame) -> dict[str, float]:
    """KPIs executivos: montantes |Δ| em «Cobrado a maior» e «Repasse de frete»."""
    imp = frete_impacto_financeiro_por_situacao(df)
    return {
        "cobrado_maior": imp["cobrado_maior"],
        "repasse": imp["repasse"],
    }


def frete_tabela_anuncios_cobrado_maior(df: pd.DataFrame) -> pd.DataFrame:
    """
    Por anúncio: linhas «Cobrado a maior» — qtde, soma frete cobrado (volume), soma |Δ| (impacto a recuperar).
    """
    if FRETE_UI_ANUNCIO not in df.columns:
        return pd.DataFrame(
            columns=["Anúncio", "Qtde ocorrências", "Valor total (R$)", "Impacto (R$)"]
        )
    sit = compute_frete_situacao_frete_column(df)
    sub = df.loc[sit.eq(FRETE_UI_ANALISADO_COBRADO_MAIOR)].copy()
    if sub.empty:
        return pd.DataFrame(
            columns=["Anúncio", "Qtde ocorrências", "Valor total (R$)", "Impacto (R$)"]
        )
    fc = pd.to_numeric(sub.get(FRETE_ML_COL), errors="coerce").fillna(0.0)
    d = pd.to_numeric(sub.get(FRETE_UI_DIFERENCA), errors="coerce").fillna(0.0)
    sub["_fc"] = fc
    sub["_ab"] = d.abs()
    g = (
        sub.groupby(FRETE_UI_ANUNCIO, dropna=False)
        .agg(
            _qtde=(FRETE_UI_N_VENDA, "count"),
            _valor_total=("_fc", "sum"),
            _impacto=("_ab", "sum"),
        )
        .reset_index()
    )
    g["Anúncio"] = g[FRETE_UI_ANUNCIO].map(lambda x: "" if pd.isna(x) else str(x).strip())
    out = pd.DataFrame(
        {
            "Anúncio": g["Anúncio"],
            "Qtde ocorrências": g["_qtde"].astype(int),
            "Valor total (R$)": g["_valor_total"].round(2),
            "Impacto (R$)": g["_impacto"].round(2),
        }
    )
    return out.sort_values("Impacto (R$)", ascending=False).reset_index(drop=True)


def frete_tabela_anuncios_repasse_frete(df: pd.DataFrame, recebido: pd.Series) -> pd.DataFrame:
    """
    Por anúncio: linhas «Repasse de frete» — qtde, soma |Δ| (valor a conferir), «Recebido?» se todas as linhas Sim.
    """
    if FRETE_UI_ANUNCIO not in df.columns:
        return pd.DataFrame(columns=["Anúncio", "Qtde ocorrências", "Valor total (R$)", FRETE_UI_RECEBIDO])
    sit = compute_frete_situacao_frete_column(df)
    sub = df.loc[sit.eq(FRETE_UI_ANALISADO_REPASSE_FRETE)].copy()
    if sub.empty:
        return pd.DataFrame(columns=["Anúncio", "Qtde ocorrências", "Valor total (R$)", FRETE_UI_RECEBIDO])
    d = pd.to_numeric(sub.get(FRETE_UI_DIFERENCA), errors="coerce").fillna(0.0)
    sub["_ab"] = d.abs()
    r = recebido.reindex(sub.index).astype(str).str.strip()
    sub["_rec_sim"] = r.eq(FRETE_VAL_RECEBIDO_SIM)

    g1 = (
        sub.groupby(FRETE_UI_ANUNCIO, dropna=False)
        .agg(
            _qtde=(FRETE_UI_N_VENDA, "count"),
            _valor=("_ab", "sum"),
        )
        .reset_index()
    )
    g2 = (
        sub.groupby(FRETE_UI_ANUNCIO, dropna=False)["_rec_sim"]
        .agg(lambda s: FRETE_VAL_RECEBIDO_SIM if bool(s.all()) else FRETE_VAL_RECEBIDO_NAO)
        .rename(FRETE_UI_RECEBIDO)
    )
    g1 = g1.set_index(FRETE_UI_ANUNCIO).join(g2, how="left").reset_index()
    g1["Anúncio"] = g1[FRETE_UI_ANUNCIO].map(lambda x: "" if pd.isna(x) else str(x).strip())
    out = pd.DataFrame(
        {
            "Anúncio": g1["Anúncio"],
            "Qtde ocorrências": g1["_qtde"].astype(int),
            "Valor total (R$)": g1["_valor"].round(2),
            FRETE_UI_RECEBIDO: g1[FRETE_UI_RECEBIDO],
        }
    )
    return out.sort_values("Valor total (R$)", ascending=False).reset_index(drop=True)


def frete_repasse_nao_conferido_rs(df: pd.DataFrame, recebido: pd.Series) -> float:
    """Soma |Δ| em linhas «Repasse de frete» com Recebido ≠ Sim."""
    sit = compute_frete_situacao_frete_column(df)
    d = pd.to_numeric(df.get(FRETE_UI_DIFERENCA), errors="coerce").fillna(0.0)
    r = recebido.reindex(df.index).astype(str).str.strip()
    m = sit.eq(FRETE_UI_ANALISADO_REPASSE_FRETE) & ~r.eq(FRETE_VAL_RECEBIDO_SIM)
    return float(d.abs()[m].sum())


def frete_situacao_com_indicador_visual(s: pd.Series) -> pd.Series:
    """Prefixo por cor (emoji) para leitura rápida na UI."""
    pref = {
        "OK": "🟢 ",
        FRETE_UI_ANALISADO_REPASSE_FRETE: "🟡 ",
        FRETE_UI_ANALISADO_COBRADO_MAIOR: "🔴 ",
        FRETE_UI_ANALISADO_COBRADO_MENOR: "🟠 ",
    }

    def _m(x: object) -> object:
        if pd.isna(x):
            return x
        vs = str(x).strip()
        return pref[vs] + vs if vs in pref else vs

    return s.map(_m)


FRETE_OPERACIONAL_REQUIRED_COLUMNS: frozenset[str] = frozenset(
    {FRETE_UI_N_VENDA, FRETE_ML_COL, "Estado"}
)


def normalize_frete_status_conc_display(df: pd.DataFrame) -> pd.DataFrame:
    """Ajusta colunas persistidas (CSV materializado): rótulos legados e «Repasse de frete» no Status."""
    if FRETE_UI_STATUS_CONC not in df.columns:
        return df
    out = df.copy()
    s = out[FRETE_UI_STATUS_CONC]  # noqa: PD901
    m_legacy = s.astype(str).str.strip() == FRETE_LEGACY_STATUS_SEM_FRETE_ML
    if m_legacy.any():
        out.loc[m_legacy, FRETE_UI_STATUS_CONC] = FRETE_UI_STATUS_SEM_FRETE_ML
    # Antigo Status técnico «Repasse de frete» (ML) → OK ou Divergência conforme |Diferença| (tol 0,02).
    m_rep = out[FRETE_UI_STATUS_CONC].astype(str).str.strip() == FRETE_UI_ANALISADO_REPASSE_FRETE
    if m_rep.any():
        tol = 0.02
        if FRETE_UI_DIFERENCA in out.columns:
            d = pd.to_numeric(out[FRETE_UI_DIFERENCA], errors="coerce")
            ok_m = m_rep & d.notna() & (d.abs() <= tol)
            div_m = m_rep & d.notna() & (d.abs() > tol)
            unk_m = m_rep & d.isna()
            out.loc[ok_m, FRETE_UI_STATUS_CONC] = "OK"
            out.loc[div_m, FRETE_UI_STATUS_CONC] = FRETE_UI_VAL_DIVERGENCIA
            out.loc[unk_m, FRETE_UI_STATUS_CONC] = "OK"
        else:
            out.loc[m_rep, FRETE_UI_STATUS_CONC] = "OK"
    return out


def validate_frete_operacional_dataframe(df: pd.DataFrame) -> None:
    """Garante o schema mínimo da tabela operacional (app live, materializado, materialize_financeiro)."""
    missing = sorted(FRETE_OPERACIONAL_REQUIRED_COLUMNS - set(df.columns))
    if missing:
        raise ValueError(
            "Dataset operacional de frete sem colunas obrigatórias: " + ", ".join(missing)
        )


class FontesFrete(NamedTuple):
    """Fontes para o painel de Frete: pastas locais (FDL_BASE_DIR) ou URLs (Secrets Cloud)."""

    vendas_path: Path | None
    frete_path: Path | None
    vendas_url: str
    frete_url: str


def _frete_secret_str(*keys: str) -> str:
    for k in keys:
        v = os.environ.get(k, "").strip()
        if v:
            return v
    try:
        sec = st.secrets
        for k in keys:
            if k in sec and str(sec[k]).strip():
                return str(sec[k]).strip()
    except Exception:
        pass
    return ""


def stable_mtime_ns_for_frete_url(url: str) -> int:
    """Chave estável para @st.cache_data quando a fonte é URL (ficheiro remoto)."""
    return int.from_bytes(hashlib.sha256(url.strip().encode()).digest()[:8], "big", signed=False)


def _is_http_url(s: str) -> bool:
    t = s.strip().lower()
    return t.startswith("http://") or t.startswith("https://")


def _download_frete_payload(
    url: str,
    debug_log: callable | None = None,
) -> tuple[bytes, str, str | None]:
    """Descarrega ficheiro de Frete — mesma estratégia que FDL_PRECOMPUTED_URL (SharePoint/Graph)."""
    from app_operacional import (  # noqa: PLC0415 — evita import circular no carregamento do módulo
        PRECOMPUTED_HTTP_TIMEOUT,
        _download_file_bytes,
        _precomputed_download_attempts,
    )

    errs: list[str] = []
    for dl_url, hdr in _precomputed_download_attempts(url.strip()):
        t0 = time.perf_counter()
        if debug_log:
            debug_log(f"download_tentativa url={dl_url}")
        try:
            payload, filename, last_modified = _download_file_bytes(
                dl_url,
                extra_headers=hdr or None,
                timeout=PRECOMPUTED_HTTP_TIMEOUT,
                http_retries=1,
            )
            if debug_log:
                elapsed = time.perf_counter() - t0
                debug_log(
                    f"download_sucesso url={dl_url} tempo={elapsed:.2f}s arquivo={filename or 'download.bin'} bytes={len(payload)}"
                )
            return payload, filename, last_modified
        except Exception as exc:  # noqa: BLE001
            if debug_log:
                elapsed = time.perf_counter() - t0
                debug_log(f"download_falha url={dl_url} tempo={elapsed:.2f}s erro={exc}")
            errs.append(str(exc))
    raise ValueError(
        "Não foi possível descarregar o ficheiro de Frete a partir do URL. "
        "Confirme partilha «qualquer pessoa com a ligação». Detalhes: "
        + " | ".join(errs[:5])
        + (" …" if len(errs) > 5 else "")
    )


def _read_vendas_ml_bytes(payload: bytes, filename: str) -> pd.DataFrame:
    """Lê export ML (.csv / .xlsx) a partir de bytes (URL remota)."""
    fn = (filename or "download").lower()
    if fn.endswith(".csv"):
        bio = BytesIO(payload)
        last_err: Exception | None = None
        for encoding in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
            try:
                bio.seek(0)
                return pd.read_csv(
                    bio,
                    encoding=encoding,
                    sep=None,
                    engine="python",
                    dtype=str,
                )
            except Exception as e:  # noqa: BLE001
                last_err = e
        raise RuntimeError(f"Falha ao ler CSV de vendas ML: {last_err}")
    head = payload.lstrip()[:4]
    is_xlsx = fn.endswith(".xlsx") or fn.endswith(".xls") or head == b"PK\x03\x04"
    if not is_xlsx:
        bio = BytesIO(payload)
        try:
            return pd.read_csv(bio, sep=None, engine="python", dtype=str, encoding="utf-8-sig")
        except Exception as exc:
            raise ValueError(
                f"Formato de ficheiro não reconhecido para vendas ML ({filename!r}). Use .csv ou .xlsx."
            ) from exc
    sfx = ".xlsx" if fn.endswith(".xlsx") or head == b"PK\x03\x04" else ".xls"
    with tempfile.NamedTemporaryFile(suffix=sfx, delete=False) as tmp:
        tmp.write(payload)
        tmp_path = Path(tmp.name)
    try:
        return _read_excel_frete_ml_best_header(tmp_path)
    finally:
        tmp_path.unlink(missing_ok=True)


def _read_excel_frete_ml_best_header(path: Path) -> pd.DataFrame:
    """
    Vários exports ML têm linhas de título antes do cabeçalho real.
    Procura a primeira linha de cabeçalho onde existem colunas de frete ML reconhecíveis.
    """
    peek = pd.read_excel(path, header=None, engine="openpyxl", nrows=35)
    nrows = len(peek)
    for hr in range(0, min(18, nrows)):
        df = pd.read_excel(path, header=hr, engine="openpyxl")
        df = df.dropna(axis=1, how="all")
        cmap = _resolve_columns(df)
        if _cmap_sufficient_for_frete_ml(cmap):
            return df
    hr = detect_excel_header_row(path)
    return pd.read_excel(path, header=hr, engine="openpyxl")


def _strip_ascii_lower(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[^a-z0-9]+", " ", s).strip()
    return re.sub(r"\s+", " ", s)


def _pick_col(columns: list[str], *needles: str) -> str | None:
    for c in columns:
        n = _strip_ascii_lower(str(c))
        if all(nd in n for nd in needles):
            return c
    return None


def _pick_sale_col(columns: list[str]) -> str | None:
    for c in columns:
        n = _strip_ascii_lower(str(c))
        t = set(n.split())
        if "venda" in t and ("n" in t or "no" in t or "numero" in n):
            return c
    return _pick_col(columns, "n", "venda")


def _resolve_columns(df: pd.DataFrame) -> dict[str, str]:
    cols = list(df.columns)
    out: dict[str, str] = {}

    if (sale := _pick_sale_col(cols)):
        out["n_venda"] = sale

    est = next((c for c in cols if str(c).strip() == "Estado"), None) or _pick_col(cols, "estado")
    if est:
        out["estado"] = est

    if (dsc := _pick_col(cols, "descri", "status")):
        out["descricao_status"] = dsc

    if (q := _pick_col(cols, "unidades")):
        out["unidades"] = q

    rec = _pick_col(cols, "receita", "envio") or _pick_col(cols, "receita", "frete")
    if rec:
        out["receita_envio"] = rec
    if "receita_envio" not in out:
        for c in cols:
            n = _strip_ascii_lower(str(c))
            if "receita" in n and ("envio" in n or "frete" in n or "shipping" in n):
                out["receita_envio"] = c
                break

    tar = next(
        (
            c
            for c in cols
            if "tarifa" in _strip_ascii_lower(str(c)) and "envio" in _strip_ascii_lower(str(c))
        ),
        None,
    )
    if tar:
        out["tarifas_envio"] = tar
    if "tarifas_envio" not in out:
        for c in cols:
            n = _strip_ascii_lower(str(c))
            if ("tarifa" in n or "tarifas" in n) and ("envio" in n or "frete" in n):
                out["tarifas_envio"] = c
                break

    cust = _pick_col(cols, "custo", "envio") or _pick_col(cols, "custo", "frete")
    if cust is None:
        for c in cols:
            n = _strip_ascii_lower(str(c))
            if "custo" in n and ("envio" in n or "shipping" in n or "frete" in n):
                cust = c
                break
    if cust:
        out["custo_envio"] = cust

    an = next((c for c in cols if "#" in str(c) and "an" in _strip_ascii_lower(str(c))), None)
    if an is None:
        an = _pick_col(cols, "de", "anuncio")
    if an is None:
        an = _pick_col(cols, "id", "anuncio")
    if an is None:
        for c in cols:
            n = _strip_ascii_lower(str(c))
            if "mlb" in n.replace(" ", "") or ("item" in n and "id" in n):
                an = c
                break
    if an:
        out["id_anuncio"] = an

    if (tit := _pick_col(cols, "titulo", "anuncio")):
        out["titulo_anuncio"] = tit

    for c in cols:
        n = _strip_ascii_lower(str(c))
        if "pre" in n and "unit" in n and "anuncio" in n:
            out["preco_unit_anuncio_produto"] = c
            break

    if (dt := _pick_col(cols, "data", "venda")):
        out["data_venda"] = dt

    return out


def _cmap_sufficient_for_frete_ml(cmap: dict[str, str]) -> bool:
    base = {"n_venda", "receita_envio", "unidades", "id_anuncio"}
    return base.issubset(cmap.keys())


def _latest_vendas_ml_path(folder: Path) -> Path | None:
    """
    Escolhe vendas para o módulo Frete. ``list_sales_files`` já ordena por mtime (mais recente primeiro).
    Não usar só o ficheiro mais recente: pastas do cliente podem misturar exports Bling/repasse (colunas
    «Data», «Número»…) com o CSV/xlsx «Pedidos» do ML; o mais recente por vezes é o errado.
    """
    if not folder.is_dir():
        return None
    try:
        files = [p for p in list_sales_files(folder) if p.suffix.lower() in {".xlsx", ".xls", ".csv"}]
    except OSError:
        return None
    if not files:
        return None
    _max_probe = 48
    for p in files[:_max_probe]:
        try:
            df = read_sales_file(p)
        except Exception:
            continue
        if getattr(df, "empty", True):
            continue
        df = df.dropna(axis=1, how="all")
        try:
            cmap = _resolve_columns(df)
        except Exception:
            continue
        if _cmap_sufficient_for_frete_ml(cmap):
            return p
    # Pastas com export «Pedidos» (resumo) + outros CSV mais recentes: preferir «Pedidos» para mensagem de erro
    # clara no loader; se não houver, compat. com o comportamento antigo (ficheiro mais recente).
    pedidos = [p for p in files if "pedidos" in p.name.lower()]
    if pedidos:
        try:
            return max(pedidos, key=lambda x: x.stat().st_mtime)
        except OSError:
            pass
    try:
        return max(files, key=lambda p: p.stat().st_mtime)
    except OSError:
        return None


def _find_frete_anuncio_path(base: Path) -> Path | None:
    """
    Descobre a planilha «Frete por Anúncio» no diretório do cliente: raiz, subpastas diretas
    e, em último caso, qualquer .xlsx cujo nome sugira frete+anúncio (mais recente por mtime).
    """
    if not base.is_dir():
        return None
    pats = (
        "*Frete*Anuncio*.xlsx",
        "*frete*anuncio*.xlsx",
        "*Frete*Anúncio*.xlsx",
        "*Frete*por*Anuncio*.xlsx",
        "*frete*por*anuncio*.xlsx",
        "*Frete*por*Anúncio*.xlsx",
    )
    cands: list[Path] = []
    for pat in pats:
        try:
            cands.extend(base.glob(pat))
        except OSError:
            pass
    for sub in [p for p in base.iterdir() if p.is_dir()]:
        for pat in pats:
            try:
                cands.extend(sub.glob(pat))
            except OSError:
                pass
    seen: set[str] = set()
    for p in cands:
        if p.is_file():
            seen.add(str(p.resolve()))
    if not seen:
        try:
            for p in base.rglob("*.xlsx"):
                if not p.is_file():
                    continue
                n = p.name.lower().replace("ú", "u")
                if "frete" in n and "anuncio" in n:
                    seen.add(str(p.resolve()))
        except OSError:
            pass
    uniq = [Path(s) for s in seen]
    if not uniq:
        return None
    try:
        return max(uniq, key=lambda p: p.stat().st_mtime)
    except OSError:
        return None


def _col_preco_frete_por_anuncio(cols: list[str], id_c: str | None) -> str | None:
    """Coluna de valor do frete unitário (evita confundir com 'Receita por frete', etc.)."""
    for c in cols:
        if c == id_c:
            continue
        if _strip_ascii_lower(str(c)) == "frete":
            return c
    for c in cols:
        if c == id_c:
            continue
        n = _strip_ascii_lower(str(c))
        if "frete" in n and "tarifa" not in n and "receita" not in n and "custo" not in n:
            return c
    return None


def _extract_frete_por_anuncio_from_df(df: pd.DataFrame) -> pd.DataFrame | None:
    """
    A partir de um DataFrame já com linha de cabeçalho, extrai MLB + preço unitário de frete.
    """
    df = df.dropna(axis=1, how="all")
    if df.shape[1] < 2:
        return None
    cols = list(df.columns)
    id_c = None
    for c in cols:
        if df[c].dropna().astype(str).str.contains(r"MLB\d", regex=True, na=False).any():
            id_c = c
            break
    if id_c is None:
        id_c = _pick_col(cols, "anuncio") or _pick_col(cols, "item")
    num_c = _col_preco_frete_por_anuncio(cols, id_c)
    if num_c is None:
        for c in cols:
            if c == id_c:
                continue
            if pd.api.types.is_numeric_dtype(df[c]):
                num_c = c
                break
    if num_c is None:
        for c in cols:
            if c == id_c:
                continue
            ok = int(pd.to_numeric(df[c], errors="coerce").notna().sum())
            if ok >= max(3, min(len(df), 5)):
                num_c = c
                break
    if id_c is None or num_c is None:
        return None

    preco = pd.to_numeric(df[num_c], errors="coerce")
    if int(preco.notna().sum()) < 1:
        s = df[num_c].astype(str).str.strip()
        s = s.str.replace("R$", "", regex=False).str.replace("r$", "", regex=False)
        s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
        preco = pd.to_numeric(s, errors="coerce")

    out = pd.DataFrame(
        {
            "_id_anuncio_norm": df[id_c]
            .astype(str)
            .str.replace(r"\s+", "", regex=True)
            .str.upper()
            .str.replace(r"^#", "", regex=True),
            "_preco_frete_unit_arquivo": preco,
        }
    )
    out = out.loc[out["_id_anuncio_norm"].str.len() > 3].drop_duplicates("_id_anuncio_norm", keep="last")
    if int(out["_preco_frete_unit_arquivo"].notna().sum()) < 1:
        return None
    return out


def _read_frete_anuncio_sheet_with_detected_header(path: Path, sheet_name: str | int) -> pd.DataFrame | None:
    """Primeira aba legada: localizar linha de cabeçalho nos primeiros 35 registos."""
    try:
        raw = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=35, engine="openpyxl")
    except Exception:
        return None
    best_i = -1
    best_score = -1
    for i in range(len(raw)):
        row = [str(x).lower() for x in raw.iloc[i].tolist() if pd.notna(x)]
        joined = " ".join(row)
        score = 0
        if "mlb" in joined or "#" in joined:
            score += 2
        if "pre" in joined or "frete" in joined or "r$" in joined:
            score += 1
        if score > best_score:
            best_score = score
            best_i = i
    if best_score < 2 or best_i < 0:
        return None
    try:
        df = pd.read_excel(path, sheet_name=sheet_name, header=int(best_i), engine="openpyxl")
    except Exception:
        return None
    return df


def _try_read_tabular_frete_anuncio(path: Path) -> pd.DataFrame | None:
    """
    Lê planilha «Frete por Anúncio»: prioriza a 2.ª aba (tabela Anúncio + Frete), depois as outras.
    A 1.ª aba costuma ser export visual do ML sem colunas tabulares.
    """
    if path.suffix.lower() not in {".xlsx", ".xls"}:
        return None
    try:
        xl = pd.ExcelFile(path, engine="openpyxl")
    except Exception:
        return None
    names = xl.sheet_names
    if not names:
        return None

    order: list[str | int] = []
    if len(names) >= 2:
        order.append(names[1])
    for n in names:
        if n not in order:
            order.append(n)

    for sheet in order:
        try:
            df0 = pd.read_excel(path, sheet_name=sheet, header=0, engine="openpyxl")
        except Exception:
            df0 = None
        if df0 is not None:
            ext = _extract_frete_por_anuncio_from_df(df0)
            if ext is not None:
                return ext

        df1 = _read_frete_anuncio_sheet_with_detected_header(path, sheet)
        if df1 is not None:
            ext = _extract_frete_por_anuncio_from_df(df1)
            if ext is not None:
                return ext

    return None


@st.cache_data(show_spinner=False, ttl=120)
def carregar_base_frete_ml(
    _org_id: str,
    vendas_path_str: str,
    vendas_mtime_ns: int,
    frete_path_str: str | None,
    frete_mtime_ns: int | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    del _org_id
    del vendas_mtime_ns
    del frete_mtime_ns
    meta: dict[str, object] = {
        "vendas_arquivo": "",
        "frete_arquivo": None,
        "frete_tabular": False,
        "avisos": [],
        "debug_logs": [],
    }

    t_global = time.perf_counter()

    def _dbg(msg: str) -> None:
        elapsed = time.perf_counter() - t_global
        meta["debug_logs"].append(f"[{elapsed:7.2f}s] {msg}")

    if _is_http_url(vendas_path_str):
        _dbg("origem_vendas=url")
        t_dl = time.perf_counter()
        payload_v, fn_v, _lm_v = _download_frete_payload(vendas_path_str.strip(), debug_log=_dbg)
        del _lm_v
        _dbg(f"download_vendas_total={time.perf_counter() - t_dl:.2f}s")
        t_parse = time.perf_counter()
        df = _read_vendas_ml_bytes(payload_v, fn_v)
        _dbg(f"parse_vendas_total={time.perf_counter() - t_parse:.2f}s")
        meta["vendas_arquivo"] = fn_v or "vendas_ml_remoto"
    else:
        _dbg(f"origem_vendas=local path={vendas_path_str}")
        meta["vendas_arquivo"] = Path(vendas_path_str).name
        t_parse = time.perf_counter()
        df = read_sales_file(Path(vendas_path_str))
        _dbg(f"parse_vendas_total={time.perf_counter() - t_parse:.2f}s")
    df = df.dropna(axis=1, how="all")
    _dbg(f"colunas_vendas={list(df.columns)}")
    cmap = _resolve_columns(df)
    _dbg(f"colunas_detectadas={cmap}")
    if not _cmap_sufficient_for_frete_ml(cmap):
        miss = sorted(
            {"n_venda", "receita_envio", "unidades", "id_anuncio"}
            - set(cmap.keys())
        )
        if miss:
            _dbg(f"colunas_obrigatorias_faltando={miss}")
            raise ValueError(
                "Relatório de vendas ML sem colunas necessárias. "
                f"Falta: {miss}. Primeiras colunas: {list(df.columns)[:20]}"
            )
    v = df.rename(columns={cmap[k]: k for k in cmap})

    v = v.copy()
    ta_ser = v["tarifas_envio"] if "tarifas_envio" in v.columns else None
    frete_ml, _repasse = _compute_frete_cobrado_ml(v["receita_envio"], ta_ser)
    meta["frete_cobrado_modo"] = "receita_mais_tarifas_sinal"
    v["frete_ml"] = frete_ml

    qtd = pd.to_numeric(v["unidades"], errors="coerce").fillna(0.0)
    v["_qtd"] = qtd
    v["_id_norm"] = (
        v["id_anuncio"].astype(str).str.strip().str.upper().str.replace(r"\s+", "", regex=True)
    )

    if "preco_unit_anuncio_produto" in v.columns:
        pu = pd.to_numeric(v["preco_unit_anuncio_produto"], errors="coerce")
        v["qtd_x_preco_produto_ml"] = qtd * pu

    if "data_venda" in v.columns:
        v["_data_venda_dt"] = frete_parse_data_venda_series(v["data_venda"])

    tab: pd.DataFrame | None = None
    if frete_path_str:
        if _is_http_url(frete_path_str):
            _dbg("origem_frete_anuncio=url")
            try:
                payload_f, fn_f, _lm_f = _download_frete_payload(
                    frete_path_str.strip(), debug_log=_dbg
                )
                del _lm_f
                sfx = Path(fn_f).suffix.lower()
                if sfx not in {".xlsx", ".xls"}:
                    meta["avisos"].append(
                        "FDL_FRETE_ANUNCIO_URL deve apontar para ficheiro .xlsx ou .xls (tabela de frete por anúncio)."
                    )
                else:
                    with tempfile.NamedTemporaryFile(suffix=sfx, delete=False) as tmp:
                        tmp.write(payload_f)
                        tmp_p = Path(tmp.name)
                    try:
                        meta["frete_arquivo"] = fn_f
                        tab = _try_read_tabular_frete_anuncio(tmp_p)
                        _dbg(f"frete_anuncio_tabular_detectado={tab is not None}")
                    finally:
                        tmp_p.unlink(missing_ok=True)
            except Exception as exc:  # noqa: BLE001
                _dbg(f"erro_frete_anuncio_url={exc}")
                meta["avisos"].append(f"Erro ao descarregar frete por anúncio (URL): {exc}")
        elif Path(frete_path_str).is_file():
            _dbg(f"origem_frete_anuncio=local path={frete_path_str}")
            meta["frete_arquivo"] = Path(frete_path_str).name
            tab = _try_read_tabular_frete_anuncio(Path(frete_path_str))
            _dbg(f"frete_anuncio_tabular_detectado={tab is not None}")

    if tab is not None:
        if tab.empty:
            meta["avisos"].append(
                "Arquivo de frete por anúncio não reconhecido como tabela (p.ex. export de ecrã). "
                "Divergências por anúncio ficam indisponíveis até existir Excel/CSV com colunas MLB + preço."
            )
        else:
            meta["frete_tabular"] = True
            m = v.merge(tab, left_on="_id_norm", right_on="_id_anuncio_norm", how="left")
            m["frete_esperado"] = m["_qtd"] * m["_preco_frete_unit_arquivo"]
            m["diferenca"] = m["frete_esperado"] - m["frete_ml"]
            tol = 0.02
            fc = m["frete_ml"]
            fe = m["frete_esperado"]
            d = m["diferenca"]
            preco_plan = m["_preco_frete_unit_arquivo"]
            receita_v = pd.to_numeric(m["receita_envio"], errors="coerce")
            tarifas_v = (
                pd.to_numeric(m["tarifas_envio"], errors="coerce")
                if "tarifas_envio" in m.columns
                else pd.Series(np.nan, index=m.index)
            )
            # Custo do envio não entra no cálculo nem no critério de status.
            ml_sem_info = receita_v.isna() & tarifas_v.isna()

            # Status só técnico: sem preço planilha; sem ML; OK / Divergência por |d| (receita sem tarifas
            # deixa de ser rótulo especial — cai no OK/Divergência como as demais linhas).
            pode_conciliar = fe.notna() & fc.notna()
            st_ok = np.select(
                [
                    preco_plan.isna(),
                    ml_sem_info,
                    pode_conciliar & (d.abs() <= tol),
                    pode_conciliar & (d.abs() > tol),
                ],
                [
                    FRETE_UI_STATUS_SEM_PRECO_PLANILHA,
                    FRETE_UI_STATUS_SEM_FRETE_ML,
                    "OK",
                    FRETE_UI_VAL_DIVERGENCIA,
                ],
                default="OK",
            )
            m["status_conc"] = st_ok
            v = m.drop(columns=["_id_anuncio_norm"], errors="ignore")

    if frete_path_str and tab is None and meta.get("frete_arquivo"):
        meta["avisos"].append(
            "Arquivo de frete por anúncio não reconhecido como tabela (p.ex. export de ecrã). "
            "Divergências por anúncio ficam indisponíveis até existir Excel/CSV com colunas MLB + preço."
        )

    drop_h = [c for c in ("_id_norm", "_qtd") if c in v.columns]
    if drop_h:
        v = v.drop(columns=drop_h)

    # Nomes finais expostos à UI — manter UTF-8; importar FRETE_UI_* em operacional_frete_ui.
    rename_final = {
        "n_venda": FRETE_UI_N_VENDA,
        "estado": "Estado",
        "descricao_status": "Descrição do status",
        "unidades": "Unidades",
        "receita_envio": "Receita por envio (BRL)",
        "tarifas_envio": "Tarifas de envio (BRL)",
        "custo_envio": "Custo do envio (BRL)",
        "frete_ml": FRETE_ML_COL,
        "frete_esperado": FRETE_UI_FRETE_ESPERADO,
        "diferenca": FRETE_UI_DIFERENCA,
        "status_conc": FRETE_UI_STATUS_CONC,
        "id_anuncio": FRETE_UI_ANUNCIO,
        "titulo_anuncio": FRETE_UI_TITULO_ANUNCIO,
        "qtd_x_preco_produto_ml": FRETE_UI_QTD_PRECO_ML,
        "_preco_frete_unit_arquivo": FRETE_UI_VALOR_FRETE_ANUNCIO,
    }
    v = v.rename(columns={a: b for a, b in rename_final.items() if a in v.columns})
    meta["linhas"] = int(len(v))
    _dbg(f"linhas_carregadas={meta['linhas']}")
    _dbg(f"colunas_saida={list(v.columns)}")
    return v, meta


def carregar_tabela_final_frete_operacional(
    org_id: str,
    vendas_path_str: str,
    vendas_mtime_ns: int,
    frete_path_str: str | None,
    frete_mtime_ns: int | None,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Dataset final de frete para o app e para `processing/materialize_financeiro` (mesmo pipeline que
    carregar_base_frete_ml, com validação de schema operacional).
    """
    df, meta = carregar_base_frete_ml(
        org_id, vendas_path_str, vendas_mtime_ns, frete_path_str, frete_mtime_ns
    )
    validate_frete_operacional_dataframe(df)
    return df, meta


def descobrir_fontes_frete(base_dir: Path | None = None) -> FontesFrete:
    """
    Fontes para construir a tabela operacional de frete (automático, como a tabela final do repasse).

    - Cloud: `FDL_FRETE_VENDAS_URL` e opcionalmente `FDL_FRETE_ANUNCIO_URL` nos Secrets.
    - Local: último ficheiro .xlsx/.xls/.csv em `Vendas - Mercado Livre` ou `Vendas_ML` sob a base
      do cliente e planilha «Frete por Anúncio» na raiz ou subpastas (ver `_find_frete_anuncio_path`).
    """
    vendas_url = _frete_secret_str("FDL_FRETE_VENDAS_URL", "FDL_FRETE_PRECOMPUTED_URL")
    frete_url = _frete_secret_str("FDL_FRETE_ANUNCIO_URL")
    root = base_dir or CLIENTE_BASE_DIR
    vendas_dir = resolve_pasta_vendas_ml(root)
    v_local = None if vendas_url else _latest_vendas_ml_path(vendas_dir)
    f_local = None if frete_url else _find_frete_anuncio_path(root)
    return FontesFrete(
        vendas_path=v_local,
        frete_path=f_local,
        vendas_url=vendas_url,
        frete_url=frete_url,
    )

