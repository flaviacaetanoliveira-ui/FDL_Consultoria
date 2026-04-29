"""Carga de CSV/XLSX de notas de entrada (Bling) — devoluções de venda."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from .fiscal_devolucoes_constants import (
    COL_TIPO_ABATIMENTO,
    NATUREZAS_DEVOLUCAO,
    SITUACOES_DEVOLUCAO_VALIDAS,
    TIPO_ABATIMENTO_DEVOLUCAO_VENDA,
)
from .io_notas_saida import (
    _read_notas_file,
    detectar_col_data_emissao,
    detectar_col_valor_total_liquido,
)
from .normalize import to_numeric_br


def _norm_txt(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _detect_col_natureza(columns: list[str]) -> str:
    for c in columns:
        if str(c).strip().casefold() == "natureza":
            return c
    for c in columns:
        cl = str(c).strip().lower()
        if "natur" in cl:
            return c
    return ""


def _detect_col_numero_nf(columns: list[str]) -> str:
    if "Número" in columns:
        return "Número"
    for c in columns:
        cl = str(c).strip().lower()
        if cl in {"numero", "número", "nr nota", "nr_nota"} and "pedido" not in cl:
            return c
    return ""


def _detect_col_situacao(columns: list[str]) -> str:
    for c in columns:
        n = str(c).lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            return c
    return ""


def _detect_col_cpf_cnpj(columns: list[str]) -> str:
    """Coluna de documento do destinatário no export Bling (ex.: «CNPJ/CPF»)."""
    for c in columns:
        if str(c).strip() == "CNPJ/CPF":
            return c
    for c in columns:
        cl = str(c).strip().lower().replace(" ", "")
        if cl in {"cpf/cnpj", "cnpj/cpf"}:
            return c
        if "cnpj" in cl and "cpf" in cl:
            return c
    for c in columns:
        cl = str(c).strip().lower()
        if ("cnpj" in cl or "cpf" in cl) and "emitente" not in cl:
            return c
    return ""


def _detect_col_nome_destinatario(columns: list[str]) -> str:
    """Nome do destinatário no bruto (ex.: «Nome» no export analisado)."""
    for prefer in ("Nome", "Nome / Razão Social"):
        if prefer in columns:
            return prefer
    for c in columns:
        if str(c).strip().casefold() == "nome":
            return c
    return ""


def _df_col_as_series(df: pd.DataFrame, col: str) -> pd.Series:
    if col not in df.columns:
        raise KeyError(col)
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        return obj.iloc[:, 0]
    return obj


def _safe_numeric(df: pd.DataFrame, col: str) -> pd.Series:
    if not col or col not in df.columns:
        return pd.Series(0.0, index=df.index, dtype="float64")
    return pd.to_numeric(to_numeric_br(_df_col_as_series(df, col)), errors="coerce").fillna(0.0).astype("float64")


def _norm_head(c: str) -> str:
    return str(c).strip().casefold()


def _detect_col_frete(columns: list[str]) -> str:
    for c in columns:
        if _norm_head(c) == "frete":
            return c
    return ""


def _detect_col_outras_despesas(columns: list[str]) -> str:
    for c in columns:
        if _norm_head(c) == "outras despesas":
            return c
    for c in columns:
        n = _norm_head(c)
        if "outras" in n and "despes" in n:
            return c
    return ""


def _detect_col_desconto_nota(columns: list[str]) -> str:
    for c in columns:
        n = _norm_head(c)
        if n == "desconto":
            return c
    for c in columns:
        n = _norm_head(c)
        if "desconto" in n and "frete" not in n and "proporcional" not in n:
            return c
    return ""


def series_valor_liquido_nota_entrada_bling(df: pd.DataFrame) -> pd.Series:
    """
    Valor por linha da NF de entrada alinhado ao total fiscal da nota no export Bling.

    Quando existem colunas auxiliares típicas (**Frete**, **Outras despesas**, **Desconto**),
    aplica::

        Valor total + Frete + Outras despesas − Desconto

    onde **Valor total** é a coluna homónima do CSV (subtotal de produtos), não o «Valor total líquido».

    Se nenhuma coluna auxiliar existir, mantém o comportamento anterior: «Valor total líquido» se
    existir, senão «Valor total».
    """
    if df.empty:
        return pd.Series(dtype="float64")
    cols = list(df.columns)
    col_frete = _detect_col_frete(cols)
    col_outras = _detect_col_outras_despesas(cols)
    col_desc = _detect_col_desconto_nota(cols)
    tem_aux = bool(col_frete or col_outras or col_desc)

    col_vl_liq = detectar_col_valor_total_liquido(cols)
    col_valor_total_exato = "Valor total" if "Valor total" in df.columns else ""

    if tem_aux:
        if col_valor_total_exato:
            base = _safe_numeric(df, col_valor_total_exato)
        elif col_vl_liq:
            base = _safe_numeric(df, col_vl_liq)
        else:
            base = pd.Series(0.0, index=df.index, dtype="float64")
        frete_v = _safe_numeric(df, col_frete) if col_frete else pd.Series(0.0, index=df.index)
        outras_v = _safe_numeric(df, col_outras) if col_outras else pd.Series(0.0, index=df.index)
        desc_v = _safe_numeric(df, col_desc) if col_desc else pd.Series(0.0, index=df.index)
        return base + frete_v + outras_v - desc_v

    if col_vl_liq:
        return _safe_numeric(df, col_vl_liq)
    if col_valor_total_exato:
        return _safe_numeric(df, col_valor_total_exato)
    return pd.Series(0.0, index=df.index, dtype="float64")


def normalizar_cpf_cnpj_somente_digitos(v: object) -> str:
    """Remove pontuação; mantém apenas dígitos (armazenamento como texto)."""
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except TypeError:
        pass
    s = str(v).strip()
    return "".join(ch for ch in s if ch.isdigit())


def aplicar_filtros_devolucao(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém apenas linhas cuja Natureza está em ``NATUREZAS_DEVOLUCAO``
    e cuja Situação está em ``SITUACOES_DEVOLUCAO_VALIDAS``.

    Usado por ``load_notas_entrada_devolucoes_from_dir`` e por testes de regressão.
    """
    if df.empty:
        return df.copy()
    cols = list(df.columns)
    col_nat = _detect_col_natureza(cols)
    col_sit = _detect_col_situacao(cols)
    if not col_nat or not col_sit:
        return pd.DataFrame()

    nat_ok = frozenset(NATUREZAS_DEVOLUCAO)
    sit_ok = frozenset(SITUACOES_DEVOLUCAO_VALIDAS)
    n_raw = _norm_txt(df[col_nat])
    s_raw = _norm_txt(df[col_sit])
    return df.loc[n_raw.isin(nat_ok) & s_raw.isin(sit_ok)].copy().reset_index(drop=True)


def load_notas_entrada_brutas_from_dir(notas_dir: Path) -> pd.DataFrame:
    """Lê e concatena CSV/XLSX de notas de entrada sem filtro fiscal."""
    notas_dir = notas_dir.expanduser().resolve()
    if not notas_dir.is_dir():
        return pd.DataFrame()

    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in notas_dir.rglob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    partes: list[pd.DataFrame] = []
    for f in files:
        df = _read_notas_file(f).dropna(axis=1, how="all").copy()
        df["__arquivo_nota__"] = f.name
        partes.append(df)
    if not partes:
        return pd.DataFrame()
    return pd.concat(partes, ignore_index=True)


def load_notas_entrada_devolucoes_from_dir(notas_dir: Path) -> pd.DataFrame:
    """
    Lê todos os CSV/XLSX sob ``notas_dir``, mantém apenas linhas com Natureza de devolução
    e situação autorizada; deduplica e marca ``_tipo_abatimento``.
    """
    out = load_notas_entrada_brutas_from_dir(notas_dir)
    if out.empty:
        return pd.DataFrame()
    out = aplicar_filtros_devolucao(out)
    if out.empty:
        return pd.DataFrame()

    col_nat = _detect_col_natureza(list(out.columns))
    col_nf = _detect_col_numero_nf(list(out.columns))
    col_dt = detectar_col_data_emissao(list(out.columns))
    col_vl = detectar_col_valor_total_liquido(list(out.columns))
    if not col_vl and "Valor total" in out.columns:
        col_vl = "Valor total"

    dedup_keys = ["__arquivo_nota__", col_nat]
    if col_nf:
        dedup_keys.append(col_nf)
    if col_dt:
        dedup_keys.append(col_dt)
    if col_vl:
        dedup_keys.append(col_vl)
    dedup_keys = [k for k in dedup_keys if k in out.columns]
    if dedup_keys:
        out = out.drop_duplicates(subset=dedup_keys, keep="first")

    out[COL_TIPO_ABATIMENTO] = TIPO_ABATIMENTO_DEVOLUCAO_VENDA
    return out.reset_index(drop=True)
