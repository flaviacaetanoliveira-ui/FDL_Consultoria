"""Escolha da coluna de preço de custo na planilha conforme a empresa (SKU continua em ``Código``)."""
from __future__ import annotations

import unicodedata
from typing import Any

from .config import (
    CUSTO_COL_PRECO,
    CUSTO_COL_VALOR_EAP,
    CUSTO_COL_VALOR_GENERIC,
    CUSTO_COL_VALOR_MEGA,
    CUSTO_COL_VALOR_STAR_GAMA,
    CUSTO_SKU_COL,
    CUSTO_UNITARIO_COL,
    SKU_NORMALIZADO_COL,
)
from .normalize import normalize_sku_key, to_numeric_br


def _empresa_token(empresa: str) -> str:
    t = unicodedata.normalize("NFKD", str(empresa).strip()).encode("ascii", "ignore").decode().casefold()
    return " ".join(t.split())


def resolve_custo_coluna_preco_nome(df_custo: pd.DataFrame, empresa: str | None) -> str:
    """
    Nome da coluna em ``df_custo`` a usar como preço unitário de custo.

    * **Mega Fácil** → ``VALOR DE COMPRA MEGA``
    * **Móveis EAP** → ``VALOR COMPRA EAP``
    * **Mega Star** / **Gama Home** → ``VALOR COMPRA STAR/GAMA``
    * Sem empresa (V1 / desconhecido): legado ``PREÇO DE CUSTO com IPI`` ou genérico ``VALOR DE COMPRA``,
      depois qualquer coluna por empresa existente.
    """
    cols = set(df_custo.columns.astype(str))

    def _has(name: str) -> bool:
        return name in cols

    et = _empresa_token(empresa) if empresa else ""

    if et:
        if "mega star" in et:
            if _has(CUSTO_COL_VALOR_STAR_GAMA):
                return CUSTO_COL_VALOR_STAR_GAMA
        elif "gama home" in et or et == "gama":
            if _has(CUSTO_COL_VALOR_STAR_GAMA):
                return CUSTO_COL_VALOR_STAR_GAMA
        elif "mega facil" in et:
            if _has(CUSTO_COL_VALOR_MEGA):
                return CUSTO_COL_VALOR_MEGA
        elif "eap" in et or "moveis" in et:
            if _has(CUSTO_COL_VALOR_EAP):
                return CUSTO_COL_VALOR_EAP

    if _has(CUSTO_COL_PRECO):
        return CUSTO_COL_PRECO
    if _has(CUSTO_COL_VALOR_GENERIC):
        return CUSTO_COL_VALOR_GENERIC
    for c in (CUSTO_COL_VALOR_MEGA, CUSTO_COL_VALOR_EAP, CUSTO_COL_VALOR_STAR_GAMA):
        if _has(c):
            return c
    raise KeyError(
        "Nenhuma coluna de preço de custo reconhecida na tabela de custos "
        f"(esperado uma de: {CUSTO_COL_PRECO!r}, {CUSTO_COL_VALOR_GENERIC!r}, "
        f"{CUSTO_COL_VALOR_MEGA!r}, {CUSTO_COL_VALOR_EAP!r}, {CUSTO_COL_VALOR_STAR_GAMA!r}). "
        f"Colunas atuais: {sorted(cols)!r}"
    )


def serie_custo_unitario_resolvida(df_custo: pd.DataFrame, empresa: str | None) -> tuple[pd.Series, dict[str, Any]]:
    """
    Série numérica de preço unitário por linha da tabela de custo, com fallback explícito.

    Fallback quando o valor da coluna da empresa é ausente (NaN): ``VALOR DE COMPRA`` → ``PREÇO DE CUSTO com IPI``.
    """
    primary = resolve_custo_coluna_preco_nome(df_custo, empresa)
    s = to_numeric_br(df_custo[primary])
    meta: dict[str, Any] = {"custo_coluna_preco": primary}

    cols = set(df_custo.columns.astype(str))
    fallbacks: list[str] = []
    if primary != CUSTO_COL_VALOR_GENERIC and CUSTO_COL_VALOR_GENERIC in cols:
        fallbacks.append(CUSTO_COL_VALOR_GENERIC)
    if primary != CUSTO_COL_PRECO and CUSTO_COL_PRECO in cols:
        fallbacks.append(CUSTO_COL_PRECO)

    for fb in fallbacks:
        s_fb = to_numeric_br(df_custo[fb])
        s = s.where(s.notna(), s_fb)

    if fallbacks:
        meta["custo_coluna_fallback_cascade"] = fallbacks

    return s, meta


def build_custo_unitario_map_por_empresa(df_custo: pd.DataFrame, empresa: str | None) -> tuple[pd.Series, dict[str, Any]]:
    """Mapa SKU normalizado → custo unitário (float), usando a coluna correta para ``empresa``."""
    s_val, meta = serie_custo_unitario_resolvida(df_custo, empresa)
    c = df_custo[[CUSTO_SKU_COL]].copy()
    c["_sku_join"] = normalize_sku_key(c[CUSTO_SKU_COL])
    c = c.drop_duplicates(subset=["_sku_join"], keep="first")
    c["_v"] = s_val.reindex(c.index).to_numpy()
    out = c.set_index("_sku_join")["_v"]
    return out, meta


def _resolve_sku_join_key_com_fallback_f(
    sku_key: str, *, keys_present: frozenset[str]
) -> str:
    """
    Se a planilha tiver código ``F`` + só dígitos (ex.: ``F6513`` → chave ``f6513``) e o pedido vier só
    numérico (``6513``), usa a chave com prefixo ``f`` para o mapa de custo.
    """
    k = str(sku_key).strip()
    if not k:
        return k
    if k in keys_present:
        return k
    if k.isdigit() and ("f" + k) in keys_present:
        return "f" + k
    return k


def join_custo_produto_por_empresa(df_pedidos: pd.DataFrame, df_custo: pd.DataFrame, empresa: str | None) -> pd.DataFrame:
    """Merge pedidos ↔ custo por ``Código`` normalizado, coluna de preço conforme ``empresa``."""
    p = df_pedidos.copy()
    cu_map, _meta = build_custo_unitario_map_por_empresa(df_custo, empresa)
    keys_present = frozenset(str(x) for x in cu_map.index.astype(str) if str(x).strip())
    raw_join = normalize_sku_key(p[CUSTO_SKU_COL]).astype(str)
    resolved = raw_join.map(lambda j: _resolve_sku_join_key_com_fallback_f(j, keys_present=keys_present))
    p["_sku_join"] = resolved
    p[SKU_NORMALIZADO_COL] = resolved
    p[CUSTO_UNITARIO_COL] = resolved.map(cu_map)
    p = p.drop(columns=["_sku_join"])
    return p
