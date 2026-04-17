"""Normalização de SKU e valores numéricos (BR / misto com ponto decimal)."""
from __future__ import annotations

import re

import numpy as np
import pandas as pd


# Sufixo colado ``01``–``09`` só para códigos numéricos longos (evita ``031601`` → ``0316``).
_MIN_NUMERIC_LEN_FOR_GLUED_0X_SUFFIX = 7


def _strip_sku_variant_suffixes_join(s: str) -> str:
    """
    Remove sufixos de variante antes do join pedidos ↔ custo.

    * ``-N``, ``_N``, ``.N`` no fim (N dígitos), ex.: ``170555-1``, ``170555_2``, ``170555.3``.
    * Só em cadeias **estritamente numéricas** (opcional ``-`` inicial): par ``01``–``09`` colado no fim,
      repetido enquanto couber, **apenas** se o corpo tiver pelo menos
      ``_MIN_NUMERIC_LEN_FOR_GLUED_0X_SUFFIX`` dígitos (ex.: ``17055501`` → ``170555``).
      Códigos mais curtos (ex.: ``031601``) não são alterados por esta regra — seguem só o lstrip de zeros
      canónico. Não aplica a códigos alfanuméricos (ex.: ``KIT05``, ``BELA4P1``).
    """
    out = s
    while out:
        prev = out
        out = re.sub(r"[-_.]\d+$", "", out)
        if out != prev:
            continue
        m = re.fullmatch(r"(-?)(\d+)", out)
        if not m:
            break
        neg, body = m.group(1), m.group(2)
        if (
            len(body) >= _MIN_NUMERIC_LEN_FOR_GLUED_0X_SUFFIX
            and len(body) >= 3
            and body[-2] == "0"
            and body[-1] in "123456789"
        ):
            body = body[:-2]
            out = f"{neg}{body}"
            continue
        break
    return out


def normalize_sku_join_key_scalar(raw: object) -> str:
    """
    Chave canónica para join pedidos ↔ custo e auditoria.

    1. texto; 2. trim; 3. remover sufixo ``.0`` típico de export Excel/float;
    4. remover sufixos de variante (``-1``, ``_2``, ``.3``; em códigos só numéricos, ``01``–``09`` colados
       só se o corpo tiver ≥ 7 dígitos, p.ex. ``17055501`` → ``170555``);
    5. remover zeros à esquerda em cadeias só numéricas (``03160`` → ``3160``);
    6. identificadores alfanuméricos: ``casefold()`` (ex.: ``BELA4P1`` vs ``Bela4P1``).
    """
    if raw is None:
        return ""
    try:
        if isinstance(raw, float) and np.isnan(raw):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(raw, str) and raw.strip().lower() in ("nan", "none", "nat", "<na>"):
        return ""
    s = str(raw).strip()
    if not s:
        return ""
    if s.lower() in ("nan", "none", "nat", "<na>"):
        return ""
    # Excel: "3160.0", "03160.0"
    if re.fullmatch(r"-?\d+\.0", s):
        s = s[:-2]
    if not s:
        return ""
    s = _strip_sku_variant_suffixes_join(s)
    if not s:
        return ""
    # Apenas dígitos (com sinal opcional): zeros à esquerda
    if re.fullmatch(r"-?\d+", s):
        neg = s.startswith("-")
        body = s[1:] if neg else s
        body = body.lstrip("0") or "0"
        return f"-{body}" if neg else body
    # Case-insensitive: planilha de custo costuma vir em MAIÚSCULAS e pedidos em TitleCase (ex.: BELA4P1 vs Bela4P1).
    return s.casefold()


def normalize_sku_key(series: pd.Series) -> pd.Series:
    """Série de chaves SKU para join e flags (mesma regra que :func:`normalize_sku_join_key_scalar`)."""
    return series.map(normalize_sku_join_key_scalar)


def normalize_pedido_join_key_scalar(raw: object) -> str:
    """
    Chave canónica para vínculo pedidos ↔ notas (número do pedido / multiloja).

    Trim, remove sufixo ``.0`` de float Excel, preserva letras e dígitos (ex. ``MLB123``).
    """
    if raw is None:
        return ""
    try:
        if isinstance(raw, float) and np.isnan(raw):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat", "<na>"):
        return ""
    if re.fullmatch(r"-?\d+\.0", s):
        s = s[:-2]
    return s.strip()


def normalize_pedido_join_key(series: pd.Series) -> pd.Series:
    return series.map(normalize_pedido_join_key_scalar)


def normalize_nf_fiscal_commercial_join_key_scalar(raw: object) -> str:
    """
    Chave para merge ``dataset_faturamento_fiscal`` ↔ grão comercial (painel NF-first).

    Reutiliza ``normalize_pedido_join_key_scalar`` (trim, sufixo ``.0``). Prefixo ``NF`` opcional
    (com ou sem ``-`` / espaço) seguido só de dígitos → normaliza como número (zeros à esquerda),
    alinhando «NF042517» a «042517» / «42517». Cadeia **só dígitos**: remove zeros à esquerda.
    """
    s = normalize_pedido_join_key_scalar(raw)
    if not s:
        return ""
    compact = re.sub(r"\s+", "", s)
    m = re.fullmatch(r"(?i)NF[\-.]?(\d+)", compact)
    if m:
        digits = m.group(1)
        return digits.lstrip("0") or "0"
    if re.fullmatch(r"\d+", compact):
        return compact.lstrip("0") or "0"
    return s


def normalize_nf_fiscal_commercial_join_key(series: pd.Series) -> pd.Series:
    return series.map(normalize_nf_fiscal_commercial_join_key_scalar)


def normalize_empresa_fiscal_commercial_join_key_scalar(raw: object) -> str:
    """Chave estável para merge fiscal ↔ comercial (evita falha Esquilo ≠ ESQUILO)."""
    xs = str(raw).strip() if raw is not None else ""
    if not xs or xs.casefold() in {"nan", "none", "nat", "<na>"}:
        return ""
    return xs.casefold()


def normalize_empresa_fiscal_commercial_join_key(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).map(normalize_empresa_fiscal_commercial_join_key_scalar)


def _parse_number_scalar(raw: object) -> float:
    """Interpreta valores com vírgula BR (1.234,56) ou ponto decimal (79.95)."""
    if raw is None:
        return float("nan")
    # Valores já numéricos: não passar por str() — «51.001167» em texto dispara remoção de '.' quando
    # há >2 casas decimais (ver ramo abaixo), corrompendo comissão/frete após rateio ML (coluna object mista).
    if isinstance(raw, bool):
        pass
    elif isinstance(raw, (int, np.integer)):
        return float(raw)
    elif isinstance(raw, (float, np.floating)):
        try:
            x = float(raw)
        except (TypeError, ValueError):
            return float("nan")
        if np.isnan(x):
            return float("nan")
        return x
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "none", "nat"):
        return float("nan")
    s = s.replace("\u00a0", " ").replace(" ", "").strip()
    neg = s.startswith("-")
    if neg:
        s = s[1:].strip()
    s = re.sub(r"[^\d,\.\-]", "", s)
    if not s or s in (".", ",", "-"):
        return float("nan")
    last_c = s.rfind(",")
    last_d = s.rfind(".")
    if last_c != -1 and last_d != -1:
        if last_c > last_d:
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif last_c != -1:
        s = s.replace(".", "").replace(",", ".")
    else:
        nd = s.count(".")
        if nd == 1:
            i = s.index(".")
            tail = s[i + 1 :]
            if len(tail) <= 2 and tail.isdigit():
                pass
            else:
                s = s.replace(".", "")
        elif nd > 1:
            s = s.replace(".", "")
    try:
        v = float(s)
    except ValueError:
        return float("nan")
    return -v if neg else v


def to_numeric_br(series: pd.Series) -> pd.Series:
    if series.dtype == object or str(series.dtype).startswith("string"):
        return series.map(_parse_number_scalar)
    return pd.to_numeric(series, errors="coerce")
