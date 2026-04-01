"""Leitura da planilha auxiliar de parâmetros (empresa + competência → alíquotas)."""
from __future__ import annotations

import re
from pathlib import Path

import pandas as pd

from .params import FaturamentoParamsError


def _norm_org_id(s: str) -> str:
    t = str(s).strip().lower()
    t = re.sub(r"[^a-z0-9_-]+", "", t.replace(" ", "_"))
    return t


def _norm_empresa_name(s: str) -> str:
    return str(s).strip().casefold()


def _parse_competencia(raw: object) -> str | None:
    """Devolve ``YYYY-MM`` ou None."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if hasattr(raw, "year") and hasattr(raw, "month"):
        try:
            return f"{int(raw.year):04d}-{int(raw.month):02d}"
        except (TypeError, ValueError):
            pass
    s = str(raw).strip()
    if not s or s.lower() in ("nan", "nat", "none"):
        return None
    m = re.match(r"^(\d{4})-(\d{2})$", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}"
    m = re.match(r"^(\d{1,2})/(\d{4})$", s)
    if m:
        mo, ye = int(m.group(1)), int(m.group(2))
        if 1 <= mo <= 12:
            return f"{ye:04d}-{mo:02d}"
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", s)
    if m:
        _d, mo, ye = int(m.group(1)), int(m.group(2)), int(m.group(3))
        if 1 <= mo <= 12:
            return f"{ye:04d}-{mo:02d}"
    ts = pd.to_datetime(s, errors="coerce", dayfirst=True)
    if pd.notna(ts):
        return f"{int(ts.year):04d}-{int(ts.month):02d}"
    return None


def _read_tabular(path: Path) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    last_err: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python")
            except Exception as e:  # noqa: BLE001
                last_err = e
    raise FaturamentoParamsError(f"Não foi possível ler params mensais: {path} ({last_err})")


def _header_map(columns: list[str]) -> dict[str, str]:
    """Normaliza cabeçalho → nome original."""
    out: dict[str, str] = {}
    for c in columns:
        k = re.sub(r"\s+", " ", str(c).strip().casefold())
        out[k] = c
    return out


def _col_by_aliases(hmap: dict[str, str], aliases: tuple[str, ...]) -> str | None:
    for a in aliases:
        k = re.sub(r"\s+", " ", a.strip().casefold())
        if k in hmap:
            return hmap[k]
    for hk, orig in hmap.items():
        hk_ns = hk.replace(" ", "").replace("_", "")
        for a in aliases:
            a_ns = a.casefold().replace(" ", "").replace("_", "")
            if hk_ns == a_ns or (a_ns and a_ns in hk_ns):
                return orig
    return None


def load_params_mensais_dataframe(path: Path) -> pd.DataFrame:
    """
    Colunas esperadas (flexível):

    * ``org_id`` **ou** ``empresa`` (pelo menos uma)
    * ``competencia`` (YYYY-MM, MM/YYYY, data)
    * ``aliquota_imposto``
    * ``despesa_fixa`` ou ``aliquota_despesas_fixas``
    """
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FaturamentoParamsError(f"Planilha de parâmetros mensais não encontrada: {path}")
    df = _read_tabular(path).dropna(axis=1, how="all")
    if df.empty:
        raise FaturamentoParamsError(f"Planilha de parâmetros mensais vazia: {path}")

    hm = _header_map(list(df.columns))
    c_org = _col_by_aliases(hm, ("org_id", "org id"))
    c_emp = _col_by_aliases(hm, ("empresa",))
    if not c_org and not c_emp:
        raise FaturamentoParamsError(
            "params mensais: coluna 'org_id' ou 'empresa' é obrigatória "
            f"(colunas lidas: {list(df.columns)})."
        )
    c_comp = _col_by_aliases(
        hm,
        ("competencia", "competência", "mes", "mês", "periodo", "período"),
    )
    if not c_comp:
        raise FaturamentoParamsError("params mensais: coluna 'competencia' é obrigatória.")
    c_aliq = _col_by_aliases(
        hm,
        ("aliquota_imposto", "alíquota imposto", "aliquota imposto"),
    )
    c_desp = _col_by_aliases(
        hm,
        ("despesa_fixa", "despesa fixa", "aliquota_despesas_fixas", "alíquota despesas fixas"),
    )
    if not c_aliq:
        raise FaturamentoParamsError("params mensais: coluna 'aliquota_imposto' é obrigatória.")
    if not c_desp:
        raise FaturamentoParamsError("params mensais: coluna 'despesa_fixa' é obrigatória.")

    comp = df[c_comp].map(_parse_competencia)
    if comp.isna().any():
        raise FaturamentoParamsError("params mensais: existe competência inválida ou vazia.")

    org_key = (
        df[c_org].map(lambda x: _norm_org_id(x) if c_org and str(x).strip() else "")
        if c_org
        else pd.Series([""] * len(df), index=df.index)
    )
    empresa_key = (
        df[c_emp].map(lambda x: _norm_empresa_name(x) if c_emp and str(x).strip() else "")
        if c_emp
        else pd.Series([""] * len(df), index=df.index)
    )

    out = pd.DataFrame(
        {
            "org_key": org_key,
            "empresa_key": empresa_key,
            "competencia": comp.astype(str),
            "aliquota_imposto": pd.to_numeric(df[c_aliq], errors="coerce"),
            "despesa_fixa": pd.to_numeric(df[c_desp], errors="coerce"),
        }
    )

    if out["aliquota_imposto"].isna().any() or out["despesa_fixa"].isna().any():
        raise FaturamentoParamsError("params mensais: valores numéricos inválidos.")
    if (out["aliquota_imposto"] < 0).any() or (out["aliquota_imposto"] > 1).any():
        raise FaturamentoParamsError("params mensais: aliquota_imposto deve estar entre 0 e 1.")
    if (out["despesa_fixa"] < 0).any() or (out["despesa_fixa"] > 1).any():
        raise FaturamentoParamsError("params mensais: despesa_fixa deve estar entre 0 e 1.")

    # Duplicados: (org_key, competencia) quando org_key preenchido; senão (empresa_key, competencia)
    for comp_u in out["competencia"].unique():
        sub = out[out["competencia"].eq(comp_u)]
        sk = sub["org_key"].ne("")
        if sk.any():
            if sub.loc[sk].duplicated(subset=["org_key"], keep=False).any():
                raise FaturamentoParamsError(
                    f"params mensais: org_id duplicado para competencia={comp_u!r}."
                )
        else:
            if sub.duplicated(subset=["empresa_key"], keep=False).any():
                raise FaturamentoParamsError(
                    f"params mensais: empresa duplicada para competencia={comp_u!r}."
                )

    return out


def lookup_parametros_mensais(
    org_id: str,
    empresa: str,
    competencia: str,
    df_params: pd.DataFrame,
) -> tuple[float, float]:
    """Devolve (aliquota_imposto, despesa_fixa) para competência ``YYYY-MM``."""
    oid = _norm_org_id(org_id)
    ename = _norm_empresa_name(empresa)
    sub = df_params[df_params["competencia"].astype(str).eq(competencia)]
    if sub.empty:
        raise FaturamentoParamsError(
            f"params mensais: sem linhas para competencia={competencia!r}."
        )
    if oid and sub["org_key"].ne("").any():
        row = sub[sub["org_key"].eq(oid)]
        if len(row) == 1:
            return float(row["aliquota_imposto"].iloc[0]), float(row["despesa_fixa"].iloc[0])
    row = sub[sub["empresa_key"].eq(ename)]
    if len(row) == 1:
        return float(row["aliquota_imposto"].iloc[0]), float(row["despesa_fixa"].iloc[0])
    if oid:
        row = sub[sub["org_key"].eq(oid)]
        if len(row) == 1:
            return float(row["aliquota_imposto"].iloc[0]), float(row["despesa_fixa"].iloc[0])
    raise FaturamentoParamsError(
        f"params mensais: sem linha para org_id={org_id!r}, empresa={empresa!r}, competencia={competencia!r}."
    )
