"""
Conciliação de Frete — vendas Mercado Livre + arquivo opcional «frete por anúncio» tabular.

Frete líquido ML (detalhe envios): soma algébrica Receita por envio + Tarifas de envio.
"""
from __future__ import annotations

import re
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd
import streamlit as st

from etapa1_vendas import list_sales_files, read_sales_file
from fdl_paths import CLIENTE_BASE_DIR


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

    an = next((c for c in cols if "#" in str(c) and "an" in _strip_ascii_lower(str(c))), None)
    if an is None:
        an = _pick_col(cols, "de", "anuncio")
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


def _latest_vendas_ml_path(folder: Path) -> Path | None:
    if not folder.is_dir():
        return None
    try:
        files = [p for p in list_sales_files(folder) if p.suffix.lower() in {".xlsx", ".xls", ".csv"}]
    except OSError:
        return None
    if not files:
        return None
    try:
        return max(files, key=lambda p: p.stat().st_mtime)
    except OSError:
        return None


def _find_frete_anuncio_path(base: Path) -> Path | None:
    if not base.is_dir():
        return None
    cands: list[Path] = []
    for pat in ("*Frete*Anuncio*.xlsx", "*frete*anuncio*.xlsx", "*Frete*Anúncio*.xlsx"):
        cands.extend(base.glob(pat))
    seen: set[str] = set()
    uniq: list[Path] = []
    for p in cands:
        if not p.is_file():
            continue
        r = str(p.resolve())
        if r not in seen:
            seen.add(r)
            uniq.append(p)
    if not uniq:
        return None
    return max(uniq, key=lambda p: p.stat().st_mtime)


def _try_read_tabular_frete_anuncio(path: Path) -> pd.DataFrame | None:
    try:
        raw = pd.read_excel(path, header=None, nrows=35, engine="openpyxl")
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
        df = pd.read_excel(path, header=int(best_i), engine="openpyxl")
    except Exception:
        return None
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
    num_c = None
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

    out = pd.DataFrame(
        {
            "_id_anuncio_norm": df[id_c]
            .astype(str)
            .str.replace(r"\s+", "", regex=True)
            .str.upper()
            .str.replace(r"^#", "", regex=True),
            "_preco_frete_unit_arquivo": pd.to_numeric(df[num_c], errors="coerce"),
        }
    )
    out = out.loc[out["_id_anuncio_norm"].str.len() > 3].drop_duplicates("_id_anuncio_norm", keep="last")
    if int(out["_preco_frete_unit_arquivo"].notna().sum()) < 1:
        return None
    return out


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
        "vendas_arquivo": Path(vendas_path_str).name,
        "frete_arquivo": Path(frete_path_str).name if frete_path_str else None,
        "frete_tabular": False,
        "avisos": [],
    }

    df = read_sales_file(Path(vendas_path_str))
    df = df.dropna(axis=1, how="all")
    cmap = _resolve_columns(df)
    needed = ("n_venda", "receita_envio", "tarifas_envio", "unidades", "id_anuncio")
    miss = [k for k in needed if k not in cmap]
    if miss:
        raise ValueError(
            "Relatório de vendas ML sem colunas necessárias. "
            f"Falta: {miss}. Primeiras colunas: {list(df.columns)[:20]}"
        )

    v = df.rename(columns={cmap[k]: k for k in cmap})

    re = pd.to_numeric(v["receita_envio"], errors="coerce")
    ta = pd.to_numeric(v["tarifas_envio"], errors="coerce")
    both_na = re.isna() & ta.isna()
    frete_ml = (re.fillna(0.0) + ta.fillna(0.0)).mask(both_na, np.nan)
    v = v.copy()
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
        v["_data_venda_dt"] = pd.to_datetime(v["data_venda"], errors="coerce", dayfirst=True)

    if frete_path_str and Path(frete_path_str).is_file():
        tab = _try_read_tabular_frete_anuncio(Path(frete_path_str))
        if tab is None or tab.empty:
            meta["avisos"].append(
                "Arquivo de frete por anúncio não reconhecido como tabela (p.ex. export de ecrã). "
                "Divergências por anúncio ficam indisponíveis até existir Excel/CSV com colunas MLB + preço."
            )
        else:
            meta["frete_tabular"] = True
            m = v.merge(tab, left_on="_id_norm", right_on="_id_anuncio_norm", how="left")
            m["frete_esperado"] = m["_qtd"] * m["_preco_frete_unit_arquivo"]
            m["diferenca"] = m["frete_ml"] - m["frete_esperado"]
            re2 = pd.to_numeric(m["receita_envio"], errors="coerce")
            ta2 = pd.to_numeric(m["tarifas_envio"], errors="coerce")
            both_na2 = re2.isna() & ta2.isna()
            tol = 0.02
            st_ok = np.where(
                m["frete_esperado"].notna() & m["frete_ml"].notna(),
                np.where(m["diferenca"].abs() <= tol, "OK", "Divergência"),
                np.where(both_na2, "Sem dados envio no ML", "Sem preço arquivo"),
            )
            m["status_conc"] = st_ok
            v = m.drop(columns=["_id_anuncio_norm"], errors="ignore")

    drop_h = [c for c in ("_id_norm", "_qtd", "_preco_frete_unit_arquivo") if c in v.columns]
    if drop_h:
        v = v.drop(columns=drop_h)

    rename_final = {
        "n_venda": "N.º venda",
        "estado": "Estado",
        "descricao_status": "Descrição do status",
        "unidades": "Unidades",
        "receita_envio": "Receita por envio (BRL)",
        "tarifas_envio": "Tarifas de envio (BRL)",
        "frete_ml": "Frete ML (receita+tarifa)",
        "frete_esperado": "Frete esperado (qtd × preço arquivo)",
        "diferenca": "Diferença",
        "status_conc": "Status conciliação",
        "id_anuncio": "# de anúncio",
        "titulo_anuncio": "Título do anúncio",
        "qtd_x_preco_produto_ml": "Qtd × preço unit. produto (ML)",
    }
    v = v.rename(columns={a: b for a, b in rename_final.items() if a in v.columns})
    meta["linhas"] = int(len(v))
    return v, meta


def descobrir_fontes_frete(base_dir: Path | None = None) -> tuple[Path | None, Path | None]:
    root = base_dir or CLIENTE_BASE_DIR
    vendas_dir = root / "Vendas - Mercado Livre"
    return _latest_vendas_ml_path(vendas_dir), _find_frete_anuncio_path(root)

