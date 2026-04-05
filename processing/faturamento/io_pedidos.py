"""Leitura do CSV de pedidos mais recente no diretório."""
from __future__ import annotations

import csv
from pathlib import Path
from typing import Any

import pandas as pd


def _read_csv_flexible(path: Path) -> pd.DataFrame:
    last_err: Exception | None = None
    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t", "|"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", dtype=str)
            except Exception as e:  # noqa: BLE001
                last_err = e
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                sep=";",
                engine="python",
                dtype=str,
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
            )
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"Falha ao ler pedidos CSV: {path} ({last_err})")


def load_latest_pedidos_csv(pedidos_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    pedidos_dir = pedidos_dir.expanduser().resolve()
    if not pedidos_dir.is_dir():
        raise FileNotFoundError(f"Diretório de pedidos não encontrado: {pedidos_dir}")
    files = sorted(pedidos_dir.glob("*.csv"), key=lambda p: p.stat().st_mtime_ns, reverse=True)
    if not files:
        raise FileNotFoundError(f"Nenhum *.csv em {pedidos_dir}")
    latest = files[0]
    df = _read_csv_flexible(latest).dropna(axis=1, how="all")
    meta = {
        "arquivo": latest.name,
        "path": str(latest.resolve()),
        "mtime_iso": pd.Timestamp.fromtimestamp(latest.stat().st_mtime, tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    return df, meta


def load_all_pedidos_csv_concatenated(pedidos_dir: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Lê **todos** os ``*.csv`` sob o diretório (incluindo subpastas), ordenados por caminho estável.

    Usado no faturamento **schema_version 2** para não perder meses quando existem vários exports
    (ex.: Jan, Fev, Mar na mesma pasta **ou** em subpastas por mês/ano) — ``load_latest_pedidos_csv``
    só devolvia um ficheiro por mtime. Cada linha fica com ``pedidos_arquivo`` = nome do ficheiro de origem.
    """
    pedidos_dir = pedidos_dir.expanduser().resolve()
    if not pedidos_dir.is_dir():
        raise FileNotFoundError(f"Diretório de pedidos não encontrado: {pedidos_dir}")
    # rglob: alinhado a ``io_notas_saida`` e exports ML com um CSV por pasta (ex.: 2026/01/, 2026/02/).
    files = sorted(
        (p for p in pedidos_dir.rglob("*.csv") if p.is_file()),
        key=lambda p: str(p.relative_to(pedidos_dir)).casefold(),
    )
    if not files:
        raise FileNotFoundError(f"Nenhum *.csv em {pedidos_dir}")
    frames: list[pd.DataFrame] = []
    detalhe: list[dict[str, Any]] = []
    for path in files:
        chunk = _read_csv_flexible(path).dropna(axis=1, how="all").copy()
        chunk["pedidos_arquivo"] = path.name
        frames.append(chunk)
        detalhe.append(
            {
                "arquivo": path.name,
                "path": str(path.resolve()),
                "mtime_iso": pd.Timestamp.fromtimestamp(path.stat().st_mtime, tz="UTC").strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                ),
            }
        )
    merged = pd.concat(frames, ignore_index=True)
    meta: dict[str, Any] = {
        "arquivos": [d["arquivo"] for d in detalhe],
        "ficheiros_detalhe": detalhe,
        "arquivo": files[0].name if len(files) == 1 else f"{len(files)} ficheiros CSV",
        "path": str(pedidos_dir),
        "mtime_iso": detalhe[-1]["mtime_iso"] if detalhe else "",
    }
    return merged, meta


def dedupe_pedidos_multiloja_codigo(
    df: pd.DataFrame,
    *,
    col_multiloja: str = "Número do pedido multiloja",
    col_codigo: str = "Código",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Remove duplicados da mesma linha lógica de pedido após concat de vários CSV (ex.: Dez + Jan).

    - Se existir **linha com ``Data`` em dezembro/2025** no grupo, mantém a de **Data mais antiga**
      entre essas (valor/comissões do pedido em dezembro, alinhado a NFs emitidas no início de jan).
    - Caso contrário (só 2026+ ou um único mês), mantém a linha com **Data mais recente**
      (último snapshot do export).

    Chave: ``(org_id, empresa, multiloja, Código)``.
    Deve correr **depois** de ``enrich_pedidos_com_notas`` para não perder colunas de NF no registo
    que se mantém (o export de dezembro costuma trazer a mesma chave de vínculo à nota).
    """
    if df.empty or "Data" not in df.columns:
        return df, {"pedidos_dedupe_multiloja_codigo": "skip_empty_or_no_data"}
    missing = [c for c in ("org_id", "empresa", col_multiloja, col_codigo) if c not in df.columns]
    if missing:
        return df, {"pedidos_dedupe_multiloja_codigo": f"skip_missing_{missing}"}

    work = df.copy()
    for c in ("org_id", "empresa", col_multiloja, col_codigo):
        work[c] = work[c].fillna("").astype(str).str.strip()

    keys = ["org_id", "empresa", col_multiloja, col_codigo]
    dts = pd.to_datetime(work["Data"], errors="coerce", dayfirst=True)

    def _pick_one(g: pd.DataFrame) -> pd.DataFrame:
        idx = g.index
        g_dt = dts.loc[idx]
        dec_mask = (g_dt.dt.year == 2025) & (g_dt.dt.month == 12)
        if dec_mask.any():
            pick = g_dt.loc[dec_mask].idxmin()
            return work.loc[[pick]]
        pick = g_dt.idxmax()
        return work.loc[[pick]]

    n_before = len(work)
    _gb = work.groupby(keys, dropna=False, group_keys=False)
    try:
        out = _gb.apply(_pick_one, include_groups=False)
    except TypeError:
        out = _gb.apply(_pick_one)
    out = out.reset_index(drop=True)
    n_after = len(out)
    meta = {
        "pedidos_dedupe_multiloja_codigo": "applied",
        "pedidos_linhas_antes": n_before,
        "pedidos_linhas_depois": n_after,
        "pedidos_linhas_removidas": n_before - n_after,
    }
    return out, meta
