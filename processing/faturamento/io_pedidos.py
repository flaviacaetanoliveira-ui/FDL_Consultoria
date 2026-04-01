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
    Lê **todos** os ``*.csv`` do diretório (nome ordenado, case-insensitive) e concatena.

    Usado no faturamento **schema_version 2** para não perder meses quando existem vários exports
    (ex.: Jan, Fev, Mar) na mesma pasta — ``load_latest_pedidos_csv`` só devolvia o mais recente por mtime.
    Cada linha fica com ``pedidos_arquivo`` = nome do ficheiro de origem.
    """
    pedidos_dir = pedidos_dir.expanduser().resolve()
    if not pedidos_dir.is_dir():
        raise FileNotFoundError(f"Diretório de pedidos não encontrado: {pedidos_dir}")
    # Ordem estável por nome (não por mtime). A coluna ``Data`` de cada linha é a referência operacional;
    # nomes «Jan / Fev / Mar» podem não coincidir com ordem alfabética em todos os idiomas.
    files = sorted(pedidos_dir.glob("*.csv"), key=lambda p: p.name.lower())
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
