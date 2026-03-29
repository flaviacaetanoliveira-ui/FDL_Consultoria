"""
Leitura de faturamento_params.json.

Alíquotas: números decimais com ponto no JSON (ex.: 0.12). Não usar vírgula como separador decimal.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class FaturamentoParams:
    aliquota_imposto: float
    aliquota_despesas_fixas: float
    pedidos_dir: str | None
    custo_xlsx: str | None
    permite_faturamento_sem_nf: bool


class FaturamentoParamsError(ValueError):
    pass


def _as_float(name: str, raw: Any) -> float:
    if raw is None:
        raise FaturamentoParamsError(f"Parâmetro obrigatório ausente: {name}")
    try:
        v = float(raw)
    except (TypeError, ValueError) as e:
        raise FaturamentoParamsError(f"{name} deve ser numérico (ex.: 0.12 no JSON com ponto).") from e
    if v < 0 or v > 1:
        raise FaturamentoParamsError(f"{name} deve estar entre 0 e 1 (decimal, ex.: 0.12).")
    return v


def load_faturamento_params(path: Path) -> FaturamentoParams:
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FaturamentoParamsError(f"Arquivo de parâmetros não encontrado: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise FaturamentoParamsError("JSON de parâmetros deve ser um objeto.")

    ai = _as_float("aliquota_imposto", raw.get("aliquota_imposto"))
    ad = _as_float("aliquota_despesas_fixas", raw.get("aliquota_despesas_fixas"))

    ps = raw.get("pedidos_dir")
    cx = raw.get("custo_xlsx")
    pedidos_dir = str(Path(str(ps)).expanduser().resolve()) if ps and str(ps).strip() else None
    custo_xlsx = str(Path(str(cx)).expanduser().resolve()) if cx and str(cx).strip() else None

    psem = raw.get("permite_faturamento_sem_nf", False)
    if not isinstance(psem, bool):
        psem = str(psem).strip().lower() in ("1", "true", "yes", "sim")

    return FaturamentoParams(
        aliquota_imposto=ai,
        aliquota_despesas_fixas=ad,
        pedidos_dir=pedidos_dir,
        custo_xlsx=custo_xlsx,
        permite_faturamento_sem_nf=bool(psem),
    )
