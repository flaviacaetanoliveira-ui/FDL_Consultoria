"""
Alíquota e regime tributário declarados em ``faturamento_params`` (schema V2).

Usado pela Apuração Fiscal para legendas e avisos — **não** substitui o cálculo de imposto materializado.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TypedDict

from .params import (
    FaturamentoParams,
    FaturamentoParamsV2,
    load_faturamento_params,
)

_REPO_ROOT = Path(__file__).resolve().parents[2]

_FALLBACK_PARAMS_JSON: dict[str, Path] = {
    "cliente_2": _REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json",
}


class AliquotaConfiguradaInfo(TypedDict):
    modo: str
    valor_unico_pct: float | None
    valores_por_empresa: dict[str, float]
    min_pct: float
    max_pct: float


class RegimesDetectadosInfo(TypedDict):
    regimes_presentes: frozenset[str]
    empresas_por_regime: dict[str, list[str]]
    tem_regime_fora_escopo: bool
    empresas_fora_escopo: list[str]


def find_empresa_faturamento_entry(
    params: FaturamentoParamsV2,
    empresa_chave: str,
):
    k = str(empresa_chave).strip()
    if not k:
        return None
    for e in params.empresas:
        if e.org_id == k or e.empresa == k:
            return e
    return None


def get_aliquota_imposto_por_empresa(
    params: FaturamentoParams | FaturamentoParamsV2 | None,
    empresa_chave: str,
) -> float | None:
    """Alíquota configurada (0–1) para ``org_id`` ou rótulo ``empresa``; ``None`` se V2 e empresa ausente."""
    if params is None:
        return None
    if isinstance(params, FaturamentoParams):
        return float(params.aliquota_imposto)
    ent = find_empresa_faturamento_entry(params, empresa_chave)
    if ent is None:
        return None
    if ent.aliquota_imposto is not None:
        return float(ent.aliquota_imposto)
    return float(params.aliquota_imposto)


def get_regime_tributario_por_empresa(
    params: FaturamentoParams | FaturamentoParamsV2 | None,
    empresa_chave: str,
) -> str | None:
    if params is None:
        return None
    if isinstance(params, FaturamentoParams):
        return None
    ent = find_empresa_faturamento_entry(params, empresa_chave)
    if ent is None:
        return None
    return ent.regime_tributario


def _ratio_para_pct(ratio: float) -> float:
    return float(ratio) * 100.0


def aliquota_configurada_para_empresas_filtradas(
    params: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> AliquotaConfiguradaInfo:
    """
    Agrega alíquotas das empresas no filtro (percentagem 0–100 na saída).

    ``empresas_chaves``: ``org_id`` ou nome de exibição conforme no JSON.
    Lista vazia ⇒ ``modo`` ``desconhecida``.
    """
    empty: AliquotaConfiguradaInfo = {
        "modo": "desconhecida",
        "valor_unico_pct": None,
        "valores_por_empresa": {},
        "min_pct": 0.0,
        "max_pct": 0.0,
    }
    if params is None:
        return empty
    if isinstance(params, FaturamentoParams):
        if not empresas_chaves:
            return empty
        v = _ratio_para_pct(float(params.aliquota_imposto))
        return {
            "modo": "unica",
            "valor_unico_pct": v,
            "valores_por_empresa": {"_default": v},
            "min_pct": v,
            "max_pct": v,
        }

    if not empresas_chaves:
        return empty

    valores_por_empresa: dict[str, float] = {}
    for ch in empresas_chaves:
        ent = find_empresa_faturamento_entry(params, ch)
        if ent is None:
            continue
        ai = ent.aliquota_imposto if ent.aliquota_imposto is not None else params.aliquota_imposto
        valores_por_empresa[ent.org_id] = _ratio_para_pct(float(ai))

    if not valores_por_empresa:
        return empty

    pct_vals = list(valores_por_empresa.values())
    mn = min(pct_vals)
    mx = max(pct_vals)
    if len(set(round(x, 6) for x in pct_vals)) == 1:
        u = pct_vals[0]
        return {
            "modo": "unica",
            "valor_unico_pct": u,
            "valores_por_empresa": valores_por_empresa,
            "min_pct": mn,
            "max_pct": mx,
        }
    return {
        "modo": "multipla",
        "valor_unico_pct": None,
        "valores_por_empresa": valores_por_empresa,
        "min_pct": mn,
        "max_pct": mx,
    }


def detectar_regimes_tributarios(
    params: FaturamentoParams | FaturamentoParamsV2 | None,
    empresas_chaves: list[str],
) -> RegimesDetectadosInfo:
    """
    Analisa regimes declarados para as empresas listadas (rótulo ou ``org_id``).

    **Fora do escopo** calibrado (Simples nesta fase): regime explícito diferente de
    ``simples_nacional`` (``lucro_presumido``, ``lucro_real``, ``mei``).
    Regime omitido ⇒ não entra como «fora do escopo».
    """
    if params is None or isinstance(params, FaturamentoParams) or not empresas_chaves:
        return {
            "regimes_presentes": frozenset(),
            "empresas_por_regime": {},
            "tem_regime_fora_escopo": False,
            "empresas_fora_escopo": [],
        }

    regimes: set[str] = set()
    por_reg: dict[str, list[str]] = {}
    fora: list[str] = []

    for ch in empresas_chaves:
        ent = find_empresa_faturamento_entry(params, ch)
        if ent is None:
            continue
        reg = ent.regime_tributario
        if reg:
            regimes.add(reg)
            por_reg.setdefault(reg, []).append(ent.org_id)
        if reg and reg != "simples_nacional":
            if ent.empresa not in fora:
                fora.append(ent.empresa)

    tem = bool(fora)
    return {
        "regimes_presentes": frozenset(regimes),
        "empresas_por_regime": por_reg,
        "tem_regime_fora_escopo": tem,
        "empresas_fora_escopo": fora,
    }


def resolve_faturamento_params_path_for_ui(load_info: dict[str, object]) -> Path | None:
    """
    Resolve o JSON de params usado na materialização: ``metadata.params_path`` quando o ficheiro existe;
    senão fallback por ``cliente`` no mesmo ``metadata.json``.
    """
    for key in ("params_path", "faturamento_params_path"):
        raw = str(load_info.get(key, "")).strip()
        if raw and Path(raw).is_file():
            return Path(raw).expanduser().resolve()

    path_final = load_info.get("faturamento_path_final_resolved")
    try:
        cli = ""
        if path_final:
            meta_path = Path(str(path_final)).expanduser().resolve().parent / "metadata.json"
            if meta_path.is_file():
                meta = json.loads(meta_path.read_text(encoding="utf-8"))
                pp = meta.get("params_path")
                if pp and Path(str(pp)).is_file():
                    return Path(str(pp)).expanduser().resolve()
                cli = str(meta.get("cliente", "")).strip()
        if not cli:
            cli = str(load_info.get("cliente_slug", "")).strip() or str(load_info.get("cliente", "")).strip()
        if cli and cli in _FALLBACK_PARAMS_JSON:
            cand = _FALLBACK_PARAMS_JSON[cli]
            if cand.is_file():
                return cand.resolve()
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        pass
    return None


def load_faturamento_params_for_ui(load_info: dict[str, object]) -> FaturamentoParams | FaturamentoParamsV2 | None:
    p = resolve_faturamento_params_path_for_ui(load_info)
    if p is None:
        return None
    try:
        return load_faturamento_params(p)
    except Exception:
        return None


def enrich_aliquota_ref_pct_for_stats(aliquotas_info: AliquotaConfiguradaInfo) -> float:
    """Valor único para gravar em stats (meio-termo quando há várias alíquotas)."""
    if aliquotas_info["modo"] == "unica" and aliquotas_info["valor_unico_pct"] is not None:
        return float(aliquotas_info["valor_unico_pct"])
    if aliquotas_info["modo"] == "multipla":
        return (aliquotas_info["min_pct"] + aliquotas_info["max_pct"]) / 2.0
    return 0.0
