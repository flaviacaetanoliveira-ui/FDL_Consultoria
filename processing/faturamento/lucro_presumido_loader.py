"""
Loader de parâmetros Lucro Presumido a partir de JSON de configuração do cliente.

Conecta o motor puro (lucro_presumido.py) ao JSON de params do cliente.
Mantém separação: motor não conhece I/O, loader não conhece cálculos.

Estrutura híbrida suportada:
- Defaults no nível raiz: lucro_presumido_defaults, icms_params_defaults
- Override por empresa: lucro_presumido_params, icms_params (em empresas[i])

Empresa identificada como LP via campo regime_tributario == "lucro_presumido".
"""

from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path
from typing import Any, Mapping

from processing.faturamento.lucro_presumido import IcmsParams, LucroPresumidoParams

logger = logging.getLogger(__name__)


def _read_json_validated(json_path: Path) -> dict[str, Any]:
    if not json_path.is_file():
        raise FileNotFoundError(f"Arquivo de parâmetros não encontrado: {json_path}")
    try:
        raw = json.loads(json_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON malformado em {json_path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ValueError(f"JSON inválido em {json_path}: raiz deve ser objeto.")
    empresas = raw.get("empresas")
    if not isinstance(empresas, list):
        raise ValueError(f"JSON inválido em {json_path}: chave 'empresas' deve ser lista.")
    return raw


def _find_empresa_by_org_id(data: dict[str, Any], org_id: str) -> dict[str, Any] | None:
    key = str(org_id).strip()
    for e in data.get("empresas", []):
        if isinstance(e, dict) and str(e.get("org_id", "")).strip() == key:
            return e
    return None


def _is_lucro_presumido(empresa: dict[str, Any]) -> bool:
    reg = str(empresa.get("regime_tributario", "")).strip().lower().replace(" ", "_")
    return reg == "lucro_presumido"


def _merge_with_defaults(raw: Mapping[str, Any] | None, defaults: Mapping[str, Any] | None) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if isinstance(defaults, Mapping):
        out.update(dict(defaults))
    if isinstance(raw, Mapping):
        out.update(dict(raw))
    return out


def _coerce_float(value: Any, default: float, field_name: str) -> float:
    if value is None or (isinstance(value, str) and not value.strip()):
        return float(default)
    try:
        return float(value)
    except (TypeError, ValueError):
        warnings.warn(
            f"Campo {field_name} com valor inválido {value!r}, usando default {default}.",
            UserWarning,
            stacklevel=2,
        )
        logger.warning("Campo %s inválido (%r), usando default %s.", field_name, value, default)
        return float(default)


def _coerce_bool(value: Any, default: bool, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "sim"}:
        return True
    if s in {"0", "false", "no", "nao", "não"}:
        return False
    warnings.warn(
        f"Campo {field_name} com valor inválido {value!r}, usando default {default}.",
        UserWarning,
        stacklevel=2,
    )
    logger.warning("Campo %s inválido (%r), usando default %s.", field_name, value, default)
    return default


def _coerce_str_float_map(
    value: Any,
    default: Mapping[str, float],
    field_name: str,
) -> dict[str, float]:
    if value is None:
        return {str(k): float(v) for k, v in default.items()}
    if not isinstance(value, Mapping):
        warnings.warn(
            f"Campo {field_name} com valor inválido {value!r}, usando default.",
            UserWarning,
            stacklevel=2,
        )
        logger.warning("Campo %s inválido (%r), usando default.", field_name, value)
        return {str(k): float(v) for k, v in default.items()}
    out: dict[str, float] = {}
    for k, v in value.items():
        key = str(k).strip().upper()
        if not key:
            continue
        out[key] = _coerce_float(v, float(default.get(key, 0.0)), f"{field_name}.{key}")
    return out


def _build_lucro_presumido_params(raw: Mapping[str, Any]) -> LucroPresumidoParams:
    d = LucroPresumidoParams()
    return LucroPresumidoParams(
        pis=_coerce_float(raw.get("pis"), d.pis, "pis"),
        cofins=_coerce_float(raw.get("cofins"), d.cofins, "cofins"),
        presuncao_irpj_ate_limite=_coerce_float(
            raw.get("presuncao_irpj_ate_limite"), d.presuncao_irpj_ate_limite, "presuncao_irpj_ate_limite"
        ),
        presuncao_irpj_acima_limite=_coerce_float(
            raw.get("presuncao_irpj_acima_limite"), d.presuncao_irpj_acima_limite, "presuncao_irpj_acima_limite"
        ),
        presuncao_csll_ate_limite=_coerce_float(
            raw.get("presuncao_csll_ate_limite"), d.presuncao_csll_ate_limite, "presuncao_csll_ate_limite"
        ),
        presuncao_csll_acima_limite=_coerce_float(
            raw.get("presuncao_csll_acima_limite"), d.presuncao_csll_acima_limite, "presuncao_csll_acima_limite"
        ),
        aliquota_irpj=_coerce_float(raw.get("aliquota_irpj"), d.aliquota_irpj, "aliquota_irpj"),
        aliquota_adicional_irpj=_coerce_float(
            raw.get("aliquota_adicional_irpj"), d.aliquota_adicional_irpj, "aliquota_adicional_irpj"
        ),
        limite_adicional_irpj_trimestral=_coerce_float(
            raw.get("limite_adicional_irpj_trimestral"),
            d.limite_adicional_irpj_trimestral,
            "limite_adicional_irpj_trimestral",
        ),
        aliquota_csll=_coerce_float(raw.get("aliquota_csll"), d.aliquota_csll, "aliquota_csll"),
        limite_receita_majoracao_anual=_coerce_float(
            raw.get("limite_receita_majoracao_anual"),
            d.limite_receita_majoracao_anual,
            "limite_receita_majoracao_anual",
        ),
        aplicar_majoracao_lc_224=_coerce_bool(
            raw.get("aplicar_majoracao_lc_224"),
            d.aplicar_majoracao_lc_224,
            "aplicar_majoracao_lc_224",
        ),
    )


def _build_icms_params(raw: Mapping[str, Any]) -> IcmsParams:
    d = IcmsParams()
    icms_inter = _coerce_str_float_map(
        raw.get("icms_interestadual_origem_sp"),
        d.icms_interestadual_origem_sp,
        "icms_interestadual_origem_sp",
    )
    fcp = _coerce_str_float_map(raw.get("fcp_destino"), d.fcp_destino, "fcp_destino")
    return IcmsParams(
        icms_interno_moveis_9403_completos=_coerce_float(
            raw.get("icms_interno_moveis_9403_completos"),
            d.icms_interno_moveis_9403_completos,
            "icms_interno_moveis_9403_completos",
        ),
        icms_interno_moveis_9403_partes_pecas=_coerce_float(
            raw.get("icms_interno_moveis_9403_partes_pecas"),
            d.icms_interno_moveis_9403_partes_pecas,
            "icms_interno_moveis_9403_partes_pecas",
        ),
        icms_interestadual_origem_sp=icms_inter,
        aliquota_destino_generica_difal=_coerce_float(
            raw.get("aliquota_destino_generica_difal"),
            d.aliquota_destino_generica_difal,
            "aliquota_destino_generica_difal",
        ),
        fcp_destino=fcp,
        fcp_default=_coerce_float(raw.get("fcp_default"), d.fcp_default, "fcp_default"),
    )


def load_lucro_presumido_params_from_json(
    json_path: Path | str,
    org_id: str,
) -> tuple[LucroPresumidoParams | None, IcmsParams | None]:
    """
    Carrega parâmetros LP + ICMS para uma empresa.

    Returns:
        - `(lp_params, icms_params)` quando empresa é Lucro Presumido.
        - `(None, None)` quando empresa não é LP ou não existe no JSON.
    """
    path = Path(json_path).expanduser().resolve()
    data = _read_json_validated(path)
    empresa = _find_empresa_by_org_id(data, org_id)
    if empresa is None:
        return None, None
    if not _is_lucro_presumido(empresa):
        return None, None

    lp_defaults = data.get("lucro_presumido_defaults")
    icms_defaults = data.get("icms_params_defaults")
    lp_raw = empresa.get("lucro_presumido_params")
    icms_raw = empresa.get("icms_params")

    if lp_raw is None and lp_defaults is None:
        msg = f"Empresa {org_id} é LP mas não tem lucro_presumido_params; usando defaults do dataclass."
        warnings.warn(msg, UserWarning, stacklevel=2)
        logger.warning(msg)

    lp_merged = _merge_with_defaults(lp_raw, lp_defaults)
    icms_merged = _merge_with_defaults(icms_raw, icms_defaults)
    return _build_lucro_presumido_params(lp_merged), _build_icms_params(icms_merged)

