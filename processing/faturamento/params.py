"""
Leitura de faturamento_params.json.

- schema_version 2: cliente_root, cliente_slug, custo compartilhado, empresas[] (**padrão oficial**).
- schema_version 1 (ou ausente): um pedidos_dir + custo_xlsx — **deprecado (legado temporário)**;
  sem novas evoluções; remoção futura quando V2 for o único fluxo suportado no app e dados.

Alíquotas: números decimais com ponto no JSON (ex.: 0.12).

No fluxo V2 com notas rateadas (``faturamento-v3``), ``coluna_base_imposto`` mantém-se no JSON por
compatibilidade; o imposto usa ``Nota_Valor_Liquido_Rateado`` / ``Base_Imposto``, não essa coluna.
Ver ``docs/faturamento_imposto_coluna_base_vs_nota.md``.
"""
from __future__ import annotations

import json
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from processing.faturamento.normalize import normalize_nf_fiscal_commercial_join_key_scalar


@dataclass(frozen=True)
class FaturamentoParams:
    """Parâmetros **deprecados** (schema_version 1): uma única pasta de pedidos + custo.

    Mantido só para compatibilidade até migração completa para ``FaturamentoParamsV2``.
    """

    aliquota_imposto: float
    aliquota_despesas_fixas: float
    pedidos_dir: str | None
    custo_xlsx: str | None
    permite_faturamento_sem_nf: bool


@dataclass(frozen=True)
class EmpresaFaturamentoEntry:
    org_id: str
    empresa: str
    pedidos_dir: str
    permite_faturamento_sem_nf: bool | None
    notas_saida_dir: str | None = None
    notas_entrada_dir: str | None = None
    aliquota_imposto: float | None = None
    aliquota_despesas_fixas: float | None = None
    excluir_notas_fiscal: tuple[str, ...] = ()


@dataclass(frozen=True)
class FaturamentoParamsV2:
    cliente_root: Path
    cliente_slug: str
    custo_xlsx_resolved: Path
    empresas: tuple[EmpresaFaturamentoEntry, ...]
    aliquota_imposto: float
    aliquota_despesas_fixas: float
    permite_faturamento_sem_nf_default: bool
    coluna_base_imposto: tuple[str, ...]
    params_mensais_resolved: Path | None
    notas_saida_dir: str
    notas_entrada_dir: str | None = None
    # Quando True (padrão), o painel NF materializado aplica custo ADS (3,5% + fixo) e desconta do resultado.
    nf_panel_ads: bool = True


class FaturamentoParamsError(ValueError):
    pass


def _sanitize_slug_segment(raw: str) -> str:
    s = raw.strip().replace("..", "").replace("/", "-").replace("\\", "-")
    s = re.sub(r"[^a-zA-Z0-9_-]+", "-", s).strip("-")
    return s or "default"


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


def _optional_float(name: str, raw: Any) -> float | None:
    if raw is None or (isinstance(raw, str) and not str(raw).strip()):
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError) as e:
        raise FaturamentoParamsError(f"{name} deve ser numérico (ex.: 0.12 no JSON com ponto).") from e
    if v < 0 or v > 1:
        raise FaturamentoParamsError(f"{name} deve estar entre 0 e 1 (decimal, ex.: 0.12).")
    return v


def _as_bool(raw: Any, default: bool = False) -> bool:
    if isinstance(raw, bool):
        return raw
    if raw is None:
        return default
    return str(raw).strip().lower() in ("1", "true", "yes", "sim")


def _optional_bool(raw: Any) -> bool | None:
    if raw is None:
        return None
    return _as_bool(raw)


def peek_faturamento_schema_version(path: Path) -> int:
    """Devolve ``1`` se o JSON omitir ``schema_version`` (compat. legado V1)."""
    path = path.expanduser().resolve()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        return 1
    v = raw.get("schema_version", 1)
    try:
        return int(v)
    except (TypeError, ValueError):
        return 1


def read_cliente_slug_v2(path: Path) -> str:
    """Lê só cliente_slug (para resolver pasta de saída na materialização)."""
    path = path.expanduser().resolve()
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise FaturamentoParamsError("JSON de parâmetros deve ser um objeto.")
    slug = str(raw.get("cliente_slug", "")).strip()
    if not slug:
        raise FaturamentoParamsError("schema_version 2 exige cliente_slug explícito.")
    return _sanitize_slug_segment(slug)


def _coluna_base_tuple(raw: Any) -> tuple[str, ...]:
    if raw is None:
        raise FaturamentoParamsError("coluna_base_imposto é obrigatória em schema_version 2.")
    if isinstance(raw, str) and raw.strip():
        return (raw.strip(),)
    if isinstance(raw, list):
        out = [str(x).strip() for x in raw if str(x).strip()]
        if not out:
            raise FaturamentoParamsError("coluna_base_imposto (lista) não pode ser vazia.")
        return tuple(out)
    raise FaturamentoParamsError("coluna_base_imposto deve ser string ou lista de strings.")


def _load_v2(path: Path, raw: dict[str, Any]) -> FaturamentoParamsV2:
    root_s = raw.get("cliente_root")
    if not root_s or not str(root_s).strip():
        raise FaturamentoParamsError("schema_version 2 exige cliente_root.")
    cliente_root = Path(str(root_s).strip()).expanduser().resolve()
    if not cliente_root.is_dir():
        raise FaturamentoParamsError(f"cliente_root não é pasta: {cliente_root}")

    slug = str(raw.get("cliente_slug", "")).strip()
    if not slug:
        raise FaturamentoParamsError("schema_version 2 exige cliente_slug explícito (alinhar a data_products / secrets).")
    slug = _sanitize_slug_segment(slug)

    cx = raw.get("custo_xlsx")
    if not cx or not str(cx).strip():
        raise FaturamentoParamsError("schema_version 2 exige custo_xlsx.")
    p_cx = Path(str(cx).strip()).expanduser()
    if p_cx.is_absolute():
        custo_xlsx_resolved = p_cx.resolve()
    else:
        custo_xlsx_resolved = (cliente_root / p_cx).resolve()
    if not custo_xlsx_resolved.is_file():
        raise FaturamentoParamsError(f"Tabela de custo não encontrada: {custo_xlsx_resolved}")

    emp_raw = raw.get("empresas")
    if not isinstance(emp_raw, list) or not emp_raw:
        raise FaturamentoParamsError("schema_version 2 exige empresas (lista não vazia).")

    default_sem_nf = _as_bool(raw.get("permite_faturamento_sem_nf", False))
    entries: list[EmpresaFaturamentoEntry] = []
    for i, e in enumerate(emp_raw):
        if not isinstance(e, dict):
            raise FaturamentoParamsError(f"empresas[{i}] deve ser objeto.")
        oid = str(e.get("org_id", "")).strip()
        ename = str(e.get("empresa", "")).strip()
        pdir = str(e.get("pedidos_dir", "")).strip()
        if not oid or not ename or not pdir:
            raise FaturamentoParamsError(f"empresas[{i}]: org_id, empresa e pedidos_dir são obrigatórios.")
        ped_path = (cliente_root / pdir).resolve()
        if not ped_path.is_dir():
            raise FaturamentoParamsError(f"Pasta de pedidos não existe: {ped_path}")
        ns_emp = e.get("notas_saida_dir")
        ns_emp_s = str(ns_emp).strip() if ns_emp is not None and str(ns_emp).strip() else None
        ne_emp = e.get("notas_entrada_dir")
        ne_emp_s = str(ne_emp).strip() if ne_emp is not None and str(ne_emp).strip() else None
        ai_e = _optional_float(f"empresas[{i}].aliquota_imposto", e.get("aliquota_imposto"))
        ad_e = _optional_float(f"empresas[{i}].aliquota_despesas_fixas", e.get("aliquota_despesas_fixas"))
        excl_raw = e.get("excluir_notas_fiscal")
        excl_tuple: tuple[str, ...] = ()
        if isinstance(excl_raw, list):
            excl_tuple = tuple(
                normalize_nf_fiscal_commercial_join_key_scalar(str(x))
                for x in excl_raw
                if str(x).strip()
            )
        elif excl_raw is not None and str(excl_raw).strip():
            raise FaturamentoParamsError(f"empresas[{i}].excluir_notas_fiscal deve ser lista de números/NF (strings).")
        entries.append(
            EmpresaFaturamentoEntry(
                org_id=_sanitize_slug_segment(oid),
                empresa=ename,
                pedidos_dir=pdir,
                permite_faturamento_sem_nf=_optional_bool(e.get("permite_faturamento_sem_nf")),
                notas_saida_dir=ns_emp_s,
                notas_entrada_dir=ne_emp_s,
                aliquota_imposto=ai_e,
                aliquota_despesas_fixas=ad_e,
                excluir_notas_fiscal=excl_tuple,
            )
        )

    ai = _as_float("aliquota_imposto", raw.get("aliquota_imposto"))
    ad = _as_float("aliquota_despesas_fixas", raw.get("aliquota_despesas_fixas"))
    cands = _coluna_base_tuple(raw.get("coluna_base_imposto"))

    pm = raw.get("params_mensais")
    params_mensais_resolved: Path | None = None
    if pm is not None and str(pm).strip():
        p_pm = Path(str(pm).strip()).expanduser()
        params_mensais_resolved = p_pm.resolve() if p_pm.is_absolute() else (cliente_root / p_pm).resolve()

    notas_saida_dir = str(raw.get("notas_saida_dir", "notas_saida") or "notas_saida").strip() or "notas_saida"

    ne_root = raw.get("notas_entrada_dir")
    notas_entrada_dir_default = str(ne_root).strip() if ne_root is not None and str(ne_root).strip() else None

    nf_panel_ads = _as_bool(raw.get("nf_panel_ads", True))

    root_digits = set(re.findall(r"\d+", cliente_root.name))
    slug_digits = set(re.findall(r"\d+", slug))
    if root_digits and slug_digits and root_digits.isdisjoint(slug_digits):
        warnings.warn(
            f"cliente_root (…«{cliente_root.name}») e cliente_slug («{slug}») têm números distintos. "
            "Confirmar alinhamento com data_products / Streamlit.",
            UserWarning,
            stacklevel=1,
        )

    return FaturamentoParamsV2(
        cliente_root=cliente_root,
        cliente_slug=slug,
        custo_xlsx_resolved=custo_xlsx_resolved,
        empresas=tuple(entries),
        aliquota_imposto=ai,
        aliquota_despesas_fixas=ad,
        permite_faturamento_sem_nf_default=default_sem_nf,
        coluna_base_imposto=cands,
        params_mensais_resolved=params_mensais_resolved,
        notas_saida_dir=notas_saida_dir,
        notas_entrada_dir=notas_entrada_dir_default,
        nf_panel_ads=nf_panel_ads,
    )


def load_faturamento_params(path: Path) -> FaturamentoParams | FaturamentoParamsV2:
    """Carrega V2 se ``schema_version >= 2``; caso contrário devolve ``FaturamentoParams`` (V1 legado)."""
    path = path.expanduser().resolve()
    if not path.is_file():
        raise FaturamentoParamsError(f"Arquivo de parâmetros não encontrado: {path}")
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise FaturamentoParamsError("JSON de parâmetros deve ser um objeto.")

    ver = raw.get("schema_version", 1)  # omitido ⇒ V1 legado (deprecado)
    try:
        ver_int = int(ver)
    except (TypeError, ValueError):
        ver_int = 1

    if ver_int >= 2:
        return _load_v2(path, raw)

    ai = _as_float("aliquota_imposto", raw.get("aliquota_imposto"))
    ad = _as_float("aliquota_despesas_fixas", raw.get("aliquota_despesas_fixas"))

    ps = raw.get("pedidos_dir")
    cx = raw.get("custo_xlsx")
    pedidos_dir = str(Path(str(ps)).expanduser().resolve()) if ps and str(ps).strip() else None
    custo_xlsx = str(Path(str(cx)).expanduser().resolve()) if cx and str(cx).strip() else None

    psem = _as_bool(raw.get("permite_faturamento_sem_nf", False))

    return FaturamentoParams(
        aliquota_imposto=ai,
        aliquota_despesas_fixas=ad,
        pedidos_dir=pedidos_dir,
        custo_xlsx=custo_xlsx,
        permite_faturamento_sem_nf=psem,
    )
