"""
Materialização de NF de entrada — devoluções de venda (abatimento fiscal).

Grava ``dataset_faturamento_devolucoes.parquet`` sem alterar ``dataset_faturamento_fiscal.parquet``.

**schema_version_devolucoes 3** — acrescenta ``Nota_Destinatario_Documento`` e ``Nota_Destinatario_Nome``
(cópia descritiva do bruto Bling; normalização do documento = só dígitos). Versão 2 permanece legível
no UI desde que as colunas opcionais falhem em falta; rematerializar preenche os novos campos.

TODO (próximo ciclo): usar documento+nome como **input** para vínculo devolução↔venda (matching CPF+SKU),
sem alterar a semântica destas colunas como metadado da NF de entrada.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from .cobertura_devolucoes import auditar_cobertura, detectar_meses_suspeitos
from .fiscal_devolucoes_constants import (
    COL_TIPO_ABATIMENTO,
    NATUREZAS_DEVOLUCAO,
    SITUACOES_DEVOLUCAO_VALIDAS,
    TIPO_ABATIMENTO_DEVOLUCAO_VENDA,
)
from .io_notas_entrada import (
    _detect_col_cpf_cnpj,
    _detect_col_nome_destinatario,
    _detect_col_numero_nf,
    aplicar_filtros_devolucao,
    load_notas_entrada_brutas_from_dir,
    normalizar_cpf_cnpj_somente_digitos,
    series_valor_liquido_nota_entrada_bling,
)
from .join_notas import (
    _filtrar_notas_por_empresa,
    _prep_notas_dataframe,
    _situacao_por_nf_agregada,
)
from .normalize import normalize_nf_fiscal_commercial_join_key_scalar, normalize_pedido_join_key
from .params import FaturamentoParams, FaturamentoParamsV2, load_faturamento_params
from .validate import FaturamentoValidationError


SCHEMA_VERSION_DEVOLUCOES = 3
PIPELINE_REVISION_DEVOLUCOES = "devolucoes-fiscais-v3"

DEVOLUCOES_CONTRACT_COLUMNS: tuple[str, ...] = (
    "org_id",
    "empresa",
    "Nota_Numero_Normalizado",
    "Nota_Data_Emissao",
    "Nota_Situacao",
    "Valor_Liquido_Devolucao",
    "Nota_Destinatario_Documento",
    "Nota_Destinatario_Nome",
    "Natureza",
    COL_TIPO_ABATIMENTO,
    "schema_version_devolucoes",
    "_versao_pipeline",
    "_data_processamento",
    "_origem_arquivo",
)


def devolucoes_contract_dataframe_valid(df: pd.DataFrame) -> bool:
    if df.empty:
        return False
    required = frozenset(
        {
            "org_id",
            "empresa",
            "Nota_Numero_Normalizado",
            "Nota_Data_Emissao",
            "Valor_Liquido_Devolucao",
        }
    )
    return required.issubset(df.columns)


def _empty_devolucoes_frame() -> pd.DataFrame:
    return pd.DataFrame(columns=list(DEVOLUCOES_CONTRACT_COLUMNS))


def build_devolucoes_fiscal_dataframe(params_path: Path) -> pd.DataFrame:
    out, _ = build_devolucoes_fiscal_dataframe_with_audit(params_path)
    return out


def build_devolucoes_fiscal_dataframe_with_audit(params_path: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Constrói o DataFrame de devoluções (entrada Bling) a partir de ``faturamento_params.json`` (V2).

    Metadados ``Nota_Destinatario_*`` vêm do CSV bruto (deteção por coluna); agregação por NF usa ``first``.
    TODO próximo ciclo: input para vínculo automático devolução↔venda (CPF+SKU), sem mudar o papel destas colunas.
    """
    params_union = load_faturamento_params(params_path)
    if isinstance(params_union, FaturamentoParams):
        return _empty_devolucoes_frame(), _empty_devolucoes_meta_snapshot()
    if not isinstance(params_union, FaturamentoParamsV2):
        return _empty_devolucoes_frame(), _empty_devolucoes_meta_snapshot()

    parts: list[pd.DataFrame] = []
    cobertura_por_empresa: dict[str, dict[str, Any]] = {}
    warnings: list[str] = []
    default_entrada = getattr(params_union, "notas_entrada_dir", None)
    default_entrada_s = str(default_entrada).strip() if default_entrada else ""
    processado_em = datetime.now(timezone.utc)

    for emp in params_union.empresas:
        rel = (emp.notas_entrada_dir or default_entrada_s).strip() or default_entrada_s
        if not rel:
            continue
        notas_dir = (params_union.cliente_root / rel).resolve()
        brutas = load_notas_entrada_brutas_from_dir(notas_dir)
        cobertura = auditar_cobertura(brutas)
        cobertura_por_empresa[str(emp.org_id).strip()] = cobertura.to_metadata()
        suspeitos = detectar_meses_suspeitos(cobertura.cobertura_mensal)
        if suspeitos:
            org_id = str(emp.org_id).strip()
            for ym in suspeitos:
                warnings.append(
                    f"{org_id}: {ym} sem devoluções entre meses ativos — verificar extração"
                )
        if cobertura.sem_data > 0:
            warnings.append(
                f"{str(emp.org_id).strip()}: {cobertura.sem_data} devoluções sem data de emissão válida"
            )
        raw = aplicar_filtros_devolucao(brutas)
        if raw.empty:
            continue
        vl_entrada = series_valor_liquido_nota_entrada_bling(raw)
        try:
            prep = _prep_notas_dataframe(raw)
        except FaturamentoValidationError:
            continue
        if prep.empty:
            continue
        # Substitui ``vl_liq`` por total alinhado ao Bling: Valor total + Frete + Outras despesas − Desconto
        # quando colunas auxiliares existem (ver ``series_valor_liquido_nota_entrada_bling``).
        _vl_adj = vl_entrada.reindex(prep.index)
        prep = prep.copy()
        prep["vl_liq"] = _vl_adj.fillna(prep["vl_liq"])
        prep = _filtrar_notas_por_empresa(prep, emp.org_id, emp.empresa)
        if prep.empty:
            continue

        col_doc_raw = _detect_col_cpf_cnpj(list(raw.columns))
        col_nom_raw = _detect_col_nome_destinatario(list(raw.columns))
        if col_doc_raw and col_doc_raw in raw.columns:
            prep["_doc_dest_agg"] = raw.loc[prep.index, col_doc_raw]
        else:
            prep["_doc_dest_agg"] = pd.Series("", index=prep.index, dtype=object)
        if col_nom_raw and col_nom_raw in raw.columns:
            prep["_nome_dest_agg"] = raw.loc[prep.index, col_nom_raw]
        else:
            prep["_nome_dest_agg"] = pd.Series("", index=prep.index, dtype=object)

        src_by_nf = pd.Series(dtype=object)
        if "__arquivo_nota__" in raw.columns:
            col_nf_raw = _detect_col_numero_nf(list(raw.columns))
            if col_nf_raw:
                nfk_raw = normalize_pedido_join_key(raw[col_nf_raw].astype(str)).astype(str).str.strip()
                src_by_nf = raw.assign(_nfk=nfk_raw).groupby("_nfk", sort=False)["__arquivo_nota__"].first()

        sit_by_nf = _situacao_por_nf_agregada(prep)
        g = prep.groupby("nf_key", sort=False)
        agg = g.agg(
            Valor_Liquido_Devolucao=("vl_liq", "sum"),
            Nota_Data_Emissao=("dt_emissao", "min"),
            Nota_Destinatario_Documento=("_doc_dest_agg", "first"),
            Nota_Destinatario_Nome=("_nome_dest_agg", "first"),
        ).reset_index()
        agg = agg.rename(columns={"nf_key": "Nota_Numero_Normalizado"})
        if not src_by_nf.empty:
            agg["_origem_arquivo"] = agg["Nota_Numero_Normalizado"].map(src_by_nf).fillna("").astype(str)
        else:
            agg["_origem_arquivo"] = ""
        agg["Nota_Numero_Normalizado"] = agg["Nota_Numero_Normalizado"].map(
            normalize_nf_fiscal_commercial_join_key_scalar
        )
        agg["Nota_Situacao"] = agg["Nota_Numero_Normalizado"].map(sit_by_nf).fillna("").astype(str)
        agg["org_id"] = str(emp.org_id).strip()
        agg["empresa"] = str(emp.empresa).strip()
        agg["Natureza"] = NATUREZAS_DEVOLUCAO[0]
        agg[COL_TIPO_ABATIMENTO] = TIPO_ABATIMENTO_DEVOLUCAO_VENDA
        agg["Valor_Liquido_Devolucao"] = pd.to_numeric(agg["Valor_Liquido_Devolucao"], errors="coerce").fillna(0.0)
        agg["Nota_Destinatario_Documento"] = agg["Nota_Destinatario_Documento"].map(
            normalizar_cpf_cnpj_somente_digitos
        )
        agg["Nota_Destinatario_Nome"] = (
            agg["Nota_Destinatario_Nome"].fillna("").astype(str).str.strip()
        )
        agg["schema_version_devolucoes"] = SCHEMA_VERSION_DEVOLUCOES
        agg["_versao_pipeline"] = PIPELINE_REVISION_DEVOLUCOES
        agg["_data_processamento"] = processado_em
        parts.append(agg[list(DEVOLUCOES_CONTRACT_COLUMNS)].copy())

    if not parts:
        return _empty_devolucoes_frame(), _devolucoes_meta_snapshot_with_cobertura(
            _empty_devolucoes_frame(),
            cobertura={"por_empresa": cobertura_por_empresa},
            warnings=warnings,
        )

    out = pd.concat(parts, ignore_index=True)
    dup = out.duplicated(subset=["org_id", "empresa", "Nota_Numero_Normalizado"], keep=False)
    if dup.any():
        ta = COL_TIPO_ABATIMENTO
        out = (
            out.groupby(["org_id", "empresa", "Nota_Numero_Normalizado"], sort=False)
            .agg(
                Nota_Data_Emissao=("Nota_Data_Emissao", "min"),
                Nota_Situacao=("Nota_Situacao", "first"),
                Valor_Liquido_Devolucao=("Valor_Liquido_Devolucao", "sum"),
                Nota_Destinatario_Documento=("Nota_Destinatario_Documento", "first"),
                Nota_Destinatario_Nome=("Nota_Destinatario_Nome", "first"),
                Natureza=("Natureza", "first"),
                **{ta: (ta, "first")},
                schema_version_devolucoes=("schema_version_devolucoes", "first"),
                _versao_pipeline=("_versao_pipeline", "first"),
                _data_processamento=("_data_processamento", "first"),
                _origem_arquivo=("_origem_arquivo", "first"),
            )
            .reset_index()
        )
    if out["Nota_Data_Emissao"].notna().any():
        out = out.sort_values("Nota_Data_Emissao", ascending=False, na_position="last")
    out = out.reset_index(drop=True)
    return out, _devolucoes_meta_snapshot_with_cobertura(
        out,
        cobertura={"por_empresa": cobertura_por_empresa},
        warnings=warnings,
    )


def _empty_devolucoes_meta_snapshot() -> dict[str, Any]:
    return {
        "pipeline_revision_devolucoes": PIPELINE_REVISION_DEVOLUCOES,
        "filtros_aplicados": {
            "naturezas": list(NATUREZAS_DEVOLUCAO),
            "situacoes_validas": list(SITUACOES_DEVOLUCAO_VALIDAS),
        },
        "cobertura": {"por_empresa": {}},
        "warnings": [],
    }


def _devolucoes_meta_snapshot_with_cobertura(
    df: pd.DataFrame,
    *,
    cobertura: dict[str, Any],
    warnings: list[str],
) -> dict[str, Any]:
    snap = devolucoes_materializado_meta_snapshot(df)
    snap["pipeline_revision_devolucoes"] = PIPELINE_REVISION_DEVOLUCOES
    snap["filtros_aplicados"] = {
        "naturezas": list(NATUREZAS_DEVOLUCAO),
        "situacoes_validas": list(SITUACOES_DEVOLUCAO_VALIDAS),
    }
    snap["cobertura"] = cobertura
    snap["warnings"] = warnings
    return snap


def devolucoes_materializado_meta_snapshot(df: pd.DataFrame) -> dict[str, Any]:
    if df.empty or not devolucoes_contract_dataframe_valid(df):
        return {
            "total_devolvido": 0.0,
            "nfs_devolucao": 0,
            "base_fiscal_composicao": "emitidas - canceladas - devolucoes",
        }
    vl = pd.to_numeric(df["Valor_Liquido_Devolucao"], errors="coerce").fillna(0.0)
    return {
        "total_devolvido": float(vl.sum()),
        "nfs_devolucao": int(len(df)),
        "base_fiscal_composicao": "emitidas - canceladas - devolucoes",
    }
