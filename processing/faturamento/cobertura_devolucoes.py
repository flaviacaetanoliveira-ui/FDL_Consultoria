"""
Auditoria de cobertura da extração de notas de entrada.

A auditoria opera sobre o arquivo bruto completo, não sobre o recorte
de período usado na UI. Use cobertura_mensal para análise por período.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from processing.faturamento.fiscal_devolucoes_constants import (
    NATUREZAS_DEVOLUCAO,
    SITUACOES_DEVOLUCAO_VALIDAS,
)
from processing.faturamento.io_notas_entrada import (
    _detect_col_natureza,
    _detect_col_situacao,
    _norm_txt,
)
from processing.faturamento.io_notas_saida import detectar_col_data_emissao


@dataclass
class CoberturaDevolucoes:
    total_brutas: int
    total_devolucoes_no_arquivo: int
    total_fiscalmente_validas: int
    cobertura_mensal: list[dict[str, Any]] = field(default_factory=list)
    excluidas_por_status: list[dict[str, Any]] = field(default_factory=list)
    sem_data: int = 0

    def to_metadata(self) -> dict[str, Any]:
        out = {
            "total_brutas": self.total_brutas,
            "devolucoes_no_arquivo": self.total_devolucoes_no_arquivo,
            "fiscalmente_validas": self.total_fiscalmente_validas,
            "cobertura_mensal": self.cobertura_mensal,
            "excluidas_por_status": self.excluidas_por_status,
        }
        if self.sem_data > 0:
            out["sem_data"] = self.sem_data
        return out


def _detect_col_data_emissao_fallback(columns: list[str]) -> str:
    col_dt = detectar_col_data_emissao(columns)
    if col_dt:
        return col_dt
    aliases = {
        "data de emissão",
        "data de emissao",
        "data emissão",
        "data emissao",
        "data_emissao",
        "data",
    }
    for c in columns:
        if str(c).strip().lower() in aliases:
            return c
    return ""


def auditar_cobertura(df_brutas: pd.DataFrame) -> CoberturaDevolucoes:
    """Recebe DataFrame BRUTO (antes do filtro fiscal) e produz relatório."""
    if df_brutas.empty:
        return CoberturaDevolucoes(
            total_brutas=0,
            total_devolucoes_no_arquivo=0,
            total_fiscalmente_validas=0,
        )

    cols = list(df_brutas.columns)
    col_nat = _detect_col_natureza(cols)
    col_sit = _detect_col_situacao(cols)
    col_dt = _detect_col_data_emissao_fallback(cols)
    if not col_nat or not col_sit:
        return CoberturaDevolucoes(
            total_brutas=int(len(df_brutas)),
            total_devolucoes_no_arquivo=0,
            total_fiscalmente_validas=0,
        )

    nat_set = frozenset(NATUREZAS_DEVOLUCAO)
    sit_set = frozenset(SITUACOES_DEVOLUCAO_VALIDAS)

    natureza_norm = _norm_txt(df_brutas[col_nat])
    devolucoes = df_brutas.loc[natureza_norm.isin(nat_set)].copy()
    if devolucoes.empty:
        return CoberturaDevolucoes(
            total_brutas=int(len(df_brutas)),
            total_devolucoes_no_arquivo=0,
            total_fiscalmente_validas=0,
        )

    situacao_dev = _norm_txt(devolucoes[col_sit])
    fiscais = devolucoes.loc[situacao_dev.isin(sit_set)].copy()
    excluidas = (
        devolucoes.loc[~situacao_dev.isin(sit_set)]
        .assign(_situacao_norm=situacao_dev.loc[~situacao_dev.isin(sit_set)])
        .groupby("_situacao_norm")
        .size()
        .reset_index(name="qtd")
        .rename(columns={"_situacao_norm": "situacao"})
        .to_dict(orient="records")
    )

    cobertura_mensal: list[dict[str, Any]] = []
    sem_data = 0
    if col_dt:
        dt_br = pd.to_datetime(df_brutas[col_dt], errors="coerce", dayfirst=True)
        dt_dev = pd.to_datetime(devolucoes[col_dt], errors="coerce", dayfirst=True)
        dt_fis = pd.to_datetime(fiscais[col_dt], errors="coerce", dayfirst=True)
        sem_data = int(dt_dev.isna().sum())

        br_count = dt_br[dt_br.notna()].dt.to_period("M").astype(str).value_counts().to_dict()
        dev_count = dt_dev[dt_dev.notna()].dt.to_period("M").astype(str).value_counts().to_dict()
        fis_count = dt_fis[dt_fis.notna()].dt.to_period("M").astype(str).value_counts().to_dict()
        for ym in sorted(br_count.keys()):
            cobertura_mensal.append(
                {
                    "ano_mes": ym,
                    "qtd_brutas": int(br_count.get(ym, 0)),
                    "qtd_devolucoes": int(dev_count.get(ym, 0)),
                    "qtd_fiscais": int(fis_count.get(ym, 0)),
                }
            )

    return CoberturaDevolucoes(
        total_brutas=int(len(df_brutas)),
        total_devolucoes_no_arquivo=int(len(devolucoes)),
        total_fiscalmente_validas=int(len(fiscais)),
        cobertura_mensal=cobertura_mensal,
        excluidas_por_status=excluidas,
        sem_data=sem_data,
    )


def detectar_meses_suspeitos(cobertura_mensal: list[dict[str, Any]]) -> list[str]:
    """
    Retorna ano-mês de meses com qtd_devolucoes == 0 quando vizinhos
    (anterior ou posterior cronológico) têm devoluções > 0.

    Use como sinal de alerta — não é confirmação de problema,
    e empresas podem legitimamente ter meses sem devolução.
    """
    if not cobertura_mensal:
        return []
    rows = sorted(cobertura_mensal, key=lambda x: str(x.get("ano_mes", "")))
    out: list[str] = []
    for i, row in enumerate(rows):
        cur = int(row.get("qtd_devolucoes", 0) or 0)
        if cur != 0:
            continue
        prev_v = int(rows[i - 1].get("qtd_devolucoes", 0) or 0) if i > 0 else 0
        next_v = int(rows[i + 1].get("qtd_devolucoes", 0) or 0) if i + 1 < len(rows) else 0
        if prev_v > 0 or next_v > 0:
            ym = str(row.get("ano_mes", "")).strip()
            if ym:
                out.append(ym)
    return out
