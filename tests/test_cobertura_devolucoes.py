"""Auditoria de cobertura bruta vs fiscal e meses suspeitos."""

from __future__ import annotations

import pandas as pd

from processing.faturamento.cobertura_devolucoes import (
    CoberturaDevolucoes,
    auditar_cobertura,
    detectar_meses_suspeitos,
)


def test_excluidas_por_status_separa_cancelada_do_total_fiscal() -> None:
    df = pd.DataFrame(
        {
            "Natureza": [
                "Entrada de Devolução",
                "Entrada de Devolução",
                "Entrada de Devolução",
            ],
            "Situação": ["Autorizada", "Emitida DANFE", "Cancelada"],
            "Data de emissão": pd.to_datetime(
                ["2026-02-01", "2026-02-02", "2026-02-03"], errors="coerce"
            ),
        }
    )
    cov = auditar_cobertura(df)
    assert cov.total_devolucoes_no_arquivo == 3
    assert cov.total_fiscalmente_validas == 2
    exc = cov.excluidas_por_status
    assert len(exc) >= 1
    cancel_row = next((r for r in exc if str(r.get("situacao", "")).strip() == "Cancelada"), None)
    assert cancel_row is not None
    assert int(cancel_row["qtd"]) == 1


def test_cobertura_mensal_agrupa_ano_mes() -> None:
    df = pd.DataFrame(
        {
            "Natureza": ["Entrada de Devolução"] * 4,
            "Situação": ["Autorizada"] * 4,
            "Data de emissão": pd.to_datetime(
                ["2026-01-05", "2026-01-20", "2026-02-01", "2026-03-10"], errors="coerce"
            ),
        }
    )
    cov = auditar_cobertura(df)
    cm = {row["ano_mes"]: row for row in cov.cobertura_mensal}
    # ``qtd_brutas`` = linhas brutas por mês (todas), não total do ficheiro.
    assert cm["2026-01"]["qtd_brutas"] == 2
    assert cm["2026-01"]["qtd_devolucoes"] == 2
    assert cm["2026-01"]["qtd_fiscais"] == 2
    assert cm["2026-02"]["qtd_devolucoes"] == 1
    assert cm["2026-03"]["qtd_devolucoes"] == 1


def test_sem_data_conta_e_so_metadata_se_positivo() -> None:
    df = pd.DataFrame(
        {
            "Natureza": ["Entrada de Devolução", "Entrada de Devolução"],
            "Situação": ["Autorizada", "Autorizada"],
            "Data de emissão": [pd.NaT, pd.Timestamp("2026-02-01")],
        }
    )
    cov = auditar_cobertura(df)
    assert cov.sem_data == 1
    meta = cov.to_metadata()
    assert "sem_data" in meta
    assert meta["sem_data"] == 1

    df_ok = pd.DataFrame(
        {
            "Natureza": ["Entrada de Devolução"],
            "Situação": ["Autorizada"],
            "Data de emissão": [pd.Timestamp("2026-02-01")],
        }
    )
    cov0 = auditar_cobertura(df_ok)
    assert cov0.sem_data == 0
    assert "sem_data" not in cov0.to_metadata()


def test_detectar_meses_suspeitos_mes_zero_entre_ativos() -> None:
    cobertura_mensal = [
        {"ano_mes": "2026-01", "qtd_brutas": 10, "qtd_devolucoes": 5, "qtd_fiscais": 5},
        {"ano_mes": "2026-02", "qtd_brutas": 8, "qtd_devolucoes": 0, "qtd_fiscais": 0},
        {"ano_mes": "2026-03", "qtd_brutas": 10, "qtd_devolucoes": 4, "qtd_fiscais": 4},
    ]
    got = detectar_meses_suspeitos(cobertura_mensal)
    assert "2026-02" in got


def test_detectar_meses_suspeitos_vazio_sem_padrao() -> None:
    assert detectar_meses_suspeitos([]) == []
    cobertura_mensal = [
        {"ano_mes": "2026-01", "qtd_devolucoes": 1},
        {"ano_mes": "2026-02", "qtd_devolucoes": 2},
    ]
    assert detectar_meses_suspeitos(cobertura_mensal) == []


def test_auditar_dataframe_totalmente_vazio() -> None:
    cov = auditar_cobertura(pd.DataFrame())
    assert cov.total_brutas == 0
    assert cov.total_devolucoes_no_arquivo == 0


def test_bruto_sem_devolucoes_retorna_dataclass_zeros() -> None:
    df = pd.DataFrame(
        {
            "Natureza": ["Venda", "Serviço"],
            "Situação": ["Autorizada", "Autorizada"],
            "Data de emissão": pd.to_datetime(["2026-01-01", "2026-02-01"]),
        }
    )
    cov = auditar_cobertura(df)
    assert isinstance(cov, CoberturaDevolucoes)
    assert cov.total_brutas == 2
    assert cov.total_devolucoes_no_arquivo == 0
    assert cov.total_fiscalmente_validas == 0
    assert cov.excluidas_por_status == []
