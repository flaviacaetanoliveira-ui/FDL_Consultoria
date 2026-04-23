"""Helpers partilhados por testes fiscais / params V2 (sem código de produção)."""

from __future__ import annotations

from pathlib import Path

from processing.faturamento.params import EmpresaFaturamentoEntry, FaturamentoParamsV2

_REPO = Path(__file__).resolve().parents[1]


def v2_min_params(
    empresas: tuple[EmpresaFaturamentoEntry, ...],
    default_ali: float = 0.11,
) -> FaturamentoParamsV2:
    r = _REPO
    return FaturamentoParamsV2(
        cliente_root=r,
        cliente_slug="t",
        custo_xlsx_resolved=r / "ops" / "faturamento_params_cliente_2_gama_star_eap.json",
        empresas=empresas,
        aliquota_imposto=default_ali,
        aliquota_despesas_fixas=0.05,
        permite_faturamento_sem_nf_default=False,
        coluna_base_imposto=("Base fiscal item",),
        params_mensais_resolved=None,
        notas_saida_dir="notas_saida",
        notas_entrada_dir=None,
        nf_panel_ads=True,
    )
