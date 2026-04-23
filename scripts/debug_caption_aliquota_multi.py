"""
Reproduz cenário: 4 empresas com alíquotas 9%, 9.5%, 11%, 11%.
Confirmar que aliquota_configurada_para_empresas_filtradas retorna modo='multipla'
e que a caption gerada contém 'múltiplas'.
"""

from __future__ import annotations

from app.components.apuracao_fiscal_panel import _aliquota_imposto_caption_safe_html_and_divergencia_ref
from processing.faturamento.params_regime import (
    aliquota_configurada_para_empresas_filtradas,
    load_faturamento_params_for_ui,
)

_FAT_PATH = "data_products/cliente_2/faturamento/current/dataset_faturamento_app.csv"


def _load_info() -> dict[str, object]:
    return {
        "cliente_slug": "cliente_2",
        "params_path": "ops/faturamento_params_cliente_2_gama_star_eap.json",
        "faturamento_path_final_resolved": _FAT_PATH,
    }


def main() -> None:
    params = load_faturamento_params_for_ui(load_info=_load_info())
    if params is None:
        print("❌ params não carregado")
        return

    todos_slugs = [e.org_id for e in params.empresas]
    print(f"Slugs: {todos_slugs}")

    aliquotas_info = aliquota_configurada_para_empresas_filtradas(params, todos_slugs)
    print(f"\naliquotas_info: {aliquotas_info}")
    print(f"  modo: {aliquotas_info.get('modo')}")
    print(f"  valores_por_empresa: {aliquotas_info.get('valores_por_empresa')}")

    caption_html, divergencia_ref = _aliquota_imposto_caption_safe_html_and_divergencia_ref(
        params_union=params,
        aliquotas_info=aliquotas_info,
        empresas_efetivas=todos_slugs,
        fallback_metadata_pct=11.0,
        ok_nf_dates=True,
    )

    print(f"\ncaption_html gerada: {caption_html}")
    print(f"divergencia_ref: {divergencia_ref}")


if __name__ == "__main__":
    main()
