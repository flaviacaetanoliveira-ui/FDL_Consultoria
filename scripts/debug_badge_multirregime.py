"""
Reproduz o cenário visual: Apuração Fiscal · Todas empresas · 01/01-17/04/2026.
Objetivo: confirmar se detectar_regimes_tributarios retorna corretamente
empresas_fora_escopo contendo Mega Fácil quando filtro é "Todas".
"""

from __future__ import annotations

from processing.faturamento.params_regime import (
    detectar_regimes_tributarios,
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

    print(f"Params carregado: {params is not None}")
    if params is None:
        print("❌ params é None — falha na carga")
        return

    print(f"Empresas no params: {[e.empresa for e in params.empresas]}")

    print("\n=== CENÁRIO 1: filtro 'Todas' como lista vazia ===")
    info_vazio = detectar_regimes_tributarios(params, [])
    print(f"  regimes_presentes: {info_vazio.get('regimes_presentes')}")
    print(f"  tem_regime_fora_escopo: {info_vazio.get('tem_regime_fora_escopo')}")
    print(f"  empresas_fora_escopo: {info_vazio.get('empresas_fora_escopo')}")

    print("\n=== CENÁRIO 2: filtro 'Todas' expandido ===")
    todos_slugs = [e.org_id for e in params.empresas]
    print(f"  slugs passados: {todos_slugs}")
    info_todos = detectar_regimes_tributarios(params, todos_slugs)
    print(f"  regimes_presentes: {info_todos.get('regimes_presentes')}")
    print(f"  tem_regime_fora_escopo: {info_todos.get('tem_regime_fora_escopo')}")
    print(f"  empresas_fora_escopo: {info_todos.get('empresas_fora_escopo')}")

    print("\n=== CENÁRIO 3: só Mega Fácil ===")
    info_mf = detectar_regimes_tributarios(params, ["mega_facil"])
    print(f"  tem_regime_fora_escopo: {info_mf.get('tem_regime_fora_escopo')}")
    print(f"  empresas_fora_escopo: {info_mf.get('empresas_fora_escopo')}")


if __name__ == "__main__":
    main()
