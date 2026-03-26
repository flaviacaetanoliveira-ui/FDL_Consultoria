from __future__ import annotations

import sys

import pandas as pd

from etapa2_liberacoes import PASTA_LIBERACOES, build_liberacoes_from_folder
from etapa4b_integracao_contas_receber import BASE_DIR, carregar_tabela_final_operacional


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    # Origem da data: ETAPA 2 (já padronizada a partir de DATE)
    liberacoes_tratadas, _, _ = build_liberacoes_from_folder(PASTA_LIBERACOES)
    src_dtype = str(liberacoes_tratadas["Data de pagamento"].dtype) if "Data de pagamento" in liberacoes_tratadas.columns else "ausente"
    src_notnull = int(liberacoes_tratadas["Data de pagamento"].notna().sum()) if "Data de pagamento" in liberacoes_tratadas.columns else 0

    final, _ = carregar_tabela_final_operacional(BASE_DIR)
    total = int(len(final))

    valor_pago = pd.to_numeric(final.get("Valor pago"), errors="coerce")
    data_pag = final.get("Data de pagamento", pd.Series([""] * total))
    data_pag = data_pag.fillna("").astype(str).str.strip()
    data_preenchida = data_pag.ne("")

    qtd_valor_pago = int(valor_pago.notna().sum())
    qtd_data = int(data_preenchida.sum())

    # consistência pedida: se tem Valor pago, deve ter Data de pagamento
    inconsistentes = final[valor_pago.notna() & ~data_preenchida].copy()
    qtd_inconsist = int(len(inconsistentes))

    exemplos = final[data_preenchida][
        ["N° de venda", "ID do pedido", "Valor pago", "Data de pagamento", "Ação sugerida"]
    ].head(10)

    print("=== VALIDAÇÃO FINAL — DATA DE PAGAMENTO (MERCADO LIVRE) ===")
    print("\n[1] Origem real da Data de pagamento")
    print("- Base origem: liberações (etapa2_liberacoes)")
    print("- Coluna de origem validada no pipeline: DATE -> Data de pagamento")
    print(f"- dtype na origem (liberacoes_tratadas): {src_dtype}")
    print(f"- Linhas com data preenchida na origem: {src_notnull}")

    print("\n[2] Validação da tabela final operacional")
    print(f"- Quantidade total de linhas: {total}")
    print(f"- Quantidade com Valor pago preenchido: {qtd_valor_pago}")
    print(f"- Quantidade com Data de pagamento preenchida: {qtd_data}")
    print(f"- Quantidade de Data de pagamento nula/vazia: {total - qtd_data}")

    print("\n[3] Consistência (se tem Valor pago, deve ter Data de pagamento)")
    print(f"- Inconsistências encontradas: {qtd_inconsist}")

    print("\n[4] 10 exemplos reais com Data de pagamento")
    if exemplos.empty:
        print("Nenhum exemplo com data preenchida na tabela final.")
    else:
        print(exemplos.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

