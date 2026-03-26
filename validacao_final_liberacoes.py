from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _id_pedido_linha(df: pd.DataFrame) -> pd.Series:
    # Mantém a mesma modelagem por pedido adotada anteriormente.
    order_id = _norm(df["ORDER_ID"])
    ext_ref = _norm(df["EXTERNAL_REFERENCE"])
    pack_id = _norm(df["PACK_ID"])
    return order_id.where(order_id.ne(""), ext_ref.where(ext_ref.ne(""), pack_id))


def build_validacao_final_liberacoes(base_dir: str | Path) -> pd.DataFrame:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    vendas_ids = set(_norm(vendas_tratadas["N° de venda"]))

    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = _norm(lib["EXTERNAL_REFERENCE"])
    lib["PACK_ID"] = _norm(lib["PACK_ID"])
    lib["ORDER_ID"] = _norm(lib["ORDER_ID"])
    lib["ID do pedido"] = _id_pedido_linha(lib)

    # Apenas liberações válidas
    validas = lib[(lib["EXTERNAL_REFERENCE"].ne("")) | (lib["PACK_ID"].ne(""))].copy()

    # Match com fallback: EXTERNAL_REFERENCE -> PACK_ID
    n_venda = pd.Series(pd.NA, index=validas.index, dtype="object")
    m_ext = validas["EXTERNAL_REFERENCE"].isin(vendas_ids) & validas["EXTERNAL_REFERENCE"].ne("")
    n_venda.loc[m_ext] = validas.loc[m_ext, "EXTERNAL_REFERENCE"]

    pend = n_venda.isna()
    m_pack = pend & validas["PACK_ID"].isin(vendas_ids) & validas["PACK_ID"].ne("")
    n_venda.loc[m_pack] = validas.loc[m_pack, "PACK_ID"]

    out = validas[
        ["ID do pedido", "EXTERNAL_REFERENCE", "PACK_ID", "Valor pago"]
    ].copy()
    out["N° de venda"] = n_venda
    out["Match sucesso"] = out["N° de venda"].notna().map({True: "Sim", False: "Não"})
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    validacao_final_liberacoes = build_validacao_final_liberacoes(BASE_DIR)

    total = int(len(validacao_final_liberacoes))
    com_venda = int(validacao_final_liberacoes["Match sucesso"].eq("Sim").sum())
    sem_venda = int(validacao_final_liberacoes["Match sucesso"].eq("Não").sum())
    cobertura = (com_venda / total * 100.0) if total else 0.0

    valor = pd.to_numeric(validacao_final_liberacoes["Valor pago"], errors="coerce")
    valor_com = float(valor[validacao_final_liberacoes["Match sucesso"].eq("Sim")].sum())
    valor_sem = float(valor[validacao_final_liberacoes["Match sucesso"].eq("Não")].sum())
    valor_total = float(valor.sum())
    cobertura_fin = (valor_com / valor_total * 100.0) if valor_total else 0.0

    print("=== VALIDAÇÃO FINAL DAS LIBERAÇÕES (base: liberações válidas) ===")
    print(f"Total de liberações válidas: {total}")
    print(f"Liberações com venda: {com_venda}")
    print(f"Liberações sem venda: {sem_venda}")
    print(f"Percentual de cobertura: {cobertura:.2f}%")

    print("\nSoma de Valor pago:")
    print(f"- Com venda: {valor_com:.2f}")
    print(f"- Sem venda: {valor_sem:.2f}")
    print(f"- Total: {valor_total:.2f}")
    print(f"Percentual financeiro coberto: {cobertura_fin:.2f}%")

    if cobertura < 90.0 or cobertura_fin < 90.0:
        sem = validacao_final_liberacoes[validacao_final_liberacoes["Match sucesso"].eq("Não")].copy()
        print("\nCobertura abaixo do esperado: investigando casos sem venda (amostra 20).")
        print(sem.head(20).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

