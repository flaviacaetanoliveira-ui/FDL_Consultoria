from __future__ import annotations

import csv
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

from etapa2_liberacoes import (
    PASTA_LIBERACOES,
    build_liberacoes_from_folder,
    normalize_col_name,
    read_input_file,
)


from fdl_paths import BASE_DIR

CLIENTE_BASE = BASE_DIR
PASTA_NOTAS = CLIENTE_BASE / "notas_saida"


def _list_files(folder: Path) -> list[Path]:
    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in folder.glob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return files


def _read_notas(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)

    last_err: Optional[Exception] = None
    for enc in ("utf-8-sig", "utf-8", "latin1", "cp1252"):
        for sep in (";", ",", "\t", "|"):
            try:
                return pd.read_csv(path, encoding=enc, sep=sep, engine="python", dtype=str)
            except Exception as e:  # noqa: BLE001
                last_err = e
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                sep=";",
                engine="python",
                dtype=str,
                on_bad_lines="skip",
                quoting=csv.QUOTE_NONE,
            )
        except Exception as e:  # noqa: BLE001
            last_err = e
    raise RuntimeError(f"Falha ao ler notas: {path} ({last_err})")


def _top_values(df: pd.DataFrame, col: str, n: int = 20) -> pd.DataFrame:
    s = df[col].fillna("").astype(str).str.strip()
    s = s[s.ne("")]
    if s.empty:
        return pd.DataFrame(columns=[col, "qtd"])
    vc = s.value_counts().head(n).rename_axis(col).reset_index(name="qtd")
    return vc


def _first_existing_col(df: pd.DataFrame, candidates_norm: set[str]) -> str:
    norm = {c: normalize_col_name(c) for c in df.columns}
    for original, n in norm.items():
        if n in candidates_norm:
            return original
    return ""


def _id_pedido_liberacoes(df: pd.DataFrame) -> pd.Series:
    # mesma regra da modelagem por pedido adotada
    def n(c: str) -> pd.Series:
        return df[c].fillna("").astype(str).str.strip()

    order_id = n("ORDER_ID")
    ext = n("EXTERNAL_REFERENCE")
    pack = n("PACK_ID")
    return order_id.where(order_id.ne(""), ext.where(ext.ne(""), pack))


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    # ========= PONTO 1 =========
    files_lib = _list_files(PASTA_LIBERACOES)
    _, _, diag_lib = build_liberacoes_from_folder(PASTA_LIBERACOES)
    latest_lib = files_lib[0]
    df_lib_raw_latest = read_input_file(latest_lib)
    df_lib_latest = df_lib_raw_latest.dropna(axis=1, how="all").copy()
    norm_cols_lib = {c: normalize_col_name(c) for c in df_lib_latest.columns}

    # Colunas relevantes do extrato ML (quando existirem)
    col_valor_usado = "SELLER_AMOUNT" if "SELLER_AMOUNT" in df_lib_latest.columns else ""
    col_tipo = _first_existing_col(df_lib_latest, {"record type", "record_type", "tipo", "tipo de registro"})
    col_desc = _first_existing_col(
        df_lib_latest, {"description", "descricao", "descricao", "natureza", "evento"}
    )
    col_saldo = _first_existing_col(
        df_lib_latest, {"balance amount", "saldo", "saldo final", "saldo disponivel"}
    )
    col_gross = _first_existing_col(df_lib_latest, {"gross amount", "valor bruto", "bruto"})
    col_net_credit = _first_existing_col(df_lib_latest, {"net credit amount"})
    col_net_debit = _first_existing_col(df_lib_latest, {"net debit amount"})
    col_fee = _first_existing_col(df_lib_latest, {"mp fee amount", "financing fee amount", "shipping fee amount"})

    print("=== PONTO 1 — LIBERAÇÕES VÁLIDAS (DIAGNÓSTICO) ===")
    print("Arquivos de liberações analisados:")
    for _, r in diag_lib.iterrows():
        print(f"- {r['Arquivo']} | linhas brutas: {int(r['Linhas brutas'])}")

    print(f"\nArquivo de referência para dicionário de colunas: {latest_lib.name}")
    print("Colunas encontradas (brutas):")
    print(", ".join(df_lib_latest.columns))

    print("\nMapeamento de colunas relevantes (se presentes):")
    print(f"- Valor usado hoje como 'Valor pago': {col_valor_usado or 'NÃO ENCONTRADA'}")
    print(f"- Tipo de movimentação: {col_tipo or 'NÃO ENCONTRADA'}")
    print(f"- Descrição/natureza: {col_desc or 'NÃO ENCONTRADA'}")
    print(f"- Saldo/ajuste: {col_saldo or 'NÃO ENCONTRADA'}")
    print(f"- Valor bruto: {col_gross or 'NÃO ENCONTRADA'}")
    print(f"- Crédito líquido: {col_net_credit or 'NÃO ENCONTRADA'}")
    print(f"- Débito líquido: {col_net_debit or 'NÃO ENCONTRADA'}")
    print(f"- Taxas/frete: {col_fee or 'NÃO ENCONTRADA'}")

    print("\nPor que SELLER_AMOUNT foi usado no projeto:")
    print("- É a coluna que representa o valor do vendedor no extrato.")
    print("- No layout ML, tende a refletir repasse da operação para o seller.")
    print("- Porém, sem filtro por tipo/descrição, pode incluir eventos não-venda.")

    if col_tipo:
        print(f"\nPrincipais valores distintos em {col_tipo}:")
        print(_top_values(df_lib_latest, col_tipo, n=30).to_string(index=False))
    if col_desc:
        print(f"\nPrincipais valores distintos em {col_desc}:")
        print(_top_values(df_lib_latest, col_desc, n=40).to_string(index=False))

    # ========= PONTO 2 =========
    files_notas = _list_files(PASTA_NOTAS)
    notas_parts: list[pd.DataFrame] = []
    diag_notas: list[dict[str, object]] = []
    for f in files_notas:
        dfn = _read_notas(f).dropna(axis=1, how="all")
        diag_notas.append({"Arquivo": f.name, "Linhas": int(len(dfn))})
        dfn["__origem_arquivo__"] = f.name
        notas_parts.append(dfn)
    notas = pd.concat(notas_parts, ignore_index=True) if notas_parts else pd.DataFrame()

    print("\n=== PONTO 2 — CHAVE LIBERAÇÕES x NOTAS (DIAGNÓSTICO) ===")
    print("Arquivos de notas analisados:")
    for r in diag_notas:
        print(f"- {r['Arquivo']} | linhas: {r['Linhas']}")

    print("\nColunas de liberações (modelo tratado): EXTERNAL_REFERENCE, ORDER_ID, PACK_ID, Data de pagamento, Valor pago")
    print("Colunas de notas encontradas:")
    if notas.empty:
        print("Nenhuma coluna (sem arquivos)")
        return 0
    print(", ".join(notas.columns))

    # detecta coluna de pedido nas notas (priorizando Número do pedido multiloja)
    norm_notas = {c: normalize_col_name(c) for c in notas.columns}
    col_pedido_multiloja = ""
    for c, n in norm_notas.items():
        if n in {"numero do pedido multiloja", "n do pedido multiloja", "pedido multiloja"}:
            col_pedido_multiloja = c
            break
    if not col_pedido_multiloja:
        for c, n in norm_notas.items():
            if "pedido" in n and "multiloja" in n:
                col_pedido_multiloja = c
                break

    col_pedido_nf = ""
    for c, n in norm_notas.items():
        if n in {"numero do pedido", "pedido", "id pedido", "id do pedido"}:
            col_pedido_nf = c
            break

    print(f"\nColuna candidata em notas para ID do pedido (multiloja): {col_pedido_multiloja or 'NÃO ENCONTRADA'}")
    print(f"Coluna alternativa de pedido em notas: {col_pedido_nf or 'NÃO ENCONTRADA'}")

    # Base de liberações válidas para teste
    liberacoes_tratadas, _, _ = build_liberacoes_from_folder(PASTA_LIBERACOES)
    lib = liberacoes_tratadas.copy()
    for c in ("EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID"):
        lib[c] = lib[c].fillna("").astype(str).str.strip()
    lib = lib[(lib["EXTERNAL_REFERENCE"].ne("")) | (lib["PACK_ID"].ne(""))].copy()
    lib["ID_PEDIDO_LIB"] = _id_pedido_liberacoes(lib)
    total_lib_testadas = int(len(lib))

    # conjuntos de notas por chave plausível
    sets_notas: dict[str, set[str]] = {}
    if col_pedido_multiloja:
        sets_notas["Número do pedido multiloja"] = set(
            notas[col_pedido_multiloja].fillna("").astype(str).str.strip()
        )
    if col_pedido_nf:
        sets_notas["Pedido (ou similar)"] = set(
            notas[col_pedido_nf].fillna("").astype(str).str.strip()
        )
    # inclui mais candidatos com "pedido" no nome
    for c, n in norm_notas.items():
        if "pedido" in n and c not in {col_pedido_multiloja, col_pedido_nf}:
            sets_notas[f"Coluna notas: {c}"] = set(notas[c].fillna("").astype(str).str.strip())

    # remove vazio
    for k in list(sets_notas):
        sets_notas[k].discard("")

    # testa chaves de liberações contra cada conjunto de notas
    print(f"\nTotal de linhas de liberações testadas (válidas): {total_lib_testadas}")
    for nome_set, valores_notas in sets_notas.items():
        if not valores_notas:
            continue
        for chave_lib, serie in [
            ("ID do pedido (lib)", lib["ID_PEDIDO_LIB"]),
            ("EXTERNAL_REFERENCE", lib["EXTERNAL_REFERENCE"]),
            ("PACK_ID", lib["PACK_ID"]),
        ]:
            s = serie.fillna("").astype(str).str.strip()
            ok = int((s.isin(valores_notas) & s.ne("")).sum())
            pct = (ok / total_lib_testadas * 100.0) if total_lib_testadas else 0.0
            print(
                f"- Cobertura {chave_lib} -> {nome_set}: {ok}/{total_lib_testadas} ({pct:.2f}%)"
            )

    print("\nConclusão diagnóstica (com base nas colunas disponíveis):")
    print("- Para repasse em liberações, SELLER_AMOUNT é a melhor candidata de valor do seller,")
    print("  mas precisa filtro por tipo/descrição para garantir 'somente repasse de venda'.")
    print("- Para ligação com notas, a melhor hipótese é usar ID do pedido (ORDER_ID/fallback),")
    print("  comparando com 'Número do pedido multiloja' (ou coluna equivalente) nas notas.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

