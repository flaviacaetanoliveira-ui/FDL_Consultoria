from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

from etapa2_liberacoes import PASTA_LIBERACOES, build_liberacoes_from_folder, normalize_col_name, read_input_file
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
    if path.suffix.lower() in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    last_err = None
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


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _id_pedido_resolvido(lib: pd.DataFrame) -> pd.Series:
    order_id = _norm(lib["ORDER_ID"]) if "ORDER_ID" in lib.columns else ""
    ext = _norm(lib["EXTERNAL_REFERENCE"]) if "EXTERNAL_REFERENCE" in lib.columns else ""
    pack = _norm(lib["PACK_ID"]) if "PACK_ID" in lib.columns else ""
    return order_id.where(order_id.ne(""), ext.where(ext.ne(""), pack))


def _pick_nf_columns(notas: pd.DataFrame) -> tuple[str, str, list[str]]:
    norm = {c: normalize_col_name(c) for c in notas.columns}

    # número da nota
    col_num_nf = ""
    for c, n in norm.items():
        if n in {"numero", "numero da nota", "numero nf", "n da nota", "n"}:
            col_num_nf = c
            break
    if not col_num_nf:
        for c, n in norm.items():
            if "numero" in n:
                col_num_nf = c
                break

    # valor da nota
    col_valor_nf = ""
    for c, n in norm.items():
        if n in {"valor total", "valor total liquido", "valor nota", "valor"}:
            col_valor_nf = c
            break
    if not col_valor_nf:
        for c, n in norm.items():
            if "valor" in n and "total" in n:
                col_valor_nf = c
                break

    # colunas candidatas de pedido nas notas
    cand_pedido = [c for c, n in norm.items() if "pedido" in n]
    # prioriza "Número do pedido multiloja"
    cand_pedido = sorted(
        cand_pedido,
        key=lambda c: 0 if "multiloja" in normalize_col_name(c) else 1,
    )
    return col_num_nf, col_valor_nf, cand_pedido


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    # liberações tratadas + raw para inspecionar todas colunas disponíveis
    lib_raw_parts = []
    for f in _list_files(PASTA_LIBERACOES):
        d = read_input_file(f).dropna(axis=1, how="all").copy()
        d["__arquivo__"] = f.name
        lib_raw_parts.append(d)
    liberacoes_raw = pd.concat(lib_raw_parts, ignore_index=True) if lib_raw_parts else pd.DataFrame()
    liberacoes_tratadas, _, _ = build_liberacoes_from_folder(PASTA_LIBERACOES)

    # notas saída
    notas_parts = []
    diag_notas = []
    for f in _list_files(PASTA_NOTAS):
        d = _read_notas(f).dropna(axis=1, how="all").copy()
        d["__arquivo__"] = f.name
        diag_notas.append((f.name, len(d)))
        notas_parts.append(d)
    notas = pd.concat(notas_parts, ignore_index=True) if notas_parts else pd.DataFrame()

    print("=== ETAPA 4A — DIAGNÓSTICO LIBERAÇÕES x NOTAS DE SAÍDA ===")
    print("\n[1] Colunas disponíveis em liberações (raw):")
    print(", ".join(liberacoes_raw.columns))

    print("\n[2] Colunas disponíveis em notas fiscais de saída:")
    print(", ".join(notas.columns))

    col_num_nf, col_valor_nf, cand_pedido_notas = _pick_nf_columns(notas)
    print("\n[3] Colunas de notas candidatas a pedido:")
    print(", ".join(cand_pedido_notas) if cand_pedido_notas else "Nenhuma encontrada")
    print(f"Coluna número da nota detectada: {col_num_nf or 'NÃO ENCONTRADA'}")
    print(f"Coluna valor da nota detectada: {col_valor_nf or 'NÃO ENCONTRADA'}")

    if notas.empty or not cand_pedido_notas:
        print("\nSem colunas suficientes para teste comparativo de chave.")
        return 0

    # base de liberações testadas: válidas (EXTERNAL_REFERENCE OU PACK_ID)
    lib = liberacoes_tratadas.copy()
    for c in ("EXTERNAL_REFERENCE", "ORDER_ID", "PACK_ID"):
        if c not in lib.columns:
            lib[c] = ""
        lib[c] = _norm(lib[c])
    lib = lib[lib["EXTERNAL_REFERENCE"].ne("") | lib["PACK_ID"].ne("")].copy()
    lib["ID do pedido resolvido"] = _id_pedido_resolvido(lib)

    # outras colunas plausíveis em liberações (IDs)
    plausiveis = ["ORDER_ID", "EXTERNAL_REFERENCE", "PACK_ID", "ID do pedido resolvido"]
    for c in liberacoes_raw.columns:
        n = normalize_col_name(c)
        if ("id" in n or "pedido" in n) and c not in plausiveis and c in lib.columns:
            plausiveis.append(c)

    # conjuntos de notas por coluna candidata de pedido
    notas_sets = {}
    for c in cand_pedido_notas:
        s = _norm(notas[c])
        notas_sets[c] = set(s[s.ne("")])

    resultados = []
    detalhes_match = {}
    total = len(lib)
    for chave_lib in plausiveis:
        if chave_lib not in lib.columns:
            continue
        s_lib = _norm(lib[chave_lib])
        best = None
        for chave_nf, set_nf in notas_sets.items():
            m = s_lib.isin(set_nf) & s_lib.ne("")
            qtd = int(m.sum())
            item = {
                "chave_liberacoes": chave_lib,
                "chave_notas": chave_nf,
                "total_testadas": total,
                "com_nota": qtd,
                "sem_nota": int(total - qtd),
                "percentual_cobertura": (qtd / total * 100.0) if total else 0.0,
                "_mask": m,
            }
            if best is None or item["com_nota"] > best["com_nota"]:
                best = item
        if best:
            resultados.append({k: v for k, v in best.items() if k != "_mask"})
            detalhes_match[best["chave_liberacoes"]] = best

    tab = pd.DataFrame(resultados).sort_values("com_nota", ascending=False).reset_index(drop=True)
    print("\n[4] Teste comparativo de chaves plausíveis")
    print(tab.to_string(index=False))

    if tab.empty:
        return 0

    melhor = tab.iloc[0]
    chave_best_lib = str(melhor["chave_liberacoes"])
    chave_best_nf = str(melhor["chave_notas"])
    m_best = detalhes_match[chave_best_lib]["_mask"]

    print(f"\n[5] Melhor chave encontrada: {chave_best_lib} -> {chave_best_nf}")
    print(
        f"Cobertura: {int(m_best.sum())}/{total} "
        f"({(m_best.sum()/total*100 if total else 0):.2f}%)"
    )

    # monta notas lookup para número/valor da nota
    nf_lookup = notas[[chave_best_nf] + ([col_num_nf] if col_num_nf else []) + ([col_valor_nf] if col_valor_nf else [])].copy()
    nf_lookup[chave_best_nf] = _norm(nf_lookup[chave_best_nf])
    nf_lookup = nf_lookup[nf_lookup[chave_best_nf].ne("")].drop_duplicates(subset=[chave_best_nf])

    base_best = lib.copy()
    base_best = base_best.reset_index(drop=True)
    m_best = m_best.reset_index(drop=True)
    base_best["Match sucesso"] = m_best.map({True: "Sim", False: "Não"})
    base_best["__key__"] = _norm(base_best[chave_best_lib])
    base_best = base_best.merge(nf_lookup, how="left", left_on="__key__", right_on=chave_best_nf)

    cols_show = [
        "EXTERNAL_REFERENCE",
        "ORDER_ID",
        "PACK_ID",
        "ID do pedido resolvido",
        "Valor pago",
        "Match sucesso",
    ]
    if col_num_nf:
        cols_show.append(col_num_nf)
    if col_valor_nf and col_valor_nf not in cols_show:
        cols_show.append(col_valor_nf)

    print("\n[6] 20 exemplos reais COM match (melhor chave)")
    print(base_best[base_best["Match sucesso"].eq("Sim")][cols_show].head(20).to_string(index=False))

    print("\n[7] 20 exemplos reais SEM match (melhor chave)")
    print(base_best[base_best["Match sucesso"].eq("Não")][cols_show].head(20).to_string(index=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

