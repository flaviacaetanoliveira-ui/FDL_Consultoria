from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

import pandas as pd

from integracao_notas_pedidos import BASE_DIR, _carregar_notas_saida, _norm


PASTA_CONTAS = BASE_DIR / "contas_receber"


def _read_contas(path: Path) -> pd.DataFrame:
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
    raise RuntimeError(f"Falha ao ler contas a receber: {path} ({last_err})")


def _load_contas() -> pd.DataFrame:
    files: list[Path] = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in PASTA_CONTAS.glob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    parts = []
    for f in files:
        d = _read_contas(f).dropna(axis=1, how="all").copy()
        d["__arquivo__"] = f.name
        parts.append(d)
    return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()


def _clean_b(x: str) -> str:
    # trim + texto + remove .0 + remove zeros à esquerda
    s = str(x or "").strip()
    s = re.sub(r"\.0+$", "", s)
    s = s.lstrip("0")
    return s


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    notas = _carregar_notas_saida()
    contas = _load_contas()

    col_nf_num = "Número"
    col_contas_num = "Número" if "Número" in contas.columns else ""

    if notas.empty or contas.empty or not col_contas_num:
        print("Dados insuficientes para diagnóstico (notas/contas/coluna Número).")
        return 0

    nf_num = _norm(notas[col_nf_num]) if col_nf_num in notas.columns else pd.Series(dtype=str)
    nf_num = nf_num[nf_num.ne("")].drop_duplicates().reset_index(drop=True)

    c_num = _norm(contas[col_contas_num])
    c_num = c_num[c_num.ne("")].drop_duplicates().reset_index(drop=True)

    print("=== DIAGNÓSTICO DE CHAVE NF -> CONTAS A RECEBER ===")
    print("\n[1] Amostras reais")
    print("- 20 valores de Número da nota (NF):")
    print(nf_num.head(20).to_string(index=False))
    print("\n- 20 valores de Número (Contas a receber):")
    print(c_num.head(20).to_string(index=False))

    # [2] formato
    def fmt_stats(s: pd.Series) -> dict[str, int]:
        sx = s.astype(str)
        return {
            "total": int(len(sx)),
            "com_espaco": int(sx.str.contains(r"^\s|\s$", regex=True).sum()),
            "com_ponto_zero": int(sx.str.contains(r"\.0+$", regex=True).sum()),
            "com_zero_esq": int(sx.str.contains(r"^0+\d+$", regex=True).sum()),
            "somente_digitos": int(sx.str.fullmatch(r"\d+").sum()),
            "alfa_numerico": int(sx.str.contains(r"[A-Za-z]").sum()),
            "com_especiais": int(sx.str.contains(r"[^A-Za-z0-9]", regex=True).sum()),
        }

    f_nf = fmt_stats(nf_num)
    f_ct = fmt_stats(c_num)
    print("\n[2] Comparação de formato")
    print(f"- NF: {f_nf}")
    print(f"- Contas: {f_ct}")

    # [3] testes de match
    set_nf_a = set(nf_num.tolist())
    set_ct_a = set(c_num.tolist())
    m_a = len(set_nf_a & set_ct_a)

    nf_b = nf_num.map(_clean_b)
    ct_b = c_num.map(_clean_b)
    set_nf_b = set(nf_b[nf_b.ne("")].tolist())
    set_ct_b = set(ct_b[ct_b.ne("")].tolist())
    m_b = len(set_nf_b & set_ct_b)

    # Teste C: inferir se Número contas é número de título (não NF)
    # Heurística: se contas tem maioria de padrões com separadores "/" "-" ou alfanuméricos de marketplace
    pct_title_like = (
        c_num.astype(str).str.contains(r"[A-Za-z]|/|-", regex=True).mean() * 100.0
        if len(c_num)
        else 0.0
    )

    print("\n[3] Testes de match em camadas")
    print(f"- Teste A (exato original): {m_a} matches")
    print(f"- Teste B (normalizado trim/texto/.0/zero-esq): {m_b} matches")
    print(
        f"- Teste C (heurística de 'Número' parecer título e não NF): "
        f"{pct_title_like:.2f}% com padrão alfanumérico/símbolos"
    )

    # exemplos com/sem match no melhor teste entre A e B
    use_b = m_b >= m_a
    if use_b:
        nf_cmp = nf_b
        ct_cmp_set = set_ct_b
        tag = "B"
    else:
        nf_cmp = nf_num
        ct_cmp_set = set_ct_a
        tag = "A"

    aux = pd.DataFrame({"numero_nf_original": nf_num, "numero_nf_cmp": nf_cmp})
    aux["match"] = aux["numero_nf_cmp"].isin(ct_cmp_set)
    ex_match = aux[aux["match"]].head(20)
    ex_sem = aux[~aux["match"]].head(20)

    print(f"\n[4] Exemplos (Teste {tag} - melhor cobertura)")
    print("- 20 com match:")
    print(ex_match.to_string(index=False))
    print("\n- 20 sem match:")
    print(ex_sem.to_string(index=False))

    # [5] outras colunas plausíveis em contas
    cand = []
    for c in contas.columns:
        n = c.lower()
        if any(k in n for k in ["nota", "documento", "origem", "hist", "obs", "pedido", "numero"]):
            cand.append(c)
    print("\n[5] Outras colunas plausíveis em contas para vínculo")
    print(", ".join(cand))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

