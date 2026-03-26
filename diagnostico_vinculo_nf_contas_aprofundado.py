from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

import pandas as pd

from integracao_notas_pedidos import BASE_DIR, _carregar_notas_saida


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
    raise RuntimeError(f"Falha ao ler contas: {path} ({last_err})")


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


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _to_num_br(s: pd.Series) -> pd.Series:
    x = _norm(s)
    x = x.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    x = x.str.replace(r"[^0-9\.-]", "", regex=True)
    return pd.to_numeric(x, errors="coerce")


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    notas = _carregar_notas_saida()
    contas = _load_contas()

    print("=== DIAGNÓSTICO APROFUNDADO: NF -> CONTAS A RECEBER ===")
    print("\n[1] Colunas disponíveis no contas a receber")
    print(", ".join(contas.columns) if not contas.empty else "Sem dados")

    if notas.empty or contas.empty:
        print("\nDados insuficientes para diagnóstico.")
        return 0

    # procura colunas com indício de referência à NF/origem/documento
    cand_nf = []
    for c in contas.columns:
        n = c.lower()
        if any(k in n for k in ["nota", "nf", "documento", "origem", "refer", "hist", "observ", "obs"]):
            cand_nf.append(c)
    print("\n[2] Colunas no contas que podem representar vínculo com NF/origem")
    print(", ".join(cand_nf) if cand_nf else "Nenhuma coluna com indício explícito encontrada")

    # Tipo
    col_tipo = "Tipo" if "Tipo" in contas.columns else ""
    if col_tipo:
        tipo_vc = _norm(contas[col_tipo]).value_counts(dropna=False).head(20).reset_index()
        tipo_vc.columns = ["Tipo", "Quantidade"]
        print("\n[3] Distribuição da coluna Tipo (contas)")
        print(tipo_vc.to_string(index=False))
    else:
        print("\n[3] Coluna 'Tipo' não encontrada.")

    # Amostras reais solicitadas
    col_nf_num = "Número" if "Número" in notas.columns else ""
    col_nf_val = "Valor total" if "Valor total" in notas.columns else ""
    col_nf_data = "Data de emissão" if "Data de emissão" in notas.columns else ""

    col_ct_num = "Número" if "Número" in contas.columns else ""
    col_ct_val = "Valor" if "Valor" in contas.columns else ""
    col_ct_data = "Data" if "Data" in contas.columns else ""

    print("\n[4] 20 notas fiscais (número, valor, data)")
    nf_show = notas[[c for c in [col_nf_num, col_nf_val, col_nf_data] if c]].head(20)
    print(nf_show.to_string(index=False))

    print("\n[5] 20 títulos de contas a receber (número, valor, data)")
    ct_show = contas[[c for c in [col_ct_num, col_ct_val, col_ct_data] if c]].head(20)
    print(ct_show.to_string(index=False))

    # padrão de número (título/parcela)
    num_ct = _norm(contas[col_ct_num]) if col_ct_num else pd.Series(dtype=str)
    parcela_like = num_ct.str.contains(r"/\d+$", regex=True).sum() if len(num_ct) else 0
    pct_parcela_like = (parcela_like / len(num_ct) * 100.0) if len(num_ct) else 0.0

    # teste indireto por valor (exato e aproximado) e data
    nf_val = _to_num_br(notas[col_nf_val]) if col_nf_val else pd.Series(dtype=float)
    ct_val = _to_num_br(contas[col_ct_val]) if col_ct_val else pd.Series(dtype=float)
    set_ct_exact = set(ct_val.dropna().round(2).tolist())
    exact_val_matches = int(nf_val.dropna().round(2).isin(set_ct_exact).sum()) if len(nf_val) else 0
    pct_exact = (exact_val_matches / nf_val.dropna().shape[0] * 100.0) if nf_val.dropna().shape[0] else 0.0

    # faixa +-0.05
    ct_vals_sorted = sorted(set(ct_val.dropna().round(2).tolist()))
    approx = 0
    for v in nf_val.dropna().round(2).tolist():
        if any(abs(v - x) <= 0.05 for x in ct_vals_sorted):
            approx += 1
    pct_approx = (approx / nf_val.dropna().shape[0] * 100.0) if nf_val.dropna().shape[0] else 0.0

    # datas
    nf_dt = pd.to_datetime(notas[col_nf_data], errors="coerce", dayfirst=True) if col_nf_data else pd.Series(dtype="datetime64[ns]")
    ct_dt = pd.to_datetime(contas[col_ct_data], errors="coerce", dayfirst=True) if col_ct_data else pd.Series(dtype="datetime64[ns]")
    nf_day = set(nf_dt.dropna().dt.date.tolist())
    ct_day = set(ct_dt.dropna().dt.date.tolist())
    same_day = len(nf_day & ct_day)

    print("\n[6] Padrões indiretos de possível vínculo")
    print(f"- % de títulos com padrão parcela no Número (ex.: 12345/01): {pct_parcela_like:.2f}%")
    print(f"- Match por valor exato (NF valor total vs contas valor): {exact_val_matches}/{nf_val.dropna().shape[0]} ({pct_exact:.2f}%)")
    print(f"- Match por valor aproximado (+/-0,05): {approx}/{nf_val.dropna().shape[0]} ({pct_approx:.2f}%)")
    print(f"- Dias em comum entre datas de emissão NF e datas de contas: {same_day} dias")

    print("\n[7] Conclusão objetiva")
    if not cand_nf and pct_parcela_like > 70:
        print("- Não existe chave direta de NF no layout atual de contas a receber.")
        print("- A coluna 'Número' representa título/parcela (não número da NF).")
        print("- Será necessário outro relatório/export do Bling com referência explícita de NF (ex.: número da NF, documento de origem).")
    else:
        print("- Há indícios de possíveis colunas de referência; necessário validar com amostra manual.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

