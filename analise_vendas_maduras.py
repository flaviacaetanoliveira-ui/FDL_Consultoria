from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

from conciliacao_valor_recebido_real import BASE_DIR, build_conciliacao_com_recebido_real
from etapa1_vendas import detect_columns, list_sales_files, normalize_col_name, read_sales_file


PASTA_VENDAS = BASE_DIR / "Vendas - Mercado Livre"
MESES_PT = {
    "janeiro": "01",
    "fevereiro": "02",
    "marco": "03",
    "março": "03",
    "abril": "04",
    "maio": "05",
    "junho": "06",
    "julho": "07",
    "agosto": "08",
    "setembro": "09",
    "outubro": "10",
    "novembro": "11",
    "dezembro": "12",
}


def _detect_sales_date_col(df: pd.DataFrame) -> str:
    best_col = ""
    best_score = 0
    for c in df.columns:
        n = normalize_col_name(c)
        t = set(n.split())
        score = 0
        if "date" in t or "data" in t:
            score += 4
        if "venda" in t or "sale" in t:
            score += 3
        if "order" in t or "pedido" in t:
            score += 2
        if n in {"data da venda", "data venda", "sale date", "date created"}:
            score += 6
        if score > best_score:
            best_score = score
            best_col = c
    return best_col if best_score > 0 else ""


def _build_data_venda_por_venda() -> tuple[pd.DataFrame, pd.DataFrame]:
    files = list_sales_files(PASTA_VENDAS)
    parts: list[pd.DataFrame] = []
    diag: list[dict[str, object]] = []

    for f in files:
        raw = read_sales_file(f).dropna(axis=1, how="all").copy()
        det = detect_columns(raw)
        date_col = _detect_sales_date_col(raw)
        diag.append(
            {
                "Arquivo": f.name,
                "Coluna venda": det.sale_col,
                "Coluna data detectada": date_col or "NÃO ENCONTRADA",
                "Linhas": int(len(raw)),
            }
        )
        if not date_col:
            continue
        tmp = raw[[det.sale_col, date_col]].copy()
        tmp.columns = ["N° de venda", "Data da venda"]
        tmp["N° de venda"] = _normalize_sale_id(tmp["N° de venda"])
        diag[-1]["Amostra data bruta"] = (
            " | ".join(tmp["Data da venda"].astype(str).head(5).tolist())
        )[:200]
        tmp["Data da venda"] = _parse_data_venda(tmp["Data da venda"])
        tmp = tmp[tmp["N° de venda"].ne("") & tmp["Data da venda"].notna()].copy()
        diag[-1]["Linhas com data válida"] = int(len(tmp))
        parts.append(tmp)

    if not parts:
        return (
            pd.DataFrame(columns=["N° de venda", "Data da venda"]),
            pd.DataFrame(diag),
        )

    all_dates = pd.concat(parts, ignore_index=True)
    data_por_venda = (
        all_dates.groupby("N° de venda", as_index=False)["Data da venda"]
        .min()
        .reset_index(drop=True)
    )
    return data_por_venda, pd.DataFrame(diag)


def _normalize_sale_id(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    num = pd.to_numeric(s, errors="coerce")
    out = s.copy()
    mask = num.notna()
    out.loc[mask] = num.loc[mask].round(0).astype("Int64").astype(str)
    out = out.replace("<NA>", "")
    return out


def _parse_data_venda(series: pd.Series) -> pd.Series:
    dt = pd.to_datetime(series, errors="coerce", dayfirst=True, format="mixed")
    if dt.notna().any():
        return dt

    s = series.fillna("").astype(str).str.strip().str.lower()
    s = s.str.replace(" hs.", "", regex=False).str.replace(" hs", "", regex=False)
    s = s.str.replace(r"\s+", " ", regex=True)

    parsed = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    m = s.str.extract(
        r"(?P<dia>\d{1,2}) de (?P<mes>[a-zç]+) de (?P<ano>\d{4}) (?P<h>\d{1,2}):(?P<mi>\d{2})"
    )
    ok = m["dia"].notna() & m["mes"].notna() & m["ano"].notna() & m["h"].notna() & m["mi"].notna()
    if ok.any():
        mx = m.loc[ok].copy()
        mx["mes_num"] = mx["mes"].map(MESES_PT)
        mx = mx[mx["mes_num"].notna()]
        if not mx.empty:
            vals = (
                mx["ano"]
                + "-"
                + mx["mes_num"]
                + "-"
                + mx["dia"].str.zfill(2)
                + " "
                + mx["h"].str.zfill(2)
                + ":"
                + mx["mi"].str.zfill(2)
            )
            parsed.loc[mx.index] = pd.to_datetime(vals, errors="coerce")
    return parsed


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_com_recebido_real(BASE_DIR)
    conc["N° de venda"] = _normalize_sale_id(conc["N° de venda"])
    data_venda_map, diag = _build_data_venda_por_venda()

    conc = conc.merge(data_venda_map, how="left", on="N° de venda")

    hoje = pd.Timestamp.now().normalize()
    cutoff = hoje - pd.Timedelta(days=15)
    mask_maduras = conc["Data da venda"].notna() & (conc["Data da venda"] <= cutoff)
    conciliacao_vendas_maduras = conc.loc[mask_maduras].copy()

    total_vendido_maduras = float(
        pd.to_numeric(conciliacao_vendas_maduras["Total BRL"], errors="coerce").sum()
    )
    total_recebido_real_maduras = float(
        pd.to_numeric(conciliacao_vendas_maduras["Valor recebido real"], errors="coerce").sum()
    )
    diferenca_ajustada = total_vendido_maduras - total_recebido_real_maduras
    pct_recebimento = (
        total_recebido_real_maduras / total_vendido_maduras * 100.0 if total_vendido_maduras else 0.0
    )

    print("=== ANÁLISE DE VENDAS MADURAS ===")
    print(f"Data de corte de maturidade: {cutoff.date()} (vendas com mais de 15 dias)")
    print("\nDiagnóstico da Data da venda (melhor coluna disponível):")
    print(diag.to_string(index=False))

    print(f"\nTotal com Data da venda preenchida: {int(conc['Data da venda'].notna().sum())}")
    print(f"Total de linhas em conciliacao_vendas_maduras: {len(conciliacao_vendas_maduras)}")
    print(f"Total vendido (maturas): {total_vendido_maduras:.2f}")
    print(f"Total recebido real (maturas): {total_recebido_real_maduras:.2f}")
    print(f"Diferença ajustada (maturas): {diferenca_ajustada:.2f}")
    print(f"Percentual de recebimento sobre vendas maduras: {pct_recebimento:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

