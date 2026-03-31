from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

from etapa3_conciliacao_vendas_liberacoes_validas import (
    BASE_DIR,
    build_conciliacao_vendas_liberacoes_validas,
)
from modelagem_por_pedido import construir_modelagem_por_pedido


def _pasta_notas(base_dir: str | Path | None) -> Path:
    return Path(base_dir or BASE_DIR).resolve() / "notas_saida"


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


def _to_num_br(series: pd.Series) -> pd.Series:
    s = series.fillna("").astype(str).str.strip()
    s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False)
    s = s.str.replace(r"[^0-9\.-]", "", regex=True)
    return pd.to_numeric(s, errors="coerce")


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


def _carregar_notas_saida(base_dir: str | Path | None = None) -> pd.DataFrame:
    pasta_notas = _pasta_notas(base_dir)
    files = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in pasta_notas.rglob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    partes = []
    for f in files:
        df = _read_notas(f).dropna(axis=1, how="all").copy()
        df["__arquivo__"] = f.name
        partes.append(df)
    if not partes:
        return pd.DataFrame()
    return pd.concat(partes, ignore_index=True)


def _inferir_plataforma(notas: pd.DataFrame) -> pd.Series:
    texto = pd.Series("", index=notas.index, dtype="object")
    for col in ("Nome da Loja", "Descrição", "Código"):
        if col in notas.columns:
            texto = (texto + " " + notas[col].fillna("").astype(str)).str.lower()
    plataforma = pd.Series("Não identificado", index=notas.index, dtype="object")
    plataforma[texto.str.contains("mercado livre|ml", regex=True, na=False)] = "Mercado Livre"
    plataforma[texto.str.contains("shopee", regex=True, na=False)] = "Shopee"
    plataforma[texto.str.contains("magalu|magazine", regex=True, na=False)] = "Magalu"
    plataforma[
        ~texto.str.contains("mercado livre|ml|shopee|magalu|magazine", regex=True, na=False)
        & texto.str.strip().ne("")
    ] = "Outros"
    return plataforma


def _filtrar_notas_validas(notas: pd.DataFrame) -> pd.DataFrame:
    if notas.empty:
        return notas
    col_status = ""
    for c in notas.columns:
        n = c.lower().strip()
        if n in {"situação", "situacao", "status"} or "situa" in n or "status" in n:
            col_status = c
            break
    if not col_status:
        return notas

    s = _norm(notas[col_status]).str.lower()
    invalidas = s.str.contains("cancel", na=False) | s.str.contains("deneg", na=False) | s.str.contains(
        "inutil", na=False
    )
    return notas.loc[~invalidas].copy()


def _detectar_col_data_emissao(columns: list[str]) -> str:
    alvos = {
        "data de emissão",
        "data de emissao",
        "data emissão",
        "data emissao",
        "emissão",
        "emissao",
        "data de saida",
        "data saída",
    }
    norm = {c: str(c).strip().lower() for c in columns}
    for c, n in norm.items():
        if n in alvos:
            return c
    for c, n in norm.items():
        if "emiss" in n or "saida" in n:
            return c
    return ""


def build_conciliacao_com_notas(
    filtrar_notas_invalidas: bool = True, base_dir: str | Path | None = None
) -> pd.DataFrame:
    """
    Fluxo obrigatório:
    VENDAS -> LIBERAÇÕES -> NOTAS
    (sem join direto vendas<->notas).
    """
    root = Path(base_dir or BASE_DIR).resolve()
    conc = build_conciliacao_vendas_liberacoes_validas(root).copy()
    model = construir_modelagem_por_pedido(root)
    de_para = model["de_para_venda_pedido"].copy()
    pagamentos_por_pedido = model["pagamentos_por_pedido"].copy()

    conc["N° de venda"] = _norm(conc["N° de venda"])
    de_para["N° de venda"] = _norm(de_para["N° de venda"])
    de_para["ID do pedido"] = _norm(de_para["ID do pedido"])
    pagamentos_por_pedido["ID do pedido"] = _norm(pagamentos_por_pedido["ID do pedido"])

    plataforma = (
        conc.get("Plataforma", pd.Series("Mercado Livre", index=conc.index))
        .fillna("Mercado Livre")
        .astype(str)
        .str.strip()
        .str.lower()
    )
    conc_ml = conc[plataforma.ne("shopee")].copy()
    conc_shopee = conc[plataforma.eq("shopee")].copy()

    # Fluxo original (ML): N° de venda -> ID do pedido (de/para) e valida no universo de pagamentos.
    conc_ml = conc_ml.merge(de_para[["N° de venda", "ID do pedido"]], how="left", on="N° de venda")
    conc_ml["ID do pedido"] = _norm(conc_ml["ID do pedido"])
    base_ml = conc_ml[conc_ml["ID do pedido"].ne("")].copy()
    base_ml = base_ml[base_ml["ID do pedido"].isin(set(pagamentos_por_pedido["ID do pedido"]))].copy()

    # Shopee: mantém linhas mesmo sem de/para ML; usa ID do pedido da própria plataforma.
    if not conc_shopee.empty:
        conc_shopee["ID do pedido"] = _norm(conc_shopee.get("ID do pedido", pd.Series("", index=conc_shopee.index)))
        conc_shopee.loc[conc_shopee["ID do pedido"].eq(""), "ID do pedido"] = conc_shopee["N° de venda"]
        base = pd.concat([base_ml, conc_shopee], ignore_index=True)
    else:
        base = base_ml

    notas = _carregar_notas_saida(root)
    if filtrar_notas_invalidas:
        notas = _filtrar_notas_validas(notas)
    if notas.empty:
        _cols = ["N° de venda", "ID do pedido", "Total BRL", "Valor pago"]
        if "Data de pagamento" in base.columns:
            _cols.append("Data de pagamento")
        if "Plataforma" in base.columns:
            _cols.append("Plataforma")
        out = base[_cols].copy()
        out["Número da nota"] = pd.NA
        out["Valor da nota"] = pd.NA
        out["Status NF"] = "Sem nota"
        if "Plataforma" not in out.columns:
            out["Plataforma"] = "Não identificado"
        else:
            out["Plataforma"] = out["Plataforma"].fillna("Não identificado").astype(str)
        return out

    col_pedido = "Número do pedido multiloja"
    col_num_nf = "Número" if "Número" in notas.columns else None
    col_valor_nf = "Valor total" if "Valor total" in notas.columns else None
    col_data_emissao = _detectar_col_data_emissao(list(notas.columns))

    if col_pedido not in notas.columns:
        raise KeyError("Coluna 'Número do pedido multiloja' não encontrada nas notas.")

    notas[col_pedido] = _norm(notas[col_pedido])
    notas = notas[notas[col_pedido].ne("")].copy()
    notas["Plataforma"] = _inferir_plataforma(notas)
    if col_num_nf is not None:
        notas[col_num_nf] = _norm(notas[col_num_nf])
    if col_valor_nf is not None:
        notas[col_valor_nf] = _to_num_br(notas[col_valor_nf])
    if col_data_emissao:
        # Layout de notas vem em padrão brasileiro (dd/mm/yyyy HH:MM:SS).
        notas[col_data_emissao] = pd.to_datetime(
            notas[col_data_emissao], errors="coerce", dayfirst=True
        )

    # Etapa LIBERAÇÕES -> NOTAS: integra por ID do pedido -> Número do pedido multiloja.
    agg = notas.groupby(col_pedido, as_index=False).agg(
        **{
            "Número da nota": (
                col_num_nf,
                lambda x: " | ".join(sorted({v for v in x if str(v).strip()})) if col_num_nf else "",
            ),
            "Valor da nota": (col_valor_nf, "sum") if col_valor_nf else (col_pedido, lambda _: pd.NA),
            "Plataforma": (
                "Plataforma",
                lambda x: x.value_counts().index[0] if len(x.value_counts()) else "Não identificado",
            ),
            "Data de emissão": (col_data_emissao, "min") if col_data_emissao else (col_pedido, lambda _: pd.NaT),
        }
    )

    out = base.merge(agg, how="left", left_on="ID do pedido", right_on=col_pedido)
    if "Plataforma_x" in out.columns or "Plataforma_y" in out.columns:
        px = out["Plataforma_x"] if "Plataforma_x" in out.columns else pd.Series(pd.NA, index=out.index)
        py = out["Plataforma_y"] if "Plataforma_y" in out.columns else pd.Series(pd.NA, index=out.index)
        py_ok = py.notna() & py.astype(str).str.strip().ne("")
        out["Plataforma"] = py.where(py_ok, px)
        out = out.drop(columns=["Plataforma_x", "Plataforma_y"], errors="ignore")
    keep_cols = [
        "N° de venda",
        "ID do pedido",
        "Total BRL",
        "Valor pago",
        "Número da nota",
        "Valor da nota",
        "Plataforma",
    ]
    if "Data de emissão" in out.columns:
        keep_cols.append("Data de emissão")
    if "Data de pagamento" in out.columns:
        keep_cols.append("Data de pagamento")
    out = out[keep_cols].copy()
    out["Plataforma"] = out["Plataforma"].fillna("Não identificado").astype(str)
    out["Número da nota"] = out["Número da nota"].fillna("").astype(str).str.strip()
    out["Valor da nota"] = pd.to_numeric(out["Valor da nota"], errors="coerce")
    if "Data de emissão" in out.columns:
        out["Data de emissão"] = pd.to_datetime(out["Data de emissão"], errors="coerce")
        out["Data de emissão"] = out["Data de emissão"].dt.strftime("%Y-%m-%d").fillna("")
    out["Status NF"] = out["Número da nota"].fillna("").astype(str).str.strip().ne("").map(
        {True: "Com nota", False: "Sem nota"}
    )
    return out


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conciliacao_com_notas = build_conciliacao_com_notas()

    total = int(len(conciliacao_com_notas))
    com_nota = int(conciliacao_com_notas["Status NF"].eq("Com nota").sum())
    sem_nota = int(conciliacao_com_notas["Status NF"].eq("Sem nota").sum())
    pct = (com_nota / total * 100.0) if total else 0.0

    print("Head (conciliacao_com_notas):")
    print(conciliacao_com_notas.head(12).to_string(index=False))
    print("\nMétricas:")
    print(f"- Total de registros analisados: {total}")
    print(f"- Com nota: {com_nota}")
    print(f"- Sem nota: {sem_nota}")
    print(f"- Percentual com nota: {pct:.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

