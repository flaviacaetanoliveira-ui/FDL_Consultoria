from __future__ import annotations

import csv
import os
import re
import sys
import unicodedata
from pathlib import Path

import pandas as pd

from integracao_notas_pedidos import BASE_DIR, build_conciliacao_com_notas
from operacional_data_config import DATASET_EMPRESA
from etapa3_conciliacao_vendas_liberacoes_validas import build_conciliacao_vendas_liberacoes_validas


def _pasta_contas(base_dir: str | Path) -> Path:
    return Path(base_dir).resolve() / "contas_receber"


PASTA_CONTAS = BASE_DIR / "contas_receber"


def _norm(s: pd.Series) -> pd.Series:
    return s.fillna("").astype(str).str.strip()


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


def _norm_header_ascii(name: object) -> str:
    s = unicodedata.normalize("NFKD", str(name or "")).encode("ascii", "ignore").decode().lower().strip()
    return " ".join(s.split())


def _detect_col(columns: list[str], candidates_raw: set[str]) -> str:
    """Cabeçalhos reais (Bling/CSV) variam; compara sem acentos e permite substring (alvos longos primeiro)."""
    if not columns:
        return ""
    cand_norms = sorted({_norm_header_ascii(x) for x in candidates_raw}, key=len, reverse=True)
    norm_map = [(c, _norm_header_ascii(c)) for c in columns if str(c).strip()]
    for c, n in norm_map:
        if n in cand_norms:
            return c
    for c, n in norm_map:
        if not n:
            continue
        for cand in cand_norms:
            if len(cand) >= 4 and cand in n:
                return c
    return ""


_STRICT_NF_CONTAS_COLS: set[str] = {
    "número da nota",
    "numero da nota",
    "numero nota",
    "número nota",
    "nota fiscal",
    "numero do documento",
    "número do documento",
    "documento fiscal",
    "nfe",
    "numero nfe",
    "número nfe",
    "numero da nf-e",
    "número da nf-e",
    "chave nfe",
    "no da nota",
    "nº da nota",
    "nº documento",
    "numero documento auxiliar",
    "número documento auxiliar",
}


def _detect_col_nf_contas(columns: list[str]) -> str:
    """
    Evita escolher colunas genéricas «Número» / «Documento» do Bling (dados errados → merge sem Situação).
    Prioriza cabeçalhos explícitos de NF; depois colunas cujo nome sugira nota/NF.
    """
    c = _detect_col(columns, _STRICT_NF_CONTAS_COLS)
    if c:
        return c
    best = ""
    best_len = -1
    for col in columns:
        n = _norm_header_ascii(col)
        if not n or len(n) < 2:
            continue
        if "pedido" in n and "nota" not in n and "nf" not in n and "nfe" not in n:
            continue
        if (
            "nota" in n
            or "nfe" in n
            or n in {"nf", "nfe"}
            or n.endswith(" nf")
            or " nf " in f" {n} "
            or n.startswith("nf ")
        ):
            if len(n) > best_len:
                best = col
                best_len = len(n)
    if best:
        return best
    # Relatório Bling «Contas a receber» costuma trazer só a coluna **Número** (título), sem «Número da nota».
    for col in columns:
        n = _norm_header_ascii(col)
        if n in {"numero", "n", "nº"}:
            return col
    return ""


def _nf_merge_key_one(val: object) -> str:
    """Chave estável para cruzar NF da nota com contas a receber (ignora prefixos tipo NF-, zeros à esquerda)."""
    s = str(val or "").strip()
    if not s:
        return ""
    s = re.sub(r"/.*$", "", s, flags=re.IGNORECASE)
    nums = re.findall(r"\d+", s)
    if not nums:
        return ""
    raw = nums[-1]
    stripped = raw.lstrip("0")
    return stripped if stripped else raw


def _nf_merge_key_series(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).map(_nf_merge_key_one)


def _classificar_acao(row: pd.Series) -> str:
    sem_bling = os.environ.get("FDL_REPASSE_SEM_BLING", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }
    tolerancia = 0.01
    valor_pago = pd.to_numeric(row["Valor pago"], errors="coerce")
    valor_receber = pd.to_numeric(row.get("Valor a receber"), errors="coerce")
    tem_pagamento = pd.notna(valor_pago) and float(valor_pago) > 0
    situ = str(row["Situação"]).strip().lower()
    pago = any(k in situ for k in ["pago", "baixado", "liquidado", "quitado"])
    diferenca = pd.NA
    if pd.notna(valor_receber) and pd.notna(valor_pago):
        diferenca = float(valor_receber) - float(valor_pago)

    if not tem_pagamento:
        return "Verificar recebimento"
    if pd.notna(valor_receber) and float(valor_receber) == 0 and float(valor_pago) > 0:
        return "Revisar venda zerada"
    if pd.notna(diferenca) and abs(float(diferenca)) > tolerancia:
        return "Analisar diferença"
    if pago:
        return "Ok"
    return "Baixado" if sem_bling else "Baixar no Bling"


def _repasse_vendas_liberacoes_only() -> bool:
    """
    Cliente sem Bling/notas/contas: monta repasse só com vendas x liberações (etapa3).
    """
    return os.environ.get("FDL_REPASSE_VENDAS_LIBERACOES_ONLY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _build_final_from_vendas_liberacoes(root: Path) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Tabela final no layout do app sem dependências de notas/contas.
    """
    base = build_conciliacao_vendas_liberacoes_validas(root).copy()
    out = pd.DataFrame(index=base.index)
    out["N° de venda"] = _norm(base.get("N° de venda", pd.Series("", index=base.index)))
    out["ID do pedido"] = ""
    out["Total BRL"] = pd.to_numeric(base.get("Total BRL"), errors="coerce")
    out["Número da nota"] = ""
    out["Numero_sem_parcela"] = ""
    out["Valor da nota"] = pd.NA
    out["Plataforma"] = (
        base.get("Plataforma", pd.Series("Mercado Livre", index=base.index))
        .fillna("Mercado Livre")
        .astype(str)
    )
    out["Situação"] = ""
    out["Valor a receber"] = pd.to_numeric(base.get("Total BRL"), errors="coerce")
    out["Valor pago"] = pd.to_numeric(base.get("Valor pago"), errors="coerce")
    out["Diferença"] = out["Valor a receber"] - out["Valor pago"]
    out["Data de pagamento"] = pd.to_datetime(base.get("Data de pagamento"), errors="coerce")
    out["Data de pagamento"] = out["Data de pagamento"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    out["Data de emissão"] = ""
    out["Ação sugerida"] = out.apply(_classificar_acao, axis=1)
    final = out[
        [
            "N° de venda",
            "ID do pedido",
            "Total BRL",
            "Número da nota",
            "Numero_sem_parcela",
            "Valor da nota",
            "Plataforma",
            "Situação",
            "Ação sugerida",
            "Valor a receber",
            "Valor pago",
            "Diferença",
            "Data de pagamento",
            "Data de emissão",
        ]
    ].copy()
    final["empresa"] = DATASET_EMPRESA
    return final, {
        "base_dir": str(root),
        "linhas": int(len(final)),
        "arquivos_contas_lidos": 0,
        "col_situacao": "",
        "col_numero": "",
    }


def _numero_sem_parcela(series: pd.Series) -> pd.Series:
    s = _norm(series)
    s = s.str.replace(r"/.*$", "", regex=True)  # remove sufixo de parcela
    s = s.str.replace(r"\.0+$", "", regex=True)
    s = s.str.lstrip("0")  # remove zeros à esquerda
    return s


def _detectar_col_data_emissao(columns: list[str]) -> str:
    norm = {c: str(c).strip().lower() for c in columns}
    for col, nome in norm.items():
        if "data" in nome and "emiss" in nome:
            return col
    for col, nome in norm.items():
        if "emiss" in nome:
            return col
    return ""


def carregar_tabela_final_operacional(base_dir: Path = BASE_DIR) -> tuple[pd.DataFrame, dict[str, object]]:
    """
    Monta a tabela operacional final.

    - **Valor pago** e **Data de pagamento** vêm sempre das **liberações** (etapa3 + integração com notas),
      não do ficheiro de contas a receber.
    - **contas_receber** serve só para enriquecer **Situação** do título (ex.: Bling), via cruzamento por NF.
    """
    root = Path(base_dir).resolve()
    if _repasse_vendas_liberacoes_only():
        return _build_final_from_vendas_liberacoes(root)
    base = build_conciliacao_com_notas(filtrar_notas_invalidas=True, base_dir=root).copy()
    base["Número da nota"] = _norm(base["Número da nota"])

    pasta_contas = _pasta_contas(root)
    files = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in pasta_contas.rglob(ptn) if p.is_file())
    files.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    partes = []
    for f in files:
        d = _read_contas(f).dropna(axis=1, how="all").copy()
        d["__arquivo__"] = f.name
        partes.append(d)
    contas = pd.concat(partes, ignore_index=True) if partes else pd.DataFrame()

    if contas.empty:
        out = base.copy()
        out["Plataforma"] = base.get("Plataforma", "Não identificado")
        out["Numero_sem_parcela"] = _numero_sem_parcela(out["Número da nota"])
        out["Situação"] = ""
        out["Valor a receber"] = pd.to_numeric(out.get("Total BRL"), errors="coerce")
        out["Valor pago"] = pd.to_numeric(out.get("Valor pago"), errors="coerce")
        out["Diferença"] = out["Valor a receber"] - out["Valor pago"]
        if "Data de pagamento" not in out.columns:
            out["Data de pagamento"] = pd.NA
        if "Data de emissão" in out.columns:
            out["Data de emissão"] = pd.to_datetime(out["Data de emissão"], errors="coerce")
            out["Data de emissão"] = out["Data de emissão"].dt.strftime("%Y-%m-%d").fillna("")
        else:
            out["Data de emissão"] = ""
        out["Data de pagamento"] = pd.to_datetime(out["Data de pagamento"], errors="coerce")
        out["Data de pagamento"] = out["Data de pagamento"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
        out["Ação sugerida"] = out.apply(_classificar_acao, axis=1)
        final = out[
            [
                "N° de venda",
                "ID do pedido",
                "Total BRL",
                "Número da nota",
                "Numero_sem_parcela",
                "Valor da nota",
                "Plataforma",
                "Situação",
                "Ação sugerida",
                "Valor a receber",
                "Valor pago",
                "Diferença",
                "Data de pagamento",
                "Data de emissão",
            ]
        ].copy()
        final["empresa"] = DATASET_EMPRESA
        return final, {
            "base_dir": str(base_dir),
            "linhas": int(len(final)),
            "arquivos_contas_lidos": 0,
            "col_situacao": "",
            "col_numero": "",
        }

    col_situ = _detect_col(
        list(contas.columns),
        {
            "situação",
            "situacao",
            "status",
            "situação do título",
            "situacao do titulo",
            "estado",
            "condição",
            "condicao",
        },
    )
    col_nf = _detect_col_nf_contas(list(contas.columns))

    contas_join = contas.copy()
    if col_nf:
        contas_join["__nota_raw__"] = _norm(contas_join[col_nf])
    else:
        contas_join["__nota_raw__"] = ""
    contas_join["__nf_key__"] = _nf_merge_key_series(contas_join["__nota_raw__"])
    if col_situ:
        contas_join["_sit_contas"] = _norm(contas_join[col_situ])
    else:
        contas_join["_sit_contas"] = ""

    contas_lookup = contas_join[["__nf_key__", "_sit_contas"]].copy()
    contas_lookup = contas_lookup[contas_lookup["__nf_key__"].ne("")].drop_duplicates(
        subset=["__nf_key__"], keep="first"
    )

    out = base.copy()
    out["__nf_key__"] = _nf_merge_key_series(out["Número da nota"])
    out = out.merge(contas_lookup, how="left", on="__nf_key__")
    out = out.drop(columns=["__nf_key__"], errors="ignore")
    out["Situação"] = _norm(out["_sit_contas"])
    out = out.drop(columns=["_sit_contas"], errors="ignore")
    out["Numero_sem_parcela"] = _numero_sem_parcela(out["Número da nota"])
    # Data de pagamento / Valor pago: só a partir de `base` (liberações); contas_lookup não os inclui.
    out["Valor a receber"] = pd.to_numeric(out.get("Total BRL"), errors="coerce")
    out["Valor pago"] = pd.to_numeric(out.get("Valor pago"), errors="coerce")
    out["Diferença"] = out["Valor a receber"] - out["Valor pago"]
    out["Ação sugerida"] = out.apply(_classificar_acao, axis=1)
    if "Data de emissão" in out.columns:
        out["Data de emissão"] = pd.to_datetime(out["Data de emissão"], errors="coerce")
        out["Data de emissão"] = out["Data de emissão"].dt.strftime("%Y-%m-%d").fillna("")
    else:
        col_data_emissao = _detectar_col_data_emissao(list(out.columns))
        if col_data_emissao:
            out["Data de emissão"] = pd.to_datetime(out[col_data_emissao], errors="coerce")
            out["Data de emissão"] = out["Data de emissão"].dt.strftime("%Y-%m-%d").fillna("")
        else:
            out["Data de emissão"] = ""

    for col_opt in ("Valor pago", "Data de pagamento"):
        if col_opt not in out.columns:
            out[col_opt] = pd.NA
    # Garante exibição consistente no app (evita None cru na tabela).
    out["Data de pagamento"] = pd.to_datetime(out["Data de pagamento"], errors="coerce")
    out["Data de pagamento"] = out["Data de pagamento"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")
    out["Data de pagamento"] = (
        out["Data de pagamento"].astype(str).str.replace("NaT", "", regex=False).str.replace("None", "", regex=False)
    )

    final = out[
        [
            "N° de venda",
            "ID do pedido",
            "Total BRL",
            "Número da nota",
            "Numero_sem_parcela",
            "Valor da nota",
            "Plataforma",
            "Situação",
            "Ação sugerida",
            "Valor a receber",
            "Valor pago",
            "Diferença",
            "Data de pagamento",
            "Data de emissão",
        ]
    ].copy()
    final["empresa"] = DATASET_EMPRESA

    return final, {
        "base_dir": str(base_dir),
        "linhas": int(len(final)),
        "arquivos_contas_lidos": int(len(files)),
        "col_situacao": col_situ,
        "col_numero": col_nf,
    }


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    final, info = carregar_tabela_final_operacional(BASE_DIR)
    contas = pd.DataFrame()  # placeholder para manter prints legíveis

    print("=== ETAPA 4B — INTEGRAÇÃO COM CONTAS A RECEBER ===")
    print("\n[1] Colunas da tabela de contas a receber:")
    print(f"arquivos de contas lidos: {info['arquivos_contas_lidos']}")
    print(f"\n[2] Coluna de situação detectada: {info['col_situacao'] or 'NÃO ENCONTRADA'}")
    print(f"[3] Coluna de número da nota detectada: {info['col_numero'] or 'NÃO ENCONTRADA'}")

    print("\n[4] Tabela final (head)")
    print(final.head(12).to_string(index=False))

    print("\n[5] Quantidade por situação")
    situ = (
        final["Situação"]
        .replace("", "(sem situação)")
        .value_counts(dropna=False)
        .rename_axis("Situação")
        .reset_index(name="Quantidade")
    )
    print(situ.to_string(index=False))

    print("\n[6] Quantidade por ação sugerida")
    acao = (
        final["Ação sugerida"]
        .value_counts(dropna=False)
        .rename_axis("Ação sugerida")
        .reset_index(name="Quantidade")
    )
    print(acao.to_string(index=False))

    com_situ = int(final["Situação"].ne("").sum())
    sem_situ = int(final["Situação"].eq("").sum())
    print("\n[7] Recalculo de situação")
    print(f"- Quantidade com situação: {com_situ}")
    print(f"- Quantidade sem situação: {sem_situ}")

    # Comparação com cenário anterior (sem filtrar notas inválidas)
    base_prev = build_conciliacao_com_notas(filtrar_notas_invalidas=False).copy()
    base_prev["Número da nota"] = _norm(base_prev["Número da nota"])
    prev = base_prev.copy()
    prev["Numero_sem_parcela"] = _numero_sem_parcela(prev["Número da nota"])
    # reconstrói lookup rápido
    contas_all = []
    files = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in PASTA_CONTAS.rglob(ptn) if p.is_file())
    for f in files:
        contas_all.append(_read_contas(f).dropna(axis=1, how="all").copy())
    contas_df = pd.concat(contas_all, ignore_index=True) if contas_all else pd.DataFrame()
    col_situ = _detect_col(
        list(contas_df.columns),
        {
            "situação",
            "situacao",
            "status",
            "situação do título",
            "situacao do titulo",
            "estado",
            "condição",
            "condicao",
        },
    )
    col_nf = _detect_col_nf_contas(list(contas_df.columns))
    contas_df["__nota_raw__"] = _norm(contas_df[col_nf]) if col_nf else ""
    contas_df["__nf_key__"] = _nf_merge_key_series(contas_df["__nota_raw__"])
    contas_df["_sit_contas"] = _norm(contas_df[col_situ]) if col_situ else ""
    contas_lookup = contas_df[["__nf_key__", "_sit_contas"]]
    contas_lookup = contas_lookup[contas_lookup["__nf_key__"].ne("")].drop_duplicates(subset=["__nf_key__"], keep="first")
    prev["__nf_key__"] = _nf_merge_key_series(prev["Número da nota"])
    prev = prev.merge(contas_lookup, how="left", on="__nf_key__")
    prev = prev.drop(columns=["__nf_key__"], errors="ignore")
    prev["Situação"] = _norm(prev["_sit_contas"])
    prev = prev.drop(columns=["_sit_contas"], errors="ignore")
    prev["Situação"] = _norm(prev["Situação"])
    prev["Ação sugerida"] = prev.apply(_classificar_acao, axis=1)
    prev_com = int(prev["Situação"].ne("").sum())
    prev_sem = int(prev["Situação"].eq("").sum())
    prev_acao = (
        prev["Ação sugerida"]
        .value_counts(dropna=False)
        .rename_axis("Ação sugerida")
        .reset_index(name="Quantidade (anterior)")
    )
    novo_acao = (
        final["Ação sugerida"]
        .value_counts(dropna=False)
        .rename_axis("Ação sugerida")
        .reset_index(name="Quantidade (novo)")
    )
    comp_acao = prev_acao.merge(novo_acao, how="outer", on="Ação sugerida").fillna(0)
    comp_acao["Delta"] = comp_acao["Quantidade (novo)"] - comp_acao["Quantidade (anterior)"]

    print("\n[8] Comparação com resultado anterior (sem filtro de notas inválidas)")
    print(f"- Com situação: anterior={prev_com} | novo={com_situ} | delta={com_situ - prev_com}")
    print(f"- Sem situação: anterior={prev_sem} | novo={sem_situ} | delta={sem_situ - prev_sem}")
    print("\nDistribuição de Ação sugerida (anterior vs novo):")
    print(comp_acao.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

