from __future__ import annotations

import csv
import sys
from pathlib import Path

import pandas as pd

from integracao_notas_pedidos import BASE_DIR, build_conciliacao_com_notas
from operacional_data_config import DATASET_EMPRESA


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


def _detect_col(columns: list[str], candidates_norm: set[str]) -> str:
    norm_map = {c: c.lower().strip() for c in columns}
    # tentativa direta sem acentos já funciona para os arquivos atuais
    for c, n in norm_map.items():
        if n in candidates_norm:
            return c
    for c, n in norm_map.items():
        if any(token in n for token in candidates_norm):
            return c
    return ""


def _classificar_acao(row: pd.Series) -> str:
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
    return "Baixar no Bling"


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
    # Base já no fluxo correto: VENDAS -> LIBERAÇÕES -> NOTAS_VALIDAS
    base = build_conciliacao_com_notas(filtrar_notas_invalidas=True).copy()
    base["Número da nota"] = _norm(base["Número da nota"])

    # Leitura consolidada de contas a receber
    files = []
    for ptn in ("*.csv", "*.xlsx", "*.xls"):
        files.extend(p for p in PASTA_CONTAS.glob(ptn) if p.is_file())
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
        {"situação", "situacao", "status", "situação do título", "situacao do titulo"},
    )
    col_nf = _detect_col(
        list(contas.columns),
        {
            "número da nota",
            "numero da nota",
            "numero nota",
            "número nota",
            "nota fiscal",
            "numero do documento",
            "número do documento",
            "documento",
            "número",
            "numero",
        },
    )

    contas_join = contas.copy()
    if col_nf:
        contas_join["__nota__"] = _norm(contas_join[col_nf])
    else:
        contas_join["__nota__"] = ""
    contas_join["Numero_sem_parcela"] = _numero_sem_parcela(contas_join["__nota__"])
    if col_situ:
        contas_join["Situação"] = _norm(contas_join[col_situ])
    else:
        contas_join["Situação"] = ""

    contas_lookup = contas_join[["Numero_sem_parcela", "Situação"]].copy()
    contas_lookup = contas_lookup[contas_lookup["Numero_sem_parcela"].ne("")].drop_duplicates(
        subset=["Numero_sem_parcela"], keep="first"
    )

    out = base.copy()
    out["Numero_sem_parcela"] = _numero_sem_parcela(out["Número da nota"])
    out = out.merge(contas_lookup, how="left", on="Numero_sem_parcela")
    out["Situação"] = _norm(out["Situação"])
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
        files.extend(p for p in PASTA_CONTAS.glob(ptn) if p.is_file())
    for f in files:
        contas_all.append(_read_contas(f).dropna(axis=1, how="all").copy())
    contas_df = pd.concat(contas_all, ignore_index=True) if contas_all else pd.DataFrame()
    col_situ = _detect_col(list(contas_df.columns), {"situação", "situacao", "status", "situação do título", "situacao do titulo"})
    col_nf = _detect_col(list(contas_df.columns), {"número da nota", "numero da nota", "numero nota", "número nota", "nota fiscal", "numero do documento", "número do documento", "documento", "número", "numero"})
    contas_df["__nota__"] = _norm(contas_df[col_nf]) if col_nf else ""
    contas_df["Numero_sem_parcela"] = _numero_sem_parcela(contas_df["__nota__"])
    contas_df["Situação"] = _norm(contas_df[col_situ]) if col_situ else ""
    contas_lookup = contas_df[["Numero_sem_parcela", "Situação"]]
    contas_lookup = contas_lookup[contas_lookup["Numero_sem_parcela"].ne("")].drop_duplicates(subset=["Numero_sem_parcela"], keep="first")
    prev = prev.merge(contas_lookup, how="left", on="Numero_sem_parcela")
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

