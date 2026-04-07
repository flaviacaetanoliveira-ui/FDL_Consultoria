"""
Dataset materializado «Controle de Devoluções»: 1 linha por venda, só **candidatas**
(devolução, reembolso, mediação, reclamação, revisão ou sinais financeiros nas liberações).
"""
from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from etapa1_vendas import detect_columns, list_sales_files, parse_brl_number, read_sales_file
from etapa2_liberacoes import (
    _deduplicar_liberacoes_concatenadas,
    build_liberacoes,
    list_liberacoes_files,
    read_input_file,
)
from etapa1_vendas import normalize_col_name as norm_col_vendas
from fdl_paths import resolve_pasta_vendas_ml

PIPELINE_REVISION_DEVOLUCOES = "devolucoes-v1"
# Classificador operacional (texto ML + reforço conservador das liberações); bump quando mudar regras.
CLASSIFICADOR_DEVOLUCOES_REVISION = "operacional-2026-04"

JACI_CEP_DIGITS = "15155038"
TOL_MONEY = 0.02

_LIB_SIGNAL_WORDS = (
    "refund",
    "reembols",
    "chargeback",
    "devolu",
    "mediation",
    "mediação",
    "mediacao",
    "claim",
    "dispute",
    "reclam",
    "revis",
    "contest",
    "contestação",
    "charge back",
)

_SALE_SIGNAL_WORDS = (
    "devolu",
    "reembols",
    "devolvido",
    "mediação",
    "mediacao",
    "reclama",
    "revisão",
    "revisao",
    "em disputa",
    "disputa",
    "cancelado",
    "cancelada",
    "estorno",
)

def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _norm_text(s: str) -> str:
    t = _strip_accents(str(s).casefold())
    t = re.sub(r"\s+", " ", t).strip()
    return t


def _norm_status_ml(raw: object) -> str:
    """Texto do status para matching: remove HTML, normaliza acentos e espaços."""
    s = re.sub(r"<[^>]+>", " ", str(raw or ""))
    s = re.sub(r"&[a-z]+;", " ", s, flags=re.I)
    return _norm_text(s)


def _is_chegou_em_tracking(t: str) -> bool:
    """Só data de chegada, sem outro contexto operacional (baixa prioridade)."""
    if not t or len(t) > 80:
        return False
    return bool(re.match(r"^chegou em \d{1,2} de [a-z]+$", t))


def _lib_blob_norm(raw: object) -> str:
    return _norm_text(str(raw or ""))


# (padrões em texto já normalizado com _norm_text, comparar com ``in t``)
# Ordem: primeiro match vence (exceto reforço de liberações e fallback).
_CLASSIFICACAO_REGRAS: tuple[tuple[tuple[str, ...], str, str], ...] = (
    # Cobrar plataforma
    (
        (
            "foi necessario cancela-lo porque encontramos irregularidades",
            "identificamos um problema no envio e nao poderemos entregar seu produto",
            "cancelada devido um problema de envio",
            "cancelada devido a um problema de envio",
            "identificamos um problema no envio",
        ),
        "Cobrar plataforma",
        "Documentar e acionar ML (envio)",
    ),
    # Pagamento / compensação ao vendedor (antes de padrões genéricos de devolução)
    (
        (
            "nao foi possivel entregar o pacote a pessoa que realizou a compra. pedimos desculpas, reembolsamos os custos da venda na sua conta",
            "reembolsamos os custos da venda na sua conta",
            "encerramos a reclamacao da venda e liberamos o dinheiro para voce",
            "o comprador cancelou a reclamacao aberta porque informou que o problema foi resolvido. te demos o dinheiro desta venda",
            "te demos o dinheiro desta venda",
            "concretizamos a venda porque ja se passaram 28 dias desde a data da compra",
            "como o comprador cancelou a devolucao que tinha pedido, liberamos o valor desta venda para voce",
            "o comprador cancelou a mediacao da venda porque o problema foi resolvido. liberamos o dinheiro para voce",
        ),
        "Pagamento liberado ao vendedor",
        "Conferir crédito liberado (MP/extrato)",
    ),
    # Reembolso ao comprador / iniciativa vendedor clara
    (
        (
            "reembolsamos o dinheiro ao comprador",
            "como voce reembolsou o dinheiro, nao sera possivel abrir uma reclamacao por esta venda",
            "o comprador aceitou o reembolso que voce ofereceu e ficou com o produto",
            "voce decidiu devolver o dinheiro e ninguem podera reclamar por esta venda",
        ),
        "Reembolso ao comprador",
        "Conferir estorno ao comprador",
    ),
    # Conferência física pendente (ampla)
    (
        (
            "devolvido no dia",
            "devolucao finalizada",
            "finalizada com reembolso para o comprador",
            "finalizada com reembolso",
            "reembolso para o comprador",
            "devolucao finalizada com reembolso",
            "produto retornou para voce",
            "devolvemos o produto a voce porque nao foi possivel entrega-lo a quem realizou a compra",
            "devolvemos o produto a voce",
            "nao foi possivel entregar o pacote a pessoa que realizou a compra",
            "embalando o pacote para devolv",
            "ja pode iniciar a devoluc",
            "pode iniciar a devoluc",
            "avisamos a pessoa que efetuou a compra que ja pode iniciar a devoluc",
            "certifique-se de nao enviar este pacote",
            "se voce nao nos avisar dentro do prazo como o produto chegou",
            "entendemos que voce recebeu o produto conforme o esperado",
            "nao foi possivel entregar o pacote a pessoa que realizou a compra. nao se preocupe, devolveremos o produto nas mesmas condicoes de quando voce o enviou",
            "o produto retornou para voce porque a entrega foi recusada pela pessoa que realizou a compra",
            "o produto retornou para voce porque o endereco de entrega da pessoa que realizou a compra estava incorreto",
            "o produto retornou para voce porque nao encontramos ninguem no endereco de compra durante as tentativas de entrega",
            "o produto retornou para voce porque a pessoa que realizou a compra solicitou o cancelamento",
            "o produto retornou para voce porque nao foi possivel entrega-lo a pessoa que realizou a compra",
            "nao conseguiu imprimir a etiqueta",
        ),
        "Conferência física pendente",
        "Conferir devolução / recebimento físico",
    ),
    # Mediação
    (
        (
            "se voce nao oferecer uma solucao, o comprador decidira",
            "escreva-nos para podermos te ajudar a prosseguir com o caso",
            "fale conosco no menu da venda",
            "caso nao chegue ate",
            "o comprador abriu uma reclamacao porque a embalagem estava em ordem mas o produto nao funciona",
            "o comprador abriu uma reclamacao porque se arrependeu da compra",
        ),
        "Mediação em andamento",
        "Responder prazo ML / ofertar solução",
    ),
    # Ambíguo — conferir extrato
    (
        (
            "voce pode ve-lo na sua conta mercado pago",
            "este dinheiro ja esta disponivel na sua",
            "dinheiro ja esta disponivel",
        ),
        "Revisar financeiramente",
        "Conferir extrato MP e liberações",
    ),
    # Encerramento favorável vendedor (textos restantes)
    (
        (
            "o comprador nao podera reiniciar uma reclamacao por esta venda",
        ),
        "Pagamento liberado ao vendedor",
        "Conferir crédito liberado (MP/extrato)",
    ),
    # Cancelamentos comprador / venda
    (
        (
            "cancelou porque se arrependeu da compra",
            "cancelou e especificou outro problema",
            "cancelou porque nao podia esperar o produto",
            "cancelada porque nao ha estoque disponivel",
            "cancelou porque garante nao ter realizado a compra",
            "cancelada porque o comprador se arrependeu",
            "cancelou porque nao conseguiu entrar em contato com voce",
            "cancelou porque nao ha estoque disponivel",
            "cancelada porque nao ha estoque disponivel",
        ),
        "Cancelada pelo comprador",
        "Conferir motivo e arquivo",
    ),
)


def _match_rules(t: str) -> tuple[str, str] | None:
    for padrões, status, acao in _CLASSIFICACAO_REGRAS:
        needles = tuple(_norm_text(p) for p in padrões)
        if any(n and n in t for n in needles):
            return status, acao
    return None


def _lib_reforco_conservador(
    t: str,
    lib_blob: str,
    atual_status: str,
    atual_acao: str,
) -> tuple[str, str]:
    """Só reforça quando o texto deixou rastreio ou «outros»; nunca pisa Cobrar/Conferência já claros."""
    if atual_status not in ("Atualização de rastreamento", "Outros — revisar texto ML"):
        return atual_status, atual_acao
    if not lib_blob:
        return atual_status, atual_acao
    has_refund = "refund" in lib_blob
    has_med = "mediation" in lib_blob
    has_rfd = "reserve_for_dispute" in lib_blob
    has_sc = "shipping_cancel" in lib_blob
    if has_refund and (has_med or has_rfd):
        return "Revisar financeiramente", "Conferir extrato MP e liberações"
    if has_refund and atual_status == "Outros — revisar texto ML":
        return "Revisar financeiramente", "Conferir extrato MP e liberações"
    if has_sc and has_refund and ("envio" in t or "entreg" in t or "pacote" in t):
        return "Cobrar plataforma", "Documentar e acionar ML (envio)"
    return atual_status, atual_acao


def _classificar_operacional(
    *,
    status_ml_raw: str,
    lib_desc_merged: str,
    classificacao_reembolso: str,
) -> tuple[str, str]:
    """
    Devolve (status_interno, acao_sugerida).
    Usa texto ML como base; liberações só como reforço conservador.
    """
    t = _norm_status_ml(status_ml_raw)

    # Devolução atrasada (texto explícito)
    if "atrasad" in t and "devolu" in t:
        return "Devolução atrasada (acompanhar)", "Acompanhar prazo da devolução"

    hit = _match_rules(t)
    if hit:
        st, ac = hit
        st2, ac2 = _lib_reforco_conservador(t, _lib_blob_norm(lib_desc_merged), st, ac)
        # Reforço: sem_dados + sinal monetário no texto
        if (
            classificacao_reembolso == "sem_dados"
            and st2 in ("Atualização de rastreamento", "Outros — revisar texto ML", "Cancelada pelo comprador")
            and ("reembols" in t or "dinheiro" in t or "liberamos" in t)
            and ("refund" in _lib_blob_norm(lib_desc_merged) or "payment" in _lib_blob_norm(lib_desc_merged))
        ):
            return "Revisar financeiramente", "Conferir extrato MP e liberações"
        return st2, ac2

    if _is_chegou_em_tracking(t):
        st, ac = "Atualização de rastreamento", "Acompanhar rastreio (baixa prioridade)"
        return _lib_reforco_conservador(t, _lib_blob_norm(lib_desc_merged), st, ac)

    st, ac = "Outros — revisar texto ML", "Revisar detalhe no ML"
    st2, ac2 = _lib_reforco_conservador(t, _lib_blob_norm(lib_desc_merged), st, ac)
    if (
        classificacao_reembolso == "sem_dados"
        and ("reembols" in t or "dinheiro" in t)
        and "refund" in _lib_blob_norm(lib_desc_merged)
    ):
        return "Revisar financeiramente", "Conferir extrato MP e liberações"
    return st2, ac2


def _is_temp_file(path: Path) -> bool:
    n = path.name.lower()
    return n.startswith("~$") or n.endswith(".tmp") or n.startswith(".~")


def _pick_column_by_norm(df: pd.DataFrame, predicates: Iterable[tuple[str, int]]) -> str:
    """``predicates``: (substring_ou_token_na_norm_col, score)."""
    best: tuple[int, str] = (-1, "")
    for c in df.columns:
        n = norm_col_vendas(c)
        sc = 0
        for needle, w in predicates:
            if needle in n:
                sc += w
        if sc > best[0]:
            best = (sc, str(c))
    return best[1] if best[0] > 0 else ""


def _collect_vendas_consolidated(base_dir: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    v_dir = resolve_pasta_vendas_ml(base_dir)
    diag: list[dict[str, Any]] = []
    if not v_dir.is_dir():
        return pd.DataFrame(), [{"erro": "pasta_vendas_inexistente", "path": str(v_dir)}]

    parts: list[pd.DataFrame] = []
    try:
        files = [p for p in list_sales_files(v_dir) if not _is_temp_file(p)]
    except FileNotFoundError:
        return pd.DataFrame(), [{"erro": "list_sales_files", "path": str(v_dir)}]

    for path in files:
        rel = str(path.relative_to(base_dir)).replace("\\", "/")
        entry: dict[str, Any] = {"arquivo": rel, "linhas_brutas": 0, "ok": False}
        try:
            raw = read_sales_file(path).dropna(axis=1, how="all")
            entry["linhas_brutas"] = int(len(raw))
            det = detect_columns(raw)
        except Exception as exc:  # noqa: BLE001
            entry["motivo"] = str(exc)
            diag.append(entry)
            continue

        df = raw.copy()
        df["arquivo_origem_venda"] = rel
        df = df.rename(columns={det.sale_col: "N° de venda"})
        df["N° de venda"] = df["N° de venda"].fillna("").astype(str).str.strip()
        df = df[df["N° de venda"].ne("")].copy()
        parts.append(df)
        entry["ok"] = True
        entry["linhas_uteis"] = int(len(df))
        diag.append(entry)

    if not parts:
        return pd.DataFrame(), diag

    merged = pd.concat(parts, ignore_index=True, sort=False)
    merged = merged.drop_duplicates(subset=["N° de venda"], keep="first").reset_index(drop=True)
    return merged, diag


def _collect_liberacoes_consolidated(base_dir: Path) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    lib_dir = base_dir / "Liberações_ML"
    diag: list[dict[str, Any]] = []
    if not lib_dir.is_dir():
        return pd.DataFrame(), [{"erro": "pasta_liberacoes_inexistente", "path": str(lib_dir)}]

    try:
        files = [p for p in list_liberacoes_files(lib_dir) if not _is_temp_file(p)]
    except FileNotFoundError:
        return pd.DataFrame(), [{"erro": "list_liberacoes_files", "path": str(lib_dir)}]

    tratadas: list[pd.DataFrame] = []
    for path in files:
        rel = str(path.relative_to(base_dir)).replace("\\", "/")
        item: dict[str, Any] = {"arquivo": rel, "linhas_brutas": 0, "ok": False}
        try:
            raw = read_input_file(path).dropna(axis=1, how="all")
            item["linhas_brutas"] = int(len(raw))
            lib_t, _ = build_liberacoes(raw)
        except Exception as exc:  # noqa: BLE001
            item["motivo"] = str(exc)
            diag.append(item)
            continue

        lib_t = lib_t.copy()
        lib_t["arquivo_origem_liberacao"] = rel
        tratadas.append(lib_t)
        item["ok"] = True
        item["linhas_tratadas"] = int(len(lib_t))
        diag.append(item)

    if not tratadas:
        return pd.DataFrame(), diag

    all_l = pd.concat(tratadas, ignore_index=True)
    all_l = _deduplicar_liberacoes_concatenadas(all_l)
    return all_l, diag


def _build_pack_to_orders(lib: pd.DataFrame) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    if lib.empty:
        return out
    for _, r in lib.iterrows():
        pk = str(r.get("PACK_ID", "")).strip()
        oid = str(r.get("ORDER_ID", "")).strip()
        if pk and oid:
            out.setdefault(pk, set()).add(oid)
    return out


def _resolve_sale_for_lib_row(
    r: pd.Series,
    sale_keys: set[str],
    pack_to_orders: dict[str, set[str]],
) -> tuple[str | None, str, str, str]:
    oid = str(r.get("ORDER_ID", "")).strip()
    ext = str(r.get("EXTERNAL_REFERENCE", "")).strip()
    pk = str(r.get("PACK_ID", "")).strip()

    if oid and oid in sale_keys:
        return oid, "ORDER_ID", "alta", ""
    if ext and ext in sale_keys:
        return ext, "EXTERNAL_REFERENCE", "alta", ""
    if pk and pk in pack_to_orders:
        orders = pack_to_orders[pk]
        if len(orders) == 1:
            only = next(iter(orders))
            conf = "media" if only in sale_keys else "baixa"
            det = "pack_id→único_order" if only in sale_keys else "order_id_sem_venda"
            return only, "PACK_ID", conf, det
        return None, "PACK_ID", "ambigua", f"orders={len(orders)}"
    return None, "", "sem_match", ""


def _lib_row_financial_signal(r: pd.Series) -> bool:
    blob = _norm_text(f"{r.get('DESCRIPTION', '')} {r.get('RECORD_TYPE', '')}")
    if any(w in blob for w in _LIB_SIGNAL_WORDS):
        return True
    deb = pd.to_numeric(r.get("NET_DEBIT_AMOUNT"), errors="coerce")
    if deb is not None and pd.notna(deb) and float(deb) > TOL_MONEY:
        return True
    return False


def _sale_row_candidate(row: pd.Series, status_col: str) -> bool:
    blob = _norm_text(" ".join(str(row.get(c, "")) for c in row.index if c not in {"arquivo_origem_venda"}))
    if any(w in blob for w in _SALE_SIGNAL_WORDS):
        return True
    if status_col and status_col in row.index:
        st = _norm_text(row.get(status_col, ""))
        if any(w in st for w in _SALE_SIGNAL_WORDS):
            return True
    # Colunas típicas booleanas / valores
    for c in row.index:
        nc = norm_col_vendas(c)
        if "mediacao" in nc or "reclam" in nc:
            v = row.get(c)
            if pd.notna(v):
                s = str(v).strip().lower()
                if s in {"sim", "true", "1", "yes"}:
                    return True
        if "cancel" in nc and "reembols" in nc:
            num = parse_brl_number(pd.Series([row.get(c)])).iloc[0]
            if pd.notna(num) and float(num) > TOL_MONEY:
                return True
    return False


def _classify_reembolso_conservador(row: pd.Series, total_brl_col: str) -> tuple[str, float | None]:
    """
    Devolve (classificação, valor_num_ou_None).
    Conservador: sem coluna clara ou valores conflituosos → ``sem_dados``.
    """
    candidates: list[tuple[str, float]] = []
    for c in row.index:
        nc = norm_col_vendas(c)
        if "reembols" in nc or nc.endswith(" reembolso") or "valor do reembolso" in nc:
            v = parse_brl_number(pd.Series([row.get(c)])).iloc[0]
            if pd.notna(v):
                candidates.append((str(c), float(v)))

    if not candidates:
        return "sem_dados", None
    # Mesmo nome duplicado ou um só valor distinto
    vals = sorted({round(x[1], 2) for x in candidates})
    if len(vals) != 1:
        return "sem_dados", None
    val = vals[0]
    if val <= TOL_MONEY:
        return "sem_reembolso_numerico", 0.0

    total = None
    if total_brl_col and total_brl_col in row.index:
        total = parse_brl_number(pd.Series([row.get(total_brl_col)])).iloc[0]
        if pd.isna(total):
            total = None
        else:
            total = float(total)

    if total is not None and total > TOL_MONEY:
        if abs(val - total) <= max(0.05, total * 0.02):
            return "reembolso_total_provavel", val
        if val < total - TOL_MONEY:
            return "reembolso_parcial", val
    return "reembolso_com_valor", val


def _jaci_flags(row: pd.Series, cep_col: str, addr_cols: list[str]) -> tuple[bool, float, str]:
    cep_ok = False
    parts: list[str] = []
    if cep_col and cep_col in row.index:
        d = re.sub(r"\D", "", str(row.get(cep_col, "")))
        if d == JACI_CEP_DIGITS:
            cep_ok = True
        if d:
            parts.append(d)
    blob = _norm_text(" ".join(str(row.get(c, "")) for c in addr_cols if c in row.index))
    score = 0.0
    if "jacarei" in blob or " jaci " in f" {blob} " or blob.endswith(" jaci"):
        score += 0.6
    if re.search(r"\bsp\b", blob) or "sao paulo" in blob or "são paulo" in blob:
        score += 0.2
    if "rua" in blob or "rodovia" in blob or "avenida" in blob:
        score += 0.1
    return cep_ok, min(1.0, score), blob[:500]


def build_devolucoes_dataset(
    base_dir: Path,
    *,
    org_id: str,
    dataset_empresa: str,
    cliente_id: str | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """
    Constrói o DataFrame materializado (já filtrado a candidatas).
    """
    base_dir = base_dir.expanduser().resolve()
    meta: dict[str, Any] = {
        "pipeline_revision": PIPELINE_REVISION_DEVOLUCOES,
        "base_dir": str(base_dir),
        "org_id": org_id,
        "dataset_empresa": dataset_empresa,
        "cliente_id": cliente_id or "",
    }

    vendas, dv = _collect_vendas_consolidated(base_dir)
    meta["vendas_arquivos_diag"] = dv
    lib, dl = _collect_liberacoes_consolidated(base_dir)
    meta["liberacoes_arquivos_diag"] = dl

    if vendas.empty:
        meta["erro"] = "sem_vendas"
        return pd.DataFrame(), meta

    sale_keys = set(vendas["N° de venda"].astype(str).str.strip())
    pack_map = _build_pack_to_orders(lib)

    # Agregação por venda a partir das liberações
    by_sale: dict[str, list[tuple[pd.Series, str, str, str]]] = defaultdict(list)
    if not lib.empty:
        for _, r in lib.iterrows():
            sk, tipo, conf, det = _resolve_sale_for_lib_row(r, sale_keys, pack_map)
            if sk:
                by_sale[sk].append((r, tipo, conf, det))

    sales_from_lib_financial: set[str] = set()
    lib_agg_rows: dict[str, dict[str, Any]] = {}
    for sk, lst in by_sale.items():
        if any(_lib_row_financial_signal(t[0]) for t in lst):
            sales_from_lib_financial.add(sk)
        # Melhor linha de vínculo: preferir ORDER_ID alta
        lst_sorted = sorted(lst, key=lambda x: (0 if x[2] == "alta" else 1 if x[2] == "media" else 2, x[1]))
        best = lst_sorted[0]
        _, tipo_b, conf_b, det_b = best
        dates = [
            pd.to_datetime(t[0].get("Data de pagamento"), errors="coerce", dayfirst=True) for t in lst
        ]
        dates_ok = [d for d in dates if pd.notna(d)]
        ult = max(dates_ok).strftime("%Y-%m-%d %H:%M:%S") if dates_ok else ""
        descs: list[str] = []
        for t in lst:
            d = str(t[0].get("DESCRIPTION", "")).strip()
            if d and d not in descs:
                descs.append(d)
        deb_sum = sum(
            float(pd.to_numeric(t[0].get("NET_DEBIT_AMOUNT"), errors="coerce") or 0) for t in lst
        )
        lib_agg_rows[sk] = {
            "lib_n_eventos": len(lst),
            "lib_ultima_data_pagamento": ult,
            "lib_descricoes_amostra": " | ".join(descs[:6]),
            "lib_soma_net_debito": round(deb_sum, 2),
            "vinculo_tipo": tipo_b,
            "vinculo_confianca": conf_b,
            "vinculo_detalhe": det_b,
        }

    status_col = _pick_column_by_norm(
        vendas,
        (
            ("descricao do status", 8),
            ("situacao", 6),
            ("status", 4),
        ),
    )
    total_col = _pick_column_by_norm(
        vendas,
        (("total", 3), ("brl", 2)),
    )
    if not total_col:
        try:
            total_col = detect_columns(vendas).total_col
        except Exception:  # noqa: BLE001
            total_col = ""

    cep_col = _pick_column_by_norm(vendas, (("cep", 10),))
    addr_cols = [
        c
        for c in vendas.columns
        if c != "N° de venda"
        and any(
            k in norm_col_vendas(c)
            for k in ("endereco", "logradouro", "rua", "cidade", "bairro", "destinatario", "recebedor")
        )
    ][:8]

    mask_sale = vendas.apply(lambda row: _sale_row_candidate(row, status_col), axis=1)
    mask_lib = vendas["N° de venda"].astype(str).isin(sales_from_lib_financial)
    candidatas = vendas.loc[mask_sale | mask_lib].copy()
    candidatas["candidato_motivo"] = ""
    candidatas.loc[mask_sale & ~mask_lib, "candidato_motivo"] = "sinais_na_venda"
    candidatas.loc[~mask_sale & mask_lib, "candidato_motivo"] = "sinais_nas_liberacoes"
    candidatas.loc[mask_sale & mask_lib, "candidato_motivo"] = "venda_e_liberacoes"

    st_series = (
        candidatas[status_col].astype(str)
        if status_col
        else pd.Series("", index=candidatas.index, dtype=str)
    )
    candidatas["status_ml_texto"] = st_series

    def _lib_field(sk: object, field: str) -> Any:
        d = lib_agg_rows.get(str(sk).strip(), {})
        if field == "lib_n_eventos":
            v = d.get(field, 0)
            if v in ("", None):
                return 0
            try:
                return int(v)
            except (TypeError, ValueError):
                return 0
        return d.get(field, "")

    for c in (
        "lib_n_eventos",
        "lib_ultima_data_pagamento",
        "lib_descricoes_amostra",
        "lib_soma_net_debito",
        "vinculo_tipo",
        "vinculo_confianca",
        "vinculo_detalhe",
    ):
        candidatas[c] = candidatas["N° de venda"].map(lambda x, col=c: _lib_field(x, col))

    candidatas["lib_n_eventos"] = pd.to_numeric(candidatas["lib_n_eventos"], errors="coerce").fillna(0).astype(int)

    reemb_cls: list[str] = []
    reemb_val: list[float | None] = []
    for _, row in candidatas.iterrows():
        cls, v = _classify_reembolso_conservador(row, total_col)
        reemb_cls.append(cls)
        reemb_val.append(v)
    candidatas["classificacao_reembolso"] = reemb_cls
    candidatas["reembolso_valor_inferido"] = reemb_val

    j_ceps: list[bool] = []
    j_scores: list[float] = []
    j_blobs: list[str] = []
    for _, row in candidatas.iterrows():
        c_ok, sc, bl = _jaci_flags(row, cep_col, addr_cols)
        j_ceps.append(c_ok)
        j_scores.append(sc)
        j_blobs.append(bl)
    candidatas["jaci_cep_15155038"] = j_ceps
    candidatas["jaci_endereco_score"] = j_scores
    candidatas["jaci_endereco_normalizado"] = j_blobs

    si_list: list[str] = []
    ac_list: list[str] = []
    for _, row in candidatas.iterrows():
        st_i, ac_i = _classificar_operacional(
            status_ml_raw=str(row.get("status_ml_texto", "")),
            lib_desc_merged=str(row.get("lib_descricoes_amostra", "")),
            classificacao_reembolso=str(row.get("classificacao_reembolso", "")),
        )
        si_list.append(st_i)
        ac_list.append(ac_i)
    candidatas["status_interno"] = si_list
    candidatas["acao_sugerida"] = ac_list

    candidatas["org_id"] = org_id
    candidatas["empresa"] = dataset_empresa

    meta["row_count_candidatas"] = int(len(candidatas))
    meta["row_count_vendas_total"] = int(len(vendas))
    meta["row_count_liberacoes"] = int(len(lib))
    meta["sales_from_lib_financial"] = len(sales_from_lib_financial)
    meta["classificador_operacional_revision"] = CLASSIFICADOR_DEVOLUCOES_REVISION
    return candidatas, meta


def build_devolucoes_source_signature(base_dir: Path) -> str:
    """Hash estável dos ficheiros em vendas ML + Liberações_ML (rel_path + mtime_ns)."""
    import hashlib
    import json

    base_dir = base_dir.expanduser().resolve()
    dirs: list[Path] = []
    vd = resolve_pasta_vendas_ml(base_dir)
    if vd.is_dir():
        dirs.append(vd)
    ld = base_dir / "Liberações_ML"
    if ld.is_dir():
        dirs.append(ld)
    out: list[tuple[str, int]] = []
    for d in dirs:
        for f in sorted(d.rglob("*")):
            if f.is_file() and not _is_temp_file(f):
                rel = str(f.relative_to(base_dir)).replace("\\", "/")
                try:
                    out.append((rel, int(f.stat().st_mtime_ns)))
                except OSError:
                    out.append((rel, 0))
    out.sort(key=lambda x: x[0])
    return hashlib.sha256(json.dumps(out, ensure_ascii=True).encode("utf-8")).hexdigest()[:32]
