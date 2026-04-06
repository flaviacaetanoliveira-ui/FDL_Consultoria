from __future__ import annotations

import sys
from pathlib import Path
import unicodedata
import re

import pandas as pd

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR
from etapa1_vendas import list_sales_files, parse_brl_number, read_sales_file
from etapa2_liberacoes import list_liberacoes_files, read_shopee_liberacoes_input_file


def classificar_status_financeiro(df: pd.DataFrame, tolerancia: float = 0.01) -> pd.Series:
    valor_pago = pd.to_numeric(df["Valor pago"], errors="coerce")
    total_brl = pd.to_numeric(df["Total BRL"], errors="coerce")
    diff_abs = (total_brl - valor_pago).abs()

    status = pd.Series("Pago a maior", index=df.index, dtype="object")
    status[(valor_pago.isna()) | (valor_pago <= 0)] = "Sem pagamento"
    status[(valor_pago > 0) & (diff_abs <= tolerancia)] = "Pago correto"
    status[(valor_pago > 0) & (valor_pago < total_brl) & (diff_abs > tolerancia)] = "Pago a menor"
    return status


def _normalize_name(name: object) -> str:
    s = str(name or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return " ".join(s.split())


def _find_col(df: pd.DataFrame, aliases: set[str]) -> str:
    cmap = {_normalize_name(c): c for c in df.columns}
    for alias in aliases:
        key = _normalize_name(alias)
        if key in cmap:
            return cmap[key]
    for alias in aliases:
        key = _normalize_name(alias)
        if not key:
            continue
        for norm_col, original_col in cmap.items():
            if key in norm_col or norm_col in key:
                return original_col
    return ""


def _first_existing(base: Path, candidates: tuple[str, ...]) -> Path | None:
    for name in candidates:
        p = base / name
        if p.is_dir():
            return p
    return None


def _find_subdir_by_tokens(base: Path, required_tokens: tuple[str, ...]) -> Path | None:
    if not base.exists():
        return None
    for p in base.iterdir():
        if not p.is_dir():
            continue
        n = _normalize_name(p.name)
        if all(tok in n for tok in required_tokens):
            return p
    return None


def _discover_shopee_dirs(
    root: Path,
    name_candidates: tuple[str, ...],
    token_pair: tuple[str, str],
) -> list[Path]:
    """
    Pastas Shopee em ``cliente_root`` **ou** um nível abaixo (ex.: ``Esquilo/Vendas_Shopee``),
    alinhado à estrutura real (empresa com subpastas próprias).
    """
    found: list[Path] = []
    seen: set[str] = set()

    def add(p: Path | None) -> None:
        if p is None or not p.is_dir():
            return
        key = str(p.resolve())
        if key in seen:
            return
        seen.add(key)
        found.append(p)

    add(_first_existing(root, name_candidates))
    add(_find_subdir_by_tokens(root, token_pair))
    if root.is_dir():
        for child in sorted(root.iterdir()):
            if not child.is_dir():
                continue
            add(_first_existing(child, name_candidates))
            add(_find_subdir_by_tokens(child, token_pair))
    return found


def _union_sales_files_sorted(folders: list[Path]) -> list[Path]:
    """Todos os CSV/XLS da lista de pastas, dedup por caminho, mais recentes primeiro."""
    all_paths: list[Path] = []
    for folder in folders:
        if not folder.is_dir():
            continue
        try:
            all_paths.extend(list_sales_files(folder))
        except FileNotFoundError:
            continue
    by_res: dict[str, Path] = {}
    for p in all_paths:
        try:
            k = str(p.resolve())
        except OSError:
            k = str(p)
        by_res[k] = p
    return sorted(by_res.values(), key=lambda x: x.stat().st_mtime, reverse=True)


def _union_liberacoes_files_sorted(folders: list[Path]) -> list[Path]:
    all_paths: list[Path] = []
    for folder in folders:
        if not folder.is_dir():
            continue
        try:
            all_paths.extend(list_liberacoes_files(folder))
        except FileNotFoundError:
            continue
    by_res: dict[str, Path] = {}
    for p in all_paths:
        try:
            k = str(p.resolve())
        except OSError:
            k = str(p)
        by_res[k] = p
    return sorted(by_res.values(), key=lambda x: x.stat().st_mtime, reverse=True)


def _discover_amazon_vendas_dirs(root: Path) -> list[Path]:
    """
    Pastas de export de pedidos/transações Amazon (layout Mega Fácil e similares).

    Espelha a convenção ``Vendas_ML`` / ``Vendas_Shopee``: ``Vendas_Amazon`` ao nível do
    ``base_dir`` da empresa (ou um nível abaixo).
    """
    return _discover_shopee_dirs(
        root,
        ("Vendas_Amazon", "Vendas Amazon", "vendas_amazon"),
        ("vendas", "amazon"),
    )


def _discover_amazon_liberacoes_dirs(root: Path) -> list[Path]:
    """Pasta de extrato/repositório ou liberações Amazon (ex.: ``Liberações_Amazon``)."""
    return _discover_shopee_dirs(
        root,
        (
            "Liberações_Amazon",
            "Liberacoes_Amazon",
            "Liberações Amazon",
            "Liberacoes Amazon",
        ),
        ("libera", "amazon"),
    )


def _amazon_classify_split_layout_files(folders: list[Path]) -> tuple[list[Path], list[Path]]:
    """
    Junta CSV/XLS de ``Vendas_Amazon`` e ``Liberações_Amazon`` e separa pelo **nome** do ficheiro.

    Na prática os exports podem estar em pastas «trocadas» (ex.: Repositório em ``Vendas_Amazon`` e
    Transações em ``Liberações_Amazon``).
    """
    by_res: dict[str, Path] = {}
    for folder in folders:
        if not folder.is_dir():
            continue
        try:
            for p in list_sales_files(folder):
                by_res[str(p.resolve())] = p
        except FileNotFoundError:
            continue
    all_sorted = sorted(by_res.values(), key=lambda x: x.stat().st_mtime, reverse=True)
    trans: list[Path] = []
    repo: list[Path] = []
    for p in all_sorted:
        nn = _normalize_name(p.name)
        if "repositorio" in nn:
            repo.append(p)
        elif "transa" in nn:
            trans.append(p)
    return trans, repo


def _dedup_shopee_liberacao_linhas(part: pd.DataFrame) -> pd.DataFrame:
    """
    Export Shopee (aba Renda): o mesmo crédito pode aparecer em linhas repetidas (sobretudo em 2026);
    antes do ``groupby`` por pedido com ``sum``, isso duplicava ``Valor pago``. Colapsa por
    pedido + dia civil do pagamento + valor arredondado (paridade com liberações ML).
    """
    if part.empty or "N° de venda" not in part.columns:
        return part
    out = part.copy()
    sk = out["N° de venda"].fillna("").astype(str).str.strip()
    day = pd.to_datetime(out["Data de pagamento"], errors="coerce").dt.normalize()
    vp = pd.to_numeric(out["Valor pago"], errors="coerce").round(2)
    out["_sk"], out["_day"], out["_vp"] = sk, day, vp
    m = sk.ne("") & vp.notna()
    ok = out.loc[m].drop_duplicates(subset=["_sk", "_day", "_vp"], keep="first")
    rest = out.loc[~m].drop(columns=["_sk", "_day", "_vp"], errors="ignore")
    ok = ok.drop(columns=["_sk", "_day", "_vp"], errors="ignore")
    return pd.concat([ok, rest], ignore_index=True)


def _read_amazon_repo_file(path: Path) -> pd.DataFrame:
    """
    Repositório Amazon tem cabeçalho real após linhas descritivas.
    """
    lines = path.read_text(encoding="utf-8-sig", errors="ignore").splitlines()
    header_idx = 0
    for i, ln in enumerate(lines[:80]):
        n = _normalize_name(ln)
        if "data hora" in n and "id de liquidacao" in n:
            header_idx = i
            break
    return pd.read_csv(path, dtype=str, encoding="utf-8-sig", skiprows=header_idx)


_AMZ_PT_MONTHS: dict[str, int] = {
    "jan": 1,
    "fev": 2,
    "mar": 3,
    "abr": 4,
    "mai": 5,
    "jun": 6,
    "jul": 7,
    "ago": 8,
    "set": 9,
    "out": 10,
    "nov": 11,
    "dez": 12,
}


def _parse_amazon_repo_datetime_series(s: pd.Series) -> pd.Series:
    """
    Ex.: '1 de jan. de 2026 04:14:24 GMT-8'
    """
    parsed = pd.to_datetime(s, errors="coerce", dayfirst=True, format="mixed")
    if parsed.notna().any():
        return parsed
    raw = s.fillna("").astype(str).str.strip()
    out = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
    rgx = re.compile(
        r"^(?P<d>\d{1,2})\s+de\s+(?P<m>[a-z]{3})\.?\s+de\s+(?P<y>\d{4})\s+(?P<h>\d{1,2})[:h](?P<mi>\d{2})[:m](?P<sec>\d{2})",
        flags=re.IGNORECASE,
    )
    for idx, txt in raw.items():
        txt_norm = unicodedata.normalize("NFKD", txt).encode("ascii", "ignore").decode("ascii").lower().strip()
        m = rgx.match(txt_norm)
        if not m:
            continue
        mon = _AMZ_PT_MONTHS.get(m.group("m").lower())
        if not mon:
            continue
        try:
            out.loc[idx] = pd.Timestamp(
                year=int(m.group("y")),
                month=int(mon),
                day=int(m.group("d")),
                hour=int(m.group("h")),
                minute=int(m.group("mi")),
                second=int(m.group("sec")),
            )
        except Exception:  # noqa: BLE001
            continue
    return out


def _build_conciliacao_amazon(base_dir: str | Path) -> pd.DataFrame:
    root = Path(base_dir)
    trans_files: list[Path] = []
    repo_files: list[Path] = []

    # Layout legado: uma pasta ``Amazon/`` com ficheiros «transações» e «repositório» no nome.
    pasta_amz = _first_existing(root, ("Amazon", "amazon"))
    if pasta_amz is not None:
        trans_files = sorted(
            [
                p
                for p in pasta_amz.rglob("*")
                if p.is_file()
                and "transa" in _normalize_name(p.name)
                and p.suffix.lower() in {".csv", ".xlsx", ".xls"}
            ],
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )
        repo_files = sorted(
            [
                p
                for p in pasta_amz.rglob("*")
                if p.is_file()
                and "repositorio" in _normalize_name(p.name)
                and p.suffix.lower() in {".csv", ".xlsx", ".xls"}
            ],
            key=lambda x: x.stat().st_mtime,
            reverse=True,
        )

    # Layout Mega Fácil / grupo: pastas ``Vendas_Amazon`` + ``Liberações_Amazon`` — o papel é
    # inferido pelo nome do ficheiro (transações vs repositório), não pela pasta.
    vd = _discover_amazon_vendas_dirs(root)
    ld = _discover_amazon_liberacoes_dirs(root)
    seen_amz: set[str] = set()
    split_dirs: list[Path] = []
    for p in vd + ld:
        k = str(p.resolve())
        if k not in seen_amz:
            seen_amz.add(k)
            split_dirs.append(p)
    if split_dirs:
        trans_split, repo_split = _amazon_classify_split_layout_files(split_dirs)
        if not trans_files:
            trans_files = trans_split
        if not repo_files:
            repo_files = repo_split

    if not trans_files or not repo_files:
        return pd.DataFrame()

    # Vendas esperadas (Amazon Transações).
    vendas_parts: list[pd.DataFrame] = []
    for file_rank, path in enumerate(trans_files):
        raw = read_sales_file(path)
        col_pedido = _find_col(raw, {"ID do pedido", "Order ID"})
        col_tipo = _find_col(raw, {"Tipo de transação", "Tipo de transacao", "Transaction type"})
        col_total = _find_col(raw, {"(total) (BRL)", "total brl", "total"})
        if not col_pedido or not col_tipo or not col_total:
            continue
        part = pd.DataFrame()
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["tipo"] = raw[col_tipo].fillna("").astype(str).str.strip()
        part["Total BRL"] = parse_brl_number(raw[col_total])
        part = part[
            part["N° de venda"].ne("")
            & part["tipo"].str.contains("Pagamento do pedido", case=False, na=False)
            & part["Total BRL"].notna()
        ].copy()
        if part.empty:
            continue
        part = part.groupby("N° de venda", as_index=False)["Total BRL"].sum(min_count=1)
        part["_file_rank"] = file_rank
        vendas_parts.append(part)
    if not vendas_parts:
        return pd.DataFrame()
    vendas = pd.concat(vendas_parts, ignore_index=True).sort_values(
        ["N° de venda", "_file_rank"], kind="stable"
    )
    vendas = vendas.drop_duplicates(subset=["N° de venda"], keep="first")
    vendas = vendas.drop(columns=["_file_rank"], errors="ignore")

    # Pagamentos (Amazon Repositório / extrato).
    pag_parts: list[pd.DataFrame] = []
    for file_rank, path in enumerate(repo_files):
        if path.suffix.lower() in {".xlsx", ".xls"}:
            raw = pd.read_excel(path, dtype=str)
        else:
            raw = _read_amazon_repo_file(path)
        col_pedido = _find_col(raw, {"id do pedido", "ID do pedido", "order id"})
        col_data = _find_col(raw, {"data/hora", "data hora", "date"})
        col_tipo = _find_col(raw, {"tipo", "type"})
        col_total = _find_col(raw, {"total", "(total) (BRL)", "total brl"})
        if not col_pedido or not col_total:
            continue
        part = pd.DataFrame()
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["tipo"] = raw[col_tipo].fillna("").astype(str).str.strip() if col_tipo else ""
        part["Data de pagamento"] = (
            _parse_amazon_repo_datetime_series(raw[col_data]) if col_data else pd.NaT
        )
        part["Valor pago"] = parse_brl_number(raw[col_total])
        part = part[part["N° de venda"].ne("") & part["Valor pago"].notna()].copy()
        # Extrato Amazon inclui taxas/frete/reembolsos; para pagamento da venda,
        # considera apenas lançamentos de pedido com crédito positivo.
        part = part[
            part["tipo"].str.contains("Pedido", case=False, na=False)
            & (pd.to_numeric(part["Valor pago"], errors="coerce") > 0)
        ].copy()
        if part.empty:
            continue
        part = part.groupby("N° de venda", as_index=False).agg(
            {"Data de pagamento": "min", "Valor pago": "sum"}
        )
        part["_file_rank"] = file_rank
        pag_parts.append(part)
    if not pag_parts:
        return pd.DataFrame()
    pagamentos = pd.concat(pag_parts, ignore_index=True).sort_values(
        ["N° de venda", "_file_rank"], kind="stable"
    )
    pagamentos = pagamentos.drop_duplicates(subset=["N° de venda"], keep="first")
    pagamentos = pagamentos.drop(columns=["_file_rank"], errors="ignore")

    c = vendas.merge(pagamentos, how="left", on="N° de venda")
    c["Valor pago"] = pd.to_numeric(c["Valor pago"], errors="coerce").round(2)
    c["Tem pagamento"] = (c["Valor pago"].notna() & (c["Valor pago"] > 0)).map(
        {True: "Sim", False: "Não"}
    )
    c["Diferença"] = c["Total BRL"] - c["Valor pago"]
    c.loc[c["Valor pago"].isna(), "Diferença"] = pd.NA
    c["Status financeiro"] = classificar_status_financeiro(c)
    c["Chave usada"] = "ID do pedido"
    c["Plataforma"] = "Amazon"
    return c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
            "Plataforma",
        ]
    ].copy()


def _build_conciliacao_shopee(base_dir: str | Path) -> pd.DataFrame:
    root = Path(base_dir)
    pastas_vendas = _discover_shopee_dirs(
        root,
        ("Vendas_Shopee", "Vendas Shopee"),
        ("vendas", "shopee"),
    )
    pastas_lib = _discover_shopee_dirs(
        root,
        ("Liberações_Shopee", "Liberacoes_Shopee", "Liberações Shopee", "Liberacoes Shopee"),
        ("libera", "shopee"),
    )
    if not pastas_vendas:
        return pd.DataFrame()

    vendas_files = _union_sales_files_sorted(pastas_vendas)
    vendas_parts: list[pd.DataFrame] = []
    for file_rank, path in enumerate(vendas_files):
        raw = read_sales_file(path)
        col_pedido = _find_col(raw, {"ID do pedido", "Order ID"})
        if not col_pedido:
            continue

        s_subtotal = parse_brl_number(raw[_find_col(raw, {"Subtotal do produto"})]) if _find_col(raw, {"Subtotal do produto"}) else pd.Series(pd.NA, index=raw.index)
        s_frete_comp = parse_brl_number(raw[_find_col(raw, {"Taxa de envio pagas pelo comprador", "Taxa de frete paga pelo comprador"})]) if _find_col(raw, {"Taxa de envio pagas pelo comprador", "Taxa de frete paga pelo comprador"}) else pd.Series(pd.NA, index=raw.index)
        s_desc_frete = parse_brl_number(raw[_find_col(raw, {"Desconto de Frete Aproximado", "Desconto de frete pela Shopee"})]) if _find_col(raw, {"Desconto de Frete Aproximado", "Desconto de frete pela Shopee"}) else pd.Series(pd.NA, index=raw.index)
        s_taxa_trans = parse_brl_number(raw[_find_col(raw, {"Taxa de transação"})]) if _find_col(raw, {"Taxa de transação"}) else pd.Series(pd.NA, index=raw.index)
        s_taxa_com = parse_brl_number(raw[_find_col(raw, {"Taxa de comissão líquida", "Net Commission Fee"})]) if _find_col(raw, {"Taxa de comissão líquida", "Net Commission Fee"}) else pd.Series(pd.NA, index=raw.index)
        s_taxa_serv = parse_brl_number(raw[_find_col(raw, {"Taxa de serviço líquida", "Service Fee"})]) if _find_col(raw, {"Taxa de serviço líquida", "Service Fee"}) else pd.Series(pd.NA, index=raw.index)
        s_aj_acao = parse_brl_number(raw[_find_col(raw, {"Ajuste por participação em ação comercial"})]) if _find_col(raw, {"Ajuste por participação em ação comercial"}) else pd.Series(0.0, index=raw.index)

        col_total_fallback = _find_col(
            raw,
            {
                "Total global",
                "Valor Total",
                "Quantia total lançada (R$)",
                "Quantia total lancada (R$)",
                "Seller Amount",
                "Net Credit Amount",
            },
        )
        s_fallback = (
            parse_brl_number(raw[col_total_fallback])
            if col_total_fallback
            else pd.Series(pd.NA, index=raw.index)
        )
        s_valor_total = (
            parse_brl_number(raw[_find_col(raw, {"Valor Total"})])
            if _find_col(raw, {"Valor Total"})
            else pd.Series(pd.NA, index=raw.index)
        )
        s_total_global = (
            parse_brl_number(raw[_find_col(raw, {"Total global"})])
            if _find_col(raw, {"Total global"})
            else pd.Series(pd.NA, index=raw.index)
        )

        # Candidato líquido conservador: subtotal - taxas/ajustes (encargos costumam vir positivos no export).
        s_fees = (
            s_taxa_trans.fillna(0)
            + s_taxa_com.fillna(0)
            + s_taxa_serv.fillna(0)
            + s_aj_acao.fillna(0)
        )
        s_formula = s_subtotal.fillna(s_valor_total).fillna(s_total_global) - s_fees

        # Escolhe o menor candidato disponível para evitar sobrestimar «Valor a receber».
        s_expected = pd.concat([s_formula, s_subtotal, s_total_global, s_valor_total, s_fallback], axis=1).min(
            axis=1, skipna=True
        )

        part = pd.DataFrame(index=raw.index)
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["Total BRL"] = s_expected
        part = part[part["N° de venda"].ne("") & part["Total BRL"].notna()].copy()
        if part.empty:
            continue
        # Um pedido pode aparecer em múltiplas linhas (itens). Mantém 1 valor por pedido no ficheiro.
        part = part.groupby("N° de venda", as_index=False)["Total BRL"].max()
        part["_file_rank"] = file_rank
        vendas_parts.append(part)
    if not vendas_parts:
        return pd.DataFrame()
    vendas = pd.concat(vendas_parts, ignore_index=True).sort_values(
        ["N° de venda", "_file_rank"], kind="stable"
    )
    # Ficheiros mais novos primeiro: evita duplicar pedidos em exports sobrepostos.
    vendas = vendas.drop_duplicates(subset=["N° de venda"], keep="first")
    vendas = vendas.drop(columns=["_file_rank"], errors="ignore")

    lib_parts: list[pd.DataFrame] = []
    lib_files = _union_liberacoes_files_sorted(pastas_lib)
    for file_rank, path in enumerate(lib_files):
        raw = read_shopee_liberacoes_input_file(path)
        col_pedido = _find_col(
            raw,
            {"ID do pedido", "Order ID", "EXTERNAL_REFERENCE", "External Reference"},
        )
        col_data = _find_col(
            raw,
            {
                "Data de conclusão do pagamento",
                "Data de conclusao do pagamento",
                "Date",
                "Payment Date",
            },
        )
        col_valor = _find_col(
            raw,
            {
                "Quantia total lançada (R$)",
                "Quantia total lancada (R$)",
                "Valor pago",
                "NET_CREDIT_AMOUNT",
                "Seller Amount",
            },
        )
        if not col_pedido or not col_data or not col_valor:
            continue
        part = pd.DataFrame()
        part["N° de venda"] = raw[col_pedido].fillna("").astype(str).str.strip()
        part["Data de pagamento"] = pd.to_datetime(
            raw[col_data], errors="coerce", dayfirst=True, format="mixed"
        )
        part["Valor pago"] = parse_brl_number(raw[col_valor])
        part = part[part["N° de venda"].ne("")].copy()
        if part.empty:
            continue
        part = _dedup_shopee_liberacao_linhas(part)
        # Consolida por pedido no ficheiro (pode haver múltiplos lançamentos por pedido).
        part = part.groupby("N° de venda", as_index=False).agg(
            {"Data de pagamento": "min", "Valor pago": "sum"}
        )
        part["_file_rank"] = file_rank
        lib_parts.append(part)
    if not lib_parts:
        c = vendas.copy()
        c["Valor pago"] = pd.NA
        c["Data de pagamento"] = pd.NaT
        c["Tem pagamento"] = "Não"
        c["Diferença"] = pd.NA
        c["Status financeiro"] = "Sem pagamento"
        c["Chave usada"] = "ID do pedido"
        c["Plataforma"] = "Shopee"
        return c[
            [
                "N° de venda",
                "Total BRL",
                "Valor pago",
                "Data de pagamento",
                "Chave usada",
                "Tem pagamento",
                "Diferença",
                "Status financeiro",
                "Plataforma",
            ]
        ].copy()
    liberacoes = pd.concat(lib_parts, ignore_index=True).sort_values(
        ["N° de venda", "_file_rank"], kind="stable"
    )
    # Evita duplicar pedidos quando há extratos anuais + mensais com sobreposição.
    liberacoes = liberacoes.drop_duplicates(subset=["N° de venda"], keep="first")
    liberacoes = liberacoes.drop(columns=["_file_rank"], errors="ignore")

    c = vendas.merge(liberacoes, how="left", on="N° de venda")
    c["Valor pago"] = pd.to_numeric(c["Valor pago"], errors="coerce").round(2)
    c["Tem pagamento"] = (c["Valor pago"].notna() & (c["Valor pago"] > 0)).map(
        {True: "Sim", False: "Não"}
    )
    c["Diferença"] = c["Total BRL"] - c["Valor pago"]
    c.loc[c["Valor pago"].isna(), "Diferença"] = pd.NA
    c["Status financeiro"] = classificar_status_financeiro(c)
    c["Chave usada"] = "ID do pedido"
    c["Plataforma"] = "Shopee"
    return c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
            "Plataforma",
        ]
    ].copy()


def build_conciliacao_vendas_liberacoes_validas(base_dir: str | Path) -> pd.DataFrame:
    vendas_tratadas, liberacoes_tratadas, _, _ = carregar_bases_consolidadas(base_dir)

    # liberações válidas: EXTERNAL_REFERENCE não vazio OU PACK_ID não vazio
    lib = liberacoes_tratadas.copy()
    lib["EXTERNAL_REFERENCE"] = lib["EXTERNAL_REFERENCE"].fillna("").astype(str).str.strip()
    lib["PACK_ID"] = lib["PACK_ID"].fillna("").astype(str).str.strip()
    mask_validas = lib["EXTERNAL_REFERENCE"].ne("") | lib["PACK_ID"].ne("")
    liberacoes_validas = lib.loc[mask_validas].copy()

    # agregação por chave para fallback
    agg_ext = (
        liberacoes_validas[liberacoes_validas["EXTERNAL_REFERENCE"].ne("")]
        .groupby("EXTERNAL_REFERENCE", as_index=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .rename(
            columns={
                "EXTERNAL_REFERENCE": "N° de venda",
                "Data de pagamento": "Data de pagamento_EXT",
                "Valor pago": "Valor pago_EXT",
            }
        )
    )

    agg_pack = (
        liberacoes_validas[liberacoes_validas["PACK_ID"].ne("")]
        .groupby("PACK_ID", as_index=False)
        .agg({"Data de pagamento": "min", "Valor pago": "sum"})
        .rename(
            columns={
                "PACK_ID": "N° de venda",
                "Data de pagamento": "Data de pagamento_PACK",
                "Valor pago": "Valor pago_PACK",
            }
        )
    )

    base = vendas_tratadas.copy()
    base["N° de venda"] = base["N° de venda"].fillna("").astype(str).str.strip()

    c = base.merge(agg_ext, how="left", on="N° de venda")
    c = c.merge(agg_pack, how="left", on="N° de venda")

    tem_ext = c["Valor pago_EXT"].notna()
    c["Valor pago"] = c["Valor pago_EXT"].where(tem_ext, c["Valor pago_PACK"])
    c["Valor pago"] = pd.to_numeric(c["Valor pago"], errors="coerce").round(2)
    c["Data de pagamento"] = c["Data de pagamento_EXT"].where(tem_ext, c["Data de pagamento_PACK"])
    c["Chave usada"] = pd.Series(pd.NA, index=c.index, dtype="object")
    c.loc[tem_ext, "Chave usada"] = "EXTERNAL_REFERENCE"
    c.loc[~tem_ext & c["Valor pago_PACK"].notna(), "Chave usada"] = "PACK_ID"

    c["Tem pagamento"] = (c["Valor pago"].notna() & (c["Valor pago"] > 0)).map(
        {True: "Sim", False: "Não"}
    )
    c["Diferença"] = c["Total BRL"] - c["Valor pago"]
    c.loc[c["Valor pago"].isna(), "Diferença"] = pd.NA
    c["Status financeiro"] = classificar_status_financeiro(c)
    c["Plataforma"] = "Mercado Livre"

    conciliacao_vendas_liberacoes_validas = c[
        [
            "N° de venda",
            "Total BRL",
            "Valor pago",
            "Data de pagamento",
            "Chave usada",
            "Tem pagamento",
            "Diferença",
            "Status financeiro",
            "Plataforma",
        ]
    ].copy()
    conc_shopee = _build_conciliacao_shopee(base_dir)
    conc_amazon = _build_conciliacao_amazon(base_dir)
    partes = [conciliacao_vendas_liberacoes_validas]
    if not conc_shopee.empty:
        partes.append(conc_shopee)
    if not conc_amazon.empty:
        partes.append(conc_amazon)
    return pd.concat(partes, ignore_index=True)


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    conc = build_conciliacao_vendas_liberacoes_validas(BASE_DIR)

    total_vendas = int(len(conc))
    vendas_com_pagamento = int(conc["Tem pagamento"].eq("Sim").sum())
    perc = (vendas_com_pagamento / total_vendas * 100.0) if total_vendas else 0.0
    soma_total = float(pd.to_numeric(conc["Total BRL"], errors="coerce").sum())
    soma_pago = float(pd.to_numeric(conc["Valor pago"], errors="coerce").sum())

    ordem = ["Sem pagamento", "Pago correto", "Pago a maior", "Pago a menor"]
    dist = (
        conc["Status financeiro"]
        .value_counts(dropna=False)
        .reindex(ordem, fill_value=0)
        .rename_axis("Status financeiro")
        .reset_index(name="Quantidade")
    )

    print("Head (conciliacao_vendas_liberacoes_validas):")
    print(conc.head(10).to_string(index=False))

    print("\nMétricas:")
    print(f"- Total de vendas: {total_vendas}")
    print(f"- Vendas com pagamento: {vendas_com_pagamento}")
    print(f"- Percentual com pagamento: {perc:.2f}%")
    print(f"- Soma de Total BRL: {soma_total:.2f}")
    print(f"- Soma de Valor pago: {soma_pago:.2f}")

    print("\nClassificação financeira:")
    print(dist.to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

