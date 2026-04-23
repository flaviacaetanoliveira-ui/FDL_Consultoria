"""Gera ``_render_faturamento_dre_nf_table_section`` a partir do bloco atual de ``app_operacional.py``."""
from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "app_operacional.py"
OUT = ROOT / "_nf_table_section_generated.py"

lines = SRC.read_text(encoding="utf-8").splitlines()
# Linhas 7928–8768 (1-based) → índice [7927:8768]
chunk_lines = lines[7927:8768]
chunk = "\n".join(line[4:] if line.startswith("    ") else line for line in chunk_lines)

chunk = chunk.replace("file_name=\"faturamento_recorte_minimo_nf.csv\"", "file_name=csv_file_name")
chunk = chunk.replace("st.markdown(\"### Tabela por NF\")", "st.markdown(table_heading)")

# Ordem: padrões mais específicos primeiro
chunk = re.sub(r'key=f"fdl_fat_min_dl_hdr_\{_oid\}"', r'key=f"{prefix_main}_dl_hdr_{_oid}"', chunk)

chunk = re.sub(r'st\.session_state\.get\("fdl_fat_min_([^"]+)"', r'st.session_state.get(f"{prefix_main}_\1"', chunk)
chunk = re.sub(r'st\.session_state\.get\("fdl_fat_nf_([^"]+)"', r'st.session_state.get(f"{prefix_nf}_\1"', chunk)

chunk = re.sub(r'st\.session_state\.pop\("fdl_fat_min_([^"]+)"', r'st.session_state.pop(f"{prefix_main}_\1"', chunk)

chunk = re.sub(r'st\.session_state\["fdl_fat_min_([^"]+)"\]', r'st.session_state[f"{prefix_main}_\1"]', chunk)
chunk = re.sub(r'st\.session_state\["fdl_fat_nf_([^"]+)"\]', r'st.session_state[f"{prefix_nf}_\1"]', chunk)

chunk = re.sub(r'if "fdl_fat_nf_tbl_plataforma" not in', r'if f"{prefix_nf}_tbl_plataforma" not in', chunk)

chunk = re.sub(r'key="fdl_fat_min_([^"]+)"', r'key=f"{prefix_main}_\1"', chunk)
chunk = re.sub(r'key="fdl_fat_nf_([^"]+)"', r'key=f"{prefix_nf}_\1"', chunk)

chunk = re.sub(r'_multiselect_stable\(\s*"fdl_fat_min_prod"', r'_multiselect_stable(\n                f"{prefix_main}_prod"', chunk)

chunk = re.sub(r'\("fdl_fat_nf_opt_([^"]+)",', r'(f"{prefix_nf}_opt_\1",', chunk)

chunk = re.sub(r'_k_sinais = "fdl_fat_min_sinais_resultado"', r'_k_sinais = f"{prefix_main}_sinais_resultado"', chunk)

lines = SRC.read_text(encoding="utf-8").splitlines()
chunk_lines = lines[7927:8768]
chunk = "\n".join(line[4:] if line.startswith("    ") else line for line in chunk_lines)

chunk = chunk.replace("file_name=\"faturamento_recorte_minimo_nf.csv\"", "file_name=csv_file_name")
chunk = chunk.replace("st.markdown(\"### Tabela por NF\")", "st.markdown(table_heading)")

chunk = re.sub(r'key=f"fdl_fat_min_dl_hdr_\{_oid\}"', r'key=f"{prefix_main}_dl_hdr_{_oid}"', chunk)
chunk = re.sub(r'st\.session_state\.get\("fdl_fat_min_([^"]+)"', r'st.session_state.get(f"{prefix_main}_\1"', chunk)
chunk = re.sub(r'st\.session_state\.get\("fdl_fat_nf_([^"]+)"', r'st.session_state.get(f"{prefix_nf}_\1"', chunk)
chunk = re.sub(r'st\.session_state\.pop\("fdl_fat_min_([^"]+)"', r'st.session_state.pop(f"{prefix_main}_\1"', chunk)
chunk = re.sub(r'st\.session_state\["fdl_fat_min_([^"]+)"\]', r'st.session_state[f"{prefix_main}_\1"]', chunk)
chunk = re.sub(r'st\.session_state\["fdl_fat_nf_([^"]+)"\]', r'st.session_state[f"{prefix_nf}_\1"]', chunk)
chunk = re.sub(r'if "fdl_fat_nf_tbl_plataforma" not in', r'if f"{prefix_nf}_tbl_plataforma" not in', chunk)
chunk = re.sub(r'key="fdl_fat_min_([^"]+)"', r'key=f"{prefix_main}_\1"', chunk)
chunk = re.sub(r'key="fdl_fat_nf_([^"]+)"', r'key=f"{prefix_nf}_\1"', chunk)
chunk = re.sub(
    r'_multiselect_stable\(\s*\n\s*"fdl_fat_min_prod"',
    r'_multiselect_stable(\n                f"{prefix_main}_prod"',
    chunk,
)
chunk = re.sub(r'_multiselect_stable\(\s*"fdl_fat_min_prod"', r'_multiselect_stable(\n                f"{prefix_main}_prod"', chunk)
chunk = re.sub(r'\("fdl_fat_nf_opt_([^"]+)",', r'(f"{prefix_nf}_opt_\1",', chunk)
chunk = re.sub(r'_k_sinais = "fdl_fat_min_sinais_resultado"', r'_k_sinais = f"{prefix_main}_sinais_resultado"', chunk)

# for x in (st.session_state.get("fdl_fat_min_prod")
chunk = re.sub(
    r'for x in \(st\.session_state\.get\("fdl_fat_min_prod"\)',
    r'for x in (st.session_state.get(f"{prefix_main}_prod")',
    chunk,
)

# bool(st.session_state.get("fdl_fat_nf_opt_ - already covered by get(
bad = [
    ln
    for ln in chunk.splitlines()
    if '"fdl_fat_min' in ln or '"fdl_fat_nf' in ln or "'fdl_fat_min" in ln or "'fdl_fat_nf" in ln
]
if bad:
    OUT.write_text(chunk, encoding="utf-8")
    raise SystemExit("still have quoted fdl_fat session keys:\n" + "\n".join(bad[:50]))

HDR = '''
def _render_faturamento_dre_nf_table_section(
    *,
    df_nf_pre: pd.DataFrame,
    df: pd.DataFrame,
    df_fiscal_pre: pd.DataFrame,
    load_info: dict[str, object],
    _min_state: "FaturamentoRecorteMinState",
    _nf_kpi_ini: object,
    _nf_kpi_fim: object,
    ok_nf_dates: bool,
    use_fiscal_kpi: bool,
    use_nf_materializado: bool,
    use_fiscal_parquet: bool,
    _nf_panel_ads_ui: bool,
    _df_fiscal_base: pd.DataFrame,
    _fiscal_base_stats: "FaturamentoFiscalBaseStats",
    _kp_cards: dict[str, float | int],
    org_id: str,
    prefix_main: str,
    prefix_nf: str,
    csv_file_name: str = "faturamento_recorte_minimo_nf.csv",
    table_heading: str = "### Tabela por NF",
) -> None:
    """Tabela por NF (filtros inline, CSV, paginação) — usada em Faturamento & DRE e Apuração Fiscal."""
    _oid = str(org_id)
'''

body = "\n".join("    " + ln if ln.strip() else ln for ln in chunk.splitlines())
OUT.write_text(HDR + body + "\n", encoding="utf-8")
print("Wrote", OUT, "lines", len(OUT.read_text(encoding="utf-8").splitlines()))
