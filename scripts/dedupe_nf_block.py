from pathlib import Path

p = Path("app_operacional.py")
text = p.read_text(encoding="utf-8")
needle = (
    "    # Só chegamos aqui com painel materializado válido: recorte = filtrar linhas já agregadas (sem recomputar DRE)."
)
positions: list[int] = []
start = 0
while True:
    i = text.find(needle, start)
    if i < 0:
        break
    positions.append(i)
    start = i + 1
if len(positions) != 2:
    raise SystemExit(f"expected 2 occurrences, got {len(positions)}")
second = positions[1]
marker_end = "\n    _fdl_fat_section_rule(\"Cobertura\")"
end = text.find(marker_end, second)
if end < 0:
    raise SystemExit("end marker not found")
replacement = """
    _render_faturamento_dre_nf_table_section(
        df_nf_pre=df_nf_pre,
        df=df,
        df_fiscal_pre=df_fiscal_pre,
        load_info=load_info,
        _min_state=_min_state,
        _nf_kpi_ini=_nf_kpi_ini,
        _nf_kpi_fim=_nf_kpi_fim,
        ok_nf_dates=ok_nf_dates,
        use_fiscal_kpi=use_fiscal_kpi,
        use_nf_materializado=use_nf_materializado,
        use_fiscal_parquet=use_fiscal_parquet,
        _nf_panel_ads_ui=_nf_panel_ads_ui,
        _df_fiscal_base=_df_fiscal_base,
        _fiscal_base_stats=_fiscal_base_stats,
        _kp_cards=_kp_cards,
        org_id=org_id,
        prefix_main="fdl_fat_min",
        prefix_nf="fdl_fat_nf",
    )
"""
new_text = text[:second] + replacement.lstrip("\n") + text[end:]
p.write_text(new_text, encoding="utf-8")
print("replaced", len(text) - len(new_text), "chars")
