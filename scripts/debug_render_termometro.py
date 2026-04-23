"""
Debug isolado: apenas o componente ``render_termometro_pace``.

Na raiz do repo:
  streamlit run scripts/debug_render_termometro.py

Se o cartão aparecer aqui mas não em ``app_operacional``, o problema é integração/contexto,
não o HTML/CSS do componente (desde que ambos usem o mesmo Streamlit).
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import streamlit as st

from app.components.termometro_pace import render_termometro_pace
from processing.faturamento.pace_mensal import PaceMensal

st.set_page_config(page_title="Debug Termometro Pace", layout="wide")
st.title("Debug isolado — Termometro de Pace")

pace_mock = PaceMensal(
    mes_referencia="04/2026",
    dia_atual=19,
    dias_totais_periodo=30,
    dias_restantes=11,
    modo="mes_corrente",
    receita_realizada=86878.81,
    pct_meta_realizada=0.688,
    meta_mensal=126292.99,
    meta_origem="ma3",
    projecao_linear=137177.07,
    desvio_projecao_pct=0.086,
    ritmo_atual_diario=4572.57,
    ritmo_necessario_diario=None,
    ajuste_ritmo_necessario_pct=None,
    nivel_alerta="ok_positivo",
    mensagem_alerta=None,
)

st.caption("Chamando render_termometro_pace com mock (mes_corrente + meta MA3).")
render_termometro_pace(pace_mock)
st.caption("Renderizacao concluida.")
