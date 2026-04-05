"""Recorte repasse: pagamento ou emissão quando pagamento vazio."""
from __future__ import annotations

from datetime import date

import pandas as pd

from repasse_period_filter import repasse_mascara_periodo_pagamento_ou_emissao


def test_shopee_sem_pagamento_entra_por_emissao_2026():
    df = pd.DataFrame(
        {
            "Data de pagamento": ["", ""],
            "Data de emissão": ["2025-06-01", "2026-01-05"],
        }
    )
    m = repasse_mascara_periodo_pagamento_ou_emissao(df, date(2026, 1, 1), date(2026, 1, 31))
    assert m.tolist() == [False, True]


def test_com_pagamento_usa_somente_pagamento():
    df = pd.DataFrame(
        {
            "Data de pagamento": ["2025-12-01 10:00:00", "2026-01-10 12:00:00"],
            "Data de emissão": ["2026-01-05", "2025-01-01"],
        }
    )
    m = repasse_mascara_periodo_pagamento_ou_emissao(df, date(2026, 1, 1), date(2026, 1, 31))
    assert m.tolist() == [False, True]


def test_pagamento_fora_mas_emissao_dentro_exclui_se_tem_pagamento():
    df = pd.DataFrame(
        {
            "Data de pagamento": ["2025-01-01 00:00:00"],
            "Data de emissão": ["2026-01-05"],
        }
    )
    m = repasse_mascara_periodo_pagamento_ou_emissao(df, date(2026, 1, 1), date(2026, 1, 31))
    assert m.tolist() == [False]


def test_sem_nenhuma_data_mostra_tudo():
    df = pd.DataFrame(
        {
            "Data de pagamento": [""],
            "Data de emissão": [""],
        }
    )
    m = repasse_mascara_periodo_pagamento_ou_emissao(df, date(2026, 1, 1), date(2026, 1, 31))
    assert bool(m.all())
