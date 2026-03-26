from __future__ import annotations

import sys
from pathlib import Path

from carregamento_bases import carregar_bases_consolidadas
from fdl_paths import BASE_DIR


def main() -> int:
    try:
        sys.stdout.reconfigure(encoding="utf-8")
        sys.stderr.reconfigure(encoding="utf-8")
    except Exception:  # noqa: BLE001
        pass

    vendas, liberacoes_t, liberacoes_a, diag = carregar_bases_consolidadas(BASE_DIR)

    print("Diagnóstico consolidado:")
    print(
        f"- Vendas: arquivos={diag.vendas.arquivos_lidos}, linhas={diag.vendas.linhas_tabela}, "
        f"tempo={diag.vendas.tempo_segundos:.3f}s"
    )
    print(
        f"- Liberações tratadas: arquivos={diag.liberacoes_tratadas.arquivos_lidos}, "
        f"linhas={diag.liberacoes_tratadas.linhas_tabela}, "
        f"tempo={diag.liberacoes_tratadas.tempo_segundos:.3f}s"
    )
    print(
        f"- Liberações agregadas: arquivos={diag.liberacoes_agregadas.arquivos_lidos}, "
        f"linhas={diag.liberacoes_agregadas.linhas_tabela}, "
        f"tempo={diag.liberacoes_agregadas.tempo_segundos:.3f}s"
    )
    print(f"- Tempo total: {diag.tempo_total_segundos:.3f}s")

    print("\nHead vendas_tratadas:")
    print(vendas.head(5).to_string(index=False))

    print("\nHead liberacoes_agregadas:")
    print(liberacoes_a.head(5).to_string(index=False))
    _ = liberacoes_t
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

