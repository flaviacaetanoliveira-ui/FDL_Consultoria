"""
Garante que o bundle Frete ↔ App está alinhado (evita ImportError na Streamlit Cloud).

Executar na raiz do repositório:
    python scripts/verify_frete_streamlit_deploy.py

Falha com código ≠0 se faltar `frete_vendas_loader_args`, `FontesFrete.vendas_paths`, etc.

Nota: não importa `app_operacional` em execução (o módulo corre autenticação ao carregar);
valida-se o `from operacional_frete import (...)` no ficheiro com AST.
"""
from __future__ import annotations

import ast
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def main() -> int:
    import operacional_frete as of

    if not hasattr(of, "frete_vendas_loader_args") or not callable(of.frete_vendas_loader_args):
        print("FAIL: operacional_frete.frete_vendas_loader_args em falta ou não é callable", file=sys.stderr)
        return 1
    if not hasattr(of, "FontesFrete"):
        print("FAIL: operacional_frete.FontesFrete em falta", file=sys.stderr)
        return 1
    fields = getattr(of.FontesFrete, "_fields", ())
    if "vendas_paths" not in fields:
        print("FAIL: FontesFrete sem campo vendas_paths (deploy parcial?)", file=sys.stderr)
        return 1
    if not hasattr(of, "descobrir_fontes_frete") or not callable(of.descobrir_fontes_frete):
        print("FAIL: operacional_frete.descobrir_fontes_frete em falta", file=sys.stderr)
        return 1

    try:
        import operacional_frete_ui  # noqa: F401
    except ImportError as e:
        print(f"FAIL: operacional_frete_ui não importa: {e}", file=sys.stderr)
        return 1

    app_path = ROOT / "app_operacional.py"
    if not app_path.is_file():
        print("FAIL: app_operacional.py em falta", file=sys.stderr)
        return 1
    tree = ast.parse(app_path.read_text(encoding="utf-8"), filename=str(app_path))
    frete_names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "operacional_frete":
            for alias in node.names:
                frete_names.add(alias.name)
    if "frete_vendas_loader_args" not in frete_names:
        print(
            "FAIL: app_operacional.py não importa frete_vendas_loader_args de operacional_frete",
            file=sys.stderr,
        )
        return 1

    print("OK: operacional_frete + UI + import em app_operacional.py estão alinhados.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
