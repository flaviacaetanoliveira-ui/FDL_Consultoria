"""
Materializa repasse, frete, devoluções e faturamento para todos os inquilinos conhecidos no repo.

Uso (na raiz do V2):
  python processing/materialize_all_clients.py

Requisitos:
  - Pastas de dados sob OneDrive FDL (mesma árvore que nos metadata em data_products/.../repasse/current/).
  - Ajuste ``_USER_ROOT`` se o utilizador Windows não for o esperado.

Gera ``ops/_faturamento_params_cliente_2_runtime.json`` quando necessário (gitignored via ops/*_runtime.json).
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MATERIALIZE = REPO_ROOT / "processing" / "materialize_financeiro.py"

# Raiz OneDrive FDL desta máquina (substitui C:\\Users\\diieg\\... nos metadados antigos).
_fdl_root_env = (os.environ.get("FDL_ONEDRIVE_FDL_ROOT") or "").strip()
if _fdl_root_env:
    _USER_ROOT = Path(_fdl_root_env).expanduser().resolve()
else:
    _USER_ROOT = (Path.home() / "OneDrive - FDL Consultoria").resolve()


def _cursor(*parts: str) -> Path:
    return _USER_ROOT / "Cursor" / Path(*parts)


# (base_dir relativo a Cursor/..., cliente, empresa pasta, org_id, display_name empresa no CSV)
TENANTS: list[tuple[Path, str, str, str, str]] = [
    (_cursor("Anto Moveis", "cliente_1"), "default", "antomoveis", "antomoveis", "Antomóveis"),
    (_cursor("Pedro", "Cliente_2", "Gama Home"), "cliente_2", "gama_home", "gama_home", "Gama Home"),
    (_cursor("Pedro", "Cliente_2", "Mega Facil"), "cliente_2", "mega_facil", "mega_facil", "Mega Fácil"),
    (_cursor("Pedro", "Cliente_2", "Mega Star"), "cliente_2", "mega_star", "mega_star", "Mega Star"),
    (_cursor("Pedro", "Cliente_2", "Móveis EAP"), "cliente_2", "moveis_eap", "moveis_eap", "Móveis EAP"),
    (_cursor("Flavio", "Cliente_4", "Esquilo"), "cliente_5", "esquilo", "esquilo", "Esquilo"),
    (_cursor("Flavio", "Cliente_4", "Wood"), "cliente_5", "wood", "wood", "Wood"),
    (_cursor("Thiago", "Cliente_3", "BP Ramiro"), "cliente_thiago", "bp_ramiro", "bp_ramiro", "BP Ramiro"),
    (_cursor("Thiago", "Cliente_3", "FMG"), "cliente_thiago", "fmg", "fmg", "FMG"),
    (_cursor("Thiago", "Cliente_3", "Let Decor"), "cliente_thiago", "let_decor", "let_decor", "Let Decor"),
    (_cursor("Thiago", "Cliente_3", "TB Paio"), "cliente_thiago", "tb_paio", "tb_paio", "TB Paio"),
]


def _run(args: list[str]) -> int:
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    cmd = [sys.executable, str(MATERIALIZE), *args]
    print(f"\n>>> {' '.join(cmd)}", flush=True)
    r = subprocess.run(cmd, cwd=str(REPO_ROOT), env=env)
    return int(r.returncode)


def _ensure_cliente_2_fat_params() -> Path:
    src = REPO_ROOT / "ops" / "faturamento_params_cliente_2_gama_star_eap.json"
    dst = REPO_ROOT / "ops" / "_faturamento_params_cliente_2_runtime.json"
    data = json.loads(src.read_text(encoding="utf-8"))
    root = _cursor("Pedro", "Cliente_2").resolve()
    data["cliente_root"] = str(root).replace("\\", "/")
    dst.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    return dst


def main() -> int:
    if not MATERIALIZE.is_file():
        print("Falta processing/materialize_financeiro.py", file=sys.stderr)
        return 1

    anto_fat = REPO_ROOT / "ops" / "_faturamento_params_antomoveis_runtime.json"
    if not anto_fat.is_file():
        print(
            "Crie ops/_faturamento_params_antomoveis_runtime.json (cliente_root = pasta cliente_1 Antomóveis).",
            file=sys.stderr,
        )
        return 1

    fat5 = REPO_ROOT / "ops" / "faturamento_params_cliente_5_flavio.json"
    if not fat5.is_file():
        print("Falta ops/faturamento_params_cliente_5_flavio.json", file=sys.stderr)
        return 1

    fat2 = _ensure_cliente_2_fat_params()

    for base, cliente, empresa, org_id, ds_emp in TENANTS:
        if not base.is_dir():
            print(f"[SKIP] pasta inexistente: {base}", file=sys.stderr)
            continue

        common = [
            "--no-lock",
            "--base-dir",
            str(base.resolve()),
            "--cliente",
            cliente,
            "--empresa",
            empresa,
            "--org-id",
            org_id,
            "--dataset-empresa",
            ds_emp,
        ]

        if cliente == "default" and empresa == "antomoveis":
            code = _run([*common, "--modulo", "all", "--faturamento-params", str(anto_fat)])
            if code != 0:
                return code
            continue

        if cliente == "cliente_2":
            for mod in ("repasse", "frete", "devolucoes"):
                code = _run([*common, "--modulo", mod])
                if code != 0:
                    return code
            continue

        if cliente == "cliente_5":
            for mod in ("repasse", "frete", "devolucoes"):
                code = _run([*common, "--modulo", mod])
                if code != 0:
                    return code
            continue

        # cliente_thiago — sem params de faturamento no repo
        code = _run([*common, "--modulo", "all"])
        if code != 0:
            return code

    # Faturamento multi-empresa (uma vez por slug)
    code = _run(["--no-lock", "--modulo", "faturamento", "--faturamento-params", str(fat2)])
    if code != 0:
        return code

    code = _run(["--no-lock", "--modulo", "faturamento", "--faturamento-params", str(fat5)])
    if code != 0:
        return code

    print("\n[materialize_all_clients] Concluído com sucesso.", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
