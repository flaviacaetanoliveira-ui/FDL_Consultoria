"""
Runner externo para processing/materialize_financeiro.py (fora do Streamlit).

- Lock para evitar execuções em paralelo (com remoção de lock obsoleto).
- Log simples em logs/materialize_runner.log
- Repassa todos os argumentos ao materializador (mesma CLI).

Agendador de Tarefas (Windows):
  Ação: Iniciar programa
  Programa: C:\\caminho\\para\\python.exe
  Argumentos: "C:\\...\\V2\\run_materialize_financeiro.py" --base-dir "C:\\...\\cliente_1" --cliente default --empresa antomoveis --modulo all
  Iniciar em: C:\\...\\V2

Ou use run_materialize_financeiro.bat na pasta do repositório.
"""
from __future__ import annotations

import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent
MATERIALIZE_SCRIPT = REPO_ROOT / "processing" / "materialize_financeiro.py"
LOG_DIR = REPO_ROOT / "logs"
LOCK_PATH = LOG_DIR / ".materialize_financeiro.lock"
# Se o processo morrer sem libertar o lock, após este tempo (s) o lock é considerado obsoleto.
_STALE_LOCK_SEC = int(os.environ.get("FDL_MATERIALIZE_LOCK_STALE_SECONDS", "86400"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _log_line(path: Path, message: str) -> None:
    line = f"{_now_iso()}\t{message}\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(line)
    sys.stdout.write(line)


def _try_acquire_lock() -> bool:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    if LOCK_PATH.is_file():
        try:
            age = time.time() - LOCK_PATH.stat().st_mtime
        except OSError:
            age = 0
        if age > _STALE_LOCK_SEC:
            try:
                LOCK_PATH.unlink()
            except OSError:
                pass
        else:
            try:
                pid_txt = LOCK_PATH.read_text(encoding="utf-8", errors="replace").strip()
            except OSError:
                pid_txt = ""
            _log_line(
                LOG_DIR / "materialize_runner.log",
                f"SKIP lock_at={LOCK_PATH} pid_hint={pid_txt!r} (outra execução ou lock recente; "
                f"stale após {_STALE_LOCK_SEC}s)",
            )
            return False
    try:
        fd = os.open(LOCK_PATH, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
        try:
            os.write(fd, str(os.getpid()).encode("ascii", errors="replace"))
        finally:
            os.close(fd)
    except FileExistsError:
        _log_line(
            LOG_DIR / "materialize_runner.log",
            "SKIP lock_exists (execução concorrente ou falha ao remover lock obsoleto)",
        )
        return False
    return True


def _release_lock() -> None:
    try:
        LOCK_PATH.unlink(missing_ok=True)
    except OSError:
        pass


def main() -> int:
    log_path = LOG_DIR / "materialize_runner.log"
    if not MATERIALIZE_SCRIPT.is_file():
        _log_line(log_path, f"FAIL script_not_found={MATERIALIZE_SCRIPT}")
        return 1

    if not _try_acquire_lock():
        return 2

    cmd = [sys.executable, str(MATERIALIZE_SCRIPT), *sys.argv[1:]]
    _log_line(log_path, f"START pid={os.getpid()} cmd={' '.join(cmd)!r} cwd={REPO_ROOT}")
    env = os.environ.copy()
    env.setdefault("PYTHONUTF8", "1")
    try:
        proc = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            env=env,
        )
        code = int(proc.returncode)
        if code == 0:
            _log_line(log_path, f"END success exit={code}")
        else:
            _log_line(log_path, f"END failure exit={code}")
        return code
    except Exception as exc:
        _log_line(log_path, f"FAIL exception={exc!r}")
        return 1
    finally:
        _release_lock()


if __name__ == "__main__":
    raise SystemExit(main())
