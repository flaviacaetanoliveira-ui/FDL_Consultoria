"""
Lock exclusivo para materialize_financeiro (evita duas execuções em paralelo).

Lock órfão: removido se mais velho que STALE_MAX_AGE_SEC (2 h), alinhado a faturamento.config.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

STALE_MAX_AGE_SEC = 7200


class MaterializeLockError(RuntimeError):
    pass


def _default_lock_path(repo_root: Path) -> Path:
    return repo_root / "agendamento" / "locks" / "materialize_financeiro.lock"


def acquire_materialize_lock(repo_root: Path, *, lock_path: Path | None = None) -> Path:
    path = lock_path or _default_lock_path(repo_root)
    path.parent.mkdir(parents=True, exist_ok=True)
    for _attempt in range(3):
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            try:
                payload = f"pid={os.getpid()}\n".encode("utf-8")
                os.write(fd, payload)
            finally:
                os.close(fd)
            return path
        except FileExistsError:
            try:
                age = time.time() - path.stat().st_mtime
            except OSError:
                age = STALE_MAX_AGE_SEC + 1
            if age > STALE_MAX_AGE_SEC:
                try:
                    path.unlink()
                except OSError:
                    continue
                continue
            raise MaterializeLockError(
                f"Outra materialização parece estar em execução. Remova o lock se for órfão: {path}"
            ) from None
    raise MaterializeLockError(f"Não foi possível adquirir lock: {path}")


def release_materialize_lock(lock_path: Path) -> None:
    try:
        lock_path.unlink(missing_ok=True)
    except OSError:
        pass
