#!/usr/bin/env bash
# Deploy idempotente: Oracle Linux, Streamlit FDL_Consultoria
# Não executa dnf update (apenas instala pacotes listados).

set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/flaviacaetanoliveira-ui/FDL_Consultoria.git}"
REPO_DIR="${REPO_DIR:-$HOME/FDL_Consultoria}"
VENV="$REPO_DIR/venv"
LOG="${STREAMLIT_LOG:-/tmp/streamlit.log}"
PID_FILE="${STREAMLIT_PID_FILE:-/tmp/streamlit_fdl.pid}"
PORT="${STREAMLIT_PORT:-8501}"
APP="${STREAMLIT_APP:-app_operacional.py}"

log() { printf '[%s] %s\n' "$(date -Iseconds)" "$*"; }

require_sudo_noninteractive() {
  if ! sudo -n true 2>/dev/null; then
    log "ERRO: sudo precisa funcionar sem senha (NOPASSWD) para dnf install, ou execute este script num terminal onde possa introduzir a palavra-passe."
    exit 1
  fi
}

stop_streamlit() {
  if [[ -f "$PID_FILE" ]]; then
    local old
    old="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old" ]] && kill -0 "$old" 2>/dev/null; then
      log "A terminar processo antigo (PID $old)..."
      kill "$old" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  # Garantir que não ficam instâncias órfãs do mesmo app
  pkill -f "[s]treamlit run.*${APP}" 2>/dev/null || true
  sleep 1
}

log "A instalar pacotes (sem dnf update)..."
require_sudo_noninteractive
sudo -n dnf install -y git python3 python3-pip python3-virtualenv

if [[ -d "$REPO_DIR/.git" ]]; then
  log "Repositório existe; a executar git pull em $REPO_DIR"
  git -C "$REPO_DIR" pull --no-edit
else
  log "A clonar repositório para $REPO_DIR"
  git clone "$REPO_URL" "$REPO_DIR"
fi

log "A criar/atualizar ambiente virtual em $VENV"
python3 -m venv "$VENV"
"$VENV/bin/pip" install -q --upgrade pip
"$VENV/bin/pip" install -r "$REPO_DIR/requirements.txt"

stop_streamlit

log "A iniciar Streamlit em background (porta $PORT, log $LOG)"
: >"$LOG"
cd "$REPO_DIR"
nohup "$VENV/bin/streamlit" run "$APP" \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  >>"$LOG" 2>&1 &
echo $! >"$PID_FILE"

sleep 2
if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  log "Streamlit em execução (PID $(cat "$PID_FILE")). Ver: $LOG"
else
  log "ERRO: o processo não ficou ativo. Últimas linhas do log:"
  tail -n 30 "$LOG" || true
  exit 1
fi
