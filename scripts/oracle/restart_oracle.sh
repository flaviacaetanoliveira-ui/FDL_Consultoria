#!/usr/bin/env bash
# Reinicia apenas o Streamlit (usa venv e código já existentes em REPO_DIR).

set -euo pipefail

REPO_DIR="${REPO_DIR:-$HOME/FDL_Consultoria}"
VENV="$REPO_DIR/venv"
LOG="${STREAMLIT_LOG:-/tmp/streamlit.log}"
PID_FILE="${STREAMLIT_PID_FILE:-/tmp/streamlit_fdl.pid}"
PORT="${STREAMLIT_PORT:-8501}"
APP="${STREAMLIT_APP:-app_operacional.py}"

log() { printf '[%s] %s\n' "$(date -Iseconds)" "$*"; }

if [[ ! -d "$VENV" ]]; then
  log "ERRO: venv não encontrado em $VENV. Execute primeiro: bash deploy_oracle.sh"
  exit 1
fi

if [[ ! -f "$REPO_DIR/$APP" ]]; then
  log "ERRO: $REPO_DIR/$APP não encontrado."
  exit 1
fi

stop_streamlit() {
  if [[ -f "$PID_FILE" ]]; then
    local old
    old="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$old" ]] && kill -0 "$old" 2>/dev/null; then
      log "A terminar PID $old..."
      kill "$old" 2>/dev/null || true
    fi
    rm -f "$PID_FILE"
  fi
  pkill -f "[s]treamlit run.*${APP}" 2>/dev/null || true
  sleep 1
}

log "A reiniciar Streamlit..."
stop_streamlit

: >>"$LOG"
cd "$REPO_DIR"
nohup "$VENV/bin/streamlit" run "$APP" \
  --server.port "$PORT" \
  --server.address 0.0.0.0 \
  >>"$LOG" 2>&1 &
echo $! >"$PID_FILE"

sleep 2
if kill -0 "$(cat "$PID_FILE")" 2>/dev/null; then
  log "Streamlit reiniciado (PID $(cat "$PID_FILE"))."
else
  log "ERRO: falha ao reiniciar. Ver $LOG"
  tail -n 30 "$LOG" || true
  exit 1
fi
