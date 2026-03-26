#!/usr/bin/env bash
# Estado do Streamlit FDL_Consultoria (Oracle Linux)

set -uo pipefail

REPO_DIR="${REPO_DIR:-$HOME/FDL_Consultoria}"
LOG="${STREAMLIT_LOG:-/tmp/streamlit.log}"
PID_FILE="${STREAMLIT_PID_FILE:-/tmp/streamlit_fdl.pid}"
PORT="${STREAMLIT_PORT:-8501}"
APP="${STREAMLIT_APP:-app_operacional.py}"

echo "=== FDL Consultoria — status ==="
echo "Diretório: $REPO_DIR"
echo "Porta:     $PORT"
echo "Log:       $LOG"
echo

if [[ -f "$PID_FILE" ]]; then
  pid="$(cat "$PID_FILE" 2>/dev/null || echo "")"
  echo "Ficheiro PID: $PID_FILE -> ${pid:-vazio}"
  if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
    echo "Processo PID $pid: ATIVO"
  else
    echo "Processo PID $pid: NÃO EXISTE ou morreu"
  fi
else
  echo "Ficheiro PID: (não existe)"
fi
echo

if command -v ss >/dev/null 2>&1; then
  echo "Sockets em $PORT (ss):"
  ss -lntp 2>/dev/null | grep ":$PORT" || echo "  (nenhum listener em :$PORT)"
elif command -v netstat >/dev/null 2>&1; then
  echo "Sockets em $PORT (netstat):"
  netstat -lntp 2>/dev/null | grep ":$PORT" || echo "  (nenhum listener em :$PORT)"
fi
echo

echo "Processos streamlit ($APP):"
pgrep -af "streamlit.*${APP}" 2>/dev/null || echo "  (nenhum)"
echo

if [[ -f "$LOG" ]]; then
  echo "Últimas 25 linhas de $LOG:"
  echo "---"
  tail -n 25 "$LOG"
  echo "---"
else
  echo "Log $LOG ainda não existe."
fi

echo
echo "Teste HTTP local (se curl existir):"
if command -v curl >/dev/null 2>&1; then
  if curl -sf -o /dev/null -m 5 "http://127.0.0.1:${PORT}/"; then
    echo "  http://127.0.0.1:${PORT}/ -> OK"
  else
    echo "  http://127.0.0.1:${PORT}/ -> sem resposta ou erro"
  fi
else
  echo "  (curl não instalado; ignorar)"
fi
