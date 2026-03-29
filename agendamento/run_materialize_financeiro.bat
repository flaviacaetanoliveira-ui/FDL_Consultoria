@echo off
REM Materializa repasse, frete e opcionalmente faturamento em data_products/.../current/.
REM FDL_BASE_DIR obrigatorio. FDL_FATURAMENTO_PARAMS opcional (inclui faturamento no all).
REM Task Scheduler: cmd /c "C:\...\V2\agendamento\run_materialize_financeiro.bat" — Iniciar em: C:\...\V2
setlocal EnableExtensions EnableDelayedExpansion
set "ROOT=%~dp0.."
cd /d "%ROOT%"

if exist "%~dp0config_local.bat" call "%~dp0config_local.bat"

set "LOGDIR=%~dp0logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" 2>nul
set "LOG=%LOGDIR%\materialize_financeiro.log"

echo. >> "%LOG%"
echo ========== %date% %time% INICIO materialize_financeiro ========== >> "%LOG%"

if not defined FDL_BASE_DIR (
  echo ERRO: defina FDL_BASE_DIR. >> "%LOG%"
  echo ERRO: defina FDL_BASE_DIR.
  exit /b 1
)

set "MOD=!FDL_MODULO!"
if not defined MOD set "MOD=all"

if defined FDL_FATURAMENTO_PARAMS (
  echo [exec] python materialize_financeiro com faturamento >> "%LOG%"
  python "processing\materialize_financeiro.py" --modulo !MOD! --base-dir "!FDL_BASE_DIR!" --faturamento-params "!FDL_FATURAMENTO_PARAMS!" 1>> "%LOG%" 2>>&1
) else (
  echo [exec] python materialize_financeiro sem FDL_FATURAMENTO_PARAMS >> "%LOG%"
  python "processing\materialize_financeiro.py" --modulo !MOD! --base-dir "!FDL_BASE_DIR!" 1>> "%LOG%" 2>>&1
)
set "EC=!errorlevel!"

if not "!EC!"=="0" (
  echo ========== %date% %time% FIM ERRO codigo !EC! ========== >> "%LOG%"
  exit /b !EC!
)
echo ========== %date% %time% FIM OK ========== >> "%LOG%"
exit /b 0
