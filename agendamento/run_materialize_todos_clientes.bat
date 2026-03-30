@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "ROOT=%~dp0.."
cd /d "%ROOT%"

if exist "%~dp0config_local.bat" call "%~dp0config_local.bat"

set "LOGDIR=%~dp0logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" 2>nul
set "LOG=%LOGDIR%\materialize_todos_clientes.log"

echo. >> "%LOG%"
echo ========== %date% %time% INICIO materialize_todos_clientes ========== >> "%LOG%"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0run_materialize_todos_clientes.ps1" -RepoRoot "%ROOT%" 1>> "%LOG%" 2>>&1
set "EC=!errorlevel!"
if not "!EC!"=="0" (
  echo ========== %date% %time% FIM ERRO codigo !EC! ========== >> "%LOG%"
  exit /b !EC!
)
echo ========== %date% %time% FIM OK ========== >> "%LOG%"
exit /b 0
