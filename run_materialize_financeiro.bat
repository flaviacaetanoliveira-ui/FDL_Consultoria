@echo off
setlocal EnableExtensions
REM Coloque este ficheiro na raiz do repositório V2.
REM Task Scheduler: Programa = caminho completo deste .bat | "Iniciar em" = pasta do repo (opcional; o .bat faz cd para a pasta dele).

cd /d "%~dp0"
python "%~dp0run_materialize_financeiro.py" %*
exit /b %ERRORLEVEL%
