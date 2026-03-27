@echo off
REM Rotina: copia vendas ML + frete por anuncio para cliente_1 (se existir) senao data_cliente; opcional export Power BI.
REM Agendador do Windows: apontar para este ficheiro. Opcoes em config_local.bat (ver .example).
setlocal EnableExtensions EnableDelayedExpansion
set "ROOT=%~dp0.."
cd /d "%ROOT%"

if exist "%~dp0config_local.bat" call "%~dp0config_local.bat"

set "LOGDIR=%~dp0logs"
if not exist "%LOGDIR%" mkdir "%LOGDIR%" 2>nul
set "LOG=%LOGDIR%\rotina.log"

echo. >> "%LOG%"
echo ========== %date% %time% INICIO ========== >> "%LOG%"

if exist "%ROOT%\cliente_1\" (
  set "DEST_BASE=%ROOT%\cliente_1"
) else (
  set "DEST_BASE=%ROOT%\data_cliente"
)
set "DEST_VENDAS=%DEST_BASE%\Vendas - Mercado Livre"
if not exist "%DEST_VENDAS%" mkdir "%DEST_VENDAS%" 2>nul

if defined FDL_SYNC_VENDAS_SRC (
  if exist "%FDL_SYNC_VENDAS_SRC%\*" (
    echo [vendas] %FDL_SYNC_VENDAS_SRC% -^> %DEST_VENDAS% >> "%LOG%"
    robocopy "%FDL_SYNC_VENDAS_SRC%" "%DEST_VENDAS%" *.xlsx *.xls *.csv /R:2 /W:5 /NP /NFL /NDL >> "%LOG%"
    if errorlevel 8 (
      echo ERRO robocopy vendas >> "%LOG%"
      exit /b 1
    )
  ) else (
    echo AVISO FDL_SYNC_VENDAS_SRC invalida ou vazia: %FDL_SYNC_VENDAS_SRC% >> "%LOG%"
  )
) else (
  echo INFO FDL_SYNC_VENDAS_SRC nao definido — copia vendas ignorada. >> "%LOG%"
)

if defined FDL_SYNC_FRETE_SRC (
  if exist "%FDL_SYNC_FRETE_SRC%\" (
    echo [frete] pasta %FDL_SYNC_FRETE_SRC% >> "%LOG%"
    robocopy "%FDL_SYNC_FRETE_SRC%" "%DEST_BASE%" *Frete*Anuncio*.xlsx *frete*anuncio*.xlsx *Frete*Anúncio*.xlsx /R:2 /W:5 /NP /NFL /NDL >> "%LOG%"
    if errorlevel 8 (
      echo ERRO robocopy frete >> "%LOG%"
      exit /b 1
    )
  ) else if exist "%FDL_SYNC_FRETE_SRC%" (
    echo [frete] ficheiro unico >> "%LOG%"
    copy /Y "%FDL_SYNC_FRETE_SRC%" "%DEST_BASE%\" >> "%LOG%" 2>&1
    if errorlevel 1 (
      echo ERRO copy frete >> "%LOG%"
      exit /b 1
    )
  ) else (
    echo AVISO FDL_SYNC_FRETE_SRC nao existe: %FDL_SYNC_FRETE_SRC% >> "%LOG%"
  )
) else (
  echo INFO FDL_SYNC_FRETE_SRC nao definido — copia frete ignorada. >> "%LOG%"
)

if /i "%SKIP_POWERBI_EXPORT%"=="1" (
  echo INFO SKIP_POWERBI_EXPORT=1 — export Python ignorado. >> "%LOG%"
  goto :fim_ok
)

if not defined FDL_BASE_DIR set "FDL_BASE_DIR=%DEST_BASE%"
echo FDL_BASE_DIR=!FDL_BASE_DIR! >> "%LOG%"
echo [powerbi_mirror] python export_powerbi_dataset.py >> "%LOG%"
python "powerbi_mirror\export_powerbi_dataset.py" 1>> "%LOG%" 2>>&1
set "EX=%errorlevel%"
if not "!EX!"=="0" (
  echo ERRO export codigo !EX! >> "%LOG%"
  exit /b !EX!
)

:fim_ok
echo ========== %date% %time% FIM OK ========== >> "%LOG%"
exit /b 0
