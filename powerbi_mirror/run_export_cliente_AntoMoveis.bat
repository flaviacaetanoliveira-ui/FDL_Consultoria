@echo off
REM Base Anto Moveis (cliente_1) no OneDrive — ajuste se o Explorer mostrar outro caminho.
setlocal
set "FDL_BASE_DIR=C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Anto Moveis\cliente_1"
cd /d "%~dp0\.."

if not exist "%FDL_BASE_DIR%\Vendas - Mercado Livre\" (
  echo ERRO: Pasta nao encontrada:
  echo   %FDL_BASE_DIR%
  echo Confira o caminho no Explorer ^(barra de endereco^) e edite este .bat.
  pause
  exit /b 1
)

echo FDL_BASE_DIR=%FDL_BASE_DIR%
python "powerbi_mirror\export_powerbi_dataset.py"
if errorlevel 1 (
  echo Falha na exportacao.
  pause
  exit /b 1
)
echo OK: powerbi_mirror\output\conciliacao_operacional.csv
pause
