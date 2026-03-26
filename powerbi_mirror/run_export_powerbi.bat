@echo off
setlocal
cd /d "%~dp0\.."

echo ==========================================
echo Exportacao para Power BI (mirror)
echo ==========================================
echo.
echo Opcional: configure FDL_BASE_DIR antes de rodar.
echo Exemplo:
echo   set FDL_BASE_DIR=C:\dados\cliente_1
echo.

python "powerbi_mirror\export_powerbi_dataset.py"
if errorlevel 1 (
  echo.
  echo Falha na exportacao. Veja mensagens acima.
  pause
  exit /b 1
)

echo.
echo Concluido. Abra no Power BI:
echo   powerbi_mirror\output\conciliacao_operacional.csv
echo.
pause
