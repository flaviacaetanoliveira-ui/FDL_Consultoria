@echo off
REM Executa export para Power BI sem pausa — use no Agendador de Tarefas do Windows.
REM Opcional: defina FDL_BASE_DIR nas propriedades da tarefa (variáveis de ambiente).
setlocal
cd /d "%~dp0\.."
set "LOG=%~dp0output\ultima_execucao_agendada.log"

echo === %date% %time% Inicio ===>> "%LOG%"
python "powerbi_mirror\export_powerbi_dataset.py" 1>> "%LOG%" 2>>&1
set "RC=%errorlevel%"
echo === %date% %time% Fim codigo %RC% ===>> "%LOG%"
exit /b %RC%
