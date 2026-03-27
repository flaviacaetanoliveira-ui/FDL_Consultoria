#Requires -Version 5.1
<#
  Regista no Agendador de Tarefas do Windows DUAS execucoes diarias:
    - 08:00
    - 14:30
  Ambas correm agendamento\run_rotina_dados.bat (sync + export).

  Executar no PowerShell (utilizador atual, uma vez):
    cd "C:\caminho\para\V2\agendamento"
    Set-ExecutionPolicy -Scope CurrentUser RemoteSigned   # se necessario
    .\register_tarefas_8h_14h30.ps1

  Remover tarefas:
    Unregister-ScheduledTask -TaskName "FDL V2 rotina dados" -Confirm:$false
#>
[CmdletBinding()]
param(
    [string] $TaskName = "FDL V2 rotina dados"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batPath  = Join-Path $PSScriptRoot "run_rotina_dados.bat"
if (-not (Test-Path -LiteralPath $batPath)) {
    throw "Nao encontrado: $batPath"
}

$arg = '/c "' + $batPath + '"'
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg -WorkingDirectory $repoRoot

$tManha = New-ScheduledTaskTrigger -Daily -At "8:00AM"
$tTarde = New-ScheduledTaskTrigger -Daily -At "2:30PM"

$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew

$description = @"
Sincroniza Vendas - Mercado Livre e frete por anuncio para data_cliente; executa powerbi_mirror/export (se nao SKIP).
Configurar caminhos em agendamento\config_local.bat (copiar de config_local.bat.example).
"@

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($tManha, $tTarde) `
    -Settings $settings `
    -Description $description `
    -Force | Out-Null

Write-Host "Tarefa registada: $TaskName"
Write-Host "Horarios: 08:00 e 14:30 (todos os dias)"
Write-Host "Script: $batPath"
Write-Host "Crie agendamento\config_local.bat a partir do .example com FDL_SYNC_VENDAS_SRC e FDL_SYNC_FRETE_SRC."
