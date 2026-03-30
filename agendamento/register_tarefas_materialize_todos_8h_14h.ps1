#Requires -Version 5.1
[CmdletBinding()]
param(
    [string] $TaskName = "Materialize Financeiro - Todos Clientes",
    [string] $LegacyTaskName = "Materialize Financeiro"
)

$ErrorActionPreference = "Stop"
$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$batPath  = Join-Path $PSScriptRoot "run_materialize_todos_clientes.bat"
if (-not (Test-Path -LiteralPath $batPath)) {
    throw "Nao encontrado: $batPath"
}

$arg = '/c "' + $batPath + '"'
$action = New-ScheduledTaskAction -Execute "cmd.exe" -Argument $arg -WorkingDirectory $repoRoot
$tManha = New-ScheduledTaskTrigger -Daily -At "8:00AM"
$tTarde = New-ScheduledTaskTrigger -Daily -At "2:00PM"
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -AllowStartIfOnBatteries `
    -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew

$description = @"
Materializa todos os clientes/empresas mapeados em data_products/*/* às 08:00 e 14:00.
Roda agendamento\run_materialize_todos_clientes.bat (sem alterar código do pipeline).
"@

Register-ScheduledTask `
    -TaskName $TaskName `
    -Action $action `
    -Trigger @($tManha, $tTarde) `
    -Settings $settings `
    -Description $description `
    -Force | Out-Null

$legacy = Get-ScheduledTask -TaskName $LegacyTaskName -ErrorAction SilentlyContinue
if ($legacy) {
    Disable-ScheduledTask -TaskName $LegacyTaskName | Out-Null
    Write-Host "Tarefa legada desabilitada: $LegacyTaskName"
}

Write-Host "Tarefa registada: $TaskName"
Write-Host "Horarios: 08:00 e 14:00 (todos os dias)"
Write-Host "Script: $batPath"
