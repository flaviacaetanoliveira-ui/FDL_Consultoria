#Requires -Version 5.1
<#
  Cria ou atualiza a tarefa agendada "Materialize Financeiro" (08:00 e 14:00 diários).
  Não exige PowerShell como Administrador: corre no contexto do utilizador atual (adequado a OneDrive local).

  Comando:
    cd "C:\caminho\para\V2"
    powershell -ExecutionPolicy Bypass -File ".\create_task_materialize.ps1"

  Nota: a tarefa só corre quando o utilizador tiver sessão iniciada (sem password guardada).

  Validar: Win + R -> taskschd.msc -> procurar "Materialize Financeiro"
  Testar: botão direito na tarefa -> Executar.
#>

[CmdletBinding()]
param()

# =============================================================================
# PARAMETRIZAÇÃO — edite aqui
# =============================================================================
$ProjectRoot       = "C:\Users\diieg\OneDrive - FDL Consultoria\V2"
$PythonExe         = ""   # vazio = resolve "python" no PATH; ou caminho absoluto para python.exe
$BaseDirCliente    = "C:\Users\diieg\OneDrive - FDL Consultoria\Cursor\Anto Moveis\cliente_1"
$ClienteSegment    = "default"
$EmpresaSegment    = "antomoveis"
$Modulo            = "all"   # repasse | frete | all

$TaskName          = "Materialize Financeiro"
# Utilizador atual (OneDrive e perfil local). Tarefa: apenas com sessão iniciada, sem senha guardada.
$TaskRunAsUser     = "$env:USERDOMAIN\$env:USERNAME"

# =============================================================================
$ErrorActionPreference = "Stop"
$script:ExitCode = 0

function Resolve-PythonExe {
    param([string] $Configured)
    if ($Configured -and (Test-Path -LiteralPath $Configured)) {
        return (Resolve-Path -LiteralPath $Configured).Path
    }
    $cmd = Get-Command python.exe -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
    throw "Não foi encontrado python.exe. Defina `$PythonExe no script com o caminho completo."
}

try {
    $ProjectRoot = (Resolve-Path -LiteralPath $ProjectRoot).Path
    $RunnerPy = Join-Path $ProjectRoot "run_materialize_financeiro.py"
    if (-not (Test-Path -LiteralPath $RunnerPy)) {
        throw "Ficheiro não encontrado: $RunnerPy"
    }

    $py = Resolve-PythonExe -Configured $PythonExe
    $baseQuoted = $BaseDirCliente.Trim()

    $argList = @(
        "`"$RunnerPy`""
        "--base-dir"
        "`"$baseQuoted`""
        "--cliente"
        $ClienteSegment
        "--empresa"
        $EmpresaSegment
        "--modulo"
        $Modulo
    )
    $argumentString = $argList -join " "

    $action = New-ScheduledTaskAction -Execute $py -Argument $argumentString -WorkingDirectory $ProjectRoot

    $trigger8 = New-ScheduledTaskTrigger -Daily -At "08:00"
    $trigger14 = New-ScheduledTaskTrigger -Daily -At "14:00"

    $settings = New-ScheduledTaskSettingsSet `
        -AllowStartIfOnBatteries `
        -DontStopIfGoingOnBatteries `
        -StartWhenAvailable `
        -MultipleInstances IgnoreNew `
        -ExecutionTimeLimit (New-TimeSpan -Hours 4)

    # Contexto do utilizador atual: sessão interativa, sem privilégios elevados na tarefa.
    $principal = New-ScheduledTaskPrincipal -UserId $TaskRunAsUser -LogonType Interactive -RunLevel Limited

    Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue | Unregister-ScheduledTask -Confirm:$false

    Register-ScheduledTask `
        -TaskName $TaskName `
        -Action $action `
        -Trigger @($trigger8, $trigger14) `
        -Principal $principal `
        -Settings $settings `
        -Description "Materializa repasse/frete (run_materialize_financeiro) para data_products/current. Só corre com utilizador com sessão." | Out-Null

    Write-Host ""
    Write-Host "OK: tarefa '$TaskName' criada ou atualizada (utilizador: $TaskRunAsUser, apenas com sessão iniciada)." -ForegroundColor Green
    Write-Host "  Python: $py"
    Write-Host "  WorkingDirectory: $ProjectRoot"
    Write-Host "  Horários: diariamente 08:00 e 14:00"
    Write-Host ""
}
catch {
    Write-Host ""
    Write-Host "ERRO: $($_.Exception.Message)" -ForegroundColor Red
    if ($_.ScriptStackTrace) { Write-Host $_.ScriptStackTrace }
    $script:ExitCode = 1
}
exit $script:ExitCode
