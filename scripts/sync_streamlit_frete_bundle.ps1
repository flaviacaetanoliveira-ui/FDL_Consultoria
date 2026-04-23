# Commita e faz push dos três ficheiros do bundle Frete na mesma revisão (Streamlit Cloud).
# Uso: na raiz do repo, executar:  powershell -File scripts/sync_streamlit_frete_bundle.ps1
$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)

$git = Get-Command git -ErrorAction SilentlyContinue
if (-not $git) {
    Write-Error "Git não está no PATH. Instale o Git for Windows ou abra o Git Bash e corra os comandos manualmente."
}

$files = @(
    "operacional_frete.py",
    "operacional_frete_ui.py",
    "app_operacional.py"
)
foreach ($f in $files) {
    if (-not (Test-Path -LiteralPath $f)) {
        Write-Error "Ficheiro em falta: $f"
    }
}

python scripts/verify_frete_streamlit_deploy.py
if ($LASTEXITCODE -ne 0) {
    Write-Error "Verificação falhou; corrija antes de fazer push."
}

git add -- $files
$status = git status --porcelain -- $files
if (-not $status) {
    Write-Host "Nada a commitar (ficheiros já alinhados com o índice)."
} else {
    git commit -m "sync: bundle frete Streamlit (operacional_frete + UI + app_operacional)"
}

git push
Write-Host ""
Write-Host "Seguinte: Streamlit Cloud > Manage app > Reboot app (ou aguarde o deploy automático)."
