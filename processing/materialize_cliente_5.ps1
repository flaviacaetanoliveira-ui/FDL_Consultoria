# Materializa Cliente 5 (Flávio): Esquilo e Wood.
# Origem no OneDrive (exemplo): ...\Cursor\Flavio\Cliente_4\ com subpastas Esquilo e Wood.
# O parâmetro -Cliente4Root deve apontar para essa pasta Cliente_4 (não para dentro de Esquilo/Wood).
# Nota: a pasta de origem chama-se Cliente_4; no repositório e no Streamlit o slug do cliente é cliente_5
# (Flávio = 5.º cliente) — data_products/cliente_5/{esquilo|wood}/...
# Saída: data_products/cliente_5/{esquilo|wood}/{repasse|frete}/current/
#
# Uso:
#   .\processing\materialize_cliente_5.ps1 -Cliente4Root "C:\Users\...\Flavio\Cliente_4"
#   .\processing\materialize_cliente_5.ps1 -Cliente4Root "C:\Users\...\Flavio\Cliente_4" -PreflightOnly

param(
    [Parameter(Mandatory = $true)]
    [string] $Cliente4Root,
    [string] $RepoRoot = "",
    [switch] $PreflightOnly
)

$ErrorActionPreference = "Stop"
if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
Set-Location $RepoRoot

$esquiloBase = Join-Path $Cliente4Root "Esquilo"
$woodBase = Join-Path $Cliente4Root "Wood"

function Invoke-Materialize {
    param(
        [string] $BaseDir,
        [string] $EmpresaSeg,
        [string] $OrgId,
        [string] $DatasetEmpresa,
        [ValidateSet("repasse", "frete", "all")]
        [string] $Modulo = "all"
    )
    $pyArgs = @(
        "processing/materialize_financeiro.py",
        "--base-dir", $BaseDir,
        "--root", "data_products",
        "--cliente", "cliente_5",
        "--empresa", $EmpresaSeg,
        "--org-id", $OrgId,
        "--dataset-empresa", $DatasetEmpresa,
        "--modulo", $Modulo
    )
    & python @pyArgs
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

function Invoke-MaterializeRepasseThenFrete {
    param(
        [string] $BaseDir,
        [string] $EmpresaSeg,
        [string] $OrgId,
        [string] $DatasetEmpresa
    )
    Invoke-Materialize -BaseDir $BaseDir -EmpresaSeg $EmpresaSeg -OrgId $OrgId -DatasetEmpresa $DatasetEmpresa -Modulo "repasse"
    & python @(
        "processing/materialize_financeiro.py",
        "--base-dir", $BaseDir,
        "--root", "data_products",
        "--cliente", "cliente_5",
        "--empresa", $EmpresaSeg,
        "--org-id", $OrgId,
        "--dataset-empresa", $DatasetEmpresa,
        "--modulo", "frete"
    )
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Frete nao gerado para $EmpresaSeg (exit $LASTEXITCODE). Use export ML Detalhe de envios; relatorio Pedidos resumido nao basta. Repasse OK."
    }
}

if (-not (Test-Path -LiteralPath $esquiloBase)) {
    Write-Error "Pasta não encontrada: $esquiloBase"
}
if (-not (Test-Path -LiteralPath $woodBase)) {
    Write-Error "Pasta não encontrada: $woodBase"
}

Write-Host "=== Preflight Esquilo ===" -ForegroundColor Cyan
& python "processing/materialize_financeiro.py" --base-dir $esquiloBase --preflight
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

Write-Host "=== Preflight Wood ===" -ForegroundColor Cyan
& python "processing/materialize_financeiro.py" --base-dir $woodBase --preflight
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

if ($PreflightOnly) {
    Write-Host 'Preflight concluido (-PreflightOnly). Nada foi materializado.' -ForegroundColor Green
    exit 0
}

Write-Host "=== Materializar Esquilo -> data_products/cliente_5/esquilo/ ===" -ForegroundColor Cyan
Invoke-MaterializeRepasseThenFrete -BaseDir $esquiloBase -EmpresaSeg "esquilo" -OrgId "esquilo" -DatasetEmpresa "Esquilo"

Write-Host "=== Materializar Wood -> data_products/cliente_5/wood/ ===" -ForegroundColor Cyan
Invoke-MaterializeRepasseThenFrete -BaseDir $woodBase -EmpresaSeg "wood" -OrgId "wood" -DatasetEmpresa "Wood"

Write-Host "Concluido." -ForegroundColor Green
exit 0
