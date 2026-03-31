# Materializa Cliente Thiago (Cliente_3): BP Ramiro, FMG, Let Decor, TB Paio.
# Saída: data_products/cliente_thiago/{bp_ramiro|fmg|let_decor|tb_paio}/{repasse|frete}/current/
#
# Uso:
#   .\processing\materialize_cliente_thiago.ps1 -Cliente3Root "C:\Users\...\Cursor\Thiago\Cliente_3"
#   .\processing\materialize_cliente_thiago.ps1 -Cliente3Root "C:\Users\...\Cursor\Thiago\Cliente_3" -PreflightOnly

param(
    [Parameter(Mandatory = $true)]
    [string] $Cliente3Root,
    [string] $RepoRoot = "",
    [switch] $PreflightOnly
)

$ErrorActionPreference = "Stop"
if (-not $RepoRoot) {
    $RepoRoot = Split-Path -Parent $PSScriptRoot
}
Set-Location $RepoRoot

$companies = @(
    @{ Folder = "BP Ramiro"; Segment = "bp_ramiro"; OrgId = "bp_ramiro"; DatasetEmpresa = "BP Ramiro" },
    @{ Folder = "FMG"; Segment = "fmg"; OrgId = "fmg"; DatasetEmpresa = "FMG" },
    @{ Folder = "Let Decor"; Segment = "let_decor"; OrgId = "let_decor"; DatasetEmpresa = "Let Decor" },
    @{ Folder = "TB Paio"; Segment = "tb_paio"; OrgId = "tb_paio"; DatasetEmpresa = "TB Paio" }
)

function Invoke-Materialize {
    param(
        [string] $BaseDir,
        [string] $EmpresaSeg,
        [string] $OrgId,
        [string] $DatasetEmpresa
    )
    $args = @(
        "processing/materialize_financeiro.py",
        "--base-dir", $BaseDir,
        "--root", "data_products",
        "--cliente", "cliente_thiago",
        "--empresa", $EmpresaSeg,
        "--org-id", $OrgId,
        "--dataset-empresa", $DatasetEmpresa,
        "--modulo", "all"
    )
    & python @args
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}

foreach ($c in $companies) {
    $base = Join-Path $Cliente3Root $c.Folder
    if (-not (Test-Path -LiteralPath $base)) {
        Write-Error "Pasta não encontrada: $base"
    }
}

$oldRepasseSemBling = $env:FDL_REPASSE_SEM_BLING
$env:FDL_REPASSE_SEM_BLING = "1"
try {
    foreach ($c in $companies) {
        $base = Join-Path $Cliente3Root $c.Folder
        Write-Host "=== Preflight $($c.Folder) ===" -ForegroundColor Cyan
        & python "processing/materialize_financeiro.py" --base-dir $base --preflight
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }

    if ($PreflightOnly) {
        Write-Host "Preflight concluido (-PreflightOnly). Nada foi materializado." -ForegroundColor Green
        exit 0
    }

    foreach ($c in $companies) {
        $base = Join-Path $Cliente3Root $c.Folder
        Write-Host "=== Materializar $($c.Folder) -> data_products/cliente_thiago/$($c.Segment)/ ===" -ForegroundColor Cyan
        Invoke-Materialize -BaseDir $base -EmpresaSeg $c.Segment -OrgId $c.OrgId -DatasetEmpresa $c.DatasetEmpresa
    }

    Write-Host "Concluido." -ForegroundColor Green
    exit 0
}
finally {
    if ($null -eq $oldRepasseSemBling) {
        Remove-Item Env:FDL_REPASSE_SEM_BLING -ErrorAction SilentlyContinue
    }
    else {
        $env:FDL_REPASSE_SEM_BLING = $oldRepasseSemBling
    }
}
