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

function Test-RepasseReady {
    param([string] $BaseDir)
    $dirs = @()
    try {
        $dirs = @(Get-ChildItem -LiteralPath $BaseDir -Directory -ErrorAction Stop)
    }
    catch {
        return $false
    }
    $hasVendas = @($dirs | Where-Object {
            $_.Name -eq "Vendas_ML" -or $_.Name -eq "Vendas - Mercado Livre"
        }).Count -gt 0
    $hasLiberacoes = @($dirs | Where-Object {
            $_.Name -like "Libera*ML*"
        }).Count -gt 0
    return ($hasVendas -and $hasLiberacoes)
}

function Test-FreteReady {
    param([string] $BaseDir)
    if (-not (Test-RepasseReady -BaseDir $BaseDir)) { return $false }
    $xlsx = Get-ChildItem -Path $BaseDir -Filter "*.xlsx" -File -ErrorAction SilentlyContinue | Where-Object {
        ($_.Name -match "(?i)frete")
    }
    return @($xlsx).Count -gt 0
}

foreach ($c in $companies) {
    $base = Join-Path $Cliente3Root $c.Folder
    if (-not (Test-Path -LiteralPath $base)) {
        Write-Error "Pasta não encontrada: $base"
    }
}

$oldRepasseSemBling = $env:FDL_REPASSE_SEM_BLING
$oldRepasseOnly = $env:FDL_REPASSE_VENDAS_LIBERACOES_ONLY
$env:FDL_REPASSE_SEM_BLING = "1"
$env:FDL_REPASSE_VENDAS_LIBERACOES_ONLY = "1"
try {
    foreach ($c in $companies) {
        $base = Join-Path $Cliente3Root $c.Folder
        Write-Host "=== Preflight $($c.Folder) ===" -ForegroundColor Cyan
        $repOk = Test-RepasseReady -BaseDir $base
        $frOk = Test-FreteReady -BaseDir $base
        Write-Host ("[preflight-thiago] repasse (vendas+liberações): " + ($(if ($repOk) { "OK" } else { "FALTA" })))
        Write-Host ("[preflight-thiago] frete (vendas+planilha): " + ($(if ($frOk) { "OK" } else { "FALTA" })))
    }

    if ($PreflightOnly) {
        Write-Host "Preflight concluido (-PreflightOnly). Nada foi materializado." -ForegroundColor Green
        exit 0
    }

    foreach ($c in $companies) {
        $base = Join-Path $Cliente3Root $c.Folder
        $repOk = Test-RepasseReady -BaseDir $base
        $frOk = Test-FreteReady -BaseDir $base
        Write-Host "=== Materializar $($c.Folder) -> data_products/cliente_thiago/$($c.Segment)/ ===" -ForegroundColor Cyan
        if ($repOk) {
            & python @(
                "processing/materialize_financeiro.py",
                "--base-dir", $base,
                "--root", "data_products",
                "--cliente", "cliente_thiago",
                "--empresa", $c.Segment,
                "--org-id", $c.OrgId,
                "--dataset-empresa", $c.DatasetEmpresa,
                "--modulo", "repasse"
            )
            if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
        }
        else {
            Write-Warning "Repasse não gerado para $($c.Folder): faltam Vendas_ML (ou Vendas - Mercado Livre) e/ou Liberações_ML."
        }
        if ($frOk) {
            & python @(
                "processing/materialize_financeiro.py",
                "--base-dir", $base,
                "--root", "data_products",
                "--cliente", "cliente_thiago",
                "--empresa", $c.Segment,
                "--org-id", $c.OrgId,
                "--dataset-empresa", $c.DatasetEmpresa,
                "--modulo", "frete"
            )
            if ($LASTEXITCODE -ne 0) {
                Write-Warning "Frete não gerado para $($c.Folder) (exit $LASTEXITCODE)."
            }
        }
        else {
            Write-Warning "Frete não gerado para $($c.Folder): falta Vendas_ML/Vendas - Mercado Livre e/ou planilha Frete por Anúncio."
        }
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
    if ($null -eq $oldRepasseOnly) {
        Remove-Item Env:FDL_REPASSE_VENDAS_LIBERACOES_ONLY -ErrorAction SilentlyContinue
    }
    else {
        $env:FDL_REPASSE_VENDAS_LIBERACOES_ONLY = $oldRepasseOnly
    }
}
