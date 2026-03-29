# Gera dataset_frete_app.csv a partir da base em cliente_1 (ou FDL_BASE_DIR).
# Uso: na raiz do repositório V2, PowerShell:
#   .\processing\run_materialize_frete.ps1

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root

$base = if ($env:FDL_BASE_DIR) { $env:FDL_BASE_DIR } else { Join-Path $root "cliente_1" }
$vendas = Join-Path $base "Vendas - Mercado Livre"

if (-not (Test-Path $vendas)) {
    Write-Host "ERRO: Pasta nao existe: $vendas" -ForegroundColor Red
    exit 1
}

$hasData = (Get-ChildItem -Path $vendas -File -Include *.xlsx,*.xls,*.csv -Recurse -ErrorAction SilentlyContinue | Measure-Object).Count -gt 0
if (-not $hasData) {
    Write-Host "ERRO: Coloque pelo menos um ficheiro .xlsx ou .csv de vendas ML em:" -ForegroundColor Yellow
    Write-Host "  $vendas" -ForegroundColor Yellow
    Write-Host "Veja cliente_1\Vendas - Mercado Livre\LEIA-ME.txt" -ForegroundColor Gray
    exit 1
}

$args = @(
    "processing/materialize_financeiro.py",
    "--base-dir", $base,
    "--cliente", "default",
    "--empresa", "antomoveis",
    "--modulo", "frete"
)

Write-Host "A materializar frete com base em: $base" -ForegroundColor Cyan
python @args
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

$out = Join-Path $root "data_products\default\antomoveis\frete\current\dataset_frete_app.csv"
if (Test-Path $out) {
    Write-Host "OK: $out" -ForegroundColor Green
}
