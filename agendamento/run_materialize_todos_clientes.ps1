param(
    [string] $RepoRoot = "",
    [string[]] $ExcludeCliente = @()
)

$ErrorActionPreference = "Stop"
if (-not $RepoRoot) {
    $RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
}
Set-Location $RepoRoot

$dataProducts = Join-Path $RepoRoot "data_products"
if (-not (Test-Path -LiteralPath $dataProducts)) {
    Write-Error "Pasta não encontrada: $dataProducts"
}

$targets = New-Object System.Collections.Generic.List[object]

Get-ChildItem -Path $dataProducts -Directory | ForEach-Object {
    $cliente = $_.Name
    Get-ChildItem -Path $_.FullName -Directory | ForEach-Object {
        $empresa = $_.Name
        $repMeta = Join-Path $_.FullName "repasse\current\metadata.json"
        $freMeta = Join-Path $_.FullName "frete\current\metadata.json"

        $baseDir = ""
        $orgId = ""

        if (Test-Path -LiteralPath $repMeta) {
            try {
                $j = Get-Content -LiteralPath $repMeta -Raw | ConvertFrom-Json
                if ($j.base_dir) { $baseDir = [string]$j.base_dir }
                if ($j.org_id) { $orgId = [string]$j.org_id }
            } catch {
            }
        }
        if ((-not $baseDir) -and (Test-Path -LiteralPath $freMeta)) {
            try {
                $j = Get-Content -LiteralPath $freMeta -Raw | ConvertFrom-Json
                if ($j.base_dir) { $baseDir = [string]$j.base_dir }
                if ($j.org_id) { $orgId = [string]$j.org_id }
            } catch {
            }
        }
        if (-not $orgId) { $orgId = $empresa }

        if ($baseDir) {
            $targets.Add([PSCustomObject]@{
                cliente = $cliente
                empresa = $empresa
                base_dir = $baseDir
                org_id = $orgId
            })
        }
    }
}

$targets = $targets | Sort-Object cliente, empresa -Unique
if (-not $targets -or $targets.Count -eq 0) {
    Write-Error "Nenhum alvo encontrado em data_products/*/* com metadata base_dir."
}

$ok = 0
$fail = 0
$skip = 0
foreach ($t in $targets) {
    $tag = "$($t.cliente)/$($t.empresa)"
    if ($ExcludeCliente -contains $t.cliente) {
        Write-Host "[SKIP] $tag (excluído)" -ForegroundColor DarkGray
        $skip++
        continue
    }
    if (-not (Test-Path -LiteralPath $t.base_dir)) {
        Write-Host "[FAIL] $tag base_dir inexistente: $($t.base_dir)" -ForegroundColor Red
        $fail++
        continue
    }

    Write-Host "[RUN ] $tag --base-dir `"$($t.base_dir)`"" -ForegroundColor Cyan
    & python "processing/materialize_financeiro.py" `
        --base-dir $t.base_dir `
        --cliente $t.cliente `
        --empresa $t.empresa `
        --org-id $t.org_id `
        --modulo all
    if ($LASTEXITCODE -eq 0) {
        Write-Host "[ OK ] $tag" -ForegroundColor Green
        $ok++
    } else {
        Write-Host "[FAIL] $tag exit=$LASTEXITCODE" -ForegroundColor Red
        $fail++
    }
}

Write-Host ("Resumo: OK={0} FAIL={1} SKIP={2} TOTAL={3}" -f $ok, $fail, $skip, $targets.Count)
if ($fail -gt 0) { exit 1 }
exit 0
