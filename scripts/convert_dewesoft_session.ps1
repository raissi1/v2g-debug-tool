param(
    [Parameter(Mandatory = $true)]
    [string]$SessionRoot,

    [string]$DewesoftExe = "C:\Program Files\DewesoftX\Bin64\DEWEsoft.exe",

    [switch]$OpenMissingInDewesoft
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-NormalizedStem {
    param([string]$Name)
    return ($Name.ToLower() -replace "[\s\-_]", "")
}

function Find-SidecarCsv {
    param([System.IO.FileInfo]$RawFile)

    $csvs = Get-ChildItem -LiteralPath $RawFile.DirectoryName -File -Filter *.csv | Sort-Object Name
    if (-not $csvs) {
        return $null
    }

    $sameStem = @($csvs | Where-Object { $_.BaseName -eq $RawFile.BaseName })
    if ($sameStem.Count -ge 1) {
        return $sameStem[0]
    }

    if (@($csvs).Count -eq 1) {
        return $csvs[0]
    }

    $rawStem = Resolve-NormalizedStem -Name $RawFile.BaseName
    $fuzzy = @($csvs | Where-Object {
        $csvStem = Resolve-NormalizedStem -Name $_.BaseName
        $rawStem.Length -gt 0 -and ($csvStem.Contains($rawStem) -or $rawStem.Contains($csvStem))
    })
    if ($fuzzy.Count -eq 1) {
        return $fuzzy[0]
    }

    return $null
}

$root = Resolve-Path -LiteralPath $SessionRoot
$manifestPath = Join-Path $root "dewesoft_conversion_manifest.json"
$rawFiles = Get-ChildItem -LiteralPath $root -Recurse -File | Where-Object { $_.Extension.ToLower() -in @(".d7d", ".dxd", ".dmd") } | Sort-Object FullName
$imageFiles = Get-ChildItem -LiteralPath $root -Recurse -File | Where-Object { $_.Extension.ToLower() -in @(".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp") } | Sort-Object FullName

$results = @()
foreach ($rawFile in $rawFiles) {
    $sidecar = Find-SidecarCsv -RawFile $rawFile
    $status = if ($null -ne $sidecar) { "ready_with_csv" } else { "missing_csv_export" }

    $expectedCsvPath = if ($null -ne $sidecar) {
        $sidecar.FullName
    } else {
        Join-Path $rawFile.DirectoryName ($rawFile.BaseName + ".csv")
    }

    $screenshots = @($imageFiles | Where-Object { $_.DirectoryName -eq $rawFile.DirectoryName })

    $results += [pscustomobject]@{
        raw_file = $rawFile.FullName
        raw_extension = $rawFile.Extension.ToLower()
        status = $status
        resolved_csv = if ($null -ne $sidecar) { $sidecar.FullName } else { $null }
        expected_csv = $expectedCsvPath
        screenshots = @($screenshots | ForEach-Object { $_.FullName })
    }

    if ($OpenMissingInDewesoft -and $status -eq "missing_csv_export" -and (Test-Path -LiteralPath $DewesoftExe)) {
        Start-Process -FilePath $DewesoftExe -ArgumentList @($rawFile.FullName) -WindowStyle Hidden
    }
}

$summary = [pscustomobject]@{
    session_root = $root.Path
    dewesoft_exe = $DewesoftExe
    total_raw_files = $rawFiles.Count
    raw_with_csv = @($results | Where-Object { $_.status -eq "ready_with_csv" }).Count
    raw_missing_csv = @($results | Where-Object { $_.status -eq "missing_csv_export" }).Count
    generated_at = (Get-Date).ToString("o")
    files = $results
}

$summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $manifestPath -Encoding UTF8

Write-Host ""
Write-Host "Dewesoft conversion summary"
Write-Host "Session root       : $($root.Path)"
Write-Host "Raw files detected : $($summary.total_raw_files)"
Write-Host "With sidecar CSV   : $($summary.raw_with_csv)"
Write-Host "Missing CSV export : $($summary.raw_missing_csv)"
Write-Host "Manifest           : $manifestPath"
Write-Host ""

foreach ($item in $results) {
    if ($item.status -eq "ready_with_csv") {
        Write-Host "[READY]  $($item.raw_file)" -ForegroundColor Green
        Write-Host "         -> $($item.resolved_csv)"
    } else {
        Write-Host "[MISSING] $($item.raw_file)" -ForegroundColor Yellow
        Write-Host "          Expected CSV: $($item.expected_csv)"
    }
}

Write-Host ""
Write-Host "Usage tips:"
Write-Host "1. Run without -OpenMissingInDewesoft to generate the manifest."
Write-Host "2. Run with -OpenMissingInDewesoft to open missing raw files in DewesoftX."
Write-Host "3. Export CSV next to each raw file using the same base name when possible."
