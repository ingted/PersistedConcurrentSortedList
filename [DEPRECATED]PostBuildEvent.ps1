param(
    $assembly
)
function Get-VersionFromFileName {
    param ([string]$fileName)
    if ($fileName -match '\d+(\.\d+)+(-\w+(\.\d+)?)?') {
        return [version]($matches[0].Split('-')[0])
    } else {
        return [version]'0.0'
    }
}
function Get-NuGetApiKey {
    $paths = @("/Nuget/apikey.txt", "G:\Nuget\apikey.txt", "C:\Nuget\apikey.txt")
    foreach ($p in $paths) {
        if (Test-Path $p) {
            return (Get-Content -Path $p -Raw).Trim()
        }
    }
    return $null
}
function Get-LibPacksContent {
    $currentDir = Get-Location

    while ($currentDir -ne [System.IO.Path]::GetPathRoot($currentDir)) {
        $libPacksPath = Join-Path -Path $currentDir -ChildPath 'lib-packs.txt'
        if (Test-Path $libPacksPath) {
            return Get-Content -Path $libPacksPath -Raw
        }
        $currentDir = (Get-Item $currentDir).Parent.FullName
    }

    Write-Host "lib-packs.txt not found in any parent directory." -ForegroundColor Red
    return $null
}
Write-Host ("[PostBuild] " + $assembly + ": Running in " + $PSVersionTable.OS)
$binPath = Join-Path (Get-Location).Path "bin/Release"
if (-not (Test-Path $binPath)) { $binPath = Join-Path (Get-Location).Path "bin" }
if (Test-Path $binPath) {
    Set-Location $binPath
    $packages = Get-ChildItem "$($assembly)*.nupkg" | Sort-Object -Property { Get-VersionFromFileName $_.Name.SubString($assembly.Length) } -Descending
    if ($packages.Count -eq 0) {
        Write-Host "No .nupkg found for $assembly" -ForegroundColor Yellow
        return
    }
    $pkg = $packages[0]
    copy   $pkg.FullName $(Get-LibPacksContent) -force
    $apiKey = Get-NuGetApiKey
    if ($null -eq $apiKey) {
        Write-Host "CRITICAL: NuGet API Key file NOT found!" -ForegroundColor Red
        return
    }
    Write-Host "Pushing: $($pkg.Name) to nuget.org..."
    $pushCmd = "dotnet nuget push `"$($pkg.Name)`" --api-key $apiKey --source https://api.nuget.org/v3/index.json --skip-duplicate"
    Invoke-Expression $pushCmd
    Set-Location ..
} else {
    Write-Host "Bin directory not found. Skipping push." -ForegroundColor Gray
}