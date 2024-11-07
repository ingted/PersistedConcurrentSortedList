param(
    $assembly
)


function Get-VersionFromFileName {
    param (
        [string]$fileName
    )

    # 提取類似於 "1.0"、"10.2.3"、"3.4.5.6" 等的版本號
    if ($fileName -match '\d+(\.\d+)+') {
        return [version]$matches[0] # 轉換為 [version] 類型進行數值比較
    } else {
        return [version]'0.0' # 如果找不到版本號，則返回最低版本
    }
}

write-host ($assembly + ': Current path: ' + (pwd).path)
cd ./bin
try {
    $pkg = (dir "$($assembly)*.nupkg" | Sort-Object -Property { Get-VersionFromFileName $_.Name } -Descending)[0].Name
    invoke-expression "dotnet nuget push $pkg --api-key $(gc G:\Nuget\apikey.txt) --source https://api.nuget.org/v3/index.json  --skip-duplicate"
}
catch {
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
    write-host $_
    write-host "=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+=+="
}
