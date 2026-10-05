# Native25MB TDD交接

需求：[RFC](RFC-PCSL-0025.25MBChunk.md)，code impact：[SA](SA.md)，詳細pseudocode：[SD](SD.md)，進度：[WBS](WBS.md)。Owner：Aster已交契約與反例，M哥負責後續實作/原queue回歸/consumer closure。

目前native未支援auto-chunk，20261006測試13個實際執行：7PASS、6FAIL、0ignored/error。六個FAIL是>25MB物理檔案未切分；例子28,118,569／58,245,341 bytes。不要降低entropy或修改size assertion換綠燈。

測試本體是[TEST.fsx](TEST.fsx)，[TEST.fsproj](TEST.fsproj)只是以指定candidate DLL及相同FSharp.Core編譯它，避免FSI預載舊Core引起無關MissingMethod。沒有copy產品實作或mock PCSL。所有fixtures在本repo ignored `temp/`，不碰服務、SQL或行情登入。

## Automation（本native repo root執行）

```powershell
dotnet build ./PersistedConcurrentSortedList.fsproj -c Release `
  -p:OutputPath=bin/net10.0/agent.aster/chunk-rfc/ `
  -p:AppendTargetFrameworkToOutputPath=false `
  -p:BaseIntermediateOutputPath=obj/agent.aster/chunk-rfc/ `
  -p:MSBuildProjectExtensionsPath=obj/agent.aster/chunk-rfc/ `
  -p:GeneratePackageOnBuild=false -p:PublishNuGetAfterPack=false

dotnet build ./doc/RFC/20261006.25MB.Chunk/TEST.fsproj -c Release `
  -p:OutputPath=bin/net10.0/agent.aster/ `
  -p:AppendTargetFrameworkToOutputPath=false `
  -p:BaseIntermediateOutputPath=obj/agent.aster/ `
  -p:GeneratePackageOnBuild=false

dotnet ./doc/RFC/20261006.25MB.Chunk/bin/net10.0/agent.aster/TEST.dll
```

另一owner改participant output並以`-p:PcslAssemblyPath=<absolute candidate DLL>`傳candidate；各project保持自己的bin/obj，不共享global絕對output。VS FSI可依檔頭調整candidate `#I`、`defaultArgumentsText`後執行，須使用與candidate相容的FSharp.Core；本次已驗的是compiled entry，不宣稱所有FSI版本已驗。

M哥先讓六個physical size反例轉綠，再補SD列出的精確threshold、custom hooks、舊class、跨process abrupt-exit及read/delete race。當前13項不含這些未開始gates，不能用全綠13項就發布。Release Pack會執行本repo PostBuildEvent；本輪只Build，不Pack/推NuGet。
