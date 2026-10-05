# Native25MB chunk development

需求：[RFC](RFC-PCSL-0025.25MBChunk.md)，code impact：[SA](SA.md)，詳細pseudocode：[SD](SD.md)，進度：[WBS](WBS.md)。Owner：Aster已交契約與反例，M哥負責後續實作/原queue回歸/consumer closure。

Native development source `10.1.402-dev.chunk25.1`已實作auto-chunk；最新完整驗收狀態見[WBS](WBS.md)。原10.1.401 baseline13：7PASS、6sizeFAIL、0ignored/error，largest58,245,341 bytes；歷史RED保留，原payload entropy與25MB assertion未弱化。

RFC測試本體仍是[TEST.fsx](TEST.fsx)，由既有native test project直接Compile同一檔案，合併queue10及storage/recovery/safety tests。Fixtures預設在system TEMP的專屬GUID目錄，也允許明示repo temp；失敗保留。測試不用mock產品，不碰服務、SQL或行情登入。[TEST_Feedback](TEST_Feedback.md)記錄入口合併、disk-full資源修正及原失敗。

## Automation（本native repo root執行）

完整canonical入口：

```powershell
./misc/verify.nativeQueue.ps1          # PLAN
./misc/verify.nativeQueue.ps1 -Execute # Release build + exact SHA + inventory + ALL tests
```

每個build使用新的TEMP output/obj；compiled inventory包含原queue10/RFC13及全部新增cases，總執行/通過數必須等於inventory，ignored/fail/error必須0。新key/更新/刪除六個fault點會真子程序abrupt exit，再用fresh child reopen與retry驗收；30秒child／120秒phase watchdog保留。以下為最初TDD baseline的historical standalone指令，請明示`PcslAssemblyPath`才改測新candidate：

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

Standalone13項不代表完整chunk gate，正式驗收使用上方合併入口。Release Pack會執行本repo PostBuildEvent；本輪只Build，不Pack/推NuGet。IFileSystem exact pin、PTCS直接physical enum/size/purge及三Host部署仍是C25-05，source/unit完成不能替代該部署驗收。
