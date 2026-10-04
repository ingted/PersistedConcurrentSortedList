# PTC Host native queue 異動與驗證

工項 H3-03／H3-04；native 開始202610042013。詳細 RFC／SA／SD／估時檢討維持 `G:/PulseTrade.fs/doc/PTC HOST 優化/` 與 `AgentPlanMemo/memo.20261004201350-001.md`。

## SA／SD 異動

PTCS backend已有同 process/root序列化，明示 `UpsertAsync(...,ifIgnoreQ=true)`，但 PCSL 的 status.Upsert及nonbuffer.Remove未收到 flag，仍經 queue。同機制存在於 RemoveAsync的base／status／forward／reverse index。修正只傳遞flag，保留 defaultfalse、durable completion及原file／index格式，不重寫queue、不調全域ThreadPool。

Pseudo：`CSL.Remove(k,flag)` → flag為true時 `LockableOp(IgnoreQ(CRemove k))`，否則原`CRemove k`；原單參數Remove → `Remove(k,false)`。`persistKeyValueBase`的兩個status.Upsert與buffer.Remove傳入flag；`removePersistedKeyValue`四個Remove同樣傳入。Core caller仍等待兩個native handles，任一寫入失敗不接受。

六處既有private modifier依適用F#policy移除；ToPb／OfPb反射搜尋同步改為Public。這是可見性調整，序列化格式不改；冷啟／duplicate與真實I/O測試覆蓋Serialize／Deserialize。原baseline99個FS0025／FS0064 warnings保留，未以suppress隱藏。

## WBS／Test

| Slice | Progress／status | Evidence |
|---|---|---|
| Native flag propagation | 100% SourceUnitComplete | 可控RED10＝6P4F0E；修正後10P0F0E。第一green因public反射尚未同步產生9E，已修正；原raw保留。native candidate SHA4273002F6A6FF3A2B71C91405EF344315602F80862D78F8697B562932C9EC816 |
| Host source runtime | 100% IsolatedSourceRuntimeComplete | actual native path/hash一致；同fullWSCore0AE916…、2007sets；MCP2×27/browser7/cache11PASS；loadp99 98.252ms／UI377.266ms；6ownedPID／3ports清理，兩Host graceful0 |
| 新 native full consumers | 100% SourceFullTestComplete | 新native4273002F候選：Core discovered/executed/passed1310，GW646，0ignored/failed/errored；原runner，GW保持Main cwd與完整ownednative fixture。全ownedexternalchildren清理 |
| package／部署／prod | 0% NotStarted | 新immutable identity與Core／IFileSystem／FSISupervisor active refs留REL工項；不覆寫已公開400、不以source candidate當package或deployment |

外部 evidence base：`C:/Users/Administrator/AppData/Local/Temp/aster-host-h3-02349fa677e543a5907b42e7ed0b884a/native-pcsl/`；Host root：`C:/Users/Administrator/AppData/Local/Temp/ptcs-performance-verification/run-7ab59cf501a5412383b7bf7a9352db42`。實際10case unit entry見[Verification](Verification.md)。

## DevLog

- 202610042025：canonical source branch `chore/fstring-repo-cleanup-and-packaging`／baselineHEAD `1f4fc51cc472f6c772894fdf7c45d9da5f858342`。既有fsproj從302更新到400的dependency WIP與NuGet400 published nuspec一致，保留；本次沒有另外發布或重寫400。
- 202610042031：sameHub真實source experiment通過，無profiler load p99從1524.26→98.252ms，UI1535.73→377.266ms；是單次固定fixture證據，非全域peak／prod保證。

- 2026-10-04T21:04:52.8235910+08:00：canonical PCSL-VFY-NATIVE-QUEUE-r1 PLAN與Execute驗證完成：fresh build／actual10P0F0E／sourceStable=true；raw根 C:/Users/Administrator/AppData/Local/Temp/pcsl-native-queue-verification-70bf14709a264e27b2baa6e776706398，該fresh輸出SHA4EA363BF…。Host/Core/GW full使用前一份4273002F…source candidate，生成於不同output/intermediate；未以不同artifact hash宣稱byte-identical package。Native產品碼沒有再改。新版native full roots為native-pcsl/full-consumers-r2（Core）與-r3（GW）；首次GW627P8F11E是wrapper cwd／externalHost fixture缺漏，原失敗保留。source/readback與package/public/deployed分列。

- Version baseline reconciliation：package `PersistedConcurrentSortedList`（fsproj的PackageId宣告為字面 `$(AssemblyName)`，MSBuild resolved名稱同project）：舊source版本 `10.1.302` -> 既有WIP／已公開baseline `10.1.400`。原因是同步已公開400的net10與FCell2／ProtoBuf／FsPickler／ParallelSeq400、FSharp.Core400、protobuf-net.Core3.4.21、ConfigurationManager10.0.11依賴；與本輪native行為patch分列。NuGet push：本輪未執行／public400未覆寫；official400已存在的網頁依賴欄確認。下游：Core、Main IFileSystem、FSISupervisor active project仍引用400；新immutable版本與exact consumers同步排在Host REL-01，尚未宣稱部署。