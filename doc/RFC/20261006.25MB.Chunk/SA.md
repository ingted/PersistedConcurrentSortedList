# SA：native與consumer程式變更分析

基線：canonical repo `G:/coldfar_py/sharftrade9/Libs5/KServer/fstring`，branch `chore/fstring-repo-cleanup-and-packaging`，commit `ae7bf4b7d1fe4fb9cd4c1bb45be6f22d110b1c79`，package10.1.401。20261006 baseline Release build成功，99個既有FS0025/FS0064警告、0 error；未Pack/publish。

| 檔案／seam | 現況、因果 | 必要改動與不變量 |
|---|---|---|
| `PB.fsx:604–654` ModelContainer | serializeFBase已接受Stream；serializeF2BArr先MemoryStream/ToArray再Deflate；write2File整包File.WriteAllBytes、read全檔decompress | 保留原codec bytes與public helper；提供stream serialize/decode route。新layout由native physical value module處理，不把PTCS契約塞入PB。 |
| `Compression.fsx` | byte-array compress/decompress會複製整包 | 新增stream seam，用相同Deflate algorithm、leaveOpen／finish順序明確；小值legacy decode保持。 |
| `PCSL2.fsx:281–350` | getOrNewAndPersistKeyHash先更新memory/index，再寫value；Remove同時刪index/value | 分離compute hash與publish index；新value durable後發布index。更新既有value時anchor atomic replace。Remove按visibility→cleanup順序，不能並行到reader看見半個value。 |
| `PCSL2.fsx:367–398,465–488` | ReadFromFile hook直讀hash.val；無index但val存在會throw；hooks可覆寫 | 讀新版anchor與legacy；新格式unpublished orphan可辨識並不對外可見，legacy orphan仍保留corruption guard。hook adapter不更動callback型別。 |
| `PCSL.fsx:416–595` | 舊class也走同serializer／單value刪除，產品仍compile此檔 | 共用同一physical module，不複製兩套chunk格式；保留舊class constructor/API。至少跨class冷讀與mutations相容驗證。 |
| `PersistedConcurrentSortedList.fsproj` | compile order為Compression→PB→CSL→PCSL/PCSL2；Release Pack含publish hook | 新`ChunkedValue.fs`置於PB後、PCSL前；baseline/build明確GeneratePackageOnBuild=false。版本由M哥在完整TDD完成後決定。 |
| `G:/PulseTrade.fs/Libs/PersistedConcurrentSortedList.IFileSystem/PcslStorage.fs` | Put/Delete/TryGet使用PCSL2<string,fCell2<string>>，Commit no-op；不直接拼`.val` | 預期不改API，只升exact package並驗大string、restart、delete。Commit no-op不是flush或Git checkpoint，文件不可宣稱提供持久化交易。 |
| `G:/PulseTrade2.fs/Libs/PulseTrade.Comm.Spa/Stream.fs` | native hooks和直接`hash.val`位置用于read、size guard、purge；不是純opaque consumer | 必須盤點其physical enumeration/delete/size assumptions；anchor小不等於logical payload小。需native inspect/delete seam或讓consumer透過既有store API，禁止只刪anchor遺留chunks。 |

## 主要風險與gate

1. atomic anchor與index是兩個檔案，不能宣稱跨檔transaction。new-key index最後發布；crash前孤兒無可見key。update index已存在，只換anchor，reader得到全舊或全新。Remove index先unpublish，再刪資料。
2. 同process多reader不能讀到剛被GC的generation；per-key read lease跨越manifest驗證、chunk開啟與decode。舊generation在無reader後才回收。不同process仍沿原single-writer規範。
3. defaultIgnoreQ=false queue與caller override=true必須保留；不要為chunk新增另一個mailbox而重新引入Native queue死鎖。
4. 新格式標記／part路徑是磁碟輸入：不得接受absolute／`..`／reparse、重複ordinal或錯key/generation；manifest大小/count上限要在allocation前檢查。
5. public file hooks可寫任意格式，須靠adapter封裝輸出，不能假設其bytes是PB。temporary spool不進Git；只有已committed `.val`/index/manifest/chunks屬restore集合。

## 最小真使用例

IFileSystem.Put("session/transcript", highEntropyText) → process關閉 →新process.TryGet同key →內容hash相同且每個committed `.val`≤25,000,000；更新小值後不留無用舊chunks；Delete後cold lookup不存在。使用者不管理part names、generation或manifest。
