# SD：physical format、pseudocode與失敗路徑

本契約已接入native development source；實際驗收與發布狀態見WBS。default25,000,000 bytes；小threshold僅scoped test seam，不作正式caller參數。

## Layout與型別

```text
schema/__keys__/<keyHash>.index               原FsPickler key JSON，唯一key visibility
schema/<keyHash>.val                         legacy bytes 或新magic+bounded manifest
schema/__values__/<keyHash>/<generation>/<ordinal>.val  ≤25,000,000 each
schema/__staging__/<keyHash>/<generation>/... 未發布，不能作恢復來源，不進Git
```

manifest magic固定8 bytes `PCSLCH01`，其後UTF8 JSON；fields包括formatVersion=1、keyHash、generation（GUID）、codec=`deflate-pb-v1`或`custom-file-hook-v1`、totalLength、payloadSha256、parts[{ordinal,length,sha256}]。路徑由key/generation/ordinal推導，不存可任意跳目錄的path。manifest本身≤1MB且≤chunkLimit；超過上限明確拒絕，不能配置成無限allocation。空payload有0個parts＋empty hash；defaultPB value不是raw empty，但custom hook需覆蓋。

`ChunkedValue.fs`：immutable `Part`／`Manifest`；`writeCommitted`, `readCommitted`, `deleteCommitted`, `inspectCommitted` effect functions；其餘validate/derive path為pure。只default stream interop實作必要Stream adapter，不建立service/actor/長期GC daemon。

## Write（新增／更新／public hooks）

```fsharp
writeValue key value ignoreQueue =
  with existing queue/write ownership (preserve IgnoreQ contract):
    hash = existing generateKeyHash key
    hadIndex = indexContains key
    oldAnchor = captureCurrentAnchor hash             // no value mutation
    generation = newGuid()
    try
      // ChunkSink只持一個bounded buffer/part；跨part的Write必須while切完。
      use sink = openChunkSink stagingRoot generation limit
      if default codec then
        use deflater = DeflateStream(sink, Compress, leaveOpen=true)
        ModelContainer.serializeF(deflater, value)   // existing stream serializer
        deflater.Dispose()                          // flush trailer BEFORE finish/hash
      else
        customWrite stagingSpoolPath value          // preserve once-per-write hook
        streamCopy stagingSpoolPath sink            // no second huge managed byte[]
      manifest = sink.finishAndFlush()               // close all handles, Flush(true)
      validate all lengths, ordinals, byte count/hash
      atomicRename staging generation -> __values__/hash/generation
      write new anchor to same-volume .tmp; Flush(true)
      atomicReplace anchor                           // commitpoint for update
      if not hadIndex then
        write original key JSON to __keys__/hash.index.tmp; flush
        atomicRename to __keys__/hash.index          // commitpoint for new key
      publish sortedList/index/status changes
      // Return Task completes only after commit, not after enqueue or serializer start.
      retire old generation when no active read leases
    with error ->
      // Before commit: previous anchor/key/status unchanged. Unpublished new key stays absent.
      // After commit: do not throw a retryable 'write failed' just for cleanup failure.
      classify phase; retain safe orphan metadata for bounded next-open cleanup; rethrow only uncommitted failure
```

`getOrNewAndPersistKeyHash` public helper仍可被caller使用；不能默默把它當transaction。storage mutation path改用「計算hash／發布index」分離函式；直接調helper的既有行為需文件與tests保留，另記不是value durability API。

Write callback的file path是storage-private staging path；callback不得依hash filename對外發布side effect。這個既有hook假設變動須先列actual consumers，若有依賴則提供明確compat mode並阻止其冒充25MB保證，不能靜默fallback單大`.val`。

## Read／legacy

```fsharp
readValue key =
  if no visible index then
    if orphan anchor is valid NEW format owned by hash then None  // aborted new key
    elif legacy anchor exists then fail ExistingOrphanGuard
    else None
  else
    with per-key read lease:
      anchor = open snapshot
      if magic absent then existingReadHook anchorPath            // legacy unchanged
      else
        parse bounded manifest; validate version/key/generation/count/ordinals
        open exact parts inside derived owner root (reject reparse)
        validate every length/hash and total hash
        if default codec then decode through verified concatenated stream + DeflateStream
        else assemble bounded-stream spool; customRead spoolPath
        return complete value only after integrity succeeds
```

避免「先decode返回後才驗hash」。可先串流驗所有parts，再在lease下第二遍decode；這會增加large-read I/O，但不必把整份value複製到巨大byte[]。優化single-pass需要能在返回前完成全hash且不暴露partial result，不能放寬規則。任一part缺失/截斷/重排/錯hash → typed InvalidData/IO error，非None或empty。

## Remove／reopen cleanup

```fsharp
remove key =
  with existing key/write ownership:
    atomicUnpublish index (rename to owned tombstone)
    remove in-memory visibility
    wait for owned read leases; delete anchor and all referenced generations
    remove tombstone after durable cleanup
    // delete failures keep tombstone; next open resumes cleanup, never resurrect key

openStore () =
  enumerate only *.index (not .tmp/.tombstone)
  validate indexed anchors and referenced generations
  process owned tombstones before serving writes
  clean unreferenced staging/old generations only when owner quiescent and no leases
  retain unknown/legacy orphan files and report; no broad recursive cleanup
```

不新增全目錄高頻polling；cleanup限定open、成功mutation後的retired generation。完成Delete應回收該key所有chunks；若filesystem拒絕，回明確pending cleanup/failure，不假稱物理清除完成。

## 必須增加的fault seam與tests

`beforePartFlush`、`afterPartFlush`、`beforeAnchorPublish`、`afterAnchorPublishBeforeIndex`、`afterIndexPublish`、`afterIndexUnpublish`是test-only injectable callback或內部filesystem seam，production default no-op。M哥須在相同Expecto suite擴：每點child process abrupt exit、reopen、duplicate/retry、old/new完整值、new-key不存在、Delete不復活、無已ACK value丟失。並驗read/delete、read/update lease交错与cancel。

原始[TEST.fsx](TEST.fsx)對10.1.401產生真RED，涵蓋完整roundtrip/大單值/更新/刪除/legacy/缺失/截斷。當時尚未有seam的crash／precision-threshold cases已在本輪加入同一完整suite；保留歷史結果，不以原13項代替新增gates。

### 2026-10-06 implementation

`ChunkedValue.fs`共用於兩個class；default使用stream codec，custom使用一次hook spool。`withKeyLock`沿原queue ownership保護同key的verify/decode及buffer publish，不新增mailbox；六個commit stages透過scoped `withTestSettings`測試。`WriteReceipt.CleanupPending`區分已發布的cleanup問題與失敗寫入。新key已有legacy orphan時串流複製rollback backup，new index發布前失敗會復原；crash後reopen也恢復backup。rollback本身I/O失敗保留generation與原始/rollback兩個exception供reopen收斂。Tombstone先完成cleanup才允許同key新寫入。

Manifest必需fields、未知/重複fields、size/count、ordinal/owner/codec/total/hash與reparse都fail closed。`inspectCommitted`回完整verified manifest；physical consumer不得用anchor file size代表logical length。原13反例未弱化，同project另有精確threshold、hooks、兩class、六點真子程序abrupt exit/fresh reopen/retry與read/update/delete/cancel案例；具體總數與最新run evidence以WBS為準。無已發布package／Host deployment或斷電驗收衍生。

## Package／consumer rollout

先native完整tests → 新immutable版本與codec docs → IFileSystem exact pin及large-content cold test → PTCS direct size/enum/purge適配＋真registry/ACK/restart → 三Host exact dependency deployment。PTCS root有writer fence仍不能熱copy當consistent Git checkpoint；停writer後才commit完整index/anchor/chunks，staging／locks不提交。

## C25-05 小值效能修正（原候選與本輪實作）

目前`writeCommitted`對512字元小值也執行staging generation、part flush、payload重讀驗證、manifest anchor flush/rename與舊generation cleanup；PTC隔離200次更新相對401有約29–34倍p50延遲及約24倍allocation。候選保留大值chunk路徑，小值改為**相同codec的owned temp＋flush＋atomic anchor replace**；不可退回原地`File.WriteAllBytes(anchor)`。必須保留new-key index-last、existing orphan rollback、舊chunk→小值後cleanup、single-writer與reader lease語意。以下是設計待驗偽碼，尚未實作或接受：

```text
persist(key, value):
  lock key; recover owned pending publication; validate existing index/anchor
  encode once into owned bounded spool; if payload exceeds 25_000_000 then
    return existing chunked writeCommitted path
  assert spool bytes use legacy-compatible codec and fit physical limit
  flush owned temp; fault(beforeAnchorPublish)
  atomicReplace(temp, anchor)              // existing key: old or new whole value
  fault(afterAnchorPublishBeforeIndex)
  if new key then publish index last         // rollback existing orphan on failure
  fault(afterIndexPublish)
  after commit, retire old owned generation only when readers release it
  on precommit failure restore previous visible state; retain repair material if rollback fails
```

定案前須證明小值格式可由既有legacy reader冷讀，涵蓋empty、25MB邊界、custom hook、舊chunk↔小值、missing/truncated、同key讀寫競態與六個真child crash points；不能用縮短flush或跳過已承諾持久化換速度。候選效能至少以401/修後402同一小值、同一GW task/status隔離負載的p50/p95、CPU、allocation、檔案/I/O量比較，之後再作Host真負載。量測與源碼修改由native owner審核；正式 package/Host gate仍未解除。

### 2026-10-11 source implementation

`10.1.402-dev.chunk25.2` 在同key lock下先辨識既有index。預設Deflate codec的既有key把壓縮後最多64 KiB寫入有上限的記憶體stream，再以同磁碟暫存檔、`Flush(true)`、atomic rename替換`.val`；不原位覆寫。超過64 KiB會轉回原streaming chunk sink，新key始終保留generation與index-last發布，故舊orphan的rollback/recovery協定不變。custom file hook仍只呼叫一次，以owned spool取得完整bytes、flush後對既有key直接atomic replace；過25 MB或與manifest magic碰撞則用chunk。`WriteReceipt.Manifest`改為`Manifest option`：直接檔案是`None`，chunk是`Some`；PCSL/PCSL2使用端原本忽略receipt，對外讀寫方法未變。

提交前的fault/cancel保留舊anchor；既有key在anchor原子替換後即算已提交，後續fault回傳`CleanupPending=true`，冷啟動清理舊generation。tombstone先收斂再建立新spool，避免刪到待發布資料。空的owned generation目錄才刪除，unknown/human目錄保留；既有key直接更新會在發布前拒絕linked舊generation owner。reparse檢查改為逐層`GetAttributes`單次查詢；atomic temp對≤4 KiB payload不預留buffer，其餘最多64 KiB，仍執行`Flush(true)`。此路徑只驗process abrupt exit，未驗硬體斷電。

同一份PCSL2 512字元、20 warmup＋200更新、雙順序隔離bench，用**完整94/94測試的同一顆DLL** SHA `45E85B90FBC118AB12644E9FCE26DEED25154E098AC8B227913D5B545695AA33`：修正版p50/p95 2.327/2.742、2.277/2.686 ms，CPU 563/609 ms、allocation 4.221/4.229 MB；正式GW native401 SHA `2C5AA47E20EA18DF371DD3C99624F741027474AC09136B934A5AFCF241D2F44A`為0.638/0.966、0.719/1.050 ms，CPU 141/156 ms、allocation 2.144/2.146 MB。兩者cold read PASS、最後3 files/124 bytes。修正版比原402約17 ms快，但相對401原位覆寫仍有atomic flush＋rename成本。

以Aster固定的postrepair GW task快照另做隔離`GatewayPcslTaskStore`負載：2293筆cold digest驗證，20個既有status暖身後輪流`SaveStatusAsync` 200次，四份相同ZIP快照、雙順序。修正版p50/p95為17.061/27.868與14.072/24.191 ms，吞吐51.3/61.8次每秒，CPU4750/4422ms；401為9.072/22.166與9.919/27.662 ms，吞吐89.8/75.6次每秒，CPU3422/3281ms。修正版process I/O readOps1600/readBytes約8.22 MB、writeOps800/writeBytes約3.18 MB；401 readOps800/readBytes約5.64 MB、writeOps1200/writeBytes約3.24 MB。兩者cold digest均PASS；allocation在兩輪明顯波動（修正版202/901 MB、401 1063/339 MB），不能據此單獨判定。這是使用真實GW DLL的隔離provider負載，仍不是完整Host服務負載；p50、吞吐與CPU退步，使正式package/部署gate維持關閉，待Aster判定下一步。

### 2026-10-11 atomic cost decomposition

在同機C:專屬synthetic fixture，對124-byte anchor逐stage各20暖身＋200次，兩個反向順序；使用上述94/94同顆DLL，`atomic-cost.fsx` SHA `67748CC0D3A9EEBAAAEED732FACB88585ADEFA80E639A59468FB1C87BD1CFAB6`。p50毫秒範圍：不安全的原位`File.WriteAllBytes`且不flush為0.201–0.210（僅作反證）；逐層reparse檢查0.195–0.206；default codec單獨0.009–0.010；temp寫入＋`Flush(true)`為0.607–0.712；已備妥temp的rename為0.253–0.260；不含path check的完整flush＋rename為0.939–1.019；`ChunkedValue.atomicWrite`含path check為1.256–1.326；完整`ChunkedValue.writeDefault`既有key direct為1.929–1.937。這是本機filesystem上的量測下限，不是所有硬體的保證；首輪flush p95也受系統噪音影響。相對401完整PCSL2小值p50約0.64–0.72，必需的flush＋rename單獨已約1ms，再加ownership/PCSL邊界後，原契約下2倍目標不易達成。沒有以移除flush、放回原位覆寫或延後ACK充當效能修正；若要改durability/ACK contract須另行設計與驗收，C25-05 package/Host gate維持關閉。

同日後續用同機C:另一份124-byte synthetic fixture比較替換原語，20暖身＋200次、反向順序、每輪anchor digest PASS。`Flush(true)`後`File.Move(..., overwrite=true)`的p50為0.977/1.036ms；`File.Replace`為2.102/2.042ms；加`FileOptions.WriteThrough`且仍`Flush(true)`後，Move為1.372/1.393ms、Replace為2.642/2.622ms。腳本`C:/Users/Administrator/AppData/Local/Temp/pcsl-c25-atomic-primitive-20261011/atomic-primitives.fsx` SHA `08DBF44208A4CBBE2EDCF49A5FC8C83CEF0269FA8D7FD0E6B26B0EE08F423FB1`。首輪p95有系統噪音，這只是原語成本量測，未建立新的source候選或重跑GW；既有GW同快照ABBA退步仍是正式gate判據。若選擇取消`Flush(true)`，同磁碟rename仍可維持process層完整檔案可見性，但會改變已ACK寫入的斷電持久化保證，需GW owner先定契約與驗收，不能當作無條件安全優化。
