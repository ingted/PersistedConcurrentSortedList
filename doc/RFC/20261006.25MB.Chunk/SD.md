# SD：physical format、pseudocode與失敗路徑

這是待實作設計，不是existing API。default25,000,000 bytes；小threshold僅internal test seam。型別名字可由M哥依現有style微調，磁碟與可觀察契約須保持。

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

目前[TEST.fsx](TEST.fsx)以現有public API可編譯並產生真RED，涵蓋完整roundtrip/大單值/更新/刪除/legacy/缺失/截斷；上述尚無seam的crash／precision-threshold cases是**未實作未執行**，不可用skip冒充完成。

## Package／consumer rollout

先native完整tests → 新immutable版本與codec docs → IFileSystem exact pin及large-content cold test → PTCS direct size/enum/purge適配＋真registry/ACK/restart → 三Host exact dependency deployment。PTCS root有writer fence仍不能熱copy當consistent Git checkpoint；停writer後才commit完整index/anchor/chunks，staging／locks不提交。
