# PCSL SD

日期：2026-03-17  
狀態：依新 workload 重定義，待實作

## 1. 設計目標

本設計以以下前提成立：

1. 資料首次大量載入
2. 載入後讀取為主
3. 更新、刪除、upsert 只是偶爾小改
4. 仍需保留排序語意與 `FirstLastN*`

因此本輪設計目標不是替換 `SortedList`，而是：

1. 保留目前 ordered in-memory read path
2. 重做 persistence spec
3. 明確定義 recovery flow
4. 補齊 API / helper / contract 未完成處

## 2. 目標架構

### 2.1 保留的部分

1. `CSL2` 繼續擔任 ordered in-memory state
2. `PCSL2` 繼續擔任對外 facade
3. `FirstLastN*` 仍建立在已排序且可切片的 in-memory state 上

### 2.2 重構的部分

1. 將檔案存取從 `PCSL2` 拆出為獨立 persistence engine
2. 用新 spec 取代 `.index/.val` 一筆一檔模式
3. 把 recovery、checkpoint、compaction 明文化

## 3. 建議模組拆分

### 3.1 `OrderedState`

職責：

1. 管理 `sortedList`
2. 提供 `FirstLastN*`
3. 管理 `sortedListIndex` / `sortedListIndexReversed`
4. 管理 `sortedListPersistenceStatus`

本輪不替換資料結構，只整理責任邊界。

### 3.2 `PersistenceEngine`

建議新增抽象：

1. `Init`
2. `LoadSnapshot`
3. `ReplayTail`
4. `ReadValue`
5. `AppendUpsert`
6. `AppendDelete`
7. `Checkpoint`
8. `Compact`
9. `Repair`

### 3.3 `SerializerCodec`

建議明確拆出：

1. key serializer
2. value serializer
3. hash / key identity
4. format version
5. checksum

### 3.4 `RecoveryCoordinator`

建議獨立出 recovery policy，不要藏在啟動流程中。

職責：

1. 定義 source of truth
2. 載入 snapshot
3. replay append log
4. 發現不一致時修復索引或標記異常

## 4. 建議 persistence spec v2

### 4.1 目標

1. 全 .NET
2. 不依賴一筆一檔
3. 可 checkpoint
4. 可 replay
5. 可 rebuild

### 4.2 建議檔案布局

1. `manifest.json`
   - format version
   - active segment
   - latest checkpoint sequence
   - checksum / metadata
2. `data-000001.log`, `data-000002.log`, ...
   - append-only data segments
   - 每筆包含 `sequence/keyHash/key/valueBytes/flags/checksum`
3. `index-snapshot.bin`
   - 目前 ordered index 與 keyHash 映射的快照
4. `pstatus-snapshot.bin`
   - persistence status 快照

### 4.3 寫入流程

對 `Upsert(key, value)`：

1. 序列化 value
2. append 到 active data log，取得 `sequence`
3. 更新 in-memory ordered state
4. 將 key status 標記為 dirty / buffered
5. 定期或明確呼叫 `Checkpoint` 時刷新 snapshot

對 `Remove(key)`：

1. append tombstone record
2. 更新 in-memory ordered state
3. 在 checkpoint/compaction 時清理無效版本

### 4.4 讀取流程

對 `TryGetValue(key)`：

1. 先查 in-memory ordered/index state
2. 若 value 已在 buffer，直接返回
3. 若僅有 key metadata，依 offset/segment 讀取 log record 並反序列化
4. 視 policy 決定是否回填 buffer

對 `FirstLastN*`：

1. 直接從 in-memory ordered state 取得 keys / values
2. 若 value 尚未 buffer，可批次回讀並填充

## 5. Recovery Flow 定義

### 5.1 Source of Truth

1. persisted log + latest valid checkpoint 為真實來源
2. in-memory ordered state 視為可重建 cache，不是最終真相

### 5.2 啟動流程

1. 讀取 `manifest`
2. 載入最新 `index-snapshot.bin` 與 `pstatus-snapshot.bin`
3. 從 `latest checkpoint sequence` 之後開始 replay data segments
4. 將 replay 結果套回 ordered state
5. 若 snapshot 不存在或損壞，退化為 full rebuild

### 5.3 修復規則

1. 若 log 有、index 無：以 log 補 index
2. 若 index 有、log 無：以 checkpoint 與 log 驗證後移除失效 index
3. 若 checksum 不符：標記 segment 損壞並停止啟動，交由 repair 工具處理

## 6. API 調整方向

### 6.1 新增管理 API

1. `Initialize(mode)`
2. `WarmupAll(?maxDoP)`
3. `Checkpoint()`
4. `Compact()`
5. `Repair()`
6. `InfoDetailed`

### 6.2 補齊既有 API

1. 補齊 `PCSL.fsx` 的 `Item.set`
2. 補齊 `DefaultHelper` 對 `OpResult` / `PCSLTaskTyp` 的覆蓋
3. 對齊 `PCSL` 與 `PCSL2` 行為

### 6.3 不做的事

1. 不在本輪把 `PCSL` 改造成 DB facade
2. 不在本輪引入 `DBreeze` / `LiteDB` 作為主後端
3. 不在本輪替換 `SortedList`

## 7. 效能調校方向

1. 初載流程：
   - 降低 index rebuild 的目錄列舉成本
   - 減少不必要的 value warmup
2. 寫入流程：
   - 讓小改只 append log，不再建立新檔
3. 讀取流程：
   - 讓 `FirstLastN*` 繼續利用 ordered in-memory state
4. 快取：
   - 重新評估 `autoCacheChange` 與 initial bulk load 的互動

## 8. 本次設計結論

本輪設計不是「換掉 `SortedList`」，而是：

1. 保留 `SortedList` 在目前 workload 下的讀取優勢
2. 把 persistence 升級成有 spec、有 checkpoint、有 recovery 的完整層
3. 補齊既有未完成 API 與 contract 問題

這樣才能在不破壞 `PCSL` 既有 identity 的前提下，先把系統做穩。
