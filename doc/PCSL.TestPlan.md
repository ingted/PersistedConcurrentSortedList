# PCSL Test Plan

日期：2026-03-17  
狀態：先定義，待實作後執行

## 1. 測試目標

這份測試計畫不是為目前碎檔案版本背書，而是為「初載大、後續小改、保留排序與 `FirstLastN*`」的整改目標預先定義驗證標準。  
任何 persistence v2 / recovery / API 補齊方案如果過不了這份計畫，就不應進入主線。

## 2. 功能正確性

### 2.1 基本 CRUD

1. `Add` 新 key 成功
2. `Add` 重複 key 失敗或符合預期
3. `Update` 僅更新已存在 key
4. `Upsert` 對存在/不存在 key 都符合預期
5. `Remove` 後不可再讀到舊值
6. `TryGetValue` / `TryGetValues` 對 miss/hit 行為一致

### 2.2 排序語意

1. key 順序必須與 comparer 一致
2. `FirstLastN` 對正/負 N 行為要固定
3. `FirstLastNKeys` / `FirstLastNValues` / `FirstLastN` 結果要彼此對齊
4. 邊界條件：
   - 空集合
   - N = 0
   - N > Count
   - 重複更新後順序不變

### 2.3 持久化與恢復

1. 重啟後資料仍可讀
2. index rebuild 後資料數量一致
3. partial warmup / lazy read 行為正確
4. compact / vacuum 後資料一致
5. 刪除後不會 resurrect 舊值

## 3. 非功能測試

### 3.1 效能指標

至少量測：

1. cold start latency
2. warm start latency
3. single read p50 / p95 / p99
4. batch read throughput
5. occasional add / upsert / remove throughput
6. `FirstLastN` latency
7. memory footprint
8. file count
9. total on-disk size

### 3.2 workload profile

每個候選實作至少要跑：

1. Initial bulk load
2. Read-heavy
3. Occasional small writes
4. Mixed read/write
5. Small values
6. Large values
7. Sequential keys
8. Random keys

## 4. 失效與恢復測試

### 4.1 Crash consistency

1. `Upsert` 進行中強制終止
2. `Remove` 進行中強制終止
3. compaction 中強制終止
4. rebuild index 中強制終止

### 4.2 Corruption handling

1. data file segment 缺塊
2. index/meta 損壞
3. checksum mismatch
4. orphaned entry / stale record

預期要求：

1. 至少要能 fail fast
2. 需要可診斷的錯誤訊息
3. 需要明確 repair / rebuild 路徑

## 5. 本輪主線驗證判準

### 5.1 Persistence v2

必測：

1. segment append 是否能取代一筆一檔
2. checkpoint / snapshot restore 成本
3. replay tail 的正確性
4. 相較現況是否明確降低 file count

### 5.2 API / Contract 補齊

必測：

1. `DefaultHelper` 全分支覆蓋
2. `PCSL` 與 `PCSL2` 的對齊程度
3. `NOT YET IMPLEMENTED` 路徑是否清除

### 5.3 可選延伸驗證

1. `FASTER bridge` 若進行 POC，需驗證 source of truth 與 recovery 複雜度
2. 若未來評估替換排序主體，再另行建立 `SortedDictionary` / paged sorted structure 專屬 benchmark

## 6. 驗收門檻

任何候選 backend 至少要滿足：

1. 功能面不退化 `FirstLastN*` 與 point lookup
2. file count 明顯優於現況碎檔案模型
3. cold start 與大規模資料下的恢復時間可接受
4. 寫入吞吐不低於現況，或能以明確理由接受 trade-off
5. recovery 行為比現況更可驗證、更可觀測

## 7. 本次執行範圍

本次沒有實作 backend，也沒有執行上述 benchmark / fault injection。  
本文件用途是把之後的「test 要測什麼」先固定，避免設計決策先行、驗證標準卻事後補。
