# PCSL Test Cases WBS

日期：2026-03-17  
狀態：部分完成

## 1. Phase 0: Baseline 修復

1. 選定主測試專案：
   - `Libs/Tests/PersistedConcurrentSortedList.Test`
2. 對齊 target framework 到 `net10.0`
3. 對齊套件版本到主專案相依
4. 讓測試專案可 restore / build / run

完成條件：

1. `dotnet build` 成功
2. 測試專案不再有 net9 incompatibility 與 downgrade blocker

目前結果：

1. 已完成

## 2. Phase 1: 測試骨架

1. 建立測試檔切分
2. 建立共用 temp directory helper
3. 建立 deterministic seed data helper
4. 建立 common assertion helper

完成條件：

1. 測試專案具備可擴充骨架

目前結果：

1. 部分完成
2. 已建立 temp directory / wait helper 與共用建構 helper
3. 測試仍集中在 `Tests.fs`，尚未依模組拆檔

## 3. Phase 2: Ordering / Query Tests

1. `CSL2` 排序正確性
2. `FirstLastN`
3. `FirstLastNKeys`
4. `FirstLastNValues`
5. boundary cases

完成條件：

1. 排序與 top/bottom N 的核心語意可由測試保護

目前結果：

1. 部分完成
2. 已覆蓋 `CSL2` 基本排序與 `FirstLastNKeys`
3. 尚未補 `FirstLastN` / `FirstLastNValues` 與 boundary cases

## 4. Phase 3: Persistence / Initialization Tests

1. empty init
2. persisted data init
3. lazy load
4. warmup
5. restart after persisted writes

完成條件：

1. 初始化與持久化基本正確性有測試覆蓋

目前結果：

1. 部分完成
2. 已覆蓋 persisted data init、lazy load、value warmup、restart after persisted writes
3. 尚未覆蓋空資料夾初始化與 metadata 異常

## 5. Phase 4: Bulk Load + Occasional Small Writes

1. initial bulk load
2. bulk load 後少量 add
3. bulk load 後少量 update/upsert
4. bulk load 後少量 remove
5. 驗證 query 與 persistence 不退化

完成條件：

1. 與新 workload 對齊的 regression 測試存在

目前結果：

1. 部分完成
2. 已覆蓋 initial persisted state 後的小量 upsert / remove / lazy read
3. 尚未補大批量 seed data regression

## 6. Phase 5: Contract / Helper Regression

1. `DefaultHelper`
2. `PCSL` / `PCSL2` parity
3. `NOT YET IMPLEMENTED` 路徑保護

完成條件：

1. 已知 code review finding 有對應測試

目前結果：

1. 已完成 `DefaultHelper` regression、`PCSL` indexer setter、`PCSL2` compare/equals/hash
2. 尚未補更多 helper 分支與 API parity matrix

## 7. Phase 6: Recovery / Fault-oriented Tests

1. rebuild index
2. restart after delete/tombstone
3. corrupted metadata handling
4. repair / rebuild fallback

完成條件：

1. recovery path 被測試保護

目前結果：

1. 尚未完成
2. 目前只覆蓋 index rebuild / value initialize happy path，尚未進入 corruption / repair

## 8. Phase 7: Benchmark / 調校

1. initial bulk load timing
2. warm start timing
3. `FirstLastN*` timing
4. occasional small writes timing

完成條件：

1. 有 baseline 數據支撐後續效能調校

目前結果：

1. 尚未開始
