# PCSL Test Cases SD

日期：2026-03-17  
狀態：部分實作

## 1. 設計目標

本測試設計文件目標為：

1. 建立 `PCSL` 的測試分層
2. 定義 unit / component / integration / regression 的落點
3. 讓未來 persistence v2 與現有碎檔案實作都能重用同一套測試語意

## 2. 測試專案策略

### 2.1 主專案

主測試專案採用：

1. `/workspace/home/work/sharftrade7/Libs/Tests/PersistedConcurrentSortedList.Test`

### 2.2 專案對齊要求

1. 與主專案同 target framework
2. 套件版本不得低於 `PersistedConcurrentSortedList.fsproj` 依賴
3. 使用 xUnit
4. 依測試類別切分模組而非全部塞進單一檔案

## 3. 測試分層

### 3.1 Unit Tests

對象：

1. `CSL2` 的純排序 / query 行為
2. `DefaultHelper`
3. fold / opResult helper
4. comparer / key ordering logic

特性：

1. 不依賴實體磁碟資料
2. 可快速執行

### 3.2 Component Tests

對象：

1. `PCSL2` 與檔案系統的互動
2. initialization / warmup
3. single-process persistence correctness

特性：

1. 使用臨時資料夾
2. 每個測試隔離 basePath

### 3.3 Integration / Regression Tests

對象：

1. initial bulk load + occasional small writes
2. restart / rebuild / reload
3. `PCSL` 與 `PCSL2` 對齊行為

特性：

1. 驗證對外 API
2. 驗證排序與 persistence 不被 regression 破壞

## 4. 建議測試模組

1. `CSL2.OrderingTests.fs`
2. `CSL2.FirstLastNTests.fs`
3. `PCSL2.PersistenceTests.fs`
4. `PCSL2.InitializationTests.fs`
5. `PCSL2.RecoveryTests.fs`
6. `PCSL.APIParityTests.fs`
7. `DefaultHelper.Tests.fs`

## 5. 重要測試案例設計

### 5.1 Initialization

1. empty directory -> initialize -> count = 0
2. seeded directory -> initialize -> index count 與 persisted records 一致
3. initialize 後 `TryGetValue` 可正常 lazy load
4. `WarmupAll` 或等價流程後 `FirstLastN*` 可立即使用

### 5.2 Bulk Load

1. 載入 10K / 100K / 1M 測試資料後：
   - count 正確
   - 排序正確
   - first/last N 正確

### 5.3 Small Writes

1. initial load 後新增 1 筆
2. initial load 後更新 1 筆
3. initial load 後刪除 1 筆
4. 以上操作後：
   - `TryGetValue` 正確
   - `FirstLastN*` 正確
   - persistence status 正確

### 5.4 Restart / Recovery

1. clean shutdown 後重開
2. 重新建構索引後重開
3. 有 tombstone 的狀況下重開
4. metadata 缺失或損壞時應 fail-fast 或走 rebuild fallback

### 5.5 Contract / Helper

1. `DefaultHelper.oFun` 覆蓋 `CDecimal` / `CKVOptList` / `FoldResult`
2. `DefaultHelper.eFun` 覆蓋 `FoldOpR` / `UtilOp` 行為
3. `kvExtract` 對各種輸出型別行為符合預期

## 6. Test Data Strategy

1. 使用 deterministic key/value generator
2. 大資料測試採固定 seed
3. 每組測試有獨立 temp folder
4. 清理測試資料夾避免交叉污染

## 7. 驗證方式

1. 功能 correctness 以 xUnit 斷言為主
2. benchmark 不混入一般 unit test，可單獨標記或使用 trait
3. recovery 類測試需保留中間檔案供除錯

## 8. 本次設計結論

`PCSL` 的測試設計不應只驗 CRUD，而應把 initialization、sorted query、restart/recovery、bulk-load-after-small-write regression 納入一級公民。

## 9. 已實作案例摘要

截至 2026-03-17，已在 `Tests.fs` 實作並驗證下列案例：

1. `PCSL` 基本 add / query / persisted file path
2. `CSL2` 基本排序與 query
3. `DefaultHelper` 對 `CDecimal`、`CKVList`、`FoldResult`、`UtilOp` 的 adapter regression
4. `PCSL2` compare / equals / hash contract
5. `FsPickler` 對 F# record / DU 的 round-trip
6. `ProtoBuf` 對 F# record / DU 的 round-trip
7. `PCSL2` 經 protobuf path 對 F# record / DU / `fCell2<string>` 的 persist + reload
8. `CSL2` 鎖持有期間的 queued write / upsert / remove 語意
9. `PCSL2` 的 non-buffered -> buffered lazy load transition
10. `PCSL2` 的 `IndexInitialize` / `ValueInitialize` 一致性
11. 舊 repo 外 smoke harness 中的 `KeyValuesOp -> foldOpResultValKVList -> kvExtract` 案例

目前仍未完成的主要項目：

1. `FirstLastN` / `FirstLastNValues` 邊界條件
2. bulk load / benchmark 類測試
3. corrupted metadata / repair / rebuild 類 fault-oriented 測試
4. 測試檔依模組拆分
