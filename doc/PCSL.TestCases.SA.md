# PCSL Test Cases SA

日期：2026-03-17  
範圍：`Libs5/KServer/fstring` 與 `Libs/Tests/PersistedConcurrentSortedList.Test`

## 1. 目標

本文件定義 `PCSL` 的測試案例分析範圍，基於下列產品型態的常見驗證面向：

1. ordered in-memory index
2. persisted key-value / embedded storage
3. initialization / warmup / replay / checkpoint
4. range-like query / top-bottom N query

本輪測試設計以 `PCSL` 目前重新定義的 workload 為前提：

1. 首次大規模載入
2. 後續以讀取為主
3. 偶爾小量更新

## 2. 主測試專案判斷

經比較兩個候選測試專案：

1. `/workspace/home/work/sharftrade7/Libs/Tests/PersistedConcurrentSortedList.Test`
2. `/workspace/home/work/PersistedConcurrentSortedList.Test`

結論：

1. 應以 repo 內的 `Libs/Tests/PersistedConcurrentSortedList.Test` 作為主測試專案。

理由：

1. 其檔案內容已升到 xUnit，且有 `net9.0;net10.0` 的新式測試專案結構。
2. 它在主 repo 中有 2026-01 的提交紀錄。
3. repo 外那份為舊的 console-style 測試草稿，且 `ProjectReference` 路徑已失效。

目前 blocker：

1. repo 內測試專案目前 restore/build 失敗，原因是：
   - `PersistedConcurrentSortedList` 已只支援 `net10.0`
   - 測試專案仍 target `net9.0;net10.0`
   - 套件版本落後主專案，出現 downgrade

## 3. 測試需求來源整理

### 3.1 對 `PCSL` 本身的需求

1. 基本 CRUD 正確性
2. 排序一致性
3. `FirstLastN*` 查詢正確性
4. 初始化與 warmup 行為
5. persistence 與 recovery
6. API 一致性與 historical regression

### 3.2 參考業界常見產品的測試面向

根據官方文件，嵌入式/有序儲存產品通常會明確驗證：

1. ordered index / ordered query
2. transaction or WAL/checkpoint
3. snapshot / checkpoint
4. recovery after crash
5. index-assisted query vs full scan

對應參考：

1. SQLite WAL 與 checkpoint：
   - https://sqlite.org/wal.html
   - https://www.sqlite.org/c3ref/wal_autocheckpoint.html
2. LiteDB 單檔、WAL、index-only / ordered query：
   - https://www.litedb.org/
   - https://www.litedb.org/docs/indexes/
3. RocksDB checkpoint / snapshot：
   - https://rocksdb.org/blog/2015/11/10/use-checkpoints-for-efficient-snapshots.html

## 4. 測試案例大類

### 4.1 Initialization / Bootstrap

1. 空資料目錄初始化
2. 既有資料目錄初始化
3. index rebuild
4. warmup / full buffer
5. cold start / warm start

### 4.2 Basic Storage Semantics

1. Add
2. Update
3. Upsert
4. Remove
5. ContainsKey
6. TryGetValue / TryGetValues

### 4.3 Ordered Query Semantics

1. key ordering
2. `FirstLastN`
3. `FirstLastNKeys`
4. `FirstLastNValues`
5. boundary cases

### 4.4 Persistence / Recovery

1. restart after clean shutdown
2. replay / rebuild
3. tombstone correctness
4. checkpoint restore
5. corrupted metadata fail-fast

### 4.5 Small Writes After Initial Bulk Load

1. bulk insert / load 後少量 upsert
2. bulk insert / load 後少量 remove
3. small writes 是否破壞排序與 query correctness

### 4.6 Regression / Contract

1. `DefaultHelper` 全分支覆蓋
2. `PCSL` vs `PCSL2` API 行為對齊
3. `NOT YET IMPLEMENTED` 的對外 API 風險

## 5. 本輪測試分析結論

本輪測試不應只寫「happy path CRUD」，而應以 `PCSL` 真正的產品價值來分層：

1. ordered query correctness
2. initialization / warmup correctness
3. persistence / restart correctness
4. initial bulk load + occasional small writes 的 regression

換句話說，`PCSL` 的測試主軸應更接近「輕量 ordered persisted store」而不只是 collection unit test。
