# PCSL WBS

日期：2026-03-17  
狀態：依新 workload 重排

## 1. 目標

本輪 WBS 對準新的主線：

1. 保留 `PCSL` 的 ordered persisted collection 定位
2. 優先完成 persistence v2、recovery 與工程補齊
3. 不在第一波替換 `SortedList`

## 2. 新功能

### 2.1 Persistence v2

1. 設計 `manifest + segmented data log + snapshot` 規格
2. 實作 append-only `AppendUpsert`
3. 實作 tombstone `AppendDelete`
4. 實作 `Checkpoint`
5. 實作 `Compact`
6. 實作 `Repair`

交付物：

1. persistence spec 文件
2. log writer / reader
3. checkpoint manager
4. repair tool 或 repair API

### 2.2 Recovery

1. 定義 source of truth
2. 實作 `LoadSnapshot`
3. 實作 `ReplayTail`
4. 實作 full rebuild fallback

交付物：

1. recovery coordinator
2. 啟動流程整合
3. recovery test cases

## 3. API

### 3.1 新增 API

1. `Initialize(mode)`
2. `WarmupAll(?maxDoP)`
3. `Checkpoint()`
4. `Compact()`
5. `Repair()`
6. `InfoDetailed`

### 3.2 API 對齊

1. 定義 `PCSL` 與 `PCSL2` 哪個是主實作
2. 對齊 `TryGetValue` / `TryGetValues` / `FirstLastN*` 行為
3. 對齊同步與 async 包裝策略

完成條件：

1. 對外 API 語意一致
2. 管理型 API 能支撐 checkpoint / compact / repair

## 4. 修正

### 4.1 Contract / Helper

1. 修正 `DefaultHelper.oFun`
2. 修正 `DefaultHelper.eFun`
3. 修正 `DefaultHelper.kvExtract`
4. 補齊 `OpResult` / `PCSLTaskTyp` 全分支覆蓋

### 4.2 Build / Compile

1. 修正 `ASYNC` compile constant 與 `net10.0` 漂移
2. 修正 `QuickStart.fsx` 的 `net9.0` sample drift
3. 清理或記錄 `FS0343` equality/hash contract 問題

### 4.3 行為一致性

1. 釐清 `PCSL.fsx` 與 `PCSL2.fsx` 的保留策略
2. 避免同名 API 在不同實作下行為不一致

## 5. 補齊 NOT YET IMPLEMENTED

1. 補齊 `PCSL.fsx` 的 `Item.set`
2. 盤點所有 `failwith "not yet implemented"`、註解掉的替代邏輯、半完成路徑
3. 為每一個未完成點決定：
   - 立即實作
   - 標示 obsolete
   - 移除或封存

完成條件：

1. 對外公開 API 不再留有明顯 runtime landmine

## 6. 效能調校

### 6.1 初載

1. 測量 initial bulk load latency
2. 測量 snapshot restore 與 full rebuild 差異
3. 重新評估 `autoCacheChange` 在 bulk load 中的開銷

### 6.2 讀取

1. 測量 `TryGetValue`
2. 測量 `TryGetValues`
3. 測量 `FirstLastN*`
4. 測量 warmup 前後差異

### 6.3 小量更新

1. 測量偶爾 `Upsert` / `Remove` 對延遲的影響
2. 測量 append-only persistence 對小改成本的改善

## 7. 測試與驗收

1. CRUD correctness
2. 排序語意與 `FirstLastN*`
3. restart / recovery / replay
4. checkpoint / compact / repair
5. initial bulk load benchmark
6. occasional small writes benchmark

完成條件：

1. 新 persistence 相比碎檔案模型，file count 明顯下降
2. 啟動與恢復有可重跑證據
3. `FirstLastN*` 不退化
4. 小量更新不引入新的 runtime inconsistency

## 8. 執行順序

1. 文件與決策歷程定稿
2. persistence spec 定稿
3. API/contract 修正
4. recovery / checkpoint / compact
5. NOT YET IMPLEMENTED 清理
6. benchmark 與效能調校
