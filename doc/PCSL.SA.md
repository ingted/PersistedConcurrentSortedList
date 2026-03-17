# PCSL SA

日期：2026-03-17  
範圍：`Libs5/KServer/fstring`

## 1. 重新定義後的目標

本次重新定義 `PersistedConcurrentSortedList` 的目標 workload 為：

1. 首次大規模載入與反序列化
2. 載入後以讀取為主
3. 僅偶爾小量新增 / 更新 / 刪除
4. 必須保留排序語意與 `FirstLastN*`

在這個前提下，`PCSL` 的整改目標不再是「全面換掉排序主體」，而是：

1. 保留目前以 ordered in-memory state 支撐排序查詢的核心價值
2. 優先解決 persistence 的碎檔案模型、recovery、啟動重建成本
3. 補齊既有 API / helper / contract 的未完成與不一致

## 2. 現況是否符合這個 workload

### 2.1 符合的部分

1. `CSL2` 使用 `SortedList<'Key,'Value>`，在讀多寫少場景下能保留穩定的排序與低成本的順序切片。
2. `CSL2.FirstLastN / FirstLastNKeys / FirstLastNValues` 能直接利用有序資料完成 top/bottom N。
3. `getKeys/getValues` 與 `createMemoryFromArr` 已將 `SortedList` 的連續記憶體特性轉成切片優勢。
4. `PCSL2` 的 `valueInitialize` / `TryGetValueNoThreadLock` 已經呈現「先 warm up、後高效率讀」的模式。

結論：

1. 對於「初載大、後續小改」這個 workload，`SortedList` 並不是第一優先要移除的元件。

### 2.2 不符合的部分

1. persistence 仍是「一筆 key 一個 `.index`、一筆 value 一個 `.val`」。
2. 啟動時 `indexInitializeBase` 需要列舉目錄並逐一重建索引。
3. 現況缺少明確的 checkpoint / snapshot / recovery flow。
4. 既有 API 邊界仍有 `NOT YET IMPLEMENTED` 與 helper contract 漏洞。

結論：

1. 目前 `PCSL` 對這個 workload 的主要問題在 persistence 與工程一致性，不在排序能力本身。

## 3. 技術答辯整理後的決策結論

### 3.1 `SortedList` 的判斷

1. `SortedList` 的硬傷是中間插入/刪除為 `O(N)`，在持續大量寫入時會明顯放大。
2. 但在本次重新定義的 workload 下，寫入不是主要熱路徑。
3. 因此 `SortedList` 仍可接受，現階段不以「替換 `SortedList`」作為主目標。

### 3.2 `SortedDictionary` 的判斷

1. `SortedDictionary` 可改善寫入複雜度，但會破壞目前依賴連續陣列的 `Memory<'T>` 切片優勢。
2. `FirstLastN*`、`SortedListCache`、`getKeys/getValues` 等路徑都會重寫。
3. 在本 workload 下，不值得作為第一波整改方向。

### 3.3 `FASTER` 的判斷

1. 直接用 `FASTER` 取代 `PCSL` 不成立，因為 `FASTER` 不直接保留 ordered semantics。
2. 若只拿 `FASTER` 取代檔案系統，作為 persistence bridge 是可行選項。
3. 但這只會解掉碎檔案與部分 I/O 問題，無法解掉：
   - `SortedList` 的寫入複雜度
   - 雙狀態一致性
   - recovery flow 定義
4. 結論：保留為候選 persistence bridge，但不是目前主線。

### 3.4 `DBreeze` / `LiteDB` 類方案的判斷

1. 它們能提供全 .NET、嵌入式、成熟 persistence 能力。
2. 但若直接導入，`PCSL` 很容易退化成一層奇怪的 wrapper / pseudo-async DB facade。
3. 在目前需求下，我們不是要把 `PCSL` 轉型成 DB 包裝層，而是要保留它作為 ordered persisted collection 的 identity。
4. 結論：不列為本輪主線。

### 3.5 Pin memory / memcopy 類優化的判斷

1. 這只能改善搬移常數，不能改變 `SortedList` 的 `O(N)` 寫入特性。
2. 對本 workload 不是優先項。

### 3.6 自研 paged sorted structure / B+Tree-like structure 的判斷

1. 這條路有價值，但已經接近自研 storage engine。
2. 只有在未來 workload 轉成大量持續更新，或現行 `SortedList` 真正成為 benchmark 熱點時，才值得升級到這一層。
3. 目前先不作為近期整改主線。

## 4. 新的問題定義

在新目標下，問題被重新排序如下：

### 4.1 第一優先：Persistence 與 Recovery

1. 一筆一檔的 layout 不適合大規模資料。
2. 啟動索引重建太依賴檔案列舉。
3. 缺少明確的 source of truth、checkpoint 與 replay 策略。

### 4.2 第二優先：API / Contract 補齊

1. `DefaultHelper` 對 `OpResult` / `PCSLTaskTyp` 覆蓋不完整。
2. `PCSL.fsx` 仍留有 `upsert not yet implemented`。
3. `PCSL2` 還有 `FS0343` 與 equality/hash contract 問題。
4. `ASYNC` compile constant 與 `net10.0` 不一致。

### 4.3 第三優先：效能調校

1. persistence 路徑的檔案數與 I/O 行為
2. warmup / full buffer 流程
3. `autoCacheChange` 與大規模初載的成本

## 5. 新的設計約束

1. 保留 `PCSL` 作為 ordered persisted collection 的定位。
2. 保留 `FirstLastN*`、順序切片與 key 全序語意。
3. 以全 .NET 為優先，不引入 native dependency。
4. 不將 `PCSL` 改造成單純的 DB wrapper。
5. 先解 persistence，再視 benchmark 決定是否要升級排序主體。

## 6. 建議方向

### 6.1 近期主線

1. 保留 `CSL2` / `SortedList` 作為目前排序主體。
2. 定義新的 persistence spec：
   - append-only / segmented data log
   - key/index snapshot
   - manifest / checkpoint metadata
   - replay-based recovery
3. 將 persistence 抽成獨立介面，避免繼續散落在 `PCSL2`。
4. 優先補齊 helper / API / contract 問題。

### 6.2 中期觀察項

1. 若 benchmark 證明「偶爾小改」其實在真實 workload 中已經足夠頻繁到拖垮 `SortedList`，再評估：
   - `SortedDictionary`
   - 自研 paged sorted structure
   - B+Tree-like ordered core

### 6.3 保留候選但不列主線

1. `FASTER bridge`
2. `DBreeze`
3. `LiteDB`

## 7. 本次結論

在「首次大規模載入、後續偶爾小改」的前提下，`PCSL` 不需要先把 `SortedList` 當成頭號敵人。  
目前最值得優先投入的，是：

1. 重新定義 persistence spec
2. 補上 recovery / checkpoint / rebuild
3. 整理 API 與 helper 邊界
4. 把 `NOT YET IMPLEMENTED` 與已知 contract 問題清掉

也就是說，整改主軸從「全面換底層引擎」改成「保留 PCSL identity，先把 persistence 與工程完整性做好」。
