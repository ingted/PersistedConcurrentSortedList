# RFC-PCSL-0025：25MB physical value auto-chunk

狀態：Proposed；需求已由人類指定，實作交M哥（MdcQuoteAgent）TDD。Aster負責本文件及可執行反例，不在此發布新package。

## 背景／目標

PTC Host所有PCSL將置於`G:/PulseTrade.fs/pcsl/`並納Git。現行native 10.1.401把一個logical value壓縮後一次寫入一個`.val`；單筆可以超過Git提交政策的50,000,000 bytes。上層stream records-per-chunk不能限制任意大單筆，也不能替代native解法。

所有**新寫入／更新完成**的physical `.val`檔必須≤**25,000,000 bytes（decimal MB，不是MiB）**。logical key/value、排序、public Add/Update/Upsert/TryGet/Remove API與結果不變。單一value超過25MB乃至50MB時自動分塊、讀取透明重組；不可截斷、拒收原本合法值或要求caller自行split。

大小以**原有serializer＋Deflate後的實際bytes**衡量，不以字元、未壓縮大小、record count估算。預設25MB，不需caller設定；測試內部可注入小threshold驗精確邊界，正式不暴露混亂的per-call設定。

## 相容性與非目標

- 舊單檔`.val`保留可讀，讀取本身不改寫；下一次成功Upsert轉新layout。既有超大legacy檔不因只讀而變小，Git checkpoint仍須先由owner遷移。
- 新layout須升immutable package版本，禁止覆寫10.1.401。新writer建立後不能讓舊reader繼續寫同root；部署先升reader、停writer後切換，回退只用切換前checkpoint。
- 保留public自訂`Write2File`／`ReadFromFile` hooks語意：詳細兼容策略见SD，不能靜默繞過hook。queue/default IgnoreQ、buffered狀態與既有錯誤不可降級。
- 不實作Git commit worker、跨process多writer協調、網路storage、壓縮算法替換或PTCS特定event解析。
- 機器断電的filesystem durability須以flush／atomic rename能力實测，不能以Task完成等同硬體永不丟資料。

## 決策與取捨

採per-key immutable generation chunks＋小型atomic `.val` manifest anchor。value commitpoint是anchor替換；new-key visibility還需既有`.index`最後發布。以bounded stream處理default codec；custom file hook用ignored staging file adapter兼容。Remove先unpublish index再回收anchor/chunks，重啟不得復活已刪key。

每段有長度／SHA256，manifest有version、key hash、generation、codec、總長度／全體hash、ordered parts。讀取驗完整後才返回value，缺段/重排/截斷/未知version明確failure，不能回None冒充不存在。SHA是完整性偵測，不是對不可信來源的身份驗證。

## 驗收／交付

[SA](SA.md) → [SD](SD.md) → [TEST.fsx](TEST.fsx)／[TEST.fsproj](TEST.fsproj) → M哥實作 → 原10個native queue tests＋physical disk/cold process/fault suites → immutable package與IFileSystem/PTCS exact consumers。

TEST目前對未實作candidate預期部分RED；不能改assertion、降低payload熵或只挑既有PASS宣稱完成。執行與進度见[WBS](WBS.md)。
