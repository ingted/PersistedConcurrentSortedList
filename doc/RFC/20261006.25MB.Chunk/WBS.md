# WBS／Test進度

時間UTC+8，owner Aster文件／M哥實作。預估是agent active time，不含NuGet外部等待；後續owner接手重估，不把Proposed設計當Done。

| ID | Status | Progress | 摘要／驗收 | 開始 | 已耗 | 剩餘Dev/Test | ETA yyyymmddhhmm |
|---|---|---:|---|---|---|---|---|
| C25-01 | Done | 100% | RFC/SA/SD＋Expecto13實跑：7PASS/6預期RED/0error/0skip；sensitive0High/Medium、checker0FAIL，交付TDD契約 | 202610060142 | 37分 | 0/0分 | 202610060219 |
| C25-02 | NotStarted | 0% | M哥：bounded stream chunk sink/manifest/legacy codec；先小threshold boundary RED | 未開始 | 0 | 50–90/30–50分 | 接手排程 |
| C25-03 | NotStarted | 0% | M哥：atomic anchor/index、mutations/hooks/reader lease/tombstone | 未開始 | 0 | 70–110/40–70分 | 依C25-02 |
| C25-04 | NotStarted | 0% | M哥：crash/partial write/cancel/讀刪競態、原10 queue regressions | 未開始 | 0 | 35–60/50–90分 | 依C25-03 |
| C25-05 | NotStarted | 0% | immutable package／IFileSystem與PTCS size/purge closure，Host驗收分列 | 未開始 | 0 | 40–80/45–90分 | 依C25-04 |

Baseline source build：10.1.401、99 warnings、0 errors，output `bin/net10.0/agent.aster/chunk-rfc/`。TEST Release build0warning/0error；實際13tests／7PASS／6FAIL／0ignored／0errored，39.584秒。失敗均為當前native未chunk：`.val`最大58,245,341 bytes；完整資料cold roundtrip已在size assertion前確認。這是TDD交付的RED baseline，並非已完成auto-chunk。原生實作檔本輪不改，無NuGet push。

Fixture root：`temp/agent.aster/chunk25/4953456bfdc242c7933963ccd352573b`。精確操作與初次harness路徑修正見`log/20261006/20261006014200.chunk_rfc.op_log`。
