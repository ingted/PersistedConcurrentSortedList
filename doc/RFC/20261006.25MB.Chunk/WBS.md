# WBS／Test進度

時間UTC+8，owner Aster文件／M哥實作。預估是agent active time，不含NuGet外部等待；後續owner接手重估，不把Proposed設計當Done。

| ID | Status | Progress | 摘要／驗收 | 開始 | 已耗 | 剩餘Dev/Test | ETA yyyymmddhhmm |
|---|---|---:|---|---|---|---|---|
| C25-01 | Done | 100% | RFC/SA/SD＋Expecto13實跑：7PASS/6預期RED/0error/0skip；sensitive0High/Medium、checker0FAIL，交付TDD契約 | 202610060142 | 37分 | 0/0分 | 202610060219 |
| C25-02 | Done | 100% | bounded default stream/custom spool、strict manifest/hash/path/count、legacy decode、exact4096±1/empty/multiple、precommit/postcommit/rollback I/O；canonical source/test84/84全部PASS | 202610060536 | 約50分（02–04共用實作/測試） | 0/0 | 202610060626 |
| C25-03 | Done | 100% | PCSL/PCSL2共用physical format；index-last/anchor atomic、Delete tombstone、memory publication順序、both-class cold/mutations、full read/buffer lease、cancel及原queue semantics實測PASS | 202610060536 | 同上，不重複計時 | 0/0 | 202610060626 |
| C25-04 | Done | 100% | canonical merged84/84 ALL PASSED：queue10＋same-source RFC13＋storage22＋recovery20＋safety19；0ignored/fail/error；六點真child abrupt exit/fresh child reopen/retry、DLL hash/sourceStable PASS；保留所有RED/resource failures | 202610060536 | 同上；final完整run93.940秒 | 0/0 | 202610060626 |
| C25-05 | InProgress_PerfBlocked | 15% | IFileSystem cold index source修正＋跨程序讀回已驗；native402小值寫入兩輪p50約16.9–17.2ms，401約0.5–0.6ms，原候選不可直接發版。待native owner修效能、同口徑回歸，再走immutable package／PTCS／三Host | 202610102216 | 30分（Aster隔離整合／分析） | 修正與測試由owner重估；後續package/Host 40–80/45–90分 | 202610111800（條件：12:00前修perf；否則重估） |

Baseline source build：10.1.401、99 warnings、0 errors，output `bin/net10.0/agent.aster/chunk-rfc/`。TEST Release build0warning/0error；實際13tests／7PASS／6FAIL／0ignored／0errored，39.584秒。失敗均為當前native未chunk：`.val`最大58,245,341 bytes；完整資料cold roundtrip已在size assertion前確認。這是TDD交付的RED baseline，並非已完成auto-chunk。原生實作檔本輪不改，無NuGet push。

Fixture root：`temp/agent.aster/chunk25/4953456bfdc242c7933963ccd352573b`。精確操作與初次harness路徑修正見`log/20261006/20261006014200.chunk_rfc.op_log`。

## Current development acceptance — 2026-10-06

Identity `10.1.402-dev.chunk25.1`；native Release build99既有FS0025/FS0064 warnings/0errors，mergedtests build0warnings/errors。`misc/verify.nativeQueue.ps1 -Execute` final fresh candidate/obj執行：84 executed/84passed/0ignored/0failed/0errored，sourceStable=true，DLL SHA `9998568A0A372FD0A3984885D30E005BD87261E296DFC6E97D24635030F032B3`；實際秒數見final receipt。原始stdout與結果：`log/20261006/20261006053629.chunk_impl.final.{tests.txt,build.txt,test-build.txt,verification.json,source-hashes.json}`；詳細operations同prefix op_log。較早84項run93.940秒／DE1E1F...亦獨立保留，不改稱最終source。完整large single-value與>50MB case未減entropy，legacy read-only不遷移、後續write遷移，實際logical hash/cold reads/physical限額全部PASS。

六commit points包含before/afterPartFlush、before/afterAnchorPublish、afterIndexPublish、afterIndexUnpublish；以真child `Environment.Exit(86)`中斷，不執行catch/finally/ACK，fresh child重開後舊/新/absent與retry完整值都驗。另驗legacy orphan rollback/crash恢復、Delete pending/retry、不復活、不刪neighbor、unknown folder保留、reparse拒絕、read/update/delete interleave、cancel不buffer、hooks一次、Add/Update duplicate/missing及public低階key-hash helper語意。

本輪baseline合併23=17PASS/6sizeFAIL；首次chunk23=17PASS/6disk-fullERROR不是PASS。G槽本輪兩GUIDfixtures驗owner/resolved path及前後relative SHA/length後移至`C:/coldfar-scratch/sharftrade9/PcslChunk25-20261006/fixture-archive/`保留；harness預設改為專屬system TEMP，見TEST_Feedback。沒有删除其他WIP。後續45/63/78/84均完整實跑零skip/error，保留前次失敗。

C25-02/03/04為Development milestone complete / Not published。C25-05正式package、IFileSystem exact pin、PTCS physical size/enum/purge/ACK/Host restart與三Host部署未執行；不是native source完成的衍生部署授權。Flush/atomic rename及process crash已測，不宣稱硬體斷電驗收。Canonical sensitive scanner在本harness未提供，該gate未驗；不能以普通checker代替或稱完整publication closure。

20261010 C25-05整合反證：PTC正式GW仍native401；Aster用exact native402 DLL SHA `9998568A0A372FD0A3984885D30E005BD87261E296DFC6E97D24635030F032B3` 在隔離GW task clone讀回2293筆、digest不變；IFileSystem 10.1.401 constructor漏`IndexInitialize()`，修正source已於PTC commit `9217f7ae6`，Expecto2/2及高熵37,333,336字元跨程序讀回PASS。小值效能兩輪結果見SA/SD與PTC `log/20261010/20261010211205.gw_pcsl_atomic_followup.log`；候選402原樣不發布、不部署。這些是source/isolated gates，非formal release。
