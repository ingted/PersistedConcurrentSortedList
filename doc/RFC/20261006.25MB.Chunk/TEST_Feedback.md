# TEST feedback — 2026-10-06

原 13 個測試的 payload entropy、25,000,000 bytes assertion、cold roundtrip、missing/truncated failure oracle均保留，不以修改資料或門檻使實作通過。

合併執行入口需要一項 harness 調整：`TEST.fsx`與現有native queue `Program.fs`都有`EntryPoint`。將RFC測試置於明確的`Chunk25ContractTests` module，僅在combined project定義`PCSL_MERGED`時省略第二個EntryPoint；原獨立TEST.fsproj／FSI入口保留。combined project直接Compile同一份RFC test source，沒有複製或漏掉測試。

既有native project明示`OutputPath=./bin`，會優先於`--artifacts-path`的預設output推導；首次候選DLL路徑選錯造成harness build failure，未執行的tests不計PASS。使用每project明確、獨立的OutputPath／BaseIntermediateOutputPath，保留失敗紀錄；不放寬RequireCandidate，也不fallback已發布package。

精確threshold、custom hooks、舊class、crash points、cancel及read/update/delete race已補入同一Expecto suite；原13＋queue10不是完整chunk驗收範圍。既有`verify.nativeQueue.ps1`原本硬限制10項，現改由同一compiled test tree提供inventory，仍強制queue10/RFC13存在、全部新增cases實跑且總數/PASS等於inventory，ignored/fail/error=0；没有降低oracle。補每project明示OutputPath與AppendTargetFrameworkToOutputPath=false，保留source hash stability與candidate DLL exact hash gate。CLI child操作經FAkka.Argu，所有child bounded、hidden window，root有resolved TEMP allow-list及owner marker。

實際fixture資源問題：合併baseline23項17PASS/6sizeFAIL；新實作初次23項17PASS/6ERROR，六個error都是G槽剩約30MB的disk-full，不是門檻assertion通過。原harness把fixtures硬限於repo/G槽，對這包高entropy測試造成資源依賴。預設改為獨立`Path.GetTempPath()/pcsl-chunk-contract/<GUID>`（本機C槽），保留repo temp作明示選項；兩種root皆resolved/allow-listed、每次新GUID，payload/limit/assertions不變。兩份本輪G槽fixture在驗ownership marker及前後逐檔hash/length後移到C槽保留，沒有清理他人的資料，也不把resource-error結果當成PASS。

入口結果修正：原verify script用Diagnostics.Process而非native shell命令，成功branch未明示exit0，會承接caller的LASTEXITCODE。實際84/84PASS曾回報外層exit1，原結果保留；PLAN及success branch現明示exit0，failure仍exit1。以真正前置native exit73再跑完整84項，驗證成功同時shell exit0；沒有在caller重設LASTEXITCODE掩蓋問題。

Encoding closure：modified source/project及外層repo既有native401 closure的UTF8 BOM只移除BOM，保留正文與原tests；每次source hash改動後重跑canonical完整suite，receipt分開保留，不覆寫舊PASS/FAIL證據。

20261010 C25-05 perf gate新增：PTC隔離bench（`G:/PulseTrade.fs/temp/agent.aster/gw-task-recovery/native402-compat/small-write-perf/bench.fsx`，開發scratch非正式evidence authority）20次暖身＋200次同key小值更新，兩輪互換執行順序，401 p50/p95=0.495/0.647與0.590/0.844ms、402=16.852/22.590與17.204/22.567ms；cold read均PASS。正式結論以PTC `log/20261010/20261010211205.gw_pcsl_atomic_followup.log` 為準。修小值路徑時先讓相同small/large/crash oracle失敗，再以完整84項及新增小值臨界／格式遷移／真child重啟測試轉綠；另量GW task/status同負載before/after CPU、allocation、I/O及延遲，不能只用微基準宣稱正式效能。

20261011 native regression：原84項新增3個既有key小值直接格式／precommit-postcommit／legacy reader案例、3個真child before/after anchor/after index abrupt exit＋fresh process cold read/retry、2個64 KiB/empty/manifest magic格式邊界，以及2個indexed direct遇到linked舊generation owner的拒絕案例，總94項。首次baseline120秒watchdog沒有summary；第一版87項81PASS/5FAIL/1ERROR精確定位到測試threshold覆蓋fault callback及待完成tombstone刪到新spool，均修正且失敗receipt保留。最終source-stable 94/94 PASS、zero ignored/failed/errored，DLL SHA `45E85B90FBC118AB12644E9FCE26DEED25154E098AC8B227913D5B545695AA33`，evidence位於本輪專屬TEMP `pcsl-native-queue-verification-1eb4a456f5154aa8b65190581c5f8451`。同顆DLL 20暖身＋200更新雙順序的具體效能數字在SD；cold read均PASS。

隔離GW provider負載：來源`G:/PulseTrade.fs/temp/agent.aster/gw-task-recovery/native402-compat/gw-task-postrepair.zip` SHA `3CB5FB6AA7836E066E11DBA08462B8F1C14D64343A16894A8D7AA6F917520E35`；ZIP entry路徑/符號連結檢查後，在`C:/Users/Administrator/AppData/Local/Temp/pcsl-c25-gwload-daa9b20c2afa4779a224856af60bb6fa/`建立四份獨立且同數量的case（每份5141資料檔＋owner marker）。F#腳本`gwbench.fsx`以exact native hash gate載入正式GW DLL、依TicketId穩定排序，取20個status各暖身一次，再循環`SaveStatusAsync`200次，ABBA；每輪cold reopen後2293筆digest與來源一致。輸出只含aggregate latency/throughput/CPU/allocation與Win32 process read/write operation/transfer counts，不列status內容。這是isolated provider call，不是完整GW Host process E2E；allocation跨輪波動，不宜獨立決策。腳本與快照保留在本輪TEMP，正式GW仍native401。

後續atomic原語對照固定124-byte synthetic anchor，20暖身＋200次、反向順序、逐輪digest PASS；腳本與p50見SD。`File.Replace`/WriteThrough均比原`Flush(true)`＋Move overwrite慢；沒有替代GW代表性負載或產生新的DLL候選。GW owner對較弱ACK持久化契約未定案前，94/94 source PASS不能升級為正式package/Host PASS。
