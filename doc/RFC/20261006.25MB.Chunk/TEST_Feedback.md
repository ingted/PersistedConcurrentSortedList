# TEST feedback — 2026-10-06

原 13 個測試的 payload entropy、25,000,000 bytes assertion、cold roundtrip、missing/truncated failure oracle均保留，不以修改資料或門檻使實作通過。

合併執行入口需要一項 harness 調整：`TEST.fsx`與現有native queue `Program.fs`都有`EntryPoint`。將RFC測試置於明確的`Chunk25ContractTests` module，僅在combined project定義`PCSL_MERGED`時省略第二個EntryPoint；原獨立TEST.fsproj／FSI入口保留。combined project直接Compile同一份RFC test source，沒有複製或漏掉測試。

既有native project明示`OutputPath=./bin`，會優先於`--artifacts-path`的預設output推導；首次候選DLL路徑選錯造成harness build failure，未執行的tests不計PASS。使用每project明確、獨立的OutputPath／BaseIntermediateOutputPath，保留失敗紀錄；不放寬RequireCandidate，也不fallback已發布package。

精確threshold、custom hooks、舊class、crash points、cancel及read/update/delete race已補入同一Expecto suite；原13＋queue10不是完整chunk驗收範圍。既有`verify.nativeQueue.ps1`原本硬限制10項，現改由同一compiled test tree提供inventory，仍強制queue10/RFC13存在、全部新增cases實跑且總數/PASS等於inventory，ignored/fail/error=0；没有降低oracle。補每project明示OutputPath與AppendTargetFrameworkToOutputPath=false，保留source hash stability與candidate DLL exact hash gate。CLI child操作經FAkka.Argu，所有child bounded、hidden window，root有resolved TEMP allow-list及owner marker。

實際fixture資源問題：合併baseline23項17PASS/6sizeFAIL；新實作初次23項17PASS/6ERROR，六個error都是G槽剩約30MB的disk-full，不是門檻assertion通過。原harness把fixtures硬限於repo/G槽，對這包高entropy測試造成資源依賴。預設改為獨立`Path.GetTempPath()/pcsl-chunk-contract/<GUID>`（本機C槽），保留repo temp作明示選項；兩種root皆resolved/allow-listed、每次新GUID，payload/limit/assertions不變。兩份本輪G槽fixture在驗ownership marker及前後逐檔hash/length後移到C槽保留，沒有清理他人的資料，也不把resource-error結果當成PASS。

入口結果修正：原verify script用Diagnostics.Process而非native shell命令，成功branch未明示exit0，會承接caller的LASTEXITCODE。實際84/84PASS曾回報外層exit1，原結果保留；PLAN及success branch現明示exit0，failure仍exit1。以真正前置native exit73再跑完整84項，驗證成功同時shell exit0；沒有在caller重設LASTEXITCODE掩蓋問題。

Encoding closure：modified source/project及外層repo既有native401 closure的UTF8 BOM只移除BOM，保留正文與原tests；每次source hash改動後重跑canonical完整suite，receipt分開保留，不覆寫舊PASS/FAIL證據。

20261010 C25-05 perf gate新增：PTC隔離bench（`G:/PulseTrade.fs/temp/agent.aster/gw-task-recovery/native402-compat/small-write-perf/bench.fsx`，開發scratch非正式evidence authority）20次暖身＋200次同key小值更新，兩輪互換執行順序，401 p50/p95=0.495/0.647與0.590/0.844ms、402=16.852/22.590與17.204/22.567ms；cold read均PASS。正式結論以PTC `log/20261010/20261010211205.gw_pcsl_atomic_followup.log` 為準。修小值路徑時先讓相同small/large/crash oracle失敗，再以完整84項及新增小值臨界／格式遷移／真child重啟測試轉綠；另量GW task/status同負載before/after CPU、allocation、I/O及延遲，不能只用微基準宣稱正式效能。
