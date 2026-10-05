# Native queue 驗證入口

Registry revision：2（2026-10-05；401 metadata/source gate）；owner：Aster。Host RFC／SA／SD／WBS／Test 的 canonical 文件為 `G:/PulseTrade.fs/doc/PTC HOST 優化/`，此處只登記 native 單元回歸。

| ID | 入口／scope | 預設／effect | Oracle |
|---|---|---|---|
| PCSL-VFY-NATIVE-QUEUE-r1 | `misc/verify.nativeQueue.ps1`；`tests/PersistedConcurrentSortedList.Tests` 的 10 個實際 native cases | 零參數 PLAN；明示 `-Execute` 才在 fresh GUID TEMP build／執行。可指定 baseline DLL；不 Pack／publish、不登入／啟停 service、不碰 prod／MDC。每個 process bounded watchdog；raw-byte stdout／stderr與結果保留 | Queue lease 明確阻塞 status／base／兩個 index；IgnoreQ 完成，defaultfalse保持排隊。buffer／nonbuffer、delete、duplicate／cold reopen、index／value I/O failure拒絕；native DLL hash、source stability及actual10/10 totals |

一般使用：先執行 `./misc/verify.nativeQueue.ps1` 查看 PLAN，再執行 `./misc/verify.nativeQueue.ps1 -Execute`。預設編譯此 canonical checkout；baseline反證可使用 `-CandidateAssembly <absolute-built-baseline-DLL>`。fixture 為獨立 TEMP 資料，failed evidence 保留，不清除既有目錄。

此入口只證明 source／unit。原10.1.400 public package 沒有被覆寫；本次尚未形成新 immutable package或部署 Host。Host 同 Hub2007sets／browser／MCP真實測試、package exact consumer、三 Host test部署與prod對帳各有獨立 gate。

## REL-01 new immutable identity 202610050535
Canonicalpackage Version10.1.400→10.1.401。Source2a138b4 queue/index/valueopt-in fixesunchanged；defaultfalse排隊保證/IFileSystem預設不變。Main Host releaseclosure/currentSA-SD見G:/PulseTrade.fs/doc/PTC HOST 優化/Release.Closure.md。實際NuGet.Versioning minimum400拒400-win1、接受401，故newstablemetadataonly；原public400不覆寫。Baselinecopied14inputsbuild9.084s stablePASS；新version復用verify.nativeQueue.ps1 actual10cases驗證、禁止PackonBuild，fullpackage/Host/prod separate。GenerateNuspec NoBuild跳過本project AfterPack發布hook；不在log記secret，不使用formalenv。新401只reserved，未pack/published，SourceCommit與package payload後續逐一驗。

### 2026-10-05T05:40:31.1582036+08:00 REL Native401 source gate
Version401 actualverify.nativeQueue10 executed=passed/0Ignored/Failed/Errored，SourceStable，producer9.633s/testbuild5.798s/tests0.989s；DLL C2B59E0A3EBDF26050AC4765E9B8DF8120C007ACF1C177F947C9CA7B26F1AA41，raw C:/Users/Administrator/AppData/Local/Temp/pcsl-native-queue-verification-60151372ab4c4cfd9b1b4c001d4419f1。Independent copied kernel source14 build9.068s stablePASS，Registry new6 source8 build7.676s stablePASS。現在Nativeowned6files metadata/docs/log/oplog strictdecode/scanner/check→commit/push；新package尚未pack/published，SourceCommit完成後新artifact SourceRevisionId/RepositoryCommit需綁實際commit。No public400 overwrite，prod/MDClogin無操作。

## PCSL-VFY-CHUNK25-r1 intent (20261006)
Entry doc/RFC/20261006.25MB.Chunk/TEST.fsx plus TEST.fsproj. 13 real native disk cases, sequential isolated repo temp fixtures; no service/SQL/login/publish. Default decimal25MB. Baseline10.1.401 expected RED for unimplemented auto-chunk, counts must be reported honestly. Existing queue10 unchanged. Candidate from exact PcslAssemblyPath, output only each project bin/net10.0/agent.aster; no package fallback. Fault injection/crash/precise-threshold cases remain future MdcQuoteAgent TDD per SD, not covered by this initial executable suite.

### C25 baseline actual 202610060209
TEST13 executed,7PASS/6FAIL/0ignored/0errored,39.584s; six physical-limit failures expected against native10.1.401. Largest physical .val58,245,341bytes proves the requested size risk. Tests build0warnings/errors; native baseline99existingwarnings/0errors. No implementation/package/runtime change. See RFC folder WBS/README and log/20261006/20261006014200.chunk_rfc.op_log; fault/threshold/consumer gates not executed.
