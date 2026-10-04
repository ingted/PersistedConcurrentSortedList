# Native queue 驗證入口

Registry revision：1（2026-10-04）；owner：Aster。Host RFC／SA／SD／WBS／Test 的 canonical 文件為 `G:/PulseTrade.fs/doc/PTC HOST 優化/`，此處只登記 native 單元回歸。

| ID | 入口／scope | 預設／effect | Oracle |
|---|---|---|---|
| PCSL-VFY-NATIVE-QUEUE-r1 | `misc/verify.nativeQueue.ps1`；`tests/PersistedConcurrentSortedList.Tests` 的 10 個實際 native cases | 零參數 PLAN；明示 `-Execute` 才在 fresh GUID TEMP build／執行。可指定 baseline DLL；不 Pack／publish、不登入／啟停 service、不碰 prod／MDC。每個 process bounded watchdog；raw-byte stdout／stderr與結果保留 | Queue lease 明確阻塞 status／base／兩個 index；IgnoreQ 完成，defaultfalse保持排隊。buffer／nonbuffer、delete、duplicate／cold reopen、index／value I/O failure拒絕；native DLL hash、source stability及actual10/10 totals |

一般使用：先執行 `./misc/verify.nativeQueue.ps1` 查看 PLAN，再執行 `./misc/verify.nativeQueue.ps1 -Execute`。預設編譯此 canonical checkout；baseline反證可使用 `-CandidateAssembly <absolute-built-baseline-DLL>`。fixture 為獨立 TEMP 資料，failed evidence 保留，不清除既有目錄。

此入口只證明 source／unit。原10.1.400 public package 沒有被覆寫；本次尚未形成新 immutable package或部署 Host。Host 同 Hub2007sets／browser／MCP真實測試、package exact consumer、三 Host test部署與prod對帳各有獨立 gate。
