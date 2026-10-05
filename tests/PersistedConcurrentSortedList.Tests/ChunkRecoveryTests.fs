module PersistedConcurrentSortedList.Tests.ChunkRecoveryTests

open System
open System.Diagnostics
open System.IO
open System.Reflection
open System.Text
open System.Threading
open System.Threading.Tasks
open Argu
open Expecto
open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type
open ChunkStorageTests

type Arguments =
    | Inventory
    | Crash_Root of string
    | Crash_Phase of string
    | Crash_Mode of string
    | Verify_Root of string
    | Verify_Expected of string
    interface IArgParserTemplate with
        member _.Usage = "Bounded synthetic child process crash/reopen test; no services or publication."

let parser = ArgumentParser.Create<Arguments>(programName = "PCSL.Tests")
let phase = function
    | "before-part" -> ChunkedValue.BeforePartFlush 0
    | "after-part" -> ChunkedValue.AfterPartFlush 0
    | "before-anchor" -> ChunkedValue.BeforeAnchorPublish
    | "after-anchor" -> ChunkedValue.AfterAnchorPublishBeforeIndex
    | "after-index" -> ChunkedValue.AfterIndexPublish
    | "after-unpublish" -> ChunkedValue.AfterIndexUnpublish
    | _ -> invalidArg "phase" "Unknown crash phase."
let phases = [ "before-part"; "after-part"; "before-anchor"; "after-anchor"; "after-index" ]
let newValue = Convert.ToBase64String(bytes 91 12000)
let checkChildRoot root =
    let absolute = Path.GetFullPath root
    let prefix = Chunk25ContractTests.fixtureTempRoot.TrimEnd(Path.DirectorySeparatorChar) + string Path.DirectorySeparatorChar
    if not (absolute.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) then failwith "Child fixture escapes dedicated TEMP root."
    if File.ReadAllText(Path.Combine(absolute, "owner.txt")) <> "PCSL chunk contract synthetic fixture" then failwith "Child fixture ownership mismatch."
    absolute

let child arguments =
    let parsed = parser.ParseCommandLine arguments
    if parsed.Contains Crash_Root then
        let root = checkChildRoot(parsed.GetResult Crash_Root)
        let point = phase(parsed.GetResult Crash_Phase)
        let mode = parsed.GetResult Crash_Mode
        let store = native root
        store.IndexInitialize()
        let fault observed =
            if observed = point then
                File.WriteAllText(Path.Combine(root, "crash-hit.txt"), sprintf "%A" point)
                Environment.Exit 86 // Actual abrupt termination: no catch/finally/disposal/ACK.
        ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () ->
            match mode with
            | "new" | "update" | "orphan" -> put store newValue
            | "delete" -> store.RemoveAsync("key", true) |> awaitAll
            | _ -> invalidArg "mode" "Unknown crash mode.")
        failwith "Crash seam was not reached."
    elif parsed.Contains Verify_Root then
        let root = checkChildRoot(parsed.GetResult Verify_Root)
        let expected = parsed.GetResult Verify_Expected
        let store = native root
        store.IndexInitialize()
        if expected = "orphan" then
            Expect.equal store._idx.CountSafe 0 "The legacy orphan has no visible key."
            let path = ChunkedValue.anchorPath (Path.Combine(root, "store")) (store.GenerateKeyHash "key")
            Expect.equal (PB.ModelContainer<fCell2<string>>.readFromFile path) (Some(fCell2.S "old")) "Recovery preserves the original orphan bytes."
            Expect.throws(fun () -> store.TryGetValueNoThreadLock("key", 60000, true) |> ignore) "Existing legacy orphan guard remains explicit."
        else
            let actual = store.TryGetValueNoThreadLock("key", 60000, true)
            let value = match expected with "absent" -> false, None | "old" -> true, Some(fCell2.S "old") | "new" -> true, Some(fCell2.S newValue) | _ -> failwith "Unknown expected state."
            if actual <> value then failwith "Fresh child reopen returned an incomplete or incorrect logical value."
        0
    else failwith "No child operation supplied."

let runChild root arguments expectedExit =
    let start = ProcessStartInfo("dotnet", UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true)
    start.ArgumentList.Add(Assembly.GetExecutingAssembly().Location)
    for argument in arguments do start.ArgumentList.Add argument
    use childProcess = new Process(StartInfo = start)
    childProcess.Start() |> ignore
    let stdout = childProcess.StandardOutput.ReadToEndAsync()
    let stderr = childProcess.StandardError.ReadToEndAsync()
    if not (childProcess.WaitForExit 30000) then
        childProcess.Kill true
        childProcess.WaitForExit()
        failwith "Synthetic child exceeded 30-second watchdog."
    let text = stdout.GetAwaiter().GetResult() + stderr.GetAwaiter().GetResult()
    File.WriteAllText(Path.Combine(root, Guid.NewGuid().ToString("N") + ".child.txt"), text)
    Expect.equal childProcess.ExitCode expectedExit "Fresh child must finish at the requested crash/reopen boundary."

let verifyChild root expected = runChild root [ "--verify-root"; root; "--verify-expected"; expected ] 0
let legacy root =
    PCSL.PersistedConcurrentSortedList<string, fCell2<string>>(1, root, "store", 60000,
        DefaultHelper.PCSLFunHelper<string, fCell2<string>>.oFun,
        DefaultHelper.PCSLFunHelper<string, fCell2<string>>.eFun, autoCache = 0)
let legacyPut (store: PCSL.PersistedConcurrentSortedList<string, fCell2<string>>) text removeBuffer =
    store.UpsertAsync("key", fCell2.S text, removeBuffer, true) |> Array.map (fun task -> task :> Task) |> awaitAll

let tests parent = testSequenced <| testList "PCSL abrupt exit and read ownership" [
    for point in ["before-anchor"; "after-anchor"] do
        testCase ("abrupt unpublished write preserves legacy orphan at " + point) (fun _ ->
            let root = fixture parent
            let store = native root
            PB.ModelContainer<fCell2<string>>.write2File (ChunkedValue.anchorPath (Path.Combine(root, "store")) (store.GenerateKeyHash "key")) (fCell2.S "old")
            runChild root ["--crash-root"; root; "--crash-phase"; point; "--crash-mode"; "orphan"] 86
            verifyChild root "orphan")
    for mode in ["new"; "update"] do
        for point in phases do
            testCase (sprintf "abrupt %s at %s then cold process and retry" mode point) (fun _ ->
                let root = fixture parent
                if mode = "update" then put (native root) "old"
                runChild root ["--crash-root"; root; "--crash-phase"; point; "--crash-mode"; mode] 86
                Expect.isTrue (File.Exists(Path.Combine(root, "crash-hit.txt"))) "This case executed the precise fault seam."
                let committed = point = "after-index" || (mode = "update" && point = "after-anchor")
                verifyChild root (if committed then "new" elif mode = "update" then "old" else "absent")
                smallThreshold(fun () -> put (native root) newValue)
                verifyChild root "new"
                for path in Directory.EnumerateFiles(root, "*.val", SearchOption.AllDirectories) do
                    Expect.isLessThanOrEqual (FileInfo(path).Length) ChunkedValue.DefaultLimit "Crash/retry preserves the physical bound.")
    testCase "abrupt delete after index unpublish never resurrects then retry" (fun _ ->
        let root = fixture parent
        smallThreshold(fun () -> put (native root) newValue)
        runChild root ["--crash-root"; root; "--crash-phase"; "after-unpublish"; "--crash-mode"; "delete"] 86
        verifyChild root "absent"
        Expect.equal (Directory.GetFiles(root, "*.val", SearchOption.AllDirectories).Length) 0 "Reopen resumes tombstone cleanup."
        smallThreshold(fun () -> put (native root) newValue)
        verifyChild root "new")
    for removeBuffer in [false; true] do
        testCase (sprintf "legacy class writes and PCSL2 cross reads removeBuffer=%b" removeBuffer) (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let older = legacy root
            legacyPut older newValue removeBuffer
            let newer = native root
            newer.IndexInitialize()
            Expect.equal (newer.TryGetValueNoThreadLock("key", 60000, true)) (true, Some(fCell2.S newValue)) "Both classes share the exact native format."
            put newer "replacement"
            let coldOld = legacy root
            coldOld.IndexInitialize()
            Expect.equal (coldOld.TryGetValueNoThreadLock("key", 60000, true)) (true, Some(fCell2.S "replacement")) "Old class reads the newer writer."
            coldOld.RemoveAsync("key", true) |> Array.map (fun task -> task :> Task) |> awaitAll
            verifyChild root "absent"
            Expect.equal (Directory.GetFiles(root, "*.val", SearchOption.AllDirectories).Length) 0 "Old class remove reclaims every chunk."))
    for oldClass in [false; true] do
        for removing in [false; true] do
            testCase (sprintf "read lease covers buffer publication old=%b delete=%b" oldClass removing) (fun _ -> smallThreshold(fun () ->
                let root = fixture parent
                put (native root) newValue
                let older = if oldClass then Some(legacy root) else None
                let newer = if oldClass then None else Some(native root)
                older |> Option.iter (fun store -> store.IndexInitialize())
                newer |> Option.iter (fun store -> store.IndexInitialize())
                let read () = match older, newer with Some store, _ -> store.TryGetValueNoThreadLock("key", 60000, true) | _, Some store -> store.TryGetValueNoThreadLock("key", 60000, true) | _ -> failwith "store"
                let mutate () =
                    match older, newer with
                    | Some store, _ when removing -> store.RemoveAsync("key", true) |> Array.map (fun task -> task :> Task) |> awaitAll
                    | Some store, _ -> legacyPut store "replacement" false
                    | _, Some store when removing -> store.RemoveAsync("key", true) |> awaitAll
                    | _, Some store -> store.UpsertAsync("key", fCell2.S "replacement", false, true) |> awaitAll
                    | _ -> failwith "store"
                use entered = new ManualResetEventSlim()
                use release = new ManualResetEventSlim()
                use mutationStarted = new ManualResetEventSlim()
                let fault point = if point = ChunkedValue.BeforeReadDecode then entered.Set(); if not (release.Wait 5000) then failwith "read barrier watchdog"
                let reading = Task.Run(fun () -> ChunkedValue.withTestSettings (configured fault CancellationToken.None) read)
                Expect.isTrue (entered.Wait 5000) "Reader reached verification/decode while holding its lease."
                let changing = Task.Run(fun () -> mutationStarted.Set(); mutate ())
                try
                    Expect.isTrue (mutationStarted.Wait 5000) "Mutation task actually started."
                    Expect.isFalse (changing.Wait 100) "Mutation cannot reclaim or replace a generation during a read."
                finally release.Set()
                Expect.equal (reading.GetAwaiter().GetResult()) (true, Some(fCell2.S newValue)) "Active read returns a complete prior snapshot."
                changing.WaitAsync(TimeSpan.FromSeconds 10.).GetAwaiter().GetResult()
                Expect.equal (read ()) (if removing then false, None else true, Some(fCell2.S "replacement")) "Completed delete cannot be undone by late buffer publication."))
    testCase "cancelled read publishes no buffer and can retry" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        put (native root) newValue
        let store = native root
        store.IndexInitialize()
        use cancellation = new CancellationTokenSource()
        let fault stage = if stage = ChunkedValue.BeforeReadDecode then cancellation.Cancel()
        Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault cancellation.Token) (fun () -> store.TryGetValueNoThreadLock("key", 60000, true) |> ignore)) "Cancelled read cannot return or cache a partial value."
        Expect.equal store._base.CountSafe 0 "No buffer was published."
        Expect.equal (store.TryGetValueNoThreadLock("key", 60000, true)) (true, Some(fCell2.S newValue)) "A fresh read succeeds after cancellation."))
]
