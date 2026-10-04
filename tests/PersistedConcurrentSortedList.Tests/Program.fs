module PersistedConcurrentSortedList.Tests.Program

open System
open System.IO
open System.Threading.Tasks
open Expecto
open PersistedConcurrentSortedList.PCSL2
open PersistedConcurrentSortedList.CSL2
open PersistedConcurrentSortedList.Type

let value text = fCell2<string>.S text

let fixture () =
    let root = Path.Combine(Path.GetTempPath(), "ptc-native-queue-tests", Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory(root) |> ignore
    File.WriteAllText(Path.Combine(root, "owner.txt"), "Aster native queue regression; synthetic data only")
    root, PersistedConcurrentSortedList<string, fCell2<string>>(1, root, "store", 5000, autoCache = 0)

let awaitAll (tasks: Task array) = Task.WhenAll(tasks).GetAwaiter().GetResult()

let wait (pending: Task) = pending.WaitAsync(TimeSpan.FromSeconds 5.).GetAwaiter().GetResult()

let holdQueue (queue: QueueProcessor<SLTyp, string, 'Value>) =
    let lease = queue.RequireLock(Some 5000, None) |> Async.RunSynchronously
    match lease with
    | Some id -> { new IDisposable with member _.Dispose() = queue.UnLock id }
    | None -> failwith "Native queue lock was not acquired."

let checkCold root key expected =
    let reopened = PersistedConcurrentSortedList<string, fCell2<string>>(1, root, "store", 5000, autoCache = 0)
    reopened.IndexInitialize()
    let found, stored = reopened.TryGetValueNoThreadLock(key, 5000, true)
    Expect.equal found true "The accepted key is durable."
    Expect.equal stored (Some expected) "Cold recovery preserves the whole payload."

let bypassUpsert removeBuffer blockValueQueue =
    let root, store = fixture ()
    let statusLease = holdQueue store._pstatus.OpQueue
    let valueLease = if blockValueQueue then Some (holdQueue store._base.OpQueue) else None
    let mutable pending: Task = Task.CompletedTask
    try
        pending <- Task.Factory.StartNew((fun () -> store.UpsertAsync("key", value "payload", removeBuffer, true) |> awaitAll), TaskCreationOptions.LongRunning)
        let completed = pending.Wait(750)
        Expect.isTrue completed "IgnoreQ must complete without waiting for a locked internal queue."
        wait pending
    finally
        valueLease |> Option.iter (fun lease -> lease.Dispose())
        statusLease.Dispose()
        wait pending
    let expectedStatus = if removeBuffer then NonBuffered else Buffered
    Expect.equal store._pstatus._base.["key"] expectedStatus "Persistence status is complete before return."
    Expect.equal store._base.CountSafe (if removeBuffer then 0 else 1) "The explicit buffer policy is preserved."
    checkCold root "key" (value "payload")

let tests =
    testList "PCSL native queue contract" [
        testCase "buffered IgnoreQ bypasses locked persistence status" (fun _ -> bypassUpsert false false)
        testCase "nonbuffered IgnoreQ bypasses locked persistence status" (fun _ -> bypassUpsert true false)
        testCase "nonbuffered IgnoreQ bypasses locked value queue" (fun _ -> bypassUpsert true true)
        testCase "default UpsertAsync still waits for persistence status queue" (fun _ ->
            let root, store = fixture ()
            let lease = holdQueue store._pstatus.OpQueue
            let pending = Task.WhenAll(store.UpsertAsync("key", value "queued", false))
            try Expect.isFalse pending.IsCompleted "Default overload must preserve queued completion."
            finally lease.Dispose()
            wait pending
            checkCold root "key" (value "queued"))
        testCase "RemoveAsync IgnoreQ bypasses all internal queues" (fun _ ->
            let root, store = fixture ()
            store.UpsertAsync("key", value "erase", false, true) |> awaitAll
            let a = holdQueue store._base.OpQueue
            let b = holdQueue store._pstatus.OpQueue
            let c = holdQueue store._idx.OpQueue
            // The reverse-index queue has different key/value types; acquire it directly.
            let reverseLease = store._idxR.OpQueue.RequireLock(Some 5000, None) |> Async.RunSynchronously |> Option.get
            let mutable pending: Task = Task.CompletedTask
            try
                pending <- Task.WhenAll(store.RemoveAsync("key", true))
                Expect.isTrue (pending.Wait(750)) "Every Remove completion must bypass its queue."
                wait pending
            finally
                a.Dispose(); b.Dispose(); c.Dispose()
                store._idxR.OpQueue.UnLock reverseLease
                wait pending
            Expect.equal store._base.CountSafe 0 "No buffered value remains."
            Expect.equal store._pstatus.CountSafe 0 "No stale status remains."
            Expect.equal store._idx.CountSafe 0 "Forward index removed."
            Expect.equal store._idxR.CountSafe 0 "Reverse index removed."
            let reopened = PersistedConcurrentSortedList<string, fCell2<string>>(1, root, "store", 5000, autoCache = 0)
            reopened.IndexInitialize()
            let found, stored = reopened.TryGetValueNoThreadLock("key", 5000, true)
            Expect.equal (found, stored) (false, None) "Cold reopen cannot recover a deleted key.")
        testCase "default RemoveAsync retains queue completion" (fun _ ->
            let _, store = fixture ()
            store.UpsertAsync("key", value "erase", false, true) |> awaitAll
            let lease = holdQueue store._pstatus.OpQueue
            let pending = Task.WhenAll(store.RemoveAsync("key"))
            try Expect.isFalse pending.IsCompleted "Default remove waits for queued status removal."
            finally lease.Dispose()
            wait pending)
        for removeBuffer in [false; true] do
            testCase (sprintf "duplicate overwrite and cold recovery removeBuffer=%b" removeBuffer) (fun _ ->
                let root, store = fixture ()
                store.UpsertAsync("key", value "first", removeBuffer, true) |> awaitAll
                store.UpsertAsync("key", value "second", removeBuffer, true) |> awaitAll
                Expect.equal store._idx.CountSafe 1 "Duplicate key preserves one forward index."
                Expect.equal store._idxR.CountSafe 1 "Duplicate key preserves one reverse index."
                checkCold root "key" (value "second"))
        testCase "failed index persistence is rejected before acceptance" (fun _ ->
            let root, store = fixture ()
            let keys = Path.Combine(root, "store", "__keys__")
            Expect.equal (Directory.GetFileSystemEntries(keys).Length) 0 "Only a fresh empty synthetic index leaf is replaced."
            Directory.Delete(keys, false)
            File.WriteAllText(keys, "owned failure fixture")
            try
                Expect.throws (fun () -> store.UpsertAsync("key", value "unaccepted", false, true) |> awaitAll) "Index I/O failure must propagate."
                Expect.equal (Directory.GetFiles(Path.Combine(root, "store"), "*.val").Length) 0 "No value exists before index success."
            finally
                File.Delete(keys)
                Directory.CreateDirectory(keys) |> ignore)
        testCase "value persistence failure propagates" (fun _ ->
            let root, store = fixture ()
            store.UpsertAsync("key", value "first", false, true) |> awaitAll
            let path = Directory.GetFiles(Path.Combine(root, "store"), "*.val") |> Array.exactlyOne
            use blockedFile = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.None)
            Expect.throws (fun () -> store.UpsertAsync("key", value "unaccepted", false, true) |> awaitAll) "Locked value file must never return accepted.")
    ]

[<EntryPoint>]
let main _ =
    Console.OutputEncoding <- Text.UTF8Encoding(false)
    runTestsWithCLIArgs [Sequenced] [||] tests
