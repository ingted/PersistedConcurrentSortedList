module PersistedConcurrentSortedList.Tests.ChunkSafetyTests

open System
open System.IO
open System.Text
open System.Threading
open System.Threading.Tasks
open Expecto
open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type
open ChunkStorageTests

let tests parent = testSequenced <| testList "PCSL publication and ownership safety" [
    for removeBuffer in [false; true] do
        testCase (sprintf "Add Update duplicate and missing preserve logical API buffer=%b" removeBuffer) (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let store = native root
            Expect.equal (store.UpdateAsync("absent", fCell2.S "rejected", removeBuffer, true).Length) 0 "Missing-key Update stays a no-op."
            store.AddAsync("key", fCell2.S "old", removeBuffer, true) |> awaitAll
            Expect.equal (store.AddAsync("key", fCell2.S "duplicate", removeBuffer, true).Length) 0 "Add cannot overwrite a visible key."
            store.UpdateAsync("key", fCell2.S ChunkRecoveryTests.newValue, removeBuffer, true) |> awaitAll
            ChunkRecoveryTests.verifyChild root "new"
            for path in Directory.GetFiles(root, "*.val", SearchOption.AllDirectories) do
                Expect.isLessThanOrEqual (FileInfo(path).Length) 4096L "Add and Update use the same bounded physical storage."))
    testCase "rollback I/O failure retains original cause and backup for reopen" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        let before = bytes 100 5000
        let anchor = ChunkedValue.anchorPath root "owned"
        File.WriteAllBytes(anchor, before)
        let mutable blocked: FileStream option = None
        let fault stage =
            if stage = ChunkedValue.AfterAnchorPublishBeforeIndex then
                blocked <- Some(new FileStream(anchor, FileMode.Open, FileAccess.Read, FileShare.None))
                raise(IOException "original publication failure")
        try
            try
                ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () -> write root (bytes 101 6000) |> ignore)
                failwith "Publication should fail."
            with :? AggregateException as error ->
                Expect.equal error.InnerExceptions.Count 2 "Publication and rollback causes are both retained."
                Expect.equal error.InnerExceptions[0].Message "original publication failure" "Rollback cannot erase the first cause."
        finally blocked |> Option.iter _.Dispose()
        ChunkedValue.recoverStore root
        Expect.equal (File.ReadAllBytes anchor) before "Unpublished backup restores the original legacy orphan."))
    testCase "invalid UTF8 manifest is a typed corruption failure" (fun _ ->
        let root = fixture parent
        File.WriteAllBytes(ChunkedValue.anchorPath root "owned", Array.append ChunkedValue.magic [|0xffuy; 0xfeuy|])
        Expect.throwsT<InvalidDataException>(fun () -> read root |> ignore) "Malformed UTF8 cannot enter the legacy decode path.")
    testCase "new key failing after anchor publication restores legacy orphan" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        let anchor = ChunkedValue.anchorPath root "owned"
        let before = bytes 101 6000
        File.WriteAllBytes(anchor, before)
        let fault stage = if stage = ChunkedValue.AfterAnchorPublishBeforeIndex then raise(IOException "synthetic unpublished failure")
        Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () -> write root (bytes 102 7000) |> ignore)) "Failed index publication cannot replace a legacy orphan."
        Expect.equal (File.ReadAllBytes anchor) before "Old physical anchor remains byte-for-byte unchanged."
        Expect.isFalse (File.Exists(ChunkedValue.indexPath root "owned")) "No new index was published."
        ChunkedValue.recoverStore root
        Expect.equal (File.ReadAllBytes anchor) before "Reopen retains the legacy orphan guard."))
    testCase "cancel at before-anchor callback cannot publish" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        let before = bytes 103 6000
        write root before |> ignore
        use cancellation = new CancellationTokenSource()
        let fault stage = if stage = ChunkedValue.BeforeAnchorPublish then cancellation.Cancel()
        Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault cancellation.Token) (fun () -> write root (bytes 104 7000) |> ignore)) "Cancellation is rechecked at the publication boundary."
        Expect.equal (read root) (Some before) "The previous committed bytes remain visible."))
    for point in [ ChunkedValue.AfterAnchorPublishBeforeIndex; ChunkedValue.AfterIndexPublish ] do
        testCase (sprintf "committed update is not rejected on postcommit fault %A" point) (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            write root (bytes 105 6000) |> ignore
            let after = bytes 106 7000
            let fault stage = if stage = point then raise(IOException "synthetic postcommit cleanup failure")
            let receipt = ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () -> write root after)
            Expect.isTrue receipt.CleanupPending "Postcommit cleanup is explicitly distinguished from failed publication."
            Expect.equal (read root) (Some after) "Accepted data remains complete."
            ChunkedValue.recoverStore root
            Expect.equal (read root) (Some after) "Recovery cannot lose an accepted value."))
    testCase "tombstone survives physical delete failure and reopen retries" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        let receipt = write root (bytes 107 6000)
        let anchor = ChunkedValue.anchorPath root "owned"
        use blocked = new FileStream(anchor, FileMode.Open, FileAccess.Read, FileShare.None)
        Expect.throws(fun () -> ChunkedValue.deleteCommitted root "owned" ignore) "Failure to physically reclaim is reported."
        Expect.isFalse (File.Exists(ChunkedValue.indexPath root "owned")) "Logical visibility has already been removed."
        Expect.isTrue (File.Exists(ChunkedValue.tombstonePath root "owned")) "Cleanup intent survives the I/O failure."
        blocked.Dispose()
        ChunkedValue.recoverStore root
        Expect.equal (Directory.GetFiles(root, "*.val", SearchOption.AllDirectories).Length) 0 "Reopen finishes bounded cleanup."
        write root (bytes 108 5000) |> ignore
        Expect.isFalse (File.Exists(ChunkedValue.tombstonePath root "owned")) "Retry cannot inherit a stale delete marker."))
    testCase "a retry finishes pending deletion before publishing a replacement" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        write root (bytes 109 5000) |> ignore
        let fault stage = if stage = ChunkedValue.AfterIndexUnpublish then raise(IOException "synthetic interrupted delete")
        Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () -> ChunkedValue.deleteCommitted root "owned" ignore)) "Delete was interrupted after visibility ended."
        let after = bytes 110 7000
        write root after |> ignore
        ChunkedValue.recoverStore root
        Expect.equal (read root) (Some after) "A prior tombstone cannot delete the new committed value."))
    testCase "cleanup retains unknown folders and legacy orphan" (fun _ -> smallThreshold(fun () ->
        let root = fixture parent
        write root (bytes 111 5000) |> ignore
        let unknown = Path.Combine(root, "__values__", "owned", "human-snapshot")
        Directory.CreateDirectory unknown |> ignore
        let retained = Path.Combine(unknown, "original.val")
        let before = bytes 112 99
        File.WriteAllBytes(retained, before)
        let orphan = ChunkedValue.anchorPath root "legacy-orphan"
        File.WriteAllBytes(orphan, before)
        write root (bytes 113 5000) |> ignore
        ChunkedValue.recoverStore root
        Expect.equal (File.ReadAllBytes retained) before "An unrecognized directory is never recursively reclaimed."
        Expect.equal (File.ReadAllBytes orphan) before "Unknown legacy orphan stays intact."))
    testCase "reparse owner is rejected without touching the linked resource" (fun _ ->
        let root = fixture parent
        let neighbor = fixture parent
        let sentinel = Path.Combine(neighbor, "sentinel.txt")
        File.WriteAllText(sentinel, "retain")
        Directory.CreateSymbolicLink(Path.Combine(root, "__values__"), neighbor) |> ignore
        Expect.throws(fun () -> smallThreshold(fun () -> write root (bytes 114 5000) |> ignore)) "Owned generation paths reject reparse links."
        Expect.equal (File.ReadAllText sentinel) "retain" "The linked resource is unchanged."
        Expect.equal (Directory.GetFileSystemEntries(neighbor).Length) 2 "Only its original owner and sentinel exist.")
    for name, content in [
        "non-object", "[]"
        "part count bound", "{\"parts\":[" + String.concat "," (List.replicate (ChunkedValue.MaximumParts + 1) "{}") + "]}"
        "malformed JSON", "{bad-json}"
        "duplicate fields", "{\"parts\":[],\"parts\":[]}"
        "oversized manifest", String(' ', int ChunkedValue.MaximumManifestLength) ] do
        testCase ("bounded manifest rejection " + name) (fun _ ->
            let root = fixture parent
            File.WriteAllBytes(ChunkedValue.anchorPath root "owned", Array.append ChunkedValue.magic (Encoding.UTF8.GetBytes content))
            Expect.throwsT<InvalidDataException>(fun () -> read root |> ignore) "Malformed and oversized metadata fails with the storage corruption contract.")
    testCase "public key-hash helper retains its explicit nontransactional behavior" (fun _ ->
        let root = fixture parent
        let store = native root
        let hash = store.GetOrNewAndPersistKeyHash("helper", true)
        Expect.isFalse (String.IsNullOrEmpty hash) "Existing public helper still returns a hash."
        Expect.isTrue (File.Exists(ChunkedValue.indexPath (Path.Combine(root, "store")) hash)) "Helper persists index as before."
        Expect.equal (Directory.GetFiles(root, "*.val", SearchOption.AllDirectories).Length) 0 "This low-level helper is not a durable value write.")
    testCase "failed custom writer never publishes value index or buffer" (fun _ ->
        let root = fixture parent
        let store = native root
        let mutable calls = 0
        store.Write2File <- fun _ _ -> calls <- calls + 1; raise(IOException "synthetic hook failure")
        Expect.throws(fun () -> put store "rejected") "Hook failure propagates before index publication."
        Expect.equal calls 1 "A failed hook is not retried."
        Expect.equal store._base.CountSafe 0 "No failed logical value is buffered."
        Expect.equal store._idx.CountSafe 0 "No failed key is published."
        Expect.equal (Directory.GetFiles(root, "*.val", SearchOption.AllDirectories).Length) 0 "Incomplete generation is reclaimed.")
]
