module PersistedConcurrentSortedList.Tests.ChunkStorageTests

open System
open System.IO
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Text.Json.Nodes
open System.Threading
open System.Threading.Tasks
open Expecto
open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type

let configured fault token = { ChunkedValue.productionSettings with Limit = 4096L; Fault = fault; Cancellation = token }
let smallThreshold action = ChunkedValue.withTestSettings (configured ignore CancellationToken.None) action
let fixture parent =
    let root = Path.Combine(parent, Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory root |> ignore
    File.WriteAllText(Path.Combine(root, "owner.txt"), "PCSL chunk contract synthetic fixture")
    root
let bytes seed length =
    let result = Array.zeroCreate<byte> length
    Random(seed).NextBytes result
    result
let write root (payload: byte array) =
    ChunkedValue.writeCustom root "owned" (fun path -> File.WriteAllBytes(path, payload))
        (Some(fun () -> ChunkedValue.publishIndex root "owned" (Encoding.UTF8.GetBytes "synthetic-key")))
let read root = ChunkedValue.readCommitted root "owned" false (fun _ -> failwith "custom codec was bypassed") (fun path -> Some(File.ReadAllBytes path))
let rewrite root (update: ChunkedValue.Manifest -> ChunkedValue.Manifest) =
    let manifest = ChunkedValue.readManifest root "owned" |> Option.get |> update
    File.WriteAllBytes(ChunkedValue.anchorPath root "owned", Array.append ChunkedValue.magic (JsonSerializer.SerializeToUtf8Bytes(manifest, ChunkedValue.jsonOptions)))
let native root = PCSL2.PersistedConcurrentSortedList<string, fCell2<string>>(1, root, "store", 60000, autoCache = 0)
let awaitAll (tasks: Task array) = Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds 90.).GetAwaiter().GetResult()
let put (store: PCSL2.PersistedConcurrentSortedList<string, fCell2<string>>) text = store.UpsertAsync("key", fCell2.S text, true, true) |> awaitAll

let tests parent =
    testSequenced <| testList "PCSL chunk storage boundaries and safety" [
        for length in [0; 4095; 4096; 4097; 8193] do
            testCase (sprintf "exact physical threshold payload=%d" length) (fun _ -> smallThreshold(fun () ->
                let root = fixture parent
                let payload = bytes 11 length
                let receipt = write root payload
                let expected = if length = 0 then 0 else (length + 4095) / 4096
                Expect.equal receipt.Manifest.Parts.Length expected "Exact multiples must not create an empty trailing part."
                Expect.equal (read root) (Some payload) "Custom bytes are reassembled without truncation."
                for path in Directory.EnumerateFiles(root, "*.val", SearchOption.AllDirectories) do
                    Expect.isLessThanOrEqual (FileInfo(path).Length) 4096L "Anchor and each part obey the exact byte threshold."
                Expect.isFalse receipt.CleanupPending "Successful fresh write has no pending cleanup."))
        testCase "default stream codec preserves legacy compressed bytes" (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let value = fCell2<string>.S(Convert.ToBase64String(bytes 22 12000))
            let original = PB.ModelContainer<fCell2<string>>.serializeF2BArr value
            let receipt = ChunkedValue.writeDefault root "owned" (fun output -> PB.ModelContainer<fCell2<string>>.serializeF(output, value)) None
            use stream = new ChunkedValue.PartReadStream(root, receipt.Manifest, CancellationToken.None)
            use assembled = new MemoryStream()
            stream.CopyTo assembled
            Expect.equal (SHA256.HashData(assembled.ToArray()) |> Convert.ToHexString) (SHA256.HashData(original) |> Convert.ToHexString) "The existing serializer and Deflate wire bytes remain unchanged."
            let actual = ChunkedValue.readCommitted root "owned" true PB.ModelContainer<fCell2<string>>.deserializeF PB.ModelContainer<fCell2<string>>.readFromFile
            Expect.equal actual (Some value) "Streaming codec returns the complete logical value."))
        for name, mutate in ([
            "unknown version", (fun (m: ChunkedValue.Manifest) -> { m with FormatVersion = 2 })
            "wrong owner", (fun (m: ChunkedValue.Manifest) -> { m with KeyHash = "neighbor" })
            "unsafe generation", (fun (m: ChunkedValue.Manifest) -> { m with Generation = "../escape" })
            "unknown codec", (fun (m: ChunkedValue.Manifest) -> { m with Codec = "unknown" })
            "wrong total", (fun (m: ChunkedValue.Manifest) -> { m with TotalLength = m.TotalLength + 1L })
            "wrong whole hash", (fun (m: ChunkedValue.Manifest) -> { m with PayloadSha256 = String('0', 64) })
            "reordered parts", (fun (m: ChunkedValue.Manifest) -> { m with Parts = Array.rev m.Parts })
            "oversized part", (fun (m: ChunkedValue.Manifest) -> { m with Parts = [| { m.Parts[0] with Length = 25_000_001L } |] }) ] : (string * (ChunkedValue.Manifest -> ChunkedValue.Manifest)) list) do
            testCase ("corruption rejects " + name) (fun _ -> smallThreshold(fun () ->
                let root = fixture parent
                write root (bytes 31 9000) |> ignore
                rewrite root mutate
                Expect.throws(fun () -> read root |> ignore) "Corrupted indexed storage must fail, not return None or a partial value."))
        testCase "missing required metadata is rejected even for an empty payload" (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let receipt = write root [||]
            let node = JsonSerializer.SerializeToNode(receipt.Manifest, ChunkedValue.jsonOptions).AsObject()
            node.Remove("totalLength") |> ignore
            File.WriteAllBytes(ChunkedValue.anchorPath root "owned", Array.append ChunkedValue.magic (Encoding.UTF8.GetBytes(node.ToJsonString())))
            Expect.throws(fun () -> read root |> ignore) "Missing zero-valued metadata is not a valid default."))
        testCase "changed part bytes fail SHA verification" (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let receipt = write root (bytes 33 5000)
            let path = ChunkedValue.partPath root receipt.Manifest receipt.Manifest.Parts[0]
            let changed = File.ReadAllBytes path
            changed[0] <- changed[0] ^^^ 1uy
            File.WriteAllBytes(path, changed)
            Expect.throws(fun () -> read root |> ignore) "Length alone cannot prove payload integrity."))
        for point in [ ChunkedValue.BeforePartFlush 0; ChunkedValue.AfterPartFlush 0; ChunkedValue.BeforeAnchorPublish ] do
            testCase (sprintf "precommit failure preserves old value %A" point) (fun _ -> smallThreshold(fun () ->
                let root = fixture parent
                let original = bytes 44 6000
                write root original |> ignore
                let injected stage = if stage = point then raise (IOException "synthetic precommit failure")
                Expect.throws(fun () -> ChunkedValue.withTestSettings (configured injected CancellationToken.None) (fun () -> write root (bytes 45 7000) |> ignore)) "The uncommitted write fails."
                Expect.equal (read root) (Some original) "Previous committed bytes are unchanged."
                ChunkedValue.recoverStore root
                Expect.equal (read root) (Some original) "Reopen preserves the old generation."))
        testCase "failed native update preserves buffered value and index" (fun _ ->
            let root = fixture parent
            let store = native root
            store.UpsertAsync("key", fCell2.S "before", false, true) |> awaitAll
            let fault stage = if stage = ChunkedValue.BeforeAnchorPublish then raise (IOException "synthetic failure")
            Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault CancellationToken.None) (fun () -> store.UpsertAsync("key", fCell2.S "after", false, true) |> awaitAll)) "A precommit exception is not accepted."
            Expect.equal (store.TryGetValueNoThreadLock("key", 60000, true)) (true, Some(fCell2.S "before")) "Memory cannot publish a failed value."
            Expect.equal store._idx.CountSafe 1 "Original index survives.")
        testCase "cancelled chunk write preserves previous value" (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let original = bytes 50 5000
            write root original |> ignore
            use cancelled = new CancellationTokenSource()
            let fault stage = if stage = ChunkedValue.AfterPartFlush 0 then cancelled.Cancel()
            Expect.throws(fun () -> ChunkedValue.withTestSettings (configured fault cancelled.Token) (fun () -> write root (bytes 51 9000) |> ignore)) "Cancellation before publication fails the write."
            Expect.equal (read root) (Some original) "Cancellation cannot expose partial data."))
        testCase "custom hooks execute once and receive compatible complete bytes" (fun _ -> smallThreshold(fun () ->
            let root = fixture parent
            let store = native root
            let mutable writes = 0
            let mutable reads = 0
            store.Write2File <- (fun path value -> writes <- writes + 1; PB.ModelContainer<fCell2<string>>.write2File path value)
            store.ReadFromFile <- (fun path -> reads <- reads + 1; PB.ModelContainer<fCell2<string>>.readFromFile path)
            let expected = Convert.ToBase64String(bytes 61 12000)
            put store expected
            let path = Directory.GetFiles(Path.Combine(root, "store"), "*.val") |> Array.exactlyOne
            Expect.equal (store.ReadFromFile path) (Some(fCell2.S expected)) "Direct public reader understands the manifest."
            Expect.equal writes 1 "Writer hook is not bypassed or retried."
            Expect.equal reads 1 "Reader hook observes one full payload."))
    ]
