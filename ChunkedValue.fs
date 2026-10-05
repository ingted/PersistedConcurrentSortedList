namespace PersistedConcurrentSortedList

open System
open System.Collections.Concurrent
open System.ComponentModel
open System.IO
open System.Security.Cryptography
open System.Text
open System.Text.Json
open System.Threading

/// Physical value storage. The index remains the logical visibility boundary.
module ChunkedValue =
    [<Literal>]
    let DefaultLimit = 25_000_000L
    [<Literal>]
    let MaximumManifestLength = 1_000_000L
    [<Literal>]
    let MaximumParts = 16_384
    let magic = Encoding.ASCII.GetBytes "PCSLCH01"
    let utf8 = UTF8Encoding(false, true)
    let jsonOptions = JsonSerializerOptions(JsonSerializerDefaults.Web)

    type CommitStage =
        | BeforePartFlush of int
        | AfterPartFlush of int
        | BeforeAnchorPublish
        | AfterAnchorPublishBeforeIndex
        | AfterIndexPublish
        | AfterIndexUnpublish
        | BeforeReadDecode

    [<EditorBrowsable(EditorBrowsableState.Never)>]
    type TestSettings =
        { Limit: int64
          Cancellation: CancellationToken
          Fault: CommitStage -> unit }

    let settingsContext = AsyncLocal<TestSettings option>()
    let productionSettings = { Limit = DefaultLimit; Cancellation = CancellationToken.None; Fault = ignore }
    let settings () = defaultArg settingsContext.Value productionSettings

    /// Scoped test seam, never a caller-facing per-write storage configuration.
    [<EditorBrowsable(EditorBrowsableState.Never)>]
    let withTestSettings configured action =
        if configured.Limit < 1024L || configured.Limit > DefaultLimit then
            invalidArg "configured" "A test threshold must be between 1024 and 25000000 bytes."
        let previous = settingsContext.Value
        settingsContext.Value <- Some configured
        try action () finally settingsContext.Value <- previous

    [<CLIMutable>]
    type Part = { Ordinal: int; Length: int64; Sha256: string }
    [<CLIMutable>]
    type Manifest =
        { FormatVersion: int
          KeyHash: string
          Generation: string
          Codec: string
          TotalLength: int64
          PayloadSha256: string
          Parts: Part array }

    type WriteReceipt = { Manifest: Manifest; CleanupPending: bool }
    let keyGates = ConcurrentDictionary<string, obj>(StringComparer.OrdinalIgnoreCase)
    let invalid reason = raise (InvalidDataException("Invalid PCSL chunk value: " + reason))

    let validateName (value: string) =
        if String.IsNullOrWhiteSpace value || value = "." || value = ".."
           || Path.IsPathRooted value || value.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0
           || value.Contains('/') || value.Contains('\\') then
            invalid "unsafe owner name"
        value

    let normalizedRoot root = Path.GetFullPath root
    let ownedPath root (segments: string list) =
        let absoluteRoot = normalizedRoot root
        let path = segments |> List.fold (fun current name -> Path.Combine(current, validateName name)) absoluteRoot |> Path.GetFullPath
        let prefix = absoluteRoot.TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + string Path.DirectorySeparatorChar
        if not (path.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) then invalid "path escapes owner"
        path

    let rejectReparse path =
        let mutable current = Path.GetFullPath path
        while not (String.IsNullOrEmpty current) do
            if (File.Exists current || Directory.Exists current)
               && (File.GetAttributes(current) &&& FileAttributes.ReparsePoint) <> enum 0 then
                invalid "reparse path"
            current <- Path.GetDirectoryName current

    let ensureDirectory path =
        rejectReparse path
        Directory.CreateDirectory path |> ignore
        rejectReparse path

    let withKeyLock root keyHash action =
        let path = ownedPath root [ validateName keyHash + ".val" ]
        lock (keyGates.GetOrAdd(path, fun _ -> obj())) action

    let anchorPath root hash = ownedPath root [ validateName hash + ".val" ]
    let indexPath root hash = ownedPath root [ "__keys__"; validateName hash + ".index" ]
    let tombstonePath root hash = ownedPath root [ "__keys__"; validateName hash + ".index.tombstone" ]
    let generationPath root area hash generation = ownedPath root [ area; validateName hash; validateName generation ]
    let partPath root manifest part =
        ownedPath root [ "__values__"; manifest.KeyHash; manifest.Generation; part.Ordinal.ToString("D6") + ".val" ]

    let deleteOwnedDirectory root target =
        let normalized = Path.GetFullPath target
        let prefix = (normalizedRoot root).TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar) + string Path.DirectorySeparatorChar
        if not (normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase)) then invalid "cleanup escapes owner"
        rejectReparse normalized
        if Directory.Exists normalized then
            // Never follow an unexpected link nested inside an owned generation.
            for item in Directory.EnumerateFileSystemEntries(normalized, "*", SearchOption.TopDirectoryOnly) do
                rejectReparse item
                if Directory.Exists item then invalid "unexpected nested generation directory"
            Directory.Delete(normalized, true)

    let atomicWrite path (bytes: byte array) =
        rejectReparse path
        let temporary = path + "." + Guid.NewGuid().ToString("N") + ".tmp"
        try
            use output = new FileStream(temporary, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536, FileOptions.WriteThrough)
            output.Write(bytes, 0, bytes.Length)
            output.Flush true
            output.Dispose()
            File.Move(temporary, path, true)
        finally
            if File.Exists temporary then File.Delete temporary

    let publishIndex root hash (keyBytes: byte array) =
        let path = indexPath root hash
        ensureDirectory (Path.GetDirectoryName path)
        atomicWrite path keyBytes

    let isDigest (value: string) =
        not (isNull value) && value.Length = 64 && (value |> Seq.forall Uri.IsHexDigit)

    let validateManifest hash (manifest: Manifest) =
        if manifest.FormatVersion <> 1 then invalid "unsupported format version"
        if manifest.KeyHash <> hash then invalid "wrong key owner"
        validateName manifest.KeyHash |> ignore
        match Guid.TryParseExact(manifest.Generation, "N") with
        | true, generation when generation <> Guid.Empty && generation.ToString("N") = manifest.Generation -> ()
        | _ -> invalid "invalid generation"
        if manifest.Codec <> "deflate-pb-v1" && manifest.Codec <> "custom-file-hook-v1" then invalid "unknown codec"
        if manifest.TotalLength < 0L || not (isDigest manifest.PayloadSha256) then invalid "invalid payload metadata"
        if isNull manifest.Parts || manifest.Parts.Length > MaximumParts then invalid "too many parts"
        let mutable total = 0L
        for ordinal, part in manifest.Parts |> Array.indexed do
            if part.Ordinal <> ordinal || part.Length <= 0L || part.Length > DefaultLimit || not (isDigest part.Sha256) then
                invalid "invalid ordered part"
            if Int64.MaxValue - total < part.Length then invalid "payload length overflow"
            total <- total + part.Length
        if total <> manifest.TotalLength then invalid "total length mismatch"
        manifest

    let readManifest root hash =
        let path = anchorPath root hash
        rejectReparse path
        if not (File.Exists path) then None
        else
            use input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
            let header = Array.zeroCreate<byte> magic.Length
            let count = input.Read(header, 0, header.Length)
            if count <> magic.Length || header <> magic then None
            else
                if input.Length > min MaximumManifestLength DefaultLimit then invalid "manifest exceeds bound"
                let length = int (input.Length - int64 magic.Length)
                let payload = Array.zeroCreate<byte> length
                input.ReadExactly(payload)
                try
                    use document = JsonDocument.Parse(ReadOnlyMemory payload, JsonDocumentOptions(MaxDepth = 8))
                    let parts = document.RootElement.GetProperty("parts")
                    if parts.ValueKind <> JsonValueKind.Array || parts.GetArrayLength() > MaximumParts then invalid "part count exceeds bound"
                    let names = document.RootElement.EnumerateObject() |> Seq.map _.Name |> Seq.toArray
                    let required = [| "formatVersion"; "keyHash"; "generation"; "codec"; "totalLength"; "payloadSha256"; "parts" |]
                    if Array.sort names <> Array.sort required then invalid "missing, duplicate or unknown manifest field"
                    for part in parts.EnumerateArray() do
                        let fields = part.EnumerateObject() |> Seq.map _.Name |> Seq.toArray |> Array.sort
                        if fields <> [| "length"; "ordinal"; "sha256" |] then invalid "missing, duplicate or unknown part field"
                    JsonSerializer.Deserialize<Manifest>(payload, jsonOptions) |> validateManifest hash |> Some
                with
                | :? JsonException -> invalid "malformed manifest"
                | :? Collections.Generic.KeyNotFoundException -> invalid "missing manifest field"
                | :? InvalidOperationException -> invalid "invalid manifest field shape"

    type ChunkSink(directory: string, limit: int64, configured: TestSettings) =
        inherit Stream()
        let parts = ResizeArray<Part>()
        let totalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
        let partHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
        let mutable output: FileStream option = None
        let mutable partLength = 0L
        let mutable totalLength = 0L
        let mutable finished = false

        let closePart () =
            match output with
            | None -> ()
            | Some file ->
                configured.Cancellation.ThrowIfCancellationRequested()
                configured.Fault(BeforePartFlush parts.Count)
                file.Flush true
                file.Dispose()
                output <- None
                parts.Add { Ordinal = parts.Count; Length = partLength; Sha256 = Convert.ToHexString(partHash.GetHashAndReset()) }
                partLength <- 0L
                configured.Fault(AfterPartFlush(parts.Count - 1))

        override _.CanRead = false
        override _.CanSeek = false
        override _.CanWrite = not finished
        override _.Length = totalLength
        override _.Position with get () = totalLength and set _ = raise (NotSupportedException())
        override _.Read(_, _, _) = raise (NotSupportedException())
        override _.Seek(_, _) = raise (NotSupportedException())
        override _.SetLength _ = raise (NotSupportedException())
        override _.Flush() = output |> Option.iter (fun file -> file.Flush())
        override _.Write(buffer, offset, count) =
            if finished then raise (ObjectDisposedException(nameof ChunkSink))
            if isNull buffer then nullArg "buffer"
            if offset < 0 || count < 0 || offset > buffer.Length - count then invalidArg "count" "Invalid write range."
            let mutable consumed = 0
            while consumed < count do
                configured.Cancellation.ThrowIfCancellationRequested()
                if output.IsNone then
                    if parts.Count >= MaximumParts then invalid "too many output parts"
                    let path = Path.Combine(directory, parts.Count.ToString("D6") + ".val")
                    output <- Some(new FileStream(path, FileMode.CreateNew, FileAccess.Write, FileShare.None, 65536, FileOptions.WriteThrough))
                let length = min (count - consumed) (int (min (limit - partLength) (int64 Int32.MaxValue)))
                output.Value.Write(buffer, offset + consumed, length)
                partHash.AppendData(buffer, offset + consumed, length)
                totalHash.AppendData(buffer, offset + consumed, length)
                partLength <- partLength + int64 length
                totalLength <- Checked.(+) totalLength (int64 length)
                consumed <- consumed + length
                if partLength = limit then closePart ()

        member _.Finish() =
            closePart ()
            finished <- true
            totalLength, Convert.ToHexString(totalHash.GetHashAndReset()), parts.ToArray()

        override _.Dispose(disposing) =
            if disposing then
                output |> Option.iter (fun file -> file.Dispose())
                output <- None
                totalHash.Dispose()
                partHash.Dispose()
                finished <- true
            base.Dispose disposing

    type PartReadStream(root: string, manifest: Manifest, cancellation: CancellationToken) =
        inherit Stream()
        let mutable ordinal = 0
        let mutable input: FileStream option = None
        let mutable position = 0L
        override _.CanRead = true
        override _.CanSeek = false
        override _.CanWrite = false
        override _.Length = manifest.TotalLength
        override _.Position with get () = position and set _ = raise (NotSupportedException())
        override _.Flush() = ()
        override _.Seek(_, _) = raise (NotSupportedException())
        override _.SetLength _ = raise (NotSupportedException())
        override _.Write(_, _, _) = raise (NotSupportedException())
        override _.Read(buffer, offset, count) =
            cancellation.ThrowIfCancellationRequested()
            if isNull buffer then nullArg "buffer"
            if offset < 0 || count < 0 || offset > buffer.Length - count then invalidArg "count" "Invalid read range."
            let mutable received = 0
            while count > 0 && received = 0 && ordinal < manifest.Parts.Length do
                if input.IsNone then
                    let path = partPath root manifest manifest.Parts[ordinal]
                    rejectReparse path
                    input <- Some(new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read))
                received <- input.Value.Read(buffer, offset, count)
                if received = 0 then
                    input.Value.Dispose()
                    input <- None
                    ordinal <- ordinal + 1
            position <- position + int64 received
            received
        override _.Dispose(disposing) =
            if disposing then input |> Option.iter (fun file -> file.Dispose())
            base.Dispose disposing

    let verifyPayload root (manifest: Manifest) =
        use totalHash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
        let buffer = Array.zeroCreate<byte> 65536
        for part in manifest.Parts do
            (settings ()).Cancellation.ThrowIfCancellationRequested()
            let path = partPath root manifest part
            rejectReparse path
            use input = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read)
            if input.Length <> part.Length then invalid "part length mismatch"
            use hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256)
            let mutable count = input.Read(buffer, 0, buffer.Length)
            while count > 0 do
                (settings ()).Cancellation.ThrowIfCancellationRequested()
                hash.AppendData(buffer, 0, count)
                totalHash.AppendData(buffer, 0, count)
                count <- input.Read(buffer, 0, buffer.Length)
            if not (String.Equals(Convert.ToHexString(hash.GetHashAndReset()), part.Sha256, StringComparison.OrdinalIgnoreCase)) then invalid "part hash mismatch"
        if not (String.Equals(Convert.ToHexString(totalHash.GetHashAndReset()), manifest.PayloadSha256, StringComparison.OrdinalIgnoreCase)) then invalid "payload hash mismatch"

    let knownGeneration (value: string) =
        match Guid.TryParseExact(value, "N") with
        | true, generation -> generation <> Guid.Empty && generation.ToString("N") = value
        | _ -> false

    let cleanupGenerations root hash keep =
        for area in [ "__values__"; "__staging__" ] do
            let directory = ownedPath root [ area; hash ]
            rejectReparse directory
            if Directory.Exists directory then
                let generations = Directory.GetDirectories directory
                if generations.Length > MaximumParts then invalid "cleanup inventory exceeds bound"
                for generation in generations do
                    let name = Path.GetFileName generation
                    if knownGeneration name && (area = "__staging__" || Some name <> keep) then
                        deleteOwnedDirectory directory generation

    let inspectCommitted root hash =
        withKeyLock root hash (fun () ->
            match readManifest root hash with
            | Some manifest -> verifyPayload root manifest; Some manifest
            | None -> None)

    let writeCommitted root hash codec (writePayload: string -> Stream -> unit) (publish: (unit -> unit) option) =
        withKeyLock root hash (fun () ->
            validateName hash |> ignore
            let configured = settings ()
            configured.Cancellation.ThrowIfCancellationRequested()
            ensureDirectory root
            let tombstone = tombstonePath root hash
            rejectReparse tombstone
            if File.Exists tombstone then
                let oldAnchor = anchorPath root hash
                rejectReparse oldAnchor
                if File.Exists oldAnchor then File.Delete oldAnchor
                cleanupGenerations root hash None
                File.Delete tombstone
            let isNew = not (File.Exists(indexPath root hash))
            if isNew && publish.IsSome then ensureDirectory (Path.GetDirectoryName(indexPath root hash))
            let generation = Guid.NewGuid().ToString("N")
            let staging = generationPath root "__staging__" hash generation
            let final = generationPath root "__values__" hash generation
            ensureDirectory staging
            let mutable anchorPublished = false
            let mutable visible = not isNew || publish.IsNone
            let mutable committedManifest: Manifest option = None
            let previousAnchor = Path.Combine(staging, "previous.anchor.tmp")
            let hadPreviousAnchor = isNew && publish.IsSome && File.Exists(anchorPath root hash)
            try
                // A failed new-key publication must also retain an existing legacy orphan.
                if hadPreviousAnchor then
                    rejectReparse (anchorPath root hash)
                    File.Copy(anchorPath root hash, previousAnchor)
                use sink = new ChunkSink(staging, configured.Limit, configured)
                writePayload staging (sink :> Stream)
                let total, payloadHash, parts = sink.Finish()
                let manifest =
                    { FormatVersion = 1; KeyHash = hash; Generation = generation; Codec = codec
                      TotalLength = total; PayloadSha256 = payloadHash; Parts = parts } |> validateManifest hash
                let anchorBytes = Array.append magic (JsonSerializer.SerializeToUtf8Bytes(manifest, jsonOptions))
                if int64 anchorBytes.Length > min MaximumManifestLength configured.Limit then invalid "manifest exceeds write threshold"
                configured.Cancellation.ThrowIfCancellationRequested()
                ensureDirectory (Path.GetDirectoryName final)
                Directory.Move(staging, final)
                verifyPayload root manifest
                configured.Fault BeforeAnchorPublish
                configured.Cancellation.ThrowIfCancellationRequested()
                atomicWrite (anchorPath root hash) anchorBytes
                anchorPublished <- true
                committedManifest <- Some manifest
                configured.Fault AfterAnchorPublishBeforeIndex
                if isNew then publish |> Option.iter (fun action -> action ())
                visible <- true
                configured.Fault AfterIndexPublish
                let pending =
                    try
                        let backup = Path.Combine(final, "previous.anchor.tmp")
                        if File.Exists backup then File.Delete backup
                        cleanupGenerations root hash (Some generation)
                        false
                    with :? IOException -> true
                { Manifest = manifest; CleanupPending = pending }
            with error ->
                if anchorPublished && visible then
                    // Publication already succeeded. A cleanup fault is not a retryable value-write failure.
                    { Manifest = committedManifest.Value; CleanupPending = true }
                else
                    try
                        if anchorPublished then
                            let backup = Path.Combine(final, "previous.anchor.tmp")
                            if hadPreviousAnchor && File.Exists backup then
                                File.Move(backup, anchorPath root hash, true)
                            else File.Delete(anchorPath root hash)
                    with rollbackError ->
                        // Keep the generation/backup for recovery and preserve both failure causes.
                        raise(AggregateException("PCSL publication and rollback failed; unpublished recovery data retained.", error, rollbackError))
                    for path in [ staging; final ] do
                        try deleteOwnedDirectory root path with :? IOException -> ()
                    raise error)

    let writeDefault root hash (serialize: Stream -> unit) publish =
        writeCommitted root hash "deflate-pb-v1"
            (fun _ output ->
                use compressed = new Compression.DeflateStream(output, Compression.CompressionMode.Compress, true)
                serialize compressed
                // Dispose writes the Deflate trailer before sink.Finish computes the manifest.
                compressed.Dispose()) publish

    let writeCustom root hash (writeHook: string -> unit) publish =
        writeCommitted root hash "custom-file-hook-v1"
            (fun staging output ->
                let spool = Path.Combine(staging, "hook.spool.tmp")
                writeHook spool
                rejectReparse spool
                use input = new FileStream(spool, FileMode.Open, FileAccess.Read, FileShare.Read)
                input.CopyTo output
                input.Dispose()
                File.Delete spool) publish

    let readCommitted root hash preferDefault decode readHook =
        withKeyLock root hash (fun () ->
            let anchor = anchorPath root hash
            if not (File.Exists anchor) then None
            else
                match readManifest root hash with
                | None -> readHook anchor
                | Some manifest ->
                    verifyPayload root manifest
                    let configured = settings ()
                    configured.Fault BeforeReadDecode
                    configured.Cancellation.ThrowIfCancellationRequested()
                    use input = new PartReadStream(root, manifest, configured.Cancellation)
                    let result =
                        if preferDefault && manifest.Codec = "deflate-pb-v1" then
                            use compressed = new Compression.DeflateStream(input, Compression.CompressionMode.Decompress, true)
                            Some(decode compressed)
                        else
                            let staging = generationPath root "__staging__" hash (Guid.NewGuid().ToString("N"))
                            ensureDirectory staging
                            let spool = Path.Combine(staging, "read.spool.tmp")
                            try
                                use output = new FileStream(spool, FileMode.CreateNew, FileAccess.Write, FileShare.None)
                                input.CopyTo output
                                output.Flush true
                                output.Dispose()
                                readHook spool
                            finally deleteOwnedDirectory root staging
                    configured.Cancellation.ThrowIfCancellationRequested()
                    result)

    let isUnpublishedNewAnchor root hash =
        withKeyLock root hash (fun () ->
            not (File.Exists(indexPath root hash)) && (readManifest root hash).IsSome)

    let deleteCommitted root hash unpublishMemory =
        withKeyLock root hash (fun () ->
            let index = indexPath root hash
            let tombstone = tombstonePath root hash
            rejectReparse index
            rejectReparse tombstone
            if File.Exists index then File.Move(index, tombstone, true)
            unpublishMemory ()
            (settings ()).Fault AfterIndexUnpublish
            let anchor = anchorPath root hash
            rejectReparse anchor
            if File.Exists anchor then File.Delete anchor
            cleanupGenerations root hash None
            if File.Exists tombstone then File.Delete tombstone)

    let recoverStore root =
        let keys = ownedPath root [ "__keys__" ]
        ensureDirectory keys
        for tombstone in Directory.EnumerateFiles(keys, "*.index.tombstone", SearchOption.TopDirectoryOnly) do
            let name = Path.GetFileName tombstone
            let hash = name.Substring(0, name.Length - ".index.tombstone".Length) |> validateName
            deleteCommitted root hash ignore
        for index in Directory.EnumerateFiles(keys, "*.index", SearchOption.TopDirectoryOnly) do
            let hash = Path.GetFileNameWithoutExtension index |> validateName
            withKeyLock root hash (fun () ->
                let anchor = anchorPath root hash
                if not (File.Exists anchor) then invalid "visible index has no value"
                match readManifest root hash with
                | None -> cleanupGenerations root hash None
                | Some manifest ->
                    verifyPayload root manifest
                    cleanupGenerations root hash (Some manifest.Generation))
        // Only recognized owned generations are reclaimed; legacy and unknown orphan anchors are retained.
        for area in [ "__values__"; "__staging__" ] do
            let directory = ownedPath root [ area ]
            rejectReparse directory
            if Directory.Exists directory then
                for key in Directory.EnumerateDirectories directory do
                    let hash = Path.GetFileName key |> validateName
                    withKeyLock root hash (fun () ->
                        if not (File.Exists(indexPath root hash)) then
                            match readManifest root hash with
                            | Some manifest ->
                                let backup = Path.Combine(generationPath root "__values__" hash manifest.Generation, "previous.anchor.tmp")
                                rejectReparse backup
                                if File.Exists backup then File.Move(backup, anchorPath root hash, true)
                                else File.Delete(anchorPath root hash)
                                cleanupGenerations root hash None
                            | None when not (File.Exists(anchorPath root hash)) -> cleanupGenerations root hash None
                            | _ -> cleanupGenerations root hash None)
