// Real native filesystem contract, synthetic isolated data only. Current 10.1.401
// is expected RED on auto-chunk. No service, SQL, network login, or package push.
// FSI: use the candidate directory below and edit defaultArgumentsText.
// Compiled TEST.fsproj accepts PcslAssemblyPath for another owner's candidate.
#if INTERACTIVE
#r "nuget: Expecto, 11.1.0"
#r "nuget: FAkka.Argu, 10.1.400"
#r "nuget: FAkka.FCell2, 10.1.400"
#r "nuget: FAkka.ProtoBuf.FSharp, 10.1.400"
#r "nuget: FAkka.FSharp.Collections.ParallelSeq, 10.1.400"
#r "nuget: FAkka.FsPickler, 10.1.400"
#r "nuget: FAkka.FsPickler.Json, 10.1.400"
#I "../../../bin/net10.0/agent.aster/chunk-rfc"
#r "PersistedConcurrentSortedList.dll"
#load "ParseLine.fsx"
#endif

open System
open System.IO
open System.Text
open System.Security.Cryptography
open System.Threading.Tasks
open Argu
open Expecto
open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type
open PersistedConcurrentSortedList.PCSL2

type Arguments =
    | Fixture_Root of string
    interface IArgParserTemplate with
        member _.Usage = "Synthetic fixture parent under this native repo temp/. Each run creates a new GUID; failures retained."

let repoRoot = Path.GetFullPath(Path.Combine(__SOURCE_DIRECTORY__, "../../.."))
let defaultArgumentsText = sprintf "--fixture-root \"%s\"" (Path.Combine(repoRoot, "temp/agent.aster/chunk25").Replace('\\','/'))
let parser = ArgumentParser.Create<Arguments>(programName="TEST.fsx")
let defaults = parser.ParseCommandLine(PL.parseLine [|' '|] (Some '"') None true defaultArgumentsText)
let limit = 25_000_000L
let timeout = 60_000
let utf8 = UTF8Encoding(false,true)
let value text = fCell2<string>.S text
let digest (text: string) = SHA256.HashData(utf8.GetBytes text) |> Convert.ToHexString
let waitAll (tasks: Task array) = Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds 90.).GetAwaiter().GetResult()
let store root = PersistedConcurrentSortedList<string,fCell2<string>>(1,root,"store",timeout,autoCache=0)
let put (db: PersistedConcurrentSortedList<string,fCell2<string>>) key text removeBuffer =
    db.UpsertAsync(key,value text,removeBuffer,true) |> waitAll
let coldRead root key =
    let db=store root
    db.IndexInitialize()
    db.TryGetValueNoThreadLock(key,timeout,true)
let equalCold root key expected =
    match coldRead root key with
    | true,Some (fCell2.S actual) -> Expect.equal (digest actual) (digest expected) "Cold decoded payload must be byte-exact, not truncated."
    | _ -> failtest "Cold key/value is missing or has the wrong DU case."
let values root = Directory.GetFiles(Path.Combine(root,"store"),"*.val",SearchOption.AllDirectories) |> Array.map FileInfo
let assertBounded root =
    let files=values root
    Expect.isGreaterThan files.Length 0 "A successful durable write must leave physical value files."
    for file in files do Expect.isLessThanOrEqual file.Length limit "Every committed physical .val, including the anchor, must respect decimal 25MB."
let entropy seed length =
    let bytes=Array.zeroCreate<byte> length
    Random(seed).NextBytes bytes
    Convert.ToBase64String bytes
// Base64 of seeded random bytes stays high entropy under the existing Deflate codec.
// Repeated 'x' would falsely pass by compressing below the threshold.
let large = lazy (entropy 2501 28_000_000)
let huge = lazy (entropy 2502 58_000_000)
let different = lazy (entropy 2503 28_000_000)

let tests fixtureParent =
    let fixture () =
        let root=Path.Combine(fixtureParent,Guid.NewGuid().ToString("N"))
        Directory.CreateDirectory(root) |> ignore
        File.WriteAllText(Path.Combine(root,"owner.txt"),"C25 synthetic fixture; failed evidence retained.",utf8)
        root,store root
    let checkLarge text removeBuffer =
        let root,db=fixture()
        put db "one" text removeBuffer
        equalCold root "one" text
        assertBounded root
    testSequenced <| testList "PCSL decimal25MB physical contract" [
        testCase "small UTF8 cold roundtrip" (fun _ -> checkLarge "繁體中文🙂\u0000payload" false)
        testCase "empty string remains a value" (fun _ -> checkLarge "" true)
        testCase "single compressed value above25MB auto chunks" (fun _ -> checkLarge large.Value false)
        testCase "single compressed value above50MB crosses multiple chunks" (fun _ -> checkLarge huge.Value false)
        testCase "nonbuffered large value follows the same contract" (fun _ -> checkLarge large.Value true)
        testCase "update small to large survives reopen" (fun _ ->
            let root,db=fixture()
            put db "one" "before" false
            put db "one" large.Value false
            equalCold root "one" large.Value
            assertBounded root)
        testCase "update large to small reclaims retired physical values" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value false
            put db "one" "after" true
            equalCold root "one" "after"
            assertBounded root
            Expect.isLessThan (values root |> Array.sumBy _.Length) 1_000_000L "Completed shrink must not retain megabytes of unreachable generations.")
        testCase "update large to different large is whole new value" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value true
            put db "one" different.Value true
            equalCold root "one" different.Value
            assertBounded root)
        testCase "remove large deletes index and every owned value part" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value true
            db.RemoveAsync("one",true) |> waitAll
            Expect.equal (coldRead root "one") (false,None) "Delete remains absent after reopen."
            Expect.equal (values root).Length 0 "Delete must not orphan parts.")
        testCase "remove does not delete neighboring key" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value true
            put db "neighbor" "keep" true
            db.RemoveAsync("one",true) |> waitAll
            equalCold root "neighbor" "keep"
            Expect.isLessThan (values root |> Array.sumBy _.Length) 1_000_000L "Only the small surviving key remains.")
        testCase "legacy large blob reads without rewrite then migrates on upsert" (fun _ ->
            let root,db=fixture()
            put db "one" "seed-index" true
            let index=Directory.GetFiles(Path.Combine(root,"store","__keys__"),"*.index") |> Array.exactlyOne
            let anchor=Path.Combine(root,"store",Path.GetFileNameWithoutExtension(index)+".val")
            // Independent old wire oracle, bypassing any future chunk write helper.
            let legacy=PB.ModelContainer<fCell2<string>>.serializeF2BArr (value large.Value)
            Expect.isGreaterThan (int64 legacy.Length) limit "The fixture must genuinely exceed the physical threshold after compression."
            File.WriteAllBytes(anchor,legacy)
            let before=SHA256.HashData legacy |> Convert.ToHexString
            equalCold root "one" large.Value
            Expect.equal (SHA256.HashData(File.ReadAllBytes anchor)|>Convert.ToHexString) before "Read-only legacy access must not migrate."
            let reopened=store root
            reopened.IndexInitialize()
            put reopened "one" different.Value true
            equalCold root "one" different.Value
            assertBounded root)
        testCase "missing committed value part fails closed" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value true
            let victim=values root |> Array.maxBy _.Length
            File.Delete victim.FullName
            Expect.throws (fun () -> coldRead root "one" |> ignore) "Visible index with missing content must be an error, not None.")
        testCase "truncated committed value part fails closed" (fun _ ->
            let root,db=fixture()
            put db "one" large.Value true
            let victim=values root |> Array.maxBy _.Length
            use file=new FileStream(victim.FullName,FileMode.Open,FileAccess.Write,FileShare.None)
            file.SetLength(0L)
            file.Dispose()
            Expect.throws (fun () -> coldRead root "one" |> ignore) "A truncated part must not become a successful empty value.")
    ]

let main argv =
    let overrides=parser.ParseCommandLine argv
    let parent=overrides.TryGetResult Fixture_Root |> Option.orElse (defaults.TryGetResult Fixture_Root) |> Option.get |> Path.GetFullPath
    let allowed=Path.Combine(repoRoot,"temp")+string Path.DirectorySeparatorChar
    if not(parent.StartsWith(allowed,StringComparison.OrdinalIgnoreCase)) then invalidArg "fixture-root" "Fixtures must remain below native repo temp/."
    let runRoot=Path.Combine(parent,Guid.NewGuid().ToString("N"))
    Directory.CreateDirectory runRoot |> ignore
    printfn "C25 native candidate=%s physicalLimit=%d fixture=%s expectedTests=13" typeof<PersistedConcurrentSortedList<string,fCell2<string>>>.Assembly.Location limit runRoot
    runTestsWithCLIArgs [] [||] (tests runRoot)

#if INTERACTIVE
fsi.CommandLineArgs |> Array.skip 1 |> Array.filter ((<>) "--") |> main |> fun code -> Environment.ExitCode <- code
#else
[<EntryPoint>]
let entry argv = main argv
#endif
