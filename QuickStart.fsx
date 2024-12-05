#if INTERACTIVE
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r @"nuget: protobuf-net"
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r @"nuget: FAkka.FsPickler, 9.0.4"
#r @"nuget: FAkka.FsPickler.Json, 9.0.4"
#r @"nuget: FAkka.ProtoBuf.FSharp, 1.0.0"
#r @"nuget: FSharp.Collections.ParallelSeq, 1.2.0"
#r @"bin\net9.0\PersistedConcurrentSortedList.dll"
#else
namespace PersistedConcurrentSortedList
#endif

open PersistedConcurrentSortedList
open System.Collections.Generic

module CSLTest =
    open DefaultHelper
    open CSL
    open PCSL
    let csl () = PCSL<string, fstring, SLTyp>(TSL, PCSLFunHelper<string, fstring>.oFun, PCSLFunHelper<string, fstring>.eFun, autoCache = 1)

    //let (Some lock) = csl.RequireLock(None, None) |> Async.RunSynchronously

    //let addTuple = csl.LockableOp(CAdd (SLK "mykey", SLV <| T ("GG", A [|S "Orz"|])), lock)

    //addTuple.Result


module PCSLTest = 
    open DefaultHelper
    open PCSL


    let testFun () = 
        let pcsl = testPCSL ()

        pcsl._idx._base.Keys |> Seq.toArray |> Array.iter (fun (SLK k) ->
            pcsl.RemoveAsync (k, false) |> ignore
        )

        let s = [|1..200000|]|>Array.map (fun i -> S $"{i}")

        printfn "%A" <| pcsl.Add ("OGC", (A [| S "GG"|]), 3000)
        printfn "%A" <| pcsl.Add ("ORZ2", (A s), 3000, false)

        printfn "%A" <| pcsl.Update ("ORZ", (S "ORZ"), 3000)

        printfn "%A" <| pcsl.Update ("", (S "ORZ"), 3000)

        printfn "%A" <| pcsl.Upsert ("123456", (S "ORZ1"), 3000)
        printfn "%A" <| pcsl.Upsert ("123456", (S "ORZ"))

        printfn "%A" <| pcsl.Remove ("123456", 3000)
        printfn "%A" <| pcsl.Remove ("123456")

        printfn "%A" <| pcsl.TryGetValue ("ORZ2")
        printfn "%A" <| (pcsl.TryGetValue ("ORZ2") |> snd |> _.Value |> (fun (A o) -> o.Length))
        printfn "%A" <| pcsl.TryGetValue ("OGC")

        printfn "%A" <| pcsl["ORZ2"]

    
        printfn "%A" <| pcsl.GenerateKeyHash "ORZ"
        printfn "%A" <| pcsl.TryGetKeyHash ("ORZ2", false)

        printfn "%A" <| pcsl._base._base
        printfn "%A" <| (pcsl._idx._base.Keys |> Seq.toArray)
        printfn "%A" <| pcsl._idxR._base.Keys
        printfn "%A" <| (pcsl._pstatus._base.Keys |> Seq.toArray)
        printfn "%A" <| (pcsl._pstatus._base.Values|> Seq.toArray)

#if ProductionExample
module TreeStructuralFileSystemDesign =
    
#r @"nuget: PersistedConcurrentSortedList, 9.0.19"
//#r @"nuget: FSharp.Collections.ParallelSeq"
open PersistedConcurrentSortedList
open CSL
open PCSL
open PB
open DefaultHelper
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading.Tasks
open FSharp.Collections.ParallelSeq


type FloatDateTimeCache () =
    // Static cache to store mappings from float to DateTime
    static let cache = ConcurrentDictionary<decimal, DateTime>()

    // Static method to convert float to DateTime with caching using arithmetic operations
    static member ConvertToDateTime (value: decimal) =
        // Attempt to retrieve from cache or add a new entry if not present
        cache.GetOrAdd(value, fun v ->          
            
            //                        yyyy MM ddh hmm ssf ff  
            // Extract each component using division and modulus
            let year        = int (value  / 1_00_00_00_00_00_000M)
            let month       = int (value  /    1_00_00_00_00_000M) % 100
              //                 yyyy MM dd hhm mss fff  
            let day         = int (value  /       1_00_00_00_000M) % 100
            let hour        = int (value  /          1_00_00_000M) % 100
            let minute      = int (value  /             1_00_000M % 100M)
            let second      = int (value  /                1_000M % 100M)
            let millisecond = int (value                         % 1_000M)

            // Construct DateTime from parsed components
            printfn "%A" (year, month, day, hour, minute, second, millisecond)
            DateTime(year, month, day, hour, minute, second, millisecond)
        )

type System.Decimal with
    member this.toDT = FloatDateTimeCache.ConvertToDateTime this


type AccountingDate = int64
type MinKey = int64
type CFPCSL = PersistedConcurrentSortedList<Set<MyCandleMetadata> * AccountingDate, fCell<MinKey>>

let cfTA root ta taParam (usingOpt:int option) (k:int) = 
    let r, u = 
        if usingOpt.IsSome then
            $@"{root}\{k}k\{ta}_{taParam}", $@"using_{usingOpt.Value}"
        else
            $@"{root}\{k}k", $"{ta}_{taParam}"

    let pcsl = CFPCSL(
        40, r, u, 3600000
        , PCSLFunHelper<_, fCell<int64>>.oFun
        , PCSLFunHelper<_, fCell<int64>>.eFun, 0, 0)
    pcsl.GenerateKeyHash <-
        fun o ->
            sprintf "%A" o
    pcsl

type MAPeriod = int
type MACDParam = int * int * int
type DMIParam = int * int
    
type WithUsing = {
    using: CD<int, CFPCSL>
}

    

type CFarTAK (friceSeries:FriceSeries<TACSL, MyCandleMetadata>, root, (k:int)) =
    let mutable _ma = new CD<MAPeriod, WithUsing>() 
    let mutable _maVol = new CD<MAPeriod, CFPCSL>() 
    let mutable _macd = new CD<MACDParam, WithUsing>() 
    let mutable _dmi = new CD<DMIParam, CFPCSL>() 
    let mdParser = candleMetadataParser<MyCandleMetadata>
    let initFromFriceSeries () =
        friceSeries.TADict.Keys
        |> Seq.iter (fun ((exComm, scale), mdSet) ->
            let rst = mdParser.ToParseResults mdSet
            if rst.Contains MA then
                let usingValue = rst.GetResult USING // 获取 USING 的整数字段
                let maParam = rst.GetResult MA |> Seq.head |> int
                if usingValue <> 4 then
                    let wu = _ma.GetOrAdd(maParam, fun _ -> 
                        let newUsing = new CD<int, CFPCSL>()
                        { using = newUsing }
                    )
                    wu.using.GetOrAdd(usingValue, fun _ -> cfTA root "ma" $"{maParam}" (Some usingValue) k) |> ignore // 添加 (using 值, cfTA root k)
                else
                    _maVol.GetOrAdd(maParam, fun _ -> cfTA root "maVol" $"{maParam}" None k) |> ignore
            elif rst.Contains MACD then
                let usingValue = rst.GetResult USING // 获取 USING 的整数字段
                let macdParams = rst.GetResult MACD |> Seq.map int |> Seq.toArray
                let macdP = macdParams.[0], macdParams.[1], macdParams.[2]
                let wu = _macd.GetOrAdd(macdP, fun _ -> 
                    let newUsing = new CD<int, CFPCSL>()
                    { using = newUsing }
                )
                wu.using.GetOrAdd(usingValue, fun _ -> cfTA root "macd" (sprintf "%A" macdP) (Some usingValue) k) |> ignore // 添加 (using 值, cfTA root k)
            elif rst.Contains DMI then
                let dmiParams = rst.GetResult DMI |> Seq.map int |> Seq.toArray
                let dmiP = dmiParams.[0], dmiParams.[1]
                _dmi.GetOrAdd(dmiP, fun _ -> cfTA root "dmi" (sprintf "%A" dmiP) None k) |> ignore
            else
                printfn "Invalid TADict Key %A" ((exComm, scale), mdSet)
        )

    do 
        initFromFriceSeries ()

    member this.ma
        with get () = _ma
        and set(v) = _ma <- v
    member this.maVol
        with get () = _maVol
        and set(v) = _maVol <- v
    member this.macd
        with get () = _macd
        and set(v) = _macd <- v
    member this.dmi
        with get () = _dmi
        and set(v) = _dmi <- v

    member this.InitFromFriceSeries = initFromFriceSeries
    member this.Item 
        with get(s:Set<MyCandleMetadata>) =
            let taPArg = mdParser.ToParseResults s
            match true with
            | _ when taPArg.Contains MA && taPArg.GetResult USING = 4 ->
                _maVol[int ((taPArg.GetResult MA)[0])] |> Some
            | _ when taPArg.Contains MA ->
                _ma[int ((taPArg.GetResult MA)[0])].using[taPArg.GetResult USING] |> Some
            | _ when taPArg.Contains MACD ->
                let macdP = taPArg.GetResult MACD
                _macd[int macdP[0], int macdP[1], int macdP[2]].using[taPArg.GetResult USING] |> Some
            | _ when taPArg.Contains DMI ->
                let dmiP = taPArg.GetResult DMI
                _dmi[int dmiP[0], int dmiP[1]] |> Some
            | _ ->
                None
     
type CFarTA (friceSeries:FriceSeries<TACSL, MyCandleMetadata>, root) =
    member val k5       = CFarTAK(friceSeries, root, 5   )
    member val k30      = CFarTAK(friceSeries, root, 30  )
    member val k60      = CFarTAK(friceSeries, root, 60  )
    member val k930     = CFarTAK(friceSeries, root, 930 )
    member val k1380    = CFarTAK(friceSeries, root, 1380)
    member this.Item 
        with get(key: int) =
            match key with
            | 5     -> this.k5
            | 30    -> this.k30
            | 60    -> this.k60
            | 930   -> this.k930
            | 1380  -> this.k1380
            | _     -> this.k1380
    member this.InitFromFriceSeries () =
        this.k5.InitFromFriceSeries ()
        this.k30.InitFromFriceSeries ()
        this.k60.InitFromFriceSeries ()
        this.k930.InitFromFriceSeries ()
        this.k1380.InitFromFriceSeries ()

let cfarta = CFarTA (friceSeries9, @"H:\cFar2")
#endif

#if INTERACTIVE
open PCSLTest
testFun ()
#endif