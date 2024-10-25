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
    open PCSL
    let csl = PCSL<string, fstring, SLTyp>(TSL, PCSLFunHelper<string, fstring>.oFun, PCSLFunHelper<string, fstring>.eFun)

    //let (Some lock) = csl.RequireLock(None, None) |> Async.RunSynchronously

module PCSLTest = 
    open DefaultHelper
    open PCSL


    let testFun () = 
        let pcsl = testPCSL ()

        pcsl._idx._base.Keys |> Seq.toArray |> Array.iter (fun (SLK k) ->
            pcsl.RemoveAsync k |> ignore
        )

        let s = [|1..200000|]|>Array.map (fun i -> S $"{i}")

        printfn "%A" <| pcsl.Add ("OGC", (A [| S "GG"|]), 3000)
        printfn "%A" <| pcsl.Add ("ORZ2", (A s), 3000)

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
        printfn "%A" <| pcsl.GetOrNewKeyHash "ORZ2"

        printfn "%A" <| pcsl._base._base
        printfn "%A" <| (pcsl._idx._base.Keys |> Seq.toArray)
        printfn "%A" <| pcsl._idxR._base.Keys
        printfn "%A" <| (pcsl._pstatus._base.Keys |> Seq.toArray)
        printfn "%A" <| (pcsl._pstatus._base.Values|> Seq.toArray)



#if INTERACTIVE
open PCSLTest
testFun ()
#endif