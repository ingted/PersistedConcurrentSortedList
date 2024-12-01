#if INTERACTIVE
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r @"nuget: NTDLS.DelegateThreadPooling, 1.4.8"
#r @"nuget: NTDLS.FastMemoryCache, 1.7.5"
#r @"nuget: NTDLS.Helpers, 1.3.3"
#r "nuget: NCalc"
#r @"nuget: Serilog, 4.0.1"
#r @"nuget: NTDLS.ReliableMessaging, 1.10.9.0"
#r @"nuget: protobuf-net"
#r @"../NTDLS.Katzebase.Shared/bin/Debug/net8.0/NTDLS.Katzebase.Shared.dll"
#r @"../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Engine.dll"
#r @"../NTDLS.Katzebase.Engine/bin/Debug/net8.0/NTDLS.Katzebase.Client.dll"
#r @"../NTDLS.Katzebase.Parsers.Generic\bin\Debug\net8.0\NTDLS.Katzebase.Parsers.Generic.dll"
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r "nuget: FAkka.FsPickler, 9.0.3"
#r "nuget: FAkka.FsPickler.Json, 9.0.3"
#r @"nuget: protobuf-net"
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\netstandard2.0\protobuf-net-fsharp.dll"
//#load @"Compression.fsx"
#r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
#r @"bin\net9.0\PersistedConcurrentSortedList.dll"
open PersistedConcurrentSortedList
#else
namespace PersistedConcurrentSortedList
#endif

open System.Collections.Generic



module DefaultHelper = 

    open PB
    open CSL
    open PCSL
    open System.Collections.Generic

    type DefKVOpFun<'Key, 'Value when 'Key : comparison and 'Value: comparison> = KVOpFun<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>

    type DefKOpFun<'Key, 'Value when 'Key : comparison and 'Value: comparison> = KOpFun<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>

    type DefVOpFun<'Key, 'Value when 'Key : comparison and 'Value: comparison> = VOpFun<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>

    type PCSLFunHelper<'Key, 'Value when 'Key: comparison and 'Value: comparison> =
        //static let kvkt = typeof<'Key>
        //static let kvvt = typeof<'Value>
        //static let kht = typeof<KeyHash>
        //static let pst = typeof<SortedListPersistenceStatus>

        static member oFun (id: SLTyp) (opResult: OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>):PCSLTaskTyp<'Key, 'Value> = 
#if DEBUG1
            printfn $"SLId: {id}: oFun"
#endif
            match opResult with
            | CUnit -> 
                match id with
                | TSLIdxR -> IdxR CUnit
                | TSLIdx  -> Idx  CUnit
                | TSLPSts -> PS   CUnit
                | TSL     -> KV   CUnit
                | _ -> failwith "Unsupported type in CUnit"

            | CBool b -> 
                match id with
                | TSLIdxR -> IdxR (CBool b)
                | TSLIdx  -> Idx  (CBool b)
                | TSLPSts -> PS   (CBool b)
                | TSL     -> KV   (CBool b)
                | _ -> failwith "Unsupported type in CBool"

            | COptionValue (exists, value) -> 
                let vt = typeof<'Value>
#if DEBUG1
                printfn "vvvvvvvv: %s %A %A" vt.Name value id
#endif
                match id with
                | TSLIdxR -> 
                    let vOpt = value |> Option.map (fun (SLK v)  -> v)
                    IdxR (COptionValue (exists, vOpt))
                | TSL -> 
                    let vOpt = value |> Option.map (fun (SLV v)  -> v)
                    KV   (COptionValue (exists, vOpt))
                | TSLIdx -> 
                    let vOpt = value |> Option.map (fun (SLKH v) -> v)
                    Idx  (COptionValue (exists, vOpt))
                | TSLPSts -> 
                    let vOpt = value |> Option.map (fun (SLPS v) -> v)
                    PS   (COptionValue (exists, vOpt))
                | _ -> failwith "Unsupported type in COptionValue"

            | CInt i -> 
                match id with
                | TSLIdxR -> IdxR (CInt i)
                | TSLIdx  -> Idx  (CInt i)
                | TSLPSts -> PS   (CInt i)
                | TSL     -> KV   (CInt i)
                | _ -> failwith "Unsupported type in CInt"

            | CKeyList keys -> 
                //match typeof<'Key>, typeof<'Value> with
                //| tk, tv when tk = kvkt && tv = kvvt -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    KV (CKeyList ks)
                //| tk, _ when tk = kht -> 
                //    let ks = keys |> Seq.map (fun (SLKH k) -> k) |> (fun s -> List<_>(s)) :> IList<KeyHash>
                //    IdxR (CKeyList ks)
                //| tk, tv when tk = kvkt && tv = pst -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    PS (CKeyList ks)
                //| tk, tv when tk = kvvt && tv = kht -> 
                //    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                //    Idx (CKeyList ks)
                //| _ -> failwith "Unsupported type in CKeyList"
                match id with
                | TSL -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    KV (CKeyList ks)
                | TSLIdxR -> 
                    let ks = keys |> Seq.map (fun (SLKH k) -> k)|> (fun s -> List<_>(s)) :> IList<KeyHash>
                    IdxR (CKeyList ks)
                | TSLPSts -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    PS (CKeyList ks)
                | TSLIdx -> 
                    let ks = keys |> Seq.map (fun (SLK k) -> k) |> (fun s -> List<_>(s)) :> IList<'Key>
                    Idx (CKeyList ks)
                | _ -> failwith "Unsupported type in CKeyList"

            | CValueList values -> 
                match id with
                | TSLIdxR -> 
                    let vs = values |> Seq.map (fun (SLK v) -> v) |> (fun s -> List<_>(s)) :> IList<'Key>
                    IdxR (CValueList vs)
                | TSL -> 
                    let vs = values |> Seq.map (fun (SLV v) -> v) |> (fun s -> List<_>(s)) :> IList<'Value>
                    KV (CValueList vs)
                | TSLIdx -> 
                    let vs = values |> Seq.map (fun (SLKH v) -> v)|> (fun s -> List<_>(s)) :> IList<KeyHash>
                    Idx (CValueList vs)
                | TSLPSts -> 
                    let vs = values |> Seq.map (fun (SLPS v) -> v)|> (fun s -> List<_>(s)) :> IList<SortedListPersistenceStatus>
                    PS (CValueList vs)
                | _ -> failwith "Unsupported type in CValueList"
            | FoldResult rwArr ->
                match id with
                | TSL -> 
                    FoldOpR opResult

                | _ -> failwith "Unsupported type in CValueList"

        static member eFun (id: SLTyp) (taskResult: PCSLTaskTyp<'Key, 'Value>): OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>> =
#if DEBUG1            
            printfn $"SLId: {id}: eFun"
#endif
            match taskResult with
            | KV (o:OpResult<'Key, 'Value>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLV v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLV v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
                
            | Idx (o:OpResult<'Key, KeyHash>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLKH v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLKH v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
            | IdxR (o:OpResult<KeyHash, 'Key>) ->
                match o with
                | CUnit -> 
                    CUnit

                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLK v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:KeyHash) -> SLKH k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLK v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs
            | PS (o:OpResult<'Key, SortedListPersistenceStatus>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CBool b -> 
                    CBool b
                | COptionValue (exists, value) -> 
                    let vOpt = value |> Option.map (fun v -> SLPS v)
                    COptionValue (exists, vOpt)
                | CInt i -> 
                    CInt i
                | CKeyList keys -> 
                    let ks = keys |> Seq.map (fun (k:'Key) -> SLK k) |> fun l -> List<_> l :> IList<_>
                    CKeyList ks
                | CValueList values -> 
                    let vs = values |> Seq.map (fun v -> SLPS v) |> fun l -> List<_> l :> IList<_>
                    CValueList vs

        static member kvExtract (opr: OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>) =
            match opr with
            | CKVList kvArr ->
                kvArr |> Array.map (fun (SLK k, SLV v) -> k, v) |> CKVList
            | CKeyList kl ->
                kl |> Seq.map (fun (SLK k) -> k) |> Seq.toArray :> IList<_> |> CKeyList
            | CValueList kl ->
                kl |> Seq.map (fun (SLV k) -> k) |> Seq.toArray :> IList<_> |> CValueList

    let testPCSL () = 
        let pcsl = PersistedConcurrentSortedList<string, fstring>(
            20, @"c:\pcsl", "test"
            , PCSLFunHelper<string, fstring>.oFun
            , PCSLFunHelper<string, fstring>.eFun)
        
        printfn "InfoPrint: %A" pcsl.InfoPrint

        pcsl



#if INTERACTIVE
open PCSLTest

#endif