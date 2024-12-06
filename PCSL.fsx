namespace PersistedConcurrentSortedList

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
//#r "nuget: FAkka.FsPickler, 9.0.3"
//#r "nuget: FAkka.FsPickler.Json, 9.0.3"
#r @"../../../Libs/FsPickler/bin/net9.0/FsPickler.dll"
#r @"../../../Libs/FsPickler.Json/bin/net9.0/FsPickler.Json.dll"
#r @"nuget: protobuf-net"
#r @"..\..\..\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\netstandard2.0\protobuf-net-fsharp.dll"
#load @"Compression.fsx"
#r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
#endif

open System
open System.Collections
open System.Collections.Generic
#if KATZEBASE
open NTDLS.Katzebase.Parsers.Interfaces
#endif
//open Newtonsoft.Json

open MBrace.FsPickler.Json
open MBrace.FsPickler.Combinators 
open ProtoBuf
open ProtoBuf.FSharp
open FSharp.Reflection


module PCSL =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL
    open PB
    open System.Threading.Tasks

    open FSharp.Collections.ParallelSeq
    open MBrace.FsPickler.Json

    

    type KeyHash = string

    type SortedListLogicalStatus =
    | Inserted
    | Updated
    | Deleted

    type SortedListPersistenceStatus =
    | Buffered //already read from disk and stored in ConcurrentSortedList<'Key, 'Value>, but not changed 
               //or just updated/persisted
    | Changed  //already read from disk and stored in ConcurrentSortedList<'Key, 'Value>, but     changed
    | NonBuffered //just updated/persisted and removed from buffer

    (*
    type TResultGeneric<'OpResult, 'Key when 'Key : comparison> (t:Task<'OpResult>) =
        member this.Result = t.Result
        member this.ResultWithTimeout (_to:int) = 
            t.ResultWithTimeout(_to).Result
        member this.WaitIgnore = t.Result |> ignore
        member this.IsCompleted = t.IsCompleted
        member this.IsCanceled = t.IsCanceled
        member this.IsCompletedSuccessfully = t.IsCompletedSuccessfully
        member this.IsFaulted = t.IsFaulted
        member this.Ignore = ()
        member this.this = t
        member this.thisT = t :> Task
    *)

    type PCSLTaskTyp<'Key, 'Value when 'Key : comparison and 'Value : comparison> =
    | KV of OpResult<'Key, 'Value>
    | Idx of OpResult<'Key, KeyHash>
    | IdxR of OpResult<KeyHash, 'Key>
    | PS of OpResult<'Key, SortedListPersistenceStatus>
    | FoldOpR of OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>
    | UtilOp
    with
                // KV 的成員處理
        member this.kvAdded =
            let (KV (CBool a)) = this
            a

        member this.kvBool =
            let (KV (CBool b)) = this
            b

        member this.kvOptionValue =
            let (KV (COptionValue (flag, value))) = this
            (flag, value)

        member this.kvKeyList =
            let (KV (CKeyList keys)) = this
            keys

        member this.kvValueList =
            let (KV (CValueList values)) = this
            values

        member this.kvInt =
            let (KV (CInt i)) = this
            i

        member this.idxAdded =
            let (Idx (CBool a)) = this
            a


        member this.idxBool =
            let (Idx (CBool b)) = this
            b

        member this.idxOptionValue =
            let (Idx (COptionValue (flag, value))) = this
            (flag, value)

        member this.idxKeyList =
            let (Idx (CKeyList keys)) = this
            keys

        member this.idxValueList =
            let (Idx (CValueList values)) = this
            values

        member this.idxInt =
            let (Idx (CInt i)) = this
            i

        member this.idxRAdded =
            let (IdxR (CBool a)) = this
            a


        member this.idxRBool =
            let (IdxR (CBool b)) = this
            b

        member this.idxROptionValue =
            let (IdxR (COptionValue (flag, value))) = this
            (flag, value)

        member this.idxRKeyList =
            let (IdxR (CKeyList keys)) = this
            keys

        member this.idxRValueList =
            let (IdxR (CValueList values)) = this
            values

        member this.idxRInt =
            let (IdxR (CInt i)) = this
            i

        member this.psAdded =
            let (PS (CBool a)) = this
            a


        member this.psBool =
            let (PS (CBool b)) = this
            b

        member this.psOptionValue =
            let (PS (COptionValue (flag, value))) = this
            (flag, value)

        member this.psKeyList =
            let (PS (CKeyList keys)) = this
            keys

        member this.psValueList =
            let (PS (CValueList values)) = this
            values

        member this.psInt =
            let (PS (CInt i)) = this
            i

        member this.foldOpRWArr =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask

        member this.foldOpResultValKList i =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask[i].OpResult |> Option.bind (fun o -> o.Result.KeyList)

        member this.foldOpResultValVList i =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask[i].OpResult |> Option.bind (fun o -> o.Result.ValueList)

        member this.foldOpResultValKVList i =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask[i].OpResult |> Option.bind (fun o -> o.Result.KVList)
            
        member this.foldOpResultValOpt i =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask[i].OpResult |> Option.bind (fun o -> o.Result.OptionValue) |> Option.bind snd

        member this.foldOpResultValOpR =
            let (FoldOpR (FoldResult keyOpTask)) = this
            keyOpTask |> Array.map (fun opr -> opr.OpResult |> Option.map _.Result)// |> Option.map (fun o -> o.Result)


    and PCSLKVTyp<'Key, 'Value
        when 'Key : comparison
        and 'Value: comparison
        > =
    | SLK of 'Key
    | SLV of 'Value
    | SLKH of KeyHash
    | SLPS of SortedListPersistenceStatus
    with
        member this.slkKey =
            let (SLK key) = this
            key

        member this.slvValue =
            let (SLV value) = this
            value

        member this.slkHash =
            let (SLKH hash) = this
            hash

        member this.slpsStatus =
            let (SLPS status) = this
            status
    (*
    type TResult<'Key when 'Key : comparison> = 
        TResultGeneric<PCSLTaskTyp<'Key>, 'Key>
        

    //type Task<'T> with
    //    member this.tr<'Key, 'Value when 'Key : comparison> () = 
    //        TResult<'Key, 'Value>(this)

    [<Extension>]
    type TaskExtensions() =
        [<Extension>]
        static member Tr<'Key, 'Value 
            when 'Key : comparison
            >(this: Task<PCSLTaskTyp<'Key>>) : TResult<'Key> =
            TResult<'Key>(this)
    *)
    ///其實是 CSL
    type PCSL<'Key, 'Value, 'ID
        when 'Key : comparison
        and 'Value: comparison
        > = ConcurrentSortedList<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>, PCSLTaskTyp<'Key, 'Value>, 'ID>

    type SLTyp =
    | TSL | TSLSts | TSLPSts | TSLIdx | TSLIdxR

    type PersistedConcurrentSortedList<'Key, 'Value
        when 'Key : comparison
        and 'Value: comparison
        >(
        maxDoP: int, basePath: string, schemaName: string
        , defaultTimeout
        , oFun: SLTyp -> OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>> -> PCSLTaskTyp<'Key, 'Value>
        , eFun: SLTyp -> PCSLTaskTyp<'Key, 'Value> -> OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>
        , ?autoCache:int
        , ?autoInitialize:int
        ///使用上要小心，有新刪修的話要小心覆蓋，盡可能唯獨情況下使用
        , ?fileNameFilter:string->bool
        ) as this =
        let js = JsonSerializer()
        let sortedList = 
            if autoCache.IsNone then
                PCSL<'Key, 'Value, SLTyp>(TSL, oFun, eFun, defaultTimeout, 0)
            else
                PCSL<'Key, 'Value, SLTyp>(TSL, oFun, eFun, defaultTimeout, autoCache.Value)
        //let sortedListStatus = PCSL<'Key, 'Value, SLTyp>(TSLSts, oFun, eFun, sortedList.LockObj, defaultTimeout)
        let sortedListPersistenceStatus = PCSL<'Key, 'Value, SLTyp>(TSLPSts, oFun, eFun, sortedList.LockObj, defaultTimeout)
        let sortedListIndexReversed = PCSL<'Key, 'Value, SLTyp>(TSLIdxR, oFun, eFun, sortedList.LockObj, defaultTimeout)
        let sortedListIndex         = PCSL<'Key, 'Value, SLTyp>(TSLIdx, oFun, eFun, sortedList.LockObj, defaultTimeout)

        let schemaPath = Path.Combine(basePath, schemaName)
        let keysPath = Path.Combine(basePath, schemaName, "__keys__")
        let mutable initialized = false

        let createPath p =
            // 创建 schema 文件夹和索引文件夹
            if not (Directory.Exists p) then
                Directory.CreateDirectory p |> ignore


        let indexInitialize () =
            [|
                sortedListIndex.Clean().thisT
                sortedListIndexReversed.Clean().thisT
                sortedListPersistenceStatus.Clean().thisT
            |] |> Task.WaitAll
            let di = DirectoryInfo keysPath
            di.GetFiles()
            
#if ASYNC
            |> PSeq.ordered
            |> PSeq.withDegreeOfParallelism maxDoP
            |> PSeq.filter (fun fi ->
                if fileNameFilter.IsNone then true
                else
                    fileNameFilter.Value fi.FullName 
            )
            |> PSeq.iter (
#else
            |> Seq.iter (
#endif
                fun fi ->
                    let key = js.UnPickleOfString<'Key> (File.ReadAllText fi.FullName)
                    let baseName = fi.Name.Replace(".index", "")
                    let idx = sortedListIndex.Add(SLK key, SLKH baseName)
                    let idxR = sortedListIndexReversed.Add(SLKH baseName, SLK key)
                    let ps = sortedListPersistenceStatus.Add(SLK key, SLPS NonBuffered)
                    let ts = 
                        [|
                            idx.thisT //索引跟 key 必須是一致的
                            idxR.thisT //索引跟 key 必須是一致的
                            ps.thisT
                        |]
                        |> Task.WaitAllWithTimeout defaultTimeout
                    try
                        let (Choice1Of3 _) = ts
                        ()
                    with
                    | exn ->
                        printfn "indexInitialization failed for %s %s" fi.FullName exn.Message
            )

        let valueInitialize maxDop =
            let (Choice1Of3 _) =
                [|
                    sortedList.Clean().thisT
                    sortedListIndex.Clean().thisT
                    sortedListIndexReversed.Clean().thisT
                    sortedListPersistenceStatus.Clean().thisT
                |]
                |> Task.WaitAllWithTimeout 1000
            indexInitialize ()
            let (Some lockId) = sortedListIndex.RequireLock(None, None) |> Async.RunSynchronously
            //printfn "Required lockId: %A" lockId
            try
                let idxs = 
                    sortedListIndex.LockableOp(CKeys, lockId).Result.idxKeyList
                printfn "Idx count: %A" idxs.Count
                let getValueTasks =
                    idxs
                    |> PSeq.ordered
                    |> PSeq.withDegreeOfParallelism maxDop
                    |> PSeq.map (fun idx -> idx, this.TryGetValueNoThreadLock(idx, defaultTimeout, true))
                    |> PSeq.toArray
                sortedListIndex.UnLock lockId
                getValueTasks
            with
            | exn ->
                sortedListIndex.UnLock lockId
                printfn "%A" exn.Message
                reraise ()


        let valueInitializeSingleThread () =
            let (Some lockId) = sortedListIndex.RequireLock(None, None) |> Async.RunSynchronously
            //printfn "Required lockId: %A" lockId
            try
                let idxs = 
                    sortedListIndex.LockableOp(CKeys, lockId).Result.idxKeyList
                printfn "Idx count: %A" idxs.Count
                let getValueTasks =
                    idxs
                    |> Seq.map (fun idx -> idx, this.TryGetValueNoThreadLock(idx, defaultTimeout, true))
                    |> Seq.toArray
                sortedListIndex.UnLock lockId
                getValueTasks
            with
            | exn ->
                sortedListIndex.UnLock lockId
                printfn "%A" exn.Message
                reraise ()


        let mutable write2File = ModelContainer<'Value>.write2File
        let mutable readFromFile = ModelContainer<'Value>.readFromFile
        // 生成 SHA-256 哈希
        let mutable generateKeyHash : 'Key -> KeyHash = fun (key: 'Key) ->
            ModelContainer<'Key>.getHashStr key
        let mutable initTask = Unchecked.defaultof<Task<unit>>
        do 
            createPath schemaPath
            createPath keysPath
            if autoInitialize.IsSome && autoInitialize.Value <> 0 then
                initTask <- task {
                    indexInitialize ()
                    initialized <- true
                }


        let tryGetKeyHash (key: 'Key, ifIgnoreQ) =
#if DEBUG1
            printfn "Query Idx"
#endif
            if ifIgnoreQ then
                let (COptionValue r) = sortedListIndex.GetValue(SLK key, true).Result |> eFun TSLIdxR
                r
            else
                sortedListIndex.TryGetValueSafeTo(SLK key, defaultTimeout)

        let getOrNewAndPersistKeyHash (key: 'Key, ifIgnoreQ) : KeyHash =
#if DEBUG1
            printfn "getOrNewAndPersistKeyHash getOrNewKeyHash"
#endif
            let kh = 
                match tryGetKeyHash (key, ifIgnoreQ) with
                | true, (Some k) -> k
                | _ ->
                    //假設初始化的時候有把全部 KEY 載入，所以後續不會再從 keyPath 讀 index，頂多新增寫入
                    generateKeyHash key |> SLKH
            [|
                sortedListIndex.Add(SLK key, kh, ifIgnoreQ).thisT
                sortedListIndexReversed.Add(kh, SLK key, ifIgnoreQ).thisT
                task {
                    let filePath = Path.Combine(keysPath, kh.slkHash + ".index") //js.UnPickleOfString<A>
                    File.WriteAllText(filePath, (js.PickleToString key))
                } :> Task
            |] 
            |> Task.WaitAllWithTimeout defaultTimeout
            //|> Task.RunSynchronously
            |> fun c ->
#if DEBUG1
                printfn "ccccccccccc: %A" c
#endif
                c
            |> fun (Choice1Of3 success) -> ()
            kh.slkHash
        // 存储 value 到文件
        let persistKeyValueBase ifRemoveFromBuffer ifIgnoreQ =
            let inline write (key: 'Key, value: 'Value) =
                let hashKey = getOrNewAndPersistKeyHash (key, ifIgnoreQ)
                let filePath = Path.Combine(schemaPath, hashKey + ".val")
                write2File filePath value
                key
            write >>
            if ifRemoveFromBuffer then
                fun key ->
                    (sortedList.Remove (SLK key)).WaitIgnore
                    sortedListPersistenceStatus.Upsert (SLK key, SLPS NonBuffered)
            else
                fun key ->
                    sortedListPersistenceStatus.Upsert (SLK key, SLPS Buffered)

        let removePersistedKeyValue (key: 'Key, ifIgnoreQ) =
            let hashKey = 
                match tryGetKeyHash (key, ifIgnoreQ) with
                | true, (Some (SLKH k)) -> k
                | _ ->
                    //假設初始化的時候有把全部 KEY 載入，所以後續不會再從 keyPath 讀 index，頂多新增寫入
                    generateKeyHash key

            let k = SLK key
            [|
                sortedList.Remove k
                sortedListPersistenceStatus.Remove k
                sortedListIndex.Remove k
                sortedListIndexReversed.Remove (SLKH hashKey)
                task {
                    let filePath = Path.Combine(schemaPath, hashKey + ".val")
                    File.Delete filePath
                    return UtilOp
                }
                task {
                    let indexPath = Path.Combine(keysPath, hashKey + ".index") //js.UnPickleOfString<A>
                    File.Delete indexPath
                    return UtilOp
                }
            |]

        let persistKeyValueNoRemove = persistKeyValueBase false
        let persistKeyValueRemove = persistKeyValueBase true
        // 存储 value 到文件
        let persistKeyValues ifRemoveFromBuffer maxDoP (keyValueSeq: ('Key * 'Value) seq) ifIgnoreQ =
            
            let persistKeyValue = persistKeyValueBase ifRemoveFromBuffer ifIgnoreQ
            keyValueSeq
            |> PSeq.ordered
            |> PSeq.withDegreeOfParallelism maxDoP
            |> PSeq.map persistKeyValue


        
        let readFromFileBase (key: 'Key, ifIgnoreQ) : KeyHash option * bool * 'Value option =
            //從 sortedListIndex 
#if DEBUG1
            printfn "readFromFileBase getOrNewKeyHash"
#endif
            match tryGetKeyHash (key, ifIgnoreQ) with
            | true, (Some keyHash) ->
                let kh = keyHash.slkHash
                let filePath = Path.Combine(schemaPath, kh + ".val")
                (Some kh), true, readFromFile filePath //None means file not existed
            | _ ->
                let filePath = Path.Combine(schemaPath, generateKeyHash key + ".val")
                if (FileInfo filePath).Exists then
                    failwith $"[WARNING] Index not consists with file {filePath}"
                else
                    None, false, None //索引無該 key

        let readFromFileBaseAndBuffer (key: 'Key, _toMilli: int, ifIgnoreQSL, ifIgnoreQSLIdx, ifIgnoreQSLPS) =
            let khOpt, fileExisted, valueOpt = readFromFileBase (key, ifIgnoreQSLIdx)
            if khOpt.IsNone then
                None
            else
                let (Choice1Of3 _) =
                    [|
                        sortedList.Add(SLK key, SLV valueOpt.Value, ifIgnoreQSL).thisT
                        sortedListPersistenceStatus.Upsert(SLK key, SLPS Buffered, ifIgnoreQSLPS).thisT
                    |]
                    |> Task.WaitAllWithTimeout _toMilli
                valueOpt


        let nonEmptyArrayResult (ts:Task<PCSLTaskTyp<_, _>>[]) (_toMilli:int) (resultIdx:int) =            
            Task.WaitAll(
                ts |> Array.map (fun t -> t.thisT)
                , _toMilli
            ) |> ignore
            sortedList.ExtractOpResult ts[resultIdx]
            

        let nonEmptyArrayResultBool (ts:Task<PCSLTaskTyp<_, _>>[]) (_toMilli:int) defaultValue resultIdx = 
            if ts.Length > 0 then
                (nonEmptyArrayResult ts _toMilli resultIdx).Bool.Value
            else
                defaultValue

        let nonEmptyArrayResultInt (ts:Task<PCSLTaskTyp<_, _>>[]) (_toMilli:int) defaultValue resultIdx = 
            if ts.Length > 0 then
                (nonEmptyArrayResult ts _toMilli resultIdx).Int.Value
            else
                defaultValue

        member this.TryGetKeyHash = tryGetKeyHash
        member this.GetOrNewAndPersistKeyHash = getOrNewAndPersistKeyHash
        member this.PersistKeyValueNoRemove = persistKeyValueNoRemove
        ///persist 之後移出 buffer (不涉及寫入 base sortedList)
        member this.PersistKeyValueRemove = persistKeyValueRemove
        member this.RemovePersistKeyValue = removePersistedKeyValue
        
        ///persist 之後留在 buffer (不涉及寫入 base sortedList)
        member this.InitTask = initTask
        member this.PersistKeyValues = persistKeyValues
        member this.Write2File 
            with get () = write2File
            and set (v) = write2File <- v

        member this.GenerateKeyHash 
            with get () = generateKeyHash
            and set (v) = generateKeyHash <- v

        member this.ReadFromFile 
            with get () = readFromFile
            and set (v) = readFromFile <- v
            
        member this.Initialized
            with get () = initialized
            and set (v) = initialized <- v

        member this.IndexInitialize = indexInitialize
        member this.ValueInitialize = valueInitialize
        member this.ValueInitializeSingleThread = valueInitializeSingleThread

        // 添加 key-value 对
        member this.AddAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
                if sortedListIndex.ContainsKeySafe (SLK key) then
                    [||]
                else
                    [|                
                        sortedList.Add(SLK key, SLV value)
                        //sortedList.LockableOps (SeqOp [                    
                        //    CAdd (SLK key, SLV value)
                        //]) |> Seq.last
                        if ifRemoveFromBuffer then
                            persistKeyValueRemove ifIgnoreQ (key,  value)
                        else
                            persistKeyValueNoRemove ifIgnoreQ (key,  value)
                    |]
#if DEBUG1
#else                
            )
#endif
        
        member this.AddAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
            this.AddAsync(key, value, ifRemoveFromBuffer, false)

        member this.Add(key: 'Key, value: 'Value, _toMilli:int, ifIgnoreQ) =
            
            let ts = this.AddAsync(key, value, false, ifIgnoreQ) 
            nonEmptyArrayResultBool ts _toMilli false 0
            //if ts.Length > 0 then
            //    Task.WaitAll(
            //        ts |> Array.map (fun t -> t.thisT)
            //        , _toMilli
            //    ) |> ignore
            //    (sortedList.ExtractOpResult ts[0]).Bool.Value
            //else
            //    false

        member this.Add(key: 'Key, value: 'Value, _toMilli:int) =
            this.Add(key, value, _toMilli, false)

        member this.Add(key, value, ifIgnoreQ) =
            this.Add(key, value, defaultTimeout, ifIgnoreQ)

        member this.Add(key, value) =
            this.Add(key, value, false)

        member this.UpdateAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
                if not <| sortedListIndex.ContainsKeySafe (SLK key) then
                    [||]
                else
                    [|
                        sortedList.Update(SLK key, SLV value, ifIgnoreQ)
                        if ifRemoveFromBuffer then
                            persistKeyValueRemove ifIgnoreQ (key,  value)
                        else
                            persistKeyValueNoRemove ifIgnoreQ (key,  value)
                    |]
#if DEBUG1
#else                
            )
#endif
        member this.UpdateAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
            this.UpdateAsync(key, value, ifRemoveFromBuffer, false)

        member this.Update(key: 'Key, value: 'Value, _toMilli:int) =
            let ts = this.UpdateAsync(key, value, false)
            nonEmptyArrayResultBool ts _toMilli false 0
            //if ts.Length > 0 then
            //    Task.WaitAll(
            //        ts |> Array.map (fun t -> t.thisT)
            //        , _toMilli
            //    ) |> ignore
            //    (sortedList.ExtractOpResult ts[0]).Bool.Value
            //else
            //    false

        member this.Update(key, value) =
            this.Update(key, value, defaultTimeout)

        member this.UpsertAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
            [|
                sortedList.Upsert(SLK key, SLV value, ifIgnoreQ)
                if ifRemoveFromBuffer then
                    persistKeyValueRemove ifIgnoreQ  (key,  value)
                else
                    persistKeyValueNoRemove ifIgnoreQ (key,  value)
            |]
#if DEBUG1
#else                
            )
#endif
        member this.UpsertAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
            this.UpsertAsync(key, value, ifRemoveFromBuffer, false)

        member this.Upsert(key: 'Key, value: 'Value, _toMilli:int, ifIgnoreQ) =
            let ts = this.UpsertAsync(key, value, false, ifIgnoreQ)
            nonEmptyArrayResultInt ts _toMilli 0 0
            //Task.WaitAll(
            //    this.UpsertAsync(key, value, false) |> Array.map (fun t -> t.thisT)
            //    , _toMilli
            //)
        member this.Upsert(key: 'Key, value: 'Value, _toMilli:int) =
            this.Upsert(key, value, _toMilli, false)

        member this.Upsert(key, value) =
            this.Upsert(key, value, defaultTimeout)

        member this.RemoveAsync(key: 'Key, ifIgnoreQ) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
                removePersistedKeyValue (key, ifIgnoreQ)
#if DEBUG1
#else                
            )
#endif
        member this.RemoveAsync(key: 'Key) =
            this.RemoveAsync(key, false)

        member this.Remove(key: 'Key, _toMilli:int, ifIgnoreQ) =
            let ts = this.RemoveAsync(key, ifIgnoreQ)
            nonEmptyArrayResultBool ts _toMilli false 0 
            //Task.WaitAll(
            //    this.RemoveAsync(key) |> Array.map (fun t -> t.thisT)
            //    , _toMilli
            //)

        member this.Remove(key: 'Key, _toMilli:int) =
            this.Remove(key, _toMilli, false)

        member this.Remove(key) =
            this.Remove(key, defaultTimeout)


        //好像沒意義，拿掉
        //member this.AddWithPersistenceAsync(key: 'Key, value: 'Value) =
        //    this.AddAsync(key, value)[0]

        //好像沒意義，拿掉
        //member this.AddWithPersistence(key: 'Key, value: 'Value, _toMilli:int) =
        //    this.AddWithPersistenceAsync(key, value).WaitAsync(_toMilli).Result

        // 获取 value，如果 ConcurrentSortedList 中不存在则从文件系统中读取
        member this.TryGetValueNoThreadLock (key: 'Key, _toMilli:int, ifIgnoreQ) = //: bool * 'Value option =
            
#if DEBUG
            let mutable trace = 0
            try
#endif
                let gr = sortedList.GetValue(SLK key, ifIgnoreQ)
#if DEBUG
                trace <- 1            
#endif
                //printfn "[TryGetValueNoThreadLock.GetValue] value task: %A" gr
                //let (Choice1Of3 _) =
                //    [|
                //        gr.thisT
                //        //us.thisT
                //    |]
                //    |> Task.WaitAllWithTimeout _toMilli
                let exists, value = gr.Result.kvOptionValue
                //printfn "[TryGetValueNoThreadLock.kvOptionValue] exists: %A, value: %A" exists value
#if DEBUG
                trace <- 2
#endif
            
                if exists then
                    let us = sortedListPersistenceStatus.Upsert(SLK key, SLPS Buffered, ifIgnoreQ).Result
                    //printfn "[TryGetValueNoThreadLock.Upsert] value task: %A" us
#if DEBUG
                    trace <- 3
#endif
            
                    true, value
                else
#if DEBUG
                    trace <- 4
#endif
                    match readFromFileBaseAndBuffer (key, _toMilli, ifIgnoreQ, ifIgnoreQ, ifIgnoreQ) with
                    | Some v ->
                        true, Some v
                    | None -> false, None
#if DEBUG
            with
            | exn ->    
                printfn "[TryGetValueNoThreadLock] %s" exn.Message
                printfn "[TryGetValueNoThreadLock][%d] Query KV %A" trace key
                false, None
#endif            

        member this.TryGetValue(key: 'Key, _toMilli:int) = 
            lock sortedList.LockObj (fun () ->
                this.TryGetValueNoThreadLock (key, _toMilli, false)
            )
        member this.TryGetValue(key) =
            this.TryGetValue(key, defaultTimeout)

        member this.Item
            with get(key: 'Key) =
                (this.TryGetValue(key) |> snd).Value
            and set(k: 'Key) (v: 'Value) =
                failwith "upsert not yet implemented"

        member this.TryGetValueAsync(key: 'Key) : Task<bool * 'Value option> =
            task {
                return this.TryGetValue(key)
            }
        // 其他成员可以根据需要进行扩展，比如 Count、Remove 等

        member val _base    = sortedList                    with get
        member val _idx     = sortedListIndex               with get
        member val _idxR    = sortedListIndexReversed       with get
        //member val _status  = sortedListStatus              with get
        member val _pstatus = sortedListPersistenceStatus   with get

        member this.Info = {|
            base_count = this._base._base.Keys    |> Seq.length
            idx_count  = this._idx._base.Keys     |> Seq.length
            idxR_count = this._idxR._base.Keys    |> Seq.length
            psts_count = this._pstatus._base.Keys |> Seq.length
        |}

        member this.InfoPrint =
            printfn "Info: %A" this.Info

        //member this.GetKeysArray() =
            

        new (maxDoP, basePath, schemaName, oFun, eFun) =
            PersistedConcurrentSortedList<'Key, 'Value>(
                maxDoP, basePath, schemaName
                , 30000, oFun, eFun)

        new (maxDoP, basePath, schemaName
            , oFun: SLTyp -> OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>> -> PCSLTaskTyp<'Key, 'Value>
            , eFun: SLTyp -> PCSLTaskTyp<'Key, 'Value> -> OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>>
            , autoCache) =
            PersistedConcurrentSortedList<'Key, 'Value>(
                maxDoP, basePath, schemaName
                , 30000, oFun = oFun, eFun = eFun, autoCache = autoCache)

