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
open System.Threading

module PCSL2 =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL2
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

    type CSL2<'Key, 'Value, 'ID
        when 'Key : comparison
        and 'Value: comparison
        > = ConcurrentSortedList<'Key, 'Value, 'ID>

    type SLTyp =
    | TSL | TSLSts | TSLPSts | TSLIdx | TSLIdxR


    type PersistedConcurrentSortedList<'Key, 'Value
        when 'Key : comparison
        and 'Value: comparison
        >(
        maxDoP: int, basePath: string, schemaName: string
        /// milliseconds
        , defaultTimeout
        , ?autoCache:int
        , ?autoInitialize:int
        ///使用上要小心，有新刪修的話要小心覆蓋，盡可能唯獨情況下使用
        , ?fileNameFilter:string->bool
        ) as this =
        let js = JsonSerializer()
        let rwLock = new ReaderWriterLockSlim()
        let sortedList = 
            if autoCache.IsNone then
                CSL2<'Key, 'Value, SLTyp>(TSL, defaultTimeout, 0)
            else
                CSL2<'Key, 'Value, SLTyp>(TSL, defaultTimeout, autoCache.Value)
        //let sortedListStatus = PCSL<'Key, 'Value, SLTyp>(TSLSts, oFun, eFun, sortedList.LockObj, defaultTimeout)
        let sortedListPersistenceStatus = CSL2<'Key, SortedListPersistenceStatus, SLTyp>(TSLPSts, sortedList.LockObj, defaultTimeout)
        let sortedListIndexReversed     = CSL2<KeyHash, 'Key, SLTyp>(TSLIdxR, sortedList.LockObj, defaultTimeout)
        let sortedListIndex             = CSL2<'Key, KeyHash, SLTyp>(TSLIdx , sortedList.LockObj, defaultTimeout)

        let schemaPath = Path.Combine(basePath, schemaName)
        let keysPath = Path.Combine(basePath, schemaName, "__keys__")
        let mutable indexInitialized = false
        let mutable fullyBuffered = false

        let createPath p =
            // 创建 schema 文件夹和索引文件夹
            if not (Directory.Exists p) then
                Directory.CreateDirectory p |> ignore

        //let mreWriteLock = new ManualResetEventSlim true
        //let mreReadLock = new ManualResetEventSlim false
        //let _ = 
        //    sortedList.mreWriteOpt <- Some mreWriteLock
        //let _ = 
        //    sortedList.mreReadOpt <- Some mreReadLock

        let indexInitializeBase () =
                fullyBuffered <- false
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
                        let idx = sortedListIndex.Add(key, baseName)
                        let idxR = sortedListIndexReversed.Add(baseName, key)
                        let ps = sortedListPersistenceStatus.Add(key, NonBuffered)
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
            
        let indexInitialize () =
            lock sortedListIndex.LockObj (fun () ->
                indexInitializeBase ()
            )

        let valueInitializeBase maxDop =
            fullyBuffered <- false
            let (Choice1Of3 _) =
                [|
                    sortedList.Clean().thisT
                |]
                |> Task.WaitAllWithTimeout 1000
            indexInitializeBase ()
            let (Some lockId) = sortedListIndex.RequireLock(None, None) |> Async.RunSynchronously
            //printfn "Required lockId: %A" lockId
            try
                let idxs = 
                    sortedListIndex.LockableOp(CKeys, lockId).Result.KeyList
                printfn "Idx count: %A" (idxs |> Option.map (fun l -> l.Count))
                let getValueTasks =
                    idxs.Value
                    |> PSeq.ordered
                    |> PSeq.withDegreeOfParallelism maxDop
                    |> PSeq.map (fun idx -> idx, this.TryGetValueNoThreadLock(idx, defaultTimeout, true))
                    |> PSeq.toArray
                sortedListIndex.UnLock lockId
                fullyBuffered <- true
                getValueTasks
            with
            | exn ->
                sortedListIndex.UnLock lockId
                printfn "%A" exn.Message
                reraise ()

        let valueInitialize2 ifForceInitialize maxDop =
            lock sortedList.LockObj (fun () ->
                if not fullyBuffered || ifForceInitialize then
                    valueInitializeBase maxDop
                else
                    sortedList._base
                    |> Seq.map (fun kv -> kv.Key, (true, Some kv.Value))
                    |> Seq.toArray
            )

        let valueInitialize = valueInitialize2 false

        let valueInitializeSingleThread () =
            let (Some lockId) = sortedListIndex.RequireLock(None, None) |> Async.RunSynchronously
            //printfn "Required lockId: %A" lockId
            try
                let idxs = 
                    sortedListIndex.LockableOp(CKeys, lockId).Result.KeyList
                printfn "Idx count: %A" (idxs |> Option.map (fun l -> l.Count))
                let getValueTasks =
                    idxs.Value
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
                    indexInitialized <- true
                }


        let tryGetKeyHash (key: 'Key, ifIgnoreQ) =
#if DEBUG1
            printfn "Query Idx"
#endif
            if ifIgnoreQ then
                let (COptionValue r) = sortedListIndex.GetValue(key, true).Result
                r
            else
                sortedListIndex.TryGetValueSafeTo(key, defaultTimeout)

        let getOrNewAndPersistKeyHash (key: 'Key, ifIgnoreQ) =
#if DEBUG1
            printfn "getOrNewAndPersistKeyHash getOrNewKeyHash"
#endif
            let kh = 
                match tryGetKeyHash (key, ifIgnoreQ) with
                | true, (Some k) -> k
                | _ ->
                    //假設初始化的時候有把全部 KEY 載入，所以後續不會再從 keyPath 讀 index，頂多新增寫入
                    generateKeyHash key

            [|
                sortedListIndex.Add(key, kh, ifIgnoreQ).thisT
                sortedListIndexReversed.Add(kh, key, ifIgnoreQ).thisT
                task {
                    let filePath = Path.Combine(keysPath, kh + ".index") //js.UnPickleOfString<A>
                    File.WriteAllText(filePath, (js.PickleToString key))
                } :> Task
            |] 
            |> Task.WaitAllWithTimeout defaultTimeout
#if DEBUG1
            |> fun c ->
                printfn "ccccccccccc: %A" c
                c
#endif
            |> fun w ->
                match w with
                | Choice1Of3 success -> ()
                | _ ->
                    failwithf "[getOrNewAndPersistKeyHash] Failed! %A" w
            kh
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
                    (sortedList.Remove (key)).WaitIgnore
                    sortedListPersistenceStatus.Upsert (key, NonBuffered)
            else
                fun key ->
                    sortedListPersistenceStatus.Upsert (key, Buffered)

        let removePersistedKeyValue (key: 'Key, ifIgnoreQ) =
            let hashKey = 
                match tryGetKeyHash (key, ifIgnoreQ) with
                | true, (Some k) -> k
                | _ ->
                    //假設初始化的時候有把全部 KEY 載入，所以後續不會再從 keyPath 讀 index，頂多新增寫入
                    generateKeyHash key

            [|
                sortedList.Remove(key).thisT
                sortedListPersistenceStatus.Remove(key).thisT
                sortedListIndex.Remove(key).thisT
                sortedListIndexReversed.Remove(hashKey).thisT
                task {
                    let filePath = Path.Combine(schemaPath, hashKey + ".val")
                    File.Delete filePath
                }
                task {
                    let indexPath = Path.Combine(keysPath, hashKey + ".index") //js.UnPickleOfString<A>
                    File.Delete indexPath
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
                let filePath = Path.Combine(schemaPath, keyHash + ".val")
                (Some keyHash), true, readFromFile filePath //None means file not existed
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
                        sortedList.Add(key, valueOpt.Value, ifIgnoreQSL).thisT
                        sortedListPersistenceStatus.Upsert(key, Buffered, ifIgnoreQSLPS).thisT
                    |]
                    |> Task.WaitAllWithTimeout _toMilli
                valueOpt


        let nonEmptyArrayResult (ts:Task[]) (_toMilli:int) (resultIdx:int) =            
            Task.WaitAll(ts, _toMilli) |> ignore
            (ts[resultIdx] :?> Task<OpResult<'Key, 'Value>>).Result
            

        let nonEmptyArrayResultBool (ts:Task[]) (_toMilli:int) defaultValue resultIdx = 
            if ts.Length > 0 then
                (nonEmptyArrayResult ts _toMilli resultIdx).Bool.Value
            else
                defaultValue

        let nonEmptyArrayResultInt (ts:Task[]) (_toMilli:int) defaultValue resultIdx = 
            if ts.Length > 0 then
                (nonEmptyArrayResult ts _toMilli resultIdx).Int.Value
            else
                defaultValue

        interface System.IComparable with
            member this.CompareTo(otherObj) =
                match otherObj with
                | :? PersistedConcurrentSortedList<'Key, 'Value> as other ->
                    let keyCompare = compare (this._idx.KeysSafe |> Seq.toArray) (other._idx.KeysSafe |> Seq.toArray)
                    if keyCompare = 0 then
                        compare (this._base.ValuesSafe |> Seq.toArray) (other._base.ValuesSafe |> Seq.toArray)
                    else
                        keyCompare
                | _ -> invalidArg "otherObj" $"Not a PersistedConcurrentSortedList<{typeof<'Key>}, {typeof<'Value>}>"

        member this.TryGetKeyHash = tryGetKeyHash
        member this.GetOrNewAndPersistKeyHash = getOrNewAndPersistKeyHash
        member this.PersistKeyValueNoRemove = persistKeyValueNoRemove
        ///persist 之後移出 buffer (不涉及寫入 base sortedList)

        
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
            
        member this.IndexInitialized
            with get () = indexInitialized
            and set (v) = indexInitialized <- v

        member this.IndexInitialize = indexInitialize
        member this.ValueInitialize = valueInitialize
        member this.ValueInitializeSingleThread = valueInitializeSingleThread

        // 添加 key-value 对
        member this.AddAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
                rwLock.EnterWriteLock()
            //lock sortedList.LockObj (fun () ->
#endif
                if sortedListIndex.ContainsKeySafe (key) then
#if DEBUG
                    printfn "[AddAsync] Key %A already exists, Value: %A" key value
#endif
#if DEBUG1
#else

                    rwLock.ExitWriteLock()
#endif
                    [||]
                else
                    let rst = [|                
                            sortedList.Add(key, value).thisT
                            ((persistKeyValueBase ifRemoveFromBuffer ifIgnoreQ) (key,  value)).thisT
                        |]
#if DEBUG1
#else                
            //)
                    rwLock.ExitWriteLock()
#endif
                    rst
        
        member this.AddAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
            this.AddAsync(key, value, ifRemoveFromBuffer, false)

        member this.AddAsync(keyValueFunc: unit -> 'Key * 'Value, ifRemoveFromBuffer) =
            task {
                let key, value = keyValueFunc()
                return this.AddAsync(key, value, ifRemoveFromBuffer, false)
            }

        member this.Add(key: 'Key, value: 'Value, _toMilli:int, ifIgnoreQ) =            
            let ts = this.AddAsync(key, value, false, ifIgnoreQ)             
            nonEmptyArrayResultBool ts _toMilli false 0
            

        member this.Add(key: 'Key, value: 'Value, _toMilli:int) =
            this.Add(key, value, _toMilli, false)

        member this.Add(key, value, ifIgnoreQ) =
            this.Add(key, value, defaultTimeout, ifIgnoreQ)

        member this.Add(key, value) =
            this.Add(key, value, false)

        member this.UpdateAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
            //lock sortedList.LockObj (fun () ->
                rwLock.EnterWriteLock()
#endif
                if not <| sortedListIndex.ContainsKeySafe key then
                    rwLock.ExitWriteLock()
                    [||]
                else
                    let rst = [|
                            sortedList.Update(key, value, ifIgnoreQ).thisT
                            ((persistKeyValueBase ifRemoveFromBuffer ifIgnoreQ) (key,  value)).thisT
                        |]
#if DEBUG1
#else                
                    rwLock.ExitWriteLock()
            //)
#endif
                    rst

        member this.UpdateAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
            this.UpdateAsync(key, value, ifRemoveFromBuffer, false)

        member this.Update(key: 'Key, value: 'Value, _toMilli:int) =
            let ts = this.UpdateAsync(key, value, false)
            nonEmptyArrayResultBool ts _toMilli false 0

        member this.Update(key, value) =
            this.Update(key, value, defaultTimeout)

        member this.UpsertAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer, ifIgnoreQ) =
#if DEBUG1
#else
            //lock sortedList.LockObj (fun () ->
            rwLock.EnterWriteLock()
#endif
            let rst =
                [|
                    sortedList.Upsert(key, value, ifIgnoreQ).thisT
                    ((persistKeyValueBase ifRemoveFromBuffer ifIgnoreQ) (key,  value)).thisT
                |]
#if DEBUG1
#else                
            rwLock.ExitWriteLock()
            //)
#endif
            rst
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
            //lock sortedList.LockObj (fun () ->
                rwLock.EnterWriteLock()
#endif
                let taskArr = removePersistedKeyValue (key, ifIgnoreQ)
#if DEBUG1
#else                
            //)
                rwLock.ExitWriteLock()                
#endif
                taskArr
        member this.RemoveAsync(key: 'Key) =
            this.RemoveAsync(key, false)

        member this.Remove(key: 'Key, _toMilli:int, ifIgnoreQ) =
            let ts = this.RemoveAsync(key, ifIgnoreQ)
            nonEmptyArrayResultBool ts _toMilli false 0 

        member this.Remove(key: 'Key, _toMilli:int) =
            this.Remove(key, _toMilli, false)

        member this.Remove(key) =
            this.Remove(key, defaultTimeout)

        member this.ContainsKey (key: 'Key, _toMilliOpt:int option) = //: bool =
            let gr = sortedListIndex.ContainsKey key
            let ifExisted = 
                if _toMilliOpt.IsSome then
                    gr.WaitAsync(_toMilliOpt.Value).Result.Bool.Value
                else
                    gr.Result.Bool.Value
            ifExisted
        member this.ContainsKey (key: 'Key) = //: bool =
            this.ContainsKey(key, Some 30000)
        


        member this.TryGetValueNoThreadLock (key: 'Key, _toMilli:int, ifIgnoreQ) = //: bool * 'Value option =
            
#if DEBUG
            let mutable trace = 0
            try
#endif
                //mreReadLock.Reset ()
                //mre.Wait ()
                let gr = sortedList.GetValue(key, ifIgnoreQ)
                //mreReadLock.Set ()
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
                let exists, value = gr.Result.OptionValue.Value
                //printfn "[TryGetValueNoThreadLock.kvOptionValue] exists: %A, value: %A" exists value
#if DEBUG
                trace <- 2
#endif
            
                if exists then
                    let us = sortedListPersistenceStatus.Upsert(key, Buffered, ifIgnoreQ).Result
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
        
        ///需要全資料初始化，所以就不需要後續的 buffered 了，不選擇各自 readfromfile 原因是要利用 CSL 的 GetValues API
        member this.TryGetValuesNoThreadLock (keys: 'Key seq, _toMilli:int, ifIgnoreQ, ?maxDoP:int) = //: bool * 'Value option =
                
                if maxDoP.IsNone then
                    valueInitialize 40 |> ignore
                else
                
                    valueInitialize maxDoP.Value |> ignore
                
                let gr = sortedList.GetValues(keys, ifIgnoreQ)

                let values = gr.Result.KVOptList.Value

                values
                //let markBuffered =
                //    values
                //    |> Seq.choose (fun (key, vOpt) ->
                //        vOpt
                //        |> Option.map (fun _ ->
                //            sortedListPersistenceStatus.Upsert(key, Buffered, ifIgnoreQ).thisT
                //        )
                //    )

                //match markBuffered |> Task.WaitAllWithTimeout _toMilli with
                //| Choice1Of3 _ ->
                //    values
                //| Choice2Of3 exnTimeout ->
                //    raise exnTimeout
                //| Choice3Of3 exnAgg ->
                //    raise exnAgg

        member this.FirstLastN (n, ?maxDoP:int) = 
            if maxDoP.IsNone then
                valueInitialize 40 |> ignore
            else            
                valueInitialize maxDoP.Value |> ignore
            
            sortedList.FirstLastN(n)

        member this.FirstLastNKeys (n, ?maxDoP:int) = 
            if maxDoP.IsNone then
                valueInitialize 40 |> ignore
            else            
                valueInitialize maxDoP.Value |> ignore
            
            sortedList.FirstLastNKeys(n)

        member this.FirstLastNValues (n, ?maxDoP:int) = 
            if maxDoP.IsNone then
                valueInitialize 40 |> ignore
            else            
                valueInitialize maxDoP.Value |> ignore
            
            sortedList.FirstLastNValues(n)


        member this.TryGetValue(key: 'Key, _toMilli:int) = 
            //lock sortedList.LockObj (fun () ->
                rwLock.EnterReadLock()
                let rst = this.TryGetValueNoThreadLock (key, _toMilli, false)
                rwLock.ExitReadLock()
                rst
            //)

        member this.TryGetValues(keys: 'Key seq, _toMilli:int) = 
                rwLock.EnterReadLock()
                let rst = this.TryGetValuesNoThreadLock (keys, _toMilli, false)
                rwLock.ExitReadLock()
                rst

        member this.TryGetValue(key) =
            this.TryGetValue(key, defaultTimeout)

        member this.TryGetValues(keys) =
            this.TryGetValues(keys, defaultTimeout)

        member this.Item
            with get(key: 'Key) =
                (this.TryGetValue(key) |> snd).Value
            and set(k: 'Key) (v: 'Value) =
                //failwith "upsert not yet implemented"
                this.Upsert(k, v, defaultTimeout, false) |> ignore


        

        member this.TryGetValueAsync(key: 'Key) : Task<bool * 'Value option> =
            task {
                return this.TryGetValue(key)
            }

        member this.TryGetValuesAsync(keys: 'Key seq) =
            task {
                return this.TryGetValues(keys)
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
            

        new (maxDoP, basePath, schemaName) =
            PersistedConcurrentSortedList<'Key, 'Value>(
                maxDoP, basePath, schemaName
                , 30000)

        new (maxDoP, basePath, schemaName
            , autoCache) =
            PersistedConcurrentSortedList<'Key, 'Value>(
                maxDoP, basePath, schemaName
                , 30000, autoCache = autoCache)



    type CSLPersist<'Key, 'Value, 'ID, 'PersistenceKey, 'PersistenceValue
        when 'Key : comparison
        and 'Value: comparison
        and 'PersistenceKey: comparison
        and 'PersistenceValue: comparison
        > = {
        csl: CSL2<'Key, 'Value, 'ID>
        pcsl: PersistedConcurrentSortedList<'PersistenceKey, 'PersistenceValue>
        archiveFun: (('Key * 'Value) seq -> 'PersistenceKey * 'PersistenceValue)
    }