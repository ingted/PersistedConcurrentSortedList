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
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\netstandard2.0\protobuf-net-fsharp.dll"
#load @"Compression.fsx"
#r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
#endif

open System
open System.Collections
open System.Collections.Generic
#if KATZEBASE
open NTDLS.Katzebase.Parsers.Interfaces
#endif
open Newtonsoft.Json

open MBrace.FsPickler
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
    | UtilOp
    with
                // KV 的成員處理
        member this.kvAdded =
            let (KV (CBool a)) = this
            a

        member this.kvUpdated =
            let (KV CUpdated) = this
            true

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

        member this.idxUpdated =
            let (Idx CUpdated) = this
            true

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

        member this.idxRUpdated =
            let (IdxR CUpdated) = this
            true

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

        member this.psUpdated =
            let (PS CUpdated) = this
            true

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

    type PCSLKVTyp<'Key, 'Value
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
        , defaultTimeout, oFun, eFun
        ) =
        let js = JsonSerializer()
        let sortedList = PCSL<'Key, 'Value, SLTyp>(TSL, oFun, eFun)
        let sortedListStatus = PCSL<'Key, 'Value, SLTyp>(TSLSts, oFun, eFun, sortedList.LockObj, defaultTimeout)
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


        let indexInitialization () =
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
                        |> Task.WaitAllWithTimeout 30000
                    try
                        let (Choice1Of3 _) = ts
                        ()
                    with
                    | exn ->
                        printfn "indexInitialization failed for %s %s" fi.FullName exn.Message
            )
        let mutable write2File = ModelContainer<'Value>.write2File


        do 
            createPath schemaPath
            createPath keysPath
            indexInitialization ()
            initialized <- true

        // 生成 SHA-256 哈希
        let generateKeyHash (key: 'Key) : KeyHash =
            ModelContainer<'Key>.getHashStr key

        let getOrNewKeyHash (key: 'Key) =
#if DEBUG1
            printfn "Query Idx"
#endif
            sortedListIndex.TryGetValueSafeTo(SLK key, 1000)

        let getOrNewAndPersistKeyHash (key: 'Key) : KeyHash =
#if DEBUG1
            printfn "getOrNewAndPersistKeyHash getOrNewKeyHash"
#endif
            let kh = 
                match getOrNewKeyHash key with
                | true, (Some k) -> k
                | _ ->
                    //假設初始化的時候有把全部 KEY 載入，所以後續不會再從 keyPath 讀 index，頂多新增寫入
                    generateKeyHash key |> SLKH
            [|
                sortedListIndex.Add(SLK key, kh).thisT
                sortedListIndexReversed.Add(kh, SLK key).thisT
                task {
                    let filePath = Path.Combine(keysPath, kh.slkHash + ".index") //js.UnPickleOfString<A>
                    File.WriteAllText(filePath, (js.PickleToString key))
                } :> Task
            |] 
            |> Task.WaitAllWithTimeout 30000
            //|> Task.RunSynchronously
            |> fun c ->
#if DEBUG1
                printfn "ccccccccccc: %A" c
#endif
                c
            |> fun (Choice1Of3 success) -> ()
            kh.slkHash
        // 存储 value 到文件
        let persistKeyValueBase ifRemoveFromBuffer =
            let inline write (key: 'Key, value: 'Value) =
                let hashKey = getOrNewAndPersistKeyHash key
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

        let removePersistedKeyValue (key: 'Key) =
            let hashKey = 
                match getOrNewKeyHash key with
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
        let persistKeyValues ifRemoveFromBuffer maxDoP (keyValueSeq: ('Key * 'Value) seq) =
            
            let persistKeyValue = persistKeyValueBase ifRemoveFromBuffer
            keyValueSeq
            |> PSeq.ordered
            |> PSeq.withDegreeOfParallelism maxDoP
            |> PSeq.map persistKeyValue


        
        let readFromFileBase (key: 'Key) : KeyHash option * bool * 'Value option =
            //從 sortedListIndex 
#if DEBUG1
            printfn "readFromFileBase getOrNewKeyHash"
#endif
            match getOrNewKeyHash key with
            | true, (Some keyHash) ->
                let kh = keyHash.slkHash
                let filePath = Path.Combine(schemaPath, kh + ".val")
                (Some kh), true, ModelContainer<'Value>.readFromFile filePath //None means file not existed
            | _ ->
                let filePath = Path.Combine(schemaPath, generateKeyHash key + ".val")
                if (FileInfo filePath).Exists then
                    failwith $"[WARNING] Index not consists with file {filePath}"
                else
                    None, false, None //索引無該 key

        let readFromFileBaseAndBuffer (key: 'Key) =
            let khOpt, fileExisted, valueOpt = readFromFileBase key
            if khOpt.IsNone then
                None
            else
                sortedList.Add(SLK key, SLV valueOpt.Value).Ignore()
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

        member this.GenerateKeyHash = generateKeyHash
        member this.GetOrNewKeyHash = getOrNewKeyHash
        member this.GetOrNewAndPersistKeyHash = getOrNewAndPersistKeyHash
        member this.PersistKeyValueNoRemove = persistKeyValueNoRemove
        ///persist 之後移出 buffer (不涉及寫入 base sortedList)
        member this.PersistKeyValueRemove = persistKeyValueRemove
        member this.RemovePersistKeyValue = removePersistedKeyValue
        
        ///persist 之後留在 buffer (不涉及寫入 base sortedList)
        member this.PersistKeyValues = persistKeyValues

        member this.Initialized = initialized
        // 添加 key-value 对
        member this.AddAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
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
                        persistKeyValueRemove(key,  value)
                    else
                        persistKeyValueNoRemove(key,  value)
                |]
#if DEBUG1
#else                
            )
#endif
        member this.Add(key: 'Key, value: 'Value, _toMilli:int) =
            
            let ts = this.AddAsync(key, value, false) 
            nonEmptyArrayResultBool ts _toMilli false 0
            //if ts.Length > 0 then
            //    Task.WaitAll(
            //        ts |> Array.map (fun t -> t.thisT)
            //        , _toMilli
            //    ) |> ignore
            //    (sortedList.ExtractOpResult ts[0]).Bool.Value
            //else
            //    false

        member this.Add(key, value) =
            this.Add(key, value, defaultTimeout)


        member this.UpdateAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
            if not <| sortedListIndex.ContainsKeySafe (SLK key) then
                [||]
            else
                [|
                    sortedList.Update(SLK key, SLV value)
                    if ifRemoveFromBuffer then
                        persistKeyValueRemove(key,  value)
                    else
                        persistKeyValueNoRemove(key,  value)
                |]
#if DEBUG1
#else                
            )
#endif
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

        member this.UpsertAsync(key: 'Key, value: 'Value, ifRemoveFromBuffer) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
            [|
                sortedList.Upsert(SLK key, SLV value)
                if ifRemoveFromBuffer then
                    persistKeyValueRemove(key,  value)
                else
                    persistKeyValueNoRemove(key,  value)
            |]
#if DEBUG1
#else                
            )
#endif
        member this.Upsert(key: 'Key, value: 'Value, _toMilli:int) =
            let ts = this.UpsertAsync(key, value, false)
            nonEmptyArrayResultInt ts _toMilli 0 0
            //Task.WaitAll(
            //    this.UpsertAsync(key, value, false) |> Array.map (fun t -> t.thisT)
            //    , _toMilli
            //)

        member this.Upsert(key, value) =
            this.Upsert(key, value, defaultTimeout)

        member this.RemoveAsync(key: 'Key) =
#if DEBUG1
#else
            lock sortedList.LockObj (fun () ->
#endif
            removePersistedKeyValue key
#if DEBUG1
#else                
            )
#endif
        member this.Remove(key: 'Key, _toMilli:int) =
            let ts = this.RemoveAsync(key)
            nonEmptyArrayResultBool ts _toMilli false 0 
            //Task.WaitAll(
            //    this.RemoveAsync(key) |> Array.map (fun t -> t.thisT)
            //    , _toMilli
            //)
        member this.Remove(key) =
            this.Remove(key, defaultTimeout)


        //好像沒意義，拿掉
        //member this.AddWithPersistenceAsync(key: 'Key, value: 'Value) =
        //    this.AddAsync(key, value)[0]

        //好像沒意義，拿掉
        //member this.AddWithPersistence(key: 'Key, value: 'Value, _toMilli:int) =
        //    this.AddWithPersistenceAsync(key, value).WaitAsync(_toMilli).Result

        // 获取 value，如果 ConcurrentSortedList 中不存在则从文件系统中读取
        member this.TryGetValue(key: 'Key, _toMilli:int) = //: bool * 'Value option =
            lock sortedList.LockObj (fun () ->
#if DEBUG1
                printfn "Query KV"
#endif
                let gr = sortedList.GetValue(SLK key)
                let (Choice1Of3 _) =
                    [|
                        gr.thisT
                        sortedListPersistenceStatus.Upsert(SLK key, SLPS Buffered).thisT
                    |]
                    |> Task.WaitAllWithTimeout _toMilli
                let exists, value = gr.Result.kvOptionValue

                if exists then
                    true, value
                else
                    match readFromFileBaseAndBuffer key with
                    | Some v ->
                        true, Some v
                    | None -> false, None
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
        member val _status  = sortedListStatus              with get
        member val _pstatus = sortedListPersistenceStatus   with get

        member this.Info = {|
            base_count = this._base._base.Keys    |> Seq.length
            idx_count  = this._idx._base.Keys     |> Seq.length
            idxR_count = this._idxR._base.Keys    |> Seq.length
            psts_count = this._pstatus._base.Keys |> Seq.length
        |}

        member this.InfoPrint =
            printfn "%A" this.Info

        new (maxDoP, basePath, schemaName, oFun, eFun) =
            PersistedConcurrentSortedList<'Key, 'Value>(
                maxDoP, basePath, schemaName
                , 30000, oFun, eFun)

