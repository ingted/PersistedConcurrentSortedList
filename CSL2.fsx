namespace PersistedConcurrentSortedList

 
module CSL2 =
    open System
    open System.Threading
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading.Tasks
    open System.Reflection
    open System.Runtime.InteropServices

    let createTask (fn: unit -> 'T) : Task<'T> =
        new Task<'T>(fun () -> fn())

    let inline createMemoryFromArr<'T> start (tLength: int) (arr: 'T []) : Memory<'T> =
        // 直接創建 Memory，指向指定的陣列區段
        Memory<'T>(arr, start, tLength)

    let 
#if RELEASE
        inline 
#endif
        getKeys (sl: SortedList<'k, _>) =
        let fi = sl.GetType().GetField("keys", BindingFlags.Instance ||| BindingFlags.NonPublic)
        fi.GetValue sl |> unbox<'k[]>

    let 
#if RELEASE
        inline 
#endif 
        getValues (sl: SortedList<_, 'v>) =
        let fi = sl.GetType().GetField("values", BindingFlags.Instance ||| BindingFlags.NonPublic)
        fi.GetValue sl |> unbox<'v[]>

    
    type SortedListCache<'k, 'v>() =
        // ConcurrentDictionary，用於緩存不同 SortedList 實例的 keys 和 values
        static let kCache = ConcurrentDictionary<SortedList<'k, 'v>, 'k[]>()
        static let vCache = ConcurrentDictionary<SortedList<'k, 'v>, 'v[]>()

        // cache 函數，將 SortedList 的 keys 和 values 緩存到 ConcurrentDictionary
        static member CacheChange(sl: SortedList<'k, 'v>) =
            kCache.AddOrUpdate(
                sl
                , (fun slInstance ->
                    let keys = getKeys slInstance
                    keys)
                , (fun slInstance _ ->
                    let keys = getKeys slInstance
                    keys)
            ) |> ignore
            
            vCache.AddOrUpdate(
                sl
                , (fun slInstance ->
                    let values = getValues slInstance
                    values)
                , (fun slInstance _ ->
                    let values = getValues slInstance
                    values)
            ) |> ignore

        // 從緩存中獲取 keys，如果不存在則拋出錯誤
        static member GetKeysCached(sl: SortedList<'k, 'v>) =
            match kCache.TryGetValue(sl) with
            | true, keys -> keys
            | _ -> failwith "Keys have not been cached for this instance. Call Cache() first."

        // 從緩存中獲取 values，如果不存在則拋出錯誤
        static member GetValuesCached(sl: SortedList<'k, 'v>) =
            match vCache.TryGetValue(sl) with
            | true, values -> values
            | _ -> failwith "Values have not been cached for this instance. Call Cache() first."
        static member KCache = kCache
        static member VCache = vCache


    
    type Layer = int
    type Tag = string

    type KVOpFun<'Key, 'Value when 'Key : comparison> = Memory<'Key> -> Memory<'Value> -> int (*已知 memory 總長*) -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>

    and KOpFun<'Key, 'Value when 'Key : comparison> = Memory<'Key> -> int (*已知 memory 總長*) -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>

    and VOpFun<'Key, 'Value when 'Key : comparison> = Memory<'Value> -> int (*已知 memory 總長*) -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>

    and Op<'Key, 'Value when 'Key : comparison> =
    | CAdd of 'Key * 'Value
    | CAdd2 of 'Key * ('Key -> 'Value)
    | CAddKVs of ('Key * 'Value) seq
    | CRemove of 'Key
    | CRemoveKeys of 'Key seq
    | CRemoveKeys2 of (unit -> 'Key seq)
    | CUpdate of 'Key * 'Value
    | CUpdate2 of 'Key * ('Key -> 'Value -> 'Value)
    | CUpdateKVs of ('Key * 'Value) seq
    | CUpdateKVs2 of 'Key seq * ('Key -> 'Value -> 'Value)
    | CUpsert of 'Key * 'Value
    | CUpsert2 of 'Key * ('Key -> 'Value option -> 'Value)
    | CUpsertKVs of ('Key * 'Value) seq
    | CUpsertKVs2 of 'Key seq * ('Key -> 'Value option -> 'Value)
    | CGet of 'Key
    | CGetByKeys of 'Key seq
    | CContains of 'Key
    | CCount
    | CValues
    | CKeys
    | CKVs 
    | CClean
    | IgnoreQ of Op<'Key, 'Value>
    | SeqOp of Op<'Key, 'Value> seq
    | FoldOp of Op<'Key, 'Value> seq
    | KeysOp of Tag option * int * int * KOpFun<'Key, 'Value>
    | ValuesOp of Tag option * int * int * VOpFun<'Key, 'Value>
    | KeyValuesOp of Tag option * int * int * KVOpFun<'Key, 'Value>

    and KVMode =
    | KeyOnlyMode
    | ValueOnlyMode
    | KVMode
    //type OpResultTyp = 
    //| TUnit         
    //| TBool         of defaultValue:bool
    //| TOptionValue  of defaultValue:bool
    //| TInt          of defaultValue:int
    //| TKeyList     
    //| TValueList   
    
    and ResultWrapper<'Key, 'Value when 'Key : comparison>(layer: Layer, ?tag: Tag, ?kMemory: Memory<'Key>, ?vMemory: Memory<'Value>, ?opResult: Task<OpResult<'Key, 'Value>>) = 
        // 屬性初始化
        member val Layer = layer with get, set
        member val Tag = tag with get, set
        member val OpResult = opResult with get, set
        member val KMemory = kMemory with get, set
        member val VMemory = vMemory with get, set

        // 預設建構子
        new(layer: Layer) = ResultWrapper(layer)

        // 提供不同參數組合的建構子
        new(layer: Layer, tag: Tag, opResult: OpResult<'Key, 'Value>) = ResultWrapper(layer, tag, opResult = Task.FromResult opResult)
        new(layer: Layer, opResult: OpResult<'Key, 'Value>) = ResultWrapper(layer, opResult = Task.FromResult opResult)

        new(layer: Layer, tag: Tag, vmemory: Memory<'Value>) = ResultWrapper(layer, tag, vMemory = vmemory)
        new(layer: Layer, tag: Tag, kmemory: Memory<'Key>) = ResultWrapper(layer, tag, kMemory = kmemory)

        new(layer: Layer, kmemory: Memory<'Key>) = ResultWrapper(layer, kMemory = kmemory)
        new(layer: Layer, vmemory: Memory<'Value>) = ResultWrapper(layer, vMemory = vmemory)
        new(layer: Layer, kmemory: Memory<'Key>, vmemory: Memory<'Value>) = ResultWrapper(layer, kMemory = kmemory, vMemory = vmemory)
        new(layer: Layer, tag: Tag, kmemory: Memory<'Key>, vmemory: Memory<'Value>) = ResultWrapper(layer, tag, kMemory = kmemory, vMemory = vmemory)


    and OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit
    | CBool         of bool
    | COptionValue  of (bool * 'Value option)
    | CInt          of int
    | CDecimal      of decimal
    | CKeyList      of IList<'Key>
    | CValueList    of IList<'Value>
    | CKVList       of ('Key * 'Value)[]
    | CKVOptList       of ('Key * 'Value option)[]
    | FoldResult    of //(Layer * Tag option * OpResult<'Key, 'Value> option * Memory<'Value> option)[]
        ResultWrapper<'Key, 'Value>[]
        with
       
            member this.Bool =
                match this with
                | CBool v -> Some v
                | _ -> None
        
            member this.OptionValue =
                match this with
                | COptionValue (b, v) -> Some (b, v)
                | _ -> None
        
            member this.Decimal =
                match this with
                | CDecimal v -> Some v
                | _ -> None

            member this.Int =
                match this with
                | CInt v -> Some v
                | _ -> None
        
            member this.KeyList =
                match this with
                | CKeyList keyList -> Some keyList
                | _ -> None
        
            member this.ValueList =
                match this with
                | CValueList valueList -> Some valueList
                | _ -> None

            member this.KVList =
                match this with
                | CKVList valueList -> Some valueList
                | _ -> None

            member this.KVOptList =
                match this with
                | CKVOptList valueList -> Some valueList
                | _ -> None
            
            member this.FoldResultRWArr =
                match this with
                | FoldResult rwArr -> Some rwArr
                | _ -> None

            member this.FoldResultRWArrN(n) =
                match this with
                | FoldResult rwArr -> rwArr[n].OpResult |> Option.map (fun wrT -> wrT.Result)
                | _ -> None

            member this.FoldResultRWArrNKey(n) = this.FoldResultRWArrN(n) |> Option.bind (fun opRst -> opRst.KeyList)
            member this.FoldResultRWArrNValue(n) = this.FoldResultRWArrN(n) |> Option.bind (fun opRst -> opRst.ValueList)
            member this.FoldResultRWArrNKV(n) = this.FoldResultRWArrN(n) |> Option.bind (fun opRst -> opRst.KVList)

            member this.FoldResultRWArr0Key = this.FoldResultRWArrN(0).Value.KeyList
            member this.FoldResultRWArr0Value = this.FoldResultRWArrN(0).Value.ValueList
            member this.FoldResultRWArr0KV = this.FoldResultRWArrN(0).Value.KVList

    type Task<'T> with
        member this.WaitAsync(_toMilli:int) = 
#if NET9_0
               
            // .NET 9 specific implementation
            this.WaitAsync(TimeSpan.FromMilliseconds _toMilli)
#else
            // .NET Standard 2.0 specific implementation
               
            Task.Run(fun () -> 
                if this.Wait(_toMilli) then 
                    this.Result
                else
                    raise (TimeoutException("Task timed out"))
            )
            
#endif
        member this.Ignore () = ()
        member this.thisT = this :> Task
        member this.WaitIgnore = this.Wait()
       

    let rec getTypeName (t: Type) =
        if t.IsGenericType then
            let genericArgs = t.GetGenericArguments() |> Array.map getTypeName |> String.concat ", "
            sprintf "%s<%s>" (t.Name.Substring(0, t.Name.IndexOf('`'))) genericArgs
        else
            t.Name

    
    type SLId = string

    type FActor<'T> = MailboxProcessor<'T>


    type QueueProcessorCmd<'Key, 'Value when 'Key : comparison> =
    | Tsk of Task<OpResult<'Key, 'Value>> * Guid option
    | Lock of AsyncReplyChannel<Guid> * TimeSpan option  // 加入 timeout 參數
    | Unlock of Guid
    | SysLock
    | SysUnlock //of Guid
    | StepForward

    type QueueProcessorStatus =
    | Locked of Guid
    | Listenning

    type QueueProcessor<'T, 'Key, 'Value when 'Key : comparison>(slId: 'T, _receiveTimeoutDefault) =

        // 定義一個 MailboxProcessor 來處理操作任務
        let opProcessor = FActor.Start(fun (inbox:FActor<QueueProcessorCmd<'Key, 'Value>>) ->
            let mutable messageQueue = Queue<QueueProcessorCmd<'Key, 'Value>>()
            let mutable messageQueueTmp = Queue<QueueProcessorCmd<'Key, 'Value>>() //Unchecked.defaultof<Queue<QueueProcessorCmd<'OpResult>>>
            let mutable status = Listenning
            let mutable _receiveTimeout = _receiveTimeoutDefault
            //let mutable curLockGuid = Guid.Empty
            let rec loop () =
                async {
                    let! taskOpt = inbox.TryReceive(_receiveTimeout)
                    if taskOpt.IsSome then
                        //let mutable ifSwitched = false
    #if DEBUG
                        printfn "[%A] dequeued %A, messageQueue count: %d" slId task messageQueue.Count
                        //printfn "Cmd received: %A" taskOpt.Value
    #endif
                    //| Some qpCmd when status = Listenning ->
                        //let rec getCmdAndProceed (latestCmdToAppendOpt: _ option) =
                        //    let ifD, m = messageQueue.TryDequeue () 
                        //    if ifD then
                        //        if latestCmdToAppendOpt.IsSome then
                        //            messageQueue.Enqueue latestCmdToAppendOpt.Value
                        //        procCmd m
                        //    else
                        //        if latestCmdToAppendOpt.IsSome then
                        //            procCmd latestCmdToAppendOpt.Value

                        let rec getCmdAndProceed () =
#if NET9_0
                            let ifD, m = messageQueue.TryDequeue () 
                            if ifD then
                                procCmd m
#else
                            if messageQueue.Count > 0 then                                
                                let m = messageQueue.Dequeue () 
                                procCmd m
#endif

                        and getCmdAndProceedBeforeLatestCmdProceed latestCmdToAppend =
#if NET9_0
                            let ifD, m = messageQueue.TryDequeue () 
                            if ifD then
#if DEBUG
                                printfn "Enqueue latestCmd first: %A" latestCmdToAppend
#endif
                                messageQueue.Enqueue latestCmdToAppend
                                procCmd m
                                
                            else
                                procCmd latestCmdToAppend
#else
                            if messageQueue.Count > 0 then
                                let m = messageQueue.Dequeue ()
                                printfn "Enqueue latestCmd first: %A" latestCmdToAppend
                                messageQueue.Enqueue latestCmdToAppend
                                procCmd m
                            else
                                procCmd latestCmdToAppend
#endif
                        
                        and goAhead _status =
                            status <- _status
                            getCmdAndProceed ()

                        and execute (task:Task<'OpResult>) =
                            task.Start ()
#if DEBUG
                            printfn "Task %A started" task.Id
#endif
                            let tr = task |> Async.AwaitTask |> Async.Catch |> Async.StartAsTask // 同步執行任務

                            match tr.Result with
                            | Choice1Of2 s -> 
#if DEBUG
                                printfn "Task %A IsCompleted: %A" task.Id task.IsCompleted
#endif
#if DEBUG1
                                printfn "Successfully: %A" s
#endif
                                ()
                            | Choice2Of2 exn -> 
                                printfn "Task %A IsCompleted: %A" task.Id task.IsCompleted
                                printfn "Failed: %A" exn.Message

                                unLockAndTrySkipCmdWithLockId ()
                        //and unLock () =
                        //    match status with
                        //    | Locked curLockGuid ->
                        //        messageQueue <- new Queue<QueueProcessorCmd<'OpResult>>(Seq.concat [messageQueueTmp; messageQueue]|>Seq.toArray)
                        //        messageQueueTmp <- new Queue<QueueProcessorCmd<'OpResult>>()
                        //        goAhead Listenning
                        //    | _ -> 
                        //        ()
                        
                        
                        and unLockAndTrySkipCmdWithLockId () =
                            match status with
                            | Locked curLockGuid ->
                                messageQueue <- new Queue<QueueProcessorCmd<'Key, 'Value>>(Seq.concat [seq messageQueueTmp; messageQueue |> Seq.choose (fun cmd -> match cmd with | Tsk (task, (Some g)) when g = curLockGuid -> None | _ -> Some cmd )]|>Seq.toArray)
                                messageQueueTmp <- new Queue<QueueProcessorCmd<'Key, 'Value>>()
                                goAhead Listenning
                            | _ ->
                                getCmdAndProceed ()
                                

                        and procCmd cmd =
#if DEBUGVV
                            printfn "Cmd proceeding: %A" cmd
#endif
                            match status with
                            | Locked curLockGuid ->
#if DEBUG
                                printfn "Current lock context: %A" curLockGuid
#endif
                                match cmd with
                                | SysUnlock ->
#if DEBUG
                                    printfn "Sys Unlocked!"
#endif
                                    unLockAndTrySkipCmdWithLockId ()

                                | Unlock g when g = curLockGuid -> 
#if DEBUG
                                    printfn "Unlocked! context:%A" g
#endif
                                    unLockAndTrySkipCmdWithLockId ()

                                | Tsk (task, (Some g)) when g = curLockGuid ->
#if DEBUG
                                    printfn "Execute task in same lockId context! context:%A, locker:%A" curLockGuid g
#endif
                                    execute task
                                    getCmdAndProceed ()
//                                | _ when ifSwitched = false ->
//#if DEBUG
//                                    printfn "Enqueue task in different context firstTime! context:%A" curLockGuid
//#endif
//                                    ifSwitched <- true
//                                    //messageQueueTmp <- new Queue<QueueProcessorCmd<'OpResult>>(messageQueue)
                                    
//                                    messageQueueTmp.Enqueue cmd
//                                    getCmdAndProceed ()
                                | _ ->
#if DEBUG
                                    printfn "Enqueue task in different context! context:%A, cmd: %A" curLockGuid cmd
#endif
                                    messageQueueTmp.Enqueue cmd
                                    getCmdAndProceed ()
                            | Listenning ->
#if DEBUG
                                printfn "Listenning, no lock context."
#endif
                                match cmd with
                                | Tsk (_, (Some g)) ->
                                    execute (task {
                                        return (failwithf "Invalid lock %A" g)
                                    })
                                    getCmdAndProceed ()
                                | Tsk (task, None) ->
                                    execute task
                                    getCmdAndProceed ()

                                | Lock (replyLockId, timeoutTimeSpanOpt) -> // 處理 lock 並加入 timeout
                                    let g = Guid.NewGuid()
                                    //status <- Locked g
#if DEBUG
                                    printfn "Lock received: Locked with %A" g
#endif
                                    replyLockId.Reply g

                                    // 啟動 timeout 計時器，如果超時則自動 unlock
                                    if timeoutTimeSpanOpt.IsSome then
                                        async {
                                            do! Async.Sleep (int timeoutTimeSpanOpt.Value.TotalMilliseconds)
                                            inbox.Post (SysUnlock)
                                        }
                                        |> Async.Start
                                    //getCmdAndProceed ()
                                    goAhead (Locked g)
                                | SysUnlock -> 
                                    printfn "CSL not in locked state!"
                                | _ ->
                                    printfn "Cmd handler haven't yet implemented or ignored! %A" cmd
                                    getCmdAndProceed ()
                        //if taskOpt.Value = StepForward then
                        //    getCmdAndProceed ()
                        //else
                        getCmdAndProceedBeforeLatestCmdProceed taskOpt.Value

//                        if ifSwitched then
//#if DEBUG
//                            printfn "queue switched"
//#endif
//                            messageQueue <- messageQueueTmp
//                            messageQueueTmp <- Queue<QueueProcessorCmd<'OpResult>>()
                        //if messageQueue.Count > 0 then //&& status = Listenning then //20241204 這樣好像會卡住不動，改反向條件? (還沒改)
                        //    //if locked and queue not empty means we need to wait unlock, so no need to StepForward
                        //    inbox.Post StepForward
                    else
                        ()
                    return! loop() // 繼續處理下一個任務

                }
            loop()
        )

        // 提供給外部調用來向 MailboxProcessor 投遞任務
        member this.Enqueue (task: Task<'OpResult>) =
            opProcessor.Post (Tsk (task, None))

        member this.EnqueueWithLock (task: Task<'OpResult>, lockId) =
            opProcessor.Post (Tsk (task, Some lockId))

        // ID 屬性
        member this.Id = slId

        member this.RequireLock (requireTimeoutOption:int option, (lockTimeoutOption: float option)) =
            let arcFun = 
                fun arc ->
                    if lockTimeoutOption.IsSome then
                        Lock (arc, Some <| TimeSpan.FromMilliseconds lockTimeoutOption.Value)
                    else
                        Lock (arc, None)
            if requireTimeoutOption.IsSome then
                opProcessor.PostAndTryAsyncReply (arcFun, requireTimeoutOption.Value)
            else
                opProcessor.PostAndTryAsyncReply arcFun

        member this.UnLock (lockId) =
            opProcessor.Post (Unlock lockId)

        member this.SysUnLock () =
            opProcessor.Post (SysUnlock)


    let ifLessThan0Then0 v = if v < 0 then 0 else v
    let ifDiffLessThan0Then0 v minus = if v - minus < 0 then 0, v else v, minus 

    type ConcurrentSortedList<'Key, 'Value, 'ID
        when 'Key : comparison
        and 'Value: comparison
        
        >(
        slId:'ID
        , lockObj: obj
        , timeoutDefault
        , ?autoCacheChangeOpt:int 
        , ?sortedListOpt:SortedList<'Key, 'Value>
        ) =
        let sortedList = 
            if sortedListOpt.IsNone then
                SortedList<'Key, 'Value>()
            else
                sortedListOpt.Value
        //let lockObj = obj()
        let opQueue = QueueProcessor<'ID, 'Key, 'Value>(slId, timeoutDefault)

        let rwLock = new ReaderWriterLockSlim()

        member val autoCacheChange = autoCacheChangeOpt with get, set
        /////寫入
        //member val mreWriteOpt: ManualResetEventSlim option = None with get, set
        /////讀取
        //member val mreReadOpt: ManualResetEventSlim option = None with get, set

        member this.Id = slId
        member this.LockObj = lockObj
        member this.OpQueue = opQueue
        /// 基础的 Add 操作（直接修改内部数据结构）
        member this.CacheChange () =
            SortedListCache<_, _>.CacheChange sortedList
        member this.TryAddBase(key: 'Key, value: 'Value) =
            
            rwLock.EnterWriteLock()
            try
                //if this.mreReadOpt.IsSome then
                //    this.mreReadOpt.Value.Wait()
                //let added = 
                //    if this.mreWriteOpt.IsSome then
                //        this.mreWriteOpt.Value.Reset ()
                //        let a = sortedList.TryAdd(key, value)
                //        this.mreWriteOpt.Value.Set ()
                //        a
                //    else
                //        sortedList.TryAdd(key, value)
#if NET9_0
                let added = sortedList.TryAdd(key, value)
#else
                let added = 
                    try
                        sortedList.Add(key, value)
                        true
                    with
                    | _ -> 
                        false
#endif

                if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                    SortedListCache<_, _>.CacheChange sortedList
#if DEBUG1
                printfn "[%A] %A, %A added" slId key value
#endif
                //rwLock.ExitWriteLock()
                added
            //with
            //| _ -> 
            //    rwLock.ExitWriteLock()
            //    reraise()
            finally    
                rwLock.ExitWriteLock()

        member this.TryAddBase(kv: ('Key * 'Value) seq) =
            
            rwLock.EnterWriteLock()
            try
                let ks = kv |> Seq.map fst |> Seq.toArray
                let mutable addedKey: 'Key = Unchecked.defaultof<'Key>
                if ks |> Array.exists (fun k -> 
                    addedKey <- k
                    sortedList.ContainsKey k) then
                    failwithf "Key already exists in the list: %A" addedKey
                kv
                |> Seq.iter (fun (k, v) -> sortedList.Add(k, v))

                if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                    SortedListCache<_, _>.CacheChange sortedList
#if DEBUG1
                printfn "[%A] %A, %A added" slId key value
#endif
                //rwLock.ExitWriteLock()
                true
            finally
                rwLock.ExitWriteLock()
            //with
            //| exn -> 
            //    printfn "Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()

        /// 基础的 Remove 操作
        member this.TryRemoveBase(key: 'Key) =
            rwLock.EnterWriteLock()
            try
                let removed = sortedList.Remove(key)
                if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                    SortedListCache<_, _>.CacheChange sortedList
                //rwLock.ExitWriteLock()
                removed
            //with
            //| _ -> 
            //    rwLock.ExitWriteLock()
            //    reraise()
            finally
                rwLock.ExitWriteLock()

        member this.TryRemoveBase(keys: 'Key seq) =
            rwLock.EnterWriteLock()
            try
                let ks = keys |> Seq.toArray
                let mutable removedKey: 'Key = Unchecked.defaultof<'Key>
                if ks |> Array.exists (fun k -> 
                    removedKey <- k
                    not <| sortedList.ContainsKey k) then
                    failwithf "Key not exists in the list: %A" removedKey
                ks
                |> Array.iter (fun k -> sortedList.Remove(k) |> ignore )
                if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                    SortedListCache<_, _>.CacheChange sortedList
                //rwLock.ExitWriteLock()
                true
            //with
            //| exn -> 
            //    printfn "TryRemoveBase keys Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()
            finally
                rwLock.ExitWriteLock()

        /// 尝试更新，如果键存在则更新值
        member this.TryUpdateBase(key: 'Key, newValue: 'Value) : bool =
            rwLock.EnterWriteLock()
            try
                if sortedList.ContainsKey(key) then
                    sortedList.[key] <- newValue
                    //rwLock.ExitWriteLock()
                    true
                else
                    //rwLock.ExitWriteLock()
                    false
            finally 
                rwLock.ExitWriteLock()
            //with
            //| exn ->
            //    printfn "TryUpdateBase k v Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()

        member this.TryUpdateBase(kv: ('Key * 'Value) seq) : bool =
            rwLock.EnterWriteLock()
            try
                let mutable uk = Unchecked.defaultof<'Key>
                if kv 
                    |> Seq.exists (fun (k, _) -> 
                            uk <- k
                            not <| sortedList.ContainsKey k) then
                    printfn "TryUpdateBase kv failed: %A not exists." uk
                    //rwLock.ExitWriteLock()
                    false
                else
                    kv |> Seq.iter (fun (k, v) -> sortedList.[k] <- v)
                    //rwLock.ExitWriteLock()
                    true
            //with
            //| exn -> 
            //    printfn "TryUpdateBase kv seq Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()
            finally
                rwLock.ExitWriteLock()

        member this.TryUpdateBase2(key: 'Key, newValueFactory: 'Key -> 'Value -> 'Value) : bool =
            rwLock.EnterWriteLock()
            try
                match sortedList.TryGetValue key with
                | true, v ->
                    sortedList.[key] <- newValueFactory key v
                    //rwLock.ExitWriteLock()
                    true
                | false, _ ->
                    //rwLock.ExitWriteLock()rwLock.ExitWriteLock()
                    false
            finally
                rwLock.ExitWriteLock()
            //with
            //| exn -> 
            //    printfn "TryUpdateBase2 k f Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()

        member this.TryUpdateBase2(keys: 'Key seq, newValueFactory: 'Key -> 'Value -> 'Value) : bool =
            rwLock.EnterWriteLock()
            let mutable uk = Unchecked.defaultof<'Key>
            try
                if keys 
                    |> Seq.exists (fun k -> 
                            uk <- k
                            not <| sortedList.ContainsKey k) then
                    printfn "TryUpdateBase2 kv failed: %A not exists." uk
                    //rwLock.ExitWriteLock()
                    false
                else
                    keys 
                    |> Seq.iter (fun k -> 
                        let v = sortedList.[k]
                        sortedList.[k] <- newValueFactory k v
                        )
                    //rwLock.ExitWriteLock()
                    true
            finally
                rwLock.ExitWriteLock()

        member this.TryUpsertBase(key: 'Key, newValue: 'Value) =
            rwLock.EnterWriteLock()
            try
                if sortedList.ContainsKey(key) then
                    sortedList.[key] <- newValue
                    //rwLock.ExitWriteLock()
                    2
                else
#if NET9_0
                    if sortedList.TryAdd (key, newValue) then
                        if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                            SortedListCache<_, _>.CacheChange sortedList
                        1
                    else
                        0
#else
                    try
                        sortedList.Add(key, newValue)
                    
                        if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                            SortedListCache<_, _>.CacheChange sortedList
                        1
                    with
                    | _ ->
                        0
#endif
                
            //with
            //| exn -> 
            //    printfn "TryUpsertBase k v Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()
            finally
                rwLock.ExitWriteLock()

        member this.TryUpsertBase(kv: ('Key * 'Value) seq) =
            rwLock.EnterWriteLock()
            try
                kv |> Seq.iter (fun (key, newValue) ->
                    if sortedList.ContainsKey(key) then
                        sortedList.[key] <- newValue
                    else
                        sortedList.Add(key, newValue) |> ignore
                )
                if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                    SortedListCache<_, _>.CacheChange sortedList
                //rwLock.ExitWriteLock()
                true
            finally 
                rwLock.ExitWriteLock()
            //with
            //| exn -> 
            //    printfn "TryUpsertBase kv seq Error: %A" exn.Message
            //    rwLock.ExitWriteLock()
            //    reraise()

        member this.TryUpsertBase2(key: 'Key, newValueFactory: 'Key -> 'Value option -> 'Value) =
            rwLock.EnterWriteLock()
            try
                match sortedList.TryGetValue key with
                | true, v ->
                    sortedList.[key] <- newValueFactory key (Some v)
                    //rwLock.ExitWriteLock()
                    2
                | _ ->
#if NET9_0
                    if sortedList.TryAdd (key, newValueFactory key None) then
                        if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                            SortedListCache<_, _>.CacheChange sortedList
                        //rwLock.ExitWriteLock()
                        1
                    else
                        //rwLock.ExitWriteLock()
                        0
#else

                    try 
                        sortedList.Add (key, newValueFactory key None)
                        if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                            SortedListCache<_, _>.CacheChange sortedList
                        rwLock.ExitWriteLock()
                        1
                    with
                    | _ ->
                        rwLock.ExitWriteLock()
                        0
#endif
            finally
                rwLock.ExitWriteLock()

        member this.TryUpsertBase2(keys: 'Key seq, newValueFactory: 'Key -> 'Value option -> 'Value) =
            rwLock.EnterWriteLock()
            try
                try
                    keys |> Seq.iter (fun key ->
                        match sortedList.TryGetValue key with
                        | true, v ->
                            sortedList.[key] <- newValueFactory key (Some v)
                        | false, _ ->
                            sortedList.Add(key, newValueFactory key None) |> ignore
                    )
                    if this.autoCacheChange.IsSome && this.autoCacheChange.Value <> 0 then
                        SortedListCache<_, _>.CacheChange sortedList
                    //rwLock.ExitWriteLock()
                    true
                with
                | exn -> 
                    printfn "TryUpsertBase2 keys f Error: %A" exn.Message
                    //rwLock.ExitWriteLock()
                    false
            finally
                rwLock.ExitWriteLock()

        member this.TryGetValuesBase(keys: 'Key seq) =
            rwLock.EnterReadLock()
            try
                let rst =
                    keys
                    |> Seq.map (fun key ->                    
                        match sortedList.TryGetValue key with
                        | true, v -> 
                            key, Some v
                        | _ -> 
                            //this.mreReadOpt.Value.Set ()
                            key, None
                    )
                    |> Seq.toArray
                //rwLock.ExitReadLock()
                rst
            //with
            //| exn ->
            //    printfn "TryGetValueBase k Error: %A" exn.Message
            //    rwLock.ExitReadLock()
            //    reraise()
            finally
                rwLock.ExitReadLock()

        member this.TryGetValueBase(key: 'Key) : bool * 'Value option =
            //if this.mreReadOpt.IsSome then
            //    this.mreReadOpt.Value.Reset ()
            //this.mreWriteOpt.Value.Wait ()
            rwLock.EnterReadLock()
            try
                match sortedList.TryGetValue key with
                | true, v -> 
                    //this.mreReadOpt.Value.Set ()
                    //rwLock.ExitReadLock()
                    true, Some v
                | _ -> 
                    //this.mreReadOpt.Value.Set ()
                    //rwLock.ExitReadLock()
                    false, None
            //with
            //| exn ->
            //    printfn "TryGetValueBase k Error: %A" exn.Message
            //    //rwLock.ExitReadLock()
            //    reraise()
            finally
                rwLock.ExitReadLock()

        member this.TryGetValueBaseOpt(keys: 'Key seq) : ('Key * 'Value option) seq =
            rwLock.EnterReadLock()
            try
                let mutable found = false
                let results = 
                    keys 
                    |> Seq.map (fun key ->
                        match sortedList.TryGetValue key with
                        | true, v -> 
                            key, Some v
                        | _ -> 
                            key, None
                    )
                //rwLock.ExitReadLock()
                results
            //with
            //| exn ->
            //    printfn "TryGetValueBaseOpt keys kvOpt seq Error: %A" exn.Message
            //    rwLock.ExitReadLock()
            //    reraise()
            finally
                rwLock.ExitReadLock()

        member this.TryGetValueBase(keys: 'Key seq) : ('Key * 'Value) seq =
            rwLock.EnterReadLock()
            try
                let mutable found = false
                let results = 
                    keys 
                    |> Seq.choose (fun key ->
                        match sortedList.TryGetValue key with
                        | true, v -> 
                            Some (key, v)
                        | _ -> 
                            None
                    )
                //rwLock.ExitReadLock()
                results
            //with
            //| exn ->
            //    printfn "TryGetValueBase keys kv seq Error: %A" exn.Message
            //    rwLock.ExitReadLock()
            //    reraise()
            finally
                rwLock.ExitReadLock()

        /// 封装操作并添加到任务队列，执行后返回 Task
        member this.LockableOps (_op: Op<'Key, 'Value>, lockIdOpt: Guid option) =
            // `processFoldOp` 函數實現，接收一個 `Op<'Key, 'Value> seq` 序列並返回一個 `FoldResult`
            let rec processOp layer arr (preResultTask: Task<OpResult<_,_>> option) (op: Op<'Key, 'Value>) (baseInstance: ConcurrentSortedList<'Key, 'Value, 'ID>) =
                let results =
                    match op with
                    | KeysOp (tagOpt, start_, length_, f) ->
                        let kc = SortedListCache<'Key, 'Value>.GetKeysCached(baseInstance._base)
                        let start, length = 
                            let s = if start_ < 0 then kc.Length + start_ else start_
                            if length_ >= 0 then
                                s, length_
                            else
                                ifDiffLessThan0Then0 s -length_
                        let keysMemory, memLength = 
                            if length = 0 then
                                let l = kc.Length - start
                                (kc |> createMemoryFromArr<'Key> start l), l
                            else 
                                (kc |> createMemoryFromArr<'Key> start length), length

                                
                        let curResult = task {return f keysMemory memLength arr preResultTask}
                        if tagOpt.IsSome then
                            ResultWrapper(layer, tagOpt.Value, kMemory=keysMemory, opResult = curResult), curResult
                        else
                            ResultWrapper(layer, opResult = curResult), curResult

                    | ValuesOp (tagOpt, start_, length_, f) ->
                        // 從緩存中取得 values 並創建 Memory
                        let vc = SortedListCache<'Key, 'Value>.GetValuesCached(baseInstance._base)
                        let start, length = 
                            let s = if start_ < 0 then vc.Length + start_ else start_
                            if length_ >= 0 then
                                s, length_
                            else
                                ifDiffLessThan0Then0 s -length_
                        let valuesMemory, memLength = 
                            if length = 0 then
                                let l = vc.Length - start
                                (vc |> createMemoryFromArr<'Value> start l), l
                            else
                                (vc |> createMemoryFromArr<'Value> start length), length
                        let curResult = task {return f valuesMemory memLength arr preResultTask}
                        if tagOpt.IsSome then
                            ResultWrapper(layer, tagOpt.Value, vMemory = valuesMemory, opResult = curResult), curResult
                        else
                            ResultWrapper(layer, vMemory = valuesMemory, opResult = curResult), curResult

                    ///start 給定負數可從陣列尾端往前幾個開始往後抓 length 個 或往前抓 -length 個(length 為負表往前，超過則從0開始抓)
                    ///如果 start 負數絕對值超過長度，則會錯，不會從0開始抓
                    | KeyValuesOp (tagOpt, start_, length_, f) ->
                        // 從緩存中取得 keys 和 values 並創建 Memory
                        let kc = SortedListCache<'Key, 'Value>.GetKeysCached(baseInstance._base)
                        let vc = SortedListCache<'Key, 'Value>.GetValuesCached(baseInstance._base)
                        let start, length = 
                            let s = if start_ < 0 then kc.Length + start_ else start_
                            if length_ >= 0 then
                                s, length_
                            else
                                ifDiffLessThan0Then0 s -length_
                        let keysMemory, valuesMemory, memLength = 
                            if length = 0 then
                                let l = kc.Length - start
                                let keysMemory = kc |> createMemoryFromArr<'Key> start l
                                let valuesMemory = vc |> createMemoryFromArr<'Value> start l
                                keysMemory, valuesMemory, l
                            else
                                let keysMemory = kc |> createMemoryFromArr<'Key> start length
                                let valuesMemory = vc |> createMemoryFromArr<'Value> start length
                                keysMemory, valuesMemory, length 
                        let curResult = task {return f keysMemory valuesMemory memLength arr preResultTask}
                        if tagOpt.IsSome then
                            ResultWrapper(layer, tagOpt.Value, kMemory = keysMemory, vMemory = valuesMemory, opResult = curResult), curResult
                        else
                            ResultWrapper(layer, kMemory = keysMemory, vMemory = valuesMemory, opResult = curResult), curResult

                    | SeqOp s ->
                        failwith "SeqOp is not supported in FoldOp"
                    | _ -> //failwith "Unsupported operation type for processOp"
                        let f = opToFun op |> Seq.item 0
                        let oprt = task {return f()}
                        ResultWrapper(layer, opResult = oprt), oprt

                results
            and opToFun op : (unit -> OpResult<'Key, 'Value>) seq = 
#if DEBUG1
                printfn "%A Current Op: %A, %s, %s" slId op (getTypeName typeof<'Key>) (getTypeName typeof<'Value>)
#endif
                match op with
                | KeysOp (_, _, _, _)
                | ValuesOp (_, _, _, _)
                | KeyValuesOp (_, _, _, _) ->
                    seq [ fun () -> FoldResult [| fst <| processOp 0 [||] None op this |] ]
                | FoldOp s ->
                    let f2opr =
                        fun () ->
                            // 以初始 `ResultWrapper` 集合為空的方式進行 fold 操作
                            let initialResultArray = Array.empty<ResultWrapper<'Key, 'Value>>
    
                            // 使用 `Seq.fold` 來累加每個操作的結果到 `ResultWrapper` 陣列中
                            let foldedResults, _ =
                                s 
                                |> Seq.indexed
                                |> Seq.fold (fun (acc, preResultTask) (layer, op) ->
                                    // 每個 `op` 使用 `processOp` 進行處理
                                    let result, opROpt = processOp layer acc preResultTask op this
                                    // 將結果累加到 `acc` 中，擴充 `ResultWrapper` 陣列
                                    (Array.append acc [| result |], Some opROpt)
                                ) (initialResultArray, None)

                            // 最後將所有結果封裝為 `FoldResult`
                            FoldResult foldedResults
                    seq [f2opr]
                | SeqOp s ->
                    s
                    |> Seq.collect (fun o ->
                        opToFun o
                    )
                | _ ->
                    match op with
                    | CAdd (k, v) ->
                        fun () ->
                            this.TryAddBase(k, v) |> CBool
                    | CAddKVs kvs ->
                        fun () ->
                            this.TryAddBase kvs |> CBool
                    | CAdd2 (k, f) ->
                        fun () ->
                            this.TryAddBase(k, f k) |> CBool
                    | CRemove k ->
                        fun () ->
                            this.TryRemoveBase(k) |> CBool
                    | CRemoveKeys ks ->
                        fun () ->
                            this.TryRemoveBase(ks) |> CBool
                            
                    | CRemoveKeys2 ksFun ->
                        fun () ->
                            this.TryRemoveBase(ksFun ()) |> CBool

                    | CUpdate (k, v) ->
                        fun () ->
                            this.TryUpdateBase(k, v) |> CBool
                    | CUpdateKVs (kvs) ->
                        fun () ->
                            this.TryUpdateBase(kvs) |> CBool

                    | CUpdate2 (k, f) ->
                        fun () ->
                            this.TryUpdateBase2(k, f) |> CBool
                            
                    | CUpdateKVs2 (ks, f) ->
                        fun () ->
                            this.TryUpdateBase2(ks, f) |> CBool
                            

                    | CUpsert (k, v) ->
                        fun () ->
                            this.TryUpsertBase(k, v) |> CInt
                            
                    | CUpsertKVs (kvs) ->
                        fun () ->
                            this.TryUpsertBase(kvs) |> CBool
                    | CUpsert2 (k, f) ->
                        fun () ->
                            this.TryUpsertBase2(k, f) |> CInt
                    | CUpsertKVs2 (ks, f) ->
                        fun () ->
                            this.TryUpsertBase2(ks, f) |> CBool
                    | CGetByKeys ks ->
                        fun () ->
                            this.TryGetValuesBase(ks) |> CKVOptList
                        
                    | CGet k ->
                        fun () ->
                            this.TryGetValueBase(k) |> COptionValue
                    | CCount ->
                        fun () -> 
                            sortedList.Count |> CInt
                    | CValues ->
                        fun () -> 
                            sortedList.Values |> CValueList
                    | CKeys ->
                        fun () -> 
                            sortedList.Keys |> CKeyList
                    | CKVs ->
                        fun () -> 
                            sortedList |> Seq.map (fun kvp -> kvp.Key, kvp.Value) |> Seq.toArray |> CKVList
                    | CClean ->
                        fun () ->
                            sortedList.Clear()
                            CUnit
                    | CContains k ->
                        fun () ->
                            sortedList.ContainsKey k |> CBool
                
                    |> fun f -> seq [f]
#if DEBUG1
            printfn "Enqueuing op %A" op
#endif
            // 将操作添加到队列并运行队列
            //let f = Func<Task<OpResult<'Key, 'value>>, 'OpResult> (fun tt ->  tt.Result)
            //let ts = opToFun op |> Seq.map (fun f -> f >> outFun slId |> createTask)
            
            let op, ifBypassQ =
                match _op with
                | IgnoreQ v ->  v, true
                | _ -> _op, false
            if ifBypassQ then
                opToFun op 
                |> Seq.map (fun f -> 
                    task {
                        //if this.mreWaiterOpt.IsSome then
                        //    this.mreWaiterOpt.Value.Wait()
                        //return
                        //    if this.mreOpt.IsSome then
                        //        this.mreOpt.Value.Reset ()
                        //        let fr = f()
                        //        this.mreOpt.Value.Set ()
                        //        fr
                        //    else
                        //        f()
                        return f()
                    }
                )
                |> Seq.toArray

            else
                let ts = 
                    opToFun op 
                    |> Seq.map (fun f -> createTask f)
                    |> Seq.toArray
    #if DEBUG
                ts |> Seq.iter (fun task -> printfn "[before starting] Task %A IsCompleted: %A" task.Id task.IsCompleted)
    #endif
                ts
                |> if lockIdOpt.IsNone then
                    Seq.iter opQueue.Enqueue 
                   else
                    Seq.iter (fun t -> opQueue.EnqueueWithLock(t, lockIdOpt.Value))
                ts

        member this.LockableOps (op: Op<'Key, 'Value>) =
            this.LockableOps (op, None)

        member this.LockableOp (op: Op<'Key, 'Value>, lockId: Guid) =
            this.LockableOps (op, Some lockId) |> Seq.item 0

        member this.LockableOp (op: Op<'Key, 'Value>) =
            this.LockableOps op |> Seq.item 0

        /// Add 方法：将添加操作封装为任务并执行
        member this.Add(k, v, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CAdd(k, v)))
            else
                this.LockableOp(CAdd(k, v))

        member this.Add(k, v) =
            this.Add(k, v, false)

        member this.Add(kvs, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CAddKVs kvs))
            else
                this.LockableOp(CAddKVs kvs)

        member this.Add(kvs) =
            this.Add(kvs, false)

        member this.Upsert2(k, f: 'Key -> 'Value option -> 'Value, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CUpsert2(k, f)))
            else
                this.LockableOp(CUpsert2(k, f))

        member this.Upsert(k, v, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CUpsert(k, v)))
            else
                this.LockableOp(CUpsert(k, v))
        member this.Upsert(k, v) =
            this.Upsert(k, v, false)
        /// Remove 方法：将移除操作封装为任务并执行
        member this.Remove(k) =
            this.LockableOp(CRemove(k))

        /// TryUpdate 方法：将更新操作封装为任务并执行
        member this.Update(k, v, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CUpdate(k, v)))
            else
                this.LockableOp(CUpdate(k, v))

        member this.Update(k, v) =
            this.Update(k, v, false)
        /// TryGetValue 同步获取值，不需要队列
#if UNSAFE
        member this.TryGetValueUnsafe(key: 'Key) : bool * 'Value option =
            lock lockObj (fun () ->
                //if sortedList.ContainsKey(key) then
                //    true, Some(sortedList.[key])
                //else
                //    false, None
                this.TryGetValueBase key
            )
#endif

        member this.GetValue(key: 'Key, ifIgnoreQ) =
#if DEBUG1            
            printfn "TryGetValue"
#endif
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CGet key))
            else
                this.LockableOp(CGet key)

        member this.GetValue(key: 'Key) =
            this.GetValue(key, false)

        member this.GetValues(keys: 'Key seq, ifIgnoreQ) =
            if ifIgnoreQ then
                this.LockableOp(IgnoreQ (CGetByKeys keys))
            else
                this.LockableOp(CGetByKeys keys)

        member this.GetValues(keys: 'Key seq) =
            this.GetValues(keys, false)

        member this.GetValueSafe(key: 'Key) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafe"
#endif
            let (COptionValue r) = this.GetValue(key).Result
            r

        member this.GetValuesSafe(keys: 'Key seq) =
            let (CKVOptList r) = this.GetValues(keys).Result
            r

        member this.TryGetValueSafeTo(key: 'Key, _toMilli:int) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafeTo"
#endif
            try
                let (COptionValue r) = this.GetValue(key).WaitAsync(_toMilli).Result
                r
            with
            | :? System.TimeoutException as te ->
                false, None

        member this.TryGetValuesSafeTo(keys: 'Key seq, _toMilli:int) =
            try
                let (CKVOptList r) = this.GetValues(keys).WaitAsync(_toMilli).Result
                r
            with
            | :? System.TimeoutException as te ->
                [||]

        /// 访问 Item：带线程锁的读写访问
        member this.Item
            with get(key: 'Key) =
                (this.GetValueSafe key |> snd).Value
            and set(k: 'Key) (v: 'Value) =
                //lock lockObj (fun () -> sortedList.[key] <- value)
                this.Update(k, v).Result |> ignore
#if UNSAFE
        /// 获取 Count
        member this.CountUnsafe =
            lock lockObj (fun () -> sortedList.Count)

        /// 获取所有 Values，Unsafe 表 QUEUE 可能還沒做完
        member this.ValuesUnsafe =
            lock lockObj (fun () -> sortedList.Values)

        /// 获取所有 Keys，Unsafe 表 QUEUE 可能還沒做完
        member this.KeysUnsafe =
            lock lockObj (fun () -> sortedList.Keys)

        /// 清空列表，Unsafe 表 QUEUE 可能還沒做完
        member this.CleanUnsafe () =
            lock lockObj (fun () -> sortedList.Clear())

        ///，Unsafe 表 QUEUE 可能還沒做完
        member this.ContainsKeyUnsafe (k:'key) =
            lock lockObj (fun () -> sortedList.ContainsKey k)
#endif        
        /// 获取 Count 版本
        member this.Count =
            this.LockableOp(CCount)

        /// 获取所有 Values 版本
        member this.Values =
            this.LockableOp(CValues)

        /// 获取所有 Keys 版本
        member this.Keys =
            this.LockableOp(CKeys)

        /// 清空列表 版本
        member this.Clean() =
            this.LockableOp(CClean)

        member this.ContainsKey (k:'Key) =
            this.LockableOp(CContains k)

        /// 获取 Count - Safe 版本 表 QUEUE 完成後取值
        member this.CountSafe : int =
            let (CInt count) = this.Count.Result
            count

        /// 获取所有 Values - Safe 版本 表 QUEUE 完成後取值
        member this.ValuesSafe : IList<'Value> =
            let (CValueList values) = this.Values.Result
            values

        /// 获取所有 Keys - Safe 版本 表 QUEUE 完成後取值
        member this.KeysSafe : IList<'Key> =
            let (CKeyList keys) = this.Keys.Result
            keys

        /// 清空列表 - Safe 版本 表 QUEUE 完成後取值
        member this.CleanSafe() =
            this.Clean().Result |> ignore

        /// Safe 版本 表 QUEUE 完成後取值
        member this.ContainsKeySafe (k:'Key) =
            let (CBool r) = this.ContainsKey(k).Result
            r

        member this.FirstLastN(n, kvMode: KVMode, _toMilliOpt: int option) = 
            // Ensure cache is updated if autoCache is not true
            if this.autoCacheChange.IsNone || this.autoCacheChange.Value = 0 then
                SortedListCache<_, _>.CacheChange(sortedList)

            let _n = if n > 0 then 0 else n
            let kvOp = 
                match kvMode with
                | KeyOnlyMode ->
                    KeysOp(None, _n, abs n, fun keysMemory memLen _ _ ->
                        let keys = keysMemory.Span.ToArray()
                        CKeyList keys
                    )
                | ValueOnlyMode ->
                    ValuesOp(None, _n, abs n, fun valuesMemory memLen _ _ ->
                        let values = valuesMemory.Span.ToArray()
                        CValueList values
                    )
                | KVMode ->
                    KeyValuesOp(None, _n, abs n, fun keysMemory valuesMemory memLen _ _ ->
                        let keys = keysMemory.Span.ToArray()
                        let values = valuesMemory.Span.ToArray()
                        CKVList ((keys, values) ||> Array.map2 (fun k v -> k,v))
                    )
            if _toMilliOpt.IsNone then
                this.LockableOp(kvOp).Result
            else
                this.LockableOp(kvOp).WaitAsync(_toMilliOpt.Value).Result

        member this.FirstLastN(n, kvMode) =
            this.FirstLastN(n, kvMode, None)

        member this.FirstLastNKeys(n) =
            defaultArg (this.FirstLastN(n, KeyOnlyMode, None).FoldResultRWArr0Key) [||]

        member this.FirstLastNValues(n) =
            defaultArg (this.FirstLastN(n, ValueOnlyMode, None).FoldResultRWArr0Value) [||]

        member this.FirstLastN(n) =
            defaultArg (this.FirstLastN(n, KVMode, None).FoldResultRWArr0KV) [||]





        member this._base = sortedList

        member this.RequireLock (rtoOpt, ltoOpt) =
            this.OpQueue.RequireLock(rtoOpt, ltoOpt)

        member this.UnLock (lockId) =
            this.OpQueue.UnLock lockId

        member this.UnLock () =
            this.OpQueue.SysUnLock ()

        new (slId, (autoCache: int)) =
            new ConcurrentSortedList<_, _, _>(slId, obj(), 300000, autoCacheChangeOpt = autoCache)

        new (slId, (timeout:int), (autoCache: int)) =
            new ConcurrentSortedList<_, _, _>(slId, obj(), timeout, autoCacheChangeOpt = autoCache)

        new (slId) =
            new ConcurrentSortedList<_, _, _>(slId, obj(), 300000)

