namespace PersistedConcurrentSortedList


module CSL =
    open System
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

    let inline getKeys (sl: SortedList<'k, _>) =
        let fi = sl.GetType().GetField("keys", BindingFlags.Instance ||| BindingFlags.NonPublic)
        fi.GetValue sl |> unbox<'k[]>

    let inline getValues (sl: SortedList<_, 'v>) =
        let fi = sl.GetType().GetField("values", BindingFlags.Instance ||| BindingFlags.NonPublic)
        fi.GetValue sl |> unbox<'v[]>

    
    type SortedListCache<'k, 'v>() =
        // ConcurrentDictionary，用於緩存不同 SortedList 實例的 keys 和 values
        static let kCache = ConcurrentDictionary<SortedList<'k, 'v>, 'k[]>()
        static let vCache = ConcurrentDictionary<SortedList<'k, 'v>, 'v[]>()

        // cache 函數，將 SortedList 的 keys 和 values 緩存到 ConcurrentDictionary
        static member Cache(sl: SortedList<'k, 'v>) =
            kCache.GetOrAdd(sl, fun slInstance ->
                let keys = getKeys slInstance
                keys
            ) |> ignore
            
            vCache.GetOrAdd(sl, fun slInstance ->
                let values = getValues slInstance
                values
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



    
    type Layer = int
    type Tag = string

    type Op<'Key, 'Value when 'Key : comparison> =
    | CAdd of 'Key * 'Value
    | CRemove of 'Key
    | CUpdate of 'Key * 'Value
    | CUpsert of 'Key * 'Value
    | CGet of 'Key
    | CContains of 'Key
    | CCount
    | CValues
    | CKeys
    | CClean
    | SeqOp of Op<'Key, 'Value> seq
    | FoldOp of Op<'Key, 'Value> seq
    | KeysOp of Tag option * int * int * (Memory<'Key> -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>)
    | ValuesOp of Tag option * int * int * (Memory<'Value> -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>)
    | KeyValuesOp of Tag option * int * int * (Memory<'Key> -> Memory<'Value> -> ResultWrapper<'Key, 'Value>[] -> Task<OpResult<'Key, 'Value>> option -> OpResult<'Key, 'Value>)

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
    | CKeyList      of IList<'Key>
    | CValueList    of IList<'Value>
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

    type Task<'T> with
        //member this.ResultWithTimeout (_to:int) = 
            
        //        task {
        //            let timeoutTask = Task.Delay(_to)
        //            let! completedTask = Task.WhenAny(this, timeoutTask)
        //            if completedTask = timeoutTask then
        //                if this.IsCompleted then
        //                    return Some this.Result
        //                else
        //                    printfn "Task did not complete in time, but it was started."
        //                    return None
        //            else
        //                printfn "Task timed out."
        //                return None
        //        }
        member this.WaitAsync(_toMilli:int) = 
            this.WaitAsync(TimeSpan.FromMilliseconds _toMilli)
        member this.Ignore () = ()
        member this.thisT = this :> Task
        member this.WaitIgnore = this.Result |> ignore
       

    let rec getTypeName (t: Type) =
        if t.IsGenericType then
            let genericArgs = t.GetGenericArguments() |> Array.map getTypeName |> String.concat ", "
            sprintf "%s<%s>" (t.Name.Substring(0, t.Name.IndexOf('`'))) genericArgs
        else
            t.Name

    
    type SLId = string

    type FActor<'T> = MailboxProcessor<'T>


    type QueueProcessorCmd<'OpResult> =
    | Tsk of Task<'OpResult> * Guid option
    | Lock of AsyncReplyChannel<Guid> * TimeSpan option  // 加入 timeout 參數
    | Unlock of Guid
    | SysLock
    | SysUnlock of Guid
    | StepForward

    type QueueProcessorStatus =
    | Locked of Guid
    | Listenning

    type QueueProcessor<'T, 'OpResult>(slId: 'T, _receiveTimeoutDefault) =

        // 定義一個 MailboxProcessor 來處理操作任務
        let opProcessor = FActor.Start(fun (inbox:FActor<QueueProcessorCmd<'OpResult>>) ->
            let mutable messageQueue = Queue<QueueProcessorCmd<'OpResult>>()
            let mutable messageQueueTmp = Queue<QueueProcessorCmd<'OpResult>>()
            let mutable status = Listenning
            let mutable _receiveTimeout = _receiveTimeoutDefault
            //let mutable curLockGuid = Guid.Empty
            let rec loop () =
                async {
                    let! taskOpt = inbox.TryReceive(_receiveTimeout)
                    if taskOpt.IsSome then
                        let mutable ifSwitched = false
    #if DEBUG
                        printfn "[%A] dequeued %A, messageQueue count: %d" slId task messageQueue.Count
    #endif
                        printfn "Cmd received: %A" taskOpt.Value
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
                            let ifD, m = messageQueue.TryDequeue () 
                            if ifD then
                                procCmd m

                        and getCmdAndProceedBeforeLatestCmdProceed latestCmdToAppend =
                            let ifD, m = messageQueue.TryDequeue () 
                            if ifD then
                                printfn "Enqueue latestCmd first: %A" latestCmdToAppend
                                messageQueue.Enqueue latestCmdToAppend
                                procCmd m
                                
                            else
                                procCmd latestCmdToAppend
                        
                        and goAhead _status =
                            status <- _status
                            getCmdAndProceed ()

                        and execute (task:Task<'OpResult>) =
                            task.Start ()
                            printfn "Task %A started" task.Id
                            let tr = task |> Async.AwaitTask |> Async.Catch |> Async.StartAsTask // 同步執行任務

                            match tr.Result with
                            | Choice1Of2 s -> 
                                printfn "Task %A IsCompleted: %A" task.Id task.IsCompleted
#if DEBUG1
                                printfn "Successfully: %A" s
#endif
                                ()
                            | Choice2Of2 exn -> 
                                printfn "Task %A IsCompleted: %A" task.Id task.IsCompleted
                                printfn "Failed: %A" exn.Message
      

                        and procCmd cmd =
                            printfn "Cmd proceeding: %A" cmd
                            match status with
                            | Locked curLockGuid ->
                                printfn "Current lock context: %A" curLockGuid
                                match cmd with
                                | SysUnlock g 
                                | Unlock g when g = curLockGuid -> 
                                    printfn "Unlocked! context:%A" g
                                    goAhead Listenning

                                | Tsk (task, (Some g)) when g = curLockGuid ->
                                    printfn "Execute task in same lockId context! context:%A, cmd:%A" curLockGuid g
                                    execute task
                                    getCmdAndProceed ()
                                | _ when ifSwitched = false ->
                                    printfn "Enqueue task in different context firstTime! context:%A" curLockGuid
                                    ifSwitched <- true
                                    messageQueueTmp.Enqueue cmd
                                    getCmdAndProceed ()
                                | _ ->
                                    printfn "Enqueue task in different context! context:%A" curLockGuid
                                    messageQueueTmp.Enqueue cmd
                                    getCmdAndProceed ()
                            | Listenning ->
                                printfn "Listenning, no lock context."
                                match cmd with
                                | Tsk (_, (Some g)) ->
                                    execute (task {
                                        return (failwithf "Invalid lock %A" g)
                                    })

                                | Tsk (task, None) ->
                                    execute task
                                    getCmdAndProceed ()

                                | Lock (replyLockId, timeoutTimeSpanOpt) -> // 處理 lock 並加入 timeout
                                    let g = Guid.NewGuid()
                                    status <- Locked g
                                    replyLockId.Reply g

                                    // 啟動 timeout 計時器，如果超時則自動 unlock
                                    if timeoutTimeSpanOpt.IsSome then
                                        async {
                                            do! Async.Sleep (int timeoutTimeSpanOpt.Value.TotalMilliseconds)
                                            inbox.Post (SysUnlock g)
                                        }
                                        |> Async.Start
                                | _ ->
                                    printfn "Cmd handler haven't yet implemented or ignored! %A" cmd

                        if taskOpt.Value = StepForward then
                            getCmdAndProceed ()
                        else
                            getCmdAndProceedBeforeLatestCmdProceed taskOpt.Value
                        if ifSwitched then
                            printfn "queue switched"
                            messageQueue <- messageQueueTmp
                            messageQueueTmp <- Queue<QueueProcessorCmd<'OpResult>>()
                        if messageQueue.Count > 0 && status = Listenning then 
                            //if locked and queue not empty means we need to wait unlock, so no need to StepForward
                            inbox.Post StepForward
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


    type ConcurrentSortedList<'Key, 'Value, 'OpResult, 'ID
        when 'Key : comparison
        and 'Value: comparison
        
        >(
        slId:'ID
        , outFun: 'ID -> OpResult<'Key, 'Value> -> 'OpResult
        , extractFunBase:'ID -> 'OpResult -> OpResult<'Key, 'Value>
        , lockObj: obj
        , timeoutDefault
        ) =
        let sortedList = SortedList<'Key, 'Value>()
        //let lockObj = obj()
        let opQueue = QueueProcessor<'ID, 'OpResult>(slId, timeoutDefault)
        let extractFun = extractFunBase slId
        let extractOpResult (opt:Task<'OpResult>) = extractFun opt.Result

        member this.Id = slId
        member this.LockObj = lockObj
        member this.OpQueue = opQueue

        member this.ExtractOpResult = extractOpResult
        /// 基础的 Add 操作（直接修改内部数据结构）
        member this.TryAddBase(key: 'Key, value: 'Value) =
            
            try
                let added = sortedList.TryAdd(key, value)
#if DEBUG1
                printfn "[%A] %A, %A added" slId key value
#endif
                added
            with
            | _ -> false

        /// 基础的 Remove 操作
        member this.TryRemoveBase(key: 'Key) =
            try
                sortedList.Remove(key)
            with
            | _ -> false

        /// 尝试更新，如果键存在则更新值
        member this.TryUpdateBase(key: 'Key, newValue: 'Value) : bool =
            if sortedList.ContainsKey(key) then
                sortedList.[key] <- newValue
                true
            else
                false

        member this.TryUpsertBase(key: 'Key, newValue: 'Value) =
            if sortedList.ContainsKey(key) then
                sortedList.[key] <- newValue
                2
            else
                if sortedList.TryAdd (key, newValue) then
                    1
                else
                    0
                

        member this.TryGetValueBase(key: 'Key) : bool * 'Value option =
            if sortedList.ContainsKey(key) then
                true, Some(sortedList.[key])
            else
                false, None


        /// 封装操作并添加到任务队列，执行后返回 Task
        member this.LockableOps (op: Op<'Key, 'Value>, lockIdOpt: Guid option) =
            // `processFoldOp` 函數實現，接收一個 `Op<'Key, 'Value> seq` 序列並返回一個 `FoldResult`
            let rec processOp layer arr (preResultTask: Task<OpResult<_,_>> option) (op: Op<'Key, 'Value>) (baseInstance: ConcurrentSortedList<'Key, 'Value, 'OpResult, 'ID>) =
                let results =
                    match op with
                    | KeysOp (tagOpt, start, length, f) ->
                        let keysMemory = SortedListCache<'Key, 'Value>.GetKeysCached(baseInstance._base) |> createMemoryFromArr<'Key> start length
                        let curResult = task {return f keysMemory arr preResultTask}
                        if tagOpt.IsSome then
                            ResultWrapper(layer, tagOpt.Value, kMemory=keysMemory, opResult = curResult), curResult
                        else
                            ResultWrapper(layer, opResult = curResult), curResult

                    | ValuesOp (tagOpt, start, length, f) ->
                        // 從緩存中取得 values 並創建 Memory
                        let valuesMemory = SortedListCache<'Key, 'Value>.GetValuesCached(baseInstance._base) |> createMemoryFromArr<'Value> start length
                        let curResult = task {return f valuesMemory arr preResultTask}
                        if tagOpt.IsSome then
                            ResultWrapper(layer, tagOpt.Value, vMemory = valuesMemory, opResult = curResult), curResult
                        else
                            ResultWrapper(layer, vMemory = valuesMemory, opResult = curResult), curResult

                    | KeyValuesOp (tagOpt, start, length, f) ->
                        // 從緩存中取得 keys 和 values 並創建 Memory
                        let keysMemory = SortedListCache<'Key, 'Value>.GetKeysCached(baseInstance._base) |> createMemoryFromArr<'Key> start length
                        let valuesMemory = SortedListCache<'Key, 'Value>.GetValuesCached(baseInstance._base) |> createMemoryFromArr<'Value> start length
                        let curResult = task {return f keysMemory valuesMemory arr preResultTask}
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
                    | CRemove k ->
                        fun () ->
                            this.TryRemoveBase(k) |> CBool

                    | CUpdate (k, v) ->
                        fun () ->
                            this.TryUpdateBase(k, v) |> CBool

                    | CUpsert (k, v) ->
                        fun () ->
                            this.TryUpsertBase(k, v) |> CInt

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
            let ts = 
                opToFun op 
                |> Seq.map (fun f -> (fun () -> f()|> outFun slId) |> createTask)
                |> Seq.toArray
            ts |> Seq.iter (fun task -> printfn "[before starting] Task %A IsCompleted: %A" task.Id task.IsCompleted)
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
        member this.Add(k, v) =
            this.LockableOp(CAdd(k, v))

        member this.Upsert(k, v) =
            this.LockableOp(CUpsert(k, v))

        /// Remove 方法：将移除操作封装为任务并执行
        member this.Remove(k) =
            this.LockableOp(CRemove(k))

        /// TryUpdate 方法：将更新操作封装为任务并执行
        member this.Update(k, v) =
            this.LockableOp(CUpdate(k, v))

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

        member this.GetValue(key: 'Key) =
#if DEBUG1            
            printfn "TryGetValue"
#endif
            this.LockableOp(CGet key)

        member this.GetValueSafe(key: 'Key) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafe"
#endif
            let (COptionValue r) = this.GetValue(key).Result |> extractFun
            r

        member this.TryGetValueSafeTo(key: 'Key, _toMilli:int) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafeTo"
#endif
            try
                let (COptionValue r) = this.GetValue(key).WaitAsync(_toMilli).Result |> extractFun
                r
            with
            | :? System.TimeoutException as te ->
                false, None

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
            let (CInt count) = this.Count.Result |> extractFun
            count

        /// 获取所有 Values - Safe 版本 表 QUEUE 完成後取值
        member this.ValuesSafe : IList<'Value> =
            let (CValueList values) = this.Values.Result |> extractFun
            values

        /// 获取所有 Keys - Safe 版本 表 QUEUE 完成後取值
        member this.KeysSafe : IList<'Key> =
            let (CKeyList keys) = this.Keys.Result |> extractFun
            keys

        /// 清空列表 - Safe 版本 表 QUEUE 完成後取值
        member this.CleanSafe() =
            this.Clean().Result |> ignore

        /// Safe 版本 表 QUEUE 完成後取值
        member this.ContainsKeySafe (k:'Key) =
            let (CBool r) = this.ContainsKey(k).Result |> extractFun
            r

        member this._base = sortedList

        member this.RequireLock (rtoOpt, ltoOpt) =
            this.OpQueue.RequireLock(rtoOpt, ltoOpt)

        member this.UnLock (lockId) =
            this.OpQueue.UnLock lockId

        new (slId, outFun, extractFunBase) =
            new ConcurrentSortedList<_, _, _, _>(slId, outFun, extractFunBase, obj(), 300000)

