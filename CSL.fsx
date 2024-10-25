namespace PersistedConcurrentSortedList


module CSL =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading.Tasks

    let createTask (fn: unit -> 'T) : Task<'T> =
        new Task<'T>(fun () -> fn())

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

    //type OpResultTyp = 
    //| TUnit         
    //| TBool         of defaultValue:bool
    //| TOptionValue  of defaultValue:bool
    //| TInt          of defaultValue:int
    //| TKeyList     
    //| TValueList   
    
    type OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit
    | CBool         of bool
    | COptionValue  of (bool * 'Value option)
    | CInt          of int
    | CKeyList      of IList<'Key>
    | CValueList    of IList<'Value>
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

    type QueueProcessorStatus =
    | Locked of Guid
    | Listenning

    type QueueProcessor<'T, 'OpResult>(slId: 'T, _receiveTimeoutDefault) =

        // 定義一個 MailboxProcessor 來處理操作任務
        let opProcessor = FActor.Start(fun (inbox:FActor<QueueProcessorCmd<'OpResult>>) ->
            let messageQueue = Queue<QueueProcessorCmd<'OpResult>>()
            let mutable status = Listenning
            let mutable _receiveTimeout = _receiveTimeoutDefault
            //let mutable curLockGuid = Guid.Empty
            let rec loop () =
                async {
                    let! taskOpt = inbox.TryReceive(_receiveTimeout)
    #if DEBUG1
                    printfn "[%A] dequeued %A" slId task
    #endif
                    if taskOpt.IsSome then
                    //| Some qpCmd when status = Listenning ->
                        let rec goAhead _status =
                            status <- _status
                            let ifD, m = messageQueue.TryDequeue () 
                            if ifD then
                                procCmd m
                        and execute (task:Task<'OpResult>) =
                            task.Start ()
                            let tr = task |> Async.AwaitTask |> Async.Catch |> Async.RunSynchronously // 同步執行任務

                            match tr with
                            | Choice1Of2 s -> 
#if DEBUG1
                                printfn "Successfully: %A" s
#endif
                                ()
                            | Choice2Of2 exn -> 
                                printfn "Failed: %A" exn.Message
                            goAhead Listenning

                        and procCmd cmd =
                            match status with
                            | Locked curLockGuid ->
                                match cmd with
                                | SysUnlock g 
                                | Unlock g when g = curLockGuid -> 
                                    goAhead Listenning
                                | Tsk (task, (Some g)) when g = curLockGuid ->
                                    execute task
                                | _ ->
                                    messageQueue.Enqueue cmd
                            
                            | Listenning ->
                                match cmd with
                                | Tsk (_, (Some g)) ->
                                    execute (task {
                                        return (failwithf "Invalid lock %A" g)
                                    })

                                | Tsk (task, None) ->
                                    execute task

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
                                    ()

                        let ifD, m = messageQueue.TryDequeue () 
                        if ifD then
                            procCmd m
                        procCmd taskOpt.Value

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
            
            let rec taskToEnqueue op = 
#if DEBUG1
                printfn "%A Current Op: %A, %s, %s" slId op (getTypeName typeof<'Key>) (getTypeName typeof<'Value>)
#endif
                match op with
                | SeqOp s ->
                    s
                    |> Seq.collect (fun o ->
                        taskToEnqueue o
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
                


                    >> (outFun slId)
                    |> fun t -> seq [createTask t]
#if DEBUG1
            printfn "Enqueuing op %A" op
#endif
            // 将操作添加到队列并运行队列
            let ts = taskToEnqueue op
            ts
            |> if lockIdOpt.IsNone then
                Seq.iter opQueue.Enqueue 
               else
                Seq.iter (fun t -> opQueue.EnqueueWithLock(t, lockIdOpt.Value))
            ts

        member this.LockableOps (op: Op<'Key, 'Value>) =
            this.LockableOps (op, None)

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

        new (slId, outFun, extractFunBase) =
            new ConcurrentSortedList<_, _, _, _>(slId, outFun, extractFunBase, obj(), 30000)

