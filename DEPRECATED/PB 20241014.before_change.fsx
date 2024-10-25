namespace fs

#if INTERACTIVE
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r "nuget: FAkka.FsPickler, 9.0.3"
#r "nuget: FAkka.FsPickler.Json, 9.0.3"
#r @"nuget: protobuf-net"
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\Debug\netstandard2.0\protobuf-net-fsharp.dll"
#endif

open System
open System.Collections
open System.Collections.Generic
open NTDLS.Katzebase.Parsers.Interfaces

open Newtonsoft.Json

open MBrace.FsPickler
open MBrace.FsPickler.Combinators 
open ProtoBuf
open ProtoBuf.FSharp
open FSharp.Reflection

[<ProtoBuf.ProtoContract>]
type fstring =
| S of string
| D of double
| A of fstring []
| T of string * fstring
    member this.toJsonString() =
        let rec toJson (f: fstring) =
            match f with
            | S s -> sprintf "%s" s
            | D d -> sprintf "%f" d
            | A arr -> 
                let elements = arr |> Array.map toJson |> String.concat ", "
                sprintf "[%s]" elements
            | T (key, value) ->
                let keyStr = sprintf "%s" key
                let valueStr = toJson value
                sprintf "%s: %s" keyStr valueStr
        toJson this

    static member compareArrays (arr1: fstring array) (arr2: fstring array): int =
        Seq.zip arr1 arr2
        |> Seq.tryPick (fun (x, y) ->
            let res = compare x y
            if res = 0 then None else Some res)
        |> Option.defaultValue 0

    static member compareLength (arr1: fstring array) (arr2: fstring array): int =
        let a1l = if arr1 = null then 0 else arr1.Length
        let a2l = if arr2 = null then 0 else arr2.Length
        compare a1l a2l

    static member Compare (x: fstring, y: fstring): int =
        match box x, box y with
        | null, null -> 0
        | _, null -> 
            if x = S null then 0 else 1
        | null, _ ->
            if y = S null then 0 else -1
        | _ ->
            match (x, y) with
            | (D d1, D d2) -> Decimal.Compare(decimal d1, decimal d2)
            | (S s1, S s2) -> String.Compare(s1, s2, StringComparison.OrdinalIgnoreCase)
            | (A arr1, A arr2) ->
                let lenComp = fstring.compareLength arr1 arr2
                if lenComp <> 0 then lenComp
                else fstring.compareArrays arr1 arr2
            | (T (tag1, f1), T (tag2, f2)) ->
                let tagComp = String.Compare(tag1, tag2, StringComparison.OrdinalIgnoreCase)
                if tagComp <> 0 then tagComp
                else fstring.Compare(f1, f2)
            | (D _, _) -> -1
            | (_, D _) -> 1
            | (S _, (A _ | T _)) -> -1
            | ((A _ | T _), S _) -> 1
            | (A _, T _) -> -1
            | (T _, A _) -> 1

    interface IComparer<fstring> with
        override this.Compare(x: fstring, y: fstring): int =
            fstring.Compare(x, y)

    member this.s =
        match this with
        | S s -> s
        | _ -> failwith "Not fstring.S."

    member this.d =
        match this with
        | D d -> d
        | _ -> failwith "Not fstring.D."

    member this.ToLowerInvariant () =
        match this with
        | S "" -> fstring.SNull
        | S s -> s.ToLowerInvariant() |> S
        | _ -> failwith "Not fstring.S."

    static member val CompareFunc : Func<fstring, fstring, bool> = 
        FuncConvert.FromFunc(fun (x: fstring) (y: fstring) -> fstring.Compare(x, y) = 0)

    static member aFromStringArr (sArr) =
        sArr
        |> Array.map S
        |> A

    static member fromStringArr (sArr) =
        sArr
        |> Array.map S

    static member val SEmpty        = S ""                          with get
    static member val AEmpty        = A [||]                        with get
    static member val SNull         = S null                        with get
    static member val ANull         = A null                        with get
    static member val Unassigned    = Unchecked.defaultof<fstring>  with get

    static member SIsNullOrEmpty (o:fstring) = if box o = null || o = fstring.SEmpty || o = fstring.SNull then true else false
    static member AIsNullOrEmpty (o:fstring) = if box o = null || o = fstring.AEmpty || o = fstring.ANull then true else false
    static member IsNull (o:fstring) = 
        if box o = null || o = fstring.ANull || o = fstring.SNull then true else false

    // 加入 IStringable 接口的實作
    interface IStringable with
        override this.GetKey () = this.s
        override this.IsNullOrEmpty () = this.s = null || this.s = ""
        override this.ToLowerInvariant () = this.ToLowerInvariant()
        override this.ToT<'T> () =
            match typeof<'T> with
            | t when t = typeof<string> -> box this.s :?> 'T
            | t when t = typeof<double> -> box (Double.Parse this.s) :?> 'T
            | t when t = typeof<int> -> box (Int32.Parse this.s) :?> 'T
            | t when t = typeof<bool> -> box (Boolean.Parse this.s) :?> 'T
            | t -> failwithf "type %s not supported" t.Name

        override this.ToT (t: Type) =
            match t with
            | t when t = typeof<string> -> box this.s
            | t when t = typeof<double> -> box (Double.Parse this.s)
            | t when t = typeof<int> -> box (Int32.Parse this.s)
            | t when t = typeof<bool> -> box (Boolean.Parse this.s)
            | _ -> failwithf "type %s not supported" t.Name

        override this.ToNullableT<'T> () =
            match typeof<'T> with
            | t when t = typeof<string> -> box this.s :?> 'T
            | t when t = typeof<double> -> box (Double.Parse this.s) :?> 'T
            | t when t = typeof<int> -> box (Int32.Parse this.s) :?> 'T
            | t when t = typeof<bool> -> box (Boolean.Parse this.s) :?> 'T
            | _ -> failwithf "type %s not supported" typeof<'T>.Name


    member this.me =
        match this with
        | S s -> s
        | D d -> d.ToString()
        | A arr -> arr |> Array.map (fun f -> f.me) |> String.concat ", "
        | T (key, value) -> sprintf "%s: %s" key value.me

    member this.Value = this.me

    //new () =
    //    S null
open System.Runtime.CompilerServices

[<Extension>]
module FS =
    let mapper = new Dictionary<Type, fstring -> obj>()

    let _ =
        mapper.Add(
            typeof<string>, (fun (S s) -> box s)
        )
    let _ =
        mapper.Add(
            typeof<double>, (fun (D d) -> box d)
        )

    let _ =
        mapper.Add(
            typeof<fstring []>, (fun (A a) -> box a)
        )

    let _ =
        mapper.Add(
            typeof<string * fstring>, (fun (T (k, v)) -> box (KeyValuePair.Create(k, v)))
        )

    [<Extension>]
    let toType (this: fstring, t:Type) = mapper[t] this



[<Extension>]
module ExtensionsString =
    [<Extension>]
    let toF(str : string) = S str
[<Extension>]
module ExtensionsDecimal =
    [<Extension>]
    let toF(d : decimal) = D (double d)
[<Extension>]
module ExtensionsDouble =
    [<Extension>]
    let toF(d : double) = D d

[<Extension>]
module ExtensionsInt =
    [<Extension>]
    let toF(d : int) = D (double d)

module PB =
    open ProtoBuf.Meta
    open ProtoBuf.FSharp
    let mutable pbModel = //lazy (
        printfn "???????????????????????????????????????????????????????????????"
        RuntimeTypeModel.Create("???")
        |> Serialiser.registerUnionIntoModel<fstring> 
        //

    let serializeFBase m (ms, o) =
        printfn "[serializeF] type: %s, %A" (o.GetType().Name) o
        Serialiser.serialise m ms o

    let deserializeFBase<'T> m ms =
        printfn "[deserializeF] 'T: %s" typeof<'T>.Name
        Serialiser.deserialise<'T> m ms

    let serializeF (ms, o) = serializeFBase pbModel (ms, o)

    let deserializeF<'T> ms = 
        deserializeFBase<'T> pbModel ms

module PB2 =
    open System.IO
    open ProtoBuf.Meta
    open ProtoBuf.FSharp
    open System.Security.Cryptography
    open System.Text

    let generateStrHash (input: string) : string =
        use sha256 = SHA256.Create()
        let bytes = Encoding.UTF8.GetBytes(input)
        let hashBytes = sha256.ComputeHash(bytes)

        // 使用 Base64 编码并替换非法字符
        let base64Hash = Convert.ToBase64String(hashBytes)
        let safeHash = base64Hash.Replace("/", "-").Replace("+", "_").Replace("=", "")

        // 返回 Windows 安全的文件名
        safeHash

    let generateBArrHash (bytes: byte[]) : string =
        use sha256 = SHA256.Create()
        let hashBytes = sha256.ComputeHash(bytes)

        // 使用 Base64 编码并替换非法字符
        let base64Hash = Convert.ToBase64String(hashBytes)
        let safeHash = base64Hash.Replace("/", "-").Replace("+", "_").Replace("=", "")

        // 返回 Windows 安全的文件名
        safeHash

    type ModelContainer<'T> () =
        static let lockObj = obj()

        static member val pbModel =
            lock lockObj (fun () ->
#if DEBUG
                printfn $"?????????????????????????? {typeof<'T>.Name} ??????????????????????????"
#endif
                RuntimeTypeModel.Create(typeof<'T>.Name)
                |> fun m ->
                    if FSharpType.IsUnion typeof<'T> then
                        Serialiser.registerUnionIntoModel<'T> m
                    elif FSharpType.IsRecord typeof<'T> then
                        Serialiser.registerRecordIntoModel<'T> m
                    else
                        Serialiser.registerTypeIntoModel<'T> m
            )
            with get, set

        static member serializeFBase (m, (ms, o)) =
            printfn "[serializeF] type: %s, %A" (o.GetType().Name) o
            Serialiser.serialise m ms o

        static member deserializeFBase (m, ms) =
            printfn "[deserializeF] 'T: %s" typeof<'T>.Name
            Serialiser.deserialiseConcreteType<'T> m ms

        static member serializeF (ms, o) = 
            ModelContainer<'T>.serializeFBase (ModelContainer<'T>.pbModel, (ms, o))

        static member serializeF2BArr o = 
            let ms = new MemoryStream()
            ModelContainer<'T>.serializeFBase (ModelContainer<'T>.pbModel, (ms, o))
            ms.ToArray()
            |> Deflate.compress

        static member getHashStr o = 
            ModelContainer<'T>.serializeF2BArr o
            |> generateBArrHash

        static member deserializeF ms = 
            ModelContainer<'T>.deserializeFBase (ModelContainer<'T>.pbModel, ms)

        static member write2File = fun filePath (o:'T) ->            
            File.WriteAllBytes(filePath, ModelContainer<'T>.serializeF2BArr o)

        static member readFromFileBase = fun filePath ->
            let serializedData = File.ReadAllBytes(filePath) |> Deflate.decompress
            // 使用反序列化来读取值
            use input = new MemoryStream(serializedData)
            input.Position <- 0
            ModelContainer<'T>.deserializeF input
        
        static member readFromFile = fun filePath ->
            if File.Exists(filePath) then
                ModelContainer<'T>.readFromFileBase filePath |> Some
            else
                None

    let registerType<'T> (rtmOpt:RuntimeTypeModel option) =
        if rtmOpt.IsNone then
            ModelContainer<'T>.pbModel
        else
            let updated = 
                //rtmOpt.Value.GetTypes() => 如果後續有需要，看是不是把所有的 type 的 ModelContainer<'T>.pbModel 都更新
                rtmOpt.Value
                |> Serialiser.registerUnionIntoModel<'T>
            ModelContainer<'T>.pbModel <- updated
            ModelContainer<'T>.pbModel



module CSL =
    open System
    open System.Collections.Generic
    open System.Collections.Concurrent
    open System.Threading.Tasks

    let createTask (fn: unit -> 'T) : Task<'T> =
        new Task<'T>(fun () -> fn())

    //let t = createTask (fun () -> 123)

    (*
    async {
        do! Async.Sleep 5000
        t.Start ()
    } |> Async.Start
    
    t.Result
    *)


    Async.RunSynchronously (async { do! Async.Sleep 2000}, 1000)

    let a = 
        [|
            task {
                failwith "orz"
                return 123
            } :> Task
            task {
        
                return 456
            } :> Task
        |] 
    a |> Task.WaitAll

    let b = new Task<int> (fun () -> 
        printfn "123"
        123
        )
    [|b:>Task|] |> Task.WaitAll

    Async.RunSynchronously(
        task {
            return! b
        }
        |> Async.AwaitTask, 3000)


    type Op<'Key, 'Value when 'Key : comparison> =
    | CAdd of 'Key * 'Value
    | CRemove of 'Key
    | CUpdate of 'Key * 'Value
    | CGet of 'Key
    | CContains of 'Key
    | CCount
    | CValues
    | CKeys
    | CClean

    type OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit
    | CAdded of bool
    | CUpdated
    | CBool of bool
    | COptionValue of (bool * 'Value option)
    | CInt of int
    | CKeyList of IList<'Key>
    | CValueList of IList<'Value>



    type A(t:int) =
        member this.o = t/10
        member this.tt() = t

    
    JsonConvert.SerializeObject(A(123))

    let qPickler = Pickler.auto<A>

    let bp = Binary.pickle qPickler (A(123))
    

    (Binary.unpickle qPickler bp).tt()

    open MBrace.FsPickler.Json

    let js = JsonSerializer()

    js.Pickle (A(123))
    js.PickleToString (A(123))

    (js.UnPickleOfString<A> """{"FsPickler":"4.0.0","type":"FSI_0021+A","value":{"t":123}}""").tt()


    type Task<'T> with
        member this.ResultWithTimeout (_to:int) = 
            
                task {
                    let timeoutTask = Task.Delay(_to)
                    let! completedTask = Task.WhenAny(this, timeoutTask)
                    if completedTask = timeoutTask then
                        if this.IsCompleted then
                            return Some this.Result
                        else
                            printfn "Task did not complete in time, but it was started."
                            return None
                    else
                        printfn "Task timed out."
                        return None
                }

    

        

    type ConcurrentSortedList<'Key, 'Value when 'Key : comparison>() =
        let sortedList = SortedList<'Key, 'Value>()
        let lockObj = obj()
        let opQueue = ConcurrentQueue<Task<OpResult<'Key, 'Value>>>() // 操作任务队列

        /// 运行队列中的任务
        let rec run () =
            async {
                match opQueue.TryDequeue() with
                | true, t -> 
                    t.RunSynchronously()  // 同步执行任务
                    do! run ()
                | false, _ -> ()
            }

        member this.Run () =
            lock lockObj (fun () ->
                run () |> Async.Start
            )
        /// 基础的 Add 操作（直接修改内部数据结构）
        member this.AddBase(key: 'Key, value: 'Value) =
            //sortedList.Add(key, value)
            sortedList.TryAdd(key, value)

        /// 基础的 Remove 操作
        member this.RemoveBase(key: 'Key) =
            sortedList.Remove(key)

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
                CUpdated
            else
                sortedList.TryAdd (key, newValue) |> CAdded
                

        member this.TryGetValueBase(key: 'Key) : bool * 'Value option =
            if sortedList.ContainsKey(key) then
                true, Some(sortedList.[key])
            else
                false, None


        /// 封装操作并添加到任务队列，执行后返回 Task
        member this.LockableOp (op: Op<'Key, 'Value>) =
            let taskToEnqueue = 
                match op with
                | CAdd (k, v) ->
                    fun () ->
                        this.AddBase(k, v) |> CBool
                | CRemove k ->
                    fun () ->
                        this.RemoveBase(k) |> CBool

                | CUpdate (k, v) ->
                    fun () ->
                        this.TryUpdateBase(k, v) |> CBool

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
                |> createTask
            
            // 将操作添加到队列并运行队列
            opQueue.Enqueue taskToEnqueue
            this.Run ()

            taskToEnqueue

        /// Add 方法：将添加操作封装为任务并执行
        member this.Add(k, v) =
            this.LockableOp(CAdd(k, v))

        /// Remove 方法：将移除操作封装为任务并执行
        member this.Remove(k) =
            this.LockableOp(CRemove(k))

        /// TryUpdate 方法：将更新操作封装为任务并执行
        member this.TryUpdate(k, v) =
            this.LockableOp(CUpdate(k, v))

        /// TryGetValue 同步获取值，不需要队列
        member this.TryGetValueUnsafe(key: 'Key) : bool * 'Value option =
            lock lockObj (fun () ->
                //if sortedList.ContainsKey(key) then
                //    true, Some(sortedList.[key])
                //else
                //    false, None
                this.TryGetValueBase key
            )


        member this.TryGetValue(key: 'Key) =
            this.LockableOp(CGet key)

        member this.TryGetValueSafe(key: 'Key) : bool * 'Value option =
            let (COptionValue r) = this.TryGetValue(key).Result
            r

        member this.TryGetValueSafeTo(key: 'Key, _to) : bool * 'Value option =
            try
                match 
                    this.TryGetValue(key).ResultWithTimeout(_to).Result                    
                    with
                | Some (COptionValue r) ->
                    r
                | _ -> false, None
            with
            | :? System.TimeoutException as te ->
                false, None

        /// 访问 Item：带线程锁的读写访问
        member this.Item
            with get(key: 'Key) =
                (this.TryGetValueSafe key |> snd).Value
            and set(k: 'Key) (v: 'Value) =
                //lock lockObj (fun () -> sortedList.[key] <- value)
                this.TryUpdate(k, v).Result |> ignore

        /// 获取 Count
        member this.CountUnsafe =
            lock lockObj (fun () -> sortedList.Count)

        /// 获取所有 Values
        member this.ValuesUnsafe =
            lock lockObj (fun () -> sortedList.Values)

        /// 获取所有 Keys
        member this.KeysUnsafe =
            lock lockObj (fun () -> sortedList.Keys)

        /// 清空列表
        member this.CleanUnsafe () =
            lock lockObj (fun () -> sortedList.Clear())

        member this.ContainsKeyUnsafe (k:'key) =
            lock lockObj (fun () -> sortedList.ContainsKey k)
        
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

        /// 获取 Count - Safe 版本
        member this.CountSafe : int =
            let (CInt count) = this.Count.Result
            count

        /// 获取所有 Values - Safe 版本
        member this.ValuesSafe : IList<'Value> =
            let (CValueList values) = this.Values.Result
            values

        /// 获取所有 Keys - Safe 版本
        member this.KeysSafe : IList<'Key> =
            let (CKeyList keys) = this.Keys.Result
            keys

        /// 清空列表 - Safe 版本
        member this.CleanSafe() =
            this.Clean().Result |> ignore

        member this.ContainsKeySafe (k:'Key) =
            let (CBool r) = this.ContainsKey(k).Result
            r


module PCSL =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL
    open PB2
    open System.Threading.Tasks

    open FSharp.Collections.ParallelSeq


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

    type PCSLTyp<'Key when 'Key : comparison> =
    | Idx of OpResult<'Key, KeyHash>
    | IdxR of OpResult<KeyHash, 'Key>
    | PS of OpResult<'Key, SortedListPersistenceStatus>

    type TResult<'Key,'Value when 'Key : comparison> = 
        TResultGeneric<OpResult<'Key, 'Value>, 'Key>
        

    //type Task<'T> with
    //    member this.tr<'Key, 'Value when 'Key : comparison> () = 
    //        TResult<'Key, 'Value>(this)

    [<Extension>]
    type TaskExtensions() =
        [<Extension>]
        static member Tr<'Key, 'Value 
            when 'Key : comparison
            >(this: Task<OpResult<'Key, 'Value>>) : TResult<'Key> =
            TResult<'Key>(this)


    module Task =
        //先取到值，再有根據值進行具 IO 副作用的 fun，執行完畢之後回傳值
        let tryGetResultWithTimeout (_toMilli: int) (tasks: Task<'T> seq) : Task<Choice<'T seq, TimeoutException, (int * AggregateException) seq>> =
            task {
                // 创建一个超时任务，表示3秒后触发
                let timeoutTask = 
                    task {
                        do! Task.Delay(_toMilli)
                        return Unchecked.defaultof<'T>
                    }

                let allTasks = tasks |> Seq.append [timeoutTask]
                // 等待任务开始执行和超时处理
                let! completedTask = Task.WhenAny allTasks
        
                if completedTask = timeoutTask then
                    // 超时
                    printfn "Task timed out."
                    return Choice2Of3 (new TimeoutException ())
                else
                    // 任务已完成
                    if tasks |> Seq.exists (fun t -> t.IsFaulted) then
                        let rtn = 
                            tasks |> Seq.indexed |> Seq.choose (fun (i, t) ->
                                if t.IsFaulted then
                                    Some (i, t.Exception)
                                else
                                    None
                            )
                        return Choice3Of3 rtn
                    else
                        let results = tasks |> Seq.map (fun f -> f.Result)
                        return Choice1Of3 results
                    
            }
            //|> Async.RunSynchronously
        let WaitAllWithTimeout (_toMilli:int) (tasks: Task seq) =
            task {
                // 创建一个超时任务，表示3秒后触发
                let timeoutTask = Task.Delay(_toMilli)

                let allTasks = tasks |> Seq.append [timeoutTask]
                // 等待任务开始执行和超时处理
                let! completedTask = Task.WhenAny allTasks

                if completedTask = timeoutTask then
                    // 超时
                    printfn "Task timed out."
                    return Choice2Of3 (new TimeoutException ())
                else
                    // 任务已完成
                    if tasks |> Seq.exists (fun t -> t.IsFaulted) then
                        let rtn = 
                            tasks |> Seq.indexed |> Seq.choose (fun (i, t) ->
                                if t.IsFaulted then
                                    Some (i, t.Exception)
                                else
                                    None
                            )
                        return Choice3Of3 rtn
                    else
                        return Choice1Of3 ()
            }


    type PersistedConcurrentSortedList<'Key, 'Value when 'Key : comparison>(maxDoP: int, basePath: string, schemaName: string) =
        let sortedList = ConcurrentSortedList<'Key, 'Value>()
        let sortedListStatus = ConcurrentSortedList<'Key, SortedListLogicalStatus>()
        let sortedListPersistenceStatus = ConcurrentSortedList<'Key, SortedListPersistenceStatus>()
        let sortedListIndexReversed = ConcurrentSortedList<KeyHash, 'Key>()
        let sortedListIndex         = ConcurrentSortedList<'Key, KeyHash>()
        let schemaPath = Path.Combine(basePath, schemaName)
        let keysPath = Path.Combine(basePath, schemaName, "__keys__")
        let mutable initialized = false

        let createPath p =
            // 创建 schema 文件夹和索引文件夹
            if not (Directory.Exists p) then
                Directory.CreateDirectory p |> ignore


        let indexInitialization () =
            sortedListIndex.Clean().Tr().WaitIgnore
            sortedListIndexReversed.Clean().Tr()
            sortedListPersistenceStatus.Clean().Tr()
            let di = DirectoryInfo keysPath
            di.GetFiles()
            |> PSeq.ordered
            |> PSeq.withDegreeOfParallelism maxDoP
            |> PSeq.iter (fun fi ->
                let key = ModelContainer<'Key>.readFromFileBase fi.FullName
                let baseName = fi.Name.Replace(".index", "")
                let idx = sortedListIndex.Add(key, baseName).Tr()
                let idxR = sortedListIndexReversed.Add(baseName, key).Tr()
                let ps = sortedListPersistenceStatus.Add(key, NonBuffered).Tr()
                let ts = 
                    [|
                        idx.thisT //索引跟 key 必須是一致的
                        idxR.thisT //索引跟 key 必須是一致的
                        ps.thisT
                    |]
                    |> Task.WaitAllWithTimeout 1000
                ()
            )



        do 
            createPath schemaPath
            createPath keysPath
            indexInitialization ()
            initialized <- true

        // 生成 SHA-256 哈希
        let generateKeyHash (key: 'Key) : KeyHash =
            ModelContainer<'Key>.getHashStr key

        let getOrNewKeyHash (key: 'Key) =
            sortedListIndex.TryGetValueSafeTo(key, 100)

        let getOrNewAndPersistKeyHash (key: 'Key) : KeyHash =
            let kh = 
                match getOrNewKeyHash key with
                | true, (Some k) -> k
                | _ ->
                    generateKeyHash key
            [|
                sortedListIndex.Add(key, kh).Tr().thisT
                sortedListIndexReversed.Add(kh, key).Tr().thisT
            |] |> Task.WaitAll
            kh
        // 存储 value 到文件
        let persistKeyValueBase ifRemoveFromBuffer =
            let inline write (key: 'Key, value: 'Value) =
                let hashKey = getOrNewAndPersistKeyHash key
                let filePath = Path.Combine(schemaPath, hashKey + ".val")
                ModelContainer<'Value>.write2File filePath value
                key
            write >>
            if ifRemoveFromBuffer then
                fun key ->
                    (sortedList.Remove key).Tr().WaitIgnore
                    sortedListPersistenceStatus.TryUpdate (key, NonBuffered)
            else
                fun key ->
                    sortedListPersistenceStatus.TryUpdate (key, Buffered)

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
            match getOrNewKeyHash key with
            | true, (Some keyHash) ->
                let filePath = Path.Combine(schemaPath, keyHash + ".val")
                (Some keyHash), true, ModelContainer<'Value>.readFromFile filePath //None means file not existed
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
                sortedList.Add(key, valueOpt.Value).Tr().Ignore
                valueOpt

        member this.Initialized = initialized
        // 添加 key-value 对
        member this.AddAsync(key: 'Key, value: 'Value) =
            lock sortedList (fun () ->
                [|
                    sortedList.Add(key, value).thisT
                    persistKeyValueNoRemove(key, value).thisT
                |]
            )
        member this.Add(key: 'Key, value: 'Value) =
            this.AddAsync(key, value) |> Task.WaitAll

        member this.AddWithPersistenceAsync(key: 'Key, value: 'Value) =
            this.AddAsync(key, value)[0]

        // 获取 value，如果 ConcurrentSortedList 中不存在则从文件系统中读取
        member this.TryGetValue(key: 'Key) : bool * 'Value option =
            lock sortedList (fun () ->
                let exists, value = sortedList.TryGetValueSafe(key)
                if exists then
                    true, value
                else
                    match readFromFileBaseAndBuffer key with
                    | Some v ->
                        true, Some v
                    | None -> false, None
            )

        member this.TryGetValueAsync(key: 'Key) : Task<bool * 'Value option> =
            task {
                return this.TryGetValue(key)
            }
        // 其他成员可以根据需要进行扩展，比如 Count、Remove 等
