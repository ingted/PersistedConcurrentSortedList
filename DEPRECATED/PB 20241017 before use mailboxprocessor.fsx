namespace fs

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
#r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\Debug\netstandard2.0\protobuf-net-fsharp.dll"
#load @"Compression.fsx"
#r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
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


type fsk = fstring
type fsv = fstring

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
#if orz
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
#endif
module PB2 =
    open System.IO
    open ProtoBuf.Meta
    open ProtoBuf.FSharp
    open System.Security.Cryptography
    open System.Text
    open fs
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
                printfn $"?????????????????????????? {typeof<'T>.Name} ??????????????????????????"
                RuntimeTypeModel.Create(typeof<'T>.Name)
                |> fun m ->
                    if FSharpType.IsUnion typeof<'T> then
                        Serialiser.registerUnionIntoModel<'T> m
                    elif FSharpType.IsRecord typeof<'T> then
                        Serialiser.registerRecordIntoModel<'T> m
                    else
                        m
            )
            with get, set

        static member serializeFBase (m, (ms, o)) =
#if DEBUG1
            printfn "[serializeF] type: %s, %A" (o.GetType().Name) o
#endif
            Serialiser.serialise m ms o

        static member deserializeFBase (m, ms) =
#if DEBUG1
            printfn "[deserializeF] 'T: %s" typeof<'T>.Name
#endif
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

#if ORZ
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

    b.WaitAsync(TimeSpan.FromMilliseconds 1000.0).Result

    [|b:>Task|] |> Task.WaitAll

    Async.RunSynchronously(
        task {
            return! b
        }
        |> Async.AwaitTask, 3000)
#endif

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

    type OpResult<'Key, 'Value when 'Key : comparison> =
    | CUnit
    | CAdded of bool
    | CUpdated
    | CBool of bool
    | COptionValue of (bool * 'Value option)
    | CInt of int
    | CKeyList of IList<'Key>
    | CValueList of IList<'Value>

#if ORZ

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
#endif

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


    type ConcurrentSortedList<'Key, 'Value, 'OpResult, 'ID
        when 'Key : comparison
        and 'Value: comparison
        
        >(
        slId:'ID
        , outFun: 'ID -> OpResult<'Key, 'Value> -> 'OpResult
        , extractFunBase:'ID -> 'OpResult -> OpResult<'Key, 'Value>
        ) =
        let sortedList = SortedList<'Key, 'Value>()
        let lockObj = obj()
        let opQueue = ConcurrentQueue<Task<'OpResult>>() // 操作任务队列
        let extractFun = extractFunBase slId

        /// 运行队列中的任务
        let rec run () =
            async {
                match opQueue.TryDequeue() with
                | true, t -> 
#if DEBUG1
                    printfn "[%A] dequeued %A" slId t
#endif
                    t.RunSynchronously()  // 同步执行任务
                    do! run ()
                | false, _ -> 
#if DEBUG1
                    printfn "[%A] dequeued nothing" slId
#endif
                    ()
            }

        member this.id = slId

        member this.Run () =
            lock lockObj (fun () ->
#if ASYNC1
                run () |> Async.Start
#else
                run () |> Async.RunSynchronously |> ignore
#endif
            )
        /// 基础的 Add 操作（直接修改内部数据结构）
        member this.AddBase(key: 'Key, value: 'Value) =
            //sortedList.Add(key, value)
            let added = sortedList.TryAdd(key, value)
            printfn "[%A] %A, %A added" slId key value
            added

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
#if DEBUG1
                printfn "%A Current Op: %A, %s, %s" slId op (getTypeName typeof<'Key>) (getTypeName typeof<'Value>)
#endif
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

                | CUpsert (k, v) ->
                    fun () ->
                        this.TryUpsertBase(k, v)

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
                |> createTask
#if DEBUG1
            printfn "Enqueuing op %A" op
#endif
            // 将操作添加到队列并运行队列
            opQueue.Enqueue taskToEnqueue
            this.Run ()

            taskToEnqueue

        /// Add 方法：将添加操作封装为任务并执行
        member this.Add(k, v) =
            this.LockableOp(CAdd(k, v))

        member this.Upsert(k, v) =
            this.LockableOp(CUpsert(k, v))

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
#if DEBUG1            
            printfn "TryGetValue"
#endif
            this.LockableOp(CGet key)

        member this.TryGetValueSafe(key: 'Key) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafe"
#endif
            let (COptionValue r) = this.TryGetValue(key).Result |> extractFun
            r

        member this.TryGetValueSafeTo(key: 'Key, _toMilli:int) : bool * 'Value option =
#if DEBUG1
            printfn "TryGetValueSafeTo"
#endif
            try
                let (COptionValue r) = this.TryGetValue(key).WaitAsync(_toMilli).Result |> extractFun
                r
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
            let (CInt count) = this.Count.Result |> extractFun
            count

        /// 获取所有 Values - Safe 版本
        member this.ValuesSafe : IList<'Value> =
            let (CValueList values) = this.Values.Result |> extractFun
            values

        /// 获取所有 Keys - Safe 版本
        member this.KeysSafe : IList<'Key> =
            let (CKeyList keys) = this.Keys.Result |> extractFun
            keys

        /// 清空列表 - Safe 版本
        member this.CleanSafe() =
            this.Clean().Result |> ignore

        member this.ContainsKeySafe (k:'Key) =
            let (CBool r) = this.ContainsKey(k).Result |> extractFun
            r

        member this._base = sortedList
//#if ORZ

module PCSL =

    open System
    open System.IO
    open System.Security.Cryptography
    open System.Text
    open CSL
    open PB2
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
    with
                // KV 的成員處理
        member this.kvAdded =
            let (KV (CAdded a)) = this
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
            let (Idx (CAdded a)) = this
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
            let (IdxR (CAdded a)) = this
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
            let (PS (CAdded a)) = this
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


    module Task =
        //先取到值，再有根據值進行具 IO 副作用的 fun，執行完畢之後回傳值
        ///[DEPRECATED]
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
#if DEBUG1
                    printfn "Task timed out."
#endif
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
            (*
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
            *)
            try
                Task.WaitAll(tasks |> Seq.toArray, TimeSpan.FromMilliseconds _toMilli) |> Choice1Of3
            with
            | :? TimeoutException as te ->
                Choice2Of3 te
            | :? AggregateException as ae ->
#if DEBUG1
                printfn "aeaeaeaeaeae: %A" ae
#endif
                Choice3Of3 ae
            | exn ->
                reraise ()

        let RunSynchronously t =
            t
            |> Async.AwaitTask
            |> Async.RunSynchronously

        let ResultAllWithTimeout (_toMilli:int) (tasks: Task<'T> seq) =

            try
                tasks 
                |> Seq.map Async.AwaitTask
                |> Async.Parallel
                |> fun a ->
                    Async.RunSynchronously(a, _toMilli) |> Choice1Of3
            with
            | :? TimeoutException as te ->
                Choice2Of3 te
            | :? AggregateException as ae ->
                Choice3Of3 ae


        

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
        , oFun, eFun
        ) =
        let js = JsonSerializer()
        let sortedList = PCSL<'Key, 'Value, SLTyp>(TSL, oFun, eFun)
        let sortedListStatus = PCSL<'Key, 'Value, SLTyp>(TSLSts, oFun, eFun)
        let sortedListPersistenceStatus = PCSL<'Key, 'Value, SLTyp>(TSLPSts, oFun, eFun)
        let sortedListIndexReversed = PCSL<'Key, 'Value, SLTyp>(TSLIdxR, oFun, eFun)
        let sortedListIndex         = PCSL<'Key, 'Value, SLTyp>(TSLIdx, oFun, eFun)

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
                ModelContainer<'Value>.write2File filePath value
                key
            write >>
            if ifRemoveFromBuffer then
                fun key ->
                    (sortedList.Remove (SLK key)).WaitIgnore
                    sortedListPersistenceStatus.TryUpdate (SLK key, SLPS NonBuffered)
            else
                fun key ->
                    sortedListPersistenceStatus.TryUpdate (SLK key, SLPS Buffered)

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

        member this.GenerateKeyHash = generateKeyHash
        member this.GetOrNewKeyHash = getOrNewKeyHash
        member this.GetOrNewAndPersistKeyHash = getOrNewAndPersistKeyHash
        member this.PersistKeyValueNoRemove = persistKeyValueNoRemove
        ///persist 之後移出 buffer (不涉及寫入 base sortedList)
        member this.PersistKeyValueRemove = persistKeyValueRemove
        
        ///persist 之後留在 buffer (不涉及寫入 base sortedList)
        member this.PersistKeyValues = persistKeyValues

        member this.Initialized = initialized
        // 添加 key-value 对
        member this.AddAsync(key: 'Key, value: 'Value) =
#if DEBUG1
#else
            lock sortedList (fun () ->
#endif
            [|
                sortedList.Add(SLK key, SLV value)
                persistKeyValueNoRemove(key,  value)
            |]
#if DEBUG1
#else                
            )
#endif
        member this.Add(key: 'Key, value: 'Value, _toMilli:int) =
            Task.WaitAll(
                this.AddAsync(key, value) |> Array.map (fun t -> t.thisT)
                , _toMilli
            )

        member this.AddWithPersistenceAsync(key: 'Key, value: 'Value) =
            this.AddAsync(key, value)[0]

        member this.AddWithPersistence(key: 'Key, value: 'Value, _toMilli:int) =
            this.AddWithPersistenceAsync(key, value).WaitAsync(_toMilli).Result

        // 获取 value，如果 ConcurrentSortedList 中不存在则从文件系统中读取
        member this.TryGetValue(key: 'Key) = //: bool * 'Value option =
            lock sortedList (fun () ->
#if DEBUG1
                printfn "Query KV"
#endif
                let exists, value = sortedList.TryGetValueSafe(SLK key)
                if exists then
                    let (Some (SLV v)) = value
                    true, Some v
                else
                    match readFromFileBaseAndBuffer key with
                    | Some v ->
                        true, Some v
                    | None -> false, None
            )

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

        member val _base = sortedList                       with get
        member val _idx = sortedListIndex                   with get
        member val _idxR = sortedListIndexReversed          with get
        member val _status = sortedListPersistenceStatus    with get

module PTEST = 

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
    #r @"G:\coldfar_py\sharftrade9\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\Debug\netstandard2.0\protobuf-net-fsharp.dll"
    //#load @"Compression.fsx"
    #r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
    #r @"bin\Debug\net9.0\fstring.dll"
    #endif

    open fs
    open PB2
    open CSL
    open PCSL
    open System.Collections.Generic

    type PFCFSLFunHelper<'Key, 'Value when 'Key: comparison and 'Value: comparison> =
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

            | CAdded added -> 
                match id with
                | TSLIdxR -> IdxR (CAdded added)
                | TSLIdx  -> Idx  (CAdded added)
                | TSLPSts  ->PS   (CAdded added)
                | TSL     -> KV   (CAdded added)
                | _ -> failwith "Unsupported type in CAdded"

            | CUpdated -> 
                match id with
                | TSLIdxR -> IdxR CUpdated
                | TSLIdx  -> Idx  CUpdated
                | TSLPSts -> PS   CUpdated
                | TSL     -> KV   CUpdated
                | _ -> failwith "Unsupported type in CUpdated"

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




        static member eFun (id: SLTyp) (taskResult: PCSLTaskTyp<'Key, 'Value>): OpResult<PCSLKVTyp<'Key, 'Value>, PCSLKVTyp<'Key, 'Value>> =
#if DEBUG1            
            printfn $"SLId: {id}: eFun"
#endif
            match taskResult with
            | KV (o:OpResult<'Key, 'Value>) ->
                match o with
                | CUnit -> 
                    CUnit
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
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
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
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
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
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
                | CAdded added -> 
                    CAdded added
                | CUpdated -> 
                    CUpdated
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

    
    //type fsk = fstring
    //type fsv = Option<fstring>

    let pcsl = PersistedConcurrentSortedList<string, fsv>(
        20, @"c:\pfcfsl", "test"
        , PFCFSLFunHelper<string, fsv>.oFun
        , PFCFSLFunHelper<string, fsv>.eFun)


    let s = [|0..100000|]|>Array.map (fun i -> S $"{i}")

    let success = pcsl.Add ("ORZ", (A [| S "GG"|]), 3000)
    let success2 = pcsl.Add ("ORZ2", (A s), 3000)

    pcsl.TryGetValue ("ORZ")
    pcsl.TryGetValue ("ORZ2")

    
    pcsl.GenerateKeyHash "ORZ"
    pcsl.GetOrNewKeyHash "ORZ2"

    let b = pcsl._base
    pcsl._idx._base.Keys
    pcsl._idxR._base.Keys
    pcsl._status._base.Keys