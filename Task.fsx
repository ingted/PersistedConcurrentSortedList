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

open System.Threading.Tasks

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

