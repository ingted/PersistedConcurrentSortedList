More example here
https://github.com/ingted/PersistedConcurrentSortedList.Test


# 2024-12-01: IgnoreQ mode supported, Ultra fast valueInitialize bug fixed!
# Supported Array/Struct similar with BigQuery in C# and F#
# Usage: take a look at QuickStart.fsx (module PCSLTest)
# ProtoBuf.FSharp mod: (if you don't want to use FAkka.ProtoBuf.FSharp) 


Actually you could use the official ProtoBuf.FSharp, just replace deserialiseConcreteType in PB with (adding it yourself)

```
let deserialiseConcreteType<'t> (model: RuntimeTypeModel) (stream: Stream) = 
    // 判斷 't 是否為泛型類別
    let tType = typeof<'t>
    let actualType =
        if tType.IsGenericType then
            // 如果是泛型類別，則根據定義創建具體的類型
            let genericTypeDefinition = tType.GetGenericTypeDefinition()
            let genericArguments = tType.GetGenericArguments()
            genericTypeDefinition.MakeGenericType(genericArguments)
        else
            // 如果不是泛型類別，直接返回 typeof<'t>
            tType

    // 使用動態生成的類型來反序列化
    model.Deserialize(stream, null, actualType) :?> 't
```

Quick start: (dotnet fsi QuickStart.fsx)

```
let pcsl = PersistedConcurrentSortedList<string, fstring>(
    20, @"c:\pcsl", "test"
    , PCSLFunHelper<string, fstring>.oFun
    , PCSLFunHelper<string, fstring>.eFun)


let s = [|1..200000|]|>Array.map (fun i -> S $"{i}")

let success = pcsl.Add ("OGC", (A [| S "GG"|]), 3000)
let success2 = pcsl.Add ("ORZ2", (A s), 3000)

pcsl.Update ("ORZ", (S "ORZ"), 3000)

pcsl.Update ("", (S "ORZ"), 3000)

pcsl.Upsert ("123456", (S "ORZ1"), 3000)
pcsl.Upsert ("123456", (S "ORZ"))

pcsl.Remove ("123456", 3000)
pcsl.Remove ("123456")

pcsl.TryGetValue ("ORZ2")
pcsl.TryGetValue ("ORZ") |> snd |> _.Value |> (fun (A o) -> o.Length)
pcsl.TryGetValue ("OGC")

pcsl["ORZ2"]

pcsl.GenerateKeyHash "ORZ"
pcsl.GetOrNewKeyHash "ORZ2"

pcsl._base._base
pcsl._idx._base.Keys |> Seq.toArray
pcsl._idxR._base.Keys
pcsl._pstatus._base.Keys |> Seq.toArray
pcsl._pstatus._base.Values|> Seq.toArray


pcsl._idx._base.Keys |> Seq.toArray |> Array.iter (fun (SLK k) ->
    pcsl.RemoveAsync k |> ignore
)

pcsl.InfoPrint

```
