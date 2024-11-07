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



[<ProtoBuf.ProtoContract>]
type fstring_legacy =
| S of string
| D of decimal
| A of fstring_legacy []
| T of string * fstring_legacy
    member this.toJsonString() =
        let rec toJson (f: fstring_legacy) =
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

    static member compareArrays (arr1: fstring_legacy array) (arr2: fstring_legacy array): int =
        Seq.zip arr1 arr2
        |> Seq.tryPick (fun (x, y) ->
            let res = compare x y
            if res = 0 then None else Some res)
        |> Option.defaultValue 0

    static member compareLength (arr1: fstring_legacy array) (arr2: fstring_legacy array): int =
        let a1l = if arr1 = null then 0 else arr1.Length
        let a2l = if arr2 = null then 0 else arr2.Length
        compare a1l a2l

    static member Compare (x: fstring_legacy, y: fstring_legacy): int =
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
                let lenComp = fstring_legacy.compareLength arr1 arr2
                if lenComp <> 0 then lenComp
                else fstring_legacy.compareArrays arr1 arr2
            | (T (tag1, f1), T (tag2, f2)) ->
                let tagComp = String.Compare(tag1, tag2, StringComparison.OrdinalIgnoreCase)
                if tagComp <> 0 then tagComp
                else fstring_legacy.Compare(f1, f2)
            | (D _, _) -> -1
            | (_, D _) -> 1
            | (S _, (A _ | T _)) -> -1
            | ((A _ | T _), S _) -> 1
            | (A _, T _) -> -1
            | (T _, A _) -> 1

    interface IComparer<fstring_legacy> with
        override this.Compare(x: fstring_legacy, y: fstring_legacy): int =
            fstring_legacy.Compare(x, y)

    member this.s =
        match this with
        | S s -> s
        | _ -> failwith "Not fstring_legacy.S."

    member this.d =
        match this with
        | D d -> d
        | _ -> failwith "Not fstring_legacy.D."

    member this.ToLowerInvariant () =
        match this with
        | S "" -> fstring_legacy.SNull
        | S s -> s.ToLowerInvariant() |> S
        | _ -> failwith "Not fstring_legacy.S."

    static member val CompareFunc : Func<fstring_legacy, fstring_legacy, bool> = 
        FuncConvert.FromFunc(fun (x: fstring_legacy) (y: fstring_legacy) -> fstring_legacy.Compare(x, y) = 0)

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
    static member val Unassigned    = Unchecked.defaultof<fstring_legacy>  with get

    static member SIsNullOrEmpty (o:fstring_legacy) = if box o = null || o = fstring_legacy.SEmpty || o = fstring_legacy.SNull then true else false
    static member AIsNullOrEmpty (o:fstring_legacy) = if box o = null || o = fstring_legacy.AEmpty || o = fstring_legacy.ANull then true else false
    static member IsNull (o:fstring_legacy) = 
        if box o = null || o = fstring_legacy.ANull || o = fstring_legacy.SNull then true else false

    // 加入 IStringable 接口的實作
#if KATZEBASE
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
#endif

    member this.me =
        match this with
        | S s -> s
        | D d -> d.ToString()
        | A arr -> arr |> Array.map (fun f -> f.me) |> String.concat ", "
        | T (key, value) -> sprintf "%s: %s" key value.me

    member this.Value = this.me
[<ProtoBuf.ProtoContract>]
type fCell<'CellTupleKey when 'CellTupleKey: comparison> =
| S of string
| D of decimal
| A of fCell<'CellTupleKey> []
| T of 'CellTupleKey * fCell<'CellTupleKey>
    member this.toJsonString() =
        let rec toJson (f: fCell<'CellTupleKey>) =
            match f with
            | S s -> sprintf "%s" s
            | D d -> sprintf "%f" d
            | A arr -> 
                let elements = arr |> Array.map toJson |> String.concat ", "
                sprintf "[%s]" elements
            | T (key, value) ->
                let keyStr = sprintf "%A" key
                let valueStr = toJson value
                sprintf "%A: %s" keyStr valueStr
        toJson this

    static member compareArrays (arr1: fCell<'CellTupleKey> array) (arr2: fCell<'CellTupleKey> array): int =
        Seq.zip arr1 arr2
        |> Seq.tryPick (fun (x, y) ->
            let res = compare x y
            if res = 0 then None else Some res)
        |> Option.defaultValue 0

    static member compareLength (arr1: fCell<'CellTupleKey> array) (arr2: fCell<'CellTupleKey> array): int =
        let a1l = if arr1 = null then 0 else arr1.Length
        let a2l = if arr2 = null then 0 else arr2.Length
        compare a1l a2l

    static member Compare (x: fCell<'CellTupleKey>, y: fCell<'CellTupleKey>): int =
        match box x, box y with
        | null, null -> 0
        | _, null -> 
            if x = S null then 0 else 1
        | null, _ ->
            if y = S null then 0 else -1
        | _ ->
            match (x, y) with
            | (D d1, D d2) -> Decimal.Compare(d1, d2)
            | (S s1, S s2) -> String.Compare(s1, s2, StringComparison.OrdinalIgnoreCase)
            | (A arr1, A arr2) ->
                let lenComp = fCell.compareLength arr1 arr2
                if lenComp <> 0 then lenComp
                else fCell.compareArrays arr1 arr2
            | (T (tag1, f1), T (tag2, f2)) ->
                let tagComp = compare tag1 tag2
                if tagComp <> 0 then tagComp
                else fCell.Compare(f1, f2)
            | (D _, _) -> -1
            | (_, D _) -> 1
            | (S _, (A _ | T _)) -> -1
            | ((A _ | T _), S _) -> 1
            | (A _, T _) -> -1
            | (T _, A _) -> 1

    interface IComparer<fCell<'CellTupleKey>> with
        override this.Compare(x: fCell<'CellTupleKey>, y: fCell<'CellTupleKey>): int =
            fCell.Compare(x, y)

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
        | S "" -> fCell<'CellTupleKey>.SNull
        | S s -> s.ToLowerInvariant() |> S
        | _ -> failwith "Not fstring.S."

    static member val CompareFunc : Func<fCell<'CellTupleKey>, fCell<'CellTupleKey>, bool> = 
        FuncConvert.FromFunc(fun (x: fCell<'CellTupleKey>) (y: fCell<'CellTupleKey>) -> fCell.Compare(x, y) = 0)

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
    static member val Unassigned    = Unchecked.defaultof<fCell<'CellTupleKey>>  with get

    static member SIsNullOrEmpty (o:fCell<'CellTupleKey>) = if box o = null || o = fCell<'CellTupleKey>.SEmpty || o = fCell<'CellTupleKey>.SNull then true else false
    static member AIsNullOrEmpty (o:fCell<'CellTupleKey>) = if box o = null || o = fCell<'CellTupleKey>.AEmpty || o = fCell<'CellTupleKey>.ANull then true else false
    static member IsNull (o:fCell<'CellTupleKey>) = 
        if box o = null || o = fCell.ANull || o = fCell<'CellTupleKey>.SNull then true else false

    // 加入 IStringable 接口的實作
#if KATZEBASE
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
#endif

    member this.me =
        match this with
        | S s -> s
        | D d -> d.ToString()
        | A arr -> arr |> Array.map (fun f -> f.me) |> String.concat ", "
        | T (key, value) -> sprintf "%A: %s" key value.me

    member this.Value = this.me


[<ProtoBuf.ProtoContract>]
type fstring = fCell<string>

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
    let toF(d : decimal) = D d
[<Extension>]
module ExtensionsDouble =
    [<Extension>]
    let toF(d : double) = D (decimal d)

[<Extension>]
module ExtensionsInt =
    [<Extension>]
    let toF(d : int) = D (decimal d)

module PB =
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