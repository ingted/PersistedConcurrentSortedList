namespace PersistedConcurrentSortedList.Type

#if INTERACTIVE
#r @"nuget: Newtonsoft.Json, 13.0.3"
#r "nuget: NCalc"
#r @"nuget: Serilog, 4.0.1"
#r @"nuget: protobuf-net"
#r @"nuget: Newtonsoft.Json, 13.0.3"
//#r "nuget: FAkka.FsPickler, 9.0.3"
//#r "nuget: FAkka.FsPickler.Json, 9.0.3"
#r @"../../../Libs/FsPickler/bin/net9.0/FsPickler.dll"
#r @"../../../Libs/FsPickler.Json/bin/net9.0/FsPickler.Json.dll"
#r @"nuget: protobuf-net"
#r @"../../..\Libs5\KServer\protobuf-net-fsharp\src\ProtoBuf.FSharp\bin\netstandard2.0\protobuf-net-fsharp.dll"
#load @"Compression.fsx"
#r "nuget: FSharp.Collections.ParallelSeq, 1.2.0"
#endif

open System
open System.Collections
open System.Collections.Generic
//open Newtonsoft.Json

#if NET9_0
open MBrace.FsPickler.Json
open MBrace.FsPickler.Combinators 
#else
open MBrace.FsPickler.nstd20.Json
open MBrace.FsPickler.nstd20.Combinators 
#endif
open ProtoBuf
open ProtoBuf.FSharp
open FSharp.Reflection


[<ProtoBuf.ProtoContract>]
type fCell2<'CellTupleKey when 'CellTupleKey: comparison> =
| B of bool
| S of string
| D of decimal
| A of fCell2<'CellTupleKey> []
| T of 'CellTupleKey * fCell2<'CellTupleKey>
    member this.toJsonString() =
        let rec toJson (f: fCell2<'CellTupleKey>) =
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

    static member compareArrays (arr1: fCell2<'CellTupleKey> array) (arr2: fCell2<'CellTupleKey> array): int =
        Seq.zip arr1 arr2
        |> Seq.tryPick (fun (x, y) ->
            let res = compare x y
            if res = 0 then None else Some res)
        |> Option.defaultValue 0

    static member compareLength (arr1: fCell2<'CellTupleKey> array) (arr2: fCell2<'CellTupleKey> array): int =
        let a1l = if arr1 = null then 0 else arr1.Length
        let a2l = if arr2 = null then 0 else arr2.Length
        compare a1l a2l

    static member Compare (x: fCell2<'CellTupleKey>, y: fCell2<'CellTupleKey>): int =
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
                let lenComp = fCell2.compareLength arr1 arr2
                if lenComp <> 0 then lenComp
                else fCell2.compareArrays arr1 arr2
            | (T (tag1, f1), T (tag2, f2)) ->
                let tagComp = compare tag1 tag2
                if tagComp <> 0 then tagComp
                else fCell2.Compare(f1, f2)
            | (D _, _) -> -1
            | (_, D _) -> 1
            | (S _, (A _ | T _)) -> -1
            | ((A _ | T _), S _) -> 1
            | (A _, T _) -> -1
            | (T _, A _) -> 1

    interface IComparer<fCell2<'CellTupleKey>> with
        override this.Compare(x: fCell2<'CellTupleKey>, y: fCell2<'CellTupleKey>): int =
            fCell2.Compare(x, y)

    member this.s =
        match this with
        | S s -> s
        | _ -> failwith "Not fstring.S."

    member this.d =
        match this with
        | D d -> d
        | _ -> failwith "Not fCell.D."

    member this.a =
        match this with
        | A a -> a
        | _ -> failwith "Not fCell.A."

    member this.aa =
        match this with
        | A a -> a |> Array.map _.a
        | _ -> failwith "Not fCell.AA."

    member this.at =
        match this with
        | A a -> a |> Array.map _.t
        | _ -> failwith "Not fCell.AT."

    member this.``as`` =
        match this with
        | A a -> a |> Array.map _.s
        | _ -> failwith "Not fCell.AS."

    member this.ad =
        match this with
        | A a -> a |> Array.map _.d
        | _ -> failwith "Not fCell.AD."

    member this.t =
        match this with
        | T (k, v) -> k, v
        | _ -> failwith "Not fCell.T."

    member this.ta =
        match this with
        | T (k, A a) -> k, a
        | _ -> failwith "Not fCell.TA."

    member this.ts =
        match this with
        | T (k, S s) -> k, s
        | _ -> failwith "Not fCell.TS."

    member this.td =
        match this with
        | T (k, D d) -> k, d
        | _ -> failwith "Not fCell.TD."

    member this.ToLowerInvariant () =
        match this with
        | S "" -> fCell2<'CellTupleKey>.SNull
        | S s -> s.ToLowerInvariant() |> S
        | _ -> failwith "Not fstring.S."

    static member val CompareFunc : Func<fCell2<'CellTupleKey>, fCell2<'CellTupleKey>, bool> = 
        FuncConvert.FromFunc(fun (x: fCell2<'CellTupleKey>) (y: fCell2<'CellTupleKey>) -> fCell2.Compare(x, y) = 0)

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
    static member val Unassigned    = Unchecked.defaultof<fCell2<'CellTupleKey>>  with get

    static member SIsNullOrEmpty (o:fCell2<'CellTupleKey>) = if box o = null || o = fCell2<'CellTupleKey>.SEmpty || o = fCell2<'CellTupleKey>.SNull then true else false
    static member AIsNullOrEmpty (o:fCell2<'CellTupleKey>) = if box o = null || o = fCell2<'CellTupleKey>.AEmpty || o = fCell2<'CellTupleKey>.ANull then true else false
    static member IsNull (o:fCell2<'CellTupleKey>) = 
        if box o = null || o = fCell2.ANull || o = fCell2<'CellTupleKey>.SNull then true else false

    member this.me =
        match this with
        | S s -> s
        | D d -> d.ToString()
        | A arr -> arr |> Array.map (fun f -> f.me) |> String.concat ", "
        | T (key, value) -> sprintf "%A: %s" key value.me

    member this.Value = this.me


open System.Runtime.CompilerServices

[<Extension>]
module FS =
    let mapper = new Dictionary<Type, fCell2<string> -> obj>()

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
            typeof<fCell2<string> []>, (fun (A a) -> box a)
        )

    let _ =
        mapper.Add(
#if NET9_0
            typeof<string * fCell2<string>>, (fun (T (k, v)) -> box (KeyValuePair.Create(k, v)))
#else
            typeof<string * fCell2<string>>, (fun (T (k, v)) -> box (KeyValuePair(k, v)))
#endif
        )

    [<Extension>]
    let toType (this: fCell2<string>, t:Type) = mapper[t] this



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

