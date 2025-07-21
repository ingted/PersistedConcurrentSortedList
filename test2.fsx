//#r @"nuget: PersistedConcurrentSortedList, 9.0.27.317"
#r @"..\..\..\Libs5\KServer\fstring\bin\net9.0\PersistedConcurrentSortedList.dll"
#r @"nuget: protobuf-net.Core"

open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type
open PCSL2

open PersistedConcurrentSortedList.Type

let chk cond msg actual =
    if actual <> cond then failwith msg

// Setup test values
let n : fCell2<string> = N ()
let bTrue : fCell2<string> = B true
let bFalse : fCell2<string> = B false
let sHello : fCell2<string> = S "Hello"
let sWorld : fCell2<string> = S "World"
let sNull = fCell2<string>.SNull
let sEmpty = fCell2<string>.SEmpty
let d1 : fCell2<string> = D 1.23M
let d2 : fCell2<string> = D 2.0M
let arrElems = [| sHello; d1; bTrue; n |]
let arr : fCell2<string> = A arrElems
let arrEmptyVal = fCell2<string>.AEmpty
let arrNullVal = fCell2<string>.ANull
let t1 : fCell2<string> = T ("Key1", sHello)
let t2 : fCell2<string> = T ("Key2", sHello)
let t3 : fCell2<string> = T ("Key1", sWorld)

// Tests
n.toJsonString() |> chk "null"    "eval001 failed"   // N -> null
bTrue.toJsonString() |> chk "true"  "eval002 failed"   // B true -> "true"
fCell2<string>.Compare(n, n) |> chk 0    "eval003 failed"   // N vs N = 0
fCell2<string>.Compare(bFalse, bTrue) |> chk -1 "eval004 failed"   // false < true
fCell2<string>.Compare(bTrue, bFalse) |> chk 1 "eval004.1 failed"   // false < true
fCell2<string>.Compare(bFalse, d1) |> chk 1    "eval005 failed"   // B false > D 1.23
fCell2<string>.Compare(bFalse, sHello) |> chk (-1) "eval006 failed"  // B false < S "Hello"
fCell2<string>.Compare(S "abc", S "ABC") |> chk -1    "eval007 failed"   // case-insensitive S compare

//System.String.Compare("abc", "aCC", System.StringComparison.Ordinal)
//System.String.Compare("abc", "aBC")

fCell2<string>.Compare(d1, d2) |> chk (-1)   "eval008 failed"   // 1.23 < 2.0
fCell2<string>.Compare(sHello, arr) |> chk (-1)  "eval009 failed"   // S < A
fCell2<string>.Compare(arr, sHello) |> chk 1    "eval010 failed"   // A > S
fCell2<string>.Compare(arr, t1) |> chk (-1)     "eval011 failed"   // A < T
fCell2<string>.Compare(t1, arr) |> chk 1        "eval012 failed"   // T > A
fCell2<string>.Compare(t1, t3) |> chk (-1)      "eval013 failed"   // same key, "Hello" < "World"
fCell2<string>.Compare(t1, t2) |> chk (-1)      "eval014 failed"   // "Key1" < "Key2"
fCell2<string>.compareArrays [| sHello; d1 |] [| sHello; d1 |] |> chk 0    "eval015 failed"   // arrays equal
fCell2<string>.compareArrays [| sHello; d1 |] [| sHello; d2 |] |> chk (-1) "eval016 failed"   // 1.23 < 2.0 in arrays
fCell2<string>.compareArrays [||] [||] |> chk 0  "eval017 failed"   // empty vs empty
let arrNullArr : fCell2<string> array = null
let arr1 : fCell2<string> array = null
let arr2 = [||]
fCell2<string>.compareArrays arrNullArr [||] |> chk -1   "eval018 failed"   // null vs empty -> equal
fCell2<string>.compareArrays [||] arrNullArr |> chk 1   "eval019 failed"   // empty vs null -> equal
fCell2<string>.compareLength arrNullArr [||] |> chk 0   "eval020 failed"   // null vs empty length
fCell2<string>.compareLength [| sHello |] [||] |> chk 1 "eval021 failed"   // length 1 > length 0
fCell2<string>.compareLength [||] [| sHello |] |> chk (-1) "eval022 failed" // length 0 < length 1
fCell2<string>.IsNull n |> chk true    "eval023 failed"   // N is null
fCell2<string>.IsNull sNull |> chk true "eval024 failed"  // S null is null
fCell2<string>.IsNull sHello |> chk false "eval025 failed" // non-null
fCell2<string>.SIsNullOrEmpty sEmpty |> chk true  "eval026 failed"  // S ""
fCell2<string>.SIsNullOrEmpty sNull |> chk true   "eval027 failed"  // S null
fCell2<string>.SIsNullOrEmpty sHello |> chk false "eval028 failed"  // S "Hello" not null/empty
fCell2<string>.AIsNullOrEmpty arrEmptyVal |> chk true  "eval029 failed"  // A []
fCell2<string>.AIsNullOrEmpty arrNullVal |> chk true   "eval030 failed"  // A null
fCell2<string>.AIsNullOrEmpty arr |> chk false         "eval031 failed"  // A with content
sHello.ToLowerInvariant().s |> chk "hello" "eval032 failed"             // "Hello".ToLower -> "hello"
fCell2<string>.SEmpty.ToLowerInvariant() |> chk (fCell2<string>.SNull) "eval033 failed"  // "" -> null
sHello.s |> chk "Hello" "eval034 failed"    // .s property
d1.d |> chk 1.23M    "eval035 failed"       // .d property
arr.a.[0] |> chk sHello "eval036 failed"    // .a property (first element)
t1.t |> chk ("Key1", sHello) "eval037 failed"   // .t property
t1.ts |> chk ("Key1", "Hello") "eval038 failed" // .ts property (key, string)
t1.toJsonString() |> chk "{\"Key1\":\"Hello\"}" "eval039 failed"   // T to JSON ("Key1": Hello)

//let escape (s:string) =
//    s.Replace("\\", "\\\\").Replace("\"", "\\\"")
//let rec toJson (f: fCell2<'CellTupleKey>) =
//    match f with
//    | N _   -> "null"
//    | B true  -> "true"
//    | B false -> "false"
//    | D d   -> d.ToString(System.Globalization.CultureInfo.InvariantCulture)
//    | S s   -> "\"" + (escape s) + "\""
//    | A arr ->
//        let elems = arr |> Array.map toJson |> String.concat ","
//        "[" + elems + "]"
//    | T (key, value) ->
//        // 如果 key 本身是 string，也要跳脫；否則就用 %A
//        let keyStr =
//            match box key with
//            | :? string as ks -> "\"" + (escape ks) + "\""
//            | _                -> sprintf "%A" key
//        "{" + keyStr + ":" + toJson value + "}"

//toJson t1


//toJson (T("KK", T ("GG", D 9487M)))
//toJson (A [|T("KK", T ("GG", S "O\"MG"))|])

arr.toJsonString() |> chk "[\"Hello\",1.23,true,null]" "eval040 failed"  // array to JSON

printfn "All tests passed."