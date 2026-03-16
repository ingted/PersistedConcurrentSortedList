//#r @"nuget: PersistedConcurrentSortedList, 9.0.27.317"

#r @"..\..\..\Libs5\KServer\fstring\bin\net9.0\PersistedConcurrentSortedList.dll"
#r @"nuget: protobuf-net.Core"

open PersistedConcurrentSortedList
open PersistedConcurrentSortedList.Type
open PCSL2


let chk cond msg actual =
    if actual <> cond then failwithf "%s: expected %A, got %A" msg cond actual

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

// *** 改為 Map 版本 ***
let t1 : fCell2<string> = T (Map.ofList [("Key1", sHello)])
let t2 : fCell2<string> = T (Map.ofList [("Key2", sHello)])
let t3 : fCell2<string> = T (Map.ofList [("Key1", sWorld)])
let tNested : fCell2<string> = T (Map.ofList [("KK", T (Map.ofList [("GG", D 9487M)]))])

// Tests
n.toJsonString() |> chk "null"    "eval001 failed"
bTrue.toJsonString() |> chk "true"  "eval002 failed"
fCell2<string>.Compare(n, n) |> chk 0    "eval003 failed"
fCell2<string>.Compare(bFalse, bTrue) |> chk -1 "eval004 failed"
fCell2<string>.Compare(bTrue, bFalse) |> chk 1 "eval004.1 failed"
fCell2<string>.Compare(bFalse, d1) |> chk 1    "eval005 failed"
fCell2<string>.Compare(bFalse, sHello) |> chk (-1) "eval006 failed"
fCell2<string>.Compare(S "abc", S "ABC") |> chk -1    "eval007 failed"
fCell2<string>.Compare(d1, d2) |> chk (-1)   "eval008 failed"
fCell2<string>.Compare(sHello, arr) |> chk (-1)  "eval009 failed"
fCell2<string>.Compare(arr, sHello) |> chk 1    "eval010 failed"
fCell2<string>.Compare(arr, t1) |> chk (-1)     "eval011 failed"
fCell2<string>.Compare(t1, arr) |> chk 1        "eval012 failed"
fCell2<string>.Compare(t1, t3) |> chk (-1)      "eval013 failed"
fCell2<string>.Compare(t1, t2) |> chk (-1)      "eval014 failed"

fCell2<string>.compareArrays [| sHello; d1 |] [| sHello; d1 |] |> chk 0    "eval015 failed"
fCell2<string>.compareArrays [| sHello; d1 |] [| sHello; d2 |] |> chk (-1) "eval016 failed"
fCell2<string>.compareArrays [||] [||] |> chk 0  "eval017 failed"
let arrNullArr : fCell2<string> array = null
fCell2<string>.compareArrays arrNullArr [||] |> chk -1   "eval018 failed"
fCell2<string>.compareArrays [||] arrNullArr |> chk 1   "eval019 failed"
fCell2<string>.compareLength arrNullArr [||] |> chk 0   "eval020 failed"
fCell2<string>.compareLength [| sHello |] [||] |> chk 1 "eval021 failed"
fCell2<string>.compareLength [||] [| sHello |] |> chk (-1) "eval022 failed"

fCell2<string>.IsNull n |> chk true    "eval023 failed"
fCell2<string>.IsNull sNull |> chk true "eval024 failed"
fCell2<string>.IsNull sHello |> chk false "eval025 failed"
fCell2<string>.SIsNullOrEmpty sEmpty |> chk true  "eval026 failed"
fCell2<string>.SIsNullOrEmpty sNull |> chk true   "eval027 failed"
fCell2<string>.SIsNullOrEmpty sHello |> chk false "eval028 failed"
fCell2<string>.AIsNullOrEmpty arrEmptyVal |> chk true  "eval029 failed"
fCell2<string>.AIsNullOrEmpty arrNullVal |> chk true   "eval030 failed"
fCell2<string>.AIsNullOrEmpty arr |> chk false         "eval031 failed"

sHello.ToLowerInvariant().s |> chk "hello" "eval032 failed"
fCell2<string>.SEmpty.ToLowerInvariant() |> chk (fCell2<string>.SNull) "eval033 failed"
sHello.s |> chk "Hello" "eval034 failed"
d1.d |> chk 1.23M    "eval035 failed"
arr.a.[0] |> chk sHello "eval036 failed"

// 改為 Map 後，直接檢查 Map 內容
let t1map = t1.t
t1map.["Key1"] |> chk sHello "eval037 failed"

// ts → array of pairs
t1.ts.[0] |> chk ("Key1", "Hello") "eval038 failed"

// JSON 輸出
t1.toJsonString() |> chk "{\"Key1\":\"Hello\"}" "eval039 failed"

// nested
tNested.toJsonString() |> chk "{\"KK\":{\"GG\":9487}}" "eval039.1 failed"

arr.toJsonString() |> chk "[\"Hello\",1.23,true,null]" "eval040 failed"

printfn "All tests passed."