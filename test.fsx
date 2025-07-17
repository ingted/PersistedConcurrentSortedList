#r @"nuget: PersistedConcurrentSortedList, 9.0.27.16"

#I @"../../../../sharftrade9/πÍ≈Á/SharFTrade.Exp/bin2/net9.0"
#r "SharFTrade.Exp.2025.dll"
//#r "PersistedConcurrentSortedList.dll"
#r "FSharp.Collections.ParallelSeq.dll"

open PersistedConcurrentSortedList
open PCSL2
open CFar

let pid = System.Diagnostics.Process.GetCurrentProcess().Id
let wrTest = newES1kPCSL_Legacy_byDay @"H:\wrTest"
let pcsl = newES1kPCSL_Legacy_byDay @"H:\cFar2"

let chk (cond) s (f) = if f <> cond then failwith s

let keys = pcsl._idx.Keys.Result.KeyList.Value 

keys.Count |> chk 3737 "T00001.001"


wrTest[20250703L] <- D 9487M
printfn "[PID] %d" pid
wrTest[20250703L] |> chk (D 9487M) "T10001.001"

wrTest._base.Keys.Result.KeyList