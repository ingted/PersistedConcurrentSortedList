// Script: Shared argument-string splitter for FSI demo scripts.
// Demo亮點: 讓 `defaultArgumentsText` 可用 shell-like quote 規則拆成 argv，再交給 `FAkka.Argu`；符合 Visual Studio FSI 框選執行的操作習慣。
// 重要知識點: demo 腳本應使用 `PL.parseLine [|' '|] (Some '"') None true`，避免再手寫 `defaultArguments = [| ... |]`。

module PL

open System.Text

let parseLine (delimiters: char[]) (quoteChar: char option) (escapeCharOpt: char option) (ifMergeDelimiters: bool) (inputLine: string) =
    let escapeChar = defaultArg escapeCharOpt '\\'
    let result = ResizeArray<string>()
    let field = StringBuilder()
    let mutable inQuotes = false
    let mutable escaped = false
    let mutable fieldStarted = false

    let addField () =
        if fieldStarted || not ifMergeDelimiters then
            result.Add(field.ToString())
            field.Clear() |> ignore
            fieldStarted <- false

    for c in inputLine do
        if escaped then
            field.Append c |> ignore
            fieldStarted <- true
            escaped <- false
        elif c = escapeChar then
            escaped <- true
            fieldStarted <- true
        elif quoteChar = Some c then
            inQuotes <- not inQuotes
            fieldStarted <- true
        elif not inQuotes && Array.contains c delimiters then
            addField ()
        else
            field.Append c |> ignore
            fieldStarted <- true

    if escaped then
        field.Append escapeChar |> ignore

    if inQuotes then
        failwith "parseLine failed: quote not finished"

    addField ()
    result.ToArray()
