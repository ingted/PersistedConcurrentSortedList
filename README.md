# PersistedConcurrentSortedList

`PersistedConcurrentSortedList` is an F# library for ordered key/value storage with on-disk persistence.

It is designed for workloads where:

- the dataset is loaded once or rebuilt at startup,
- reads dominate after initialization,
- only occasional small updates are expected,
- ordered queries such as `FirstLastN*` still matter.

## Features

- ordered in-memory index over persisted values
- `Add`, `Update`, `Upsert`, and `Remove`
- `TryGetValue`, key hashing helpers, and buffered/non-buffered value loading
- protobuf and FsPickler serialization paths
- support for F# records and discriminated unions in the value path

## Package

NuGet package id:

```text
PersistedConcurrentSortedList
```

## Quick Start

See [QuickStart.fsx](QuickStart.fsx) for a runnable sample.

```fsharp
let pcsl =
    PersistedConcurrentSortedList<string, fstring>(
        20,
        @"c:\pcsl",
        "test",
        PCSLFunHelper<string, fstring>.oFun,
        PCSLFunHelper<string, fstring>.eFun
    )

pcsl.Add("OGC", A [| S "GG" |], 3000) |> ignore
pcsl.Upsert("123456", S "ORZ")

let found, cell = pcsl.TryGetValue("OGC")
```

## Serialization

The library currently supports:

- `FAkka.ProtoBuf.FSharp`
- `FAkka.FsPickler`
- `FAkka.FsPickler.Json`

The protobuf path includes the compatibility handling required by the current `fCell2<'T>` model used in this repository.

## Documentation

Additional design and test documents are under [doc/](doc/README.md).

## Development

Build:

```bash
dotnet build PersistedConcurrentSortedList.fsproj
```

Pack:

```bash
dotnet pack PersistedConcurrentSortedList.fsproj -c Release
```
