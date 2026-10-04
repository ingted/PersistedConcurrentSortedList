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

## Native queue regression

Explicit IgnoreQ now propagates through persistence status, buffer removal and RemoveAsync. Default overloads keep queued completion. See [verification](Verification.md) and [Host change](HOST.NativeQueue.md). Run misc/verify.nativeQueue.ps1 with no arguments for PLAN, or with -Execute for synthetic native filesystem tests. This source milestone has not replaced the public 10.1.400 package.

## REL-01 new immutable identity 202610050535
Canonicalpackage Version10.1.400→10.1.401。Source2a138b4 queue/index/valueopt-in fixesunchanged；defaultfalse排隊保證/IFileSystem預設不變。Main Host releaseclosure/currentSA-SD見G:/PulseTrade.fs/doc/PTC HOST 優化/Release.Closure.md。實際NuGet.Versioning minimum400拒400-win1、接受401，故newstablemetadataonly；原public400不覆寫。Baselinecopied14inputsbuild9.084s stablePASS；新version復用verify.nativeQueue.ps1 actual10cases驗證、禁止PackonBuild，fullpackage/Host/prod separate。GenerateNuspec NoBuild跳過本project AfterPack發布hook；不在log記secret，不使用formalenv。新401只reserved，未pack/published，SourceCommit與package payload後續逐一驗。
