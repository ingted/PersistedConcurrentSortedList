namespace PersistedConcurrentSortedList
open System
open System.Collections.Generic
open System.Runtime.CompilerServices

type MemoryExtensions =
    [<Extension>]
    static member ToIList<'T>(memory: Memory<'T>) : IList<'T> =
        memory.ToArray() :> IList<'T>