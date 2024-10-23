namespace fs

open System.IO
open System.IO.Compression
open System.Text

module Deflate =
    
    let compress (bytes: byte[]) : byte[] =
        use msi = new MemoryStream(bytes)
        use mso = new MemoryStream()
        use gs = new DeflateStream(mso, CompressionMode.Compress)
        msi.CopyTo(gs)
        gs.Close()  // 确保压缩流已关闭
        mso.ToArray()

    let decompress (bytes: byte[]) : byte[] =
        use msi = new MemoryStream(bytes)
        use mso = new MemoryStream()
        use gs = new DeflateStream(msi, CompressionMode.Decompress)
        gs.CopyTo(mso)
        mso.ToArray()

    let compressString (str: string) : byte[] =
        compress (Encoding.UTF8.GetBytes(str))

    let decompressToString (bytes: byte[]) : string =
        Encoding.UTF8.GetString(decompress(bytes))
