# Usage: take a look at PTEST module
# ProtoBuf.FSharp: 

Actually you could use the official ProtoBuf.FSharp, just replace deserialiseConcreteType in PB2 with

```
    let deserialiseConcreteType<'t> (model: RuntimeTypeModel) (stream: Stream) = 
        // 判斷 't 是否為泛型類別
        let tType = typeof<'t>
        let actualType =
            if tType.IsGenericType then
                // 如果是泛型類別，則根據定義創建具體的類型
                let genericTypeDefinition = tType.GetGenericTypeDefinition()
                let genericArguments = tType.GetGenericArguments()
                genericTypeDefinition.MakeGenericType(genericArguments)
            else
                // 如果不是泛型類別，直接返回 typeof<'t>
                tType

        // 使用動態生成的類型來反序列化
        model.Deserialize(stream, null, actualType) :?> 't
```
