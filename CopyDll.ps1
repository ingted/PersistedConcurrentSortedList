param(
    $assembly
)
write-host ("[CopyDll]" + $assembly + ': Current path: ' + (pwd).path)
cd ./bin

copy net9.0\PersistedConcurrentSortedList.dll ../../../..\實驗\SharFTrade.Exp\bin2\net9.0


cd ../