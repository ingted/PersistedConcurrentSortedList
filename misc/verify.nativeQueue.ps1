param([switch]$Execute,[string]$CandidateAssembly='',[ValidateRange(30,300)][int]$TimeoutSeconds=120)
$ErrorActionPreference='Stop'
$u=[Text.UTF8Encoding]::new($false,$true)
$repo=[IO.Path]::GetFullPath((Join-Path $PSScriptRoot '..'))
$project=Join-Path $repo 'PersistedConcurrentSortedList.fsproj'
$tests=Join-Path $repo 'tests/PersistedConcurrentSortedList.Tests/PersistedConcurrentSortedList.Tests.fsproj'
if(-not $Execute){[pscustomobject]@{Mode='PLAN';Producer=$project;Tests=$tests;CandidateAssembly=$CandidateAssembly;ExpectedTests='complete merged inventory: queue + RFC + storage + crash + safety';Pack=$false;Production=$false}|ConvertTo-Json -Compress;exit 0}
$root=Join-Path ([IO.Path]::GetTempPath()) ('pcsl-native-queue-verification-'+[Guid]::NewGuid().ToString('N'))
[IO.Directory]::CreateDirectory($root)|Out-Null
[IO.File]::WriteAllText((Join-Path $root 'owner.txt'),'Native unit regression; synthetic filesystem only; no Pack/publication/service mutation.',$u)
$inputs=@($project,$tests,(Join-Path (Split-Path $tests) 'Program.fs'),$PSCommandPath)
[xml]$xml=[IO.File]::ReadAllText($project,$u)
$inputs+=@($xml.SelectNodes('//Compile[@Include]')|ForEach-Object{[IO.Path]::GetFullPath((Join-Path $repo $_.Include))})
[xml]$testXml=[IO.File]::ReadAllText($tests,$u)
$inputs+=@($testXml.SelectNodes('//Compile[@Include]')|ForEach-Object{[IO.Path]::GetFullPath((Join-Path (Split-Path $tests) $_.Include))})
$before=@($inputs|Sort-Object -Unique|ForEach-Object{[pscustomobject]@{Path=$_;SHA256=(Get-FileHash -LiteralPath $_).Hash}})
[IO.File]::WriteAllText((Join-Path $root 'source-before.json'),($before|ConvertTo-Json -Depth 3),$u)
function Run([string]$Name,[string[]]$Arguments){
    $psi=[Diagnostics.ProcessStartInfo]::new('dotnet');$psi.WorkingDirectory=$repo;$psi.UseShellExecute=$false;$psi.CreateNoWindow=$true;$psi.RedirectStandardOutput=$true;$psi.RedirectStandardError=$true
    foreach($a in $Arguments){$psi.ArgumentList.Add($a)}
    $stdout=Join-Path $root ($Name+'.stdout.raw');$stderr=Join-Path $root ($Name+'.stderr.raw');$of=[IO.File]::Create($stdout);$ef=[IO.File]::Create($stderr)
    $clock=[Diagnostics.Stopwatch]::StartNew();$proc=[Diagnostics.Process]::Start($psi);$ot=$proc.StandardOutput.BaseStream.CopyToAsync($of);$et=$proc.StandardError.BaseStream.CopyToAsync($ef)
    try{if(-not $proc.WaitForExit($TimeoutSeconds*1000)){$proc.Kill($true);throw "$Name watchdog"};$ot.GetAwaiter().GetResult()|Out-Null;$et.GetAwaiter().GetResult()|Out-Null}finally{$of.Dispose();$ef.Dispose()}
    [pscustomobject]@{Name=$Name;Exit=$proc.ExitCode;Seconds=[math]::Round($clock.Elapsed.TotalSeconds,3);Raw=$stdout}
}
$phases=[Collections.Generic.List[object]]::new()
$candidate=Join-Path $root 'native/PersistedConcurrentSortedList.dll'
if($CandidateAssembly){
    $given=[IO.Path]::GetFullPath($CandidateAssembly)
    if(-not [IO.File]::Exists($given)){throw 'Candidate DLL does not exist'}
    [IO.Directory]::CreateDirectory((Split-Path $candidate))|Out-Null;[IO.File]::Copy($given,$candidate,$false)
}else{
    $stage=Run 'producer' @('build',$project,'-c','Release','--artifacts-path',(Join-Path $root 'producer-artifacts'),('-p:OutputPath='+(Split-Path $candidate)),'-p:AppendTargetFrameworkToOutputPath=false','-p:GeneratePackageOnBuild=false','-p:PublishNuGetAfterPack=false','--nologo')
    $phases.Add($stage);if($stage.Exit -ne 0){throw "Producer build failed; evidence=$root"}
}
$nativeHash=(Get-FileHash -LiteralPath $candidate).Hash
$testOut=Join-Path $root 'tests'
$stage=Run 'test-build' @('build',$tests,'-c','Release','--artifacts-path',(Join-Path $root 'test-artifacts'),('-p:OutputPath='+$testOut),'-p:AppendTargetFrameworkToOutputPath=false',('-p:PcslAssemblyPath='+$candidate),'-p:GeneratePackageOnBuild=false','--nologo')
$phases.Add($stage);if($stage.Exit -ne 0){throw "Test build failed; evidence=$root"}
if((Get-FileHash -LiteralPath (Join-Path $testOut 'PersistedConcurrentSortedList.dll')).Hash -cne $nativeHash){throw 'Test candidate hash mismatch'}
$inventory=Run 'inventory' @((Join-Path $testOut 'PersistedConcurrentSortedList.Tests.dll'),'--inventory');$phases.Add($inventory)
if($inventory.Exit -ne 0){throw "Inventory failed; evidence=$root"}
$declared=[IO.File]::ReadAllText($inventory.Raw,$u)|ConvertFrom-Json
if($declared.NativeQueue -ne 10 -or $declared.Rfc -ne 13 -or $declared.Storage -le 0 -or $declared.Recovery -le 0 -or $declared.Safety -le 0){throw 'Merged inventory lost required native/RFC/fault cases'}
$expected=[int]$declared.NativeQueue+[int]$declared.Rfc+[int]$declared.Storage+[int]$declared.Recovery+[int]$declared.Safety
$stage=Run 'tests' @((Join-Path $testOut 'PersistedConcurrentSortedList.Tests.dll'));$phases.Add($stage)
$text=[Text.Encoding]::ASCII.GetString([IO.File]::ReadAllBytes($stage.Raw))
$text=[regex]::Replace($text,'\x1B\[[0-?]*[ -/]*[@-~]','')
$summary=[regex]::Match($text,'(\d+) tests run[\s\S]*? (\d+) passed, (\d+) ignored, (\d+) failed, (\d+) errored')
if(-not $summary.Success){throw "Actual suite summary missing; evidence=$root"}
$counts=[ordered]@{Executed=[int]$summary.Groups[1].Value;Passed=[int]$summary.Groups[2].Value;Ignored=[int]$summary.Groups[3].Value;Failed=[int]$summary.Groups[4].Value;Errored=[int]$summary.Groups[5].Value}
$stable=@($before|Where-Object{(Get-FileHash -LiteralPath $_.Path).Hash -cne $_.SHA256}).Count -eq 0
$passed=$stage.Exit -eq 0 -and $counts.Executed -eq $expected -and $counts.Passed -eq $expected -and $counts.Ignored+$counts.Failed+$counts.Errored -eq 0 -and $stable
$result=[pscustomobject]@{Passed=$passed;Counts=$counts;Inventory=$declared;SourceStable=$stable;NativeSHA256=$nativeHash;SourceCandidate=$true;Pack=$false;Production=$false;Phases=$phases;Evidence=$root}
[IO.File]::WriteAllText((Join-Path $root 'result.json'),($result|ConvertTo-Json -Depth 5),$u)
$result|ConvertTo-Json -Depth 5 -Compress
if(-not $passed){exit 1}
exit 0
