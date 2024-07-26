function Log($file, $msg) {
    $msg = "[" + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") + "] " + $msg
    for ($i=0; $i -lt 9999; $i++) {
        try {
            Add-Content -Path $file -Value $msg -ErrorAction Stop
            break
        }
        catch { Start-Sleep -Milliseconds 10 }
    }
}

function Log-Status($file, $jobs) {
    $msg = ''
    foreach ($job in $jobs) {
        $duration = New-TimeSpan -Start $job.PSBeginTime -End (Get-Date)
        $msg += ($job.State + ' | ' + $job.Name + ' | ' + $duration + "`r`n")
    }
   $msg = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") + "`r`n" + $msg

    for ($i=0; $i -lt 9999; $i++) {
        try {
            Set-Content -Path $file -Value $msg -NoNewline -ErrorAction Stop
            break
        }
        catch { Start-Sleep -Milliseconds 10 }
    }
}

function Set-SQLJobs-State($ins, $jobs, $state) {
    foreach ($job in $jobs) {
        $sql = "EXEC dbo.sp_update_job @job_name='$job', @enabled=$state;`r`nGO"

        # Try three times
        for ($i=1; $i -le 3; $i++) {
            try {
                Invoke-Sqlcmd -ServerInstance $ins -Database 'msdb' -Query $sql -ErrorAction Stop -MultiSubnetFailover
                break
            }
            catch {
                Log -file $mainLog -msg $sql
                Log -file $mainLog -msg $_.Exception.Message
                if ($i -eq 3) { return $false }
                else { Start-Sleep -Milliseconds 100 }
            }
        }

        Start-Sleep -Milliseconds 100
    }
    return $true
}

function Clear-DirString($str) {
    $clr = $str.Replace('\','_').Replace(';','_')
    return $clr
}

function Create-Dir($path) {
    if (!(Test-Path -Path $path)) { $ret = New-Item -Path $path -ItemType Directory }
}

function Get-DBParams($params, $db) {
    # Default from instance
    $dbParams = @{}
    $dbParams.min_page_count = $params.min_page_count
    $dbParams.min_frag_pct_reorganize = $params.min_frag_pct_reorganize
    $dbParams.min_frag_pct_rebuild = $params.min_frag_pct_rebuild
    $dbParams.offline_rebuild = $params.offline_rebuild

    # Parameters for the database
    if ($params.databases.$db) {
        $p = $params.databases.$db
        if ($p.min_page_count | Out-String) { $dbParams.min_page_count = $p.min_page_count }
        if ($p.min_frag_pct_reorganize | Out-String) { $dbParams.min_frag_pct_reorganize = $p.min_frag_pct_reorganize }
        if ($p.min_frag_pct_rebuild | Out-String) { $dbParams.min_frag_pct_rebuild = $p.min_frag_pct_rebuild }
        if ($p.offline_rebuild | Out-String) { $dbParams.offline_rebuild = $p.offline_rebuild }
    }

    return $dbParams
}

#---------------------------------------------------------------------------------------------------------------------------------------------------------

$jobFunctions = {
function Log($file, $msg) {
    $msg = "[" + (Get-Date).ToString("yyyy-MM-dd HH:mm:ss") + "] " + $msg
    for ($i=0; $i -lt 9999; $i++) {
        try {
            Add-Content -Path $file -Value $msg -ErrorAction Stop
            break
        }
        catch { Start-Sleep -Milliseconds 10 }
        $cnt -= 1
    }
}

function Set-RecoveryModel($server, $database, $model) {
    $sql = "ALTER DATABASE [$database] SET RECOVERY $model WITH NO_WAIT;"
    $ret = Run-Query -query $sql -server $server -database 'master'
    return $ret
}

function Run-Query($query, $server, $database='master') {
    # Return code
    # 0 - ALL OK
    # 1 - Other error
    # 2 - An online operation cannot be performed for index
    # 3 - Cannot continue the execution because the session is in the kill state

    try {
        $ret = %{ Invoke-Sqlcmd -Query $query -ServerInstance $server -Database $database -QueryTimeout 0 -ErrorAction Stop -Verbose -MultiSubnetFailover } 4>&1
    }
    catch {
        Log -file $jobLog -msg ('ERROR: ' + $_.Exception.Message)

        if ($_.Exception.Message -like '*An online operation cannot be performed for index*' ) { return 2,$ret }
        if ($_.Exception.Message -like '*session is in the kill state*' ) { return 3,$ret }
        else { return 1,$ret }
    }
    return 0,$ret
}

} # END jobFunctions

#---------------------------------------------------------------------------------------------------------------------------------------------------------
