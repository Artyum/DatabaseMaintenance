$baseDir = (Split-Path $script:MyInvocation.MyCommand.Path) + '\'
Set-Location -Path $baseDir

$dataDir = $baseDir + 'Data\'
$apiDir = $baseDir + '..\API\'
$logDir = $dataDir + 'Logs\Logs_' + (Get-Date).ToString('yyyy-MM-dd') + '\'

. ($baseDir + 'functions.ps1')

$mainLog = $logDir + 'Maintenance.log'
$statusLog = $logDir + 'Status.log'

$phaze1Log = $logDir + 'Phaze1.log'    # List of indexes interrupted in phase 1
$phaze2Log = $logDir + 'Phaze2.log'    # List of indexes interrupted in phase 2
$rebuildLog = $logDir + 'Rebuild.log'  # List of indexes rebuilt in OFFLINE mode

$jsonFile = $dataDir + 'config.json'

# Counter of parallel threads per instance
$jobCount = @{}

# Number of reindexation attempts
$passCnt = 2

# Indicator whether SQL jobs are running on the instance:
#  0 = jobs are running, maintenance cannot start on the instance
#  1 = maintenance can start on the instance
#  2 = maintenance finished, backup tasks have been enabled
$canStart = @{}

$timerBackup = @{}   # Software timer to check running backups per instance
$timerSuspend = 60   # Software timer to check suspended SQL sessions
$dbList = @{}        # List of databases on each instance
$finish = $false     # Global indicator to end the processing loop

$startTime = (Get-Date)

# Load configuration from json
try {
    $json = Get-Content -Path $jsonFile | Out-String | ConvertFrom-Json
}
catch {
    Log -file $mainLog -msg "Error: Invalid json file"
    exit
}

# Global variables from json
$dryRun = $json.config.dry_run
$alterTimeout = $json.config.timeout_alteridx_min
$updateStatsTimeout = $json.config.timeout_updstats_min

# List of all instances from json
$instances = $json.instances.psobject.properties.Name

# Create log directory
Create-Dir -path $logDir

# Start
$m = 'Database Maintenance START'
if ($dryRun) {
    $m += ' (DRY-RUN)'
    $passCnt = 1
}

Log -file $mainLog -msg $m

# Prepare instances
foreach ($ins in $instances) {
    $jobCount[$ins] = 0      # Reset job counter for the instance to 0
    $canStart[$ins] = 0      # Initial setting before the first check of running jobs
    $timerBackup[$ins] = 0   # Set timer

    # Create log directory for the instance
    $logDirIns = $logDir + (Clear-DirString -str $ins) + '\'
    Create-Dir -Path $logDirIns

    # Disable backup jobs for the instance
    $bckJobs = $json.instances.$ins.backup_jobs
    if ($bckJobs -and $dryRun -eq 0) {
        $m = "Disable backup jobs for $ins"
        Log -file $mainLog -msg $m

        $ret = Set-SQLJobs-State -ins $ins -jobs $bckJobs -state 0
        if ($ret -eq $false) {
            # If there's a problem disabling, the instance is not processed
            $instances = $instances | ?{$_ -ne $ins}
            Log -file $mainLog -msg 'Set-SQLJobs-State Failed'
        }
    }
}

# Generate database list for the instance
foreach ($ins in $instances) {
    $params = $json.instances.$ins

    # If the instance is enabled in json
    if ($params.enabled -eq 1) {

        # Generate include and exclude lists
        if ($params.include) { $incl = "and d.name in ('"+ ($params.include -join "','") +"')" } else { $incl = '' }
        if ($params.exclude) { $excl = "and d.name not in ('"+ ($params.exclude -join "','") +"')" } else { $excl = '' }

        # SQL to retrieve the database list with recovery mode
        # Job statuses:
        #   0 - database to be processed
        #   1 - database being processed
        #   2 - database processed
        $sql = "select
            d.name DBName,
            sum(mf.size/131072) [Size GB],
            d.recovery_model_desc,
            0 [Status]
        from
            sys.master_files mf
            join sys.databases d on d.database_id = mf.database_id
        where
            d.name not in ('master','model','msdb','tempdb','distribution')
            and d.state = 0 /*ONLINE*/
            "+ $incl + "
            "+ $excl + "
        group by
            d.name,
            d.recovery_model_desc
        having
            sum(mf.size/131072) <= " + $params.max_db_size_GB + "
        order by 2 desc, 1"

        # Retrieve database list from the instance
        try {
            $dbs = Invoke-Sqlcmd -ServerInstance $ins -Database 'master' -Query $sql -ErrorAction Stop -MultiSubnetFailover
            if ($dbs) { $dbList[$ins] += $dbs }
        }
        catch {
            Log -file $mainLog -msg ('ERROR: ' + $_.Exception.Message)
        }
    }
}

# Clean up the list of instances from inactive or empty database lists
$instances = $instances | ?{$_ -in $dbList.psbase.Keys}

# Final check of the instance list
if (($instances|measure).Count -eq 0) { $finish = $true }

while (-not $finish) {
    # Start threads for each instance in dbList
    foreach ($ins in $instances) {
        $params = $json.instances.$ins

        # Check if backup jobs are running
        if ($canStart[$ins] -eq 0 -and $timerBackup[$ins] -eq 0) {
            # Delay in seconds
            $timerBackup[$ins] = 60

            # List of running jobs
            $sql = "SELECT
                j.name AS job_name
            FROM
                msdb.dbo.sysjobactivity ja
                JOIN msdb.dbo.sysjobs j ON ja.job_id = j.job_id
            WHERE
                ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
                AND ja.start_execution_date is not null
                AND ja.stop_execution_date is null"

            $chk = 1
            if ($dryRun -eq 0) {
                try {
                    $ret = Invoke-Sqlcmd -ServerInstance $ins -Database 'master' -Query $sql -ErrorAction Stop -MultiSubnetFailover
                    foreach ($j in $ret) { if ($params.backup_jobs.Contains($j.job_name)) { $chk = 0 } }
                }
               catch { $chk = 0 }
            }

            if ($chk -eq 1) { Log -file $mainLog -msg "Starting maintenance on $ins" } else { Log -file $mainLog -msg "Waiting for backup to finish on $ins" }
            $canStart[$ins] = $chk
        }

        # Decrement timer
        if ($timerBackup[$ins] -gt 0) { $timerBackup[$ins]-- }

        # If backup jobs are not running on the instance and the number of running threads is less than max
        if ($canStart[$ins] -eq 1 -and $jobCount[$ins] -lt $params.max_jobs_per_ins) {

            # Retrieve the list of databases to be processed
            $db = @($dbList[$ins] | Where-Object { $_.Status -eq 0 })

            # If there are databases to be processed
            if (($db | measure).Count -gt 0) {

                # Parameters for the database
                $database = $db[0].DBName
                $recModel = $db[0].recovery_model_desc
                $dbParams = Get-DBParams -params $params -db $database

                # Mark the database as "being processed"
                $db[0].status = 1

                # Increase the number of running jobs on the instance
                $jobCount[$ins] += 1

                # Log file for the job
                $jobLog = $logDir + (Clear-DirString -str $ins) + '\' + $database + '.log'

                # Job name
                $jobName = $ins + ';' + $database
                Log -file $mainLog -msg "Start | $jobName"

                $job = Start-Job -Name $jobName -InitializationScript $jobFunctions -ScriptBlock {
                    $startTime = Get-Date

                    # Params
                    $jobIns = $using:ins
                    $jobLog = $using:jobLog
                    $jobP1Log = $using:phaze1Log
                    $jobP2Log = $using:phaze2Log
                    $jobRebuildLog = $using:rebuildLog
                    $jobDB = $using:database
                    $jobRecModel = $using:recModel
                    $jobParams = $using:dbParams
                    $jobPassCnt = $using:passCnt

                    if ($using:dryRun -eq 1) { $run = 0 } else { $run = 1 }

                    # Start
                    if ($run) { Log -file $jobLog -msg "START $jobDB" } else { Log -file $jobLog -msg "START $jobDB DRY RUN" }

                    # Switch the database to SIMPLE recovery model
                    if ($jobRecModel -eq 'FULL') {
                        Log -file $jobLog -msg "Set recovery model SIMPLE"
                        if ($run) {
                            $ret = Set-RecoveryModel -server $jobIns -database $jobDB -model "SIMPLE"
                            if ($ret[0] -ne 0) { return 1 }
                        }
                    }

                    # Retrieve the list of indexes
                    $sql = "select
                        t3.name as table_name,
                        t2.name as index_name,
                        avg(t1.avg_fragmentation_in_percent) 'avg_fragmentation_in_percent',
                        sum(t1.page_count) 'page_count',
                        SCHEMA_NAME(schema_id) 'schema',
                        count(*) 'partitions'
                    from
                        sys.dm_db_index_physical_stats (db_id(),null,null,null,'limited') as t1
                        join sys.indexes as t2 on t1.index_id=t2.index_id and t1.object_id=t2.object_id
                        join sys.tables as t3 on t3.object_id=t1.object_id
                    where
                        t2.name is not null
                        and t3.name is not null
                        and t1.page_count > 0
                    group by
                        t3.name,
                        t2.name,
                        SCHEMA_NAME(schema_id)
                    having
                        avg(t1.avg_fragmentation_in_percent) >= " + $jobParams.min_frag_pct_reorganize + "
                        and sum(t1.page_count) >= " + $jobParams.min_page_count + "
                    order by 3,1,2"

                    $idxList = Run-Query -query $sql -server $jobIns -database $jobDB
                    if ($idxList[0] -ne 0) { return 1 }

                    # Reindexation passes
                    # In the first pass, reorganize is performed. In subsequent passes, only rebuild is performed
                    for ($pass=1; $pass -le $jobPassCnt; $pass++) {

                        $tot = ($idxList[1] | measure).Count
                        $cnt = 1
                        Log -file $jobLog -msg "Pass: $pass/$jobPassCnt | Count: $tot"

                        # Iterate through indexes
                        foreach ($idx in $idxList[1]) {
                            $alter = 'alter index [' + $idx.index_name + '] on [' + $idx.schema + '].[' + $idx.table_name + '] '

                            # Reorganize
                            if ($pass -eq 1 -and $idx.avg_fragmentation_in_percent -lt $jobParams.min_frag_pct_rebuild) {
                                $sql = $alter + 'reorganize'
                                Log -file $jobLog -msg ("$cnt | $sql | " + $idx.avg_fragmentation_in_percent)
                                if ($run) {
                                    $ret = Run-Query -query $sql -server $jobIns -database $jobDB
                                    if ($ret[0] -eq 3) { Log -file $jobP1Log -msg "$jobIns | $jobDB | $sql" }
                                }
                            }

                            # Rebuild
                            else {
                                # $sql = $alter + 'rebuild PARTITION = ALL WITH (ONLINE = ON)'
                                # https://www.brentozar.com/archive/2015/01/testing-alter-index-rebuild-wait_at_low_priority-sql-server-2014/
                                # https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-index-transact-sql?view=sql-server-ver16
                                $sql = $alter + 'rebuild PARTITION = ALL WITH (ONLINE = ON (WAIT_AT_LOW_PRIORITY ( MAX_DURATION = 15 MINUTES, ABORT_AFTER_WAIT = SELF )))'

                                Log -file $jobLog -msg ("$cnt | $sql | " + $idx.avg_fragmentation_in_percent)
                                if ($run) {
                                    $ret = Run-Query -query $sql -server $jobIns -database $jobDB

                                    # If ONLINE rebuild cannot be performed and OFFLINE rebuild is enabled
                                    if ($ret[0] -eq 2 -and $jobParams.offline_rebuild -eq 1) {
                                        $sql = $alter + 'rebuild PARTITION = ALL'
                                        Log -file $jobLog -msg ("$cnt | $sql | " + $idx.avg_fragmentation_in_percent)
                                        Log -file $jobRebuildLog -msg "$jobIns | $jobDB | $sql"
                                        $ret = Run-Query -query $sql -server $jobIns -database $jobDB
                                    }

                                    if ($ret[0] -eq 3) { Log -file $jobP2Log -msg "$jobIns | $jobDB | $sql" }
                                }
                            }

                            # Remove the index from the list after successful processing
                            # After the pass, the list will contain only indexes that could not be processed
                            if ($run -and $ret[0] -eq 0) { $idxList[1] = $idxList[1] | ? {$_.table_name+$_.schema+$_.index_name -ne $idx.table_name+$idx.schema+$idx.index_name} }

                            $cnt++
                        }
                    }

                    # Update all statistics on the database
                    $sql = 'EXEC sp_updatestats;'
                    Log -file $jobLog -msg $sql
                    if ($run) {
                        $ret = Run-Query -query $sql -server $jobIns -database $jobDB
                        if ($ret[0] -eq 0) { Log -file $jobLog -msg $ret[1] }
                    }

                    # Restore the recovery model
                    if ($jobRecModel -eq 'FULL') {
                        Log -file $jobLog -msg "Restore recovery model FULL"
                        if ($run) { Set-RecoveryModel -server $jobIns -database $jobDB -model "FULL" }
                    }

                    $execTime = New-TimeSpan -Start $startTime -End (Get-Date)
                    Log -file $jobLog -msg ("FINISH | " + $execTime.ToString())

                    return 0
                }
            }
        }
    }

    # Job handling
    $jobs = Get-Job
    foreach ($job in $jobs) {
        # If the job is running
        #if ($job.State -eq 'Running') {
            # Check time and possibly kill
            #"Running: " + $job.Name + ' ' + $job.PSBeginTime | Out-Default
        #}

        # If the job is finished
        if ($job.State -ne 'Running' -and $job.State -ne 'NotStarted') {
            # Retrieve the job result
            $ret = Receive-Job -Id $job.Id
            $jobDuration = New-TimeSpan -Start $job.PSBeginTime -End (Get-Date)

            if ($ret -eq 0) { $jobReturn = 'Success' } else { $jobReturn = 'Error' }

            Log -file $mainLog -msg ('Finish | ' + $job.Name + ' | ' + $jobReturn + ' | ' + $jobDuration.ToString())

            $item = $job.Name.Split(';')
            $ins = $item[0]
            $db = $item[1]

            # Decrease the job counter on the instance
            $jobCount[$ins] -= 1

            # Mark the database as done
            ($dbList[$ins] | where {$_.DBName -eq $db}).Status = 2

            # Remove the job
            $job | Remove-Job
        }
    }

    # Kill suspended or long-running sessions
    if ($timerSuspend -eq 0) {
        $timerSuspend = 60

        foreach ($ins in $instances) {
            # Retrieve the list of processes on SQL
            $sql = "SELECT
                p.spid,
                t.text,
                p.login_time,
                p.last_batch,
                r.start_time,
                DB_NAME(p.dbid) [db_name],
                p.status proc_status,
                s.status ses_status,
                p.hostname,
                s.host_name,
                p.nt_domain,
                p.nt_username,
                p.loginame,
                p.cmd,
                r.command
                --,p.*
                --,s.*
                --,r.*
            FROM
                sys.sysprocesses p
                CROSS APPLY sys.dm_exec_sql_text(sql_handle) t
                INNER JOIN sys.dm_exec_sessions s ON p.spid=s.session_id
                LEFT JOIN sys.dm_exec_requests r ON p.spid=r.session_id
            where
                p.hostname = 'DEIHAATOOLS'
                and p.status not in ('running')
                and (
                    (t.text like 'alter index%' and r.start_time < DATEADD(MINUTE, -" + $alterTimeout + ", GETDATE()))
                    or (t.text like 'update statistics%' and r.start_time < DATEADD(MINUTE, -" + $updateStatsTimeout + ", GETDATE()))
                )"

            $sesList = Invoke-Sqlcmd -ServerInstance $ins -Database 'master' -Query $sql -ErrorAction SilentlyContinue -MultiSubnetFailover

            foreach ($ses in $sesList) {
                $sql = "kill " + $ses.spid
                Invoke-Sqlcmd -ServerInstance $ins -Database 'master' -Query $sql -ErrorAction SilentlyContinue -MultiSubnetFailover

                $msg = $ins + ';' + $ses.db_name + ' | ' + $ses.text
                Log -file $mainLog -msg "Kill | $msg"
            }
        }
    }

    # Job status
    Log-Status -file $statusLog -jobs (Get-Job)

    # Check the completion of tasks for individual instances
    foreach ($ins in $instances) {
        if ($canStart[$ins] -eq 1) {
            $minStatus = ($dbList.$ins.Status | measure -Minimum).Minimum
            if ($minStatus -ge 2) {
                $canStart[$ins] = 2
                $params = $json.instances.$ins

                $m = "Enable backup jobs for $ins"
                Log -file $mainLog -msg $m
                
                if ($dryRun -eq 0) {
                    $ret = Set-SQLJobs-State -ins $ins -jobs $params.backup_jobs -state 1
                    if ($ret -eq $false) { Log -file $mainLog -msg 'Set-SQLJobs-State Failed' }
                }
            }
        }
    }

    # Check the completion of all tasks -> if the minimum status of all tasks is >= 2
    $minStatus = ($dbList.Values.Status | measure -Minimum).Minimum
    if ($minStatus -ge 2) {
        Log -file $mainLog -msg 'Database Maintenance FINISH'
        $finish = $true
    }

    if ($timerSuspend -gt 0) { $timerSuspend-- }

    Start-Sleep -Seconds 1
}

$execTime = New-TimeSpan -Start $startTime -End (Get-Date)
Log -file $mainLog -msg ("FINISH | " + $execTime.ToString())
