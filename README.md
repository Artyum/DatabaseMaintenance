# Reindexing of MSSQL databases

Maintenance of databases is performed based on the documentation:
- [Optimize index maintenance to improve query performance and reduce resource consumption](https://learn.microsoft.com/en-us/sql/relational-databases/indexes/reorganize-and-rebuild-indexes?view=sql-server-ver16)
- [ALTER INDEX (Transact-SQL)](https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-index-transact-sql?view=sql-server-ver16)
- [sys.databases](https://learn.microsoft.com/en-us/sql/relational-databases/system-catalog-views/sys-databases-transact-sql?view=sql-server-ver16)

Operations are performed in parallel on several databases per instance.

## Microsoft Best Practices

- Indexes with `page_count >= 1000` should be reindexed.
- For fragmentation up to 30%, `REORGANIZE` is performed. For higher fragmentation, a rebuild is recommended.

## Database Maintenance Procedure

1. **Disable Backup Jobs on the Instance**
2. **Wait for Running Backups to Complete**
3. **Switch Recovery Model from FULL to SIMPLE**
4. **Phase 1**
   - `REORGANIZE` for low fragmentation or `REBUILD ONLINE` for high fragmentation
   - If `REBUILD ONLINE` is not possible, then `REBUILD OFFLINE`
   - Operations longer than X minutes in `SUSPENDED` status are automatically killed
5. **Phase 2**
   - Retry operations stopped in Phase 1. Only `REBUILD` is performed in the second phase.
   - Operations longer than X minutes in `SUSPENDED` status are automatically killed
6. **Update All Statistics on the Database**
   - Kill operations after some time if they are `SUSPENDED`
7. **Restore FULL Recovery Model (if it was originally)**
8. **Enable Backup Jobs**

## Recommended Usage

It is recommended to run the system once a week using the Windows Task Scheduler.

## Operations Performed on Databases

### Reorganize

- For indexes with a fragmentation degree between `min_frag_pct_reorganize` and `min_frag_pct_rebuild`

### Rebuild

- For indexes with a fragmentation degree above `min_frag_pct_rebuild`
- Rebuild is performed online by default. If the index cannot be rebuilt online, it is rebuilt offline.

### sp_updatestats

- Update all statistics on the database

## Logging

- Logs are created in the subdirectory `Logs_yyyy-MM-dd`
- A separate directory is created for each instance, containing log files for each database
- Additionally, logs `JobHistory`, `JobStatus.log`, and `KillLog.log` are created to track the progress of the automation
