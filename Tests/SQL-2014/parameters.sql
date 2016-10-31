exec dbo.cstore_SuggestedTables @minRowsToConsider = 499999, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minRowsToConsider = 500000, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minRowsToConsider = 500001, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.005, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.006, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.007, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @schemaName = 'db', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @schemaName = 'dbo', @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Based', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Base', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @indexLocation = 'In-Memory', @tableName = 'SuggestedTables_Test1'
--------
exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 1, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 0, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 1, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 0, @tableName = 'SuggestedTables_Test1'



-- showUnsupportedColumnsDetails
-- columnstoreIndexTypeForTSQL
-- updateMemoryOptimisedStats


-- Min Rows
-- Min Size in GB
-- Schema Name
-- Table Name
-- Index Location 
-- considerColumnsOver8K
-- showReadyTablesOnly
-- showUnsupportedColumnsDetails
-- columnstoreIndexTypeForTSQL
-- updateMemoryOptimisedStats

-- Row Count
-- Min RowGroups
-- Cols Count
-- Sum Length
-- Unsupported
-- LOBs
-- Computed
-- Clustered Index
-- Nonclustered Index
-- XML Indexes
-- Spatial Indexes
-- Primary Key
-- Foreign Keys
-- Unique Constraints
-- Triggers
-- RCSI
-- Snapshot
-- CDC
-- CT
-- InMemoryOLTP
-- Replication
-- FileStream
-- FileTable



	--@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	--@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	--@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	--@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	--@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
	--@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
	--@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
	--@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
	--@showTSQLCommandsBeta bit = 0,								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
	--@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered',		-- Allows to define the type of Columnstore Index to be created eith possible values of 'Clustered' and 'Nonclustered'
	--@updateMemoryOptimisedStats bit = 0							-- Allows statistics update on the InMemory tables, since they are stalled within SQL Server 2014
