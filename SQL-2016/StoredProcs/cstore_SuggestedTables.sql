/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.6.0, January 2018

	Copyright 2015-2018 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

	Licensed under the Apache License, Version 2.0 (the "License");
	you may not use this file except in compliance with the License.
	You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

/*
Known Issues & Limitations: 
	- @showTSQLCommandsBeta parameter is in alpha version and not pretending to be complete any time soon. This output is provided as a basic help & guide convertion to Columnstore Indexes.
	- CLR support is not included or tested
	- Output [Min RowGroups] is not taking present partitions into calculations yet :)
	- In-Memory suggestion supports direct conversion from the Memory-Optimize tables. Support for the Disk-Based -> Memory Optimized tables conversion will be included in the future
	
Changes in 1.0.3
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.0.4
	- Bug fix for displaying the same primary key index twice in the T-SQL drop script

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed displaying wrong number of rows for the found suggested tables
	- Fixed error for filtering out the secondary nonclustered indexes in some bigger databases
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Updated with information on replication, which supports Nonclustered Columnstore in SQL Server 2016
	+ Added support for InMemory Tables
	+ Added information about the converted table location (In-Memory or Disk-Based)
	+ Added new parameter for filtering the table location - @indexLocation with possible values (In-Memory or Disk-Based)

Changes in 1.3.1
	- Fixed a bug with filtering out the exact number of @minRows instead of including it
	- Fixed a bug when @indexLocation was a non-correct value it would include all results. Now a wrong value will return no results.

Changes in 1.4.1
	+ Suggestion capability improvements
	+ Added support for the SP1 which allows support of Columnstore Indexes on any edition

Changes in 1.4.2
	- Fixed bug on the size of the @minSizeToConsiderInGB parameter
	+ Small Improvements for the @columnstoreIndexTypeForTSQL parameter with better quality generation for the complex objects with Primary Keys

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
	- Fixed bug with the partitioned table not showing the correct number of rows
	+ Added new result column [Partitions] showing the total number of the partitions

Changes in 1.6.0
	+ Added new parameter for specifying the name of the database, where the Columnstore Indexes should be located (@dbName)
	- Fixed the bug with the data type of the [Min RowGroups] column from SMALLINT to INT (Thanks to Thorsten)
	- Fixed the bug with the total number of computed columns per database being shown instead of the number of computed columns per table
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2016
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server 2016. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

IF EXISTS (SELECT 1 WHERE SERVERPROPERTY('EngineEdition') <> 3 AND cast(SERVERPROPERTY('ProductLevel') as nvarchar(128)) NOT LIKE 'SP%')
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition or your are not running Service Pack 1 or later for SQL Server 2016. Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_SuggestedTables as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.6.0, January 2018
*/
alter procedure dbo.cstore_SuggestedTables(
-- Params --
	@dbName SYSNAME = NULL,										-- Identifies the Database to run the stored procedure against. If this parameter is left to be NULL, then the current database is used
	@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	@preciseSearch bit = 0,										-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@objectId INT = NULL,										-- Allows to show data filtered down to the specific object_id
	@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
	@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
	@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
	@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
	@showTSQLCommandsBeta bit = 0,								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
	@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered'		-- Allows to define the type of Columnstore Index to be created eith possible values of 'Clustered' and 'Nonclustered'
-- end of --
) as 
begin
	set nocount on;

	IF @dbName IS NULL
		SET @dbName = DB_NAME(DB_ID());

	DECLARE @dbId INT = DB_ID(@dbName);
	DECLARE @sql NVARCHAR(MAX);

	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName)
	BEGIN
		DECLARE @errorMessage NVARCHAR(500) = 'The Database ''' + @dbName + ''' does not exist on this SQL Server Instance!';
		THROW 51000, @errorMessage, 1;
	END

	declare 
		@readCommitedSnapshot tinyint = 0,
		@snapshotIsolation tinyint = 0;

	-- Verify Snapshot Isolation Level or Read Commited Snapshot 
	select @readCommitedSnapshot = is_read_committed_snapshot_on, 
		@snapshotIsolation = snapshot_isolation_state
		from sys.databases
		where database_id = @dbId;

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	drop table IF EXISTS #TablesToColumnstore;

	create table #TablesToColumnstore(
		[ObjectId] int NOT NULL PRIMARY KEY,
		[TableLocation] varchar(15) NOT NULL,
		[TableName] nvarchar(1000) NOT NULL,
		[ShortTableName] nvarchar(256) NOT NULL,
		[Partitions] BIGINT NOT NULL,
		[Row Count] bigint NOT NULL,
		[Min RowGroups] INT NOT NULL,
		[Size in GB] decimal(16,3) NOT NULL,
		[Cols Count] smallint NOT NULL,
		[String Cols] smallint NOT NULL,
		[Sum Length] int NOT NULL,
		[Unsupported] smallint NOT NULL,
		[LOBs] smallint NOT NULL,
		[Computed] smallint NOT NULL,
		[Clustered Index] tinyint NOT NULL,
		[Nonclustered Indexes] smallint NOT NULL,
		[XML Indexes] smallint NOT NULL,
		[Spatial Indexes] smallint NOT NULL,
		[Primary Key] tinyint NOT NULL,
		[Foreign Keys] smallint NOT NULL,
		[Unique Constraints] smallint NOT NULL,
		[Triggers] smallint NOT NULL,
		[RCSI] tinyint NOT NULL,
		[Snapshot] tinyint NOT NULL,
		[CDC] tinyint NOT NULL,
		[CT] tinyint NOT NULL,
		[InMemoryOLTP] tinyint NOT NULL,
		[Replication] tinyint NOT NULL,
		[FileStream] tinyint NOT NULL,
		[FileTable] tinyint NOT NULL
	);

	SET @sql = N'
	insert into #TablesToColumnstore
	select t.object_id as [ObjectId]
		, case ind.data_space_id when 0 then ''In-Memory'' else ''Disk-Based'' end 
		, quotename(object_schema_name(t.object_id, @dbId)) + ''.'' + quotename(object_name(t.object_id, @dbId)) as ''TableName''
		, replace(object_name(t.object_id, @dbId),'' '', '''') as ''ShortTableName''
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as ''Row Count''
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as ''Min RowGroups'' 
		, isnull(cast( sum(memory_allocated_for_table_kb) / 1024. / 1024 as decimal(16,3) ),0) + cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3))  as ''size in GB'' 
		, (select count(*) from ' + QUOTENAME(@dbName) + N'.sys.columns as col
			where t.object_id = col.object_id ) as ''Cols Count''
		, (select count(*) 
				from ' + QUOTENAME(@dbName) + N'.sys.columns as col
					inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'',''SYSNAME'') 
		   ) as ''String Cols''
		, isnull((select sum(col.max_length) 
				from ' + QUOTENAME(@dbName) + N'.sys.columns as col
					inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ),0) as ''Sum Length''
		, (select count(*) 
				from ' + QUOTENAME(@dbName) + N'.sys.columns as col
					inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in (''TEXT'',''NTEXT'',''TIMESTAMP'',''HIERARCHYID'',''SQL_VARIANT'',''XML'',''GEOGRAPHY'',''GEOMETRY'') OR
					  (UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as ''Unsupported''
		, (select count(*) 
				from ' + QUOTENAME(@dbName) + N'.sys.columns as col
					inner join ' + QUOTENAME(@dbName) + N'. sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as ''LOBs''
		   ';
	SET @sql += N'
		, (select count(*) 
				from ' + QUOTENAME(@dbName) + N'.sys.columns as col
				where is_computed = 1 AND col.object_id = t.object_id ) as ''Computed''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as ''Clustered Index''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as ''Nonclustered Indexes''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as ''XML Indexes''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as ''Spatial Indexes''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.objects
				where UPPER(type) = ''PK'' AND parent_object_id = t.object_id ) as ''Primary Key''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.objects
				where UPPER(type) = ''F'' AND parent_object_id = t.object_id ) as ''Foreign Keys''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.objects
				where UPPER(type) in (''UQ'') AND parent_object_id = t.object_id ) as ''Unique Constraints''
		, (select count(*)
				from ' + QUOTENAME(@dbName) + N'.sys.objects
				where UPPER(type) in (''TA'',''TR'') AND parent_object_id = t.object_id ) as ''Triggers''
		, @readCommitedSnapshot as ''RCSI''
		, @snapshotIsolation as ''Snapshot''
		, t.is_tracked_by_cdc as [CDC]
		, (select count(*) 
				from ' + QUOTENAME(@dbName) + N'.sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from ' + QUOTENAME(@dbName) + N'.sys.change_tracking_databases ctdb)) as ''CT''
		, t.is_memory_optimized as ''InMemoryOLTP''
		, t.is_replicated as ''Replication''
		, coalesce(t.filestream_data_space_id,0,1) as ''FileStream''
		, t.is_filetable as ''FileTable''
		';
	SET @sql += N'
		from ' + QUOTENAME(@dbName) + N'.sys.tables t
			inner join ' + QUOTENAME(@dbName) + N'.sys.partitions as p 
				ON t.object_id = p.object_id
			inner join ' + QUOTENAME(@dbName) + N'.sys.allocation_units as a 
				ON p.partition_id = a.container_id
			inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				on ind.object_id = p.object_id and ind.index_id = p.index_id
			left join ' + QUOTENAME(@dbName) + N'.sys.dm_db_xtp_table_memory_stats xtpMem
				on xtpMem.object_id = t.object_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id, @dbId) like ''%'' + @tableName + ''%'') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id, @dbId) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id, @dbId ) like ''%'' + @schemaName + ''%'')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id, @dbId ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from ' + QUOTENAME(@dbName) + N'.sys.columns as col
							inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in (''TEXT'',''NTEXT'',''TIMESTAMP'',''HIERARCHYID'',''SQL_VARIANT'',''XML'',''GEOGRAPHY'',''GEOMETRY''))
						) = 0 
					--and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, ind.data_space_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from ' + QUOTENAME(@dbName) + N'.sys.columns as col
						inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(isnull(cast( sum(memory_allocated_for_table_kb) / 1024. / 1024 as decimal(16,3) ),0) + cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB)';

	SET @sql += N'
	union all
	select t.object_id as [ObjectId]
		, ''Disk-Based''
		, quotename(object_schema_name(t.object_id)) + ''.'' + quotename(object_name(t.object_id)) as ''TableName''
		, replace(object_name(t.object_id),'' '', '''') as ''ShortTableName''
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as ''Row Count''
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as ''Min RowGroups'' 		
		, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as ''size in GB''
		, (select count(*) from sys.columns as col
			where t.object_id = col.object_id ) as ''Cols Count''
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'',''SYSNAME'') 
		   ) as ''String Cols''
		, isnull((select sum(col.max_length) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ),0) as ''Sum Length''
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in (''TEXT'',''NTEXT'',''TIMESTAMP'',''HIERARCHYID'',''SQL_VARIANT'',''XML'',''GEOGRAPHY'',''GEOMETRY'') OR
					  (UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as ''Unsupported''
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as ''LOBs''
		, (select count(*) 
				from sys.columns as col
				where is_computed = 1 AND col.object_id = t.object_id ) as ''Computed''
		, (select count(*)
				from sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as ''Clustered Index''
		, (select count(*)
				from sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as ''Nonclustered Indexes''
		, (select count(*)
				from sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as ''XML Indexes''
		, (select count(*)
				from sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as ''Spatial Indexes''
		, (select count(*)
				from sys.objects
				where UPPER(type) = ''PK'' AND parent_object_id = t.object_id ) as ''Primary Key''
		, (select count(*)
				from sys.objects
				where UPPER(type) = ''F'' AND parent_object_id = t.object_id ) as ''Foreign Keys''
		, (select count(*)
				from sys.objects
				where UPPER(type) in (''UQ'') AND parent_object_id = t.object_id ) as ''Unique Constraints''
		, (select count(*)
				from sys.objects
				where UPPER(type) in (''TA'',''TR'') AND parent_object_id = t.object_id ) as ''Triggers''
		, @readCommitedSnapshot as ''RCSI''
		, @snapshotIsolation as ''Snapshot''
		, t.is_tracked_by_cdc as [CDC]
		, (select count(*) 
				from sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as ''CT''
		, t.is_memory_optimized as ''InMemoryOLTP''
		, t.is_replicated as ''Replication''
		, coalesce(t.filestream_data_space_id,0,1) as ''FileStream''
		, t.is_filetable as ''FileTable'' ';
	SET @sql += N'
		from tempdb.sys.tables t
			left join tempdb.sys.partitions as p 
				on t.object_id = p.object_id
			left join tempdb.sys.allocation_units as a 
				on p.partition_id = a.container_id
			inner join sys.indexes ind
				on ind.object_id = p.object_id and p.index_id = ind.index_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id,db_id(''tempdb'')) like ''%'' + @tableName + ''%'') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id,db_id(''tempdb'')) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id,db_id(''tempdb'') ) like ''%'' + @schemaName + ''%'')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id,db_id(''tempdb'') ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
		 			--and ind.data_space_id = isnull( case @indexLocation when ''In-Memory'' then 0 when ''Disk-Based'' then 1 else ind.data_space_id end, ind.data_space_id )
		 	 and ind.data_space_id = case isnull(@indexLocation,''Null'') 
													when ''In-Memory'' then 0
													when ''Disk-Based'' then 1 
													when ''Null'' then ind.data_space_id
													else 255 
									end
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from sys.columns as col
							inner join sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in (''TEXT'',''NTEXT'',''TIMESTAMP'',''HIERARCHYID'',''SQL_VARIANT'',''XML'',''GEOGRAPHY'',''GEOMETRY''))
						) = 0 
					--and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from sys.columns as col
						inner join sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB);
	';

	DECLARE @paramDefinition NVARCHAR(1000) =  '@indexLocation varchar(15),			
												@minRowsToConsider bigint,
												@minSizeToConsiderInGB bigint,				
												@showReadyTablesOnly bit,	
												@considerColumnsOver8K bit,
												@showUnsupportedColumnsDetails bit,
												@readCommitedSnapshot tinyint = 0,
												@snapshotIsolation tinyint = 0,
												@preciseSearch bit,						
												@tableName nvarchar(256),			
												@schemaName nvarchar(256),			
												@objectId int,	
												@dbId int';						

	EXEC sp_executesql @sql, @paramDefinition, @indexLocation = @indexLocation,
											   @minRowsToConsider = @minRowsToConsider, @minSizeToConsiderInGB = @minSizeToConsiderInGB,
											   @showReadyTablesOnly = @showReadyTablesOnly, @considerColumnsOver8K = @considerColumnsOver8K,
											   @showUnsupportedColumnsDetails = @showUnsupportedColumnsDetails,
											   @readCommitedSnapshot = @readCommitedSnapshot,	@snapshotIsolation = @snapshotIsolation,
											   @preciseSearch = @preciseSearch, @tableName = @tableName,
											   @schemaName = @schemaName, @objectId = @objectId,
											   @dbId = @dbId;


	-- Show the found results
	SET @sql = N'
	select case when ([Triggers] + [FileStream] + [FileTable] + [Unsupported] - ([LOBs] + [Computed])) > 0 then ''None'' 
				when ([Clustered Index] + [CDC] + [CT] +
					  [Unique Constraints] + [Triggers] + [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) = 0 and [Unsupported] = 0
					  AND TableLocation <> ''In-Memory'' then ''Both Columnstores'' 
				when ( [Triggers] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 
					  AND TableLocation <> ''In-Memory'' then ''Nonclustered Columnstore''  
				when ( [Clustered Index] + [CDC] + [CT] +
					  [Unique Constraints] + [Triggers] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 
					  AND TableLocation = ''In-Memory'' then ''Clustered Columnstore''  
		   end as ''Compatible With''
		, TableLocation		
		, [TableName], [Partitions], [Row Count], [Min RowGroups], [Size in GB], [Cols Count], [String Cols], [Sum Length], [Unsupported], [LOBs], [Computed]
		, [Clustered Index], [Nonclustered Indexes], [XML Indexes], [Spatial Indexes], [Primary Key], [Foreign Keys], [Unique Constraints]
		, [Triggers], [RCSI], [Snapshot], [CDC], [CT], [InMemoryOLTP], [Replication], [FileStream], [FileTable]
		from #TablesToColumnstore tempRes
		where TableLocation = isnull(@indexLocation, TableLocation)
		order by [Row Count] desc;'

	SET @paramDefinition =  '@indexLocation varchar(15)';
	EXEC sp_executesql @sql, @paramDefinition, @indexLocation = @indexLocation;

	if( @showUnsupportedColumnsDetails = 1 ) 
	begin
		SET @sql = N'
		select quotename(object_schema_name(t.object_id,@dbId)) + ''.'' + quotename(object_name (t.object_id,@dbId)) as ''TableName'',
			col.name as ''Unsupported Column Name'',
			tp.name as ''Data Type'',
			col.max_length as ''Max Length'',
			col.precision as ''Precision'',
			col.is_computed as ''Computed''
			from ' + QUOTENAME(@dbName) + N'.sys.tables t
				inner join ' + QUOTENAME(@dbName) + N'.sys.columns as col
					on t.object_id = col.object_id 
				inner join ' + QUOTENAME(@dbName) + N'.sys.types as tp
					on col.user_type_id = tp.user_type_id 
				where  ((UPPER(tp.name) in (''TEXT'',''NTEXT'',''TIMESTAMP'',''HIERARCHYID'',''SQL_VARIANT'',''XML'',''GEOGRAPHY'',''GEOMETRY'') OR
						(UPPER(tp.name) in (''VARCHAR'',''NVARCHAR'',''CHAR'',''NCHAR'') and (col.max_length = 8000 or col.max_length = -1)) 
						) 
						OR col.is_computed = 1 )
				 and t.object_id in (select ObjectId from #TablesToColumnstore);'

		SET @paramDefinition =  '@dbId int';
		EXEC sp_executesql @sql, @paramDefinition, @dbId = @dbId;
	end



	if( @showTSQLCommandsBeta = 1 ) 
	begin
		SET @sql = N'
		select coms.TableName, coms.[TSQL Command], coms.[type]
			from (
				select t.TableName, 
						''create '' + @columnstoreIndexTypeForTSQL + '' columnstore index '' + 
						case @columnstoreIndexTypeForTSQL when ''Clustered'' then ''CCI'' when ''Nonclustered'' then ''NCCI'' end 
						+ ''_'' + t.[ShortTableName] + 
						'' on '' + t.TableName + case @columnstoreIndexTypeForTSQL when ''Nonclustered'' then ''()'' else '''' end + '';'' as [TSQL Command]
					   , ''CCL'' as type,
					   101 as [Sort Order]
					from #TablesToColumnstore t
					where TableLocation = ''Disk-Based''
				union all
					select t.TableName, 
						''alter table '' + t.TableName +
						'' add index CCI_'' + t.[ShortTableName] + '' clustered columnstore;'' as [TSQL Command]
					   , ''CCL'' as type,
					   102 as [Sort Order]
					from #TablesToColumnstore t
					where TableLocation = ''In-Memory''
				union all
				select t.TableName, ''alter table '' + t.TableName + '' drop constraint '' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + '';'' as [TSQL Command], [type], 
					   case UPPER(type) when ''PK'' then 100 when ''F'' then 1 when ''UQ'' then 100 end as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in (''PK'')
						and t.TableLocation <> ''In-Memory''
				union all
				select t.TableName, ''drop trigger '' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + '';'' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in (''TR'')
				union all ';
			SET @sql += N'
				select t.TableName, ''drop assembly '' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + '' WITH NO DEPENDENTS ;'' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in (''TA'')	
				union all 
				select t.TableName, ''drop index '' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + '' on '' + t.TableName + '';'' as [TSQL Command], ''CL'' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 1 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join ' + QUOTENAME(@dbName) + N'.sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in (''PK'')
								and quotename(ind.name) <> quotename(so1.name)
								and t1.TableLocation <> ''In-Memory'')
						and t.TableLocation <> ''In-Memory''
				union all 
				select t.TableName, ''drop index '' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + '' on '' + t.TableName + '';'' as [TSQL Command], ''NC'' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 2 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join ' + QUOTENAME(@dbName) + N'.sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in (''PK'')
								and quotename(ind.name) <> quotename(so1.name) and t.ObjectId = t1.ObjectId 
								and t1.TableLocation <> ''In-Memory'')
						and t.TableLocation <> ''In-Memory''
				union all ';
			SET @sql += N'
				select t.TableName, ''drop index '' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + '' on '' + t.TableName + '';'' as [TSQL Command], ''XML'' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 3
				union all 
				select t.TableName, ''drop index '' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + '' on '' + t.TableName + '';'' as [TSQL Command], ''SPAT'' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 4
				union all 
				select t.TableName, 
					'''', 
					'''',
					0 as [Sort Order]
					from #TablesToColumnstore t
			) coms
		order by coms.type desc, coms.[Sort Order]; ';

		SET @paramDefinition				    =  '@indexLocation varchar(15),			
													@minRowsToConsider bigint,
													@minSizeToConsiderInGB bigint,				
													@showReadyTablesOnly bit,	
													@considerColumnsOver8K bit,
													@showUnsupportedColumnsDetails bit,
													@readCommitedSnapshot tinyint = 0,
													@snapshotIsolation tinyint = 0,
													@preciseSearch bit,						
													@tableName nvarchar(256),			
													@schemaName nvarchar(256),			
													@objectId int,	
													@columnstoreIndexTypeForTSQL varchar(20),
													@dbId int';						
		PRINT @sql;
		EXEC sp_executesql @sql, @paramDefinition, @indexLocation = @indexLocation,
												   @minRowsToConsider = @minRowsToConsider, @minSizeToConsiderInGB = @minSizeToConsiderInGB,
												   @showReadyTablesOnly = @showReadyTablesOnly, @considerColumnsOver8K = @considerColumnsOver8K,
												   @showUnsupportedColumnsDetails = @showUnsupportedColumnsDetails,
												   @readCommitedSnapshot = @readCommitedSnapshot,	@snapshotIsolation = @snapshotIsolation,
												   @preciseSearch = @preciseSearch, @tableName = @tableName,
												   @schemaName = @schemaName, @objectId = @objectId,
												   @columnstoreIndexTypeForTSQL = @columnstoreIndexTypeForTSQL,
												   @dbId = @dbId;
			 
	end

	DROP TABLE IF EXISTS #TablesToColumnstore; 
end

GO
