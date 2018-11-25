/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
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
	- InMemory Tables have a bug in SQL Server 2014, that does not allow to know the number of rows - https://connect.microsoft.com/SQLServer/feedback/details/2909569, forcing the statistics update for discovering the number of rows

Changes in 1.0.3
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions	

Changes in 1.0.4
	- Bug fixes for the Nonclustered Columnstore Indexes creation conditions
	- Buf fixes for the data types of the monitored functionalities, that in certain condition would give an error message
	- Bug fix for displaying the same primary key index twice in the T-SQL drop script
	
Changes in 1.2.0
	- Fixed displaying wrong number of rows for the found suggested tables
	- Fixed error for filtering out the secondary nonclustered indexes in some bigger databases
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for InMemory Tables
	+ Added information about the converted table location (In-Memory or Disk-Based)
	+ Added new parameter for filtering the table location - @indexLocation with possible values (In-Memory or Disk-Based)
	+ Added new parameter for controlling the needed statistics update for Memory Optimised tables - @updateMemoryOptimisedStats with default value set on false

Changes in 1.3.1
	- Fixed a bug with filtering out the exact number of @minRows instead of including it
	- Fixed a bug when @indexLocation was a non-correct value it would include all results. Now it will return none

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
	- Fixed the bug with the data type of the [Min RowGroups] column from SMALLINT to INT (Thanks to Thorsten)
	- Fixed the bug with the total number of computed columns per database being shown instead of the number of computed columns per table
*/

-- Params --
declare @minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
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
		@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered',		-- Allows to define the type of Columnstore Index to be created with possible values of 'Clustered' and 'Nonclustered'
		@updateMemoryOptimisedStats bit = 0							-- Allows statistics update on the InMemory tables, since they are stalled within SQL Server 2014
-- end of --

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2014
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'12'
begin
	set @errorMessage = (N'You are not running a SQL Server 2014. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2014 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end



--------------------------------------------------------------------------------------------------------------------
set nocount on;

declare 
	@readCommitedSnapshot tinyint = 0,
	@snapshotIsolation tinyint = 0;

-- Verify Snapshot Isolation Level or Read Commited Snapshot 
select @readCommitedSnapshot = is_read_committed_snapshot_on, 
	@snapshotIsolation = snapshot_isolation_state
	from sys.databases
	where database_id = DB_ID();

-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
if OBJECT_ID('tempdb..#TablesToColumnstore') IS NOT NULL
	drop table #TablesToColumnstore;

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

insert into #TablesToColumnstore
select t.object_id as [ObjectId]
	, case max(ind.data_space_id) when 0 then 'In-Memory' else 'Disk-Based' end 
	, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id)) as 'TableName'
	, replace(object_name(t.object_id),' ', '') as 'ShortTableName'
	, COUNT(DISTINCT p.partition_number) as [Partitions]
	, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
	, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 
	, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
	, (select count(*) from sys.columns as col
		where t.object_id = col.object_id ) as 'Cols Count'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
	   ) as 'String Cols'
	, (select sum(col.max_length) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id 
	  ) as 'Sum Length'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
				  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
				 )
	   ) as 'Unsupported'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
	   ) as 'LOBs'
    , (select count(*) 
			from sys.columns as col
			where is_computed = 1 AND col.object_id = t.object_id ) as 'Computed'
	, (select count(*)
			from sys.indexes ind
			where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
	, (select count(*)
			from sys.indexes ind
			where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
	, (select count(*)
			from sys.indexes ind
			where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
	, (select count(*)
			from sys.indexes ind
			where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
	, (select count(*)
			from sys.objects
			where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
	, (select count(*)
			from sys.objects
			where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
	, (select count(*)
			from sys.objects
			where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
	, (select count(*)
			from sys.objects
			where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
    , @readCommitedSnapshot as 'RCSI'
	, @snapshotIsolation as 'Snapshot'
	, t.is_tracked_by_cdc as 'CDC'
	, (select count(*) 
			from sys.change_tracking_tables ctt with(READUNCOMMITTED)
			where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
				  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
	, t.is_memory_optimized as 'InMemoryOLTP'
	, t.is_replicated as 'Replication'
	, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
	, t.is_filetable as 'FileTable'
	from sys.tables t
		inner join sys.partitions as p 
			ON t.object_id = p.object_id
		inner join sys.allocation_units as a 
			ON p.partition_id = a.container_id
		inner join sys.indexes ind
			on ind.object_id = p.object_id and ind.index_id = p.index_id
		left join sys.dm_db_xtp_table_memory_stats xtpMem
			on xtpMem.object_id = t.object_id
	where p.data_compression in (0,1,2) -- None, Row, Page
		 and (select count(*)
				from sys.indexes ind
				where t.object_id = ind.object_id
					and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
		 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%') 
			  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id) = @tableName) )
		 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id ) like '%' + @schemaName + '%')
			  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id ) = @schemaName))
		 AND (ISNULL(@objectId,t.object_id) = t.object_id)
		 and t.is_memory_optimized = case isnull(@indexLocation,'Null') 
												when 'In-Memory' then 1 
												when 'Disk-Based' then 0 
												when 'Null' then t.is_memory_optimized
										else 255 end
		 and (( @showReadyTablesOnly = 1 
				and  
				(select count(*) 
					from sys.columns as col
						inner join sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id and 
							(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
					) = 0 
				and (select count(*)
						from sys.objects so
						where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
				and (select count(*)
						from sys.indexes ind
						where t.object_id = ind.object_id
							and ind.type in (3,4) ) = 0
				and (select count(*) 
						from sys.change_tracking_tables ctt with(READUNCOMMITTED)
						where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
								and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) = 0
				and t.is_tracked_by_cdc = 0
				and t.is_memory_optimized = 0
				and t.is_replicated = 0
				and coalesce(t.filestream_data_space_id,0,1) = 0
				and t.is_filetable = 0
			  )
			 or @showReadyTablesOnly = 0)
	group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
	having 
			(sum(p.rows) >= @minRowsToConsider or (sum(p.rows) = 0 and is_memory_optimized = 1) )
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
			(sum(a.total_pages) + isnull(sum(memory_allocated_for_table_kb),0) / 1024. / 1024 * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)
union all
select t.object_id as [ObjectId]
	, 'Disk-Based'
	, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id, db_id('tempdb'))) as 'TableName'
	, replace(object_name(t.object_id, db_id('tempdb')),' ', '') as 'ShortTableName'
	, COUNT(DISTINCT p.partition_number) as [Partitions]
	, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
	, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 
	, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
	, (select count(*) from tempdb.sys.columns as col
		where t.object_id = col.object_id ) as 'Cols Count'
	, (select count(*) 
			from tempdb.sys.columns as col
				inner join tempdb.sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
	   ) as 'String Cols'
	, (select sum(col.max_length) 
			from tempdb.sys.columns as col
				inner join tempdb.sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id 
	  ) as 'Sum Length'
	, (select count(*) 
			from tempdb.sys.columns as col
				inner join tempdb.sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
				  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
				 )
	   ) as 'Unsupported'
	, (select count(*) 
			from tempdb.sys.columns as col
				inner join tempdb.sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
	   ) as 'LOBs'
    , (select count(*) 
			from tempdb.sys.columns as col
			where is_computed = 1 AND col.object_id = t.object_id ) as 'Computed'
	, (select count(*)
			from tempdb.sys.indexes ind
			where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
	, (select count(*)
			from tempdb.sys.indexes ind
			where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
	, (select count(*)
			from tempdb.sys.indexes ind
			where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
	, (select count(*)
			from tempdb.sys.indexes ind
			where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
	, (select count(*)
			from tempdb.sys.objects
			where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
	, (select count(*)
			from tempdb.sys.objects
			where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
	, (select count(*)
			from tempdb.sys.objects
			where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
	, (select count(*)
			from tempdb.sys.objects
			where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
    , @readCommitedSnapshot as 'RCSI'
	, @snapshotIsolation as 'Snapshot'
	, t.is_tracked_by_cdc as 'CDC'
	, (select count(*) 
			from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
			where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
				  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
	, t.is_memory_optimized as 'InMemoryOLTP'
	, t.is_replicated as 'Replication'
	, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
	, t.is_filetable as 'FileTable'
	from tempdb.sys.tables t
		inner join tempdb.sys.partitions as p 
			ON t.object_id = p.object_id
		inner join tempdb.sys.allocation_units as a 
			ON p.partition_id = a.container_id
	where p.data_compression in (0,1,2) -- None, Row, Page
		 and (select count(*)
				from sys.indexes ind
				where t.object_id = ind.object_id
					and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
		 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
			  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) = @tableName) )
		 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
			  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) = @schemaName))
		 AND (ISNULL(@objectId,t.object_id) = t.object_id)
		 and t.is_memory_optimized = case isnull(@indexLocation,'Null') 
												when 'In-Memory' then 1 
												when 'Disk-Based' then 0 
												when 'Null' then t.is_memory_optimized
										else 255 end
		 and (( @showReadyTablesOnly = 1 
				and  
				(select count(*) 
					from tempdb.sys.columns as col
						inner join tempdb.sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id and 
							(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
					) = 0 
				and (select count(*)
						from tempdb.sys.objects so
						where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
				and (select count(*)
						from tempdb.sys.indexes ind
						where t.object_id = ind.object_id
							and ind.type in (3,4) ) = 0
				and (select count(*) 
						from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
						where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
								and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) = 0
				and t.is_tracked_by_cdc = 0
				and t.is_memory_optimized = 0
				and t.is_replicated = 0
				and coalesce(t.filestream_data_space_id,0,1) = 0
				and t.is_filetable = 0
			  )
			 or @showReadyTablesOnly = 0)
	group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
	having sum(p.rows) >= @minRowsToConsider 
			and
			(((select sum(col.max_length) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.system_type_id = tp.system_type_id
				where t.object_id = col.object_id 
			  ) < 8000 and @considerColumnsOver8K = 0 ) 
			  OR
			 @considerColumnsOver8K = 1 )
			and 
			(sum(a.total_pages) * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)

-- Get the information on Memory Optimised Tables
if @updateMemoryOptimisedStats = 1
begin 
	declare @updateStatTSQL nvarchar(1000);
	declare inmemRowCountCursor CURSOR LOCAL READ_ONLY for
   		select N'Update Statistics ' + TableName + ' WITH FULLSCAN, NORECOMPUTE'
			from #TablesToColumnstore
			where TableLocation = 'In-Memory';

	open inmemRowCountCursor;

	fetch next 
		from inmemRowCountCursor 
			into @updateStatTSQL;

	while @@FETCH_STATUS = 0 BEGIN
		exec sp_executesql @updateStatTSQL;
		fetch next from inmemRowCountCursor 
			into @updateStatTSQL;
	END

	close inmemRowCountCursor
	deallocate inmemRowCountCursor


	update #TablesToColumnstore
		set [Row Count] = ISNULL(st.[rows],0),
			[Min RowGroups] = ceiling(ISNULL(st.[rows],0)/1045678.),
			[Size in GB] = cast( memory_allocated_for_table_kb / 1024. / 1024 as decimal(16,3) )
		from #TablesToColumnstore temp
			inner join sys.dm_db_xtp_index_stats AS ind
				on temp.ObjectId = ind.object_id
			cross apply sys.dm_db_stats_properties (ind.object_id,ind.index_id) st
			inner join sys.dm_db_xtp_table_memory_stats xtpMem
				on temp.ObjectId = xtpMem.object_id
		  where ind.index_id = 2 and temp.TableLocation = 'In-Memory';

end 

delete from #TablesToColumnstore
	where [Size in GB] < @minSizeToConsiderInGB
		or [Row Count] < @minRowsToConsider;

-- Show the found results
select case when ([InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) <= 0 then 'Nonclustered Columnstore'
			when ([Primary Key] + [Foreign Keys] + [Unique Constraints] + [Triggers] + [CDC] + [CT] +
				  [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) > 0 then 'None' 
			when ([Clustered Index] + [Nonclustered Indexes] + [Primary Key] + [Foreign Keys] + [CDC] + [CT] +
				  [Unique Constraints] + [Triggers] + [RCSI] + [Snapshot] + [CDC] + [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) = 0 and [Unsupported] = 0 then 'Both Columnstores'  
	   end as 'Compatible With'
	, TableLocation	
	, [TableName], [Partitions], [Row Count], [Min RowGroups], [Size in GB], [Cols Count], [String Cols], [Sum Length], [Unsupported], [LOBs], [Computed]
	, [Clustered Index], [Nonclustered Indexes], [XML Indexes], [Spatial Indexes], [Primary Key], [Foreign Keys], [Unique Constraints]
	, [Triggers], [RCSI], [Snapshot], [CDC], [CT], [InMemoryOLTP], [Replication], [FileStream], [FileTable]
	from #TablesToColumnstore
	order by [Row Count] desc;

if( @showUnsupportedColumnsDetails = 1 ) 
begin
	select quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name (t.object_id)) as 'TableName',
		col.name as 'Unsupported Column Name',
		tp.name as 'Data Type',
		col.max_length as 'Max Length',
		col.precision as 'Precision',
		col.is_computed as 'Computed'
		from sys.tables t
			inner join sys.columns as col
				on t.object_id = col.object_id 
			inner join sys.types as tp
				on col.user_type_id = tp.user_type_id 
			where  ((UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
					(UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
					) 
					OR col.is_computed = 1 )
			 and t.object_id in (select ObjectId from #TablesToColumnstore);
end

if( @showTSQLCommandsBeta = 1 ) 
begin
	select coms.TableName, coms.[TSQL Command], coms.[type] 
		from (
			select t.TableName, 
					'create ' + @columnstoreIndexTypeForTSQL + ' columnstore index ' + 
					case @columnstoreIndexTypeForTSQL when 'Clustered' then 'CCI' when 'Nonclustered' then 'NCCI' end 
					+ '_' + t.[ShortTableName] + 
					' on ' + t.TableName + case @columnstoreIndexTypeForTSQL when 'Nonclustered' then '()' else '' end + ';' as [TSQL Command]
				   , 'CCL' as type,
				   101 as [Sort Order]
				from #TablesToColumnstore t
			union all
			select t.TableName, 'alter table ' + t.TableName + ' drop constraint ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], [type], 
				   case UPPER(type) when 'PK' then 100 when 'F' then 1 when 'UQ' then 100 end as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.objects so
					on t.ObjectId = so.parent_object_id or t.ObjectId = so.object_id
				where UPPER(type) in ('PK','F','UQ')
			union all
			select t.TableName, 'drop trigger ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], type,
				50 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.objects so
					on t.ObjectId = so.parent_object_id
				where UPPER(type) in ('TR')
			union all
			select t.TableName, 'drop assembly ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ' WITH NO DEPENDENTS ;' as [TSQL Command], type,
				50 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.objects so
					on t.ObjectId = so.parent_object_id
				where UPPER(type) in ('TA')	
			union all 
			select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'CL' as type,
				10 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.indexes ind
					on t.ObjectId = ind.object_id
				where type = 1 and not exists
					(select 1 from #TablesToColumnstore t1
						inner join sys.objects so1
							on t1.ObjectId = so1.parent_object_id
						where UPPER(so1.type) in ('PK','F','UQ')
							and quotename(ind.name) <> quotename(so1.name))
			union all 
			select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'NC' as type,
				10 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.indexes ind
					on t.ObjectId = ind.object_id
				where type = 2 and not exists
					(select * from #TablesToColumnstore t1
						inner join sys.objects so1
							on t1.ObjectId = so1.parent_object_id 
						where UPPER(so1.type) in ('PK','F','UQ')
							and quotename(ind.name) <> quotename(so1.name) and t.ObjectId = t1.ObjectId )
			union all 
			select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'XML' as type,
				10 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.indexes ind
					on t.ObjectId = ind.object_id
				where type = 3
			union all 
			select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'SPAT' as type,
				10 as [Sort Order]
				from #TablesToColumnstore t
				inner join sys.indexes ind
					on t.ObjectId = ind.object_id
				where type = 4
		) coms
	order by coms.type desc, coms.[Sort Order]; --coms.TableName 
			 
end

drop table #TablesToColumnstore; 
