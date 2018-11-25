/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
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

Changes in 1.0.3
	+ Added parameter for showing aggregated information on the whole table, instead of partitioned view as before
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Removed Tombstones from the calculations of Deleted Rows, Active Rows and Total Rows
	- Fixed bug with including aggregating tables without taking care of the database name, thus potentially including results from the equally named table from a different database	
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the Deleted Buffer (Nonclustered Columnstore)
	+ Added support for the InMemory Columnstore Index
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed bug with partition information not being shown correctly
	+ Added new parameter for filtering a specific partition

Changes in 1.4.0
	- Fixed an extremely rare bug with the sys.dm_db_index_usage_stats DMV, where it contains queries for the local databases object made from other databases only
	- Added support for the Indexed Views with Nonclustered Columnstore Indexes
	- Added new parameter for filtering the Columnstore Object Type with possible values 'Table' & 'Indexed View'

Changes in 1.4.1
	+ Added support for the SP1 which allows support of Columnstore Indexes on any edition

Changes in 1.4.2
	- Fixed bug on lookup for the Object Name for the empty Columnstore tables

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0

Changes in 1.6.0
	+ Added new parameter for specifying the name of the database, where the Columnstore Indexes should be located (@dbName)
	* Greatly improved performance against the databases with thousands of Row Groups	
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroups as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.6.0, January 2018
*/
alter procedure dbo.cstore_GetRowGroups(
-- Params --
	@dbName SYSNAME = NULL,							-- Identifies the Database to run the stored procedure against. If this parameter is left to be NULL, then the current database is used
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectType varchar(20) = NULL,					-- Allows to filter the object type with 2 possible supported values: 'Table' & 'Indexed View'
	@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
	@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
	@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
	@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionDetails bit = 0,					-- Allows to show details of each of the available partitions
	@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --
	) as
begin
	SET ANSI_WARNINGS OFF;
	SET NOCOUNT ON;

	IF @dbName IS NULL
		SET @dbName = DB_NAME(DB_ID());

	DECLARE @dbId INT = DB_ID(@dbName);
	DECLARE @sql NVARCHAR(MAX);
	
	IF NOT EXISTS (SELECT 1 FROM sys.databases WHERE name = @dbName)
	BEGIN
		DECLARE @errorMessage NVARCHAR(500) = 'The Database ''' + @dbName + ''' does not exist on this SQL Server Instance!';
		THROW 51000, @errorMessage, 1;
	END
	
	SET @sql = N'
	with partitionedInfo as (
	select quotename(object_schema_name(ind.object_id, @dbId)) + ''.'' + quotename(object_name(ind.object_id, @dbId)) as [TableName], 
			case ind.type when 5 then ''Clustered'' when 6 then ''Nonclustered'' end as [Type],
			case obj.type_desc when ''USER_TABLE'' then ''Table'' when ''VIEW'' then ''Indexed View'' else obj.type_desc end as [ObjectType],
			case ind.data_space_id when 0 then ''In-Memory'' else ''Disk-Based'' end as [Location],
			part.partition_number as Partition, 
			case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else ''Multiple'' end  as [Compression Type],
			sum(case state when 0 then 1 else 0 end) as [Bulk Load RG],
			sum(case state when 1 then 1 else 0 end) as [Open DS],
			sum(case state when 2 then 1 else 0 end) as [Closed DS],
			sum(case state when 4 then 1 else 0 end) as [Tombstones],	
			sum(case state when 3 then 1 else 0 end) as [Compressed],
			count(rg.object_id) as [Total],
			cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + isnull(sum(intpart.rows),0)
				   )/1000000. as Decimal(16,6)) as [Deleted Rows (M)],
			cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) - isnull(sum(intpart.rows),0)
				   )/1000000. as Decimal(16,6)) as [Active Rows (M)],	
			cast( sum(isnull(case state when 4 then 0 else total_rows end,0))/1000000. as Decimal(16,6)) as [Total Rows (M)],
			cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from ' + QUOTENAME(@dbName) + '.sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as [Size in GB],
			isnull(sum(stat.user_scans)/count(*),0) as [Scans],
			isnull(sum(stat.user_updates)/count(*),0) as [Updates],
			max(stat.last_user_scan) as [LastScan]
			from ' + QUOTENAME(@dbName) + N'.sys.indexes ind
				inner join ' + QUOTENAME(@dbName) + N'.sys.objects obj
					on ind.object_id = obj.object_id
				left join ' + QUOTENAME(@dbName) + N'.sys.column_store_row_groups rg
					on ind.object_id = rg.object_id and ind.index_id = rg.index_id
				left join ' + QUOTENAME(@dbName) + N'.sys.partitions part with(READUNCOMMITTED)
					on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
					AND ind.index_id = part.index_id
				left join ' + QUOTENAME(@dbName) + N'.sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
					on rg.object_id = stat.object_id and ind.index_id = stat.index_id
					   and isnull(stat.database_id,db_id()) = db_id()
				LEFT HASH JOIN ' + QUOTENAME(@dbName) + N'.sys.internal_partitions intpart
					ON ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4
			where ind.type >= 5 and ind.type <= 6				-- Clustered & Nonclustered Columnstore
				  and part.data_compression BETWEEN 3 AND 4
				  and ind.data_space_id = isnull( case @indexLocation when ''In-Memory'' then 0 when ''Disk-Based'' then 1 else ind.data_space_id end, ind.data_space_id )
				  and case @indexType when ''CC'' then 5 when ''NC'' then 6 else ind.type end = ind.type
				  and case @compressionType when ''Columnstore'' then 3 when ''Archive'' then 4 else part.data_compression end = part.data_compression
		 		  and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id, @dbId) like ''%'' + @tableName + ''%'') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id, @dbId) = @tableName) )
				  and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name(ind.object_id, @dbId) like N''%'' + @schemaName + ''%'')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name(ind.object_id, @dbId) = @schemaName))
				  AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
				  and obj.type_desc = ISNULL(case @objectType when ''Table'' then ''USER_TABLE'' when ''Indexed View'' then ''VIEW'' end,obj.type_desc)
			group by ind.object_id, ind.type, obj.type_desc, rg.partition_number, ind.data_space_id,
					part.partition_number
			having cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) + 
					  (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
							from sys.dm_db_xtp_memory_consumers xtpMem 
							where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */) 
					 )
					as Decimal(8,2)) >= @minSizeInGB
					and sum(isnull(total_rows,0)) >= @minTotalRows
	union all
	select quotename(object_schema_name(ind.object_id, db_id(''tempdb''))) + ''.'' + quotename(object_name(ind.object_id, db_id(''tempdb''))) as [TableName], 
		case ind.type when 5 then ''Clustered'' when 6 then ''Nonclustered'' end as [Type],
		case obj.type_desc when ''USER_TABLE'' then ''Table'' when ''VIEW'' then ''Indexed View'' else obj.type_desc end as ObjectType,
		case ind.data_space_id when 0 then ''In-Memory'' else ''Disk-Based'' end as [Location],
		part.partition_number as Partition,
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else ''Multiple'' end  as [Compression Type],
			sum(case state when 0 then 1 else 0 end) as [Bulk Load RG],
			sum(case state when 1 then 1 else 0 end) as [Open DS],
			sum(case state when 2 then 1 else 0 end) as [Closed DS],
			sum(case state when 4 then 1 else 0 end) as [Tombstones],	
			sum(case state when 3 then 1 else 0 end) as [Compressed],
			count(rg.object_id) as [Total],	
		cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + isnull(sum(intpart.rows),0)
				)/1000000. as Decimal(16,6)) as [Deleted Rows (M)],
		cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) - isnull(sum(intpart.rows),0)
				)/1000000. as Decimal(16,6)) as [Active Rows (M)],	
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as [Total Rows (M)],
		cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as [Size in GB],
			isnull(sum(stat.user_scans)/count(*),0) as [Scans],
			isnull(sum(stat.user_updates)/count(*),0) as [Updates],
			max(stat.last_user_scan) as [LastScan]
		from tempdb.sys.indexes ind
			inner join sys.objects obj
				on ind.object_id = obj.object_id
			left join tempdb.sys.column_store_row_groups rg
				on ind.object_id = rg.object_id and ind.index_id = rg.index_id
			left join tempdb.sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join tempdb.sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
			LEFT HASH JOIN tempdb.sys.internal_partitions intpart
					ON ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4
		where ind.type >= 5 and ind.type <= 6				-- Clustered & Nonclustered Columnstore
				and part.data_compression BETWEEN 3 AND 4   
				and case @indexType when ''CC'' then 5 when ''NC'' then 6 else ind.type end = ind.type
				and ind.data_space_id = isnull( case @indexLocation when ''In-Memory'' then 0 when ''Disk-Based'' then 1 else ind.data_space_id end, ind.data_space_id )
				and case @compressionType when ''Columnstore'' then 3 when ''Archive'' then 4 else part.data_compression end = part.data_compression
				and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id,db_id(''tempdb'')) like ''%'' + @tableName + ''%'') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id,db_id(''tempdb'')) = @tableName) )
				and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id,db_id(''tempdb'') ) like ''%'' + @schemaName + ''%'')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id,db_id(''tempdb'') ) = @schemaName))
				AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
				and obj.type_desc = ISNULL(case @objectType when ''Table'' then ''USER_TABLE'' when ''Indexed View'' then ''VIEW'' end,obj.type_desc)
		group by ind.object_id, ind.type, obj.type_desc, rg.partition_number,
				ind.data_space_id,
				part.partition_number
		having cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) + 
					  (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
							from tempdb.sys.dm_db_xtp_memory_consumers xtpMem 
							where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */) 
					 )
					as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
	)
	select [TableName], 
		[Type], 
		[ObjectType],
		[Location], (case @showPartitionDetails when 1 then Partition else 1 end) as [Partition], 
		max([Compression Type]) as [Compression Type], 
		sum([Bulk Load RG]) as [Bulk Load RG], 
		sum([Open DS]) as [Open DS], 
		sum([Closed DS]) as [Closed DS], 
		sum(Tombstones) as Tombstones, 
		sum(Compressed) as Compressed, 
		sum(Total) as Total, 
		sum([Deleted Rows (M)]) as [Deleted Rows (M)], sum([Active Rows (M)]) as [Active Rows (M)], sum([Total Rows (M)]) as [Total Rows (M)], 
		sum([Size in GB]) as [Size in GB], sum(ISNULL(Scans,0)) as Scans, sum(ISNULL(Updates,0)) as Updates, NULL as LastScan
		from partitionedInfo
		where Partition = isnull(@partitionId, Partition)  -- Partition Filtering
		group by TableName, Type, ObjectType, Location, (case @showPartitionDetails when 1 then Partition else 1 end)
		order by TableName,	(case @showPartitionDetails when 1 then Partition else 1 end)
		';

	SET ANSI_WARNINGS ON; 

	DECLARE @paramDefinition NVARCHAR(1000) =  '@indexType char(2),				
												@indexLocation varchar(15),			
												@objectType varchar(20),				
												@compressionType varchar(15),		
												@minTotalRows bigint,				
												@minSizeInGB Decimal(16,3),			
												@preciseSearch bit,						
												@tableName nvarchar(256),			
												@schemaName nvarchar(256),			
												@objectId int,						
												@showPartitionDetails bit,				
												@partitionId int,
												@dbId int';						

	EXEC sp_executesql @sql, @paramDefinition, @indexType = @indexType, @indexLocation = @indexLocation,
											   @objectType = @objectType, @compressionType = @compressionType,
											   @minTotalRows = @minTotalRows, @minSizeInGB = @minSizeInGB,
											   @preciseSearch = @preciseSearch, @tableName = @tableName,
											   @schemaName = @schemaName, @objectId = @objectId, 
											   @showPartitionDetails = @showPartitionDetails, @partitionId = @partitionId,
											   @dbId = @dbId;
end

GO

