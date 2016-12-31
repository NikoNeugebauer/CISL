/*
	Columnstore Indexes Scripts Library for Azure SQL Database: 
	Row Groups - Shows detailed information on the Columnstore Row Groups
	Version: 1.4.2, December 2016

	Copyright 2015-2016 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the Deleted Buffer (Nonclustered Columnstore)
	+ Added support for the InMemory Columnstore Index
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed bug with partition information not being shown correctly
	+ Added new parameter for filtering a specific partition
	- Added a couple of bug fixes for the Azure SQLDatabase changes related to Temp Tables

Changes in 1.4.0
	- Added support for the Indexed Views with Nonclustered Columnstore Indexes
	- Added new parameter for filtering the Columnstore Object Type with possible values 'Table' & 'Indexed View'

Changes in 1.4.2
	- Fixed bug on lookup for the Object Name for the empty Columnstore tables
*/

-- Params --
declare @indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
		@objectType varchar(20) = NULL,					-- Allows to filter the object type with 2 possible supported values: 'Table' & 'Indexed View'
		@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
		@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
		@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
		@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
		@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
		@showPartitionDetails bit = 0,					-- Allows to show details of each of the available partitions
		@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

set nocount on;

with partitionedInfo as (
	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
			case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
			case obj.type_desc when 'USER_TABLE' then 'Table' when 'VIEW' then 'Indexed View' else obj.type_desc end as ObjectType,
			case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
			part.partition_number as Partition, 
			case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
			sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
			sum(case state when 1 then 1 else 0 end) as 'Open DS',
			sum(case state when 2 then 1 else 0 end) as 'Closed DS',
			sum(case state when 4 then 1 else 0 end) as 'Tombstones',	
			sum(case state when 3 then 1 else 0 end) as 'Compressed',
			count(rg.object_id) as 'Total',
			cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + 
					(select isnull(sum(intpart.rows),0)
						from sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   )/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
			cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) -
					(select isnull(sum(intpart.rows),0)
						from sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   ) /1000000. as Decimal(16,6)) as 'Active Rows (M)',
			cast( sum(isnull(case state when 4 then 0 else total_rows end,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
			cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as 'Size in GB',
			isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
			isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
			max(stat.last_user_scan) as 'LastScan'
			from sys.indexes ind
				inner join sys.objects obj
					on ind.object_id = obj.object_id
				left join sys.column_store_row_groups rg
					on ind.object_id = rg.object_id and ind.index_id = rg.index_id
				left join sys.partitions part with(READUNCOMMITTED)
					on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
				left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
					on rg.object_id = stat.object_id and ind.index_id = stat.index_id
			where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
				  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				  and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
				  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
				  and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
				  and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
				  and obj.type_desc = ISNULL(case @objectType when 'Table' then 'USER_TABLE' when 'Indexed View' then 'VIEW' end,obj.type_desc)
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
	select quotename(isnull(object_schema_name(obj.object_id, db_id('tempdb')),'dbo')) + '.' + quotename(obj.name),
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		case obj.type_desc when 'USER_TABLE' then 'Table' when 'VIEW' then 'Indexed View' else obj.type_desc end as ObjectType,
		case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
		part.partition_number as Partition,
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
			sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
			sum(case state when 1 then 1 else 0 end) as 'Open DS',
			sum(case state when 2 then 1 else 0 end) as 'Closed DS',
			sum(case state when 4 then 1 else 0 end) as 'Tombstones',	
			sum(case state when 3 then 1 else 0 end) as 'Compressed',
			count(rg.object_id) as 'Total',	
		cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + 
					(select isnull(sum(intpart.rows),0)
						from tempdb.sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   )/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
			cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) -
					(select isnull(sum(intpart.rows),0)
						from tempdb.sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   ) /1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as 'Size in GB',
		isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
		isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
		max(stat.last_user_scan) as 'LastScan'
		from tempdb.sys.indexes ind
			inner join tempdb.sys.objects obj
				on ind.object_id = obj.object_id
			left join tempdb.sys.column_store_row_groups rg
				on ind.object_id = rg.object_id and ind.index_id = rg.index_id
			left join tempdb.sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
				and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
				and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
				and (@tableName is null or ind.name like '%' + @tableName + '%')
				and (@schemaName is null or object_schema_name(ind.object_id, db_id('tempdb')) = @schemaName)
		--		and isnull(stat.database_id,db_id('tempdb')) = db_id('tempdb')
				and obj.type_desc = ISNULL(case @objectType when 'Table' then 'USER_TABLE' when 'Indexed View' then 'VIEW' end,obj.type_desc)
		group by ind.object_id, obj.object_id, obj.type_desc, obj.name, ind.type, rg.partition_number,
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
	select TableName, 
		Type, 
		ObjectType,
		Location, (case @showPartitionDetails when 1 then Partition else 1 end) as [Partition], 
		max([Compression Type]) as [Compression Type], sum([Bulk Load RG]) as [Bulk Load RG], sum([Open DS]) as [Open DS], sum([Closed DS]) as [Closed DS], 
		sum(Tombstones) as Tombstones, sum(Compressed) as Compressed, sum(Total) as Total, 
		sum([Deleted Rows (M)]) as [Deleted Rows (M)], sum([Active Rows (M)]) as [Active Rows (M)], sum([Total Rows (M)]) as [Total Rows (M)], 
		sum([Size in GB]) as [Size in GB], sum(Scans) as Scans, sum(Updates) as Updates, max(LastScan) as LastScan
		from partitionedInfo
		where Partition = isnull(@partitionId, Partition)  -- Partition Filtering
		group by TableName, Type, ObjectType, Location, (case @showPartitionDetails when 1 then Partition else 1 end)
		order by TableName,	(case @showPartitionDetails when 1 then Partition else 1 end);