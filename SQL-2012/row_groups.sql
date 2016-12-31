/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	Row Groups - Shows detailed information on the Columnstore Row Groups
	Version: 1.4.1, November 2016

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
	- Fixed bug with conversion to bigint for row_count
	- Fixed bug with including aggregating tables without taking care of the database name, thus potentially including results from the equally named table from a different database	
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added new parameter for filtering a specific partition

Changes in 1.4.0
	- Fixed an extremely rare bug with the sys.dm_db_index_usage_stats DMV, where it contains queries for the local databases object made from other databases only

Changes in 1.4.2
	- Fixed bug on lookup for the Object Name for the empty Columnstore tables
*/

-- Params --
declare @indexType char(2) = NULL,						-- Ignored for this version
		@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
		@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
		@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
		@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@showPartitionDetails bit = 1,					-- Allows to show details of each of the available partitions
		@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

 --Ensure that we are running SQL Server 2012
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'11'
begin
	set @errorMessage = (N'You are not running a SQL Server 2012. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2012 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

set nocount on;

select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
	case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
	(case @showPartitionDetails when 1 then ISNULL(part.partition_number,1) else 1 end) as 'Partition',
	ISNULL(part.data_compression_desc,'COLUMNSTORE') as 'Compression Type',
	0 as 'Bulk Load RG',
	0 as 'Open DS',
	0 as 'Closed DS',
	count(distinct segment_id) as 'Compressed',
	count(distinct segment_id) as 'Total',
	cast( sum(isnull(0,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
	cast( sum(isnull(cast(row_count as bigint)-0,0))/(case count(distinct column_id) when 0 then 1 else count(distinct column_id) end)/1000000. as Decimal(16,6)) as 'Active Rows (M)',
	cast( sum(isnull(cast(row_count as bigint),0))/(case count(distinct column_id) when 0 then 1 else count(distinct column_id) end)/1000000. as Decimal(16,6)) as 'Total Rows (M)',
	cast( sum(isnull(on_disk_size,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
	isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
	isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
	max(stat.last_user_scan) as 'LastScan'
	from sys.column_store_segments rg		
		left join sys.partitions part with(READUNCOMMITTED)
			on rg.hobt_id = part.hobt_id and isnull(rg.partition_id,1) = part.partition_id
		right join sys.indexes ind
			on ind.object_id = part.object_id 
		left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
			on part.object_id = stat.object_id and ind.index_id = stat.index_id
				and isnull(stat.database_id,db_id()) = db_id()			  
	where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
			and ((part.object_id is NOT NULL 
				and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
				and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
				)
				OR
				part.object_id is NULL )	
	group by ind.object_id, ind.type, part.partition_number, part.data_compression_desc
	having cast( sum(isnull(on_disk_size,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
			and sum(isnull(cast(row_count as bigint),0)) >= @minTotalRows
union all
select quotename(object_schema_name(ind.object_id, db_id('tempdb'))) + '.' + quotename(object_name(ind.object_id, db_id('tempdb'))) as 'TableName', 
	case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
	(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
	part.data_compression_desc as 'Compression Type',
	0 as 'Bulk Load RG',
	0 as 'Open DS',
	0 as 'Closed DS',
	count(distinct segment_id) as 'Compressed',
	count(distinct segment_id) as 'Total',
	cast( sum(isnull(0,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
	cast( sum(isnull(cast(row_count as bigint)-0,0))/count(distinct column_id)/1000000. as Decimal(16,6)) as 'Active Rows (M)',
	cast( sum(isnull(cast(row_count as bigint),0))/count(distinct column_id)/1000000. as Decimal(16,6)) as 'Total Rows (M)',
	cast( sum(isnull(on_disk_size,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
	isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
	isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
	max(stat.last_user_scan) as 'LastScan'
	from tempdb.sys.column_store_segments rg		
		left join tempdb.sys.partitions part with(READUNCOMMITTED)
			on rg.hobt_id = part.hobt_id and isnull(rg.partition_id,1) = part.partition_id
		inner join tempdb.sys.indexes ind
			on ind.object_id = part.object_id 
		left join tempdb.sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
			on part.object_id = stat.object_id and ind.index_id = stat.index_id
	where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			and part.data_compression_desc in ('COLUMNSTORE') 
			and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			and (@tableName is null or object_name (part.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(part.object_id, db_id('tempdb')) = @schemaName)
			and isnull(stat.database_id, db_id('tempdb')) = db_id('tempdb')	
			and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
	group by ind.object_id, ind.type, part.partition_number, part.data_compression_desc
	having cast( sum(isnull(on_disk_size,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
			and sum(isnull(cast(row_count as bigint),0)) >= @minTotalRows
	order by TableName,
			(case @showPartitionDetails when 1 then ISNULL(part.partition_number,1) else 1 end);

