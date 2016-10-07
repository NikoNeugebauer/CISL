/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.4.0, October 2016

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
	- View Permission State is required to run this stored procedure.

Changes in 1.0.3
	+ Added parameter for showing aggregated information on the whole table, instead of partitioned view as before
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed bug with showing 1 row group for an empty Columnstore (now showing correctly 0 row groups)
	- Fixed bugs for filtering by schema & name of the columnstore table (it is now using the sys.indexes DMV as the base, thus guaranteeing correct results for the empty Columnstore)	
	- Fixed bug with including aggregating tables without taking care of the database name, thus potentially including results from the equally named table from a different database
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added new parameter for filtering a specific partition
	+ Added new column for the Index Location (Disk-Based)

Changes in 1.4.0
	- Fixed an extremely rare bug with the sys.dm_db_index_usage_stats DMV, where it contains queries for the local databases object made from other databases only
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

	--Ensure that we are running SQL Server 2014
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroups as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.4.0, October 2016
*/
alter procedure dbo.cstore_GetRowGroups(
-- Params --
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
	@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
	@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionDetails bit = 0,					-- Allows to show details of each of the available partitions
	@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --
	) as
begin
	set nocount on;

	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		'Disk-Based' as Location,
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case rg.state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case rg.state when 1 then 1 else 0 end) as 'Open DS',
		sum(case rg.state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case rg.state when 3 then 1 else 0 end) as 'Compressed',
		count(rg.row_group_id) as 'Total',
		cast( sum(isnull(rg.deleted_rows,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(rg.total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(rg.size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
		isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
		isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
		max(stat.last_user_scan) as 'LastScan'
		from sys.indexes ind
			left join sys.column_store_row_groups rg
				on ind.object_id = rg.object_id
			left join sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
					and isnull(stat.database_id,db_id()) = db_id()
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
			  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			  and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
			  and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
			  and ind.object_id = isnull(@objectId, ind.object_id)
			  and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) --, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
	union all
	select quotename(object_schema_name(ind.object_id, db_id('tempdb'))) + '.' + quotename(object_name(ind.object_id, db_id('tempdb'))) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		'Disk-Based' as Location,
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case rg.state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case rg.state when 1 then 1 else 0 end) as 'Open DS',
		sum(case rg.state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case rg.state when 3 then 1 else 0 end) as 'Compressed',
		count(rg.row_group_id) as 'Total',
		cast( sum(isnull(rg.deleted_rows,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(rg.total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(rg.size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
		isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
		isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
		max(stat.last_user_scan) as 'LastScan'
		from tempdb.sys.indexes ind
			left join tempdb.sys.column_store_row_groups rg
				on ind.object_id = rg.object_id
			left join tempdb.sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join tempdb.sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
			  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			  and (@tableName is null or object_name (ind.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			  and (@schemaName is null or object_schema_name(ind.object_id, db_id('tempdb')) = @schemaName)
			  and ind.object_id = isnull(@objectId, ind.object_id)
			  and isnull(stat.database_id,db_id('tempdb')) = db_id('tempdb')
			  and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) --, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
		order by TableName,
				(case @showPartitionDetails when 1 then part.partition_number else 1 end);

end
GO
