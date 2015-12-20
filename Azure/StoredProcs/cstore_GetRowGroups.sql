/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.0.4, December 2015

	Copyright 2015 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	Drop Procedure dbo.cstore_GetRowGroups;
GO

create procedure dbo.cstore_GetRowGroups(
-- Params --
		@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
		@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
		@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
		@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
		@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@showPartitionDetails bit = 1					-- Allows to show details of each of the available partitions
-- end of --
	) as
begin
	set nocount on;

	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case state when 1 then 1 else 0 end) as 'Open DS',
		sum(case state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case state when 4 then 1 else 0 end) as 'Tombstones',
		sum(case state when 3 then 1 else 0 end) as 'Compressed',
		count(*) as 'Total',
		cast( sum(isnull(deleted_rows,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
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
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
			  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			  and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
			  and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end)--, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
		order by quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)),
				(case @showPartitionDetails when 1 then part.partition_number else 1 end);
end

