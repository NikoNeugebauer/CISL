/*
	Columnstore Indexes Scripts Library for Azure SQLDW: 
	Row Groups - Shows detailed information on the Columnstore Row Groups
	Version: 1.5.0, January 2017

	Copyright 2015-2017 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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

-- Params --
--declare @indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
--		@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
--		@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
--		@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
--		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
--		@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
--		@showPartitionDetails bit = 0					-- Allows to show details of each of the available partitions
-- end of --

declare @indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
		@objectType varchar(20) = NULL,					-- Allows to filter the object type with 2 possible supported values: 'Table' & 'Indexed View'
		@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
		@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
		@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
		@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
		@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@showPartitionDetails bit = 1,					-- Allows to show details of each of the available partitions
		@partitionId int = 3							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 


declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductLevel') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDW
if SERVERPROPERTY('EngineEdition') <> 6
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDW: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

select  quotename(schema_name(obj.schema_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'IndexType',
		'Table' as Object,
		'Disk-Based' as Location,
		case count( distinct part.data_compression_desc) when 1 then lower(max(part.data_compression_desc)) else 'Multiple' end  as 'Compression',
		case @showPartitionDetails when 1 then part.partition_number else 1 end as 'Partition',
		--	+ cast(case when count(distinct part.partition_number) = 1 then 1 else count(distinct part.partition_number) end as Varchar(5)) as 'Partitions',	
		count( case state when 0 then rg.object_id  else NULL end ) as 'Bulk Load',
		count( case state when 1 then rg.object_id else NULL end ) as 'Open DS',
		count( case state when 2 then rg.object_id else NULL end ) as 'Closed DS',
		count( case state when 4 then rg.object_id else NULL end ) as 'Tombstones',
		count( case state when 3 then rg.object_id else NULL end ) as 'Compressed',
		sum( case when rg.object_id is not null then 1 else 0 end ) as 'Total',
		cast( sum(isnull(deleted_rows,0) )/1000000./count(distinct part.partition_number) as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(part.rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
		count( distinct part.distribution_id ) as [Distributions],
		count( distinct part.pdw_node_id ) as [Nodes]	
		from sys.indexes ind
			inner join sys.tables obj
				on ind.object_id = obj.object_id
			inner join sys.pdw_index_mappings IndexMap
				on ind.object_id = IndexMap.object_id
					and ind.index_id = IndexMap.index_id
			inner join sys.pdw_nodes_indexes AS NI
				on IndexMap.physical_name = NI.name
					and IndexMap.index_id = NI.index_id
			left join sys.pdw_nodes_column_store_row_groups rg
				on NI.object_id = rg.object_id
					and rg.pdw_node_id = NI.pdw_node_id
					and rg.distribution_id = NI.distribution_id
			inner join sys.pdw_table_mappings as TMAP
				on TMAP.object_id = obj.object_id
			inner join sys.pdw_nodes_tables as NTables
				on NTables.name = TMap.physical_name
					and NTables.pdw_node_id = NI.pdw_node_id
					and NTables.distribution_id = NI.distribution_id
			inner join sys.pdw_nodes_partitions part
				on part.object_id = NTables.object_id
					and part.pdw_node_id = NI.pdw_node_id
					and part.distribution_id = NI.distribution_id
					and part.pdw_node_id = rg.pdw_node_id
					and part.distribution_id = rg.distribution_id
					and part.partition_number = rg.partition_number
		WHERE 	  ind.type >= 5 and ind.type <= 6
				  --and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
				  and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
				  and (@schemaName is null or schema_name(ind.object_id) = @schemaName)
				  and obj.type_desc = ISNULL(case @objectType when 'Table' then 'USER_TABLE' when 'Indexed View' then 'VIEW' end,obj.type_desc)
				  and case @showPartitionDetails when 1 then part.partition_number else 1 end = isnull(@partitionId, case @showPartitionDetails when 1 then part.partition_number else 1 end)  -- Partition Filtering
		group by obj.schema_id, ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) 
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
		order by quotename(schema_name(obj.schema_id)), quotename(object_name(ind.object_id))
				,(case @showPartitionDetails when 1 then part.partition_number else 1 end);
