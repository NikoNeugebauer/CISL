/*
	Columnstore Indexes Scripts Library for Azure SQLDW: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.5.0, August 2017

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
declare @schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
		@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
		@indexLocation varchar(15) = NULL,				-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
		@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
		@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
		@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which were trimmed (not reached the maximum possible size)
		@showNonCompressedOnly bit = 0,					-- Filters out the compressed Row Groups
		@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
		@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
		@maxSizeInMB Decimal(16,3) = NULL, 				-- Maximum size in MB for a table to be included
		@minCreatedDateTime Datetime = NULL,			-- The earliest create datetime for Row Group to be included
		@maxCreatedDateTime Datetime = NULL,			-- The lateste create datetime for Row Group to be included
		@trimReason tinyint = NULL,						-- Row Groups Trimming Reason. The possible values are NULL - do not filter, 1 - NO_TRIM, 2 - BULKLOAD, 3 – REORG, 4 – DICTIONARY_SIZE, 5 – MEMORY_LIMITATION, 6 – RESIDUAL_ROW_GROUP, 7 - STATS_MISMATCH, 8 - SPILLOVER
		@compressionOperation tinyint = NULL,			-- Allows filtering on the compression operation. The possible values are NULL - do not filter, 1- NOT_APPLICABLE, 2 – INDEX_BUILD, 3 – TUPLE_MOVER, 4 – REORG_NORMAL, 5 – REORG_FORCED, 6 - BULKLOAD, 7 - MERGE		
		@showNonOptimisedOnly bit = 0;					-- Allows to filter out the Row Groups that were not optimized with Vertipaq compression
-- end of --

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDW
if SERVERPROPERTY('EngineEdition') <> 6
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDW: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

select  quotename(schema_name(obj.schema_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'IndexType',
		'Table' as Object,
		--'Disk-Based' as Location,
		case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as [Location],	
		part.partition_number, 
		rg.row_group_id,
		rg.pdw_node_id,
		rg.distribution_id, 		
		rg.state,
		rg.state_description,
		rg.total_rows,
		rg.deleted_rows,
		cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]		
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
			WHERE ind.type >= 5 and ind.type <= 6
				--and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%') 
					OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id) = @tableName) )
				and (@preciseSearch = 0 AND (@schemaName is null or schema_name( ind.object_id ) like '%' + @schemaName + '%')
					OR @preciseSearch = 1 AND (@schemaName is null or schema_name( ind.object_id ) = @schemaName))				
				--and isnull(rg.trim_reason,1) <> case isnull(@showTrimmedGroupsOnly,-1) when 1 then 1 /* NO_TRIM */ else -1 end 
				and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
				and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
				and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
				and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
				and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
				and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
				--and isnull(rg.created_time,getDate()) >= coalesce(@minCreatedDateTime,rg.created_time,getDate()) 
				--and isnull(rg.created_time,getDate()) <= coalesce(@maxCreatedDateTime,rg.created_time,getDate())
				--and isnull(rg.trim_reason,255) = coalesce(@trimReason, rg.trim_reason,255)
				--and isnull(rg.transition_to_compressed_state,255) = coalesce(@compressionOperation,rg.transition_to_compressed_state,255)
				--and isnull(rg.has_vertipaq_optimization,1) = case @showNonOptimisedOnly when 1 then 0 else isnull(rg.has_vertipaq_optimization,1) end
		order by quotename(schema_name(obj.schema_id)), quotename(object_name(ind.object_id)),
				part.partition_number, rg.row_group_id, rg.pdw_node_id, rg.distribution_id;
