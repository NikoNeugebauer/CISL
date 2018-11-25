/*
	Columnstore Indexes Scripts Library for SQL Server vNext: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
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

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
*/

-- Params --
declare @schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
		@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
		@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
		@indexLocation varchar(15) = NULL,				-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
		@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
		@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
		@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which were trimmed (not reached the maximum possible size)
		@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
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

-- --Ensure that we are running SQL Server vNext
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'14'
begin
	set @errorMessage = (N'You are not running a SQL Server vNext. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

set nocount on;

select quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)) as [Table Name],
	case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as [Location],	
	rg.partition_number as partition_nr,
	rg.row_group_id,
	rg.state,
	rg.state_desc as state_description,
	rg.total_rows,
	rg.deleted_rows,
	cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
	rg.trim_reason,
	rg.trim_reason_desc,
	rg.transition_to_compressed_state as compress_op, 
	rg.transition_to_compressed_state_desc as compress_op_desc,
	rg.has_vertipaq_optimization as optimised,
	rg.generation,
	rg.closed_time,	
	rg.created_time
	from sys.dm_db_column_store_row_group_physical_stats rg
		inner join sys.indexes ind
			on ind.object_id = rg.object_id and rg.index_id = ind.index_id
	where isnull(rg.trim_reason,1) <> case isnull(@showTrimmedGroupsOnly,-1) when 1 then 1 /* NO_TRIM */ else -1 end 
		and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
		and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
		and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
		and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
		and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%') 
			OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id) = @tableName) )
		and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id ) like '%' + @schemaName + '%')
			OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id ) = @schemaName))
		AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
		and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		and isnull(rg.created_time,getDate()) >= coalesce(@minCreatedDateTime,rg.created_time,getDate()) 
		and isnull(rg.created_time,getDate()) <= coalesce(@maxCreatedDateTime,rg.created_time,getDate())
		and isnull(rg.trim_reason,255) = coalesce(@trimReason, rg.trim_reason,255)
		and isnull(rg.transition_to_compressed_state,255) = coalesce(@compressionOperation,rg.transition_to_compressed_state,255)
		and isnull(rg.has_vertipaq_optimization,1) = case @showNonOptimisedOnly when 1 then 0 else isnull(rg.has_vertipaq_optimization,1) end
union all
select quotename(object_schema_name(rg.object_id, db_id('tempdb'))) + '.' + quotename(object_name(rg.object_id, db_id('tempdb'))) as [Table Name],
	case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as [Location],	
	rg.partition_number as partition_nr,
	rg.row_group_id,
	rg.state,
	rg.state_desc as state_description,
	rg.total_rows,
	rg.deleted_rows,
	cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
	rg.trim_reason,
	rg.trim_reason_desc,
	rg.transition_to_compressed_state as compress_op, 
	rg.transition_to_compressed_state_desc as compress_op_desc,
	rg.has_vertipaq_optimization as optimised,
	rg.generation,
	rg.closed_time,	
	rg.created_time	
	from tempdb.sys.dm_db_column_store_row_group_physical_stats rg
		inner join tempdb.sys.indexes ind
			on ind.object_id = rg.object_id and rg.index_id = ind.index_id
	where isnull(rg.trim_reason,1) <> case isnull(@showTrimmedGroupsOnly,-1) when 1 then 1 /* NO_TRIM */ else -1 end 
		and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
		and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
		and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
		and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
		and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) = @tableName) )
		and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) = @schemaName))
		AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
		and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		and isnull(rg.created_time,getDate()) >= coalesce(@minCreatedDateTime,rg.created_time,getDate()) 
		and isnull(rg.created_time,getDate()) <= coalesce(@maxCreatedDateTime,rg.created_time,getDate())
		and isnull(rg.trim_reason,255) = coalesce(@trimReason, rg.trim_reason,255)
		and isnull(rg.transition_to_compressed_state,255) = coalesce(@compressionOperation,rg.transition_to_compressed_state,255)
		and isnull(rg.has_vertipaq_optimization,1) = case @showNonOptimisedOnly when 1 then 0 else isnull(rg.has_vertipaq_optimization,1) end
	order by [Table Name], rg.partition_number, rg.row_group_id