/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
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

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the SQL Server 2016 internals information on Row Group Trimming, Build Process, Vertipaq Optimisations, Sequential Generation Id, Closed DateTime & Creation DateTime
	+ Added 7 new parameters for filtering out the Index Location (In-Memory or Disk-Based), Index Type (CC or NC), Row Group Trimming, Build Process Identification, Vertipaq Optimisations, Min & Max Creation DateTimes

Changes in 1.3.1
	- Fixed missing ORDER BY clause which was ordering the Row Groups by the object, partition and the row group number

Changes in 1.4.1
	+ Added support for the SP1 which allows support of Columnstore Indexes on any edition
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroupsDetails as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.4.1, November 2016
*/
alter procedure dbo.cstore_GetRowGroupsDetails(
-- Params --
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
	@indexLocation varchar(15) = NULL,				-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
	@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which size <> 1048576
	@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
	@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
	@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
	@maxSizeInMB Decimal(16,3) = NULL, 				-- Maximum size in MB for a table to be included
	@minCreatedDateTime Datetime = NULL,			-- The earliest create datetime for Row Group to be included
	@maxCreatedDateTime Datetime = NULL,			-- The lateste create datetime for Row Group to be included
	@trimReason tinyint = NULL,						-- Row Groups Trimming Reason. The possible values are NULL - do not filter, 1 - NO_TRIM, 2 - BULKLOAD, 3 – REORG, 4 – DICTIONARY_SIZE, 5 – MEMORY_LIMITATION, 6 – RESIDUAL_ROW_GROUP, 7 - STATS_MISMATCH, 8 - SPILLOVER
	@compressionOperation tinyint = NULL,			-- Allows filtering on the compression operation. The possible values are NULL - do not filter, 1- NOT_APPLICABLE, 2 – INDEX_BUILD, 3 – TUPLE_MOVER, 4 – REORG_NORMAL, 5 – REORG_FORCED, 6 - BULKLOAD, 7 - MERGE		
	@showNonOptimisedOnly bit = 0					-- Allows to filter out the Row Groups that were not optimized with Vertipaq compression
-- end of --
	) as
BEGIN
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
			and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
			and rg.object_id = isnull(@objectId, rg.object_id)
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
			and (@tableName is null or object_name (rg.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id, db_id('tempdb')) = @schemaName)
			and rg.object_id = isnull(@objectId, rg.object_id)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
			and isnull(rg.created_time,getDate()) >= coalesce(@minCreatedDateTime,rg.created_time,getDate()) 
			and isnull(rg.created_time,getDate()) <= coalesce(@maxCreatedDateTime,rg.created_time,getDate())
			and isnull(rg.trim_reason,255) = coalesce(@trimReason, rg.trim_reason,255)
			and isnull(rg.transition_to_compressed_state,255) = coalesce(@compressionOperation,rg.transition_to_compressed_state,255)
			and isnull(rg.has_vertipaq_optimization,1) = case @showNonOptimisedOnly when 1 then 0 else isnull(rg.has_vertipaq_optimization,1) end
	order by [Table Name], rg.partition_number, rg.row_group_id
END

GO
