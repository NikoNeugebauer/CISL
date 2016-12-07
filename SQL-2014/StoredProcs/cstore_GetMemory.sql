/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	MemoryInfo - Shows the content of the Columnstore Object Pool
	Version: 1.4.1, November 2016

	Copyright (C): Niko Neugebauer, OH22 IS (http://www.oh22.is)
	http://www.nikoport.com/columnstore	
	All rights reserved.

	This software is free to use as long as the original notes are included.
	You are not allowed to use this script, nor its modifications in the commercial software.

    THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED 
    TO THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE. 
*/

/*
Changes in 1.0.4
	+ Added new parameter for filtering on the schema - @schemaName
	* Changed the output from '% of Total' to '% of Total Column Structures' for better clarity
	- Fixed error where the delta-stores were counted as one of the objects to be inside Columnstore Object Pool

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.
*/

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetMemory' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetMemory as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	MemoryInfo - Shows the content of the Columnstore Object Pool
	Version: 1.4.1, November 2016
*/
alter procedure dbo.cstore_GetMemory(
-- Params --
	@showColumnDetails bit = 1,					-- Drills down into each of the columns inside the memory
	@showObjectTypeDetails bit = 1,				-- Shows details about the type of the object that is located in memory
	@minMemoryInMb Decimal(8,2) = 0.0,			-- Filters the minimum amount of memory that the Columnstore object should occupy
	@schemaName nvarchar(256) = NULL,			-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,			-- Allows to show data filtered down to 1 particular table
	@objectId int = NULL,						-- Allows to idenitfy a table thorugh the ObjectId
	@columnName nvarchar(256) = NULL,			-- Allows to filter a specific column name
	@objectType nvarchar(50) = NULL				-- Allows to filter a specific type of the memory object. Possible values are 'Segment','Global Dictionary','Local Dictionary','Primary Dictionary Bulk','Deleted Bitmap'
-- end of --
) as 
begin
	set nocount on;

	with memCache as (
		select name, entry_data, pages_kb, cast( '<cache ' + replace(substring(entry_data,2,len(entry_data)-1),'''','"') as xml) as 'cache'
			from sys.dm_os_memory_cache_entries mem
			where type = 'CACHESTORE_COLUMNSTOREOBJECTPOOL'
	),
	MemCacheXML as (
		select cache.value('(/cache/@hobt_id)[1]', 'bigint') as Hobt, 
				part.object_id, part.partition_number,
			object_schema_name(part.object_id) + '.' + object_name(part.object_id) as TableName,
			cache.value('(/cache/@column_id)[1]', 'int')-1 as ColumnId,
			cache.value('(/cache/@object_type)[1]', 'tinyint') as ObjectType,
			memCache.name, 
			entry_data, 
			pages_kb
			from memCache
				inner join sys.partitions part
					on cache.value('(/cache/@hobt_id)[1]', 'bigint') = part.hobt_id 
			where cache.value('(/cache/@db_id)[1]', 'smallint') = db_id()
				and (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%')
				and (@schemaName is null or object_schema_name(part.object_id) = @schemaName)
				and part.object_id = isnull(@objectId, part.object_id)
	)
	select TableName, 
			case @showColumnDetails when 1 then ColumnId else NULL end as ColumnId, 
			case @showColumnDetails when 1 then cols.name else NULL end as ColumnName, 
			case @showColumnDetails when 1 then tp.name else NULL end as ColumnType,
			case @showObjectTypeDetails 
				when 1 then 
					case ObjectType 
						when 1 then 'Segment' 
						when 2 then 'Global Dictionary'
						when 4 then 'Local Dictionary'
						when 5 then 'Primary Dictionary Bulk'
						when 6 then 'Deleted Bitmap'
					else 'Unknown' end
				else NULL end as ObjectType,
			count(*)  as Fragments,
			cast((select count(mem.TableName) * 100./count(distinct rg.row_group_id) 
						 * max(case ObjectType when 1 then 1 else 0 end)									-- Count only Segments
						 * max(case @showObjectTypeDetails & @showColumnDetails when 1 then 1 else 0 end)	-- Show calculations only when @showObjectTypeDetails & @showColumnDetails are set 
						 + max(case @showObjectTypeDetails & @showColumnDetails when 1 then (case ObjectType when 1 then 0 else NULL end) else NULL end)	
																											-- Resets to -1 when when @showObjectTypeDetails & @showColumnDetails are not set 
					from sys.column_store_row_groups rg
						where rg.object_id = mem.object_id
							and rg.state = 3 
							AND rg.delta_store_hobt_id is NULL ) as Decimal(8,2)) as '% of Total Column Structures',
			cast( sum( pages_kb ) / 1024. as Decimal(8,3) ) as 'SizeInMB',
			isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
			isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
			max(stat.last_user_scan) as 'LastScan'
		from MemCacheXML mem
			left join sys.columns cols
				on mem.object_id = cols.object_id and mem.ColumnId = cols.column_id
			inner join sys.types tp
				on cols.system_type_id = tp.system_type_id and cols.user_type_id = tp.user_type_id
			left join sys.dm_db_index_usage_stats stat
				on mem.object_id = stat.object_id
		where cols.name = isnull(@columnName,cols.name)
			and (case ObjectType 
						when 1 then 'Segment' 
						when 2 then 'Global Dictionary'
						when 4 then 'Local Dictionary'
						when 5 then 'Primary Dictionary Bulk'
						when 6 then 'Deleted Bitmap'
					else 'Unknown' end = isnull(@objectType,
													case ObjectType 
														when 1 then 'Segment' 
														when 2 then 'Global Dictionary'
														when 4 then 'Local Dictionary'
														when 5 then 'Primary Dictionary Bulk'
														when 6 then 'Deleted Bitmap'
													else 'Unknown' end))
		group by mem.object_id, TableName, 
				case @showColumnDetails when 1 then ColumnId else NULL end, 
				case @showObjectTypeDetails 
				when 1 then 
					case ObjectType 
						when 1 then 'Segment' 
						when 2 then 'Global Dictionary'
						when 4 then 'Local Dictionary'
						when 5 then 'Primary Dictionary Bulk'
						when 6 then 'Deleted Bitmap'
					else 'Unknown' end
				else NULL end,
				case @showColumnDetails when 1 then cols.name else NULL end, 
				case @showColumnDetails when 1 then tp.name else NULL end
		having sum( pages_kb ) / 1024. >= @minMemoryInMb
		order by TableName, ColumnId, sum( pages_kb ) / 1024. desc;


end
GO
