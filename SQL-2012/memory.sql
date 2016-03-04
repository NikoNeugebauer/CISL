/*
       Columnstore Indexes Scripts Library for SQL Server 2012: 
       MemoryInfo - Shows the content of the Columnstore Object Pool
       Version: 1.2.0, March 2016

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
Changes in 1.0.4
	+ Added new parameter for filtering on the schema - @schemaName
	* Changed the output from '% of Total' to '% of Total Column Structures' for better clarity
	- Fixed error where the delta-stores were counted as one of the objects to be inside Columnstore Object Pool
*/

-- Params --
declare
	@showColumnDetails bit = 1,					-- Drills down into each of the columns inside the memory
	@showObjectTypeDetails bit = 1,				-- Shows details about the type of the object that is located in memory
	@minMemoryInMb Decimal(8,2) = 0.0,			-- Filters the minimum amount of memory that the Columnstore object should occupy
	@schemaName nvarchar(256) = NULL,			-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,			-- Allows to show data filtered down to 1 particular table
	@columnName nvarchar(256) = NULL,			-- Allows to filter a specific column name
	@objectType nvarchar(50) = NULL;			-- Allows to filter a specific type of the memory object. Possible values are 'Segment','Global Dictionary','Local Dictionary','Primary Dictionary Bulk','Deleted Bitmap'
-- end of --

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
             @SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
             @SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2012
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

--------------------------------------------------------------------------------------------------------------------
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
             cast((select count(mem.TableName) * 100./count(distinct rg.segment_id) 
                                   * max(case ObjectType when 1 then 1 else 0 end)                                                           -- Count only Segments
                                  * max(case @showObjectTypeDetails & @showColumnDetails when 1 then 1 else 0 end)  -- Show calculations only when @showObjectTypeDetails & @showColumnDetails are set 
                                   + max(case @showObjectTypeDetails & @showColumnDetails when 1 then (case ObjectType when 1 then 0 else NULL end) else NULL end)      
                                                                                                                                                                               -- Resets to -1 when when @showObjectTypeDetails & @showColumnDetails are not set 
                           from sys.column_store_segments rg
                                                  inner join sys.partitions part
                                                       on rg.hobt_id = part.hobt_id and rg.partition_id = part.partition_id
                                               where part.object_id = mem.object_id) as Decimal(8,2)) as '% of Total',
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




