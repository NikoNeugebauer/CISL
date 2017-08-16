/*
	Columnstore Indexes Scripts Library for Azure SQLDW: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
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
declare
 	@showDetails bit = 0,								-- Enables showing the details of all Dictionaries
	@showWarningsOnly bit = 0,							-- Enables to filter out the dictionaries based on the Dictionary Size (@warningDictionarySizeInMB) and Entry Count (@warningEntryCount)
	@warningDictionarySizeInMB Decimal(8,2) = 6.,		-- The size of the dictionary, after which the dictionary should be selected. The value is in Megabytes 
	@warningEntryCount Int = 1000000,					-- Enables selecting of dictionaries with more than this number 
	@showAllTextDictionaries bit = 0,					-- Enables selecting all textual dictionaries independently from their warning status
	@showDictionaryType nvarchar(52) = NULL,			-- Enables to filter out dictionaries by type with possible values 'Local', 'Global' or NULL for both 
	@schemaName nvarchar(256) = NULL,					-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,					-- Allows to show data filtered down to 1 particular table
	@preciseSearch bit = 0,								-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@objectId INT = NULL,								-- Allows to show data filtered down to the specific object_id
	@partitionNumber int = 0,							-- Allows to filter data on a specific partion. 
	@columnName nvarchar(256) = NULL;					-- Allows to filter out data base on 1 particular column name
-- end of --


--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDW
if SERVERPROPERTY('EngineEdition') <> 6
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDW: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
SELECT QuoteName(schema_name(obj.schema_id)) + '.' + QuoteName(object_name(ind.object_id)) as 'TableName', 
		part.partition_number as 'Partition',
		(select count(distinct rg.row_group_id) 
			from sys.pdw_nodes_column_store_row_groups rg
			where NI.object_id = rg.object_id
					--and rg.pdw_node_id = NI.pdw_node_id
					--and rg.index_id = NI.index_id
					--and rg.distribution_id = NI.distribution_id
					and rg.partition_number = part.partition_number
				    and rg.state = 3 
				  ) as 'RowGroups',
		count(csd.column_id) as 'Dictionaries',
		sum(csd.entry_count) as 'EntriesCount',
		(select sum(rg2.total_rows) 
			from sys.pdw_nodes_column_store_row_groups rg2
			where NI.object_id = rg2.object_id
					and rg2.partition_number = part.partition_number
				  ) as 'Rows Serving',
		cast( SUM(csd.on_disk_size)/(1024.0*1024.0) as Decimal(8,3)) as 'Total Size in MB',
		cast( MAX(case dictionary_id when 0 then csd.on_disk_size else 0 end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Global Size in MB',
		cast( MAX(case dictionary_id when 0 then 0 else csd.on_disk_size end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Local Size in MB'
	from sys.indexes ind
		inner join sys.tables obj
			on ind.object_id = obj.object_id
		inner join sys.pdw_index_mappings IndexMap
			on ind.object_id = IndexMap.object_id
				and ind.index_id = IndexMap.index_id
		inner join sys.pdw_nodes_indexes AS NI
			on IndexMap.physical_name = NI.name
				and IndexMap.index_id = NI.index_id
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
		inner join sys.pdw_nodes_column_store_dictionaries AS csd
			on csd.hobt_id = part.hobt_id 
				and csd.partition_id = part.partition_id	
				and csd.pdw_node_id = NI.pdw_node_id
				and csd.distribution_id = NI.distribution_id
		inner join sys.pdw_nodes_column_store_row_groups rg
			on NI.object_id = rg.object_id
				and rg.pdw_node_id = NI.pdw_node_id
				and rg.index_id = NI.index_id
				and rg.distribution_id = NI.distribution_id
    where ind.type in (5,6)
		AND (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%') 
			OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id) = @tableName) )
		AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id ) like '%' + @schemaName + '%')
			OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id ) = @schemaName))
		AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
		AND part.partition_number = case @partitionNumber when 0 then part.partition_number else @partitionNumber end
	group by QuoteName(schema_name(obj.schema_id)) + '.' + QuoteName(object_name(ind.object_id)), 
		ind.object_id, 
		part.partition_number,
		NI.object_id
	order by ind.object_id, part.partition_number;


if @showDetails = 1
select QuoteName(schema_name(obj.schema_id)) + '.' + QuoteName(object_name(ind.object_id)) as 'TableName',
		ind.name as 'IndexName', 
		part.partition_number as 'Partition',
		csd.pdw_node_id,
		csd.distribution_id,
		cols.name as ColumnName, 
		csd.column_id as ColumnId,
		csd.dictionary_id as 'SegmentId',
		tp.name as ColumnType,
		csd.column_id as 'ColumnId', 
		case dictionary_id when 0 then 'Global' else 'Local' end as 'Type', 
		part.rows as 'Rows Serving', 
		entry_count as 'Entry Count', 
		cast( on_disk_size / 1024. / 1024. as Decimal(8,2)) 'SizeInMb'
	from sys.indexes ind
		inner join sys.tables obj
			on ind.object_id = obj.object_id
		inner join sys.pdw_index_mappings IndexMap
			on ind.object_id = IndexMap.object_id
				and ind.index_id = IndexMap.index_id
		inner join sys.pdw_nodes_indexes AS NI
			on IndexMap.physical_name = NI.name
				and IndexMap.index_id = NI.index_id
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
		inner join sys.pdw_nodes_column_store_dictionaries AS csd
			on csd.hobt_id = part.hobt_id 
				and csd.partition_id = part.partition_id	
				and csd.pdw_node_id = NI.pdw_node_id
				and csd.distribution_id = NI.distribution_id
		inner join sys.columns cols
			on ind.object_id = cols.object_id and csd.column_id = cols.column_id
		inner join sys.types tp
			on cols.system_type_id = tp.system_type_id and cols.user_type_id = tp.user_type_id
	where 
		(( @showWarningsOnly = 1 
			AND 
			( cast( on_disk_size / 1024. / 1024. as Decimal(8,2)) > @warningDictionarySizeInMB OR
				entry_count > @warningEntryCount
			)
		) OR @showWarningsOnly = 0 )
		AND
		(( @showAllTextDictionaries = 1 
			AND
			case tp.name 
				when 'char' then 1
				when 'nchar' then 1
				when 'varchar' then 1
				when 'nvarchar' then 1
				when 'sysname' then 1
			end = 1
		) OR @showAllTextDictionaries = 0 )
		AND (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%') 
			OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id) = @tableName) )
		and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id ) like '%' + @schemaName + '%')
			OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id ) = @schemaName))
		AND (ISNULL(@objectId,part.object_id) = part.object_id)
		AND part.partition_number = case @partitionNumber when 0 then part.partition_number else @partitionNumber end
		and cols.name = isnull(@columnName,cols.name)
		and case dictionary_id when 0 then 'Global' else 'Local' end = isnull(@showDictionaryType, case dictionary_id when 0 then 'Global' else 'Local' end)
	order by QuoteName(schema_name(obj.schema_id)) + '.' + QuoteName(object_name(ind.object_id)), 
			ind.name, part.partition_number, csd.pdw_node_id, csd.distribution_id, csd.column_id;

