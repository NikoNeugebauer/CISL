/*
	Columnstore Indexes Scripts Library for Azure SQL DataWarehouse: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.5.0, January 2017

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
	- Tables with just 1 Row Group are shown that they can be improved. This will be corrected in the future version.*/

-- Params --
declare
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to 1 particular table
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@showPartitionStats bit = 1;					-- Allows to drill down fragmentation statistics on the partition level
-- end of --

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDW
if SERVERPROPERTY('EngineEdition') <> 6
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDW: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
set nocount on;

SELECT  quotename(schema_name(obj.schema_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		ind.name as 'IndexName',
		case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
		replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
		case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', 
		cast( Avg( (rg.deleted_rows * 1. / rg.total_rows) * 100 ) as Decimal(5,2)) as 'Fragmentation Perc.',
		sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) as 'Deleted RGs',
		cast( (sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) * 1. / count(*)) * 100 as Decimal(5,2)) as 'Deleted RGs Perc.',
		sum( case rg.total_rows when 1048576 then 0 else 1 end ) as 'Trimmed RGs',
		cast(sum( case rg.total_rows when 1048576 then 0 else 1 end ) * 1. / count(*) * 100 as Decimal(5,2)) as 'Trimmed Perc.',
		avg(rg.total_rows - rg.deleted_rows) as 'Avg Rows',
		sum(rg.total_rows) as [Total Rows],
		count(*) - ceiling( 1. * sum(rg.total_rows - rg.deleted_rows) / 1048576) as 'Optimisable RGs',
		cast((count(*) - ceiling( 1. * sum(rg.total_rows - rg.deleted_rows) / 1048576)) / count(*) * 100 as Decimal(8,2)) as 'Optimisable RGs Perc.',
		count(*) as 'Row Groups'
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
			inner join sys.pdw_nodes_partitions p
				on p.object_id = NTables.object_id
					and p.pdw_node_id = NI.pdw_node_id
					and p.distribution_id = NI.distribution_id
					and p.pdw_node_id = rg.pdw_node_id
					and p.distribution_id = rg.distribution_id
					and p.partition_number = rg.partition_number
	where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
		and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6.)
		and p.index_id in (1,2)
		and p.data_compression in (3,4)
		and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
		and (@schemaName is null or schema_name(rg.object_id) = @schemaName)
		and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
	group by p.object_id, ind.object_id, obj.schema_id, ind.data_space_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
	order by TableName, [Partition];	

