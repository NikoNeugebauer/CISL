/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.4.2, December 2016

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
	- Tables with just 1 Row Group are shown that they can be improved. This will be corrected in the future version.

Changes in 1.0.3
	- Solved error with wrong partitioning information
	+ Added information on the total number of rows
	* Changed the format of the table returned in Result Set, now being returned with brackets []

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the InMemory Columnstore Index
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Added a couple of bug fixes for the Azure SQLDatabase changes related to Temp Tables
	- Fixed a bug for the trimmed row groups with just 1 row giving wrong information about a potential optimizable row group

Changes in 1.3.1
	- Fixed wrong behaviour for the @tableName parameter
	- Fixed bug reporting wrong data on the Clustered Tables with Nonclustered Columnstore Index
	- Added support for Databases with collations different to TempDB
*/

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

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
set nocount on;

SELECT  quotename(object_schema_name(p.object_id)) + '.' + quotename(object_name(p.object_id)) as 'TableName',
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
	FROM sys.partitions AS p 
		INNER JOIN sys.column_store_row_groups rg
			ON p.object_id = rg.object_id and p.partition_number = rg.partition_number
		INNER JOIN sys.indexes ind
			on rg.object_id = ind.object_id and rg.index_id = ind.index_id
	where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
		and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6. Note: There are no Deleted Bitmaps in NCCI in SQL 2012 & 2014)
		and p.index_id in (1,2)
		and p.data_compression in (3,4)
		and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
	group by p.object_id, ind.data_space_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
union all
SELECT  quotename(isnull(object_schema_name(obj.object_id, db_id('tempdb')),'dbo')) + '.' + quotename(obj.name) as 'TableName',
		ind.name COLLATE DATABASE_DEFAULT as 'IndexName',
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
	FROM tempdb.sys.partitions AS p 
		inner join tempdb.sys.objects obj
			on p.object_id = obj.object_id
		INNER JOIN tempdb.sys.column_store_row_groups rg
			ON p.object_id = rg.object_id and p.partition_number = rg.partition_number
		INNER JOIN tempdb.sys.indexes ind
			on rg.object_id = ind.object_id and rg.index_id = ind.index_id
	where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
		and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6. Note: There are no Deleted Bitmaps in NCCI in SQL 2012 & 2014)
		and p.index_id in (1,2)
		and p.data_compression in (3,4)
		and (@tableName is null or object_name (rg.object_id,db_id('tempdb')) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(rg.object_id,db_id('tempdb')) = @schemaName)
		and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
	group by p.object_id, ind.name, obj.object_id, obj.name, ind.data_space_id, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
	order by TableName;	

