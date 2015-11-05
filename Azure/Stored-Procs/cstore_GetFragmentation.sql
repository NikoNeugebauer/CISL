/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.0.2, November 2015

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
		- Tables with just 1 Row Group are shown that they can be improved. This will be corrected in the future version.
*/

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
	Drop Procedure dbo.cstore_GetFragmentation;
GO

create procedure dbo.cstore_GetFragmentation (
-- Params --
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to 1 particular table
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@showPartitionStats bit = 1						-- Allows to drill down fragmentation statistics on the partition level
-- end of --
) as 
begin
	set nocount on;

	SELECT  object_schema_name(p.object_id) + '.' +	object_name(p.object_id) as 'TableName',
			ind.name as 'IndexName',
			replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
			case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', --p.partition_number as 'Partition',
			cast( Avg( (rg.deleted_rows * 1. / rg.total_rows) * 100 ) as Decimal(5,2)) as 'Fragmentation Perc.',
			sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) as 'Deleted RGs',
			cast( (sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) * 1. / count(*)) * 100 as Decimal(5,2)) as 'Deleted RGs Perc.',
			sum( case rg.total_rows when 1048576 then 0 else 1 end ) as 'Trimmed RGs',
			cast(sum( case rg.total_rows when 1048576 then 0 else 1 end ) * 1. / count(*) * 100 as Decimal(5,2)) as 'Trimmed Perc.',
			avg(rg.total_rows - rg.deleted_rows) as 'Avg Rows',
			count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576) as 'Optimisable RGs',
			cast((count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576)) / count(*) * 100 as Decimal(8,2)) as 'Optimisable RGs Perc.',
			count(*) as 'Row Groups'
		FROM sys.partitions AS p 
			INNER JOIN sys.column_store_row_groups rg
				ON p.object_id = rg.object_id
			INNER JOIN sys.indexes ind
				on rg.object_id = ind.object_id and rg.index_id = ind.index_id
		where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
			and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6. Note: There are no Deleted Bitmaps in NCCI in SQL 2012 & 2014)
			and p.index_id in (1,2)
			and rg.object_id = isnull(object_id(@tableName),rg.object_id)
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		group by p.object_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
		order by object_schema_name(p.object_id) + '.' + object_name(p.object_id);

end
