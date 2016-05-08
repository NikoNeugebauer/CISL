/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.2.0, May 2016

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
	- no support for Multi-Dimensional Segment Clustering in this version
	- no support for the Temporary Tables

Changes in 1.0.2
	+ Added schema information and quotes for the table name

Changes in 1.0.4
	+ Added new parameter for filtering on the schema - @schemaName

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.
	* Added support for DROP TABLE IF EXISTS construct for the temporary table inside the code
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2016
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server 2016. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetAlignment as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetAlignment(
-- Params --
	@schemaName nvarchar(256) = NULL,		-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular table
	@objectId int = NULL,					-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1,			-- Shows alignment statistics based on the partition
	@showUnsupportedSegments bit = 1,		-- Shows unsupported Segments in the result set
	@columnName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular column name
	@columnId int = NULL					-- Allows to filter one specific column Id
-- end of --
) as 
begin

	set nocount on;

	DROP TABLE IF EXISTS #column_store_segments

	SELECT part.object_id, part.partition_number, part.hobt_id, part.partition_id, seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
	INTO #column_store_segments
	FROM sys.column_store_segments seg
	INNER JOIN sys.partitions part
		   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
	WHERE part.object_id = isnull(object_id(@tableName),part.object_id)

	ALTER TABLE #column_store_segments
		ADD UNIQUE (hobt_id, partition_id, column_id, min_data_id, segment_id);

	ALTER TABLE #column_store_segments
		ADD UNIQUE (hobt_id, partition_id, column_id, max_data_id, segment_id);

	with cteSegmentAlignment as (
		select  part.object_id,  case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
				seg.partition_id, seg.column_id, cols.name as ColumnName, tp.name as ColumnType,
				seg.segment_id, 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS hasOverlappingSegment
			from sys.column_store_segments seg
				inner join sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join sys.columns cols
					on part.object_id = cols.object_id and seg.column_id = cols.column_id
				inner join sys.types tp
					on cols.system_type_id = tp.system_type_id and cols.user_type_id = tp.user_type_id
				outer apply (
					SELECT TOP 1 otherSeg.segment_id
					FROM #column_store_segments otherSeg WITH (FORCESEEK)
					WHERE seg.hobt_id = otherSeg.hobt_id 
							AND seg.partition_id = otherSeg.partition_id 
							AND seg.column_id = otherSeg.column_id
							AND seg.segment_id <> otherSeg.segment_id
							AND (seg.min_data_id < otherSeg.min_data_id and seg.max_data_id > otherSeg.min_data_id )  -- Scenario 1 
					UNION ALL
					SELECT TOP 1 otherSeg.segment_id
					FROM #column_store_segments otherSeg WITH (FORCESEEK)
					WHERE seg.hobt_id = otherSeg.hobt_id 
							AND seg.partition_id = otherSeg.partition_id 
							AND seg.column_id = otherSeg.column_id
							AND seg.segment_id <> otherSeg.segment_id
							AND (seg.min_data_id < otherSeg.max_data_id and seg.max_data_id > otherSeg.max_data_id )  -- Scenario 2 
				) filteredSeg
			where (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%')
				and (@schemaName is null or object_schema_name(part.object_id) = @schemaName)
				and part.object_id = isnull(@objectId, part.object_id)
			group by part.object_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id
	)
	select quotename(object_schema_name(object_id)) + '.' + quotename(object_name(object_id)) as TableName, partition_number as 'Partition', cte.column_id as 'Column Id', cte.ColumnName, 
		cte.ColumnType,
		case cte.ColumnType when 'numeric' then 'Segment Elimination is not supported' 
							when 'datetimeoffset' then 'Segment Elimination is not supported' 
							when 'char' then 'Segment Elimination is not supported' 
							when 'nchar' then 'Segment Elimination is not supported' 
							when 'varchar' then 'Segment Elimination is not supported' 
							when 'nvarchar' then 'Segment Elimination is not supported' 
							when 'sysname' then 'Segment Elimination is not supported' 
							when 'binary' then 'Segment Elimination is not supported' 
							when 'varbinary' then 'Segment Elimination is not supported' 
							when 'uniqueidentifier' then 'Segment Elimination is not supported' 
			else 'OK' end as 'Segment Elimination',
		sum(CONVERT(INT, hasOverlappingSegment)) as [Dealigned Segments],
		count(*) as [Total Segments],
		100 - cast( sum(CONVERT(INT, hasOverlappingSegment)) * 100.0 / (count(*)) as Decimal(6,2)) as [Segment Alignment %]
		from cteSegmentAlignment cte
		where ((@showUnsupportedSegments = 0 and cte.ColumnType not in ('numeric','datetimeoffset','char', 'nchar', 'varchar', 'nvarchar', 'sysname','binary','varbinary','uniqueidentifier'))
			  OR @showUnsupportedSegments = 1)
			  and cte.ColumnName = isnull(@columnName,cte.ColumnName)
			  and cte.column_id = isnull(@columnId,cte.column_id)
		group by quotename(object_schema_name(object_id)) + '.' + quotename(object_name(object_id)), partition_number, cte.column_id, cte.ColumnName, cte.ColumnType
		order by quotename(object_schema_name(object_id)) + '.' + quotename(object_name(object_id)), partition_number, cte.column_id;

end

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.2.0, May 2016

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
Changes in 1.0.1:
	+ Added information about Id of the column in the dictionary, for better debugging
	+ Added ordering by the columnId
	+ Added new parameter to filter Dictionaries by the type: @showDictionaryType
	+ Added quotes for displaying the name of any tables correctly
	
Changes in 1.0.3:
	+ Added information about maximum sizes for the Global & Local dictionaries	
	+ Added new parameter for enabling the details of all available dictionaries
	
Changes in 1.0.4
	+ Added new parameter for filtering on the schema - @schemaName		

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

-- Ensure that we are running SQL Server 2016
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server 2016. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetDictionaries as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetDictionaries(
-- Params --
	@showDetails bit = 1,								-- Enables showing the details of all Dictionaries
	@showWarningsOnly bit = 0,							-- Enables to filter out the dictionaries based on the Dictionary Size (@warningDictionarySizeInMB) and Entry Count (@warningEntryCount)
	@warningDictionarySizeInMB Decimal(8,2) = 6.,		-- The size of the dictionary, after which the dictionary should be selected. The value is in Megabytes 
	@warningEntryCount Int = 1000000,					-- Enables selecting of dictionaries with more than this number 
	@showAllTextDictionaries bit = 0,					-- Enables selecting all textual dictionaries indepentantly from their warning status
	@showDictionaryType nvarchar(52) = NULL,			-- Enables to filter out dictionaries by type with possible values 'Local', 'Global' or NULL for both 
	@objectId int = NULL,								-- Allows to idenitfy a table thorugh the ObjectId
	@schemaName nvarchar(256) = NULL,					-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,					-- Allows to show data filtered down to 1 particular table
	@columnName nvarchar(256) = NULL					-- Allows to filter out data base on 1 particular column name
-- end of --
) as 
begin
	set nocount on;

	declare @table_object_id int = NULL;

	if (@tableName is not NULL )
		set @table_object_id = isnull(object_id(@tableName),-1);
	else 
		set @table_object_id = NULL;

	SELECT QuoteName(object_schema_name(i.object_id)) + '.' + QuoteName(object_name(i.object_id)) as 'TableName', 
		p.partition_number as 'Partition',
		(select count(rg.row_group_id) from sys.column_store_row_groups rg
			where rg.object_id = i.object_id and rg.partition_number = p.partition_number
				  and rg.state = 3 ) as 'RowGroups',
		count(csd.column_id) as 'Dictionaries', 
		sum(csd.entry_count) as 'EntriesCount',
		min(p.rows) as 'Rows Serving',
		cast( SUM(csd.on_disk_size)/(1024.0*1024.0) as Decimal(8,3)) as 'Total Size in MB',
		cast( MAX(case dictionary_id when 0 then csd.on_disk_size else 0 end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Global Size in MB',
		cast( MAX(case dictionary_id when 0 then 0 else csd.on_disk_size end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Local Size in MB'
    FROM sys.indexes AS i
		inner join sys.partitions AS p
			on i.object_id = p.object_id 
		inner join sys.column_store_dictionaries AS csd
			on csd.hobt_id = p.hobt_id and csd.partition_id = p.partition_id
    where i.type in (5,6)
		and (@tableName is null or object_name (i.object_id) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(i.object_id) = @schemaName)
	group by object_schema_name(i.object_id) + '.' + object_name(i.object_id), i.object_id, p.partition_number
	union all
	SELECT QuoteName(object_schema_name(i.object_id,db_id('tempdb'))) + '.' + QuoteName(object_name(i.object_id,db_id('tempdb'))) as 'TableName', 
			p.partition_number as 'Partition',
			(select count(rg.row_group_id) from tempdb.sys.column_store_row_groups rg
				where rg.object_id = i.object_id and rg.partition_number = p.partition_number
					  and rg.state = 3 ) as 'RowGroups',
			count(csd.column_id) as 'Dictionaries', 
			sum(csd.entry_count) as 'EntriesCount',
			min(p.rows) as 'Rows Serving',
			cast( SUM(csd.on_disk_size)/(1024.0*1024.0) as Decimal(8,3)) as 'Total Size in MB',
			cast( MAX(case dictionary_id when 0 then csd.on_disk_size else 0 end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Global Size in MB',
			cast( MAX(case dictionary_id when 0 then 0 else csd.on_disk_size end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Local Size in MB'
		FROM tempdb.sys.indexes AS i
			inner join tempdb.sys.partitions AS p
				on i.object_id = p.object_id 
			inner join tempdb.sys.column_store_dictionaries AS csd
				on csd.hobt_id = p.hobt_id and csd.partition_id = p.partition_id
		where i.type in (5,6)
			and (@tableName is null or object_name (i.object_id,db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(i.object_id,db_id('tempdb')) = @schemaName)
		group by object_schema_name(i.object_id,db_id('tempdb')) + '.' + object_name(i.object_id,db_id('tempdb')), i.object_id, p.partition_number;


	if @showDetails = 1
	select QuoteName(object_schema_name(part.object_id)) + '.' + QuoteName(object_name(part.object_id)) as 'TableName',
			ind.name as 'IndexName', 
			part.partition_number as 'Partition',
			cols.name as ColumnName, 
			dict.column_id as ColumnId,
			dict.dictionary_id as 'SegmentId',
			tp.name as ColumnType,
			dict.column_id as 'ColumnId', 
			case dictionary_id when 0 then 'Global' else 'Local' end as 'Type', 
			part.rows as 'Rows Serving', 
			entry_count as 'Entry Count', 
			cast( on_disk_size / 1024. / 1024. as Decimal(8,2)) 'SizeInMb'
		from sys.column_store_dictionaries dict
			inner join sys.partitions part
				ON dict.hobt_id = part.hobt_id and dict.partition_id = part.partition_id
			inner join sys.indexes ind
				on part.object_id = ind.object_id and part.index_id = ind.index_id
			inner join sys.columns cols
				on part.object_id = cols.object_id and dict.column_id = cols.column_id
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
			and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
			and cols.name = isnull(@columnName,cols.name)
			and case dictionary_id when 0 then 'Global' else 'Local' end = isnull(@showDictionaryType, case dictionary_id when 0 then 'Global' else 'Local' end)
	union all
	select QuoteName(object_schema_name(part.object_id,db_id('tempdb'))) + '.' + QuoteName(object_name(part.object_id,db_id('tempdb'))) as 'TableName',
			ind.name as 'IndexName', 
			part.partition_number as 'Partition',
			cols.name as ColumnName, 
			dict.column_id as ColumnId,
			dict.dictionary_id as 'SegmentId',
			tp.name as ColumnType,
			dict.column_id as 'ColumnId', 
			case dictionary_id when 0 then 'Global' else 'Local' end as 'Type', 
			part.rows as 'Rows Serving', 
			entry_count as 'Entry Count', 
			cast( on_disk_size / 1024. / 1024. as Decimal(8,2)) 'SizeInMb'
		from tempdb.sys.column_store_dictionaries dict
			inner join tempdb.sys.partitions part
				ON dict.hobt_id = part.hobt_id and dict.partition_id = part.partition_id
			inner join tempdb.sys.indexes ind
				on part.object_id = ind.object_id and part.index_id = ind.index_id
			inner join tempdb.sys.columns cols
				on part.object_id = cols.object_id and dict.column_id = cols.column_id
			inner join tempdb.sys.types tp
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
			and (@tableName is null or object_name(ind.object_id,db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(ind.object_id,db_id('tempdb')) = @schemaName)
			and cols.name = isnull(@columnName,cols.name)
			and case dictionary_id when 0 then 'Global' else 'Local' end = isnull(@showDictionaryType, case dictionary_id when 0 then 'Global' else 'Local' end)
		order by TableName, ind.name, part.partition_number, dict.column_id;

end

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.2.0, May 2016

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

Changes in 1.0.3
	- Solved error with wrong partitioning information
	+ Added information on the total number of rows
	* Changed the format of the table returned in Result Set, now being returned with brackets []	

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)
*/

--------------------------------------------------------------------------------------------------------------------
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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetFragmentation as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetFragmentation (
-- Params --
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to 1 particular table
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1						-- Allows to drill down fragmentation statistics on the partition level
-- end of --
) as 
begin
	set nocount on;

	SELECT  quotename(object_schema_name(p.object_id)) + '.' + quotename(object_name(p.object_id)) as 'TableName',
			ind.name as 'IndexName',
			replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
			case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', --p.partition_number as 'Partition',
			cast( Avg( (rg.deleted_rows * 1. / rg.total_rows) * 100 ) as Decimal(5,2)) as 'Fragmentation Perc.',
			sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) as 'Deleted RGs',
			cast( (sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) * 1. / count(*)) * 100 as Decimal(5,2)) as 'Deleted RGs Perc.',
			sum( case rg.total_rows when 1048576 then 0 else 1 end ) as 'Trimmed RGs',
			cast(sum( case rg.total_rows when 1048576 then 0 else 1 end ) * 1. / count(*) * 100 as Decimal(5,2)) as 'Trimmed Perc.',
			avg(rg.total_rows - rg.deleted_rows) as 'Avg Rows',
			sum(rg.total_rows) as [Total Rows],
			count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576) as 'Optimisable RGs',
			cast((count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576)) / count(*) * 100 as Decimal(8,2)) as 'Optimisable RGs Perc.',
			count(*) as 'Row Groups'
		FROM sys.partitions AS p 
			INNER JOIN sys.column_store_row_groups rg
				ON p.object_id = rg.object_id and p.partition_number = rg.partition_number
			INNER JOIN sys.indexes ind
				on rg.object_id = ind.object_id and rg.index_id = ind.index_id
		where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
			and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6. Note: There are no Deleted Bitmaps in NCCI in SQL 2012 & 2014)
			and p.index_id in (1,2)
			and rg.object_id = isnull(@objectId,rg.object_id)
			and rg.object_id = isnull(object_id(@tableName),rg.object_id)
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		group by p.object_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
	union all
	SELECT  quotename(object_schema_name(p.object_id,db_id('tempdb'))) + '.' + quotename(object_name(p.object_id,db_id('tempdb'))) as 'TableName',
			ind.name as 'IndexName',
			replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
			case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', --p.partition_number as 'Partition',
			cast( Avg( (rg.deleted_rows * 1. / rg.total_rows) * 100 ) as Decimal(5,2)) as 'Fragmentation Perc.',
			sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) as 'Deleted RGs',
			cast( (sum (case rg.deleted_rows when rg.total_rows then 1 else 0 end ) * 1. / count(*)) * 100 as Decimal(5,2)) as 'Deleted RGs Perc.',
			sum( case rg.total_rows when 1048576 then 0 else 1 end ) as 'Trimmed RGs',
			cast(sum( case rg.total_rows when 1048576 then 0 else 1 end ) * 1. / count(*) * 100 as Decimal(5,2)) as 'Trimmed Perc.',
			avg(rg.total_rows - rg.deleted_rows) as 'Avg Rows',
			sum(rg.total_rows) as [Total Rows],
			count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576) as 'Optimisable RGs',
			cast((count(*) - ceiling(count(*) * 1. * avg(rg.total_rows - rg.deleted_rows) / 1048576)) / count(*) * 100 as Decimal(8,2)) as 'Optimisable RGs Perc.',
			count(*) as 'Row Groups'
		FROM tempdb.sys.partitions AS p 
			INNER JOIN tempdb.sys.column_store_row_groups rg
				ON p.object_id = rg.object_id and p.partition_number = rg.partition_number
			INNER JOIN tempdb.sys.indexes ind
				on rg.object_id = ind.object_id and rg.index_id = ind.index_id
		where rg.state in (2,3) -- 2 - Closed, 3 - Compressed	(Ignoring: 0 - Hidden, 1 - Open, 4 - Tombstone) 
			and ind.type in (5,6) -- Index Type (Clustered Columnstore = 5, Nonclustered Columnstore = 6. Note: There are no Deleted Bitmaps in NCCI in SQL 2012 & 2016)
			and p.index_id in (1,2)
			and rg.object_id = isnull(object_id(@tableName,db_id('tempdb')),rg.object_id)
			and (@schemaName is null or object_schema_name(rg.object_id,db_id('tempdb')) = @schemaName)
		group by p.object_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
		order by TableName;

end

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	MemoryInfo - Shows the content of the Columnstore Object Pool
	Version: 1.2.0, May 2016

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

-- Ensure that we are running SQL Server 2016
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server 2016. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetMemory' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetMemory as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	MemoryInfo - Shows the content of the Columnstore Object Pool
	Version: 1.2.0, May 2016
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
								where rg.object_id = mem.object_id) as Decimal(8,2)) as '% of Total',
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
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.2.0, May 2016

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

Changes in 1.0.3
	+ Added parameter for showing aggregated information on the whole table, instead of partitioned view as before
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Removed Tombstones from the calculations of Deleted Rows, Active Rows and Total Rows
	- Fixed bug with including aggregating tables without taking care of the database name, thus potentially including results from the equally named table from a different database	
	+ Included support for the temporary tables with Columnstore Indexes (global & local)
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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroups as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetRowGroups(
-- Params --
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
	@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
	@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionDetails bit = 1					-- Allows to show details of each of the available partitions
-- end of --
	) as
begin
	set nocount on;

	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case state when 1 then 1 else 0 end) as 'Open DS',
		sum(case state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case state when 4 then 1 else 0 end) as 'Tombstones',
		sum(case state when 3 then 1 else 0 end) as 'Compressed',
		count(*) as 'Total',
		cast( sum(isnull(case state when 4 then 0 else deleted_rows end,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) /1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(case state when 4 then 0 else total_rows end,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
		isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
		isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
		max(stat.last_user_scan) as 'LastScan'
		from sys.indexes ind
			left join sys.column_store_row_groups rg
				on ind.object_id = rg.object_id
			left join sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
			  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			  and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
			  and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
			  and ind.object_id = isnull(@objectId, ind.object_id)
			  and isnull(stat.database_id, db_id()) = db_id()			  
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end)--, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
	union all
	select quotename(object_schema_name(ind.object_id, db_id('tempdb'))) + '.' + quotename(object_name(ind.object_id, db_id('tempdb'))) as 'TableName', 
	case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
	(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
	case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case state when 1 then 1 else 0 end) as 'Open DS',
		sum(case state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case state when 4 then 1 else 0 end) as 'Tombstones',	
		sum(case state when 3 then 1 else 0 end) as 'Compressed',
		count(*) as 'Total',
	cast( sum(isnull(rg.deleted_rows,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
	cast( sum(isnull(rg.total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
	cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
	cast( sum(isnull(rg.size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
	isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
	isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
	max(stat.last_user_scan) as 'LastScan'
	from tempdb.sys.indexes ind
		left join tempdb.sys.column_store_row_groups rg
			on ind.object_id = rg.object_id
		left join tempdb.sys.partitions part with(READUNCOMMITTED)
			on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
		left join tempdb.sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
			on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
	where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
			and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
			and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
			and (@tableName is null or object_name (ind.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(ind.object_id, db_id('tempdb')) = @schemaName)
			and isnull(stat.database_id,db_id('tempdb')) = db_id('tempdb')
	group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) --, part.data_compression_desc
	having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
			and sum(isnull(total_rows,0)) >= @minTotalRows
	order by TableName,
			(case @showPartitionDetails when 1 then part.partition_number else 1 end);


end

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.2.0, May 2016

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

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)
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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroupsDetails as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetRowGroupsDetails(
-- Params --
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
	@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
	@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which size <> 1048576
	@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
	@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
	@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
	@maxSizeInMB Decimal(16,3) = NULL 				-- Maximum size in MB for a table to be included
-- end of --
	) as
BEGIN
	set nocount on;

	select quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)) as [Table Name],
		rg.partition_number,
		rg.row_group_id,
		rg.state,
		rg.state_description,
		rg.total_rows,
		rg.deleted_rows,
		cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]
		from sys.column_store_row_groups rg
		where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
			and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
			and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
			and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
			and rg.object_id = isnull(@objectId, rg.object_id)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
	union all
	select quotename(object_schema_name(rg.object_id, db_id('tempdb'))) + '.' + quotename(object_name(rg.object_id, db_id('tempdb'))) as [Table Name],
		rg.partition_number,
		rg.row_group_id,
		rg.state,
		rg.state_description,
		rg.total_rows,
		rg.deleted_rows,
		cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]
		from tempdb.sys.column_store_row_groups rg
		where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
			and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
			and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
			and (@tableName is null or object_name (rg.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id, db_id('tempdb')) = @schemaName)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		order by [Table Name], rg.partition_number, rg.row_group_id
END

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.2.0, May 2016

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
		- Custom non-standard (non-CU & non-SP) versions are not targeted yet
		- Duplicate Fixes & Improvements (CU12 for SP1 & CU2 for SP2, for example) are not eliminated from the list yet
*/

/*
Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published	
	+ Added information on CTP 3.1 & CTP 3.2

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Added information on CTP 3.3, RC0, RC1, RC2, RC3
*/

--------------------------------------------------------------------------------------------------------------------
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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetSQLInfo' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetSQLInfo as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetSQLInfo(
-- Params --
	@showUnrecognizedTraceFlags bit = 1,		-- Enables showing active trace flags, even if they are not columnstore indexes related
	@identifyCurrentVersion bit = 1,			-- Enables identification of the currently used SQL Server Instance version
	@showNewerVersions bit = 0					-- Enables showing the SQL Server versions that are posterior the current version-- end of --
) as 
begin
	declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
			@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));

	declare @SQLServerBuild smallint  = substring(@SQLServerVersion,CHARINDEX('.',@SQLServerVersion,5)+1,CHARINDEX('.',@SQLServerVersion,8)-CHARINDEX('.',@SQLServerVersion,5)-1);

	if OBJECT_ID('tempdb..#SQLColumnstoreImprovements', 'U') IS NOT NULL
		drop table #SQLColumnstoreImprovements;
	if OBJECT_ID('tempdb..#SQLBranches', 'U') IS NOT NULL
		drop table #SQLBranches;
	if OBJECT_ID('tempdb..#SQLVersions', 'U') IS NOT NULL
		drop table #SQLVersions;

	--  
	create table #SQLColumnstoreImprovements(
		BuildVersion smallint not null,
		SQLBranch char(3) not null,
		Description nvarchar(500) not null,
		URL nvarchar(1000)
	);

	create table #SQLBranches(
		SQLBranch char(3) not null Primary Key,
		MinVersion smallint not null );

	create table #SQLVersions(
		SQLBranch char(3) not null,
		SQLVersion smallint not null Primary Key,
		ReleaseDate datetime not null,	
		SQLVersionDescription nvarchar(100) );

	insert into #SQLBranches (SQLBranch, MinVersion)
		values ('CTP', 200 ), ( 'RC0', 1100 ), ( 'RC1', 1200 ), ( 'RC2', 1300 ), ( 'RC3', 1400 );

	insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
		values 
		( 'CTP', 200, convert(datetime,'27-05-2015',105), 'CTP 2 for SQL Server 2016' ),
		( 'CTP', 300, convert(datetime,'24-06-2015',105), 'CTP 2.1 for SQL Server 2016' ),
		( 'CTP', 400, convert(datetime,'23-07-2015',105), 'CTP 2.2 for SQL Server 2016' ),
		( 'CTP', 500, convert(datetime,'28-08-2015',105), 'CTP 2.3 for SQL Server 2016' ),
		( 'CTP', 600, convert(datetime,'30-09-2015',105), 'CTP 2.4 for SQL Server 2016' ),
		( 'CTP', 700, convert(datetime,'28-10-2015',105), 'CTP 3 for SQL Server 2016' ),
		( 'CTP', 800, convert(datetime,'30-11-2015',105), 'CTP 3.1 for SQL Server 2016' ),
		( 'CTP', 900, convert(datetime,'16-12-2015',105), 'CTP 3.2 for SQL Server 2016' ),
		( 'CTP', 1000, convert(datetime,'03-02-2016',105), 'CTP 3.3 for SQL Server 2016' ),
		( 'RC0', 1100, convert(datetime,'07-03-2016',105), 'RC 0 for SQL Server 2016' ),
		( 'RC1', 1200, convert(datetime,'16-03-2016',105), 'RC 1 for SQL Server 2016' ),
		( 'RC2', 1300, convert(datetime,'01-04-2016',105), 'RC 2 for SQL Server 2016' ),
		( 'RC3', 1400, convert(datetime,'15-04-2016',105), 'RC 3 for SQL Server 2016' );


	if @identifyCurrentVersion = 1
	begin
		if OBJECT_ID('tempdb..#TempVersionResults') IS NOT NULL
			drop table #TempVersionResults;

		create table #TempVersionResults(
			MessageText nvarchar(512) NOT NULL,		
			SQLVersionDescription nvarchar(200) NOT NULL,
			SQLBranch char(3) not null,
			SQLVersion smallint NULL );

		-- Identify the number of days that has passed since the installed release
		declare @daysSinceLastRelease int = NULL;
		select @daysSinceLastRelease = datediff(dd,max(ReleaseDate),getdate())
			from #SQLVersions
			where SQLBranch = ServerProperty('ProductLevel')
				and SQLVersion = cast(@SQLServerBuild as int);

		-- Get information about current SQL Server Version
		if( exists (select 1
						from #SQLVersions
						where SQLVersion = cast(@SQLServerBuild as int) ) )
			select 'You are Running:' as MessageText, SQLVersionDescription, SQLBranch, SQLVersion as BuildVersion, 'Your version is ' + cast(@daysSinceLastRelease as varchar(3)) + ' days old' as DaysSinceRelease
				from #SQLVersions
				where SQLVersion = cast(@SQLServerBuild as int);
		else
			select 'You are Running a Non RTM/SP/CU standard version:' as MessageText, '-' as SQLVersionDescription, 
				ServerProperty('ProductLevel') as SQLBranch, @SQLServerBuild as SQLVersion, 'Your version is ' + cast(@daysSinceLastRelease as varchar(3)) + ' days old' as DaysSinceRelease;
		

		-- Select information about all newer SQL Server versions that are known
		if @showNewerVersions = 1
		begin 
			insert into #TempVersionResults
				select 'Available Newer Versions:' as MessageText, '' as SQLVersionDescription, 
					'' as SQLBranch, NULL as BuildVersion
				UNION ALL
				select '' as MessageText, SQLVersionDescription as SQLVersionDescription, 
						SQLBranch as SQLVersionDescription, SQLVersion as BuildVersion
						from #SQLVersions
						where  @SQLServerBuild <  SQLVersion;

			select * 
				from #TempVersionResults;

			drop table #TempVersionResults;
		end 

	end

	select imps.BuildVersion, vers.SQLVersionDescription, imps.Description, imps.URL
		from #SQLColumnstoreImprovements imps
			inner join #SQLBranches branch
				on imps.SQLBranch = branch.SQLBranch
			inner join #SQLVersions vers
				on imps.BuildVersion = vers.SQLVersion
		where BuildVersion > @SQLServerBuild 
			and branch.SQLBranch = ServerProperty('ProductLevel')
			and branch.MinVersion < BuildVersion;

	drop table #SQLColumnstoreImprovements;
	drop table #SQLBranches;
	drop table #SQLVersions;

	--------------------------------------------------------------------------------------------------------------------
	-- Trace Flags part
	create table #ActiveTraceFlags(	
		TraceFlag nvarchar(20) not null,
		Status bit not null,
		Global bit not null,
		Session bit not null );

	insert into #ActiveTraceFlags
		exec sp_executesql N'DBCC TRACESTATUS()';

	create table #ColumnstoreTraceFlags(
		TraceFlag int not null,
		Description nvarchar(500) not null,
		URL nvarchar(600),
		SupportedStatus bit not null 
	);

	insert into #ColumnstoreTraceFlags (TraceFlag, Description, URL, SupportedStatus )
		values 
		(  634, 'Disables the background columnstore compression task.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		(  834, 'Enable Large Pages', 'https://support.microsoft.com/en-us/kb/920093?wa=wsignin1.0', 0 ),
		(  646, 'Gets text output messages that show what segments (row groups) were eliminated during query processing', 'http://social.technet.microsoft.com/wiki/contents/articles/5611.verifying-columnstore-segment-elimination.aspx', 1 ),
		( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2016/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 ),
		(10207, 'Skips Corrupted Columnstore Segments (Fixed in CU8 for SQL Server 2014 RTM and CU1 for SQL Server 2014 SP1)', 'https://support.microsoft.com/en-us/kb/3067257', 1 );

	select tf.TraceFlag, isnull(conf.Description,'Unrecognized') as Description, isnull(conf.URL,'-') as URL, SupportedStatus
		from #ActiveTraceFlags tf
			left join #ColumnstoreTraceFlags conf
				on conf.TraceFlag = tf.TraceFlag
		where @showUnrecognizedTraceFlags = 1 or (@showUnrecognizedTraceFlags = 0 AND Description is not null);

	drop table #ColumnstoreTraceFlags;
	drop table #ActiveTraceFlags;


end

GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.2.0, May 2016

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
	- @showTSQLCommandsBeta parameter is in alpha version and not pretending to be complete any time soon. This output is provided as a basic help & guide convertion to Columnstore Indexes.
	- CLR support is not included or tested
	- Output [Min RowGroups] is not taking present partitions into calculations yet :)
	- InMemory OLTP compatibility is not tested
	
Changes in 1.0.3
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.0.4
	- Bug fix for displaying the same primary key index twice in the T-SQL drop script

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed displaying wrong number of rows for the found suggested tables
	- Fixed error for filtering out the secondary nonclustered indexes in some bigger databases
	+ Included support for the temporary tables with Columnstore Indexes (global & local)
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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end
--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_SuggestedTables as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_SuggestedTables(
-- Params --
	@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
	@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
	@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
	@showTSQLCommandsBeta bit = 0,								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
	@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered'		-- Allows to define the type of Columnstore Index to be created eith possible values of 'Clustered' and 'Nonclustered'
-- end of --
) as 
begin
	set nocount on;

	declare 
		@readCommitedSnapshot tinyint = 0,
		@snapshotIsolation tinyint = 0;

	-- Verify Snapshot Isolation Level or Read Commited Snapshot 
	select @readCommitedSnapshot = is_read_committed_snapshot_on, 
		@snapshotIsolation = snapshot_isolation_state
		from sys.databases
		where database_id = DB_ID();

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	drop table IF EXISTS #TablesToColumnstore;

	create table #TablesToColumnstore(
		[ObjectId] int NOT NULL PRIMARY KEY,
		[TableName] nvarchar(1000) NOT NULL,
		[ShortTableName] nvarchar(256) NOT NULL,
		[Row Count] bigint NOT NULL,
		[Min RowGroups] smallint NOT NULL,
		[Size in GB] decimal(16,3) NOT NULL,
		[Cols Count] smallint NOT NULL,
		[String Cols] smallint NOT NULL,
		[Sum Length] int NOT NULL,
		[Unsupported] smallint NOT NULL,
		[LOBs] smallint NOT NULL,
		[Computed] smallint NOT NULL,
		[Clustered Index] tinyint NOT NULL,
		[Nonclustered Indexes] smallint NOT NULL,
		[XML Indexes] smallint NOT NULL,
		[Spatial Indexes] smallint NOT NULL,
		[Primary Key] tinyint NOT NULL,
		[Foreign Keys] smallint NOT NULL,
		[Unique Constraints] smallint NOT NULL,
		[Triggers] smallint NOT NULL,
		[RCSI] tinyint NOT NULL,
		[Snapshot] tinyint NOT NULL,
		[CDC] tinyint NOT NULL,
		[CT] tinyint NOT NULL,
		[InMemoryOLTP] tinyint NOT NULL,
		[Replication] tinyint NOT NULL,
		[FileStream] tinyint NOT NULL,
		[FileTable] tinyint NOT NULL
	);

	insert into #TablesToColumnstore
	select t.object_id as [ObjectId]
		, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id)) as 'TableName'
		, replace(object_name(t.object_id),' ', '') as 'ShortTableName'
		, max(p.rows) as 'Row Count'
		, ceiling(max(p.rows)/1045678.) as 'Min RowGroups' 
		, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
		, (select count(*) from sys.columns as col
			where t.object_id = col.object_id ) as 'Cols Count'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
		   ) as 'String Cols'
		, (select sum(col.max_length) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ) as 'Sum Length'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
					  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as 'Unsupported'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as 'LOBs'
		, (select count(*) 
				from sys.columns as col
				where is_computed = 1 ) as 'Computed'
		, (select count(*)
				from sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
		, (select count(*)
				from sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
		, (select count(*)
				from sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
		, (select count(*)
				from sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
		, (select count(*)
				from sys.objects
				where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
		, (select count(*)
				from sys.objects
				where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
		, (select count(*)
				from sys.objects
				where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
		, (select count(*)
				from sys.objects
				where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
		, @readCommitedSnapshot as 'RCSI'
		, @snapshotIsolation as 'Snapshot'
		, t.is_tracked_by_cdc as 'CDC'
		, (select count(*) 
				from sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
		, t.is_memory_optimized as 'InMemoryOLTP'
		, t.is_replicated as 'Replication'
		, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
		, t.is_filetable as 'FileTable'
		from sys.tables t
			inner join sys.partitions as p 
				ON t.object_id = p.object_id
			inner join sys.allocation_units as a 
				ON p.partition_id = a.container_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%')
			 and (@schemaName is null or object_schema_name( t.object_id ) = @schemaName)
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from sys.columns as col
							inner join sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					--and (select count(*)
					--		from sys.objects so
					--		where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
					--and (select count(*)
					--		from sys.indexes ind
					--		where t.object_id = ind.object_id
					--			and ind.type in (3,4) ) = 0
					--and t.is_memory_optimized = 0
					and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) > @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from sys.columns as col
						inner join sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(sum(a.total_pages) * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)
	union all
	select t.object_id as [ObjectId]
	, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id)) as 'TableName'
	, replace(object_name(t.object_id),' ', '') as 'ShortTableName'
	, max(p.rows) as 'Row Count'
	, ceiling(max(p.rows)/1045678.) as 'Min RowGroups' 
	, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
	, (select count(*) from sys.columns as col
		where t.object_id = col.object_id ) as 'Cols Count'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
	   ) as 'String Cols'
	, isnull((select sum(col.max_length) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id 
	  ),0) as 'Sum Length'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
				  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
				 )
	   ) as 'Unsupported'
	, (select count(*) 
			from sys.columns as col
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id
			where t.object_id = col.object_id and 
				 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
	   ) as 'LOBs'
    , (select count(*) 
			from sys.columns as col
			where is_computed = 1 ) as 'Computed'
	, (select count(*)
			from sys.indexes ind
			where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
	, (select count(*)
			from sys.indexes ind
			where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
	, (select count(*)
			from sys.indexes ind
			where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
	, (select count(*)
			from sys.indexes ind
			where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
	, (select count(*)
			from sys.objects
			where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
	, (select count(*)
			from sys.objects
			where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
	, (select count(*)
			from sys.objects
			where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
	, (select count(*)
			from sys.objects
			where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
    , @readCommitedSnapshot as 'RCSI'
	, @snapshotIsolation as 'Snapshot'
	, t.is_tracked_by_cdc as 'CDC'
	, (select count(*) 
			from sys.change_tracking_tables ctt with(READUNCOMMITTED)
			where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
				  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
	, t.is_memory_optimized as 'InMemoryOLTP'
	, t.is_replicated as 'Replication'
	, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
	, t.is_filetable as 'FileTable'
	from tempdb.sys.tables t
		left join tempdb.sys.partitions as p 
			ON t.object_id = p.object_id
		left join tempdb.sys.allocation_units as a 
			ON p.partition_id = a.container_id
	where p.data_compression in (0,1,2) -- None, Row, Page
		 and (select count(*)
				from sys.indexes ind
				where t.object_id = ind.object_id
					and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
		 and (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%')
		 and (@schemaName is null or object_schema_name( t.object_id ) = @schemaName)
		 and (( @showReadyTablesOnly = 1 
				and  
				(select count(*) 
					from sys.columns as col
						inner join sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id and 
							(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
					) = 0 
				--and (select count(*)
				--		from sys.objects so
				--		where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
				--and (select count(*)
				--		from sys.indexes ind
				--		where t.object_id = ind.object_id
				--			and ind.type in (3,4) ) = 0
				--and t.is_memory_optimized = 0
				and t.is_replicated = 0
				and coalesce(t.filestream_data_space_id,0,1) = 0
				and t.is_filetable = 0
			  )
			 or @showReadyTablesOnly = 0)
	group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
	having sum(p.rows) > @minRowsToConsider 
			and
			(((select sum(col.max_length) 
				from sys.columns as col
					inner join sys.types as tp
						on col.system_type_id = tp.system_type_id
				where t.object_id = col.object_id 
			  ) < 8000 and @considerColumnsOver8K = 0 ) 
			  OR
			 @considerColumnsOver8K = 1 )
			and 
			(sum(a.total_pages) * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)
	
		--order by max(p.rows) desc, sum(a.total_pages) desc;

	-- Show the found results
	select case when ([Triggers] + [Replication] + [FileStream] + [FileTable] + [Unsupported] - ([LOBs] + [Computed])) > 0 then 'None' 
				when ([Clustered Index] + [CDC] + [CT] +
					  [Unique Constraints] + [Triggers] + [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) = 0 and [Unsupported] = 0 then 'Both Columnstores' 
				when ( [Triggers] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 then 'Nonclustered Columnstore'  
		   end as 'Compatible With'
		, [TableName], [Row Count], [Min RowGroups], [Size in GB], [Cols Count], [String Cols], [Sum Length], [Unsupported], [LOBs], [Computed]
		, [Clustered Index], [Nonclustered Indexes], [XML Indexes], [Spatial Indexes], [Primary Key], [Foreign Keys], [Unique Constraints]
		, [Triggers], [RCSI], [Snapshot], [CDC], [CT], [InMemoryOLTP], [Replication], [FileStream], [FileTable]
		from #TablesToColumnstore
		order by [Row Count] desc;

	if( @showUnsupportedColumnsDetails = 1 ) 
	begin
		select quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name (t.object_id)) as 'TableName',
			col.name as 'Unsupported Column Name',
			tp.name as 'Data Type',
			col.max_length as 'Max Length',
			col.precision as 'Precision',
			col.is_computed as 'Computed'
			from sys.tables t
				inner join sys.columns as col
					on t.object_id = col.object_id 
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id 
				where  ((UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
						(UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
						) 
						OR col.is_computed = 1 )
				 and t.object_id in (select ObjectId from #TablesToColumnstore);
	end

	if( @showTSQLCommandsBeta = 1 ) 
	begin
		select coms.TableName, coms.[TSQL Command], coms.[type] 
			from (
				select t.TableName, 
						'create ' + @columnstoreIndexTypeForTSQL + ' columnstore index ' + 
						case @columnstoreIndexTypeForTSQL when 'Clustered' then 'CCI' when 'Nonclustered' then 'NCCI' end 
						+ '_' + t.[ShortTableName] + 
						' on ' + t.TableName + case @columnstoreIndexTypeForTSQL when 'Nonclustered' then '()' else '' end + ';' as [TSQL Command]
					   , 'CCL' as type,
					   101 as [Sort Order]
					from #TablesToColumnstore t
				union all
				select t.TableName, 'alter table ' + t.TableName + ' drop constraint ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], [type], 
					   case UPPER(type) when 'PK' then 100 when 'F' then 1 when 'UQ' then 100 end as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('PK','F','UQ')
				union all
				select t.TableName, 'drop trigger ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('TR')
				union all
				select t.TableName, 'drop assembly ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ' WITH NO DEPENDENTS ;' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('TA')	
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'CL' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 1 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in ('PK','F','UQ')
								and quotename(ind.name) <> quotename(so1.name))
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'NC' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 2 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in ('PK','F','UQ')
								and quotename(ind.name) <> quotename(so1.name) and t.ObjectId = t1.ObjectId )
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'XML' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 3
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'SPAT' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 4
				union all 
				select t.TableName, '-- - - - - - - - - - - - - - - - - - - - - -' as [TSQL Command], '---' as type,
					0 as [Sort Order]
					from #TablesToColumnstore t
			) coms
		order by coms.type desc, coms.[Sort Order]; --coms.TableName 
			 
	end

	drop table #TablesToColumnstore; 
end

GO
