/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.3.0, July 2016

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

Changes in 1.0.2
	+ Added schema information and quotes for the table name

Changes in 1.0.4
	+ Added new parameter for filtering on the schema - @schemaName

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed bug with non-functioning @objectId parameter
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
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
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetAlignment as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.3.0, July 2016
*/
alter procedure dbo.cstore_GetAlignment(
-- Params --
	@schemaName nvarchar(256) = NULL,		-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular table
	@indexLocation varchar(15) = NULL,		-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectId int = NULL,					-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1,			-- Shows alignment statistics based on the partition
	@showUnsupportedSegments bit = 1,		-- Shows unsupported Segments in the result set
	@columnName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular column name
	@columnId int = NULL					-- Allows to filter one specific column Id
-- end of --
) as 
begin
	set nocount on;

	IF OBJECT_ID('tempdb..#column_store_segments', 'U') IS NOT NULL
		DROP TABLE #column_store_segments

	SELECT SchemaName, TableName, object_id, partition_number, hobt_id, partition_id, column_id, segment_id, min_data_id, max_data_id
	INTO #column_store_segments
	FROM ( select object_schema_name(part.object_id) as SchemaName, object_name(part.object_id) as TableName, part.object_id, part.partition_number, part.hobt_id, part.partition_id, seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM sys.column_store_segments seg
				INNER JOIN sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
		union all
		select object_schema_name(part.object_id,db_id('tempdb')) as SchemaName, object_name(part.object_id,db_id('tempdb')) as TableName, part.object_id, part.partition_number, part.hobt_id, part.partition_id, seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM tempdb.sys.column_store_segments seg
				INNER JOIN tempdb.sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
		) as Res

	ALTER TABLE #column_store_segments
	ADD UNIQUE (hobt_id, partition_id, column_id, min_data_id, segment_id);

	ALTER TABLE #column_store_segments
	ADD UNIQUE (hobt_id, partition_id, column_id, max_data_id, segment_id);

	with cteSegmentAlignment as (
		select  part.object_id,  
				quotename(object_schema_name(part.object_id)) + '.' + quotename(object_name(part.object_id)) as TableName,
				case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
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
				and (@objectId is null or part.object_id = @objectId)
				and 1 = case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else 1 end
			group by part.object_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id
		UNION ALL
		select  part.object_id,  
				quotename(object_schema_name(part.object_id,db_id('tempdb'))) + '.' + quotename(object_name(part.object_id,db_id('tempdb'))) as TableName,
				case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
				seg.partition_id, seg.column_id, cols.name as ColumnName, tp.name as ColumnType,
				seg.segment_id, 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS hasOverlappingSegment
			from tempdb.sys.column_store_segments seg
				inner join tempdb.sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join tempdb.sys.columns cols
					on part.object_id = cols.object_id and seg.column_id = cols.column_id
				inner join tempdb.sys.types tp
					on cols.system_type_id = tp.system_type_id and cols.user_type_id = tp.user_type_id
				outer apply (
					SELECT TOP 1 otherSeg.segment_id
					FROM #column_store_segments otherSeg --WITH (FORCESEEK)
					WHERE seg.hobt_id = otherSeg.hobt_id 
							AND seg.partition_id = otherSeg.partition_id 
							AND seg.column_id = otherSeg.column_id
							AND seg.segment_id <> otherSeg.segment_id
							AND (seg.min_data_id < otherSeg.min_data_id and seg.max_data_id > otherSeg.min_data_id )  -- Scenario 1 
					UNION ALL
					SELECT TOP 1 otherSeg.segment_id
					FROM #column_store_segments otherSeg --WITH (FORCESEEK)
					WHERE seg.hobt_id = otherSeg.hobt_id 
							AND seg.partition_id = otherSeg.partition_id 
							AND seg.column_id = otherSeg.column_id
							AND seg.segment_id <> otherSeg.segment_id
							AND (seg.min_data_id < otherSeg.max_data_id and seg.max_data_id > otherSeg.max_data_id )  -- Scenario 2 
				) filteredSeg
			where (@tableName is null or object_name (part.object_id,db_id('tempdb')) like '%' + @tableName + '%')
				and (@schemaName is null or object_schema_name(part.object_id,db_id('tempdb')) = @schemaName)
				and (@objectId is null or part.object_id = @objectId)	
				and 1 = case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else 1 end
			group by part.object_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id

	)
	select TableName, 'Disk-Based' as Location, partition_number as 'Partition', cte.column_id as 'Column Id', cte.ColumnName, 
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
		group by TableName, partition_number, cte.column_id, cte.ColumnName, cte.ColumnType
		order by TableName, partition_number, cte.column_id;

end
GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.3.0, July 2016

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

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	* Removed Duplicate information on the ColumnId
	* Changed the title of the return information for the column from the SegmentId to the DictionaryId
	+ Added information on the Index Location (In-Memory or Disk-Based) and the respective filter
	+ Added information on the type of the Index (Clustered or Nonclustered) and the respective filter
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetDictionaries as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.3.0, July 2016
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
	@columnName nvarchar(256) = NULL,					-- Allows to filter out data base on 1 particular column name
	@indexLocation varchar(15) = NULL,					-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@indexType char(2) = NULL							-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)

-- end of --
) as 
begin
	set nocount on;

	SELECT QuoteName(object_schema_name(i.object_id)) + '.' + QuoteName(object_name(i.object_id)) as 'TableName', 
			case i.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
			'Disk-Based' as [Location],	
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
			and i.object_id = isnull(@objectId, i.object_id)
			and (@tableName is null or object_name (i.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(i.object_id) = @schemaName)
			and i.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else i.data_space_id end, i.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else i.type end = i.type
		group by object_schema_name(i.object_id) + '.' + object_name(i.object_id), i.object_id, i.type, p.partition_number
	union all
	SELECT QuoteName(object_schema_name(i.object_id,db_id('tempdb'))) + '.' + QuoteName(object_name(i.object_id,db_id('tempdb'))) as 'TableName', 
			case i.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
			'Disk-Based' as [Location],	
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
			and i.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else i.data_space_id end, i.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else i.type end = i.type
		group by object_schema_name(i.object_id,db_id('tempdb')) + '.' + object_name(i.object_id,db_id('tempdb')), i.object_id, i.type, p.partition_number;


	if @showDetails = 1
	SELECT QuoteName(object_schema_name(part.object_id)) + '.' + QuoteName(object_name(part.object_id)) as 'TableName',
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
			and ind.object_id = isnull(@objectId, ind.object_id)
			and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
			and cols.name = isnull(@columnName,cols.name)
			and case dictionary_id when 0 then 'Global' else 'Local' end = isnull(@showDictionaryType, case dictionary_id when 0 then 'Global' else 'Local' end)
			and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
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
			and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
		order by TableName, ind.name, part.partition_number, dict.column_id;



end
GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.3.0, July 2016

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

Changes in 1.3.0
	+ Added support for the Index Location (Disk-Based)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed a bug for the trimmed row groups with just 1 row giving wrong information about a potential optimizable row group
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetFragmentation as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.3.0, July 2016
*/
alter procedure dbo.cstore_GetFragmentation (
-- Params --
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to 1 particular table
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1						-- Allows to drill down fragmentation statistics on the partition level
-- end of --
) as 
begin
	set nocount on;

	SELECT  quotename(object_schema_name(p.object_id)) + '.' + quotename(object_name(p.object_id)) as 'TableName',
			ind.name as 'IndexName',
			case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',		
			replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
			case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', --p.partition_number as 'Partition',
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
			and rg.object_id = isnull(object_id(@tableName),rg.object_id)
			and rg.object_id = isnull(@objectId,rg.object_id) 
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		group by p.object_id, ind.data_space_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
	union all
	SELECT  quotename(isnull(object_schema_name(obj.object_id, db_id('tempdb')),'dbo')) + '.' + quotename(obj.name) as 'TableName',
			ind.name as 'IndexName',
			case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',					
			replace(ind.type_desc,' COLUMNSTORE','') as 'IndexType',
			case @showPartitionStats when 1 then p.partition_number else 1 end as 'Partition', --p.partition_number as 'Partition',
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
			and rg.object_id = isnull(object_id(@tableName,db_id('tempdb')),rg.object_id)
			and (@schemaName is null or object_schema_name(rg.object_id,db_id('tempdb')) = @schemaName)
			and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
		group by p.object_id, ind.data_space_id, obj.object_id, obj.name, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
		order by TableName;

end
GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	MemoryInfo - Shows the content of the Columnstore Object Pool
	Version: 1.3.0, July 2016

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
	Version: 1.3.0, July 2016
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
							and rg.state = 3 ) as Decimal(8,2)) as '% of Total Column Structures',
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
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.3.0, July 2016

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
	- View Permission State is required to run this stored procedure.

Changes in 1.0.3
	+ Added parameter for showing aggregated information on the whole table, instead of partitioned view as before
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed bug with showing 1 row group for an empty Columnstore (now showing correctly 0 row groups)
	- Fixed bugs for filtering by schema & name of the columnstore table (it is now using the sys.indexes DMV as the base, thus guaranteeing correct results for the empty Columnstore)	
	- Fixed bug with including aggregating tables without taking care of the database name, thus potentially including results from the equally named table from a different database
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added new parameter for filtering a specific partition
	+ Added new column for the Index Location (Disk-Based)
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

	--Ensure that we are running SQL Server 2014
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroups as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.3.0, July 2016
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
	@showPartitionDetails bit = 0,					-- Allows to show details of each of the available partitions
	@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --
	) as
begin
	set nocount on;

	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		'Disk-Based' as Location,
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case rg.state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case rg.state when 1 then 1 else 0 end) as 'Open DS',
		sum(case rg.state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case rg.state when 3 then 1 else 0 end) as 'Compressed',
		count(rg.row_group_id) as 'Total',
		cast( sum(isnull(rg.deleted_rows,0))/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
		cast( sum(isnull(rg.total_rows-isnull(deleted_rows,0),0))/1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( sum(isnull(rg.size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) as 'Size in GB',
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
			  and (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%')
			  and (@schemaName is null or object_schema_name(ind.object_id) = @schemaName)
			  and ind.object_id = isnull(@objectId, ind.object_id)
			  and isnull(stat.database_id,db_id()) = db_id()
			  and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) --, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
	union all
	select quotename(object_schema_name(ind.object_id, db_id('tempdb'))) + '.' + quotename(object_name(ind.object_id, db_id('tempdb'))) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		'Disk-Based' as Location,
		(case @showPartitionDetails when 1 then part.partition_number else 1 end) as 'Partition',
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
		sum(case rg.state when 0 then 1 else 0 end) as 'Bulk Load RG',
		sum(case rg.state when 1 then 1 else 0 end) as 'Open DS',
		sum(case rg.state when 2 then 1 else 0 end) as 'Closed DS',
		sum(case rg.state when 3 then 1 else 0 end) as 'Compressed',
		count(rg.row_group_id) as 'Total',
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
			  and ind.object_id = isnull(@objectId, ind.object_id)
			  and isnull(stat.database_id,db_id('tempdb')) = db_id('tempdb')
			  and part.partition_number = isnull(@partitionId, part.partition_number)  -- Partition Filtering
		group by ind.object_id, ind.type, (case @showPartitionDetails when 1 then part.partition_number else 1 end) --, part.data_compression_desc
		having cast( sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
		order by TableName,
				(case @showPartitionDetails when 1 then part.partition_number else 1 end);

end
GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.3.0, July 2016

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

Modifications:

Changes in 1.1.0
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added compatibility support for the SQL Server 2016 internals information on Row Group Trimming, Build Process, Vertipaq Optimisations, Sequential Generation Id, Closed DateTime & Creation DateTime
	+ Added 2 new compatibility parameters for filtering out the Min & Max Creation DateTimes
	- Fixed error for the temporary tables support
	* Changed the name of the second result column from partition_number to partition
*/


declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

--Ensure that we are running SQL Server 2014
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroupsDetails as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.3.0, July 2016
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
	@maxSizeInMB Decimal(16,3) = NULL, 				-- Maximum size in MB for a table to be included
	@minCreatedDateTime Datetime = NULL,			-- The earliest create datetime for Row Group to be included
	@maxCreatedDateTime Datetime = NULL				-- The lateste create datetime for Row Group to be included
-- end of --
) as
BEGIN
	set nocount on;

	select quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)) as [Table Name],
		'Disk-Based' as [Location],
		rg.partition_number as partition,
		rg.row_group_id,
		rg.state,
		rg.state_description,
		rg.total_rows,
		rg.deleted_rows,
		cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
		NULL as trim_reason,
		NULL as trim_reason_desc,
		NULL compress_op, 
		NULL as compress_op_desc,
		NULL as optimised,
		NULL as generation,
		NULL as closed_time,	
		ind.create_date as created_time
		from sys.column_store_row_groups rg
			inner join sys.objects ind
				on rg.object_id = ind.object_id 
		where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
			and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
			and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
			and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
			and rg.object_id = isnull(@objectId, rg.object_id)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
			and ind.create_date between isnull(@minCreatedDateTime,ind.create_date) and isnull(@maxCreatedDateTime,ind.create_date)
	UNION ALL
	select quotename(object_schema_name(rg.object_id, db_id('tempdb'))) + '.' + quotename(object_name(rg.object_id, db_id('tempdb'))) as [Table Name],
		'Disk-Based' as [Location],	
		rg.partition_number as partition,
		rg.row_group_id,
		rg.state,
		rg.state_description,
		rg.total_rows,
		rg.deleted_rows,
		cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
		NULL as trim_reason,
		NULL as trim_reason_desc,
		NULL compress_op, 
		NULL as compress_op_desc,
		NULL as optimised,
		NULL as generation,
		NULL as closed_time,	
		ind.create_date as created_time
		from tempdb.sys.column_store_row_groups rg
			inner join tempdb.sys.objects ind
				on rg.object_id = ind.object_id 
		where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
			and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
			and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
			and (@tableName is null or object_name (rg.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(rg.object_id, db_id('tempdb')) = @schemaName)
			and rg.object_id = isnull(@objectId, rg.object_id)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
			and ind.create_date between isnull(@minCreatedDateTime,ind.create_date) and isnull(@maxCreatedDateTime,ind.create_date)
		order by [Table Name], rg.partition_number, rg.row_group_id;
END
GO
/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.3.0, July 2016

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
Changes in 1.0.1
	+ Added drops for the existing temp tables: #SQLColumnstoreImprovements, #SQLBranches, #SQLVersions
	+ Added new parameter for Enables showing the SQL Server versions that are posterior the current version
	* Added more source code description in the comments
	+ Removed some redundant information (column UpdateName from the #SQLColumnstoreImprovements) which were left from the very early versions
	- Fixed erroneous build version for the SQL Server 2014 SP2 CU2

Changes in 1.0.2
	+ Added information about CU 3 for SQL Server 2014 SP1 and CU 10 for SQL Server 2014 RTM
	+ Added column with the CU Version for the Bugfixes output
	- Fixed bug with the wrong CU9 Version 
	* Updated temporary tables in order to avoid error messages

Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published
	+ Added information about CU 4 for SQL Server 2014 SP1 and CU 11 for SQL Server 2014 RTM

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Added Information about CU 5 & CU 6 for SQL Server 2014 SP1 & about CU 12 & CU 13 for SQL Server 2014 RTM

Changes in 1.3.0
	+ Added Information about updated CU 6A, CU 7 for SQL Server 2014 SP1 & CU 14 for SQL Server 2014 RTM
	+ Added Information about SQL Server 2014 SP2
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetSQLInfo' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetSQLInfo as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.3.0, July 2016
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

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
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
		values ('RTM', 2000 ), ('SP1', 4100), ('SP2', 5000) ;

	insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
		values 
		( 'RTM', 2000, convert(datetime,'01-04-2014',105), 'SQL Server 2014 RTM' ),
		( 'RTM', 2342, convert(datetime,'21-04-2014',105), 'CU 1 for SQL Server 2014 RTM' ),
		( 'RTM', 2370, convert(datetime,'27-06-2014',105), 'CU 2 for SQL Server 2014 RTM' ),
		( 'RTM', 2402, convert(datetime,'18-08-2014',105), 'CU 3 for SQL Server 2014 RTM' ),
		( 'RTM', 2430, convert(datetime,'21-10-2014',105), 'CU 4 for SQL Server 2014 RTM' ),
		( 'RTM', 2456, convert(datetime,'18-12-2014',105), 'CU 5 for SQL Server 2014 RTM' ),
		( 'RTM', 2480, convert(datetime,'16-02-2015',105), 'CU 6 for SQL Server 2014 RTM' ),
		( 'RTM', 2495, convert(datetime,'23-04-2015',105), 'CU 7 for SQL Server 2014 RTM' ),
		( 'RTM', 2546, convert(datetime,'22-06-2015',105), 'CU 8 for SQL Server 2014 RTM' ),
		( 'RTM', 2553, convert(datetime,'17-08-2015',105), 'CU 9 for SQL Server 2014 RTM' ),
		( 'RTM', 2556, convert(datetime,'20-10-2015',105), 'CU 10 for SQL Server 2014 RTM' ),
		( 'RTM', 2560, convert(datetime,'22-12-2015',105), 'CU 11 for SQL Server 2014 RTM' ),
		( 'RTM', 2564, convert(datetime,'22-02-2016',105), 'CU 12 for SQL Server 2014 RTM' ),
		( 'RTM', 2568, convert(datetime,'19-04-2016',105), 'CU 13 for SQL Server 2014 RTM' ),
		( 'RTM', 2569, convert(datetime,'20-06-2016',105), 'CU 14 for SQL Server 2014 RTM' ),
		( 'SP1', 4100, convert(datetime,'14-05-2015',105), 'SQL Server 2014 SP1' ),
		( 'SP1', 4416, convert(datetime,'22-06-2015',105), 'CU 1 for SQL Server 2014 SP1' ),
		( 'SP1', 4422, convert(datetime,'17-08-2015',105), 'CU 2 for SQL Server 2014 SP1' ),
		( 'SP1', 4427, convert(datetime,'21-10-2015',105), 'CU 3 for SQL Server 2014 SP1' ),
		( 'SP1', 4436, convert(datetime,'22-12-2015',105), 'CU 4 for SQL Server 2014 SP1' ),
		( 'SP1', 4439, convert(datetime,'22-02-2016',105), 'CU 5 for SQL Server 2014 SP1' ),
		( 'SP1', 4449, convert(datetime,'19-04-2016',105), 'CU 6 for SQL Server 2014 SP1' ),
		( 'SP1', 4457, convert(datetime,'31-05-2016',105), 'CU 6A for SQL Server 2014 SP1' ),
		( 'SP1', 4459, convert(datetime,'20-06-2016',105), 'CU 7 for SQL Server 2014 SP1' ),
		( 'SP1', 5000, convert(datetime,'11-07-2016',105), 'SQL Server 2014 SP2' );

	insert into #SQLColumnstoreImprovements (BuildVersion, SQLBranch, Description, URL )
		values 
		( 2342, 'RTM', 'FIX: Error 35377 when you build or rebuild clustered columnstore index with maxdop larger than 1 through MARS connection in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2942895' ),
		( 2370, 'RTM', 'FIX: Loads or queries on CCI tables block one another in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2931815' ),
		( 2370, 'RTM', 'FIX: Access violation when you insert data into a table that has a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2966096' ),
		( 2370, 'RTM', 'FIX: Error when you drop a clustered columnstore index table during recovery in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2974397' ),
		( 2370, 'RTM', 'FIX: Poor performance when you bulk insert into partitioned CCI in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2969421' ),
		( 2370, 'RTM', 'FIX: Truncated CCI partitioned table runs for a long time in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2969419' ),
		( 2370, 'RTM', 'FIX: DBCC SHRINKDATABASE or DBCC SHRINKFILE cannot move pages that belong to the nonclustered columnstore index', 'https://support.microsoft.com/en-us/kb/2967198' ),
		( 2402, 'RTM', 'FIX: UPDATE or INSERT statement on CCI makes sys.partitions not match actual row count in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2978472' ), 
		( 2402, 'RTM', 'FIX: Cannot create indexed view on a clustered columnstore index and BCP on the table fails in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2981764' ),
		( 2402, 'RTM', 'FIX: Some columns in sys.column_store_segments view show NULL value when the table has non-dbo schema in SQL Server', 'https://support.microsoft.com/en-us/kb/2989704' ),
		( 2430, 'RTM', 'FIX: Error 8654 when you run "INSERT INTO … SELECT" on a table with clustered columnstore index in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/2998301' ),
		( 2430, 'RTM', 'FIX: UPDATE STATISTICS performs incorrect sampling and processing for a table with columnstore index in SQL Server', 'https://support.microsoft.com/en-us/kb/2986627' ),
		( 2456, 'RTM', 'FIX: Error 35377 occurs when you try to access clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3020113' ),
		( 2480, 'RTM', 'FIX: Access violation occurs when you delete rows from a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029762' ),
		( 2480, 'RTM', 'FIX: OS error 665 when you execute DBCC CHECKDB command for database that contains columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029977' ),
		( 2480, 'RTM', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 2480, 'RTM', 'FIX: Improved memory management for columnstore indexes to deliver better query performance in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3053664' ),
		( 2495, 'RTM', 'FIX: Partial results in a query of a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067257' ),
		( 2546, 'RTM', 'FIX: Error 33294 occurs when you alter column types on a table that has clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3070139' ),
		( 2546, 'RTM', '"Non-yielding Scheduler" error when a database has columnstore indexes on a SQL Server 2014 instance', 'https://support.microsoft.com/en-us/kb/3069488' ),
		( 2546, 'RTM', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067968' ),
		( 2553, 'RTM', 'FIX: Rare index corruption when you build a columnstore index with parallelism on a partitioned table in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3080155' ), 
		( 2556, 'RTM', 'FIX: Access violation when you query against a table that contains column store indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3097601' ),
		( 2556, 'RTM', 'FIX: FIX: Assert occurs when you change the type of column in a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3098529' ),
		( 2560, 'RTM', 'FIX: "Non-yielding Scheduler" condition when you query a partitioned table that has a column store index in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3121647' ),
		( 2564, 'RTM', 'FIX: Columnstore index corruption occurs when you use AlwaysOn Availability Groups in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3135751' ),
		( 2568, 'RTM', 'Query plan generation improvement for some columnstore queries in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3146123' ),
		( 4100, 'SP1', 'LOB reads are shown as zero when "SET STATISTICS IO" is on during executing a query with clustered columnstore index.', 'https://support.microsoft.com/en-us/kb/3058865' ),
		( 4100, 'SP1', 'FIX: OS error 665 when you execute DBCC CHECKDB command for database that contains columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029977' ),
		( 4100, 'SP1', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 4416, 'SP1', 'FIX: Improved memory management for columnstore indexes to deliver better query performance in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3053664' ),
		( 4416, 'SP1', '"Non-yielding Scheduler" error when a database has columnstore indexes on a SQL Server 2014 instance', 'https://support.microsoft.com/en-us/kb/3069488' ),
		( 4416, 'SP1', 'FIX: Access violation occurs when you delete rows from a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029762' ),
		( 4416, 'SP1', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3067968' ),
		( 4416, 'SP1', 'FIX: Severe error in SQL Server 2014 during compilation of a query on a table with clustered columnstore index', 'https://support.microsoft.com/en-us/kb/3068297' ),
		( 4416, 'SP1', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 4416, 'SP1', 'FIX: Partial results in a query of a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067257' ),
		( 4416, 'SP1', 'FIX: Error 33294 occurs when you alter column types on a table that has clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3070139' ),
		( 4427, 'SP1', 'FIX: Rare index corruption when you build a columnstore index with parallelism on a partitioned table in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3080155' ),
		( 4436, 'SP1', 'FIX: Query stops responding when you run a parallel query on a table that has a columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3110497' ),
		( 4439, 'SP1', 'FIX: Error 35377 occurs when you try to access clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3020113' ),
		( 4449, 'SP1', 'FIX: Columnstore index corruption occurs when you use AlwaysOn Availability Groups in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3135751' ),
		( 4449, 'SP1', 'FIX: SELECT…INTO statement retrieves incorrect result from a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3152606' ),
		( 4459, 'SP1', 'FIX: DBCC CHECKTABLE returns an incorrect result after the clustered columnstore index is rebuilt in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3168712' ),
		( 4459, 'SP1', 'Query plan generation improvement for some columnstore queries in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3146123' );


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

		-- Display the current information about this SQL Server 
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
		( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2014/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 ),
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
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.3.0, July 2016

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

Changes in 1.0.3
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions	

Changes in 1.0.4
	- Bug fixes for the Nonclustered Columnstore Indexes creation conditions
	- Buf fixes for the data types of the monitored functionalities, that in certain condition would give an error message.
	- Bug fix for displaying the same primary key clustered index twice in the T-SQL drop script

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed displaying wrong number of rows for the found suggested tables
	- Fixed error for filtering out the secondary nonclustered indexes in some bigger databases
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for InMemory Tables
	+ Added information about the converted table location (In-Memory or Disk-Based)
	+ Added new parameter for filtering the table location - @indexLocation with possible values (In-Memory or Disk-Based)
	+ Added new parameter for controlling the needed statistics update for Memory Optimised tables - @updateMemoryOptimisedStats with default value set on false
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_SuggestedTables as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.3.0, July 2016
*/
alter procedure dbo.cstore_SuggestedTables(
-- Params --
	@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
	@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
	@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
	@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
	@showTSQLCommandsBeta bit = 0,								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
	@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered',		-- Allows to define the type of Columnstore Index to be created eith possible values of 'Clustered' and 'Nonclustered'
	@updateMemoryOptimisedStats bit = 0							-- Allows statistics update on the InMemory tables, since they are stalled within SQL Server 2014
-- end of --
) as 
begin
	set nocount on;

	declare 
		@readCommitedSnapshot bit = 0,
		@snapshotIsolation bit = 0;

	-- Verify Snapshot Isolation Level or Read Commited Snapshot 
	select @readCommitedSnapshot = is_read_committed_snapshot_on, 
		@snapshotIsolation = snapshot_isolation_state
		from sys.databases
		where database_id = DB_ID();

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	if OBJECT_ID('tempdb..#TablesToColumnstore') IS NOT NULL
		drop table #TablesToColumnstore;

	create table #TablesToColumnstore(
		[ObjectId] int NOT NULL PRIMARY KEY,
		[TableLocation] varchar(15) NOT NULL,
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
		, case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end 
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
			inner join sys.indexes ind
				on ind.object_id = p.object_id and ind.index_id = p.index_id
			left join sys.dm_db_xtp_table_memory_stats xtpMem
				on xtpMem.object_id = t.object_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
				from sys.indexes ind
				where t.object_id = ind.object_id
					and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%')
			 and (@schemaName is null or object_schema_name( t.object_id ) = @schemaName)
			 and t.is_memory_optimized = case @indexLocation when 'In-Memory' then 1 when 'Disk-Based' then 0 else t.is_memory_optimized end
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from sys.columns as col
							inner join sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					and (select count(*)
							from sys.objects so
							where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
					and (select count(*)
							from sys.indexes ind
							where t.object_id = ind.object_id
								and ind.type in (3,4) ) = 0
					and (select count(*) 
							from sys.change_tracking_tables ctt with(READUNCOMMITTED)
							where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
									and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) = 0
					and t.is_tracked_by_cdc = 0
					and t.is_memory_optimized = 0
					and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, ind.data_space_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having (sum(p.rows) > @minRowsToConsider or (sum(p.rows) = 0 and is_memory_optimized = 1) )
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
				(sum(a.total_pages) + isnull(sum(memory_allocated_for_table_kb),0) / 1024. / 1024 * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)
	union all
	select t.object_id as [ObjectId]
		, 'Disk-Based'
		, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id, db_id('tempdb'))) as 'TableName'
		, replace(object_name(t.object_id, db_id('tempdb')),' ', '') as 'ShortTableName'
		, max(p.rows) as 'Row Count'
		, ceiling(max(p.rows)/1045678.) as 'Min RowGroups' 
		, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
		, (select count(*) from tempdb.sys.columns as col
			where t.object_id = col.object_id ) as 'Cols Count'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
		   ) as 'String Cols'
		, (select sum(col.max_length) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ) as 'Sum Length'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
					  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as 'Unsupported'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as 'LOBs'
		, (select count(*) 
				from tempdb.sys.columns as col
				where is_computed = 1 ) as 'Computed'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
		, @readCommitedSnapshot as 'RCSI'
		, @snapshotIsolation as 'Snapshot'
		, t.is_tracked_by_cdc as 'CDC'
		, (select count(*) 
				from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
		, t.is_memory_optimized as 'InMemoryOLTP'
		, t.is_replicated as 'Replication'
		, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
		, t.is_filetable as 'FileTable'
		from tempdb.sys.tables t
			inner join tempdb.sys.partitions as p 
				ON t.object_id = p.object_id
			inner join tempdb.sys.allocation_units as a 
				ON p.partition_id = a.container_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@tableName is null or object_name( t.object_id, db_id('tempdb') ) like '%' + @tableName + '%')
			 and (@schemaName is null or object_schema_name( t.object_id, db_id('tempdb') ) = @schemaName)
			 and t.is_memory_optimized = case @indexLocation when 'In-Memory' then 1 when 'Disk-Based' then 0 else t.is_memory_optimized end
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from tempdb.sys.columns as col
							inner join tempdb.sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					and (select count(*)
							from tempdb.sys.objects so
							where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
					and (select count(*)
							from tempdb.sys.indexes ind
							where t.object_id = ind.object_id
								and ind.type in (3,4) ) = 0
					and (select count(*) 
							from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
							where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
									and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) = 0
					and t.is_tracked_by_cdc = 0
					and t.is_memory_optimized = 0
					and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) > @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from tempdb.sys.columns as col
						inner join tempdb.sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(sum(a.total_pages) * 8.0 / 1024. / 1024 >= @minSizeToConsiderInGB)

	-- Get the information on Memory Optimised Tables
	if @updateMemoryOptimisedStats = 1
	begin 
		declare @updateStatTSQL nvarchar(1000);
		declare inmemRowCountCursor CURSOR LOCAL READ_ONLY for
   			select N'Update Statistics ' + TableName + ' WITH FULLSCAN, NORECOMPUTE'
				from #TablesToColumnstore
				where TableLocation = 'In-Memory';

		open inmemRowCountCursor;

		fetch next 
			from inmemRowCountCursor 
				into @updateStatTSQL;

		while @@FETCH_STATUS = 0 BEGIN
			exec sp_executesql @updateStatTSQL;
			fetch next from inmemRowCountCursor 
				into @updateStatTSQL;
		END

		close inmemRowCountCursor
		deallocate inmemRowCountCursor


		update #TablesToColumnstore
			set [Row Count] = ISNULL(st.[rows],0),
				[Min RowGroups] = ceiling(ISNULL(st.[rows],0)/1045678.),
				[Size in GB] = cast( memory_allocated_for_table_kb / 1024. / 1024 as decimal(16,3) )
			from #TablesToColumnstore temp
				inner join sys.dm_db_xtp_index_stats AS ind
					on temp.ObjectId = ind.object_id
				cross apply sys.dm_db_stats_properties (ind.object_id,ind.index_id) st
				inner join sys.dm_db_xtp_table_memory_stats xtpMem
					on temp.ObjectId = xtpMem.object_id
			  where ind.index_id = 2 and temp.TableLocation = 'In-Memory';
	end 

	delete from #TablesToColumnstore
		where [Size in GB] < @minSizeToConsiderInGB
			or [Row Count] < @minRowsToConsider;


	-- Show the found results
	select case when ([InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) <= 0 then 'Nonclustered Columnstore'
			when ([Primary Key] + [Foreign Keys] + [Unique Constraints] + [Triggers] + [CDC] + [CT] +
				  [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) > 0 then 'None' 
			when ([Clustered Index] + [Nonclustered Indexes] + [Primary Key] + [Foreign Keys] + [CDC] + [CT] +
				  [Unique Constraints] + [Triggers] + [RCSI] + [Snapshot] + [CDC] + [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
				  - ([LOBs] + [Computed])) = 0 and [Unsupported] = 0 then 'Both Columnstores'  
		   end as 'Compatible With'
		, TableLocation	
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
	Version: 1.3.0, July 2016

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

Changes in 1.2.0
	+ Added Primary Key for dbo.cstore_Clustering table
	+ Improved setup script for dbo.cstore_Clustering table, for avoiding adding already existing tables
	- Fixed bug for the tables with no comrpessed Row Groups, which were never maintained, even though under some conditions forcing not completely full Delta-Store is important

Changes in 1.3.0
	+ Added logic for the Optimizable Row Groups, meaning that if there is no potential gain for the Rebuild even with trimmed Row Groups - then no Rebuild will take place
	+ Added new parameter for executing maintenance on a specific partition: @partition_number
	* Updated to support the new output columns of the CISL 1.3.0 functions
	+ Added logic to support automated canceling of execution on the Availability Groups Seconary Replicas
	* Improved debug logging output with less useless messages
*/

declare @createLogTables bit = 1;

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
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

-- ------------------------------------------------------------------------------------------------------------------------------------------------------
-- Verification of the required Stored Procedures from CISL
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetRowGroups Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetAlignment Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetFragmentation Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetDictionaries Stored Procedure from CISL before advancing!', 1; 
	Return;
end

-- Setup of the logging tables
IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
begin
	-- Maintenance statistics log 
	create table dbo.cstore_MaintenanceData_Log(
		id int not null identity(1,1) primary key,
		ExecutionId uniqueidentifier,
		MonitoringTimestamp datetime not null default (GetDate()),
		TableName nvarchar(256) not null,
		IndexName nvarchar(256) not null,
		IndexType nvarchar(256) not null,

		Partition int,

		[CompressionType] varchar(50),
		[BulkLoadRGs] int,
		[OpenDeltaStores] int,
		[ClosedDeltaStores] int,
		[CompressedRowGroups] int,

		ColumnId int,
		ColumnName nvarchar(256),
		ColumntType nvarchar(256),
		SegmentElimination varchar(50),
		DealignedSegments int,
		TotalSegments int,
		SegmentAlignment Decimal(8,2),


		Fragmentation Decimal(8,2),
		DeletedRGs int,
		DeletedRGsPerc Decimal(8,2),
		TrimmedRGs int,
		TrimmedRGsPerc Decimal(8,2),
		AvgRows bigint not null,
		TotalRows bigint not null,
		OptimizableRGs int,
		OptimizableRGsPerc Decimal(8,2),
		RowGroups int,
		TotalDictionarySizes Decimal(9,3),
		MaxGlobalDictionarySize Decimal(9,3),
		MaxLocalDictionarySize Decimal(9,3)
	
	);
end

IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Operation_Log' and schema_id = SCHEMA_ID('dbo') )
begin 
	-- Operation Log table
	create table dbo.cstore_Operation_Log(
		id int not null identity(1,1) constraint [PK_cstore_Operation_Log] primary key clustered,
		ExecutionId uniqueidentifier,
		TableName nvarchar(256),
		Partition int,
		OperationType varchar(10),
		OperationReason varchar(50),
		OperationCommand nvarchar(max),
		OperationCollected bit NOT NULL default(0),
		OperationConfigured bit NOT NULL default(0),
		OperationExecuted bit NOT NULL default (0)
	);
end 

IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Clustering' and schema_id = SCHEMA_ID('dbo') )
begin
	-- Configuration table for the Segment Clustering
	create table dbo.cstore_Clustering(
		TableName nvarchar(256)  constraint [PK_cstore_Clustering] primary key clustered,
		Partition int,
		ColumnName nvarchar(256)
	);

	IF OBJECT_ID('tempdb..#ColumnstoreIndexes') IS NOT NULL
		DROP TABLE #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Compressed RowGroups] int,
		[Total RowGroups] int,
		[Deleted Rows] Decimal(18,6),
		[Active Rows] Decimal(18,6),
		[Total Rows] Decimal(18,6),
		[Size in GB] Decimal(18,3),
		[Scans] int,
		[Updates] int,
		[LastScan] DateTime
	);

	insert into #ColumnstoreIndexes
		exec dbo.cstore_GetRowGroups @indexType = 'CC', @showPartitionDetails = 1;

	insert into dbo.cstore_Clustering( TableName, Partition, ColumnName )
		select TableName, Partition, NULL 
			from #ColumnstoreIndexes ci
			where TableName not in (select clu.TableName from dbo.cstore_Clustering clu);
end


-- **************************************************************************************************************************
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_doMaintenance' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_doMaintenance as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
	Version: 1.3.0, July 2016
*/
alter procedure [dbo].[cstore_doMaintenance](
-- Params --
	@execute bit = 0,								-- Controls if the maintenace is executed or not
	@orderSegments bit = 0,							-- Controls whether Segment Clustering is being applied or not
	@executeReorganize bit = 0,						-- Controls if the Tuple Mover is being invoked or not. We can execute just it, instead of the full rebuild
	@closeOpenDeltaStores bit = 0,					-- Controls if the Open Delta-Stores are closed and compressed
	@usePartitionLevel bit = 1,						-- Controls if whole table is maintained or the maintenance is done on the partition level
	@partition_number int = NULL,					-- Allows to specify a partition to execute maintenance on
	@tableName nvarchar(max) = NULL,				-- Allows to filter out only a particular table 
	@useRecommendations bit = 1,					-- Activates internal optimizations for a more correct maintenance proceedings
	@maxdop tinyint = 0,							-- Allows to control the maximum degreee of parallelism
	@logData bit = 1,								-- Controls if functionalites are being logged into the logging tables
	@debug bit = 0,									-- Prints out the debug information and the commands that will be executed if the @execute parameter is set to 1
    @minSegmentAlignmentPercent tinyint = 70,		-- Sets the minimum alignment percentage, after which the Segment Alignment is forced
	@logicalFragmentationPerc int = 15,				-- Defines the maximum logical fragmentation for the Rebuild
	@deletedRGsPerc int = 10,						-- Defines the maximum percentage of the Row Groups that can be marked as Deleted
	@deletedRGs int = NULL,							-- Defines the maximum number of Row Groups that can be marked as Deleted before Rebuild. NULL means to be ignored.
	@trimmedRGsPerc int = 30,						-- Defines the maximum percentage of the Row Groups that are trimmed (not full)
	@trimmedRGs int = NULL,							-- Defines the maximum number of the Row Groups that are trimmed (not full). NULL means to be ignored.
	@minAverageRowsPerRG int = 550000,				-- Defines the minimum average number of rows per Row Group for triggering Rebuild
	@maxDictionarySizeInMB Decimal(9,3) = 10.,		-- Defines the maximum size of a dictionary to determine the dictionary pressure and avoid rebuilding
	@ignoreInternalPressures bit = 0				-- Allows to execute rebuild of the Columnstore, while ignoring the signs of memory & dictionary pressures
) as
begin
	SET ANSI_WARNINGS OFF;
	set nocount on;

	declare @objectId int = NULL;
	declare @currentTableName nvarchar(256) = NULL;  
	declare @indexName nvarchar(256) = NULL;  
	declare @orderingColumnName nvarchar(128) = NULL;

	-- Alignment
	declare @columnId int = NULL;

	-- Internal Variables
	declare @workid int = -1;
	declare @partitionNumber int = -1;
	declare @isPartitioned bit = 0;
	declare @compressionType varchar(30) = '';
	declare @rebuildNeeded bit = 0;
	declare @orderSegmentsNeeded  bit = 0;
	declare @openDeltaStores int = 0;
	declare @closedDeltaStores int = 1;
	declare @maxGlobalDictionarySizeInMB Decimal(9,3) = -1;
	declare @maxLocalDictionarySizeInMB Decimal(9,3) = -1;
	declare @rebuildReason varchar(100) = NULL;
	declare @SQLCommand nvarchar(4000) = NULL;
	declare @execId uniqueidentifier = NEWID();
	declare @loggingTableExists bit = 0;
	declare @loggingCommand nvarchar(max) = NULL;
	--

	-- Verify if the principal logging table exists and thus enabling logging
	IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Operation_Log' and schema_id = SCHEMA_ID('dbo') )
		set @loggingTableExists = 1;
		
	-- Check if we are running on the secondary replica and exit if not, because the AG readable secondary replica is not supported in SQL Server 2014
	IF exists (select *
					from sys.databases databases
					  INNER JOIN sys.availability_databases_cluster adc 
						ON databases.group_database_id = adc.group_database_id
					  INNER JOIN sys.availability_groups ag 
						ON adc.group_id = ag.group_id
					  WHERE databases.name = DB_NAME() )
		
	begin
		declare @replicaStatus int;
		select @replicaStatus = sys.fn_hadr_is_primary_replica ( DB_NAME() );
		
		if @replicaStatus is NOT NULL or @replicaStatus <> 1 
		begin 
			if @loggingTableExists = 1
			begin 
				set @loggingCommand = N'
									insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
										select ''' + convert(nvarchar(50),@execId) + ''', ''NULL'', ' + cast(@partitionNumber as varchar(10)) + ', ''Exit'', 
												''Secondary Replica'', ''NULL'', 1, ' + cast(case when (@executeReorganize = 1 OR @execute = 1) then 1 else 0 end as char(1));			
				exec (@loggingCommand);
			end
		end 
		return;
	end

	-- ***********************************************************
	-- Enable Reorganize automatically if the Trace Flag 634 is enabled
	if( @useRecommendations = 1 )
	begin
		create table #ActiveTraceFlags(	
			TraceFlag nvarchar(20) not null,
			Status bit not null,
			Global bit not null,
			Session bit not null );

		insert into #ActiveTraceFlags
			exec sp_executesql N'DBCC TRACESTATUS() WITH NO_INFOMSGS';

		create table #ColumnstoreTraceFlags(
			TraceFlag int not null,
			Description nvarchar(500) not null,
			URL nvarchar(600),
			SupportedStatus bit not null 
		);

		if( exists (select TraceFlag from #ActiveTraceFlags where TraceFlag = '634') )
			select @executeReorganize = 1, @closeOpenDeltaStores = 1;
	end


	-- ***********************************************************
	-- Process MAXDOP variable and update it according to the number of visible cores or to the number of the cores, specified in Resource Governor
	declare @coresDop smallint;
	select @coresDop = count(*)
		from sys.dm_os_schedulers 
		where upper(status) = 'VISIBLE ONLINE' and is_online = 1

	declare @effectiveDop smallint  
	select @effectiveDop = effective_max_dop 
		from sys.dm_resource_governor_workload_groups
		where group_id in (select group_id from sys.dm_exec_requests where session_id = @@spid)
	
	if( @maxdop < 0 )
		set @maxdop = 0;
	if( @maxdop > @coresDop )
		set @maxdop = @coresDop;
	if( @maxdop > @effectiveDop )
		set @maxdop = @effectiveDop;

	if @debug = 1
	begin	
		print 'MAXDOP: ' + cast( @maxdop as varchar(3) );
		print 'EFECTIVE DOP: ' + cast( @effectiveDop as varchar(3) );
	end

	-- ***********************************************************
	-- Get All Columnstore Indexes for the maintenance
	IF OBJECT_ID('tempdb..#ColumnstoreIndexes') IS NOT NULL
		DROP TABLE #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Location] varchar(15),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Compressed RowGroups] int,
		[Total RowGroups] int,
		[Deleted Rows] Decimal(18,6),
		[Active Rows] Decimal(18,6),
		[Total Rows] Decimal(18,6),
		[Size in GB] Decimal(18,3),
		[Scans] int,
		[Updates] int,
		[LastScan] DateTime
	)
	
	-- Obtain only Clustered Columnstore Indexes for SQL Server 2014
	insert into #ColumnstoreIndexes
		exec dbo.cstore_GetRowGroups @tableName = @tableName, @indexType = 'CC', @showPartitionDetails = @usePartitionLevel, @partitionId = @partition_number; 
	
	if( @debug = 1 )
	begin
		select *
			from #ColumnstoreIndexes;
	end

	while( exists (select * from #ColumnstoreIndexes) )
	begin
		select top 1 @workid = id,
				@partitionNumber = Partition,
				@currentTableName = TableName,
				@compressionType = [Compression Type],
				@openDeltaStores = [Open DeltaStores],
				@closedDeltaStores = [Closed DeltaStores],
				@orderingColumnName = NULL,
				@maxGlobalDictionarySizeInMB = -1,
				@maxLocalDictionarySizeInMB = -1,
				@rebuildNeeded = 0, 
				@rebuildReason = NULL,
				@orderSegmentsNeeded = @orderSegments
			from #ColumnstoreIndexes
				--where TableName = isnull(@currentTableName,TableName)
			order by id;

		if @debug = 1
		begin
			print '------------------------------------------------';
			print 'Current Table: ' + @currentTableName;
		end
	
		-- Get the object_id of the table
		select @objectId = object_id(@currentTableName);

		-- Obtain pre-configured clustering column name
		IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Clustering' and schema_id = SCHEMA_ID('dbo') )
			select @orderingColumnName = ColumnName
				from dbo.cstore_Clustering
				where TableName = @currentTableName and Partition = @partitionNumber;		

		-- If the column name is not set, then do not force Segments Clustering
		if @orderingColumnName is NULL
		begin
			set @orderSegmentsNeeded = 0;
		end

		-- ***********************************************************
		-- Get Segments Alignment
		IF OBJECT_ID('tempdb..#ColumnstoreAlignment') IS NOT NULL
			DROP TABLE #ColumnstoreAlignment

		create table #ColumnstoreAlignment(
			TableName nvarchar(256),
			Location varchar(15),
			Partition bigint,
			ColumnId int,
			ColumnName nvarchar(256),
			ColumnType nvarchar(256),
			SegmentElimination varchar(50),
			DealignedSegments int,
			TotalSegments int,
			SegmentAlignment Decimal(8,2)
		)

		-- If we are executing no Segment Clustering, then do not look for it - just get results for the very first column
		if( @orderSegmentsNeeded = 0 )
			set @columnId = 1;
		else
			set @columnId = NULL;

		-- Get Results from "cstore_GetAlignment" Stored Procedure
		insert into #ColumnstoreAlignment ( TableName, Location, Partition, ColumnId, ColumnName, ColumnType, SegmentElimination, DealignedSegments, TotalSegments, SegmentAlignment )
				exec dbo.cstore_GetAlignment @objectId = @objectId, 
											@showPartitionStats = @usePartitionLevel, 
											@showUnsupportedSegments = 1, @columnName = @orderingColumnName, @columnId = @columnId;		

		if( --@rebuildNeeded = 0 AND 
			@orderSegmentsNeeded = 1 )
		begin	
			declare @currentSegmentAlignment Decimal(6,2) = 100.;

			select @currentSegmentAlignment = SegmentAlignment
				from #ColumnstoreAlignment
				where SegmentElimination = 'OK' and Partition = @partitionNumber;

			if( @currentSegmentAlignment <= @minSegmentAlignmentPercent )
				Select @rebuildNeeded = 1, @rebuildReason = 'Dealignment';

		end
		
		-- ***********************************************************
		-- Get Fragmentation
		IF OBJECT_ID('tempdb..#Fragmentation') IS NOT NULL
			DROP TABLE #Fragmentation;

		create table #Fragmentation(
			TableName nvarchar(256),
			IndexName nvarchar(256),
			Location varchar(15),
			IndexType nvarchar(256),
			Partition int,
			Fragmentation Decimal(8,2),
			DeletedRGs int,
			DeletedRGsPerc Decimal(8,2),
			TrimmedRGs int,
			TrimmedRGsPerc Decimal(8,2),
			AvgRows bigint,
			TotalRows bigint,
			OptimizableRGs int,
			OptimizableRGsPerc Decimal(8,2),
			RowGroups int
		);

		-- Obtain Columnstore logical fragmentation information
		insert into #Fragmentation
			exec cstore_GetFragmentation @objectId = @objectId, @showPartitionStats = 1;

		-- Obtain the name of the Columnstore index we are working with
		select @indexName = IndexName
			from #Fragmentation
			where TableName = @currentTableName

		-- In the case when there is no fragmentation whatsoever (because there is just open Delta-Stores, for example)
		-- Get the Index Name directly from the DMV
		if @indexName is NULL
		begin 
			select @indexName = ind.name 
				from sys.indexes ind
				where ind.type in (5,6) and ind.object_id = @objectId;
		end

		-- Reorganize for Open Delta-Stores
		if @openDeltaStores > 0 AND (@executeReorganize = 1 OR @execute = 1)
		begin
			set @SQLCommand = 'alter index ' + @indexName + ' on ' + @currentTableName + ' Reorganize';

			if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
				set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
		
			-- Force open Delta-Stores closure
			if( @closeOpenDeltaStores = 1 )
				set @SQLCommand += ' with (compress_all_row_groups = on ) ';

			if @logData = 1
			begin				
				if @loggingTableExists = 1 
				begin
					set @loggingCommand = N'
							insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
								select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''Reorganize'', 
										''Open Delta-Stores'', ''' + @SQLCommand + ''', 1, ' + cast(case when (@executeReorganize = 1 OR @execute = 1) then 1 else 0 end as char(1));
				
					exec (@loggingCommand);
				end
			end

			if( @debug = 1 )
			begin
				print 'Reorganize Open Delta-Stores';
				print @SQLCommand;
			end

			if( @execute = 1 OR @executeReorganize = 1 )
				exec ( @SQLCommand  );
		end

		-- Obtain Dictionaries informations
		IF OBJECT_ID('tempdb..#Dictionaries') IS NOT NULL
			DROP TABLE #Dictionaries;

		create table #Dictionaries(
			TableName nvarchar(256),
			[Type] varchar(20),
			[Location] varchar(15),
			Partition int,
			RowGroups bigint,
			Dictionaries bigint,
			EntryCount bigint,
			RowsServing bigint,
			TotalSizeMB Decimal(8,3),
			MaxGlobalSizeMB Decimal(8,3),
			MaxLocalSizeMB Decimal(8,3),
		);

		insert into #Dictionaries (TableName, Type, Location, Partition, RowGroups, Dictionaries, EntryCount, RowsServing, TotalSizeMB, MaxGlobalSizeMB, MaxLocalSizeMB )
			exec dbo.cstore_GetDictionaries @objectId = @objectId, @showDetails = 0;

		-- Get the current maximum sizes for the dictionaries
		select @maxGlobalDictionarySizeInMB = MaxGlobalSizeMB, @maxLocalDictionarySizeInMB = MaxLocalSizeMB
			from #Dictionaries
			where TableName = @currentTableName and Partition = @partitionNumber;

		-- Store current information in the logging table
		if @logData = 1
		begin
			IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
			begin
				insert into dbo.cstore_MaintenanceData_Log( ExecutionId, TableName, IndexName, IndexType, Partition, 
														[CompressionType], [BulkLoadRGs], [OpenDeltaStores], [ClosedDeltaStores], [CompressedRowGroups],
														ColumnId, ColumnName, ColumntType, 
														SegmentElimination, DealignedSegments, TotalSegments, SegmentAlignment, 
														Fragmentation, DeletedRGs, DeletedRGsPerc, TrimmedRGs, TrimmedRGsPerc, AvgRows, 
														TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups,
														TotalDictionarySizes, MaxGlobalDictionarySize, MaxLocalDictionarySize )
					select top 1 @execId, align.TableName, IndexName, IndexType, align.Partition, 
							[Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores], [Compressed RowGroups],
							align.ColumnId, align.ColumnName, align.ColumnType, 
							align.SegmentElimination, align.DealignedSegments, align.TotalSegments, align.SegmentAlignment,
							frag.Fragmentation, frag.DeletedRGs, frag.DeletedRGsPerc, frag.TrimmedRGs, frag.TrimmedRGsPerc, frag.AvgRows, frag.TotalRows, 
							frag.OptimizableRGs, frag.OptimizableRGsPerc, frag.RowGroups,
							dict.TotalSizeMB, dict.MaxGlobalSizeMB, dict.MaxLocalSizeMB
						from #ColumnstoreAlignment align
						inner join #Fragmentation frag
							on align.TableName = frag.TableName and align.Partition = frag.Partition
						inner join #ColumnstoreIndexes ind
							on ind.TableName = align.TableName and ind.Partition = align.Partition
						inner join #Dictionaries dict
							on ind.TableName = dict.TableName and ind.Partition = dict.Partition
						where align.Partition = @partitionNumber and id = @workid;
			end
			
		end

		-- Remove currently processed record
		delete from #ColumnstoreIndexes
			where id = @workid;


		-- Find a rebuild reason
		if( @rebuildNeeded = 0 )
		begin		
			declare @currentlogicalFragmentationPerc int = 0,
					@currentDeletedRGsPerc int = 0,
					@currentDeletedRGs int = 0,
					@currentTrimmedRGsPerc int = 0,
					@currentTrimmedRGs int = 0,
					@currentOptimizableRGs int = 0,
					@currentMinAverageRowsPerRG int = 0,
					@currentRowGroups int = 0;
			
			-- Determine current fragmentation parameters, as well as the number of row groups
			select @currentlogicalFragmentationPerc = Fragmentation,
					@currentDeletedRGsPerc = DeletedRGsPerc,
					@currentDeletedRGs = DeletedRGs,
					@currentTrimmedRGsPerc = TrimmedRGsPerc, 
					@currentTrimmedRGs = TrimmedRGs,
					@currentOptimizableRGs = OptimizableRgs,
					@currentMinAverageRowsPerRG = AvgRows,
					@currentRowGroups = RowGroups
				from #Fragmentation
				where Partition = @partitionNumber;
			
			-- Advance for searching for rebuilding only if there is more then 1 Row Group
			if( @currentRowGroups > 1 )
			begin 
				if( @rebuildNeeded = 0 AND @currentlogicalFragmentationPerc >= @logicalFragmentationPerc )
					select @rebuildNeeded = 1, @rebuildReason = 'Logical Fragmentation';

				if( @rebuildNeeded = 0 AND @currentDeletedRGsPerc >= @deletedRGsPerc )
					select @rebuildNeeded = 1, @rebuildReason = 'Deleted RowGroup Percentage';

				if( @rebuildNeeded = 0 AND @currentDeletedRGs >= isnull(@deletedRGs,2147483647) )
					select @rebuildNeeded = 1, @rebuildReason = 'Deleted RowGroups';

				-- !!! Check if the trimmed Row Groups are the last ones in the partition/index, and if yes then extract the number of available cores
				-- For that use GetRowGroupsDetails
				if( @currentOptimizableRGs > 0 AND @useRecommendations = 1 )
				begin
					if( @rebuildNeeded = 0 AND @currenttrimmedRGsPerc >= @trimmedRGsPerc )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroup Percentage';
					if( @rebuildNeeded = 0 AND @currenttrimmedRGs >= isnull(@trimmedRGs,2147483647) )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroups';

					if( @rebuildNeeded = 0 AND @currentMinAverageRowsPerRG <= @minAverageRowsPerRG )
						select @rebuildNeeded = 1, @rebuildReason = 'Average Rows per RowGroup';
				end 
		
				-- Verify the dictionary pressure and avoid rebuilding in this case do not rebuild Columnstore
				if( (@maxDictionarySizeInMB <= @maxGlobalDictionarySizeInMB OR @maxDictionarySizeInMB <= @maxLocalDictionarySizeInMB) AND
					@rebuildReason in ('Trimmed RowGroups','Trimmed RowGroup Percentage','Average Rows per RowGroup') )
				begin
					if @ignoreInternalPressures = 0 
						select @rebuildNeeded = 0, @rebuildReason += ' - Dictionary Pressure';
				end
			end
		end

		
		if( @debug = 1 )
		begin
			print 'Reason: ' + isnull(@rebuildReason,'-');
			print 'Rebuild: ' + case @rebuildNeeded when 1 then 'true' else 'false' end;
		end
	
		-- Verify if we are working with a partitioned table
		select @isPartitioned = case when count(*) > 1 then 1 else 0 end 
			from sys.partitions p
			where object_id = object_id(@currentTableName)
				and data_compression in (3,4);

		-- Execute Table Rebuild if needed
		--if( @rebuildNeeded = 1 )
		begin
			if( @orderSegmentsNeeded = 1 AND @orderingColumnName is not null AND 
				@isPartitioned = 0 )
			begin
				set @SQLCommand = 'create clustered index ' + @indexName + ' on ' + @currentTableName + '(' + @orderingColumnName + ') with (drop_existing = on, maxdop = ' + cast(@maxdop as varchar(3)) + ');';

				if( @debug = 1 )
				begin
					print @SQLCommand;
				end
			
				-- Let's recreate Clustered Columnstore Index
				set @SQLCommand += 'create clustered columnstore index ' + @indexName + ' on ' + @currentTableName;
				set @SQLCommand += ' with (data_compression = ' + @compressionType + ', drop_existing = on, maxdop = 1);';

				if @logData = 1
				begin
					--insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
					--	select @execId, @currentTableName, @partitionNumber, 'Recreate', @rebuildReason, @SQLCommand, @execute, @rebuildNeeded;
					if @loggingTableExists = 1 
					begin
						set @loggingCommand = N'
								insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
									select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''Recreate'', 
											''' + @rebuildReason + ''', ''' + @SQLCommand + ''', '+ cast(@execute as char(1)) + ', ' + cast(@rebuildNeeded as char(1));
				
						exec (@loggingCommand);
					end
				end

				if( @debug = 1 )
				begin
					print @SQLCommand;
				end

				-- This command will execute 2 operations at once: creation of rowstore index & creation of columnstore index
				if( @execute = 1 AND @rebuildNeeded = 1 )
				begin 
					begin try 
						exec ( @SQLCommand );
					end try
					begin catch
						-- In the future, to add a logging of the error message
						SELECT	 ERROR_NUMBER() AS ErrorNumber
								,ERROR_SEVERITY() AS ErrorSeverity
								,ERROR_STATE() AS ErrorState
								,ERROR_PROCEDURE() AS ErrorProcedure
								,ERROR_LINE() AS ErrorLine
								,ERROR_MESSAGE() AS ErrorMessage;
						Throw;
					end catch 
				end
			end
		
			-- Process Partitioned Table
			if( @orderSegmentsNeeded = 0 OR (@orderSegmentsNeeded = 1 and @orderingColumnName is NULL) OR
				@isPartitioned = 1 )
			begin
				set @SQLCommand = 'alter table ' + @currentTableName + ' rebuild';
				if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
					set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
				set @SQLCommand += ' with (maxdop = ' + cast(@maxdop as varchar(3)) + ')';

				if( @debug = 1 )
				begin
					print 'Rebuild ' + @rebuildReason;
					print @SQLCommand;
				end

				if @logData = 1
				begin
					if @loggingTableExists = 1 
					begin
						set @loggingCommand = N'
								insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
									select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''' + case @rebuildNeeded when 1 then 'Rebuild' else '' end + ''', 
											''' + @rebuildReason + ''', ''' + @SQLCommand + ''', '+ cast(@execute as char(1)) + ', ' + cast(@rebuildNeeded as char(1));
				
						exec (@loggingCommand);
					end
				end

				if( @execute = 1 AND @rebuildNeeded = 1 )
					exec ( @SQLCommand  );
			end

		end


	
	end

	if( @debug = 1 )
	begin
		--select * from #Fragmentation;
		--select * from #Dictionaries;
		--select * from #ColumnstoreAlignment;
		--select * from #ColumnstoreIndexes;

		-- Output the content of the maintenance log inserted during the execution
		IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
			select *
				from dbo.cstore_MaintenanceData_Log
				where ExecutionId = @execId;
	end
end

GO
