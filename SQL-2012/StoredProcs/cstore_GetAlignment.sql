/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2012: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
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
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetAlignment as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2012: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.2.0, March 2016
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

	IF OBJECT_ID('tempdb..#column_store_segments') IS NOT NULL
		DROP TABLE #column_store_segments

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

