/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.5.0, August 2017

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

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (local)

Changes in 1.3.0
	+ Added support InMemory Columnstore Indexes
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed bug with non-functioning @objectId parameter

Changes in 1.5.0
	+ Added new parameter that allows to filter the results by specific partition number (@partitionNumber)
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
	+ Added new parameter for showing the number of distinct values within the segments, the percentage related to the total number of the rows within table/partition (@countDistinctValues)
	+ Added new parameter for showing the frequency of the column usage as predicates during querying (@scanExecutionPlans), which results are included in the overall recommendation for segment elimination
	+ Added new parameter for showing the overall recommendation number for each table/partition (@showSegmentAnalysis)
	+ Added information on the Predicate Pushdown support (it will be showing results depending on the used edition) - column [Predicate Pushdown]
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetAlignment as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_GetAlignment(
-- Params --
	@schemaName nvarchar(256) = NULL,		-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular table
	@preciseSearch bit = 0,					-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@showSegmentAnalysis BIT = 0,			-- Allows showing the overall recommendation for aligning order for each table/partition
	@countDistinctValues BIT = 0,			-- Allows showing the number of distinct values within the segments, the percentage related to the total number of the rows within table/partition (@countDistinctValues)
	@scanExecutionPlans BIT = 0,			-- Allows showing the frequency of the column usage as predicates during querying (@scanExecutionPlans), which results is included in the overall recommendation for segment elimination
	@indexLocation varchar(15) = NULL,		-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectId int = NULL,					-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1,			-- Shows alignment statistics based on the partition
	@partitionNumber int = 0,				-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
	@showUnsupportedSegments bit = 1,		-- Shows unsupported Segments in the result set
	@columnName nvarchar(256) = NULL,		-- Allows to show data filtered down to 1 particular column name
	@columnId int = NULL					-- Allows to filter one specific column Id
-- end of --
) as 
begin
	set nocount on;

	DROP TABLE IF EXISTS #column_store_segments;

	create table #column_store_segments(
		SchemaName sysname,
		TableName sysname,
		object_id int,
		partition_number int,
		hobt_id bigint,
		partition_id bigint,
		column_id int,
		segment_id int,
		min_data_id bigint,
		max_data_id bigint
	);

	insert into #column_store_segments
	SELECT SchemaName, TableName, object_id, partition_number, hobt_id, partition_id, column_id, segment_id, min_data_id, max_data_id
	FROM ( select object_schema_name(part.object_id) as SchemaName, object_name(part.object_id) as TableName, part.object_id, part.partition_number, part.hobt_id, part.partition_id, seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM sys.column_store_segments seg
				inner join sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
		union all
		select isnull(object_schema_name(part.object_id),'dbo') as SchemaName, 
				obj.name as TableName, part.object_id, part.partition_number, part.hobt_id, part.partition_id, seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM tempdb.sys.column_store_segments seg
				inner join tempdb.sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
				inner join tempdb.sys.objects obj
					on part.object_id = obj.object_id
		) as Res

	ALTER TABLE #column_store_segments
	ADD UNIQUE (hobt_id, partition_id, column_id, min_data_id, segment_id);

	ALTER TABLE #column_store_segments
	ADD UNIQUE (hobt_id, partition_id, column_id, max_data_id, segment_id);

	DROP TABLE IF EXISTS #SegmentAlignmentResults;

	with cteSegmentAlignment as (
		select  part.object_id,  
				case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
				quotename(object_schema_name(part.object_id)) + '.' + quotename(object_name(part.object_id)) as TableName,
				case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
				seg.partition_id, seg.column_id, cols.name as ColumnName, tp.name as ColumnType,
				seg.segment_id, 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS hasOverlappingSegment
			from sys.column_store_segments seg
				inner join sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join sys.indexes ind
					on part.object_id = ind.object_id and ind.type in (5,6)
				inner join sys.columns cols
					on part.object_id = cols.object_id and (seg.column_id = cols.column_id + case ind.data_space_id when 0 then 1 else 0 end )
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
			where (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id) = @tableName) )
				 AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id ) like '%' + @schemaName + '%')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id ) = @schemaName))
				 AND (ISNULL(@objectId,part.object_id) = part.object_id)
				 AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
				and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			group by part.object_id, ind.data_space_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id
		UNION ALL
		select  part.object_id,  
				case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
				quotename(isnull(object_schema_name(part.object_id,db_id('tempdb')),'dbo')) + '.' + quotename(isnull(object_name(part.object_id,db_id('tempdb')),obj.name)) as TableName,
				case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
				seg.partition_id, seg.column_id, cols.name COLLATE DATABASE_DEFAULT as ColumnName, tp.name COLLATE DATABASE_DEFAULT as ColumnType,
				seg.segment_id, 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS hasOverlappingSegment
			from tempdb.sys.column_store_segments seg
				inner join tempdb.sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join tempdb.sys.indexes ind
					on part.object_id = ind.object_id and ind.type in (5,6)
				inner join tempdb.sys.objects obj
					on obj.type = 'U' and obj.object_id = part.object_id
				left join tempdb.sys.columns cols
					on part.object_id = cols.object_id and seg.column_id = cols.column_id
				left join tempdb.sys.types tp
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
			where (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id,db_id('tempdb')) = @tableName) )
				 AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id,db_id('tempdb') ) = @schemaName))
				 AND (ISNULL(@objectId,part.object_id) = part.object_id)
				 AND ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			group by part.object_id, ind.data_space_id, obj.name, 
				case @showPartitionStats when 1 then part.partition_number else 1 end, 
				seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id

	)
	select TableName, Location, partition_number as 'Partition', cte.column_id as 'Column Id', cte.ColumnName, 
		cte.ColumnType,
		case cte.ColumnType when 'numeric' then 'not supported' 
							when 'datetimeoffset' then 'not supported' 
							when 'char' then 'not supported' 
							when 'nchar' then 'not supported' 
							when 'varchar' then 'not supported' 
							when 'nvarchar' then 'not supported' 
							when 'sysname' then 'not supported' 
							when 'binary' then 'not supported' 
							when 'varbinary' then 'not supported' 
							when 'uniqueidentifier' then 'not supported' 
			else 'OK' end as 'Segment Elimination',
		case cte.ColumnType when 'numeric' then 'not supported' 
						when 'datetimeoffset' then 'not supported' 
						when 'binary' then 'not supported' 
						when 'varbinary' then 'not supported' 
						when 'uniqueidentifier' then 'not supported' 
			else 'OK' end as [Predicate Pushdown],
		sum(CONVERT(INT, hasOverlappingSegment)) as [Dealigned Segments],
		count(*) as [Total Segments],
		100 - cast( sum(CONVERT(INT, hasOverlappingSegment)) * 100.0 / (count(*)) as Decimal(6,2)) as [Segment Alignment %]
		INTO #SegmentAlignmentResults
		from cteSegmentAlignment cte
		where ((@showUnsupportedSegments = 0 and cte.ColumnType COLLATE DATABASE_DEFAULT not in ('numeric','datetimeoffset','char', 'nchar', 'varchar', 'nvarchar', 'sysname','binary','varbinary','uniqueidentifier') ) 
			  OR @showUnsupportedSegments = 1)
			  and cte.ColumnName COLLATE DATABASE_DEFAULT = isnull(@columnName,cte.ColumnName COLLATE DATABASE_DEFAULT)
			  and cte.column_id = isnull(@columnId,cte.column_id)
		group by Location, TableName, partition_number, cte.column_id, cte.ColumnName, cte.ColumnType
		order by TableName, partition_number, cte.column_id;

	--- *****************************************************
	IF @showSegmentAnalysis = 1 
	BEGIN

		DECLARE @alignedColumnList NVARCHAR(MAX) = NULL;
		DECLARE @alignedColumnNamesList NVARCHAR(MAX) = NULL;
		DECLARE @alignedTable NVARCHAR(128) = NULL,
				@alignedPartition INT = NULL,
				@partitioningClause NVARCHAR(500) = NULL;

		DROP TABLE IF EXISTS #DistinctCounts;
		CREATE TABLE #DistinctCounts(
			TableName SYSNAME NOT NULL,
			PartitionId INT NOT NULL,
			ColumnName SYSNAME NOT NULL,
			DistinctCount BIGINT NOT NULL,
			TotalRowCount BIGINT NOT NULL
		);

		DECLARE alignmentTablesCursor CURSOR LOCAL FAST_FORWARD FOR
			SELECT DISTINCT TableName, [Partition]
				FROM #SegmentAlignmentResults;

		OPEN alignmentTablesCursor  

		FETCH NEXT FROM alignmentTablesCursor   
			INTO @alignedTable, @alignedPartition;

		WHILE @@FETCH_STATUS = 0  
		BEGIN  
			IF @countDistinctValues = 1
			BEGIN
				-- Define Partitioning Clause when showing partitioning information
				SET @partitioningClause = '';
				SELECT @partitioningClause = 'WHERE $PARTITION.[' + pf.name + ']([' + cols.name + ']) = ' + CAST(@alignedPartition AS VARCHAR(8))
 								 FROM sys.indexes ix
								 INNER JOIN sys.partition_schemes ps on ps.data_space_id = ix.data_space_id
								 INNER JOIN sys.partition_functions pf on pf.function_id = ps.function_id 
								INNER JOIN sys.index_columns ic	
									ON ic.object_id = ix.object_id AND ix.index_id = ic.index_id
								INNER JOIN sys.all_columns cols
									ON ic.column_id = cols.column_id AND ic.object_id = cols.object_id 
								WHERE ix.object_id = object_id(@alignedTable)
									AND ic.partition_ordinal = 1 AND @showPartitionStats = 1;
		
				-- Get the list with COUNT(DISTINCT [ColumnName])
				SELECT @alignedColumnList = STUFF((
					SELECt ', COUNT( DISTINCT ' + QUOTENAME(name) + ') as [' + name + ']'
						FROM sys.columns cols
						WHERE OBJECT_ID(@alignedTable) = cols.object_id 
							AND cols.name = isnull(@columnName,cols.name)
							AND cols.column_id = isnull(@columnId,cols.column_id)
						ORDER BY cols.column_id DESC
						FOR XML PATH('')
					), 1, 1, '');
	
				SELECT @alignedColumnNamesList = STUFF((
					SELECt ', [' + name + ']'
						FROM sys.columns cols
						WHERE OBJECT_ID(@alignedTable) = cols.object_id
							AND cols.name = isnull(@columnName,cols.name)
							AND cols.column_id = isnull(@columnId,cols.column_id)						 
						ORDER BY cols.column_id DESC
						FOR XML PATH('')
					), 1, 1, '');

				-- Insert Count(*) and COUNT(DISTINCT*) into the #DistinctCounts table
				EXEC ( N'INSERT INTO #DistinctCounts ' +
						'SELECT ''' + @alignedTable + ''' as TableName, ' + @alignedPartition + ' as PartitionNumber, ColumnName, DistinctCount, __TotalRowCount__ as TotalRowCount ' +
						'	FROM (SELECT ''DistCount'' as [__Op__], COUNT(*) as __TotalRowCount__, ' + @alignedColumnList + 
										 ' FROM ' + @alignedTable + @partitioningClause + ') res ' +
						' UNPIVOT ' +
						'	  ( DistinctCount FOR ColumnName IN(' + @alignedColumnNamesList + ') ' + 
						'	  ) AS finalResult;' );
			END

			FETCH NEXT FROM alignmentTablesCursor   
				INTO @alignedTable, @alignedPartition;
		END

		CLOSE alignmentTablesCursor;  
		DEALLOCATE alignmentTablesCursor;
	
		-- Create table storing results of the access via cached execution plans
		DROP TABLE IF EXISTS #CachedAccessToColumnstore;
		CREATE TABLE #CachedAccessToColumnstore(
			[Schema] SYSNAME NOT NULL,
			[Table] SYSNAME NOT NULL,
			[TableName] SYSNAME NOT NULL,
			[ColumnName] SYSNAME NOT NULL,
			[ScanFrequency] BIGINT,
			[ScanRank] DECIMAL(16,6)
		);

		-- Scan cached execution plans and extract the frequency with which the table columns are searched
		IF @scanExecutionPlans = 1
		BEGIN
			
			-- Extract information from the cached execution plans to determine the frequency of the used(pushed down) predicates against Columnstore Indexes
			;WITH XMLNAMESPACES (DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')   
			INSERT INTO #CachedAccessToColumnstore
			SELECT [Schema], 
				   [Table], 
				   [Schema] + '.' + [Table] as TableName, 
				   [Column] as ColumnName, 
				   SUM(execution_count) as ScanFrequency,
				   Cast(0. as Decimal(16,6)) as ScanRank
				FROM (
					SELECT x.value('(@Database)[1]', 'nvarchar(128)') AS [Database],
						   x.value('(@Schema)[1]', 'nvarchar(128)') AS [Schema],
						   x.value('(@Table)[1]', 'nvarchar(128)') AS [Table],
						   x.value('(@Alias)[1]', 'nvarchar(128)') AS [Alias],
						   x.value('(@Column)[1]', 'nvarchar(128)') AS [Column],
						   xmlRes.execution_count			   
						FROM (
							SELECT dm_exec_query_plan.query_plan,
								   dm_exec_query_stats.execution_count
								FROM sys.dm_exec_query_stats
									CROSS APPLY sys.dm_exec_sql_text(dm_exec_query_stats.sql_handle)
									CROSS APPLY sys.dm_exec_query_plan(dm_exec_query_stats.plan_handle)
								WHERE query_plan.exist('//RelOp//IndexScan//Object[@Storage = "ColumnStore"]') = 1
									  AND query_plan.exist('//RelOp//IndexScan//Predicate//ColumnReference') = 1
						) xmlRes
							CROSS APPLY xmlRes.query_plan.nodes('//RelOp//IndexScan//Predicate//ColumnReference') x1(x) --[@Database = "[' + @dbName + ']"]	
								) res
				WHERE res.[Database] = QUOTENAME(DB_NAME()) AND res.[Schema] IS NOT NULL AND res.[Table] IS NOT NULL
					AND res.[Column]= isnull(@columnName,res.[Column])
				GROUP BY [Schema], [Table], [Column];

			-- Distribute Rank based on the values between 0 & 100
			UPDATE #CachedAccessToColumnstore
				SET ScanRank = ScanFrequency * 100. / (SELECT MAX(ScanFrequency) FROM #CachedAccessToColumnstore);
		END

		-- Deliver the final result
		SELECT res.*, cnt.DistinctCount, cnt.TotalRowCount, 
			CAST(cnt.DistinctCount * 100. / CASE cnt.TotalRowCount WHEN 0 THEN 1 ELSE cnt.TotalRowCount END  as Decimal(8,3)) as [PercDistinct],
			ISNULL(ScanFrequency,0) AS ScanFrequency,
			DENSE_RANK() OVER ( PARTITION BY res.[TableName], [Partition] 
						  ORDER BY ISNULL(ScanRank,-100) + 
								CASE WHEN [DistinctCount] < [Total Segments] OR [DistinctCount] < 2 THEN - 100 ELSE 0 END +
								( ISNULL(cnt.DistinctCount,0) * 100. / CASE ISNULL(cnt.TotalRowCount,0) WHEN 0 THEN 1 ELSE cnt.TotalRowCount END) 
								- CASE [Segment Elimination] WHEN 'OK' THEN 0. ELSE 1000. END
								DESC ) AS [Recommendation]		
			FROM #SegmentAlignmentResults res
			LEFT OUTER JOIN #DistinctCounts cnt
				ON res.TableName = cnt.TableName AND res.ColumnName = cnt.ColumnName AND res.[Partition] = cnt.PartitionId
			LEFT OUTER JOIN #CachedAccessToColumnstore cache
				ON res.TableName = cache.TableName AND res.ColumnName = cache.ColumnName 
			ORDER BY res.TableName, res.Partition, res.[Column Id];

	END
	ELSE
	BEGIN
		SELECT res.*
				FROM #SegmentAlignmentResults res
			ORDER BY res.TableName, res.Partition, res.[Column Id];
	END

	-- Cleanup
	DROP TABLE IF EXISTS #SegmentAlignmentResults;
	DROP TABLE IF EXISTS #DistinctCounts;
	DROP TABLE IF EXISTS #CachedAccessToColumnstore;

end

GO
/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.5.0, August 2017

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

Changes in 1.3.1
	- Added support for Databases with collations different to TempDB

Changes in 1.4.0
	- Fixed a bug for Memory-Optimised Tables not showing the total number of rows

Changes in 1.5.0
	+ Added new parameter that allows to filter the results by specific partition number (@partitionNumber)
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
*/

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetDictionaries as select 1');
GO

/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Dictionaries Analysis - Shows detailed information about the Columnstore Dictionaries
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_GetDictionaries(
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
	@preciseSearch bit = 0,								-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@partitionNumber int = 0,							-- Allows to filter data on a specific partion.
	@columnName nvarchar(256) = NULL,					-- Allows to filter out data base on 1 particular column name
	@indexLocation varchar(15) = NULL,					-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@indexType char(2) = NULL							-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
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
		case i.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		case i.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as [Location],	
		p.partition_number as 'Partition',
		(select count(rg.row_group_id) from sys.column_store_row_groups rg
			where rg.object_id = i.object_id and rg.partition_number = p.partition_number
				  and rg.state = 3 ) as 'RowGroups',
		count(csd.column_id) as 'Dictionaries', 
		sum(csd.entry_count) as 'EntriesCount',
		(select sum(isnull(rg.total_rows,0) - isnull(rg.deleted_rows,0)) from sys.column_store_row_groups rg
			where rg.object_id = i.object_id and rg.partition_number = p.partition_number
				  and rg.state = 3 ) as 'Rows Serving',
		cast( SUM(csd.on_disk_size)/(1024.0*1024.0) as Decimal(8,3)) as 'Total Size in MB',
		cast( MAX(case dictionary_id when 0 then csd.on_disk_size else 0 end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Global Size in MB',
		cast( MAX(case dictionary_id when 0 then 0 else csd.on_disk_size end)/(1024.0*1024.0) as Decimal(8,3)) as 'Max Local Size in MB'
    FROM sys.indexes AS i
		inner join sys.partitions AS p
			on i.object_id = p.object_id 
		inner join sys.column_store_dictionaries AS csd
			on csd.hobt_id = p.hobt_id and csd.partition_id = p.partition_id
    where i.type in (5,6)
		AND (@preciseSearch = 0 AND (@tableName is null or object_name (i.object_id) like '%' + @tableName + '%') 
			OR @preciseSearch = 1 AND (@tableName is null or object_name (i.object_id) = @tableName) )
		AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( i.object_id ) like '%' + @schemaName + '%')
			OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( i.object_id ) = @schemaName))
		AND (ISNULL(@objectId,i.object_id) = i.object_id)
		AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
		and i.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else i.data_space_id end, i.data_space_id )
		and case @indexType when 'CC' then 5 when 'NC' then 6 else i.type end = i.type
	group by object_schema_name(i.object_id) + '.' + object_name(i.object_id), i.object_id, i.data_space_id, i.type, p.partition_number
	union all
	SELECT QuoteName(object_schema_name(i.object_id,db_id('tempdb'))) + '.' + QuoteName(object_name(i.object_id,db_id('tempdb'))) as 'TableName', 
			case i.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
			case i.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as [Location],	
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
			AND (@preciseSearch = 0 AND (@tableName is null or object_name (p.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (p.object_id,db_id('tempdb')) = @tableName) )
			AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( p.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( p.object_id,db_id('tempdb') ) = @schemaName))
			AND (ISNULL(@objectId,p.object_id) = p.object_id)
			AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
			and i.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else i.data_space_id end, i.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else i.type end = i.type
		group by object_schema_name(i.object_id,db_id('tempdb')) + '.' + object_name(i.object_id,db_id('tempdb')), i.object_id, i.type, i.data_space_id, p.partition_number;


	if @showDetails = 1
	select QuoteName(object_schema_name(part.object_id)) + '.' + QuoteName(object_name(part.object_id)) as 'TableName',
			ind.name COLLATE DATABASE_DEFAULT as 'IndexName', 
			part.partition_number as 'Partition',
			cols.name COLLATE DATABASE_DEFAULT as ColumnName, 
			dict.column_id as ColumnId,
			dict.dictionary_id as 'DictionaryId',
			tp.name COLLATE DATABASE_DEFAULT as ColumnType,
			case dictionary_id when 0 then 'Global' else 'Local' end as 'Type', 
			(select sum(isnull(rg.total_rows,0) - isnull(rg.deleted_rows,0)) from sys.column_store_row_groups rg
				where rg.object_id = part.object_id and rg.partition_number = part.partition_number
					  and rg.state = 3 ) as 'Rows Serving', 
			entry_count as 'Entry Count', 
			cast( on_disk_size / 1024. / 1024. as Decimal(8,2)) 'SizeInMb'
		from sys.column_store_dictionaries dict
			inner join sys.partitions part
				ON dict.partition_id = part.partition_id and dict.partition_id = part.partition_id
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
			AND (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id) = @tableName) )
			and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id ) = @schemaName))
			AND (ISNULL(@objectId,part.object_id) = part.object_id)
			AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
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
			dict.dictionary_id as 'DictionaryId',
			tp.name as ColumnType,
			case dictionary_id when 0 then 'Global' else 'Local' end as 'Type', 
			(select sum(isnull(rg.total_rows,0) - isnull(rg.deleted_rows,0)) from sys.column_store_row_groups rg
				where rg.object_id = part.object_id and rg.partition_number = part.partition_number
					  and rg.state = 3 ) as 'Rows Serving',
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
			AND (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id,db_id('tempdb')) = @tableName) )
			AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id,db_id('tempdb') ) = @schemaName))
			AND (ISNULL(@objectId,part.object_id) = part.object_id)
			AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end			and cols.name = isnull(@columnName,cols.name)
			and case dictionary_id when 0 then 'Global' else 'Local' end = isnull(@showDictionaryType, case dictionary_id when 0 then 'Global' else 'Local' end)
			and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
		order by TableName, ind.name, part.partition_number, dict.column_id;

end

GO
/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.5.0, August 2017

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

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetFragmentation as select 1');
GO

/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Fragmenttion - Shows the different types of Columnstore Indexes Fragmentation
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_GetFragmentation (
-- Params --
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to 1 particular table
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionStats bit = 1,					-- Allows to drill down fragmentation statistics on the partition level
	@partitionNumber int = 0						-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
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
			and p.data_compression in (3,4)
			AND (@preciseSearch = 0 AND (@tableName is null or object_name ( p.object_id ) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name ( p.object_id ) = @tableName) )
			AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( p.object_id ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( p.object_id ) = @schemaName))
			AND (ISNULL(@objectId,rg.object_id) = rg.object_id)
			AND rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end		group by p.object_id, ind.data_space_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
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
			and p.data_compression in (3,4)
			AND (@preciseSearch = 0 AND (@tableName is null or object_name (p.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (p.object_id,db_id('tempdb')) = @tableName) )
			AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( p.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( p.object_id,db_id('tempdb') ) = @schemaName))
			AND (ISNULL(@objectId,rg.object_id) = rg.object_id)
			AND rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		group by p.object_id, obj.object_id, obj.name, ind.data_space_id, ind.name, ind.type_desc, case @showPartitionStats when 1 then p.partition_number else 1 end 
		order by TableName;	


end

GO
/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.5.0, August 2017

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
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added support for the Deleted Buffer (Nonclustered Columnstore)
	+ Added support for the InMemory Columnstore Index
	+ Added support for the Index Location (Disk-Based, InMemory)
	+ Added new parameter for filtering the indexes, based on their location (Disk-Based or In-Memory) - @indexLocation
	- Fixed bug with partition information not being shown correctly
	+ Added new parameter for filtering a specific partition
	- Added a couple of bug fixes for the Azure SQLDatabase changes related to Temp Tables

Changes in 1.4.0
	- Added support for the Indexed Views with Nonclustered Columnstore Indexes
	- Added new parameter for filtering the Columnstore Object Type with possible values 'Table' & 'Indexed View'

Changes in 1.4.2
	- Fixed bug on lookup for the Object Name for the empty Columnstore tables

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroups as select 1');
GO

/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups - Shows detailed information on the Columnstore Row Groups inside current Database
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_GetRowGroups(
-- Params --
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@indexLocation varchar(15) = NULL,				-- ALlows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@objectType varchar(20) = NULL,					-- Allows to filter the object type with 2 possible supported values: 'Table' & 'Indexed View'
	@compressionType varchar(15) = NULL,			-- Allows to filter by the compression type with following values 'ARCHIVE', 'COLUMNSTORE' or NULL for both
	@minTotalRows bigint = 000000,					-- Minimum number of rows for a table to be included
	@minSizeInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be included
	@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name pattern
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@showPartitionDetails bit = 0,					-- Allows to show details of each of the available partitions
	@partitionId int = NULL							-- Allows to filter data on a specific partion. Works only if @showPartitionDetails is set = 1 
-- end of --
	) as
begin
	set nocount on;

	with partitionedInfo as (
	select quotename(object_schema_name(ind.object_id)) + '.' + quotename(object_name(ind.object_id)) as 'TableName', 
			case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
			case obj.type_desc when 'USER_TABLE' then 'Table' when 'VIEW' then 'Indexed View' else obj.type_desc end as ObjectType,			
			case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
			part.partition_number as Partition, 
			case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
			sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
			sum(case state when 1 then 1 else 0 end) as 'Open DS',
			sum(case state when 2 then 1 else 0 end) as 'Closed DS',
			sum(case state when 4 then 1 else 0 end) as 'Tombstones',	
			sum(case state when 3 then 1 else 0 end) as 'Compressed',
			count(rg.object_id) as 'Total',
			cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + 
					(select isnull(sum(intpart.rows),0)
						from sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   )/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
			cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) -
					(select isnull(sum(intpart.rows),0)
						from sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   ) /1000000. as Decimal(16,6)) as 'Active Rows (M)',
			cast( sum(isnull(case state when 4 then 0 else total_rows end,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
			cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as 'Size in GB',
			isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
			isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
			max(stat.last_user_scan) as 'LastScan'
			from sys.indexes ind
				inner join sys.objects obj
					on ind.object_id = obj.object_id
				left join sys.column_store_row_groups rg
					on ind.object_id = rg.object_id and ind.index_id = rg.index_id
				left join sys.partitions part with(READUNCOMMITTED)
					on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
				left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
					on rg.object_id = stat.object_id and ind.index_id = stat.index_id
			where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
				  and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				  and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
				  and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				  and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
		 		  and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id) = @tableName) )
				  and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id ) like '%' + @schemaName + '%')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id ) = @schemaName))
				  AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
				  and obj.type_desc = ISNULL(case @objectType when 'Table' then 'USER_TABLE' when 'Indexed View' then 'VIEW' end,obj.type_desc)
			group by ind.object_id, ind.type, obj.type_desc, rg.partition_number, ind.data_space_id,
					part.partition_number
			having cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) + 
					  (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
							from sys.dm_db_xtp_memory_consumers xtpMem 
							where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */) 
					 )
					as Decimal(8,2)) >= @minSizeInGB
					and sum(isnull(total_rows,0)) >= @minTotalRows
	union all
	select quotename(isnull(object_schema_name(obj.object_id, db_id('tempdb')),'dbo')) + '.' + quotename(obj.name) as 'TableName', 
		case ind.type when 5 then 'Clustered' when 6 then 'Nonclustered' end as 'Type',
		case obj.type_desc when 'USER_TABLE' then 'Table' when 'VIEW' then 'Indexed View' else obj.type_desc end as ObjectType,
		case ind.data_space_id when 0 then 'In-Memory' else 'Disk-Based' end as 'Location',
		part.partition_number as Partition,
		case count( distinct part.data_compression_desc) when 1 then max(part.data_compression_desc) else 'Multiple' end  as 'Compression Type',
			sum(case state when 0 then 1 else 0 end) as 'Bulk Load RG',
			sum(case state when 1 then 1 else 0 end) as 'Open DS',
			sum(case state when 2 then 1 else 0 end) as 'Closed DS',
			sum(case state when 4 then 1 else 0 end) as 'Tombstones',	
			sum(case state when 3 then 1 else 0 end) as 'Compressed',
			count(rg.object_id) as 'Total',	
		cast( (sum(isnull(case state when 4 then 0 else deleted_rows end,0)) + 
					(select isnull(sum(intpart.rows),0)
						from tempdb.sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   )/1000000. as Decimal(16,6)) as 'Deleted Rows (M)',
			cast( (sum(isnull(case state when 4 then 0 else (total_rows-isnull(deleted_rows,0)) end,0)) -
					(select isnull(sum(intpart.rows),0)
						from tempdb.sys.internal_partitions intpart
						where ind.object_id = intpart.object_id and rg.partition_number = intpart.partition_number
							and intpart.internal_object_type = 4 /* Deleted Buffer */ ) 
				   ) /1000000. as Decimal(16,6)) as 'Active Rows (M)',
		cast( sum(isnull(rg.total_rows,0))/1000000. as Decimal(16,6)) as 'Total Rows (M)',
		cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) +
				   (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
						from sys.dm_db_xtp_memory_consumers xtpMem 
						where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */)
				  ) as Decimal(8,2)) as 'Size in GB',
		isnull(sum(stat.user_scans)/count(*),0) as 'Scans',
		isnull(sum(stat.user_updates)/count(*),0) as 'Updates',
		max(stat.last_user_scan) as 'LastScan'
		from tempdb.sys.indexes ind
			inner join tempdb.sys.objects obj
				on ind.object_id = obj.object_id
			left join tempdb.sys.column_store_row_groups rg
				on ind.object_id = rg.object_id and ind.index_id = rg.index_id
			left join tempdb.sys.partitions part with(READUNCOMMITTED)
				on ind.object_id = part.object_id and isnull(rg.partition_number,1) = part.partition_number
			left join sys.dm_db_index_usage_stats stat with(READUNCOMMITTED)
				on rg.object_id = stat.object_id and ind.index_id = stat.index_id 
		where ind.type in (5,6)				-- Clustered & Nonclustered Columnstore
				and part.data_compression_desc in ('COLUMNSTORE','COLUMNSTORE_ARCHIVE') 
				and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
				and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
				and case @compressionType when 'Columnstore' then 3 when 'Archive' then 4 else part.data_compression end = part.data_compression
				and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) = @tableName) )
				and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) = @schemaName))
				AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
		--		and isnull(stat.database_id,db_id('tempdb')) = db_id('tempdb')
				and obj.type_desc = ISNULL(case @objectType when 'Table' then 'USER_TABLE' when 'Indexed View' then 'VIEW' end,obj.type_desc)
		group by ind.object_id, obj.object_id, obj.name, ind.type, obj.type_desc, rg.partition_number,
				ind.data_space_id,
				part.partition_number
		having cast( (sum(isnull(size_in_bytes,0) / 1024. / 1024 / 1024) + 
					  (select isnull(sum(xtpMem.allocated_bytes) / 1024. / 1024 / 1024,0) 
							from tempdb.sys.dm_db_xtp_memory_consumers xtpMem 
							where ind.object_id = xtpMem.object_id and xtpMem.memory_consumer_type = 5 /* HKCS_COMPRESSED */) 
					 )
					as Decimal(8,2)) >= @minSizeInGB
				and sum(isnull(total_rows,0)) >= @minTotalRows
	)
	select TableName, 
		Type, 
		ObjectType,
		Location, (case @showPartitionDetails when 1 then Partition else 1 end) as [Partition], 
		max([Compression Type]) as [Compression Type], sum([Bulk Load RG]) as [Bulk Load RG], sum([Open DS]) as [Open DS], sum([Closed DS]) as [Closed DS], 
		sum(Tombstones) as Tombstones, sum(Compressed) as Compressed, sum(Total) as Total, 
		sum([Deleted Rows (M)]) as [Deleted Rows (M)], sum([Active Rows (M)]) as [Active Rows (M)], sum([Total Rows (M)]) as [Total Rows (M)], 
		sum([Size in GB]) as [Size in GB], sum(Scans) as Scans, sum(Updates) as Updates, max(LastScan) as LastScan
		from partitionedInfo
		where Partition = isnull(@partitionId, Partition)  -- Partition Filtering
		group by TableName, Type, ObjectType, Location, (case @showPartitionDetails when 1 then Partition else 1 end)
		order by TableName,	(case @showPartitionDetails when 1 then Partition else 1 end);

end

GO
/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.5.0, August 2017

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
	- Added a couple of bug fixes for the Azure SQLDatabase changes related to Temp Tables

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
*/



declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroupsDetails as select 1');
GO

/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_GetRowGroupsDetails(
-- Params --
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
    @schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
	@preciseSearch bit = 0,							-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@indexLocation varchar(15) = NULL,				-- Allows to filter Columnstore Indexes based on their location: Disk-Based & In-Memory
	@indexType char(2) = NULL,						-- Allows to filter Columnstore Indexes by their type, with possible values (CC for 'Clustered', NC for 'Nonclustered' or NULL for both)
	@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
	@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which were trimmed (not reached the maximum possible size)
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
			and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id) like '%' + @tableName + '%') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id) = @tableName) )
			and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id ) like '%' + @schemaName + '%')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id ) = @schemaName))
			AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
			and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
			and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
			and isnull(rg.created_time,getDate()) >= coalesce(@minCreatedDateTime,rg.created_time,getDate()) 
			and isnull(rg.created_time,getDate()) <= coalesce(@maxCreatedDateTime,rg.created_time,getDate())
			and isnull(rg.trim_reason,255) = coalesce(@trimReason, rg.trim_reason,255)
			and isnull(rg.transition_to_compressed_state,255) = coalesce(@compressionOperation,rg.transition_to_compressed_state,255)
			and isnull(rg.has_vertipaq_optimization,1) = case @showNonOptimisedOnly when 1 then 0 else isnull(rg.has_vertipaq_optimization,1) end
	union all
	select quotename(isnull(object_schema_name(rg.object_id, db_id('tempdb')),'dbo')) + '.' + quotename(obj.name) as [Table Name],
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
			inner join tempdb.sys.objects obj
				on ind.object_id = obj.object_id
		where isnull(rg.trim_reason,1) <> case isnull(@showTrimmedGroupsOnly,-1) when 1 then 1 /* NO_TRIM */ else -1 end 
			and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			and case @indexType when 'CC' then 5 when 'NC' then 6 else ind.type end = ind.type
			and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
			and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
			and (@preciseSearch = 0 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
					OR @preciseSearch = 1 AND (@tableName is null or object_name (ind.object_id,db_id('tempdb')) = @tableName) )
			and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
					OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( ind.object_id,db_id('tempdb') ) = @schemaName))
			AND (ISNULL(@objectId,ind.object_id) = ind.object_id)
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
/*
	Columnstore Indexes Scripts Library for Azure SQL Database: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.5.0, August 2017

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

/*
Known Issues & Limitations: 
	- @showTSQLCommandsBeta parameter is in alpha version and not pretending to be complete any time soon. This output is provided as a basic help & guide convertion to Columnstore Indexes.
	- CLR support is not included or tested
	- Output [Min RowGroups] is not taking present partitions into calculations yet :)
	- In-Memory suggestion supports direct conversion from the Memory-Optimize tables. Support for the Disk-Based -> Memory Optimized tables conversion will be included in the future
	
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

Changes in 1.3.0
	+ Updated with information on replication, which supports Nonclustered Columnstore in SQL Server 2016
	+ Added support for InMemory Tables
	+ Added information about the converted table location (In-Memory or Disk-Based)
	+ Added new parameter for filtering the table location - @indexLocation with possible values (In-Memory or Disk-Based)
	- Added a couple of bug fixes for the Azure SQLDatabase changes related to Temp Tables

Changes in 1.3.1
	- Fixed a bug with filtering out the exact number of @minRows instead of including it

Changes in 1.4.1
	+ Suggestion capability improvements

Changes in 1.4.2
	- Fixed bug on the size of the @minSizeToConsiderInGB parameter
	+ Small Improvements for the @columnstoreIndexTypeForTSQL parameter with better quality generation for the complex objects with Primary Keys

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
	- Fixed bug with the partitioned table not showing the correct number of rows
	+ Added new result column [Partitions] showing the total number of the partitions
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_SuggestedTables as select 1');
GO

/*
	Columnstore Indexes Scripts Library for Azure SQL Database: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE dbo.cstore_SuggestedTables(
-- Params --
	@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	@preciseSearch bit = 0,										-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
	@objectId INT = NULL,										-- Allows to show data filtered down to the specific object_id
	@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
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
		[TableLocation] varchar(15) NOT NULL,
		[TableName] nvarchar(1000) NOT NULL,
		[ShortTableName] nvarchar(256) NOT NULL,
		[Partitions] BIGINT NOT NULL,
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
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 
		, isnull(cast( sum(memory_allocated_for_table_kb) / 1024. / 1024 as decimal(16,3) ),0) + cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3))  as 'size in GB' 
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
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id ) like '%' + @schemaName + '%')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from sys.columns as col
							inner join sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					--and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, ind.data_space_id, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
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
				(isnull(cast( sum(memory_allocated_for_table_kb) / 1024. / 1024 as decimal(16,3) ),0) + cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB)
	union all
	select t.object_id as [ObjectId]
		, 'Disk-Based'
		, quotename(isnull(object_schema_name(obj.object_id,db_id('tempdb')),'dbo')) + '.' + quotename(obj.name) as 'TableName' 
		, replace(obj.name,' ', '')  as 'ShortTableName'
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, SUM(p.rows) as 'Row Count'
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 
		, (select count(*) from tempdb.sys.columns as col
			where t.object_id = col.object_id ) as 'Cols Count'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
		   ) as 'String Cols'
		, isnull((select sum(col.max_length) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ),0) as 'Sum Length'
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
			left join tempdb.sys.partitions as p 
				on t.object_id = p.object_id
			left join tempdb.sys.allocation_units as a 
				on p.partition_id = a.container_id
			inner join tempdb.sys.indexes ind
				on ind.object_id = p.object_id and p.index_id = ind.index_id
			inner join tempdb.sys.objects obj
				on p.object_id = obj.object_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from tempdb.sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0    -- Filtering out tables with existing Columnstore Indexes
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
			 and ind.data_space_id = isnull( case @indexLocation when 'In-Memory' then 0 when 'Disk-Based' then 1 else ind.data_space_id end, ind.data_space_id )
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from tempdb.sys.columns as col
							inner join tempdb.sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					--and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, obj.object_id, obj.name, t.is_tracked_by_cdc, t.is_memory_optimized, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
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
				(cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB);


	-- Show the found results
	select case when ([Triggers] + [FileStream] + [FileTable] + [Unsupported] - ([LOBs] + [Computed])) > 0 then 'None' 
				when ([Clustered Index] + [CDC] + [CT] +
					  [Unique Constraints] + [Triggers] + [InMemoryOLTP] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) = 0 and [Unsupported] = 0
					  AND TableLocation <> 'In-Memory' then 'Both Columnstores' 
				when ( [Triggers] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 
					  AND TableLocation <> 'In-Memory' then 'Nonclustered Columnstore'  
				when ( [Clustered Index] + [CDC] + [CT] +
					  [Unique Constraints] + [Triggers] + [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 
					  AND TableLocation = 'In-Memory' then 'Clustered Columnstore'  
		   end as 'Compatible With'
		, TableLocation		
		, [TableName], [Partitions], [Row Count], [Min RowGroups], [Size in GB], [Cols Count], [String Cols], [Sum Length], [Unsupported], [LOBs], [Computed]
		, [Clustered Index], [Nonclustered Indexes], [XML Indexes], [Spatial Indexes], [Primary Key], [Foreign Keys], [Unique Constraints]
		, [Triggers], [RCSI], [Snapshot], [CDC], [CT], [InMemoryOLTP], [Replication], [FileStream], [FileTable]
		from #TablesToColumnstore tempRes
		where TableLocation = isnull(@indexLocation, TableLocation)
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
					where TableLocation = 'Disk-Based'
				union all
					select t.TableName, 
						'alter table ' + t.TableName +
						' add index CCI_' + t.[ShortTableName] + ' clustered columnstore;' as [TSQL Command]
					   , 'CCL' as type,
					   102 as [Sort Order]
					from #TablesToColumnstore t
					where TableLocation = 'In-Memory'
				union all
				select t.TableName, 'alter table ' + t.TableName + ' drop constraint ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], [type], 
					   case UPPER(type) when 'PK' then 100 when 'F' then 1 when 'UQ' then 100 end as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('PK')
						and t.TableLocation <> 'In-Memory'
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
							where UPPER(so1.type) in ('PK')
								and quotename(ind.name) <> quotename(so1.name)
								and t1.TableLocation <> 'In-Memory')
						and t.TableLocation <> 'In-Memory'
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
							where UPPER(so1.type) in ('PK')
								and quotename(ind.name) <> quotename(so1.name) and t.ObjectId = t1.ObjectId 
								and t1.TableLocation <> 'In-Memory')
						and t.TableLocation <> 'In-Memory'
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
/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
	Version: 1.5.0, August 2017

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

/*
Known Limitations:
	- Segment Clustering is supported only for the Disk-Based Clustered Columnstore Indexes. 
	- Segment Clustering is not supported on the partition level

Changes in 1.4.0
	+ Added support for the Indexed Views with Nonclustered Columnstore Indexes

Changes in 1.5.0
	+ Added support for the new cstore_GetAlignment funciton with partition level support
	+ Added support for the schema parameter of the cstore_getRowGroups funciton
	- Fixed bug with the Primary Key of the cstore_Clustering table covering only the table name and not the partition (Thanks to Thomas Frohlich)
	+ Added @schemaName parameter for supporting schema filtering (Thanks to Thomas Frohlich)
	- Fixed bugs for the case-sensitive instances where variables had wrong names (Thanks to Kendra Little)
*/

declare @createLogTables bit = 1;

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script on Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

-- ------------------------------------------------------------------------------------------------------------------------------------------------------
-- Verification of the required Stored Procedures from CISL
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetRowGroups Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetRowGroupsDetails Stored Procedure from CISL before advancing!', 1; 
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
		IndexLocation varchar(15) not null,

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

IF @createLogTables = 1 
begin
	IF NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Clustering' and schema_id = SCHEMA_ID('dbo') )
	BEGIN
		-- Configuration table for the Segment Clustering
		create table dbo.cstore_Clustering(
			TableName nvarchar(256) NOT NULL,
			Partition int NOT NULL,
			ColumnName nvarchar(256),
			CONSTRAINT PK_cstore_Clustering PRIMARY KEY CLUSTERED ([TableName], [Partition])
		);
	END
	ELSE
	BEGIN
		ALTER TABLE dbo.cstore_Clustering
			ALTER COLUMN TableName nvarchar(256) NOT NULL;
		
		ALTER TABLE dbo.cstore_Clustering
			ALTER COLUMN Partition int NOT NULL;

		ALTER TABLE dbo.cstore_Clustering
			DROP CONSTRAINT PK_cstore_Clustering;

		ALTER TABLE dbo.cstore_Clustering
			ADD CONSTRAINT PK_cstore_Clustering PRIMARY KEY CLUSTERED ([TableName], [Partition]);
	END

	DROP TABLE IF EXISTS #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[ObjectType] varchar(20),
		[Location] varchar(15),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Tombstones] int,
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

	insert into #ColumnstoreIndexes (TableName, Type, ObjectType, Location, Partition, [Compression Type], 
									 BulkLoadRGs, [Open DeltaStores], [Closed DeltaStores], [Tombstones], [Compressed RowGroups], [Total RowGroups], 
									[Deleted Rows], [Active Rows], [Total Rows], [Size in GB], Scans, Updates, LastScan)
		exec dbo.cstore_GetRowGroups @showPartitionDetails = 1;

	insert into dbo.cstore_Clustering( TableName, Partition, ColumnName )
		select DISTINCT TableName, Partition, NULL 
			from #ColumnstoreIndexes ci
			where NOT EXISTS (select clu.TableName from dbo.cstore_Clustering clu WHERE ci.TableName = clu.TableName AND ci.Partition = clu.Partition);
end
GO


-- **************************************************************************************************************************
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_doMaintenance' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_doMaintenance as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
	Version: 1.5.0, August 2017
*/
ALTER PROCEDURE [dbo].[cstore_doMaintenance](
-- Params --
	@execute bit = 0,								-- Controls if the maintenace is executed or not
	@orderSegments bit = 0,							-- Controls whether Segment Clustering is being applied or not
	@executeReorganize bit = 0,						-- Controls if the Tuple Mover is being invoked or not. We can execute just it, instead of the full rebuild
	@closeOpenDeltaStores bit = 0,					-- Controls if the Open Delta-Stores are closed and compressed
	@forceRebuild bit = 0,							-- Allows to force rebuild operation on the tables
	@usePartitionLevel bit = 1,						-- Controls if whole table is maintained or the maintenance is done on the partition level
	@partition_number int = NULL,					-- Allows to specify a partition to execute maintenance on
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
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
	declare @indexType varchar(20) = NULL;
	declare @indexLocation varchar(15) = NULL;
	declare	@totalRows Decimal(18,6) = NULL;
	declare @openRows Decimal(18,6) = NULL;

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

	-- ***********************************************************
	-- Enable Reorganize automatically if the Trace Flag 634 or 10204 is enabled
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

		-- TF 10204: Disables merge/recompress during columnstore index reorganization.
		-- In this case there is no reason to Reorganize the Index.
		if( exists (select TraceFlag from #ActiveTraceFlags where TraceFlag = '10204') )
			select @executeReorganize = 0, @closeOpenDeltaStores = 0, @execute = 1; 
	end


	-- ***********************************************************
	-- Process MAXDOP variable and update it according to the number of visible cores or to the number of the cores, specified in Resource Governor
	declare @coresDop smallint;
	select @coresDop = count(*)
		from sys.dm_os_schedulers 
		where upper(status) = 'VISIBLE ONLINE' and is_online = 1

	declare @effectiveDop smallint  
	-- dm_resource_governor_workload_groups is not supported in Azure SQLDatabase yet
	--select @effectiveDop = effective_max_dop 
	--	from sys.dm_resource_governor_workload_groups
	--	where group_id in (select group_id from sys.dm_exec_requests where session_id = @@spid)
	select @effectiveDop = cast( value as int )
		from sys.database_scoped_configurations
		where name = 'MAXDOP' and cast( value as int ) < @effectiveDop;
	
	if( @maxdop < 0 )
		set @maxdop = 0;
	if( @maxdop > @coresDop )
		set @maxdop = @coresDop;
	if( @maxdop > @effectiveDop AND @maxdop <> 0 AND @effectiveDop <> 0  )
		set @maxdop = @effectiveDop;

	if @debug = 1
	begin	
		print 'MAXDOP: ' + cast( @maxdop as varchar(3) );
		print 'EFECTIVE DOP: ' + cast( @effectiveDop as varchar(3) );
	end
	-- ***********************************************************
	-- Get All Columnstore Indexes for the maintenance
	DROP TABLE IF EXISTS #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[ObjectType] varchar(20),
		[Location] varchar(15),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Tombstones] int,
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
	
	-- Obtain all Columnstore Indexes
	insert into #ColumnstoreIndexes 
		exec dbo.cstore_GetRowGroups @schemaName = @schemaName, @tableName = @tableName, @showPartitionDetails = @usePartitionLevel, @partitionId = @partition_number; 
	
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
				@indexType = Type,
				@indexLocation = Location,
				@totalRows = [Total Rows],
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
		-- Get Number of Rows within open Delta-Stores. This is especially useful to determine if In-Memory Tables can have "Row Migration" process.
		DROP TABLE IF EXISTS #RowGroupsDetails;

		create table #RowGroupsDetails(
			[Table Name] nvarchar(512),
			Location varchar(15),
			partition_nr int,
			row_group_id int,
			state tinyint,
			state_description nvarchar(60),
			total_rows bigint,
			deleted_rows bigint,
			[Size in MB] decimal(8,3),
			trim_reason tinyint,
			trim_reason_desc nvarchar(60),
			compress_op tinyint,
			compress_op_desc nvarchar(60),
			optimised bit,
			generation bigint,
			closed_time datetime,
			created_time datetime );

		insert into #RowGroupsDetails
			exec dbo.cstore_GetRowGroupsDetails @objectId = @objectId, @showNonCompressedOnly = 1;

		select @openRows = sum([total_rows] - [deleted_rows]) / 1000000.
			from #RowGroupsDetails

		-- ***********************************************************
		-- Get Segments Alignment
		DROP TABLE IF EXISTS #ColumnstoreAlignment;

		create table #ColumnstoreAlignment(
			TableName nvarchar(256),
			Location varchar(15),
			Partition bigint,
			ColumnId int,
			ColumnName nvarchar(256),
			ColumnType nvarchar(256),
			SegmentElimination varchar(25),
			PredicatePushdown varchar(25),
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
		insert into #ColumnstoreAlignment ( TableName, Location, Partition, ColumnId, ColumnName, ColumnType, SegmentElimination, PredicatePushdown, DealignedSegments, TotalSegments, SegmentAlignment )
				exec dbo.cstore_GetAlignment @objectId = @objectId, 
											@showPartitionStats = @usePartitionLevel, 
											@showSegmentAnalysis = 0, @scanExecutionPlans = 0,  @countDistinctValues = 0, @partitionNumber = @partitionNumber,
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
		DROP TABLE IF EXISTS #Fragmentation;

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
			if @indexLocation = 'Disk-Based' 
			begin 
				set @SQLCommand = 'alter index ' + @indexName + ' on ' + @currentTableName + ' Reorganize';

				if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
					set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
		
				-- Force open Delta-Stores closure
				if( @closeOpenDeltaStores = 1 )
					set @SQLCommand += ' with (compress_all_row_groups = on ) ';
			end
			else if @indexLocation = 'In-Memory'
			begin
				-- Execute Row Migration process for the InMemory Tables (if we are forcing reorganize or if have over 500.000 rows in the Tail Row Group
				if @openRows >= 0.5 OR @executeReorganize = 1
					set @SQLCommand = 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */';
				else
					set @SQLCommand = '';			
			end
			

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
				print 'Location: ' + @indexLocation;
				print isnull(@SQLCommand, 'NULL');
				print '+++++';
			end

			if( @execute = 1 OR @executeReorganize = 1 )
				exec ( @SQLCommand  );
		end

		-- Obtain Dictionaries informations
		DROP TABLE IF EXISTS #Dictionaries;

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
				insert into dbo.cstore_MaintenanceData_Log( ExecutionId, TableName, IndexName, IndexType, IndexLocation, Partition, 
														[CompressionType], [BulkLoadRGs], [OpenDeltaStores], [ClosedDeltaStores], [CompressedRowGroups],
														ColumnId, ColumnName, ColumntType, 
														SegmentElimination, DealignedSegments, TotalSegments, SegmentAlignment, 
														Fragmentation, DeletedRGs, DeletedRGsPerc, TrimmedRGs, TrimmedRGsPerc, AvgRows, 
														TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups,
														TotalDictionarySizes, MaxGlobalDictionarySize, MaxLocalDictionarySize )
					select top 1 @execId, align.TableName, IndexName, ind.Type, ind.Location, align.Partition, 
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
					if( @rebuildNeeded = 0 AND @currentTrimmedRGsPerc >= @trimmedRGsPerc )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroup Percentage';
					if( @rebuildNeeded = 0 AND @currentTrimmedRGs >= isnull(@trimmedRGs,2147483647) )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroups';

					if( @rebuildNeeded = 0 AND @currentMinAverageRowsPerRG <= @minAverageRowsPerRG )
						select @rebuildNeeded = 1, @rebuildReason = 'Average Rows per RowGroup';
				end 
		
				-- Verify the dictionary pressure and avoid rebuilding in this case
				if( (@maxDictionarySizeInMB <= @maxGlobalDictionarySizeInMB OR @maxDictionarySizeInMB <= @maxLocalDictionarySizeInMB) AND
					@rebuildReason in ('Trimmed RowGroups','Trimmed RowGroup Percentage','Average Rows per RowGroup') )
				begin
					if @ignoreInternalPressures = 0 
						select @rebuildNeeded = 0, @rebuildReason += ' - Dictionary Pressure';
				end
			end
		end
		
		if( @rebuildNeeded = 0 )
			set @SQLCommand = '';
		
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
				@isPartitioned = 0 AND @indexType = 'Clustered')
			begin
				if @indexLocation = 'Disk-Based' 
				begin
					set @SQLCommand = 'create clustered index ' + @indexName + ' on ' + @currentTableName + '(' + @orderingColumnName + ') with (drop_existing = on, maxdop = ' + cast(@maxdop as varchar(3)) + ');';
			
					-- Let's recreate Clustered Columnstore Index
					set @SQLCommand += 'create clustered columnstore index ' + @indexName + ' on ' + @currentTableName;
					set @SQLCommand += ' with (data_compression = ' + @compressionType + ', drop_existing = on, maxdop = 1);';

					if( @debug = 1 )
						print 'SQLCommand:' + @SQLCommand;
				end
				else if @indexLocation = 'In-Memory' 
				begin
					set @SQLCommand = 'alter table ' + @currentTableName + 
										' drop index ' + @indexName + ';';

					set @SQLCommand += 'alter table ' + @currentTableName + 
										' add index ' + @indexName + ' CLUSTERED COLUMNSTORE';
					set @SQLCommand += ' with (data_compression = ' + @compressionType + ');';
				end 

				if @logData = 1
				begin
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
					print 'SQLCommand:' + @SQLCommand;
				end

				-- Execute Rebuild
				if( @execute = 1 AND @rebuildNeeded = 1 AND @indexLocation = 'Disk-Based' )
				begin 
					begin try 
						exec ( @SQLCommand );

						if @debug = 1
							print 'Executed Successfully';
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
			if( @orderSegmentsNeeded = 0 OR										-- No Segment Clustering 
				(@orderSegmentsNeeded = 1 and @orderingColumnName is NULL) OR   -- Forcing Segment Clustering but no Column is configured
				@isPartitioned = 1 )											-- Partitioned Table
			begin
				if @indexLocation = 'Disk-Based'
				begin 
					if @forceRebuild = 0
					begin
						set @SQLCommand = 'alter index ' + @indexName + ' on ' + @currentTableName + ' reorganize';
						if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
							set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5)) + ';';

						-- The second invocation for the elimination of the Tombstones and new potential 
						set @SQLCommand += @SQLCommand;
					end
					else
					begin 
						set @SQLCommand = 'alter table ' + @currentTableName + ' rebuild';
						if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
							set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
						set @SQLCommand += ' with (maxdop = ' + cast(@maxdop as varchar(3)) + ')';
					end
				end
				else if @indexLocation = 'In-Memory'
				begin
					-- Invoking twice so some of the more complex processes, such as deleted Row Groups can be truly removed from the InMemory Table
					set @SQLCommand = 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */;' + CHAR(10);
					--set @SQLCommand += 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */;';
				end

				if( @debug = 1 )
				begin
					print 'Rebuild ' + @rebuildReason;
					print 'SQLCommand:' + @SQLCommand;
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
				begin
					exec ( @SQLCommand  );
					if @debug = 1
						print 'Executed Successfully';
				end
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
