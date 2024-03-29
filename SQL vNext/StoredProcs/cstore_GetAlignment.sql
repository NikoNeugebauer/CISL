/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2019: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.5.1, September 2017

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

Changes in 1.5.0
	+ Added new parameter that allows to filter the results by specific partition number (@partitionNumber)
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
	+ Added new parameter for showing the number of distinct values within the segments, the percentage related to the total number of the rows within table/partition (@countDistinctValues)
	+ Added new parameter for showing the frequency of the column usage as predicates during querying (@scanExecutionPlans), which results are included in the overall recommendation for segment elimination
	+ Added new parameter for showing the overall recommendation number for each table/partition (@showSegmentAnalysis)
	+ Added information on the Predicate Pushdown support (it will be showing results depending on the used edition) - column [Predicate Pushdown]

Changes in 1.5.1
	+ Added new parameter for specifying the name of the database, where the Columnstore Indexes should be located (@dbName)
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2019 or newer
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) < N'15'
begin
	set @errorMessage = (N'You are not running SQL Server 2019 or newer. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end
GO
--------------------------------------------------------------------------------------------------------------------

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server vNext: 
	Columnstore Alignment - Shows the alignment (ordering) between the different Columnstore Segments
	Version: 1.5.1, September 2017
*/
create or alter procedure dbo.cstore_GetAlignment(
-- Params --
	@dbName SYSNAME = NULL,					-- Identifies the Database to run the stored procedure against. If this parameter is left to be NULL, then the current database is used
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
	SET NOCOUNT ON;

	IF @dbName IS NULL
	SET @dbName = DB_NAME(DB_ID());

	DECLARE @dbId INT = DB_ID(@dbName);
	DECLARE @sql NVARCHAR(MAX);


	DROP TABLE IF EXISTS #column_store_segments;
	CREATE TABLE #column_store_segments(
		[SchemaName] SYSNAME NOT NULL,
		[TableName] SYSNAME NOT NULL,
		[object_id] INT NOT NULL,
		[partition_number] INT NOT NULL,
		[hobt_id] INT,
		[partition_id] INT NOT NULL,
		[column_id] INT NOT NULL,
		[segment_id] INT NOT NULL,
		[min_data_id] INT NOT NULL,
		[max_data_id] INT NOT NULL
	);

	SET @sql = N'
	SELECT SchemaName, TableName, object_id, partition_number, hobt_id, partition_id, column_id, segment_id, min_data_id, max_data_id
	INTO #column_store_segments
	FROM ( select object_schema_name(part.object_id,@dbId) as SchemaName, object_name(part.object_id,@dbId) as TableName, 
				part.object_id, part.partition_number, part.hobt_id, part.partition_id, 
				seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM ' + QUOTENAME(@dbName) + N'.sys.column_store_segments seg
				INNER JOIN ' + QUOTENAME(@dbName) + N'.sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
			union all
			select object_schema_name(part.object_id,db_id(''tempdb'')) as SchemaName, object_name(part.object_id,db_id(''tempdb'')) as TableName, 
				part.object_id, part.partition_number, part.hobt_id, part.partition_id, 
				seg.column_id, seg.segment_id, seg.min_data_id, seg.max_data_id
				FROM tempdb.sys.column_store_segments seg
				INNER JOIN tempdb.sys.partitions part
				   ON seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id
		) as Res';

	DECLARE @paramDefinition NVARCHAR(1000) =  '@dbId int';						

	EXEC sp_executesql @sql, @paramDefinition, @dbId = @dbId;

	ALTER TABLE #column_store_segments
		ADD UNIQUE (hobt_id, partition_id, column_id, min_data_id, segment_id);

	ALTER TABLE #column_store_segments
		ADD UNIQUE (hobt_id, partition_id, column_id, max_data_id, segment_id);

	DROP TABLE IF EXISTS #SegmentAlignmentResults;

	--- ***********
	CREATE TABLE #SegmentAlignmentResults(
		[TableName] NVARCHAR(256) NOT NULL,
		[Location] VARCHAR(15) NOT NULL,
		[Partition] INT NOT NULL, 
		[Column Id] INT NOT NULL, 
		[ColumnName] NVARCHAR(128) NOT NULL, 
		[ColumnType] NVARCHAR(128) NOT NULL, 
		[Segment Elimination] VARCHAR(25) NOT NULL,
		[Predicate Pushdown] VARCHAR(25) NOT NULL,
		[Dealigned Segments] INT NOT NULL,
		[Total Segments] INT NULL,
		[Segment Alignment %] DECIMAL(6,2) NOT NULL); 

	SET @sql = N'
	WITH cteSegmentAlignment as (
		select  part.object_id,  
				case ind.data_space_id when 0 then ''In-Memory'' else ''Disk-Based'' end as [Location],
				quotename(object_schema_name(part.object_id,@dbId)) + ''.'' + quotename(object_name(part.object_id,@dbId)) as [TableName],
				case @showPartitionStats when 1 then part.partition_number else 1 end as [partition_number], 
				seg.[partition_id], seg.[column_id], cols.name as [ColumnName], tp.name as [ColumnType],
				seg.[segment_id], 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS [hasOverlappingSegment]
			from ' + QUOTENAME(@dbName) + N'.sys.column_store_segments seg
				inner join ' + QUOTENAME(@dbName) + N'.sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join ' + QUOTENAME(@dbName) + N'.sys.indexes ind
					on part.object_id = ind.object_id and ind.type in (5,6)
				inner join ' + QUOTENAME(@dbName) + N'.sys.columns cols
					on part.object_id = cols.object_id and (seg.column_id = cols.column_id + case ind.data_space_id when 0 then 1 else 0 end )
				inner join ' + QUOTENAME(@dbName) + N'.sys.types tp
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
			where 
			    (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id, @dbId) like ''%'' + @tableName + ''%'') 
				OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id, @dbId) = @tableName) )
			and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id, @dbId ) like ''%'' + @schemaName + ''%'')
				OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id, @dbId ) = @schemaName))
				 AND (ISNULL(@objectId,part.object_id) = part.object_id)
				 AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
				and ind.data_space_id = isnull( case @indexLocation when ''In-Memory'' then 0 when ''Disk-Based'' then 1 else ind.data_space_id end, ind.data_space_id )
			group by part.object_id, ind.data_space_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id
		UNION ALL
		select  part.object_id,  
				case ind.data_space_id when 0 then ''In-Memory'' else ''Disk-Based'' end as [Location],
				quotename(object_schema_name(part.object_id,db_id(''tempdb''))) + ''.'' + quotename(object_name(part.object_id,db_id(''tempdb''))) as TableName,
				case @showPartitionStats when 1 then part.partition_number else 1 end as partition_number, 
				seg.partition_id, seg.column_id, cols.name COLLATE DATABASE_DEFAULT as ColumnName, tp.name COLLATE DATABASE_DEFAULT as ColumnType,
				seg.segment_id, 
				CONVERT(BIT, MAX(CASE WHEN filteredSeg.segment_id IS NOT NULL THEN 1 ELSE 0 END)) AS hasOverlappingSegment
			from tempdb.sys.column_store_segments seg
				inner join tempdb.sys.partitions part
					on seg.hobt_id = part.hobt_id and seg.partition_id = part.partition_id 
				inner join tempdb.sys.indexes ind
					on part.object_id = ind.object_id and ind.type in (5,6)
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
			where (@preciseSearch = 0 AND (@tableName is null or object_name (part.object_id,db_id(''tempdb'')) like ''%'' + @tableName + ''%'') 
					  OR @preciseSearch = 1 AND (@tableName is null or object_name (part.object_id,db_id(''tempdb'')) = @tableName) )
				 AND (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( part.object_id,db_id(''tempdb'') ) like ''%'' + @schemaName + ''%'')
					  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( part.object_id,db_id(''tempdb'') ) = @schemaName))
				 AND (ISNULL(@objectId,part.object_id) = part.object_id)
				 AND ind.data_space_id = isnull( case @indexLocation when ''In-Memory'' then 0 when ''Disk-Based'' then 1 else ind.data_space_id end, ind.data_space_id )
				 AND partition_number = case @partitionNumber when 0 then partition_number else @partitionNumber end
			group by part.object_id, ind.data_space_id, case @showPartitionStats when 1 then part.partition_number else 1 end, seg.partition_id, seg.column_id, cols.name, tp.name, seg.segment_id

	)
	INSERT INTO #SegmentAlignmentResults
	select TableName, Location, 
		partition_number as [Partition], cte.column_id as [Column Id], cte.ColumnName, 
		cte.ColumnType,
		case cte.ColumnType when ''numeric'' then ''not supported''
							when ''datetimeoffset'' then ''not supported'' 
							when ''char'' then ''not supported'' 
							when ''nchar'' then ''not supported'' 
							when ''varchar'' then ''not supported'' 
							when ''nvarchar'' then ''not supported''
							when ''sysname'' then ''not supported'' 
							when ''binary'' then ''not supported'' 
							when ''varbinary'' then ''not supported'' 
							when ''uniqueidentifier'' then ''not supported'' 
			else ''OK'' end as [Segment Elimination],
		case cte.ColumnType when ''numeric'' then ''not supported'' 
						when ''datetimeoffset'' then ''not supported'' 
						when ''char'' then CASE WHEN SERVERPROPERTY(''EngineEdition'') = 3 THEN ''OK'' ELSE ''not supported'' END
						when ''nchar'' then CASE WHEN SERVERPROPERTY(''EngineEdition'') = 3 THEN ''OK'' ELSE ''not supported'' END
						when ''varchar'' then CASE WHEN SERVERPROPERTY(''EngineEdition'') = 3 THEN ''OK'' ELSE ''not supported'' END
						when ''nvarchar'' then CASE WHEN SERVERPROPERTY(''EngineEdition'') = 3 THEN ''OK'' ELSE ''not supported'' END
						when ''sysname'' then CASE WHEN SERVERPROPERTY(''EngineEdition'') = 3 THEN ''OK'' ELSE ''not supported'' END
						when ''binary'' then ''not supported'' 
						when ''varbinary'' then ''not supported'' 
						when ''uniqueidentifier'' then ''not supported'' 
			else ''OK'' end as [Predicate Pushdown],
		sum(CONVERT(INT, hasOverlappingSegment)) as [Dealigned Segments],
		count(*) as [Total Segments],
		100 - cast( sum(CONVERT(INT, hasOverlappingSegment)) * 100.0 / (count(*)) as Decimal(6,2)) as [Segment Alignment %]
		
		from cteSegmentAlignment cte
		where ((@showUnsupportedSegments = 0 and cte.ColumnType COLLATE DATABASE_DEFAULT not in (''numeric'',''datetimeoffset'',''char'', ''nchar'', ''varchar'', ''nvarchar'', ''sysname'',''binary'',''varbinary'',''uniqueidentifier'') ) 
			  OR @showUnsupportedSegments = 1)
			  and cte.ColumnName  = isnull(@columnName,cte.ColumnName COLLATE DATABASE_DEFAULT)
			  and cte.column_id = isnull(@columnId,cte.column_id)
		group by TableName, Location, partition_number, cte.column_id, cte.ColumnName, cte.ColumnType
		order by TableName, partition_number, cte.column_id;';




	SET @paramDefinition				    =  '@indexLocation varchar(15),					
												@preciseSearch bit,						
												@tableName nvarchar(256),			
												@schemaName nvarchar(256),			
												@objectId int,							
												@partitionNumber int,
												
												@showSegmentAnalysis BIT,
												@countDistinctValues BIT,
												@scanExecutionPlans BIT,
												@showPartitionStats BIT,
												@showUnsupportedSegments bit,
												@columnName nvarchar(256),
												@columnId int,	
												@dbId int';						

	EXEC sp_executesql @sql, @paramDefinition, @indexLocation = @indexLocation,
											   @preciseSearch = @preciseSearch, @tableName = @tableName,
											   @schemaName = @schemaName, @objectId = @objectId, 
											   @partitionNumber = @partitionNumber, 
											   @showSegmentAnalysis = @showSegmentAnalysis,
											   @countDistinctValues = @countDistinctValues,
											   @scanExecutionPlans = @scanExecutionPlans,
											   @showPartitionStats = @showPartitionStats,
											   @showUnsupportedSegments = @showUnsupportedSegments,
											   @columnName = @columnName, @columnId = @columnId,
											   @dbId = @dbId;



	--- *****************************************************
	IF @showSegmentAnalysis = 1 
	BEGIN

		DECLARE @alignedColumnList NVARCHAR(MAX) = NULL;
		DECLARE @alignedColumnNamesList NVARCHAR(MAX) = NULL;
		DECLARE @alignedTable NVARCHAR(256) = NULL,
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
				SET @sql = N'
				SELECT @partitioningClause = ''WHERE $PARTITION.['' + pf.name + ''](['' + cols.name + '']) = '' + CAST(@alignedPartition AS VARCHAR(8))
 								 FROM ' + QUOTENAME(@dbName) + N'.sys.indexes ix
								 INNER JOIN ' + QUOTENAME(@dbName) + N'.sys.partition_schemes ps on ps.data_space_id = ix.data_space_id
								 INNER JOIN ' + QUOTENAME(@dbName) + N'.sys.partition_functions pf on pf.function_id = ps.function_id 
								INNER JOIN ' + QUOTENAME(@dbName) + N'.sys.index_columns ic	
									ON ic.object_id = ix.object_id AND ix.index_id = ic.index_id
								INNER JOIN ' + QUOTENAME(@dbName) + N'.sys.all_columns cols
									ON ic.column_id = cols.column_id AND ic.object_id = cols.object_id 
								WHERE ix.object_id = object_id(@alignedTable)
									AND ic.partition_ordinal = 1 AND @showPartitionStats = 1; ';	

				SET @paramDefinition =  '@alignedTable NVARCHAR(128),
										 @alignedPartition INT,
										 @showPartitionStats BIT,	
										 @dbId int,
										 @partitioningClause NVARCHAR(500) OUTPUT';						

				EXEC sp_executesql @sql, @paramDefinition, @alignedTable = @alignedTable,
														   @alignedPartition = @alignedPartition,
														   @showPartitionStats = @showPartitionStats,
														   @partitioningClause = @partitioningClause OUTPUT,
														   @dbId = @dbId;

				

				-- Get the list with COUNT(DISTINCT [ColumnName])
				SET @sql = N'
				SELECT @alignedColumnList = STUFF((
					SELECt '', COUNT( DISTINCT '' + QUOTENAME(name) + '') as ['' + name + '']''
						FROM ' + QUOTENAME(@dbName) + N'.sys.columns cols
						WHERE OBJECT_ID(''' + QUOTENAME(@dbName) + N'.' + @alignedTable + ''') = cols.object_id 
							AND cols.name = isnull(@columnName,cols.name)
							AND cols.column_id = isnull(@columnId,cols.column_id)
						ORDER BY cols.column_id DESC
						FOR XML PATH('''')
					), 1, 1, '''');'
				
				
				SET @paramDefinition =  '@alignedTable NVARCHAR(256),
										 @columnName nvarchar(256),	
										 @columnId INT,
										 @dbId int,
										 @alignedColumnList NVARCHAR(MAX) OUTPUT';						

				EXEC sp_executesql @sql, @paramDefinition, @alignedTable = @alignedTable,
														   @columnName = @columnName,
														   @columnId = @columnId,
														   @alignedColumnList = @alignedColumnList OUTPUT,
														   @dbId = @dbId;


				-- Getting the list with the column names
				SET @sql = N'
				SELECT @alignedColumnNamesList = STUFF((
					SELECt '', ['' + name + '']''
						FROM ' + QUOTENAME(@dbName) + N'.sys.columns cols
						WHERE OBJECT_ID(''' + QUOTENAME(@dbName) + N'.' + @alignedTable + ''') = cols.object_id 
							AND cols.name = isnull(@columnName,cols.name)
							AND cols.column_id = isnull(@columnId,cols.column_id)
						ORDER BY cols.column_id DESC
						FOR XML PATH('''')
					), 1, 1, '''');';

				SET @paramDefinition =  '@alignedTable NVARCHAR(128),
										 @columnName nvarchar(256),	
										 @columnId INT,
										 @dbId int,
										 @alignedColumnNamesList NVARCHAR(MAX) OUTPUT';						

				EXEC sp_executesql @sql, @paramDefinition, @alignedTable = @alignedTable,
														   @columnName = @columnName,
														   @columnId = @columnId,
														   @alignedColumnNamesList = @alignedColumnNamesList OUTPUT,
														   @dbId = @dbId;


				-- Insert Count(*) and COUNT(DISTINCT*) into the #DistinctCounts table
				SET @sql = ( N'INSERT INTO #DistinctCounts ' +
						'SELECT ''' + @alignedTable + ''' as TableName, ' + cast(@alignedPartition as VARCHAR(10)) + ' as PartitionNumber, ColumnName, DistinctCount, __TotalRowCount__ as TotalRowCount ' +
						'	FROM (SELECT ''DistCount'' as [__Op__], COUNT(*) as __TotalRowCount__, ' + @alignedColumnList + 
										 ' FROM ' + QUOTENAME(@dbName) + N'.' + @alignedTable + ISNULL(@partitioningClause,'') + ') res ' +
						' UNPIVOT ' +
						'	  ( DistinctCount FOR ColumnName IN(' + @alignedColumnNamesList + ') ' + 
						'	  ) AS finalResult;' );


				SET @paramDefinition =  '@alignedTable NVARCHAR(128),
										 @columnName nvarchar(256),	
										 @columnId INT,
										 @alignedPartition INT,
										 @dbName SYSNAME,
										 @dbId int,
										 @alignedColumnList NVARCHAR(MAX),
										 @alignedColumnNamesList NVARCHAR(MAX)';						


				EXEC sp_executesql @sql, @paramDefinition, @alignedTable = @alignedTable,
														   @columnName = @columnName,
														   @columnId = @columnId,
														   @alignedColumnNamesList = @alignedColumnNamesList,
														   @alignedPartition = @alignedPartition,
														   @alignedColumnList = @alignedColumnList,
														   @dbName = @dbName,
														   @dbId = @dbId;
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
			SET @sql = N'
			;WITH XMLNAMESPACES (DEFAULT ''http://schemas.microsoft.com/sqlserver/2004/07/showplan'')   
			INSERT INTO #CachedAccessToColumnstore
			SELECT [Schema], 
				   [Table], 
				   [Schema] + ''.'' + [Table] as TableName, 
				   [Column] as ColumnName, 
				   SUM(execution_count) as ScanFrequency,
				   Cast(0. as Decimal(16,6)) as ScanRank
				FROM (
					SELECT x.value(''(@Database)[1]'', ''nvarchar(128)'') AS [Database],
						   x.value(''(@Schema)[1]'', ''nvarchar(128)'') AS [Schema],
						   x.value(''(@Table)[1]'', ''nvarchar(128)'') AS [Table],
						   x.value(''(@Alias)[1]'', ''nvarchar(128)'') AS [Alias],
						   x.value(''(@Column)[1]'', ''nvarchar(128)'') AS [Column],
						   xmlRes.execution_count			   
						FROM (
							SELECT dm_exec_query_plan.query_plan,
								   dm_exec_query_stats.execution_count
								FROM ' + QUOTENAME(@dbName) + N'.sys.dm_exec_query_stats
									CROSS APPLY ' + QUOTENAME(@dbName) + N'.sys.dm_exec_sql_text(dm_exec_query_stats.sql_handle)
									CROSS APPLY ' + QUOTENAME(@dbName) + N'.sys.dm_exec_query_plan(dm_exec_query_stats.plan_handle)
								WHERE query_plan.exist(''//RelOp//IndexScan//Object[@Storage = "ColumnStore"]'') = 1
									  AND query_plan.exist(''//RelOp//IndexScan//Predicate//Compare//ScalarOperator//Identifier//ColumnReference'') = 1
						) xmlRes
							CROSS APPLY xmlRes.query_plan.nodes(''//RelOp//IndexScan//Predicate//Compare//ScalarOperator//Identifier//ColumnReference'') x1(x) --[@Database = "[' + @dbName + ']"]	
							WHERE 
							-- Avoid Inter-column Search references since they are not supporting Segment Elimination
							NOT (query_plan.exist(''(//RelOp//IndexScan//Predicate//Compare//ScalarOperator//Identifier//ColumnReference[@Table])[1]'') = 1
								AND query_plan.exist(''(//RelOp//IndexScan//Predicate//Compare//ScalarOperator//Identifier//ColumnReference[@Table])[2]'') = 1 
								AND x.value(''(@Table)[1]'', ''nvarchar(128)'') = x.value(''(@Table)[2]'', ''nvarchar(128)'') )
							) res
				WHERE res.[Database] = QUOTENAME(@dbName) AND res.[Schema] IS NOT NULL AND res.[Table] IS NOT NULL
					AND res.[Column] COLLATE DATABASE_DEFAULT = isnull(@columnName,res.[Column])
				GROUP BY [Schema], [Table], [Column];'

				SET @paramDefinition =  '@alignedTable NVARCHAR(128),
										 @columnName nvarchar(256),	
										 @columnId INT,
										 @dbName SYSNAME,
										 @dbId int';						

				EXEC sp_executesql @sql, @paramDefinition, @alignedTable = @alignedTable,
														   @columnName = @columnName,
														   @columnId = @columnId,
														   @dbName = @dbName,
														   @dbId = @dbId;

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
				ON res.TableName = cnt.TableName COLLATE DATABASE_DEFAULT
				AND res.ColumnName = cnt.ColumnName COLLATE DATABASE_DEFAULT
				AND res.[Partition] = cnt.PartitionId
			LEFT OUTER JOIN #CachedAccessToColumnstore cache
				ON res.TableName = cache.TableName COLLATE DATABASE_DEFAULT 
					AND res.ColumnName = cache.ColumnName COLLATE DATABASE_DEFAULT
			ORDER BY res.TableName, res.Partition, res.[Column Id];
	END
	ELSE
	BEGIN
		SELECT res.*
				FROM #SegmentAlignmentResults res
			ORDER BY res.[TableName], res.[Partition], res.[Column Id];
	END

	-- Cleanup
	DROP TABLE IF EXISTS #SegmentAlignmentResults;
	DROP TABLE IF EXISTS #DistinctCounts;
	DROP TABLE IF EXISTS #CachedAccessToColumnstore;


end

GO
