/*
	Columnstore Indexes Scripts Library: 
	cstore_sp_estimate_columnstore_compression_savings - Samples the source table into the destination (by detault a temp table) and estimates the overall achieved compression
	Version: 1.6.0, June 2018

	Copyright 2015-2018 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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
	- no support for the filtered Nonclustered Indexes
	- no support for Azure SQL Database Managed Instances
	- no support for the Azure SQL DataWarehouse
	- no support for the Azure SQL Elastic Pools
	- no support for Partition Sampling 
	- no support for the In-Memory tables 
	- mixing names in Azure SQL Database can lead to following problems: 
			Reference to database and/or server name in 'tempdb.dbo.SampledTable' is not supported in this version of SQL Server.
*/

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_sp_estimate_columnstore_compression_savings' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_sp_estimate_columnstore_compression_savings as select 1');
GO

/*
	Columnstore Indexes Scripts Library: 
	cstore_sp_estimate_columnstore_compression_savings - Samples the source table into the destination (by detault a temp table) and estimates the overall achieved compression
	Version: 1.6.0, June 2018
*/
ALTER PROCEDURE dbo.cstore_sp_estimate_columnstore_compression_savings(
-- Params --
	@dbName NVARCHAR(128) = NULL,								-- Allows to specify the name of the source database
	@schemaName NVARCHAR(128) = NULL, 							-- Allows to specify the schema of the source table
	@tableName NVARCHAR(128) = NULL, 							-- Allows to specify the name of the source table
	@indexId INT = NULL,										-- Id of the analysied and compared index (sys.indexes) and by default forcing the CLUSTERED COLUMNSTORE INDEX
	@destinationDbName NVARCHAR(128) = 'tempdb',				-- The database destination for the sampled table
	@destinationSchema NVARCHAR(128) = 'dbo',					-- The schema destination for the sampled table
	@destinationTable NVARCHAR(128) = '#SampledTable',			-- The table name for the sampled table, by default using #SampledData and most of the time you shou
	@data_compression NVARCHAR(20) = 'COLUMNSTORE',				-- The compression type to be used (COLUMNSTORE | COLUMNSTORE_ARCHIVE)
	@rowGroupsNr INT = 4,										-- The number of Columnstore Row Groups * 1048576 to be sampled from the source table (by default is 4 Row Groups) 
	@cancelIfDestTableExists BIT = 1,							-- Allows to cancel the execution if the destination table exists
	@deleteAfterSampling BIT = 1,								-- Allows to control if the sampled table should persist after the script execution or it should be deleted
	@maxDOP INT = 0,											-- The MAX DOP to be used during process execution
	@showTrimmingInfo BIT = 1,									-- Controls the showing of the trimming reasons of the sampled table
	@debug BIT = 0												-- Debugging information for error analysis
-- end of --
) AS
BEGIN
	DECLARE @indexColumns NVARCHAR(MAX) = NULL;

	IF @dbName IS NULL
		SET @dbName = DB_NAME();
	IF @schemaName IS NULL
		SET @schemaName = 'dbo';

	-- If the table name starts with the symbol '#', then we set database name to 'tempdb'
	IF( LEFT(LTRIM(@destinationTable),1) = '#' )
		SET @destinationDbName = 'tempdb';
	IF( LTRIM(@dbName) = '' )
		SET @dbName = DB_NAME();
	IF( LTRIM(@destinationDbName) = '' )
		SET @destinationDbName = DB_NAME();

	DECLARE @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
			@SQLServerVersionNumber TINYINT = 0,
			@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
			@errorMessage nvarchar(512);

	SET @SQLServerVersionNumber = substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1);

	DECLARE
		@sourceTableFullNameWithDB NVARCHAR(768) = IIF( SERVERPROPERTY('EngineEdition') <> 5, QUOTENAME(@dbName) + '.' + QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName), QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName) ),
		@srcDbNameWithDot NVARCHAR(128) = IIF( SERVERPROPERTY('EngineEdition') <> 5, QUOTENAME(@dbName) + '.', '' ),
		@destDbNameWithDot NVARCHAR(128) = IIF( SERVERPROPERTY('EngineEdition') <> 5, QUOTENAME(@destinationDbName) + '.', QUOTENAME(@destinationDbName) + '.' ),
		@sampleTableFullName NVARCHAR(512) = QUOTENAME(@destinationSchema) + '.' + QUOTENAME(@destinationTable),
		@sampleTableFullNameWithDB NVARCHAR(768) = IIF( SERVERPROPERTY('EngineEdition') <> 5, QUOTENAME(@destinationDbName) + '.' + QUOTENAME(@destinationSchema) + '.' + QUOTENAME(@destinationTable), CASE WHEN @destinationDbName = 'tempdb' THEN 'tempdb.' ELSE '' END + QUOTENAME(@destinationSchema) + '.' + QUOTENAME(@destinationTable) ),
		@tableFullName NVARCHAR(512) = NULL,
		@srcTableCompression NVARCHAR(25) = 'UNKNOWN',
		@indexType NVARCHAR(20) = '',
		@SourceTableRowCount BIGINT = 0,
		@SourceTableSizeInGB DECIMAL(9,3),
		@DestTableActualRows BIGINT = 0,
		@DestTableSizeInGB DECIMAL(9,3),
		@DataCountPercentage NVARCHAR(25),
		@paramDefinition NVARCHAR(1000),
		@sql NVARCHAR(MAX),
		@sqlColumns NVARCHAR(MAX);
   

	SET NOCOUNT ON;



	-- Ensure that if we are running Azure SQLDatabase than S2 or better Edition that supports Columnstore Indexes is being used
	DECLARE @azureEdition NVARCHAR(128),
			@azureSLO NVARCHAR(5);
	SELECT @azureEdition = CAST(DATABASEPROPERTYEX(DB_NAME(), 'Edition') AS NVARCHAR),
		@azureSLO = CAST(DATABASEPROPERTYEX(DB_NAME(), 'ServiceObjective') AS NVARCHAR);
	IF( SERVERPROPERTY('EngineEdition') = 5 
		AND (@azureEdition <> 'Premium' AND (@azureEdition = 'Standard' AND CAST(SUBSTRING(@azureSLO,1,LEN(@azureSLO)) AS INT) < 2))
		OR  @azureEdition = 'Basic' )
	BEGIN
		SET @errorMessage = (N'Your are not running this script on Azure SQLDatabase that supports Columnstore Indexes. Your are running a ' + @SQLServerEdition + ' with ' + @azureSLO );
		THROW 51000, @errorMessage, 1;
	END

	-- Check if the schema and the table names are specified
	IF( @schemaName IS NULL OR @tableName IS NULL )
	BEGIN
		THROW 51001, 'No table is specified', 1;
	END
	SET @tableFullName = QUOTENAME(@schemaName) + '.' + QUOTENAME(@tableName); 

	IF( @SQLServerVersionNumber = 11 AND @data_compression = 'COLUMNSTORE_ARCHIVE' )
	BEGIN
		THROW 51001, 'Columnstore Archival Compression does not exist in SQL Server 2012!', 1;
	END

	IF( (@destinationDbName <> 'tempdb' AND SERVERPROPERTY('EngineEdition') = 5) AND
		NOT EXISTS (SELECT * FROM sys.databases WHERE name = @destinationDbName) )
	BEGIN
		THROW 51001, 'Specified Destination Database does not exist!', 1;
	END
	IF( SERVERPROPERTY('EngineEdition') = 5 AND (DB_NAME() <> @dbName OR (DB_NAME() <> @destinationDbName AND @destinationDbName <> 'tempdb')) ) 
	BEGIN
		THROW 51001, 'Azure SQL Database does not support cross-database queries!', 1;
	END
	
	-- Verify the source table existance
	DECLARE @sourceTableSQL NVARCHAR(MAX) = N'IF OBJECT_ID( ''' + @sourceTableFullNameWithDB + ''', ''U'') IS NULL 
					BEGIN
					THROW 55001, ''Specified source table does not exists in the specified schema!'', 1;
					END
	';
	EXECUTE sp_executesql @sourceTableSQL;	
	
	SET @sourceTableSQL = N'IF( NOT EXISTS (SELECT 1 FROM ' + @srcDbNameWithDot + N'sys.indexes o
							INNER JOIN ' + @srcDbNameWithDot + N'sys.index_columns c
								ON c.object_id = o.object_id AND o.index_id = c.index_id
									AND o.object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName + N''')
							INNER JOIN ' + @srcDbNameWithDot + N'sys.columns cols
								ON c.object_id = cols.object_id AND c.column_id = cols.column_id
											
							WHERE ' + 
							CASE WHEN @indexId IS NULL THEN ' (c.index_id = 0 OR c.index_id = 1) ' 
								ELSE ' c.index_id = ' + CAST(@indexId AS NVARCHAR(10)) END + N' ) )
							BEGIN
								THROW 55001, ''Specified index id does not exist!'', 1;
							END';
	EXECUTE sp_executesql @sourceTableSQL;

	-- Cancel execution if the destination table already exists
	IF @cancelIfDestTableExists = 1
	BEGIN 
		SET @sql = N'IF OBJECT_ID( ''' + @sampleTableFullNameWithDB + ''', ''U'') IS NOT NULL 
					 BEGIN
						THROW 55001, ''The Destination Table already exists!'', 1;
					 END
		';
		EXECUTE sp_executesql @sql;		
	END
	ELSE
	BEGIN
		-- Ensure that the destination table is dropped
		SET @sql = N'IF OBJECT_ID( ''' + @sampleTableFullNameWithDB + ''', ''U'') IS NOT NULL 
						DROP TABLE  ' + @sampleTableFullNameWithDB + ';
		';
	END

	-- Print the prepared SQL Statement
	IF( @debug = 1 )
		PRINT @sql;

	-- Drop the destination table if it already exists
	EXECUTE sys.sp_executesql @sql;


	
	SET @sqlColumns = N'SELECT @indexType = (SELECT MAX( CASE o.type WHEN 1 THEN ''CLUSTERED'' WHEN 5 THEN ''CLUSTERED'' ELSE ''NONCLUSTERED'' END ) FROM ' + @srcDbNameWithDot + N'sys.indexes o
							INNER JOIN ' + @srcDbNameWithDot + N'sys.index_columns c
								ON c.object_id = o.object_id AND o.index_id = c.index_id
									AND o.object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName + N''')
							INNER JOIN ' + @srcDbNameWithDot + N'sys.columns cols
								ON c.object_id = cols.object_id AND c.column_id = cols.column_id
											
							WHERE ' + 
							CASE WHEN @indexId IS NULL THEN ' (c.index_id = 0 OR c.index_id = 1) ' 
								ELSE ' c.index_id = ' + CAST(@indexId AS NVARCHAR(10)) END + N'	)
						,
						@indexColumns = CAST(STUFF(
						(SELECT '','' + QUOTENAME(cols.name)
							FROM ' + @srcDbNameWithDot + N'sys.indexes o
							INNER JOIN ' + @srcDbNameWithDot + N'sys.index_columns c
								ON c.object_id = o.object_id AND o.index_id = c.index_id
									AND o.object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName + N''')
							INNER JOIN ' + @srcDbNameWithDot + N'sys.columns cols
								ON c.object_id = cols.object_id AND c.column_id = cols.column_id
											
							WHERE ' + 
							CASE WHEN @indexId IS NULL THEN ' (c.index_id = 0 OR c.index_id = 1) ' 
								ELSE ' c.index_id = ' + CAST(@indexId AS NVARCHAR(10)) END + N'		
						FOR XML PATH('''') ), 1, 1, ''''
							) AS NVARCHAR(4000));';
	IF( @debug = 1 )
		PRINT @sqlColumns;

	SET @paramDefinition  =  '@indexType NVARCHAR(20) OUTPUT,
							  @indexColumns NVARCHAR(MAX) OUTPUT';
			
	EXECUTE sp_executesql @sqlColumns, 
							@paramDefinition, 
							@indexType = @indexType OUTPUT,
							@indexColumns = @indexColumns OUTPUT;
								  


	
	IF @indexType = 'CLUSTERED'
		SET @indexColumns = '*'
		
	-- Copy the meta-structure of the table
	SET @sql += N'SELECT TOP (0) ' + @indexColumns + N'
				INTO ' + @sampleTableFullNameWithDB + '
				FROM ' + @sourceTableFullNameWithDB + '
				WHERE 1 = 0
				UNION ALL 
				SELECT ' + @indexColumns + N' 
					FROM ' + @sourceTableFullNameWithDB + '
				WHERE 1 = 0;';

	-- Add the Clustered Columnstore Index right in the begining, or add the Nonclustered Columnstore Index after loading the data
	IF( @indexType = 'CLUSTERED' )
	BEGIN
		SET @sql += N'
					  CREATE CLUSTERED COLUMNSTORE INDEX CCI_SampledTable_1234567890987654321 ON ' + @sampleTableFullNameWithDB + (CASE WHEN @SQLServerVersionNumber >= 12 THEN ' WITH (DATA_COMPRESSION = ' + @data_compression + ') ' ELSE '' END) + ';
	';
	END

	
	-- Define the amount of the data to be read from the source table 
	SET @DataCountPercentage = '(@rowGroupsNr*1048576)';

	SET @sql += N'INSERT INTO ' + @sampleTableFullNameWithDB + '  WITH(TABLOCKX)
					SELECT TOP ' + @DataCountPercentage + ' ' + @indexColumns + N' 
						FROM ' + @sourceTableFullNameWithDb + 
						CASE WHEN @maxDOP > 0 THEN N' OPTION( MAXDOP ' + CAST(@maxDOP as NVARCHAR(4)) + N')' ELSE '' END + ';';

	-- Add the Clustered Columnstore Index right in the begining, or add the Nonclustered Columnstore Index after loading the data
	IF( @indexType = 'NONCLUSTERED' )
	BEGIN
	
		

		SET @sql += N'
					  CREATE NONCLUSTERED COLUMNSTORE INDEX CCI_SampledTable_1234567890987654321 ON ' + @sampleTableFullNameWithDB + N' (' + @indexColumns + N')
						' + (CASE WHEN @SQLServerVersionNumber >= 12 THEN ' WITH (DATA_COMPRESSION = ' + @data_compression  ELSE '' END) + 
						CASE WHEN @maxDOP > 0 THEN N' , MAXDOP =' + CAST(@maxDOP as NVARCHAR(4))  ELSE '' END + ' ) 
	';
	END

	-- Force closure of any of the Open Delta-Stores (with CC & SQL Server 2014+ or NCC & SQL Server 2016+)
	IF( (@indexType = 'CLUSTERED' AND @SQLServerVersionNumber >= 12) OR
		(@indexType = 'NONCLUSTERED' AND @SQLServerVersionNumber >= 13) )
	BEGIN 
		SET @sql += N'
		ALTER INDEX CCI_SampledTable_1234567890987654321 ON ' + @sampleTableFullNameWithDB + '    
			REORGANIZE WITH (COMPRESS_ALL_ROW_GROUPS = ON);
		';
	END




	SET @sql += N'
	SELECT @SourceTableSizeInGB = SUM(a.used_pages) * 8 / 1024. / 1024.  
	FROM 
		' + @srcDbNameWithDot + 'sys.tables t
	INNER JOIN      
		' + @srcDbNameWithDot + 'sys.indexes i ON t.OBJECT_ID = i.object_id
	INNER JOIN 
		' + @srcDbNameWithDot+ 'sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
	INNER JOIN 
		' + @srcDbNameWithDot + 'sys.allocation_units a ON p.partition_id = a.container_id
	LEFT OUTER JOIN 
		' + @srcDbNameWithDot + 'sys.schemas s ON t.schema_id = s.schema_id
	WHERE 
		t.object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName + ''') ' +
		-- Depending on the index type, specify the condition
		CASE WHEN @indexId IS NULL THEN ' AND (i.index_id = 0 OR i.index_id = 1) ' 
			ELSE ' AND i.index_id = ' + CAST(@indexId AS NVARCHAR(10)) END + N'		
	';

	SET @sql += N'
	SELECT @DestTableSizeInGB = 
	SUM(st.used_page_count) / 128.0 / 1024   
	FROM ' + @destDbNameWithDot +  'sys.indexes i
		INNER JOIN ' + @destDbNameWithDot +  'sys.dm_db_partition_stats st
			ON i.object_id = st.object_id AND I.index_id = st.index_id
	WHERE 
		i.object_id = OBJECT_ID(''' + @sampleTableFullNameWithDB + ''') AND 
		i.name = ''CCI_SampledTable_1234567890987654321'' AND
		i.type IN (5,6)
	;';

	-- Get the Compression from the source table
	SET @sql += N' SELECT @srcTableCompression = MAX(part.data_compression_desc)
					FROM ' + @srcDbNameWithDot + 'sys.partitions part
					INNER JOIN ' + @srcDbNameWithDot + 'sys.tables AS t 
						ON t.[object_id] = part.[object_id]
					CROSS APPLY (SELECT MIN(p2.partition_number) as MinPartition FROM ' + @srcDbNameWithDot + 'sys.partitions p2 WHERE p2.object_id = part.object_id) part2
					WHERE part.index_id IN (0,1)
						AND part2.MinPartition = part.partition_number
						AND part.object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName+ ''')
					GROUP BY part.object_id;	
	;';

	-- Get the number of rows in our original and in our sampled table
	SET @sql += N'
	SELECT @DestTableActualRows = COUNT(*)
		FROM ' + @sampleTableFullNameWithDB + ';
	';
	SET @sql += N'
	SELECT @SourceTableRowCount = SUM(row_count)
		FROM ' + @srcDbNameWithDot + 'sys.dm_db_partition_stats
		WHERE object_id = OBJECT_ID(''' + @srcDbNameWithDot + @tableFullName + ''')   
			AND (index_id=0 or index_id=1);
	';


	-- Show the Row Group Trimming Status, only if we are running SQL Server 2016 or posterior versions
	DECLARE @trimmingSQL NVARCHAR(MAX),
			@NEWID NVARCHAR(128) = REPLACE(CAST(NEWID() AS nvarchar(64)),'-','');
	--IF OBJECT_ID( '[tempdb].[dbo].[##TrimReasons]', 'U') IS NOT NULL 
	--	DROP TABLE [tempdb].[dbo].[##TrimReasons];


	SET @trimmingSQL = N'
	IF OBJECT_ID( ''[tempdb].[dbo].[##TrimReasons_' + @NEWID + ']'', ''U'') IS NOT NULL 
		DROP TABLE [tempdb].[dbo].[##TrimReasons_' + @NEWID + '];

	CREATE TABLE ##TrimReasons_' + @NEWID + '
	(
		TrimReason NVARCHAR(50),
		TrimCount INT
	);';

	EXECUTE sp_executesql @trimmingSQL;

	IF( (SERVERPROPERTY('EngineEdition') = 5 OR @SQLServerVersionNumber > 12) AND @showTrimmingInfo = 1 )
	BEGIN
		SET @sql += N'
		INSERT INTO ##TrimReasons_' + @NEWID + '
			SELECT st.trim_reason_desc as [Trim Reason], COUNT(*) as [Count]
			FROM ' + @destDbNameWithDot + N'sys.dm_db_column_store_row_group_physical_stats st
			WHERE st.object_id = OBJECT_ID(''' + @sampleTableFullNameWithDB + N''')
				AND st.state IN (1,2,3)
			GROUP BY st.trim_reason_desc
		';
	END

	
	IF @debug = 1 
	BEGIN
		PRINT @sql;													
	END 

	-- Delete the sampled table if specified
	IF @deleteAfterSampling = 1 
	BEGIN
		SET @sql += N'
					  DROP TABLE ' + @sampleTableFullNameWithDB + ';';
	END

	-- Set up the parameters for the Dynamic T-SQL code
	SET @paramDefinition  =  '@tableFullName NVARCHAR(512),				
							  @rowGroupsNr INT,
							  @SourceTableRowCount BIGINT OUTPUT,
							  @DestTableActualRows BIGINT OUTPUT,
							  @SourceTableSizeInGB DECIMAL(9,3) OUTPUT,
							  @srcTableCompression NVARCHAR(25) OUTPUT,
							  @DestTableSizeInGB DECIMAL(9,3) OUTPUT';

	
	EXECUTE sp_executesql @sql, 
						  @paramDefinition, 
						  @tableFullName = @tableFullName, 
						  @rowGroupsNr = @rowGroupsNr, 
						  @SourceTableRowCount = @SourceTableRowCount OUTPUT,
						  @DestTableActualRows = @DestTableActualRows OUTPUT, 
						  @SourceTableSizeInGB = @SourceTableSizeInGB OUTPUT,
						  @srcTableCompression = @srcTableCompression OUTPUT,
						  @DestTableSizeInGB = @DestTableSizeInGB OUTPUT;

	IF( @debug = 1 )
	BEGIN
		PRINT '@tableFullName:' + @tableFullName;
		PRINT '@SourceTableRowCount:' + CAST(@SourceTableRowCount AS VARCHAR(30));
		PRINT '@SourceTableSizeInGB:' + CAST(@SourceTableSizeInGB AS VARCHAR(30));
		PRINT '@srcTableCompression:' + @srcTableCompression;
		PRINT '@sampleTableFullNameWithDB:' + @sampleTableFullNameWithDB;
		PRINT '@DestTableActualRows:' +  CAST(@DestTableActualRows AS VARCHAR(30));
		PRINT '@DestTableSizeInGB:' +  CAST(@DestTableSizeInGB AS VARCHAR(30));
		PRINT '@data_compression:' +  @data_compression;
	END

	SELECT @tableFullName as [Table Name], 
		   @SourceTableRowCount as [Row Count],
		   @SourceTableSizeInGB as [SizeInGB],
		   @srcTableCompression as [Compression],
		   @sampleTableFullNameWithDB as [Sampled Table],
		   @indexType as [Type],
		   @DestTableActualRows as [SampledRows],
		   CAST((@DestTableActualRows* 100.) / (CASE WHEN @SourceTableRowCount > 0 THEN @SourceTableRowCount ELSE 1 END) AS DECIMAL(18,2)) as [%],
		   @DestTableSizeInGB as [Size in GB],
		   @data_compression as [NEW Compression], 
		   CAST( @DestTableSizeInGB / ( IIF(@DestTableActualRows<>0,@DestTableActualRows,1) * 1. / CASE WHEN @SourceTableRowCount > 0 THEN @SourceTableRowCount ELSE 1 END ) AS DECIMAL(18,3)) as [Estimated GB],
		   --CAST( (1 - ((@DestTableSizeInGB / ( IIF(@DestTableActualRows<>0,@DestTableActualRows,1) * 1. / CASE WHEN @SourceTableRowCount > 0 THEN @SourceTableRowCount ELSE 1 END ) )  / IIF(@SourceTableSizeInGB > 0.0, @SourceTableSizeInGB, 1) )) * 100 AS DECIMAL(18,2)) as  [Improvement %]
		   CAST( 100 * @SourceTableSizeInGB / ( @DestTableSizeInGB / ( IIF(@DestTableActualRows<>0,@DestTableActualRows,1) * 1. / CASE WHEN @SourceTableRowCount > 0 THEN @SourceTableRowCount ELSE 1 END )  )  - 100
			AS DECIMAL(18,2)) as [Improvement %]

	-- If @showTrimmingInfo is enabled, then show the reasons for trimmed Row Groups
	IF @showTrimmingInfo = 1 
	BEGIN
		SET @trimmingSQL = N'SELECT * FROM ##TrimReasons_' + @NEWID + ';
							 DROP TABLE ##TrimReasons_' + @NEWID + ';';
		EXECUTE sp_executesql @trimmingSQL;
	END
END