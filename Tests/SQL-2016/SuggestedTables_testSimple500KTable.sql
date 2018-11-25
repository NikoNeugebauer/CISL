/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable] AS
BEGIN
	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	drop table if exists #ActualSuggestedTables;

	create table #ActualSuggestedTables(
		[Compatible With] varchar(50) NOT NULL,
		[TableLocation] varchar(15) NOT NULL,
		[TableName] nvarchar(1000) NOT NULL,
		[Partitions] INT NOT NULL,
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

	select top (0) *
		into #ExpectedSuggestedTables
		from #ActualSuggestedTables;	

	-- Insert expected result for 499999 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minRowsToConsider = 499999, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 500000 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minRowsToConsider = 500000, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 500001 rows - the results should be empty :) 

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minRowsToConsider = 500001, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 0.005 GB
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.005, @tableName = 'SuggestedTables_Test1';

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 0.006 GB
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.006, @tableName = 'SuggestedTables_Test1';

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 0.007 GB - the results should be empty
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.007, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'DB' Schema - the results should be empty
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @schemaName = 'dbx', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	
	-- ******************************************************************************************************
	-- Insert expected result for the 'DBO' Schema
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @schemaName = 'dbo', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Based', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Base' Index Location (Wrong Name)

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Base', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'In-Memory' Index Location (Wrong Location)

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @indexLocation = 'In-Memory', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 1, 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;


END

GO
