/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
	Version: 1.4.1, November 2016

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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_schemaName' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_schemaName] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_schemaName] AS
BEGIN
	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	if OBJECT_ID('tempdb..#ActualSuggestedTables') IS NOT NULL
		drop table #ActualSuggestedTables;

	create table #ActualSuggestedTables(
		[Compatible With] varchar(50) NOT NULL,
		[TableLocation] varchar(15) NOT NULL,
		[TableName] nvarchar(1000) NOT NULL,
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

	-- Insert expected result for the 'DB' Schema - the results should be empty
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @schemaName = 'db', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	
	-- ******************************************************************************************************
	-- Insert expected result for the 'DBO' Schema
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @schemaName = 'dbo', @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

END

GO
