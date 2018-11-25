/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2012: 
	Columnstore Tests - cstore_GetRowGroups is tested with the columnstore table containing 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'test1RowTable' and schema_id = SCHEMA_ID('RowGroups') )
	exec ('create procedure [RowGroups].[test1RowTable] as select 1');
GO

ALTER PROCEDURE [RowGroups].[test1RowTable] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroups') IS NOT NULL
		DROP TABLE #ExpectedRowGroups;

	create table #ExpectedRowGroups(
		[TableName] nvarchar(256),
		[Type] varchar(20),
		--[Location] varchar(15),
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

	select top (0) *
		into #ActualRowGroups
		from #ExpectedRowGroups;


	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type,/* Location,*/ Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Heap]', 'Nonclustered',/* 'Disk-Based',*/ 1, 'COLUMNSTORE', 0, 0, 0, 
				1, 1 /*Total*/, 0.0 /*Del Rows*/, 0.000001 /*Active Rows*/, 0.000001 /*Total Rows*/, 0.0, 0, 0, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'OneRowNCI_Heap';

	update #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	TRUNCATE TABLE #ExpectedRowGroups;
	TRUNCATE TABLE #ActualRowGroups;


	-- NCI on Clustered
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type,/* Location,*/ Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Clustered]', 'Nonclustered',/* 'Disk-Based',*/ 1, 'COLUMNSTORE', 0, 0, 0, 
				1, 1 /*Total*/, 0.0 /*Del Rows*/, 0.000001 /*Active Rows*/, 0.000001 /*Total Rows*/, 0.0, 0, 0, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'OneRowNCI_Clustered';

	update #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	TRUNCATE TABLE #ExpectedRowGroups;
	TRUNCATE TABLE #ActualRowGroups;
END

GO
