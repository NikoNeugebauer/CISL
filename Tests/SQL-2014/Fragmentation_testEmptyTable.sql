/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with an empty columnstore table 
	Version: 1.3.1, August 2016

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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyTable' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[testEmptyTable] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[testEmptyTable] AS
BEGIN
	if EXISTS (select * from sys.objects where type = 'u' and name = 'TestCase1' and schema_id = SCHEMA_ID('dbo') )
		drop table dbo.TestCase1;

	create table dbo.TestCase1(
		c1 int );

	create clustered columnstore index CCI_TestCase1
		on dbo.TestCase1;

	/* ********************************************************************************** */
	IF OBJECT_ID('tempdb..#ExpectedFragmentation') IS NOT NULL
		DROP TABLE #ExpectedFragmentation

	create table #ExpectedFragmentation(
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

	select top (0) *
		into #ActualFragmentation
		from #ExpectedFragmentation;

	-- CCI
	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'TestCase1';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';

	-- NCI on HEAP
	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'EmptyNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';

	-- NCI on Clustered
	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'EmptyNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
END

GO
