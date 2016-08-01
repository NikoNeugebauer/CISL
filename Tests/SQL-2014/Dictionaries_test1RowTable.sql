/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with the columnstore table containing 1 row
	Version: 1.3.1, July 2016

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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'test1RowTable' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[test1RowTable] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[test1RowTable] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedDictionaries') IS NOT NULL
		DROP TABLE #ExpectedDictionaries;

	create table #ExpectedDictionaries(
		TableName nvarchar(256),
		Type varchar(12),
		[Location] varchar(10),			
		[Partition] int,
		RowGroups int,
		Dictionaries int,
		EntriesCount bigint,
		[Rows Serving] bigint,
		[Total Size in MB] Decimal(8,3),
		[Max Global Size in MB] Decimal(8,3),
		[Max Local Size in MB] Decimal(8,3)
	);

	select top (0) *
		into #ActualDictionaries
		from #ExpectedDictionaries;

	-- CCI
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'CCI_OneRowCCI', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

	-- NCI on HEAP
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'OneRowNCI_Heap', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

	-- NCI on Clustered
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'OneRowNCI_Clustered', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';
END

GO
