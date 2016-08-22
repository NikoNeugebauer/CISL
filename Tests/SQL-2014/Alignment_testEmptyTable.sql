/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with an empty columnstore table 
	Version: 1.3.1, August 2016

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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyTable' and schema_id = SCHEMA_ID('Alignment') )
	exec ('create procedure [Alignment].[testEmptyTable] as select 1');
GO

ALTER PROCEDURE [Alignment].[testEmptyTable] AS
BEGIN
		create table #ExpectedAlignment(
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
	);

	select top (0) *
		into #ActualAlignment
		from #ExpectedAlignment;

	-- CCI
	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'EmptyCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';


	-- NCI on HEAP
	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'EmptyNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';

	-- NCI on Clustered
	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'EmptyNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';
	TRUNCATE TABLE #ExpectedAlignment;
	TRUNCATE TABLE #ActualAlignment;
END

GO
