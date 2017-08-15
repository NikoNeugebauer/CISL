/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Tests - cstore_GetFragmentation is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[testOneDeletedRowGroup] AS
BEGIN
	DROP TABLE IF EXISTS #ExpectedFragmentation

	CREATE TABLE #ExpectedFragmentation(
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
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[OneDeletedRowGroupCCI]', 'CCI_OneDeletedRowGroupCCI', 'Disk-Based', 'CLUSTERED', 1, 100. /*Fragmentation*/, 1, 100., 
				1, 100.0, 0, 1 /*Total Rows*/, 1, 100., 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneDeletedRowGroupCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
END

GO
