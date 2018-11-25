/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with an empty columnstore table 
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyTable' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testEmptyTable] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testEmptyTable] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroupsDetails') IS NOT NULL
		DROP TABLE #ExpectedRowGroupsDetails;

	create table #ExpectedRowGroupsDetails(
		[TableName] nvarchar(256),
		[Location] varchar(15),			
		[Partition] int,
		[row_group_id] int,
		[state] tinyint,
		[state_description] nvarchar(60),
		[total_rows] bigint,
		[deleted_rows] bigint,
		[Size In MB] Decimal(8,3),
		trim_reason tinyint,  
		trim_reason_desc nvarchar(60),
		compress_op tinyint,
		compress_op_desc nvarchar(60),
		optimised bit,
		generation bigint,
		closed_time datetime,
		created_time datetime 
	);

	select top (0) *
		into #ActualRowGroupsDetails
		from #ExpectedRowGroupsDetails;

	-- CCI
	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'EmptyCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';

	-- NCI on HEAP
	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'EmptyNCI_Heap';
	
	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';

	-- NCI on Clustered
	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'EmptyNCI_Clustered';
	
	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';

END

GO
