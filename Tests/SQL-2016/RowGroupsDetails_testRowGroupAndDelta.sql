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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testRowGroupAndDelta] AS
BEGIN
	DROP TABLE IF EXISTS #ExpectedRowGroupsDetails;

	CREATE TABLE #ExpectedRowGroupsDetails(
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
	-- Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[RowGroupAndDeltaCCI]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 0, 0.0 /*Size in MB*/,
				6, 'RESIDUAL_ROW_GROUP', 2, 'INDEX_BUILD', 1, 1, NULL, GetDate()
		union all
		select '[dbo].[RowGroupAndDeltaCCI]', 'Disk-Based', 1, 1, 1, 'OPEN', 1, 0, 0.016 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'RowGroupAndDelta';

	update top (2) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (2) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';
	TRUNCATE TABLE #ExpectedRowGroupsDetails;
	TRUNCATE TABLE #ActualRowGroupsDetails;

	-- NCI on HEAP
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Heap]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 0, 0.0 /*Size in MB*/,
				6, 'RESIDUAL_ROW_GROUP', 2, 'INDEX_BUILD', 1, 1, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'OneRowNCI_Heap';

	update top (1) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (1) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';
	TRUNCATE TABLE #ExpectedRowGroupsDetails;
	TRUNCATE TABLE #ActualRowGroupsDetails;


	-- NCI on Clustered
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Clustered]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 0, 0.0 /*Size in MB*/,
				6, 'RESIDUAL_ROW_GROUP', 2, 'INDEX_BUILD', 1, 1, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'OneRowNCI_Clustered';

	update top (1) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (1) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';
	TRUNCATE TABLE #ExpectedRowGroupsDetails;
	TRUNCATE TABLE #ActualRowGroupsDetails;
END

GO
