/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with an empty delta-store at the columnstore table 
	Version: 1.4.2, December 2016

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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testEmptyDeltaStore] AS
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

	-- CCI Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyDeltaStoreCCI]', 'Disk-Based', 1, 0, 1, 'OPEN', 0, 0, 0.016 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'EmptyDeltaStoreCCI';

	update top (1) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (1) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';

END

GO
