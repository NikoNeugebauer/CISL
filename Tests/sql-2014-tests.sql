/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - creates new tsqlt test classes for the CISL
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


EXEC tSQLt.NewTestClass 'BasicTests';

-- Create a separate class for each of the CISL functionalities
EXEC tSQLt.NewTestClass 'Alignment';

EXEC tSQLt.NewTestClass 'Dictionaries';

EXEC tSQLt.NewTestClass 'Memory';

EXEC tSQLt.NewTestClass 'Fragmentation';

EXEC tSQLt.NewTestClass 'RowGroups';

EXEC tSQLt.NewTestClass 'RowGroupsDetails';

EXEC tSQLt.NewTestClass 'SuggestedTables';

-- Installation tests
EXEC tSQLt.NewTestClass 'Installation';

-- Cleanup tests
EXEC tSQLt.NewTestClass 'Cleanup';
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - creates test tables
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

-- Clustered Columnstore
if EXISTS (select * from sys.objects where type = 'u' and name = 'EmptyCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.EmptyCCI;

create table dbo.EmptyCCI(
	c1 int );

create clustered columnstore index CCI_EmptyCCI
	on dbo.EmptyCCI;

-- Nonclustered Columnstore on HEAP
if EXISTS (select * from sys.objects where type = 'u' and name = 'EmptyNCI_Heap' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.EmptyNCI_Heap;

create table dbo.EmptyNCI_Heap(
	c1 int );

create nonclustered columnstore index CCI_EmptyNCI_Heap
	on dbo.EmptyNCI_Heap (c1);

-- Nonclustered Columnstore on Clustered Index
if EXISTS (select * from sys.objects where type = 'u' and name = 'EmptyNCI_Clustered' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.EmptyNCI_Clustered;

create table dbo.EmptyNCI_Clustered(
	c1 int not null primary key clustered );

create nonclustered columnstore index NCI_EmptyNCI_Clustered
	on dbo.EmptyNCI_Clustered (c1);

-- **************************************************************************************
-- Clustered Columnstore
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowCCI;

create table dbo.OneRowCCI(
	c1 int );

create clustered columnstore index CCI_OneRowCCI
	on dbo.OneRowCCI;

insert into dbo.OneRowCCI
	values (1)

-- Nonclustered Columnstore on HEAP
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowNCI_Heap' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowNCI_Heap;

create table dbo.OneRowNCI_Heap(
	c1 int );

insert into dbo.OneRowNCI_Heap
	values (1)
option( recompile );

create nonclustered columnstore index NCI_OneRowNCI_Heap
	on dbo.OneRowNCI_Heap(c1);

-- Nonclustered Columnstore on Clustered Index
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowNCI_Clustered' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowNCI_Clustered;

create table dbo.OneRowNCI_Clustered(
	c1 int not null primary key clustered );

insert into dbo.OneRowNCI_Clustered
	values (1)
option( recompile );

create nonclustered columnstore index NCI_OneRowNCI_Clustered
	on dbo.OneRowNCI_Clustered(c1);

-- **************************************************************************************
-- Clustered Columnstore table with an Empty Delta Store
if EXISTS (select * from sys.objects where type = 'u' and name = 'EmptyDeltaStoreCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.EmptyDeltaStoreCCI;

create table dbo.EmptyDeltaStoreCCI(
	c1 int );

create clustered columnstore index CCI_EmptyDeltaStoreCCI
	on dbo.EmptyDeltaStoreCCI;

insert into dbo.EmptyDeltaStoreCCI
	values (1);

delete from dbo.EmptyDeltaStoreCCI;

alter index CCI_EmptyDeltaStoreCCI
	on dbo.EmptyDeltaStoreCCI
		Reorganize;

-- **************************************************************************************
-- Clustered Columnstore table with 2 Row Groups - 1 compressed with 1 Row and 1 Delta Store containing 1 row

if EXISTS (select * from sys.objects where type = 'u' and name = 'RowGroupAndDeltaCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.RowGroupAndDeltaCCI;

create table dbo.RowGroupAndDeltaCCI(
	c1 int );

create clustered columnstore index CCI_RowGroupAndDeltaCCI
	on dbo.RowGroupAndDeltaCCI;

insert into dbo.RowGroupAndDeltaCCI
	values (1);

alter table dbo.RowGroupAndDeltaCCI
	rebuild;

insert into dbo.RowGroupAndDeltaCCI
	values (2);

-- **************************************************************************************
-- Clustered Columnstore table with 1 compressed Row Group containing 1 row that is deleted

if EXISTS (select * from sys.objects where type = 'u' and name = 'OneDeletedRowGroupCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneDeletedRowGroupCCI;

create table dbo.OneDeletedRowGroupCCI(
	c1 int );

create clustered columnstore index CCI_OneDeletedRowGroupCCI
	on dbo.OneDeletedRowGroupCCI;

insert into dbo.OneDeletedRowGroupCCI
	values (1);

alter table dbo.OneDeletedRowGroupCCI
	rebuild;

delete from dbo.OneDeletedRowGroupCCI
	where c1 = 1;

-- **************************************************************************************
-- Suggested Tables scenario Test 1: 500.000 rows in a simple table with 1 integer column

if EXISTS (select * from sys.objects where type = 'u' and name = 'SuggestedTables_Test1' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.SuggestedTables_Test1;

create table dbo.SuggestedTables_Test1(
	c1 int identity(1,1) not null
) WITH (DATA_COMPRESSION = PAGE);

set nocount on;
declare @i as int;
declare @max as int;
select @max = isnull(max(C1),0) from dbo.SuggestedTables_Test1;
set @i = 1;

begin tran
while @i <= 500000
begin
	insert into dbo.SuggestedTables_Test1
		default values

	set @i+=1;
end;
commit;

/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with the columnstore table containing 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'test1RowTable' and schema_id = SCHEMA_ID('Alignment') )
	exec ('create procedure [Alignment].[test1RowTable] as select 1');
GO

ALTER PROCEDURE [Alignment].[test1RowTable] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedAlignment') IS NOT NULL
		DROP TABLE #ExpectedAlignment;

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
	-- Insert expected result
	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'OneRowCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';


	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedAlignment
		(TableName, Location, Partition, [ColumnId], ColumnName, ColumnType, [SegmentElimination], [DealignedSegments], [TotalSegments], SegmentAlignment)
		values 
		('[dbo].[OneRowNCI_Heap]', 'Disk-Based', 1,	1, 'c1', 'int', 'OK', 0, 1,	100.00 );

	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'OneRowNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';
	TRUNCATE TABLE #ExpectedAlignment;
	TRUNCATE TABLE #ActualAlignment;


	-- NCI on Clustered
	-- Insert expected result
	insert into #ExpectedAlignment
		(TableName, Location, Partition, [ColumnId], ColumnName, ColumnType, [SegmentElimination], [DealignedSegments], [TotalSegments], SegmentAlignment)
		values 
		('[dbo].[OneRowNCI_Clustered]', 'Disk-Based', 1, 1, 'c1', 'int', 'OK', 0, 1,	100.00 );

	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'OneRowNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';
	TRUNCATE TABLE #ExpectedAlignment;
	TRUNCATE TABLE #ActualAlignment;
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with an empty delta-store at the columnstore table 
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('Alignment') )
	exec ('create procedure [Alignment].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [Alignment].[testEmptyDeltaStore] AS
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
		exec dbo.cstore_GetAlignment @tableName = 'EmptyDeltaStoreCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';


END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with an empty columnstore table 
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('Alignment') )
	exec ('create procedure [Alignment].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [Alignment].[testOneDeletedRowGroup] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedAlignment') IS NOT NULL
		DROP TABLE #ExpectedAlignment;

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
	-- Insert expected result
	insert into #ExpectedAlignment
		(TableName, Location, Partition, [ColumnId], ColumnName, ColumnType, [SegmentElimination], [DealignedSegments], [TotalSegments], SegmentAlignment)
		values 
		('[dbo].[OneDeletedRowGroupCCI]', 'Disk-Based', 1,	1, 'c1', 'int', 'OK', 0, 1,	100.00 );

	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'OneDeletedRowGroupCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';


	
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetAlignment is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('Alignment') )
	exec ('create procedure [Alignment].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [Alignment].[testRowGroupAndDelta] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedAlignment') IS NOT NULL
		DROP TABLE #ExpectedAlignment;

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
	-- Insert expected result
	insert into #ExpectedAlignment
		(TableName, Location, Partition, [ColumnId], ColumnName, ColumnType, [SegmentElimination], [DealignedSegments], [TotalSegments], SegmentAlignment)
		values 
		('[dbo].[RowGroupAndDeltaCCI]', 'Disk-Based', 1, 1, 'c1', 'int', 'OK', 0, 1, 100.00 );

	insert into #ActualAlignment 
		exec dbo.cstore_GetAlignment @tableName = 'RowGroupAndDelta';

	exec tSQLt.AssertEqualsTable '#ExpectedAlignment', '#ActualAlignment';

END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with the columnstore table containing 1 row
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with an empty columnstore table 
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[testEmptyDeltaStore] AS
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
		exec dbo.cstore_GetDictionaries @tableName = 'EmptyDeltaStoreCCI', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with an empty columnstore table 
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyTable' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[testEmptyTable] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[testEmptyTable] AS
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
		exec dbo.cstore_GetDictionaries @tableName = 'EmptyCCI', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

	-- NCI on HEAP
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'EmptyNCI_Heap', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

	-- NCI on Clustered
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'EmptyNCI_Clustered', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[testOneDeletedRowGroup] AS
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
		exec dbo.cstore_GetDictionaries @tableName = 'OneDeletedRowGroupCCI', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetDictionaries is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[testRowGroupAndDelta] AS
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
		exec dbo.cstore_GetDictionaries @tableName = 'RowGroupAndDelta', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with the columnstore table containing 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'test1RowTable' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[test1RowTable] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[test1RowTable] AS
BEGIN
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
	-- Insert expected result
	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneRowCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';

	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[OneRowNCI_Heap]', 'NCI_OneRowNCI_Heap', 'Disk-Based', 'NONCLUSTERED', 1, 0 /*Fragmentation*/, 0, 0, 
				1, 100.0, 1, 1 /*Total Rows*/, 0, 0, 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneRowNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
	TRUNCATE TABLE #ExpectedFragmentation;
	TRUNCATE TABLE #ActualFragmentation;


	-- NCI on Clustered
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[OneRowNCI_Clustered]', 'NCI_OneRowNCI_Clustered', 'Disk-Based', 'NONCLUSTERED', 1, 0 /*Fragmentation*/, 0, 0, 
				1, 100.0, 1, 1 /*Total Rows*/, 0, 0, 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneRowNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
	TRUNCATE TABLE #ExpectedFragmentation;
	TRUNCATE TABLE #ActualFragmentation;
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[testEmptyDeltaStore] AS
BEGIN
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
	-- Insert expected result
	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'EmptyDeltaStoreCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';

	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[OneRowNCI_Heap]', 'NCI_OneRowNCI_Heap', 'Disk-Based', 'NONCLUSTERED', 1, 0 /*Fragmentation*/, 0, 0, 
				1, 100.0, 1, 1 /*Total Rows*/, 0, 0, 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneRowNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
	TRUNCATE TABLE #ExpectedFragmentation;
	TRUNCATE TABLE #ActualFragmentation;


	-- NCI on Clustered
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[OneRowNCI_Clustered]', 'NCI_OneRowNCI_Clustered', 'Disk-Based', 'NONCLUSTERED', 1, 0 /*Fragmentation*/, 0, 0, 
				1, 100.0, 1, 1 /*Total Rows*/, 0, 0, 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'OneRowNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
	TRUNCATE TABLE #ExpectedFragmentation;
	TRUNCATE TABLE #ActualFragmentation;
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with an empty columnstore table 
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[testOneDeletedRowGroup] AS
BEGIN
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetFragmentation is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('Fragmentation') )
	exec ('create procedure [Fragmentation].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [Fragmentation].[testRowGroupAndDelta] AS
BEGIN
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
	-- Insert expected result
	insert into #ExpectedFragmentation (TableName, IndexName, Location, IndexType, Partition, Fragmentation, DeletedRGs, DeletedRGsPerc, 
										TrimmedRGs, TrimmedRGsPerc, AvgRows, TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups)
		select '[dbo].[RowGroupAndDeltaCCI]', 'CCI_RowGroupAndDeltaCCI', 'Disk-Based', 'CLUSTERED', 1, 0 /*Fragmentation*/, 0, 0, 
				1, 100.0, 1, 1 /*Total Rows*/, 0, 0, 1;

	insert into #ActualFragmentation 
		exec dbo.cstore_GetFragmentation @tableName = 'RowGroupAndDelta';

	exec tSQLt.AssertEqualsTable '#ExpectedFragmentation', '#ActualFragmentation';
END

GO
exec dbo.cstore_SuggestedTables @minRowsToConsider = 499999, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minRowsToConsider = 500000, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minRowsToConsider = 500001, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.005, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.006, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.007, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @schemaName = 'db', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @schemaName = 'dbo', @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Based', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @indexLocation = 'Disk-Base', @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @indexLocation = 'In-Memory', @tableName = 'SuggestedTables_Test1'
--------
exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 1, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 0, @tableName = 'SuggestedTables_Test1'

--------
exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 1, @tableName = 'SuggestedTables_Test1'

exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 0, @tableName = 'SuggestedTables_Test1'



-- showUnsupportedColumnsDetails
-- columnstoreIndexTypeForTSQL
-- updateMemoryOptimisedStats


-- Min Rows
-- Min Size in GB
-- Schema Name
-- Table Name
-- Index Location 
-- considerColumnsOver8K
-- showReadyTablesOnly
-- showUnsupportedColumnsDetails
-- columnstoreIndexTypeForTSQL
-- updateMemoryOptimisedStats

-- Row Count
-- Min RowGroups
-- Cols Count
-- Sum Length
-- Unsupported
-- LOBs
-- Computed
-- Clustered Index
-- Nonclustered Index
-- XML Indexes
-- Spatial Indexes
-- Primary Key
-- Foreign Keys
-- Unique Constraints
-- Triggers
-- RCSI
-- Snapshot
-- CDC
-- CT
-- InMemoryOLTP
-- Replication
-- FileStream
-- FileTable



	--@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
	--@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
	--@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
	--@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
	--@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
	--@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
	--@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
	--@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
	--@showTSQLCommandsBeta bit = 0,								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
	--@columnstoreIndexTypeForTSQL varchar(20) = 'Clustered',		-- Allows to define the type of Columnstore Index to be created eith possible values of 'Clustered' and 'Nonclustered'
	--@updateMemoryOptimisedStats bit = 0							-- Allows statistics update on the InMemory tables, since they are stalled within SQL Server 2014
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroups is tested with the columnstore table containing 1 row
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
		[Location] varchar(15),
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



	-- CCI
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowCCI]', 'Clustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 1, 0, 
				0, 1, 0.0 /*Del Rows*/, 0.000001 /*Active Rows*/, 0.000001 /*Total Rows*/, 0.0, 0, 1, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'OneRowCCI';

	update #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	TRUNCATE TABLE #ExpectedRowGroups;
	TRUNCATE TABLE #ActualRowGroups;

	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Heap]', 'Nonclustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
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
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Clustered]', 'Nonclustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroups is tested with an empty columnstore table 
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('RowGroups') )
	exec ('create procedure [RowGroups].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [RowGroups].[testEmptyDeltaStore] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroups') IS NOT NULL
		DROP TABLE #ExpectedRowGroups;

	create table #ExpectedRowGroups(
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Location] varchar(15),
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

	-- CCI
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyDeltaStoreCCI]', 'Clustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 1, 0, 
				0, 1, 0.0 /*Del Rows*/, 0.000000 /*Active Rows*/, 0.000000 /*Total Rows*/, 0.0, 0, 1, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'EmptyDeltaStoreCCI';

	update #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';


END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroups is tested with an empty columnstore table 
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyTable' and schema_id = SCHEMA_ID('RowGroups') )
	exec ('create procedure [RowGroups].[testEmptyTable] as select 1');
GO

ALTER PROCEDURE [RowGroups].[testEmptyTable] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroups') IS NOT NULL
		DROP TABLE #ExpectedRowGroups;

	create table #ExpectedRowGroups(
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Location] varchar(15),
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

	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyCCI]', 'Clustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
				0, 0, 0, 0.0 /*Del Rows*/, 0.0, 0.0 /*Total Rows*/, 0, 0, NULL;

	-- CCI
	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'EmptyCCI';

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	truncate table #ActualRowGroups;
	truncate table #ExpectedRowGroups;

	-- NCI on HEAP
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyNCI_Heap]', 'Nonclustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
				0, 0, 0, 0.0 /*Del Rows*/, 0.0, 0.0 /*Total Rows*/, 0, 0, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'EmptyNCI_Heap';

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	truncate table #ActualRowGroups;
	truncate table #ExpectedRowGroups;

	--- NCI on Clustered
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyNCI_Clustered]', 'Nonclustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
				0, 0, 0, 0.0 /*Del Rows*/, 0.0, 0.0 /*Total Rows*/, 0, 0, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'EmptyNCI_Clustered';

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroups is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('RowGroups') )
	exec ('create procedure [RowGroups].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [RowGroups].[testOneDeletedRowGroup] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroups') IS NOT NULL
		DROP TABLE #ExpectedRowGroups;

	create table #ExpectedRowGroups(
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Location] varchar(15),
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

	-- CCI
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneDeletedRowGroupCCI]', 'Clustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 0, 0, 
				1, 1, 0.000001 /*Del Rows*/, 0.000000 /*Active Rows*/, 0.000001 /*Total Rows*/, 0.0, 0, 1, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'OneDeletedRowGroupCCI';

	update top(1) #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update  top(1) #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';
	
END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroups is tested with the columnstore table containing 1 row in compressed row group and a Delta-Store with 1 row 
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('RowGroups') )
	exec ('create procedure [RowGroups].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [RowGroups].[testRowGroupAndDelta] AS
BEGIN
	IF OBJECT_ID('tempdb..#ExpectedRowGroups') IS NOT NULL
		DROP TABLE #ExpectedRowGroups;

	create table #ExpectedRowGroups(
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[Location] varchar(15),
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

	-- CCI
	-- Insert expected result
	insert into #ExpectedRowGroups (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
									[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[RowGroupAndDeltaCCI]', 'Clustered', 'Disk-Based', 1, 'COLUMNSTORE', 0, 1, 0, 
				1, 2, 0.0 /*Del Rows*/, 0.000002 /*Active Rows*/, 0.000002 /*Total Rows*/, 0.0, 0, 1, NULL;

	insert into #ActualRowGroups
		exec dbo.cstore_GetRowGroups @tableName = 'RowGroupAndDelta';

	update #ExpectedRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;
	update #ActualRowGroups
		set Scans = NULL, Updates = NULL, LastScan = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroups', '#ActualRowGroups';


END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with the columnstore table containing 1 row
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'test1RowTable' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[test1RowTable] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[test1RowTable] AS
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
	-- Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowCCI]', 'Disk-Based', 1, 0, 1, 'OPEN', 1, NULL, 0.0 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'OneRowCCI';

	update top (1) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (1) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';
	TRUNCATE TABLE #ExpectedRowGroupsDetails;
	TRUNCATE TABLE #ActualRowGroupsDetails;

	-- NCI on HEAP
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneRowNCI_Heap]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 0, 0.0 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

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
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with an empty delta-store at the columnstore table 
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

if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testEmptyDeltaStore' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testEmptyDeltaStore] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testEmptyDeltaStore] AS
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

	-- CCI Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[EmptyDeltaStoreCCI]', 'Disk-Based', 1, 0, 1, 'OPEN', 0, NULL, 0.0 /*Size in MB*/,
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with an empty columnstore table 
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
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - cstore_GetRowGroupsDetails is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testOneDeletedRowGroup] AS
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
	-- Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[OneDeletedRowGroupCCI]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 1, 0.0 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

	insert into #ActualRowGroupsDetails
		exec dbo.cstore_GetRowGroupsDetails @tableName = 'OneDeletedRowGroupCCI';

	update top (1) #ExpectedRowGroupsDetails
		set created_time = NULL;

	update top (1) #ActualRowGroupsDetails
		set created_time = NULL;

	exec tSQLt.AssertEqualsTable '#ExpectedRowGroupsDetails', '#ActualRowGroupsDetails';
	
END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testRowGroupAndDelta' and schema_id = SCHEMA_ID('RowGroupsDetails') )
	exec ('create procedure [RowGroupsDetails].[testRowGroupAndDelta] as select 1');
GO

ALTER PROCEDURE [RowGroupsDetails].[testRowGroupAndDelta] AS
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
	-- Insert expected result
	insert into #ExpectedRowGroupsDetails
						-- (TableName, Type, Location, Partition, [Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores],
							--		[Compressed RowGroups], [Total RowGroups], [Deleted Rows], [Active Rows], [Total Rows], [Size in GB], [Scans], [Updates],  [LastScan])
		select '[dbo].[RowGroupAndDeltaCCI]', 'Disk-Based', 1, 0, 3, 'COMPRESSED', 1, 0, 0.0 /*Size in MB*/,
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate()
		union all
		select '[dbo].[RowGroupAndDeltaCCI]', 'Disk-Based', 1, 1, 1, 'OPEN', 1, NULL, 0.0 /*Size in MB*/,
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
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

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
				NULL, NULL, NULL, NULL, NULL, NULL, NULL, GetDate();

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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable] AS
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

	-- Insert expected result for 499999 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minRowsToConsider = 499999, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 500000 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.005, @tableName = 'SuggestedTables_Test1';

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 0.006 GB
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;


END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_considerColumnsOver8K' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_considerColumnsOver8K] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_considerColumnsOver8K] AS
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

		-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @considerColumnsOver8K = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;


END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_indexLocation' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_indexLocation] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_indexLocation] AS
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

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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

END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_minRowsToConsider' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_minRowsToConsider] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_minRowsToConsider] AS
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

	-- Insert expected result for 499999 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minRowsToConsider = 499999, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 500000 rows
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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


END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_minSizeToConsiderInGB' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_minSizeToConsiderInGB] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_minSizeToConsiderInGB] AS
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

	-- ******************************************************************************************************
	-- Insert expected result for 0.005 GB
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;

	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @minSizeToConsiderInGB = 0.005, @tableName = 'SuggestedTables_Test1';

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for 0.006 GB
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
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



END

GO
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testSimple500KTable_showReadyTablesOnly' and schema_id = SCHEMA_ID('SuggestedTables') )
	exec ('create procedure [SuggestedTables].[testSimple500KTable_showReadyTablesOnly] as select 1');
GO

ALTER PROCEDURE [SuggestedTables].[testSimple500KTable_showReadyTablesOnly] AS
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

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 1, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;

	-- ******************************************************************************************************
	-- Insert expected result for the 'Disk-Based' Index Location
	insert into #ExpectedSuggestedTables
		select 'Nonclustered Columnstore', 'Disk-Based', '[dbo].[SuggestedTables_Test1]', 500000, 1, 0.006,	1, 0, 4,
				0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0;
	
	insert into #ActualSuggestedTables
		exec dbo.cstore_SuggestedTables @showReadyTablesOnly = 0, @tableName = 'SuggestedTables_Test1'

	exec tSQLt.AssertEqualsTable '#ExpectedSuggestedTables', '#ActualSuggestedTables';
	TRUNCATE TABLE #ExpectedSuggestedTables;
	TRUNCATE TABLE #ActualSuggestedTables;


END

GO
/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - Executes all configured tests
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


exec [tSQLt].[RunAll]
