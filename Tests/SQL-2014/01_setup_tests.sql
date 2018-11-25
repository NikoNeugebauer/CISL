/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - creates test tables
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

