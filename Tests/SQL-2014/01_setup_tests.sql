/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - creates test tables
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

create clustered columnstore index NCCI_EmptyNCI_Heap
	on dbo.EmptyNCI_Heap;

-- Nonclustered Columnstore on Clustered Index
if EXISTS (select * from sys.objects where type = 'u' and name = 'EmptyNCI_Clustered' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.EmptyNCI_Clustered;

create table dbo.EmptyNCI_Clustered(
	c1 int );

create clustered columnstore index NCI_EmptyNCI_Clustered
	on dbo.EmptyNCI_Clustered;

-- **************************************************************************************
-- Clustered Columnstore
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowCCI' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowCCI;

create table dbo.OneRowCCI(
	c1 int );

create clustered columnstore index CCI_OneRowCCI
	on dbo.OneRowCCI;

insert into dbo.OneRowCCI
	values (1);

-- Nonclustered Columnstore on HEAP
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowNCI_Heap' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowNCI_Heap;

create table dbo.OneRowNCI_Heap(
	c1 int );

insert into dbo.OneRowNCI_Heap
	values (1);

create nonclustered columnstore index NCI_OneRowNCI_Heap
	on dbo.OneRowNCI_Heap(c1);

-- Nonclustered Columnstore on Clustered Index
if EXISTS (select * from sys.objects where type = 'u' and name = 'OneRowNCI_Clustered' and schema_id = SCHEMA_ID('dbo') )
	drop table dbo.OneRowNCI_Clustered;

create table dbo.OneRowNCI_Clustered(
	c1 int );

insert into dbo.OneRowNCI_Clustered
	values (1);

create nonclustered columnstore index NCI_OneRowNCI_Clustered
	on dbo.OneRowNCI_Clustered(c1);