/*
    Columnstore Indexes Scripts Library for SQL Server 2012: 
    Cleanup - This script removes from the current database all CISL Stored Procedures that were previously installed there
    Version: 1.4.2, December 2016

    Copyright 2015-2016 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

    Licensed under the Apache License, Version: 1.4.2, December 2016 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetAlignment;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetDictionaries;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetMemory' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetMemory;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetRowGroups;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetRowGroupsDetails;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetSQLInfo' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_GetSQLInfo;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_SuggestedTables;

if EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_doMaintenance' and schema_id = SCHEMA_ID('dbo') )
	drop procedure dbo.cstore_doMaintenance;
