/*
    Columnstore Indexes Scripts Library for Azure SQL Database: 
    Cleanup - This script removes from the current database all CISL Stored Procedures that were previously installed there
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

drop procedure if exists dbo.cstore_GetAlignment;

drop procedure if exists dbo.cstore_GetDictionaries;

drop procedure if exists dbo.cstore_GetFragmentation;

drop procedure if exists dbo.cstore_GetMemory;

drop procedure if exists dbo.cstore_GetRowGroups;

drop procedure if exists dbo.cstore_GetRowGroupsDetails;

drop procedure if exists dbo.cstore_SuggestedTables;

drop procedure if exists dbo.cstore_doMaintenance;

drop table if exists [dbo].[cstore_Clustering];

drop table if exists [dbo].[cstore_MaintenanceData_Log];

drop table if exists [dbo].[cstore_Operation_Log];