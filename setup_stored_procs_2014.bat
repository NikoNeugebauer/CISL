REM 
REM CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
REM Columnstore Tests - Prepares all Stored Procedures as well as their tests to be delivered in a single file in the respective folders
REM Version: 1.3.0, July 2016
REM 
REM Copyright 2015 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)
REM 
REM Licensed under the Apache License, Version 2.0 (the "License");
REM you may not use this file except in compliance with the License.
REM You may obtain a copy of the License at
REM 
REM      http://www.apache.org/licenses/LICENSE-2.0
REM 
REM   Unless required by applicable law or agreed to in writing, software
REM   distributed under the License is distributed on an "AS IS" BASIS,
REM   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
REM   See the License for the specific language governing permissions and
REM   limitations under the License.
REM 

type SQL-2014\StoredProcs\cstore_GetAlignment.sql SQL-2014\StoredProcs\cstore_GetDictionaries.sql SQL-2014\StoredProcs\cstore_GetFragmentation.sql SQL-2014\StoredProcs\cstore_GetMemory.sql SQL-2014\StoredProcs\cstore_GetRowGroups.sql SQL-2014\StoredProcs\cstore_GetRowGroupsDetails.sql SQL-2014\StoredProcs\cstore_GetSQLInfo.sql SQL-2014\StoredProcs\cstore_SuggestedTables.sql SQL-2014\StoredProcs\cstore_doMaintenance.sql > SQL-2014\StoredProcs\cstore_install_all_stored_procs.sql

type Tests\SQL-2014\*.sql > Tests\sql-2014-tests.sql