#	CISL - Columnstore Indexes Scripts Library for SQL Server
#	Powershell Script to setup the Stored Procedures & Tests for the CISL
#	Version: 1.3.1, August 2016
#
#	Copyright 2015-2016 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)
#
#	Licensed under the Apache License, Version 2.0 (the "License");
#	you may not use this file except in compliance with the License.
#	You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific lan guage governing permissions and
#    limitations under the License.

###############################################################################
# SQL Server 2012
Get-Content SQL-2012\StoredProcs\cstore_GetAlignment.sql, SQL-2012\StoredProcs\cstore_GetDictionaries.sql, `
           SQL-2012\StoredProcs\cstore_GetMemory.sql, SQL-2012\StoredProcs\cstore_GetRowGroups.sql,  `
           SQL-2012\StoredProcs\cstore_GetRowGroupsDetails.sql  , SQL-2012\StoredProcs\cstore_GetSQLInfo.sql,  `
           SQL-2012\StoredProcs\cstore_SuggestedTables.sql | `
    Set-Content SQL-2012\StoredProcs\cstore_install_all_stored_procs.sql


###############################################################################
# SQL Server 2014
Get-Content SQL-2014\StoredProcs\cstore_GetAlignment.sql, SQL-2014\StoredProcs\cstore_GetDictionaries.sql, `
            SQL-2014\StoredProcs\cstore_GetFragmentation.sql, SQL-2014\StoredProcs\cstore_GetMemory.sql, `
            SQL-2014\StoredProcs\cstore_GetRowGroups.sql, SQL-2014\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            SQL-2014\StoredProcs\cstore_GetSQLInfo.sql, SQL-2014\StoredProcs\cstore_SuggestedTables.sql, `
            SQL-2014\StoredProcs\cstore_doMaintenance.sql |
    Set-Content SQL-2014\StoredProcs\cstore_install_all_stored_procs.sql

# Unit Tests for SQL Server 2014
Get-Content Tests\SQL-2014\*.sql | Set-Content Tests\sql-2014-tests.sql

###############################################################################
# SQL Server 2016
Get-Content SQL-2016\StoredProcs\cstore_GetAlignment.sql, SQL-2016\StoredProcs\cstore_GetDictionaries.sql, `
            SQL-2016\StoredProcs\cstore_GetFragmentation.sql, SQL-2016\StoredProcs\cstore_GetMemory.sql, `
            SQL-2016\StoredProcs\cstore_GetRowGroups.sql, SQL-2016\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            SQL-2016\StoredProcs\cstore_GetSQLInfo.sql, SQL-2016\StoredProcs\cstore_SuggestedTables.sql, `
            SQL-2016\StoredProcs\cstore_doMaintenance.sql |
    Set-Content SQL-2016\StoredProcs\cstore_install_all_stored_procs.sql

###############################################################################
# Azure SQL Database
Get-Content Azure\StoredProcs\cstore_GetAlignment.sql, Azure\StoredProcs\cstore_GetDictionaries.sql, `
            Azure\StoredProcs\cstore_GetFragmentation.sql,  `
            Azure\StoredProcs\cstore_GetRowGroups.sql, Azure\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            Azure\StoredProcs\cstore_SuggestedTables.sql, `
            Azure\StoredProcs\cstore_doMaintenance.sql |
    Set-Content Azure\StoredProcs\cstore_install_all_stored_procs.sql
 