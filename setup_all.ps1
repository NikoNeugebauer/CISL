#	CISL - Columnstore Indexes Scripts Library for SQL Server
#	Powershell Script to setup the Stored Procedures & Tests for the CISL
#	Version: 1.4.1, November 2016
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

$scriptRootPath = Split-Path -Parent $PSCommandPath

###############################################################################
# SQL Server 2012
Get-Content $scriptRootPath\SQL-2012\StoredProcs\cstore_GetAlignment.sql, $scriptRootPath\SQL-2012\StoredProcs\cstore_GetDictionaries.sql, `
           $scriptRootPath\SQL-2012\StoredProcs\cstore_GetMemory.sql, $scriptRootPath\SQL-2012\StoredProcs\cstore_GetRowGroups.sql,  `
           $scriptRootPath\SQL-2012\StoredProcs\cstore_GetRowGroupsDetails.sql, $scriptRootPath\SQL-2012\StoredProcs\cstore_GetSQLInfo.sql,  `
           $scriptRootPath\SQL-2012\StoredProcs\cstore_SuggestedTables.sql | `
    Set-Content $scriptRootPath\SQL-2012\StoredProcs\cstore_install_all_stored_procs.sql

# Unit Tests for SQL Server 2012
Get-Content $scriptRootPath\Tests\SQL-2012\*.sql | Set-Content $scriptRootPath\Tests\sql-2012-tests.sql

# Extended Events for SQL Server 2012
Get-Content -Path "$($scriptRootPath)\SQL-2012\Extended Events\*.*" -Include *.sql -Exclude setup_all_extended_events.sql | Set-Content "$($scriptRootPath)\SQL-2012\Extended Events\setup_all_extended_events.sql"

###############################################################################
# SQL Server 2014
Get-Content $scriptRootPath\SQL-2014\StoredProcs\cstore_GetAlignment.sql, $scriptRootPath\SQL-2014\StoredProcs\cstore_GetDictionaries.sql, `
            $scriptRootPath\SQL-2014\StoredProcs\cstore_GetFragmentation.sql, $scriptRootPath\SQL-2014\StoredProcs\cstore_GetMemory.sql, `
            $scriptRootPath\SQL-2014\StoredProcs\cstore_GetRowGroups.sql, $scriptRootPath\SQL-2014\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            $scriptRootPath\SQL-2014\StoredProcs\cstore_GetSQLInfo.sql, $scriptRootPath\SQL-2014\StoredProcs\cstore_SuggestedTables.sql, `
            $scriptRootPath\SQL-2014\StoredProcs\cstore_doMaintenance.sql |
    Set-Content $scriptRootPath\SQL-2014\StoredProcs\cstore_install_all_stored_procs.sql

# Unit Tests for SQL Server 2014
Get-Content $scriptRootPath\Tests\SQL-2014\*.sql | Set-Content $scriptRootPath\Tests\sql-2014-tests.sql

# Extended Events for SQL Server 2014
Get-Content -Path "$($scriptRootPath)\SQL-2014\Extended Events\*.*" -Include *.sql -Exclude setup_all_extended_events.sql | Set-Content "$($scriptRootPath)\SQL-2014\Extended Events\setup_all_extended_events.sql"

###############################################################################
# SQL Server 2016
Get-Content $scriptRootPath\SQL-2016\StoredProcs\cstore_GetAlignment.sql, $scriptRootPath\SQL-2016\StoredProcs\cstore_GetDictionaries.sql, `
            $scriptRootPath\SQL-2016\StoredProcs\cstore_GetFragmentation.sql, $scriptRootPath\SQL-2016\StoredProcs\cstore_GetMemory.sql, `
            $scriptRootPath\SQL-2016\StoredProcs\cstore_GetRowGroups.sql, $scriptRootPath\SQL-2016\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            $scriptRootPath\SQL-2016\StoredProcs\cstore_GetSQLInfo.sql, $scriptRootPath\SQL-2016\StoredProcs\cstore_SuggestedTables.sql, `
            $scriptRootPath\SQL-2016\StoredProcs\cstore_doMaintenance.sql |
    Set-Content $scriptRootPath\SQL-2016\StoredProcs\cstore_install_all_stored_procs.sql

# Extended Events for SQL Server 2016
Get-Content -Path "$($scriptRootPath)\SQL-2016\Extended Events\*.*" -Include *.sql -Exclude setup_all_extended_events.sql | Set-Content "$($scriptRootPath)\SQL-2016\Extended Events\setup_all_extended_events.sql"

###############################################################################
# SQL Server vNext
Get-Content "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetAlignment.sql", "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetDictionaries.sql", `
            "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetFragmentation.sql", "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetMemory.sql", `
            "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetRowGroups.sql", "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetRowGroupsDetails.sql", `
            "$scriptRootPath\SQL vNext\StoredProcs\cstore_GetSQLInfo.sql", "$scriptRootPath\SQL vNext\StoredProcs\cstore_SuggestedTables.sql", `
            "$scriptRootPath\SQL vNext\StoredProcs\cstore_doMaintenance.sql" |
    Set-Content "$scriptRootPath\SQL vNext\StoredProcs\cstore_install_all_stored_procs.sql"

# Extended Events for SQL Server vNext
Get-Content -Path "$($scriptRootPath)\SQL vNext\Extended Events\*.*" -Include *.sql -Exclude setup_all_extended_events.sql | Set-Content "$($scriptRootPath)\SQL vNext\Extended Events\setup_all_extended_events.sql"

###############################################################################
# Azure SQL Database
Get-Content $scriptRootPath\Azure\StoredProcs\cstore_GetAlignment.sql, $scriptRootPath\Azure\StoredProcs\cstore_GetDictionaries.sql, `
            $scriptRootPath\Azure\StoredProcs\cstore_GetFragmentation.sql,  `
            $scriptRootPath\Azure\StoredProcs\cstore_GetRowGroups.sql, $scriptRootPath\Azure\StoredProcs\cstore_GetRowGroupsDetails.sql, `
            $scriptRootPath\Azure\StoredProcs\cstore_SuggestedTables.sql, `
            $scriptRootPath\Azure\StoredProcs\cstore_doMaintenance.sql |
    Set-Content $scriptRootPath\Azure\StoredProcs\cstore_install_all_stored_procs.sql

Get-Content -Path "$($scriptRootPath)\Azure\Extended Events\*.*" -Include *.sql -Exclude setup_all_extended_events.sql | Set-Content "$($scriptRootPath)\Azure\Extended Events\setup_all_extended_events.sql"

$cred = Get-Credential;

Install-CISL -SQLInstance .\SQL12
Install-CISL -SQLInstance .\SQL14
Install-CISL -SQLInstance .\SQL16
Install-CISL -SQLInstance "columnstore.database.windows.net" -cred $cred 