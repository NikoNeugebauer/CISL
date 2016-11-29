#	CISL - Columnstore Indexes Scripts Library for SQL Server
#	Powershell Script to extract the DACPACs
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

$CISLVersion = "141"
$sqlPackageLocation = "C:\Program Files (x86)\Microsoft SQL Server\130\DAC\bin\sqlpackage.exe"
$dacpacLocation = "Z:\MEOCloud\CISL GitHub\CISL\Releases\DacPacs\"
$sql2012 = ".\SQL12"
$sql2014 = ".\SQL14"
$sql2016 = ".\SQL16"
$sqlAzure = "x.database.windows.net"
$sqlAzureUser = "x"
$sqlAzurePass = "x"

# Verify if the SQLPackage.exe location is correctly specified
if( !(Test-Path $sqlPackageLocation) ){
    Throw "Please configure correctly the variable $sqlPackageLocation, the current location $($sqlPackageLocation) is wrong"
}

# Verify if the path exists, and if not, create the folder
If ( !(Test-Path $dacpacLocation) ){
    New-Item -Path $dacpacLocation -ItemType Directory
}


# Extract the Azure SQLDB
& $sqlPackageLocation "/a:Extract" "/ssn:$($sqlAzure)" "/SourceUser:$($sqlAzureUser)" "/SourcePassword:$($sqlAzurePass)" "/sdn:CISL" "/tf:$($dacpacLocation)CISL-$($CISLVersion)-Azure.dacpac" "/of:True" 

# Extract the SQL Server 2012 DB
& $sqlPackageLocation "/a:Extract" "/ssn:$($sql2012)" "/sdn:CISL" "/tf:$($dacpacLocation)CISL-$($CISLVersion)-2012.dacpac" "/of:True" 

# Extract the SQL Server 2014 DB
& $sqlPackageLocation "/a:Extract" "/ssn:$($sql2014)" "/sdn:CISL" "/tf:$($dacpacLocation)CISL-$($CISLVersion)-2014.dacpac" "/of:True" 

# Extract the SQL Server 2016 DB
& $sqlPackageLocation "/a:Extract" "/ssn:$($sql2016)" "/sdn:CISL" "/tf:$($dacpacLocation)CISL-$($CISLVersion)-2016.dacpac" "/of:True" 
