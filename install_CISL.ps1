﻿#	CISL - Columnstore Indexes Scripts Library for SQL Server
#	Powershell Script to install the CISL in the user databases
#	Version: 1.5.0, August 2017
#
#	Copyright 2015-2017 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)
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


function Install-CISL(
    [parameter(Mandatory=$true)]
    [String]$SQLInstance = ".\SQL14",

    [parameter(Mandatory=$false)][ValidateSet("script","dacpac")] 
    [String]$installType = "script",  # You can install either a sql script or you can install pre-configured dacpacs (database snapshots)

    [parameter(Mandatory=$false)][ValidateSet(2012,2014,2016,"Azure")] 
    [String]$forceSqlVersion, # Forces SQL Server version. The values 2012, 2014, 2016 & Azure are supported

    [parameter(Mandatory=$false)] # Credentials to be used when connecting to SQL Server
    [pscredential]$cred,

    [parameter(Mandatory=$false)] # Using this argument will allow the installation in the specified databases only
    $installDBs,                  

    [parameter(Mandatory=$false)]
    $excludeDBs = ("master", "model", "tempdb", "msdb", "SSISDB", "MDS", "ReportServer", "ReportServerTempDB", "DQS_MAIN", "DQS_PROJECTS", "DQS_STAGING_DATA"),

    #[parameter(Mandatory=$false)]
    #[bool]$showDetails = $false,

    [parameter(Mandatory=$false)] # Setting this argument to true will allow the installation in the databases that already have Columnstore Indexes
    [bool]$installForExistingCStoreOnly = $false,

    [parameter(Mandatory=$false)] # location of the SQLPackage.exe for installing DacPacs 
    $sqlPackageLocation = "C:\Program Files (x86)\Microsoft SQL Server\130\DAC\bin\sqlpackage.exe"
)
{
    # Writing the header
    Write-Host "CISL - Columnstore Indexes Scripts Library for SQL Server: " -ForegroundColor Green
    Write-Host "Version: 1.5.0, August 2017" -ForegroundColor Green
    Write-Host "(c) 2015-2017 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)" -ForegroundColor Green
    Write-Host "----------------------------------------------------------------------------------------------------"
    
    # Load SMO Assembly
    if ([Reflection.Assembly]::LoadWithPartialName("Microsoft.SqlServer.SMO") -eq $null )
    { throw "Quitting: SMO Required. You can download it from http://goo.gl/R4yA6u" }

    try{
        $SQLDB = "master";     # Use MASTER DB by default
        $Server = New-Object ('Microsoft.SQLServer.Management.Smo.Server') $SQLInstance 
    }
    catch [Exception]
    {
        Write-Host "Failed to connect to the SQL Server Instance '$($SQLInstance)'" -ForegroundColor Red;
        Write-Host $_.Exception.ToString()
        return;
    }

    if( $cred ){
        $server.ConnectionContext.LoginSecure = $false
        $server.ConnectionContext.set_Login($cred.username)
        $server.ConnectionContext.set_SecurePassword($cred.Password)

        try { $server.ConnectionContext.Connect() } 
        catch { throw "Can't connect to $($SQLInstance) or access denied. Quitting." }
    }

    # Verify if the connection was succesfull
    if( !$Server ){
        Write-Host "Failed to connect to the SQL Server Instance '$($SQLInstance)'" -ForegroundColor Red;
        return;
    }

    # Determine the current SQL Server version
    if( !$forceSqlVersion ){
        $ds = $server.Databases[0].ExecuteWithResults(“SELECT SUBSTRING(cast(SERVERPROPERTY('productversion') as CHAR(2)),1,2) as SQLServerVersion”)        

        switch( $ds.Tables[0].SQLServerVersion ){
            11 { $sqlVersion = "2012" }
            12 { $sqlVersion = "2014" }
            13 { $sqlVersion = "2016" }
            default { Write-Host -ForegroundColor Red "Your SQL Server version ($ds.Tables[0].SQLServerVersion) is not supported." }
        }
        
        # Getting the Count of existing Columnstore Indexes Segments
        $ds = $server.Databases[0].ExecuteWithResults(“SELECT SERVERPROPERTY('EngineEdition') as Engine”)        
        $SQLServerEngine = $ds.Tables[0].Engine;

        if ( $SQLServerEngine -eq 5 ){
            $sqlVersion = "Azure"
        }
    }

    # Use specified SQL Server Version
    if( $forceSqlVersion ){
        Write-Host -ForegroundColor Red "Warning: Your CISL SQL Server version is being forced to SQL Server $forceSqlVersion"
        $sqlVersion = $forceSqlVersion;
    }

    # Write the SQL Server Version Identifier
    Write-Host "Using SQL Server $($sqlVersion): `nInstalling CISL on the user databases of the instance '$SQLInstance':`n" -ForegroundColor Yellow;

    # Get SMO Databases Object 
    if( $installDBs ){
        $dbArray = $Server.Databases | Where-Object -FilterScript {$_.Name -like "*$($installDBs)*"}
    }
    else{
        $dbArray = $Server.Databases;
    }


    # Parse through all instance databases
    foreach ($db in $dbArray)
    {  
        if( (!($excludeDBs -contains $db.Name) ) ){
            
            # Checking Database Status 
            if( $db.Readonly -eq $true ){
                Write-Host "CISL is not being installed in the Database '$($db.Name)' because it is ReadOnly" -ForegroundColor Magenta;
                continue;
            }

            if( $db.IsDatabaseSnapshot -eq $true ){
                Write-Host "CISL is not being installed in the Database '$($db.Name)' because it is a Snapshot" -ForegroundColor Magenta;
                continue;
            }

            if( $db.IsAccessible -eq $false ){
                Write-Host "CISL is not being installed in the Database '$($db.Name)' because it is not accessible" -ForegroundColor Magenta;
                continue;
            }

            if( $db.IsUpdateable -eq $false ){
                Write-Host "CISL is not being installed in the Database '$($db.Name)' because it is not updatable" -ForegroundColor Magenta;
                continue;
            }


            # if set, avoiding installing CISL in the databases that have no Columnstore Indexes
            if( $installForExistingCStoreOnly ){
                
                # Getting the Count of existing Columnstore Indexes Segments
                $ds = $db.ExecuteWithResults(“SELECT count(*) as SegmentCount FROM sys.column_store_segments”)        
                $SegmentCount = $ds.Tables[0].SegmentCount;
                
                if( $SegmentCount -lt 15 ){
                    Write-Host "CISL is not being installed in the Database '$($db.Name)'" -ForegroundColor Magenta;
                    continue;
                }
            }

            
            Write-Host "Installing CISL in the Database '$($db.Name)':" -ForegroundColor Green -NoNewline;

            try{
                
                # Defining the script access path
                if ( $SQLServerEngine -eq 5 ){ $SQLPath = "Azure" }
                else { $SQLPath = "SQL-$($sqlVersion)"; }

                try{
                    if ( !$cred ){ 
                        # Decide if you are deploying DACPAC or SCRIPT
                        if( $installType -eq "dacpac" ){
                            & $sqlPackageLocation "/a:Publish" "/TargetServerName:$($SQLInstance)" "/TargetDatabaseName:$($db.Name)" "/SourceFile:$($PSScriptRoot)\Releases\Dacpacs\CISL-$($CISLVersion)-$($sqlVersion).dacpac" "/p:DropObjectsNotInSource=False"
                        }
                        else{
                            Invoke-Sqlcmd -InputFile "$($PSScriptRoot)\$($SQLPath)\StoredProcs\cstore_install_all_stored_procs.sql" `
                                        -ServerInstance $SQLInstance -Database $db.Name -Verbose 
                        }
                    }
                    else{
                        # Decide if you are deploying DACPAC or SCRIPT
                        if( $installType -eq "dacpac" ){
                            & $sqlPackageLocation "/a:Publish" "/TargetServerName:$($SQLInstance)" "/TargetDatabaseName:$($db.Name)" "/TargetUser:$($cred.UserName)" `
                                            "/TargetPassword:$($cred.GetNetworkCredential().password)" `
                                            "/SourceFile:$($PSScriptRoot)\Releases\Dacpacs\CISL-$($CISLVersion)-$($sqlVersion).dacpac" `
                                            "/p:DropObjectsNotInSource=False"
                        }
                        else{
                            Invoke-Sqlcmd -InputFile "$($PSScriptRoot)\$($SQLPath)\StoredProcs\cstore_install_all_stored_procs.sql" `
                                        -ServerInstance "$($SQLInstance)" -Database $db.Name -Username $cred.UserName -Password $cred.GetNetworkCredential().password
                        }
                    }
                
                    Write-Host "... Success!" -ForegroundColor Green
                }
                catch {
                    Write-Host "... ERROR!" -ForegroundColor Red
                    Write-Host($error)
                }
            }
            catch [System.Exception]
            {
                Write-Host "... Failure!" -ForegroundColor Red
                Write-Host $_.Exception -ForegroundColor Red
            }
        }
        else{
            Write-Host -ForegroundColor Magenta "CISL is not being installed in the Database '$($db.Name)', because it was filtered with the execution parameter '`$excludeDBs'"  
        }
    }

    $server.ConnectionContext.Disconnect();
}