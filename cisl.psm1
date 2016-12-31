#	CISL - Columnstore Indexes Scripts Library for SQL Server
#	Powershell Script to install .ps1 scripts
#	Version: 1.4.2, December 2016
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

Write-Host "Loading Module 'CISL' (Columnstore Indexes Script Library) "

foreach ($function in (Get-ChildItem "$PSScriptRoot\*_CISL.ps1")) { . $function }