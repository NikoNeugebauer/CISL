/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2014: 
	Columnstore Tests - creates new tsqlt test classes for the CISL
	Version: 1.4.1, November 2016

	Copyright 2015-2016 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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


EXEC tSQLt.NewTestClass 'BasicTests';

-- Create a separate class for each of the CISL functionalities
EXEC tSQLt.NewTestClass 'Alignment';

EXEC tSQLt.NewTestClass 'Dictionaries';

EXEC tSQLt.NewTestClass 'Memory';

EXEC tSQLt.NewTestClass 'Fragmentation';

EXEC tSQLt.NewTestClass 'RowGroups';

EXEC tSQLt.NewTestClass 'RowGroupsDetails';

EXEC tSQLt.NewTestClass 'SuggestedTables';

-- Installation tests
EXEC tSQLt.NewTestClass 'Installation';

-- Cleanup tests
EXEC tSQLt.NewTestClass 'Cleanup';
