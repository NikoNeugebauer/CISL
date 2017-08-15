/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2016: 
	Columnstore Tests - cstore_GetDictionaries is tested with the table that has 1 compressed Row Group containing 1 row that is deleted
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

IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'testOneDeletedRowGroup' and schema_id = SCHEMA_ID('Dictionaries') )
	exec ('create procedure [Dictionaries].[testOneDeletedRowGroup] as select 1');
GO

ALTER PROCEDURE [Dictionaries].[testOneDeletedRowGroup] AS
BEGIN
	DROP TABLE IF EXISTS #ExpectedDictionaries;

	CREATE TABLE #ExpectedDictionaries(
		TableName nvarchar(256),
		Type varchar(12),
		[Location] varchar(10),			
		[Partition] int,
		RowGroups int,
		Dictionaries int,
		EntriesCount bigint,
		[Rows Serving] bigint,
		[Total Size in MB] Decimal(8,3),
		[Max Global Size in MB] Decimal(8,3),
		[Max Local Size in MB] Decimal(8,3)
	);

	select top (0) *
		into #ActualDictionaries
		from #ExpectedDictionaries;

	-- CCI
	insert into #ActualDictionaries
		exec dbo.cstore_GetDictionaries @tableName = 'OneDeletedRowGroupCCI', @showDetails = 0;

	exec tSQLt.AssertEqualsTable '#ExpectedDictionaries', '#ActualDictionaries';

END

GO
