/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.6.0, January 2018

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

/*
	Known Issues & Limitations: 
		- Custom non-standard (non-CU & non-SP) versions are not targeted yet
*/

/*
Changes in 1.0.1
	+ Added drops for the existing temp tables: #SQLColumnstoreImprovements, #SQLBranches, #SQLVersions
	+ Added new parameter for Enables showing the SQL Server versions that are posterior the current version
	* Added more source code description in the comments
	+ Removed some redundant information (column UpdateName from the #SQLColumnstoreImprovements) which were left from the very early versions
	- Fixed erroneous build version for the SQL Server 2014 SP2 CU2

Changes in 1.0.2
	+ Added information about CU 3 for SQL Server 2014 SP1 and CU 10 for SQL Server 2014 RTM
	+ Added column with the CU Version for the Bugfixes output
	- Fixed bug with the wrong CU9 Version 
	* Updated temporary tables in order to avoid error messages

Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published
	+ Added information about CU 4 for SQL Server 2014 SP1 and CU 11 for SQL Server 2014 RTM

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Added Information about CU 5 & CU 6 for SQL Server 2014 SP1 & about CU 12 & CU 13 for SQL Server 2014 RTM

Changes in 1.3.0
	+ Added Information about updated CU 6A, CU 7 for SQL Server 2014 SP1 & CU 14 for SQL Server 2014 RTM
	+ Added Information about SQL Server 2014 SP2

Changes in 1.3.1
	+ Added Information about updated CU 8 for SQL Server 2014 SP1 & CU 1 for SQL Server 2014 SP2

Changes in 1.4.0
	- Fixed Bug with Duplicate Fixes & Improvements (CU12 for SP1 & CU2 for SP2, for example) not being eliminated from the list
	- Added information on the CU 9 for SQL Server 2014 SP1 & CU 2 for SQL Server 2014 SP2

Changes in 1.4.2
	- Added information on the CU 10 for SQL Server 2014 SP1 & CU 3 for SQL Server 2014 SP2

Changes in 1.5.0
	+ Added information on the CU 11, CU 12, CU 13 for SQL Server 2014 SP1 & CU 4, CU 5, CU 6 & CU 7 for SQL Server 2014 SP2
	+ Added displaying information on the date of each of the service releases (when using parameter @showNewerVersions)

Changes in 1.6.0
	+ Added information on the CU 8 & CU 9 for SQL Server 2014 SP2
	+ Added information on the Trace Flag 2469 - Fixing: Intra-query deadlock when values are inserted into a partitioned clustered columnstore index 
*/

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2014
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'12'
begin
	set @errorMessage = (N'You are not running a SQL Server 2014. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2014 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetSQLInfo' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetSQLInfo as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.6.0, January 2018
*/
alter procedure dbo.cstore_GetSQLInfo(
-- Params --
	@showUnrecognizedTraceFlags bit = 1,		-- Enables showing active trace flags, even if they are not columnstore indexes related
	@identifyCurrentVersion bit = 1,			-- Enables identification of the currently used SQL Server Instance version
	@showNewerVersions bit = 0					-- Enables showing the SQL Server versions that are posterior the current version-- end of --
) as 
begin
	declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
			@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));

	declare @SQLServerBuild smallint  = substring(@SQLServerVersion,CHARINDEX('.',@SQLServerVersion,5)+1,CHARINDEX('.',@SQLServerVersion,8)-CHARINDEX('.',@SQLServerVersion,5)-1);

	if OBJECT_ID('tempdb..#SQLColumnstoreImprovements', 'U') IS NOT NULL
		drop table #SQLColumnstoreImprovements;
	if OBJECT_ID('tempdb..#SQLBranches', 'U') IS NOT NULL
		drop table #SQLBranches;
	if OBJECT_ID('tempdb..#SQLVersions', 'U') IS NOT NULL
		drop table #SQLVersions;

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	create table #SQLColumnstoreImprovements(
		BuildVersion smallint not null,
		SQLBranch char(3) not null,
		Description nvarchar(500) not null,
		URL nvarchar(1000)
	);

	create table #SQLBranches(
		SQLBranch char(3) not null Primary Key,
		MinVersion smallint not null );

	create table #SQLVersions(
		SQLBranch char(3) not null,
		SQLVersion smallint not null Primary Key,
		ReleaseDate datetime not null,
		SQLVersionDescription nvarchar(100) );

	insert into #SQLBranches (SQLBranch, MinVersion)
		values ('RTM', 2000 ), ('SP1', 4100), ('SP2', 5000) ;

	insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
		values 
		( 'RTM', 2000, convert(datetime,'01-04-2014',105), 'SQL Server 2014 RTM' ),
		( 'RTM', 2342, convert(datetime,'21-04-2014',105), 'CU 1 for SQL Server 2014 RTM' ),
		( 'RTM', 2370, convert(datetime,'27-06-2014',105), 'CU 2 for SQL Server 2014 RTM' ),
		( 'RTM', 2402, convert(datetime,'18-08-2014',105), 'CU 3 for SQL Server 2014 RTM' ),
		( 'RTM', 2430, convert(datetime,'21-10-2014',105), 'CU 4 for SQL Server 2014 RTM' ),
		( 'RTM', 2456, convert(datetime,'18-12-2014',105), 'CU 5 for SQL Server 2014 RTM' ),
		( 'RTM', 2480, convert(datetime,'16-02-2015',105), 'CU 6 for SQL Server 2014 RTM' ),
		( 'RTM', 2495, convert(datetime,'23-04-2015',105), 'CU 7 for SQL Server 2014 RTM' ),
		( 'RTM', 2546, convert(datetime,'22-06-2015',105), 'CU 8 for SQL Server 2014 RTM' ),
		( 'RTM', 2553, convert(datetime,'17-08-2015',105), 'CU 9 for SQL Server 2014 RTM' ),
		( 'RTM', 2556, convert(datetime,'20-10-2015',105), 'CU 10 for SQL Server 2014 RTM' ),
		( 'RTM', 2560, convert(datetime,'22-12-2015',105), 'CU 11 for SQL Server 2014 RTM' ),
		( 'RTM', 2564, convert(datetime,'22-02-2016',105), 'CU 12 for SQL Server 2014 RTM' ),
		( 'RTM', 2568, convert(datetime,'19-04-2016',105), 'CU 13 for SQL Server 2014 RTM' ),
		( 'RTM', 2569, convert(datetime,'20-06-2016',105), 'CU 14 for SQL Server 2014 RTM' ),
		( 'SP1', 4100, convert(datetime,'14-05-2015',105), 'SQL Server 2014 SP1' ),
		( 'SP1', 4416, convert(datetime,'22-06-2015',105), 'CU 1 for SQL Server 2014 SP1' ),
		( 'SP1', 4422, convert(datetime,'17-08-2015',105), 'CU 2 for SQL Server 2014 SP1' ),
		( 'SP1', 4427, convert(datetime,'21-10-2015',105), 'CU 3 for SQL Server 2014 SP1' ),
		( 'SP1', 4436, convert(datetime,'22-12-2015',105), 'CU 4 for SQL Server 2014 SP1' ),
		( 'SP1', 4439, convert(datetime,'22-02-2016',105), 'CU 5 for SQL Server 2014 SP1' ),
		( 'SP1', 4449, convert(datetime,'19-04-2016',105), 'CU 6 for SQL Server 2014 SP1' ),
		( 'SP1', 4457, convert(datetime,'31-05-2016',105), 'CU 6A for SQL Server 2014 SP1' ),
		( 'SP1', 4459, convert(datetime,'20-06-2016',105), 'CU 7 for SQL Server 2014 SP1' ),
		( 'SP1', 4468, convert(datetime,'15-08-2016',105), 'CU 8 for SQL Server 2014 SP1' ),
		( 'SP1', 4474, convert(datetime,'18-10-2016',105), 'CU 9 for SQL Server 2014 SP1' ),
		( 'SP1', 4491, convert(datetime,'18-12-2016',105), 'CU 10 for SQL Server 2014 SP1' ),
		( 'SP1', 4502, convert(datetime,'21-02-2017',105), 'CU 11 for SQL Server 2014 SP1' ),
		( 'SP1', 4511, convert(datetime,'18-04-2017',105), 'CU 12 for SQL Server 2014 SP1' ),
		( 'SP1', 4522, convert(datetime,'08-08-2017',105), 'CU 13 for SQL Server 2014 SP1' ),
		( 'SP2', 5000, convert(datetime,'11-07-2016',105), 'SQL Server 2014 SP2' ),
		( 'SP2', 5511, convert(datetime,'25-08-2016',105), 'CU 1 for SQL Server 2014 SP2' ),
		( 'SP2', 5522, convert(datetime,'18-10-2016',105), 'CU 2 for SQL Server 2014 SP2' ),
		( 'SP2', 5537, convert(datetime,'28-12-2016',105), 'CU 3 for SQL Server 2014 SP2' ),
		( 'SP2', 5540, convert(datetime,'21-02-2017',105), 'CU 4 for SQL Server 2014 SP2' ),
		( 'SP2', 5546, convert(datetime,'18-04-2017',105), 'CU 5 for SQL Server 2014 SP2' ),
		( 'SP2', 5553, convert(datetime,'08-08-2017',105), 'CU 6 for SQL Server 2014 SP2' ),
		( 'SP2', 5556, convert(datetime,'29-08-2017',105), 'CU 7 for SQL Server 2014 SP2' ),
		( 'SP2', 5557, convert(datetime,'16-10-2017',105), 'CU 8 for SQL Server 2014 SP2' ),
		( 'SP2', 5563, convert(datetime,'19-12-2017',105), 'CU 9 for SQL Server 2014 SP2' );

	insert into #SQLColumnstoreImprovements (BuildVersion, SQLBranch, Description, URL )
		values 
		( 2342, 'RTM', 'FIX: Error 35377 when you build or rebuild clustered columnstore index with maxdop larger than 1 through MARS connection in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2942895' ),
		( 2370, 'RTM', 'FIX: Loads or queries on CCI tables block one another in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2931815' ),
		( 2370, 'RTM', 'FIX: Access violation when you insert data into a table that has a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2966096' ),
		( 2370, 'RTM', 'FIX: Error when you drop a clustered columnstore index table during recovery in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2974397' ),
		( 2370, 'RTM', 'FIX: Poor performance when you bulk insert into partitioned CCI in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2969421' ),
		( 2370, 'RTM', 'FIX: Truncated CCI partitioned table runs for a long time in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2969419' ),
		( 2370, 'RTM', 'FIX: DBCC SHRINKDATABASE or DBCC SHRINKFILE cannot move pages that belong to the nonclustered columnstore index', 'https://support.microsoft.com/en-us/kb/2967198' ),
		( 2402, 'RTM', 'FIX: UPDATE or INSERT statement on CCI makes sys.partitions not match actual row count in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2978472' ), 
		( 2402, 'RTM', 'FIX: Cannot create indexed view on a clustered columnstore index and BCP on the table fails in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/2981764' ),
		( 2402, 'RTM', 'FIX: Some columns in sys.column_store_segments view show NULL value when the table has non-dbo schema in SQL Server', 'https://support.microsoft.com/en-us/kb/2989704' ),
		( 2430, 'RTM', 'FIX: Error 8654 when you run "INSERT INTO … SELECT" on a table with clustered columnstore index in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/2998301' ),
		( 2430, 'RTM', 'FIX: UPDATE STATISTICS performs incorrect sampling and processing for a table with columnstore index in SQL Server', 'https://support.microsoft.com/en-us/kb/2986627' ),
		( 2456, 'RTM', 'FIX: Error 35377 occurs when you try to access clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3020113' ),
		( 2480, 'RTM', 'FIX: Access violation occurs when you delete rows from a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029762' ),
		( 2480, 'RTM', 'FIX: OS error 665 when you execute DBCC CHECKDB command for database that contains columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029977' ),
		( 2480, 'RTM', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 2480, 'RTM', 'FIX: Improved memory management for columnstore indexes to deliver better query performance in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3053664' ),
		( 2495, 'RTM', 'FIX: Partial results in a query of a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067257' ),
		( 2546, 'RTM', 'FIX: Error 33294 occurs when you alter column types on a table that has clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3070139' ),
		( 2546, 'RTM', '"Non-yielding Scheduler" error when a database has columnstore indexes on a SQL Server 2014 instance', 'https://support.microsoft.com/en-us/kb/3069488' ),
		( 2546, 'RTM', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067968' ),
		( 2553, 'RTM', 'FIX: Rare index corruption when you build a columnstore index with parallelism on a partitioned table in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3080155' ), 
		( 2556, 'RTM', 'FIX: Access violation when you query against a table that contains column store indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3097601' ),
		( 2556, 'RTM', 'FIX: FIX: Assert occurs when you change the type of column in a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3098529' ),
		( 2560, 'RTM', 'FIX: "Non-yielding Scheduler" condition when you query a partitioned table that has a column store index in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3121647' ),
		( 2564, 'RTM', 'FIX: Columnstore index corruption occurs when you use AlwaysOn Availability Groups in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3135751' ),
		( 2568, 'RTM', 'Query plan generation improvement for some columnstore queries in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3146123' ),
		( 4100, 'SP1', 'LOB reads are shown as zero when "SET STATISTICS IO" is on during executing a query with clustered columnstore index.', 'https://support.microsoft.com/en-us/kb/3058865' ),
		( 4100, 'SP1', 'FIX: OS error 665 when you execute DBCC CHECKDB command for database that contains columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029977' ),
		( 4100, 'SP1', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 4416, 'SP1', 'FIX: Improved memory management for columnstore indexes to deliver better query performance in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3053664' ),
		( 4416, 'SP1', '"Non-yielding Scheduler" error when a database has columnstore indexes on a SQL Server 2014 instance', 'https://support.microsoft.com/en-us/kb/3069488' ),
		( 4416, 'SP1', 'FIX: Access violation occurs when you delete rows from a table that has clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3029762' ),
		( 4416, 'SP1', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3067968' ),
		( 4416, 'SP1', 'FIX: Severe error in SQL Server 2014 during compilation of a query on a table with clustered columnstore index', 'https://support.microsoft.com/en-us/kb/3068297' ),
		( 4416, 'SP1', 'FIX: Error 8646 when you run DML statements on a table with clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3035165' ),
		( 4416, 'SP1', 'FIX: Partial results in a query of a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067257' ),
		( 4416, 'SP1', 'FIX: Error 33294 occurs when you alter column types on a table that has clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3070139' ),
		( 4427, 'SP1', 'FIX: Rare index corruption when you build a columnstore index with parallelism on a partitioned table in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3080155' ),
		( 4436, 'SP1', 'FIX: Query stops responding when you run a parallel query on a table that has a columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3110497' ),
		( 4439, 'SP1', 'FIX: Error 35377 occurs when you try to access clustered columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3020113' ),
		( 4449, 'SP1', 'FIX: Columnstore index corruption occurs when you use AlwaysOn Availability Groups in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3135751' ),
		( 4449, 'SP1', 'FIX: SELECT…INTO statement retrieves incorrect result from a clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3152606' ),
		( 4459, 'SP1', 'FIX: DBCC CHECKTABLE returns an incorrect result after the clustered columnstore index is rebuilt in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3168712' ),
		( 4459, 'SP1', 'Query plan generation improvement for some columnstore queries in SQL Server 2014 ', 'https://support.microsoft.com/en-us/kb/3146123' ),
		( 4474, 'SP1', 'FIX: Access violation when you run a query that uses clustered columnstore index with trace flag 2389, 2390, or 4139', 'https://support.microsoft.com/en-us/kb/3189645' ),
		( 4491, 'SP1', 'FIX: Out-of-memory errors when you execute DBCC CHECKDB on database that contains columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3201416' ),
		( 4491, 'SP1', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3067968' ),
		( 4491, 'SP1', 'FIX: Out-of-memory errors when you execute DBCC CHECKDB on database that contains columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3201416' ),
		( 4522, 'SP1', 'FIX: Access violation with query to retrieve data from a clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/4024184/fix-access-violation-with-query-to-retrieve-data-from-a-clustered-colu' ),
		( 5522, 'SP2', 'FIX: Access violation when you run a query that uses clustered columnstore index with trace flag 2389, 2390, or 4139', 'https://support.microsoft.com/en-us/kb/3189645' ),
		( 5522, 'SP2', 'FIX: Deadlock when you execute a query plan with a nested loop join in batch mode in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/kb/3195825' ),
		( 5522, 'SP2', 'Improved SQL Server stability and concurrent query execution for some columnstore queries in SQL Server 2014 and 2016', 'https://support.microsoft.com/en-us/kb/3191487' ),
		( 5537, 'SP2', 'FIX: Out-of-memory errors when you execute DBCC CHECKDB on database that contains columnstore indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3201416' ),
		( 5537, 'SP2', 'FIX: Intra-query deadlock when values are inserted into a partitioned clustered columnstore index in SQL Server 2014', 'https://support.microsoft.com/en-us/kb/3204769' ),
		( 5540, 'SP2', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014', 'https://support.microsoft.com/en-us/help/3067968' ),
		( 5546, 'SP2', 'FIX: Access violation in SQL Server 2014 when large number of rows are inserted into a partitioned columnstore index', 'https://support.microsoft.com/en-us/help/4014327/fix-access-violation-in-sql-server-2014-when-large-number-of-rows-are' ),
		( 5553, 'SP2', 'FIX: Access violation with query to retrieve data from a clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/4024184/fix-access-violation-with-query-to-retrieve-data-from-a-clustered-colu' ),
		( 5556, 'SP2', 'FIX: Access violation with query to retrieve data from a clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/4024184/fix-access-violation-with-query-to-retrieve-data-from-a-clustered-colu' ),
		( 5563, 'SP2', 'FIX: SELECT query that uses batch mode hash aggregate operator that counts multiple nullable columns returns bad results in SQL Server', 'https://support.microsoft.com/en-us/help/4052633/fix-select-query-that-uses-batch-mode-hash-aggregate-operator-that-cou' ),
		( 5563, 'SP2', 'FIX: Access violation when you query against a table that contains column store indexes in SQL Server 2014', 'https://support.microsoft.com/en-us/help/3097601/fix-access-violation-when-you-query-against-a-table-that-contains-colu' );

	if @identifyCurrentVersion = 1
	begin
		if OBJECT_ID('tempdb..#TempVersionResults') IS NOT NULL
			drop table #TempVersionResults;

		create table #TempVersionResults(
			MessageText nvarchar(512) NOT NULL,		
			SQLVersionDescription nvarchar(200) NOT NULL,
			SQLBranch char(3) not null,
			SQLVersion smallint NULL,
			ReleaseDate Date NULL );

	
		-- Identify the number of days that has passed since the installed release
		declare @daysSinceLastRelease int = NULL;
		select @daysSinceLastRelease = datediff(dd,max(ReleaseDate),getdate())
			from #SQLVersions
			where SQLBranch = ServerProperty('ProductLevel')
				and SQLVersion = cast(@SQLServerBuild as int);

		-- Display the current information about this SQL Server 
		if( exists (select 1
						from #SQLVersions
						where SQLVersion = cast(@SQLServerBuild as int) ) )
			select 'You are Running:' as MessageText, SQLVersionDescription, SQLBranch, SQLVersion as BuildVersion, 'Your version is ' + cast(@daysSinceLastRelease as varchar(3)) + ' days old' as DaysSinceRelease
				from #SQLVersions
				where SQLVersion = cast(@SQLServerBuild as int);
		else
			select 'You are Running a Non RTM/SP/CU standard version:' as MessageText, '-' as SQLVersionDescription, 
				ServerProperty('ProductLevel') as SQLBranch, @SQLServerBuild as SQLVersion, 'Your version is ' + cast(@daysSinceLastRelease as varchar(3)) + ' days old' as DaysSinceRelease;


		-- Select information about all newer SQL Server versions that are known
		if @showNewerVersions = 1
		begin 
			insert into #TempVersionResults
				select 'Available Newer Versions:' as MessageText
					, '' as SQLVersionDescription
					, '' as SQLBranch, NULL as BuildVersion
					, NULL as ReleaseDate
				UNION ALL
				select '' as MessageText, SQLVersionDescription as SQLVersionDescription
						, SQLBranch as SQLVersionDescription
						, SQLVersion as BuildVersion
						, ReleaseDate as ReleaseDate
						from #SQLVersions
						where  @SQLServerBuild <  SQLVersion;

			select * 
				from #TempVersionResults;

			drop table #TempVersionResults;
		end 

	end

	select min(imps.BuildVersion) as BuildVersion, min(vers.SQLVersionDescription) as SQLVersionDescription, imps.Description, imps.URL
		from #SQLColumnstoreImprovements imps
			inner join #SQLBranches branch
				on imps.SQLBranch = branch.SQLBranch
			inner join #SQLVersions vers
				on imps.BuildVersion = vers.SQLVersion
		where BuildVersion > @SQLServerBuild 
			and branch.SQLBranch >= ServerProperty('ProductLevel')
			and branch.MinVersion < BuildVersion
		group by Description, URL, SQLVersionDescription
		having min(imps.BuildVersion) = (select min(imps2.BuildVersion)	from #SQLColumnstoreImprovements imps2 where imps.Description = imps2.Description and imps2.BuildVersion > @SQLServerBuild group by imps2.Description)
		order by BuildVersion;

	drop table #SQLColumnstoreImprovements;
	drop table #SQLBranches;
	drop table #SQLVersions;

	--------------------------------------------------------------------------------------------------------------------
	-- Trace Flags part
	create table #ActiveTraceFlags(	
		TraceFlag nvarchar(20) not null,
		Status bit not null,
		Global bit not null,
		Session bit not null );

	insert into #ActiveTraceFlags
		exec sp_executesql N'DBCC TRACESTATUS()';

	create table #ColumnstoreTraceFlags(
		TraceFlag int not null,
		Description nvarchar(500) not null,
		URL nvarchar(600),
		SupportedStatus bit not null 
	);

	insert into #ColumnstoreTraceFlags (TraceFlag, Description, URL, SupportedStatus )
		values 
		(  634, 'Disables the background columnstore compression task.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		(  834, 'Enable Large Pages', 'https://support.microsoft.com/en-us/kb/920093?wa=wsignin1.0', 0 ),
		(  646, 'Gets text output messages that show what segments (row groups) were eliminated during query processing', 'http://social.technet.microsoft.com/wiki/contents/articles/5611.verifying-columnstore-segment-elimination.aspx', 1 ),
		( 2469, 'FIX: Intra-query deadlock when values are inserted into a partitioned clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-ph/help/3204769/fix-intra-query-deadlock-when-values-are-inserted-into-a-partitioned-c', 1 ),
		( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2014/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 ),
		(10207, 'Skips Corrupted Columnstore Segments (Fixed in CU8 for SQL Server 2014 RTM and CU1 for SQL Server 2014 SP1)', 'https://support.microsoft.com/en-us/kb/3067257', 1 );

	select tf.TraceFlag, isnull(conf.Description,'Unrecognized') as Description, isnull(conf.URL,'-') as URL, SupportedStatus
		from #ActiveTraceFlags tf
			left join #ColumnstoreTraceFlags conf
				on conf.TraceFlag = tf.TraceFlag
		where @showUnrecognizedTraceFlags = 1 or (@showUnrecognizedTraceFlags = 0 AND Description is not null);

	drop table #ColumnstoreTraceFlags;
	drop table #ActiveTraceFlags;

end
GO
