/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.5.0, January 2017

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
		- Duplicate Fixes & Improvements (CU12 for SP1 & CU2 for SP2, for example) are not eliminated from the list yet
*/

/*
Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published	
	+ Added information on CTP 3.1 & CTP 3.2

Changes in 1.2.0
	+ Added information on CTP 3.3, RC0, RC1, RC2, RC3

Changes in 1.3.0
	+ Added information on RTM

Changes in 1.3.1
	+ Added information on CU 1 for SQL Server 2016 RTM
	+ Added information on the new trace flags 9347, 9349, 9358, 9389 & 10204
	+ Added information on the trace flag 4199 which affects batch mode sort operations in a complex parallel query 

Changes in 1.4.0
	+ Added information on CU 2 for SQL Server 2016 RTM & On-Demand fix for CU 2 for SQL Server 2016
	- Fixed Bug with Duplicate Fixes & Improvements (CU12 for SP1 & CU2 for SP2, for example) not being eliminated from the list
	+ Added information on the new trace flags 9354

Changes in 1.4.1
	+ Added support for the SP1 which allows support of Columnstore Indexes on any edition
	+ Added information on the Service Pack 1 for SQL Server 2016 and CU3 for SQL Server 2016 RTM

Changes in 1.5.0
	+ Added information on the CU1, CU2, CU3, CU4 for SQL Server 2016 SP1 and CU3, CU4, CU5, CU6, CU7 for SQL Server 2016 RTM
	+ Added displaying information on the date of each of the service releases (when using parameter @showNewerVersions)
	+ Added information on the Trace Flag 6404
	* Small changes for taking advantages of SQL Server 2016 syntax
*/

-- Params --
declare @showUnrecognizedTraceFlags bit = 1,		-- Enables showing active trace flags, even if they are not columnstore indexes related
		@identifyCurrentVersion bit = 1,			-- Enables identification of the currently used SQL Server Instance version
		@showNewerVersions bit = 0;					-- Enables showing the SQL Server versions that are posterior the current version
-- end of --

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2016
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server 2016. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end


IF EXISTS (SELECT 1 WHERE SERVERPROPERTY('EngineEdition') <> 3 AND cast(SERVERPROPERTY('ProductLevel') as nvarchar(128)) NOT LIKE 'SP%')
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition or your are not running Service Pack 1 or later for SQL Server 2016. Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


--------------------------------------------------------------------------------------------------------------------
set @SQLServerBuild = substring(@SQLServerVersion,CHARINDEX('.',@SQLServerVersion,5)+1,CHARINDEX('.',@SQLServerVersion,8)-CHARINDEX('.',@SQLServerVersion,5)-1);

drop table IF EXISTS #SQLColumnstoreImprovements;
drop table IF EXISTS #SQLBranches;
drop table IF EXISTS #SQLVersions;

--  
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
	values ('CTP', 200 ), ( 'RC0', 1100 ), ( 'RC1', 1200 ), ( 'RC2', 1300 ), ( 'RC3', 1400 ), ( 'RTM', 1601 ), ( 'SP1', 4001 );

insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
	values 
	( 'CTP', 200, convert(datetime,'27-05-2015',105), 'CTP 2 for SQL Server 2016' ),
	( 'CTP', 300, convert(datetime,'24-06-2015',105), 'CTP 2.1 for SQL Server 2016' ),
	( 'CTP', 400, convert(datetime,'23-07-2015',105), 'CTP 2.2 for SQL Server 2016' ),
	( 'CTP', 500, convert(datetime,'28-08-2015',105), 'CTP 2.3 for SQL Server 2016' ),
	( 'CTP', 600, convert(datetime,'30-09-2015',105), 'CTP 2.4 for SQL Server 2016' ),
	( 'CTP', 700, convert(datetime,'28-10-2015',105), 'CTP 3 for SQL Server 2016' ),
	( 'CTP', 800, convert(datetime,'30-11-2015',105), 'CTP 3.1 for SQL Server 2016' ),
	( 'CTP', 900, convert(datetime,'16-12-2015',105), 'CTP 3.2 for SQL Server 2016' ),
	( 'CTP', 1000, convert(datetime,'03-02-2016',105), 'CTP 3.3 for SQL Server 2016' ),
	( 'RC0', 1100, convert(datetime,'07-03-2016',105), 'RC 0 for SQL Server 2016' ),
	( 'RC1', 1200, convert(datetime,'16-03-2016',105), 'RC 1 for SQL Server 2016' ),
	( 'RC2', 1300, convert(datetime,'01-04-2016',105), 'RC 2 for SQL Server 2016' ),
	( 'RC3', 1400, convert(datetime,'15-04-2016',105), 'RC 3 for SQL Server 2016' ),
	( 'RTM', 1601, convert(datetime,'01-06-2016',105), 'RTM for SQL Server 2016' ),
	( 'RTM', 2149, convert(datetime,'25-07-2016',105), 'CU 1 for SQL Server 2016' ),
	( 'RTM', 2164, convert(datetime,'22-09-2016',105), 'CU 2 for SQL Server 2016' ),
	( 'RTM', 2170, convert(datetime,'26-10-2016',105), 'On-Demand fix for CU 2 for SQL Server 2016' ),
	( 'RTM', 2186, convert(datetime,'17-11-2016',105), 'CU 3 for SQL Server 2016' ),
	( 'RTM', 2193, convert(datetime,'18-01-2017',105), 'CU 4 for SQL Server 2016' ),
	( 'RTM', 2197, convert(datetime,'21-03-2017',105), 'CU 5 for SQL Server 2016' ),
	( 'RTM', 2204, convert(datetime,'15-05-2017',105), 'CU 6 for SQL Server 2016' ),
	( 'RTM', 2210, convert(datetime,'08-08-2017',105), 'CU 7 for SQL Server 2016' ),
	( 'SP1', 4001, convert(datetime,'16-11-2016',105), 'Service Pack 1 for SQL Server 2016' ),
	( 'SP1', 4411, convert(datetime,'18-01-2017',105), 'CU 1 for SQL Server 2016 SP 1' ),
	( 'SP1', 4422, convert(datetime,'22-03-2017',105), 'CU 2 for SQL Server 2016 SP 1' ),
	( 'SP1', 4435, convert(datetime,'15-05-2017',105), 'CU 3 for SQL Server 2016 SP 1' ),
	( 'SP1', 4446, convert(datetime,'08-08-2017',105), 'CU 4 for SQL Server 2016 SP 1' );

insert into #SQLColumnstoreImprovements (BuildVersion, SQLBranch, Description, URL )
	values 
	( 2149, 'RTM', 'FIX: All data goes to deltastores when you bulk load data into a clustered columnstore index under memory pressure', 'https://support.microsoft.com/en-nz/kb/3174073' ),
	( 2149, 'RTM', 'FIX: Online index operations block DML operations when the database contains a clustered columnstore index', 'https://support.microsoft.com/en-nz/kb/3172960' ),
	( 2149, 'RTM', 'FIX: Error 8624 occurs when you run a query against a nonclustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3171544' ),
	( 2149, 'RTM', 'Behavior changes when you add uniqueidentifier columns in a clustered Columnstore Index in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3173436' ),
	( 2149, 'RTM', 'FIX: Incorrect number of rows in sys.partitions for a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3172974' ),
	( 2149, 'RTM', 'FIX: Error 5283 when you run DBCC CHECKDB on a database that contains non-clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3174088' ),
	( 2149, 'RTM', 'Query plan generation improvement for some columnstore queries in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-nz/kb/3146123' ),
	( 2149, 'RTM', 'A query that accesses data in a columnstore index causes the Database Engine to receive a floating point exception in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3171759' ),
	( 2149, 'RTM', 'Adds trace flag 9358 to disable batch mode sort operations in a complex parallel query in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3171555' ),
	( 2149, 'RTM', 'FIX: Can''t disable batch mode sorted by session trace flag 9347 or the query hint QUERYTRACEON 9347 in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3172787' ),
	( 2164, 'RTM', 'Updating while compression is in progress can lead to nonclustered columnstore index corruption in SQL Server 2016', 'https://support.microsoft.com/en-us/kb/3188950' ),
	( 2164, 'RTM', 'Query returns incorrect results from nonclustered columnstore index under snapshot isolation level in SQL Server 2016', 'https://support.microsoft.com/en-us/kb/3189372' ),
	( 2170, 'RTM', 'FIX: SQL Server 2016 crashes when a Tuple Mover task is terminated unexpectedly', 'https://support.microsoft.com/en-us/kb/3195901' ),
	( 2170, 'RTM', 'FIX: Intermittent non-yielding conditions, performance problems and intermittent connectivity failures in SQL Server 2016', 'https://support.microsoft.com/en-us/kb/3189855' ),
	( 2170, 'RTM', 'FIX: Deadlock when you execute a query plan with a nested loop join in batch mode in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/kb/3195825' ),
	( 2170, 'RTM', 'FIX: Performance regression in the expression service during numeric arithmetic operations in SQL Server 2016', 'https://support.microsoft.com/en-us/kb/3197952' ),
	( 2186, 'RTM', 'FIX: SQL Server 2016 crashes when a Tuple Mover task is terminated unexpectedly', 'https://support.microsoft.com/en-us/kb/3195901' ),
	( 2193, 'RTM', 'FIX: "Non-yielding Scheduler" condition when you parallel-load data into a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3205411/fix-non-yielding-scheduler-condition-when-you-parallel-load-data-into-a-columnstore-index-in-sql-server-2016' ),
	( 2193, 'RTM', 'FIX: Cannot insert data into a table that uses a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3211602/fix-cannot-insert-data-into-a-table-that-uses-a-clustered-columnstore-index-in-sql-server-2016' ),
	( 2193, 'RTM', 'FIX: Error 3628 when you create or rebuild a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3213283/fix-error-3628-when-you-create-or-rebuild-a-columnstore-index-in-sql-server-2016' ),
	( 2193, 'RTM', 'FIX: An assertion occurs when you bulk insert data into a table from multiple connections in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3205964/fix-an-assertion-occurs-when-you-bulk-insert-data-into-a-table-from-multiple-connections-in-sql-server-2016' ),
	( 2193, 'RTM', 'FIX: Out-of-memory errors when you execute DBCC CHECKDB on database that contains columnstore indexes in SQL Server', 'https://support.microsoft.com/en-us/help/3201416/fix-out-of-memory-errors-when-you-execute-dbcc-checkdb-on-database-that-contains-columnstore-indexes-in-sql-server-2014' ),
	( 2193, 'RTM', 'FIX: An assert error occurs when you insert data into a memory-optimized table that contains a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3211338/fix-an-assert-error-occurs-when-you-insert-data-into-a-memory-optimized-table-that-contains-a-clustered-columnstore-index-in-sql-server-2016' ),
	( 2197, 'RTM', 'FIX: Wrong number of rows returned in sys.partitions for Columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3195752/fix-wrong-number-of-rows-returned-in-sys-partitions-for-columnstore-in' ),
	( 2197, 'RTM', 'FIX: The sys.column_store_segments catalog view displays incorrect values in the column_id column in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4013118/fix-the-sys-column-store-segments-catalog-view-displays-incorrect-valu' ),
	( 2197, 'RTM', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3067968/fix-memory-is-paged-out-when-columnstore-index-query-consumes-lots-of' ),
	( 2197, 'RTM', 'FIX: Intra-query deadlock when values are inserted into a partitioned clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3204769/fix-intra-query-deadlock-when-values-are-inserted-into-a-partitioned-c' ),
	( 2204, 'RTM', 'FIX: Query against sys.dm_db_partition_stats DMV runs slow if the database contains large numbers of columnstore partitions in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4019903/fix-query-against-sys-dm-db-partition-stats-dmv-runs-slow-if-the-datab' ),
	( 2204, 'RTM', 'FIX: Deadlock when you use sys.column_store_row_groups and sys.dm_db_column_store_row_group_physical_stats DMV with large DDL operations in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4016946/fix-deadlock-when-you-use-sys-column-store-row-groups-and-sys-dm-db-co' ),
	( 2204, 'RTM', 'Intra-query deadlock on communication buffer when you run a bulk load against a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4017154/intra-query-deadlock-on-communication-buffer-when-you-run-a-bulk-load' ),
	( 4001, 'SP1', 'FIX: Deadlock when you execute a query plan with a nested loop join in batch mode in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/kb/3195825' ),
	( 4001, 'SP1', 'Batch sort and optimized nested loop may cause stability and performance issues.', 'https://support.microsoft.com/en-us/kb/3182545' ),
	( 4411, 'SP1', 'FIX: The “sys.dm_db_column_store_row_group_physical_stats” query runs slowly on SQL Server 2016', 'https://support.microsoft.com/en-us/help/3210747/fix-the-sys.dm-db-column-store-row-group-physical-stats-query-runs-slowly-on-sql-server-2016' ),
	( 4411, 'SP1', 'FIX: An assert error occurs when you insert data into a memory-optimized table that contains a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3211338/fix-an-assert-error-occurs-when-you-insert-data-into-a-memory-optimized-table-that-contains-a-clustered-columnstore-index-in-sql-server-2016' ),
	( 4411, 'SP1', 'FIX: Error 3628 when you create or rebuild a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3213283/fix-error-3628-when-you-create-or-rebuild-a-columnstore-index-in-sql-server-2016' ),
	( 4422, 'SP1', 'FIX: Cannot insert data into a table that uses a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3211602' ),
	( 4422, 'SP1', 'FIX: Wrong number of rows returned in sys.partitions for Columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3195752' ),
	( 4422, 'SP1', 'FIX: Deadlock when you execute a query plan with a nested loop join in batch mode in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3195825' ),
	( 4422, 'SP1', 'FIX: Data type conversion error in a query that involves a column store index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4013883' ),
	( 4422, 'SP1', 'FIX: "Non-yielding Scheduler" condition when you parallel-load data into a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3205411' ),
	( 4422, 'SP1', 'FIX: "Incorrect syntax for definition of the ''default'' constraint" error when you add an arbitrary columnstore column in SQL Server 2016', 'https://support.microsoft.com/en-us/help/5852300' ),
	( 4422, 'SP1', 'FIX: Error when you add a NOT NULL column with default values to a non-empty clustered columnstore index in SQL Server 2016 Standard and Express edition', 'https://support.microsoft.com/en-us/help/4013851' ),
	( 4422, 'SP1', 'FIX: Intra-query deadlock when values are inserted into a partitioned clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3204769' ),
	( 4422, 'SP1', 'FIX: The sys.column_store_segments catalog view displays incorrect values in the column_id column in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4013118' ),
	( 4422, 'SP1', 'FIX: Memory is paged out when columnstore index query consumes lots of memory in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3067968' ),
	( 4422, 'SP1', 'FIX: Out-of-memory errors when you execute DBCC CHECKDB on database that contains columnstore indexes in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/3201416' ),
	( 4422, 'SP1', 'FIX: Error 3628 when you create or rebuild a columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3213283' ),
	( 4435, 'SP1', 'FIX: Query against sys.dm_db_partition_stats DMV runs slow if the database contains large numbers of columnstore partitions in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4019903/fix-query-against-sys-dm-db-partition-stats-dmv-runs-slow-if-the-datab' ),
	( 4435, 'SP1', 'FIX: Access violation when you use SELECT TOP query to retrieve data from clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4016902/fix-access-violation-when-you-use-select-top-query-to-retrieve-data-fr' ),
	( 4435, 'SP1', 'FIX: SQL Server 2016 consumes more memory when you reorganize a columnstore index', 'https://support.microsoft.com/en-us/help/4019028/fix-sql-server-2016-consumes-more-memory-when-you-reorganize-a-columns' ),
	( 4435, 'SP1', 'Intra-query deadlock on communication buffer when you run a bulk load against a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4017154/intra-query-deadlock-on-communication-buffer-when-you-run-a-bulk-load' ),
	( 4435, 'SP1', 'FIX: Wrong number of rows returned in sys.partitions for Columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/3195752/fix-wrong-number-of-rows-returned-in-sys-partitions-for-columnstore-in' ),
	( 4435, 'SP1', 'FIX: An assertion occurs when you run an UPDATE statement on a clustered columnstore index in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4015034/fix-an-assertion-occurs-when-you-run-an-update-statement-on-a-clustere' ),
	( 4435, 'SP1', 'FIX: The sys.column_store_segments catalog view displays incorrect values in the column_id column in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4013118/fix-the-sys-column-store-segments-catalog-view-displays-incorrect-valu' ),
	( 4435, 'SP1', 'FIX: Intra-query deadlock when you execute a parallel query that contains outer join operators in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4019718/fix-intra-query-deadlock-when-you-execute-a-parallel-query-that-contai' ),
	( 4446, 'SP1', 'FIX: Deadlock when you use sys.column_store_row_groups and sys.dm_db_column_store_row_group_physical_stats DMV with large DDL operations in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4016946/fix-deadlock-when-you-use-sys-column-store-row-groups-and-sys-dm-db-co' ),
	( 4446, 'SP1', 'FIX: Access violation with query to retrieve data from a clustered columnstore index in SQL Server 2014 or 2016', 'https://support.microsoft.com/en-us/help/4024184/fix-access-violation-with-query-to-retrieve-data-from-a-clustered-colu' ),
	( 4446, 'SP1', 'FIX: Access violation occurs when you run a query in SQL Server 2016', 'https://support.microsoft.com/en-us/help/4034056/fix-access-violation-occurs-when-you-run-a-query-in-sql-server-2016' );
	
	
if @identifyCurrentVersion = 1
begin
	DROP TABLE IF EXISTS #TempVersionResults;

	create table #TempVersionResults(
		MessageText nvarchar(512) NOT NULL,		
		SQLVersionDescription nvarchar(200) NOT NULL,
		SQLBranch char(3) not null,
		SQLVersion smallint NULL,
		ReleaseDate date NULL );

	-- Identify the number of days that has passed since the installed release
	declare @daysSinceLastRelease int = NULL;
	select @daysSinceLastRelease = datediff(dd,max(ReleaseDate),getdate())
		from #SQLVersions
		where SQLVersion = cast(@SQLServerBuild as int);

	-- Get information about current SQL Server Version
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
drop table if exists #ActiveTraceFlags;

create table #ActiveTraceFlags(	
	TraceFlag nvarchar(20) not null,
	Status bit not null,
	Global bit not null,
	Session bit not null );

insert into #ActiveTraceFlags
	exec sp_executesql N'DBCC TRACESTATUS()';

drop table if exists #ColumnstoreTraceFlags;

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
	( 4199, 'The batch mode sort operations in a complex parallel query are also disabled when trace flag 4199 is enabled.', 'https://support.microsoft.com/en-nz/kb/3171555', 1 ),
	( 6404, 'Fixes the amount of memory for ALTER INDEX REORGANIZE on 4GB/16GB depending on the Server size.', 'https://support.microsoft.com/en-us/help/4019028/fix-sql-server-2016-consumes-more-memory-when-you-reorganize-a-columns', 1 ),
	( 9347, 'FIX: Can''t disable batch mode sorted by session trace flag 9347 or the query hint QUERYTRACEON 9347 in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3172787', 1 ),
	( 9349, 'Disables batch mode top sort operator.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
	( 9358, 'Disable batch mode sort operations in a complex parallel query in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3171555', 1 ),
	( 9389, 'Enables dynamic memory grant for batch mode operators', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
	( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2016/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 ),
	( 9354, 'Disables Aggregate Pushdown', '', 0 ),
	(10204, 'Disables merge/recompress during columnstore index reorganization.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
	(10207, 'Skips Corrupted Columnstore Segments (Fixed in CU8 for SQL Server 2014 RTM and CU1 for SQL Server 2014 SP1)', 'https://support.microsoft.com/en-us/kb/3067257', 1 );

select tf.TraceFlag, isnull(conf.Description,'Unrecognized') as Description, isnull(conf.URL,'-') as URL, SupportedStatus
	from #ActiveTraceFlags tf
		left join #ColumnstoreTraceFlags conf
			on conf.TraceFlag = tf.TraceFlag
	where @showUnrecognizedTraceFlags = 1 or (@showUnrecognizedTraceFlags = 0 AND Description is not null);

drop table #ColumnstoreTraceFlags;
drop table #ActiveTraceFlags;

