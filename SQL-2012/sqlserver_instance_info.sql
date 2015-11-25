/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.0.3, November 2015

	Copyright 2015 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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
Changes in 1.0.1
	+ Added drops for the existing temp tables: #SQLColumnstoreImprovements, #SQLBranches, #SQLVersions
	+ Added new parameter for Enables showing the SQL Server versions that are posterior the current version
	* Added more source code description in the comments
	+ Removed some redundant information (column UpdateName from the #SQLColumnstoreImprovements) which were left from the very early versions
	+ Added information about CU8 for SQL Server 2012 SP 2

Changes in 1.0.2
	+ Added column with the CU Version for the Bugfixes output
	* Updated temporary tables in order to avoid error messages

Changes in 1.0.3
	+ Added information about CU9 for SQL Server 2012 SP 2
	+ Added information about SQL Server 2012 SP 3
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

-- Ensure that we are running SQL Server 2012
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'11'
begin
	set @errorMessage = (N'You are not running a SQL Server 2012. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2012 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


--------------------------------------------------------------------------------------------------------------------
set @SQLServerBuild = substring(@SQLServerVersion,CHARINDEX('.',@SQLServerVersion,5)+1,CHARINDEX('.',@SQLServerVersion,8)-CHARINDEX('.',@SQLServerVersion,5)-1);

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
	SQLVersionDescription nvarchar(100) );

insert into #SQLBranches (SQLBranch, MinVersion)
	values ('RTM', 2100 ), ('SP1', 3000), ('SP2', 5058), ('SP3', 6020);

insert #SQLVersions( SQLBranch, SQLVersion, SQLVersionDescription )
	values 
	( 'RTM', 2000, 'SQL Server 2012 RTM' ),
	( 'RTM', 2316, 'CU 1 for SQL Server 2012 RTM' ),
	( 'RTM', 2325, 'CU 2 for SQL Server 2012 RTM' ),
	( 'RTM', 2332, 'CU 3 for SQL Server 2012 RTM' ),
	( 'RTM', 2383, 'CU 4 for SQL Server 2012 RTM' ),
	( 'RTM', 2395, 'CU 5 for SQL Server 2012 RTM' ),
	( 'RTM', 2401, 'CU 6 for SQL Server 2012 RTM' ),
	( 'RTM', 2405, 'CU 7 for SQL Server 2012 RTM' ),
	( 'RTM', 2410, 'CU 8 for SQL Server 2012 RTM' ),
	( 'RTM', 2419, 'CU 9 for SQL Server 2012 RTM' ),
	( 'RTM', 2420, 'CU 10 for SQL Server 2012 RTM' ),
	( 'RTM', 2424, 'CU 11 for SQL Server 2012 RTM' ),
	( 'SP1', 3000, 'SQL Server 2012 SP1' ),
	( 'SP1', 3321, 'CU 1 for SQL Server 2012 SP1' ),
	( 'SP1', 3339, 'CU 2 for SQL Server 2012 SP1' ),
	( 'SP1', 3349, 'CU 3 for SQL Server 2012 SP1' ),
	( 'SP1', 3368, 'CU 4 for SQL Server 2012 SP1' ),
	( 'SP1', 3373, 'CU 5 for SQL Server 2012 SP1' ),
	( 'SP1', 3381, 'CU 6 for SQL Server 2012 SP1' ),
	( 'SP1', 3393, 'CU 7 for SQL Server 2012 SP1' ),
	( 'SP1', 3401, 'CU 8 for SQL Server 2012 SP1' ),
	( 'SP1', 3412, 'CU 9 for SQL Server 2012 SP1' ),
	( 'SP1', 3431, 'CU 10 for SQL Server 2012 SP1' ),
	( 'SP1', 3449, 'CU 11 for SQL Server 2012 SP1' ),
	( 'SP1', 3470, 'CU 12 for SQL Server 2012 SP1' ),
	( 'SP1', 3482, 'CU 13 for SQL Server 2012 SP1' ),
	( 'SP1', 3486, 'CU 14 for SQL Server 2012 SP1' ),
	( 'SP1', 3487, 'CU 15 for SQL Server 2012 SP1' ),
	( 'SP1', 3492, 'CU 16 for SQL Server 2012 SP1' ),
	( 'SP1', 5058, 'SQL Server 2012 SP2' ),
	( 'SP2', 5532, 'CU 1 for SQL Server 2012 SP2' ),
	( 'SP2', 5548, 'CU 2 for SQL Server 2012 SP2' ),
	( 'SP2', 5556, 'CU 3 for SQL Server 2012 SP2' ),
	( 'SP2', 5569, 'CU 4 for SQL Server 2012 SP2' ),
	( 'SP2', 5582, 'CU 5 for SQL Server 2012 SP2' ),
	( 'SP2', 5592, 'CU 6 for SQL Server 2012 SP2' ),
	( 'SP2', 5623, 'CU 7 for SQL Server 2012 SP2' ),
	( 'SP2', 5634, 'CU 8 for SQL Server 2012 SP2' ),
	( 'SP2', 5641, 'CU 9 for SQL Server 2012 SP2' ),
	( 'SP3', 6020, 'SQL Server 2012 SP3' );

insert into #SQLColumnstoreImprovements (BuildVersion, SQLBranch, Description, URL )
	values 
	( 2325, 'RTM', 'FIX: An access violation occurs intermittently when you run a query against a table that has a columnstore index in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2711683' ),
	( 2332, 'RTM', 'FIX: Incorrect results when you run a parallel query that uses a columnstore index in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2703193' ),
	( 2332, 'RTM', 'FIX: Access violation when you try to build a columnstore index for a table in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2708786' ), 
	( 3321, 'SP1', 'FIX: Incorrect results when you run a parallel query that uses a columnstore index in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2703193' ),
	( 3321, 'SP1', 'FIX: Access violation when you try to build a columnstore index for a table in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2708786' ),
	( 3368, 'SP1', 'FIX: Out of memory error when you build a columnstore index on partitioned tables in SQL Server 2012', 'https://support.microsoft.com/en-us/kb/2834062' ), 
	( 3470, 'SP1',  'FIX: Some columns in sys.column_store_segments view show NULL value when the table has non-dbo schema in SQL Server', 'https://support.microsoft.com/en-us/kb/2989704' ),
	( 5548, 'SP2', 'FIX: UPDATE STATISTICS performs incorrect sampling and processing for a table with columnstore index in SQL Server', 'https://support.microsoft.com/en-us/kb/2986627' ),
	( 5548, 'SP2', 'FIX: Some columns in sys.column_store_segments view show NULL value when the table has non-dbo schema in SQL Server', 'https://support.microsoft.com/en-us/kb/2989704' );	


if @identifyCurrentVersion = 1
begin
	if OBJECT_ID('tempdb..#TempVersionResults') IS NOT NULL
		drop table #TempVersionResults;

	create table #TempVersionResults(
		MessageText nvarchar(512) NOT NULL,		
		SQLVersionDescription nvarchar(200) NOT NULL,
		SQLBranch char(3) not null,
		SQLVersion smallint NULL );

	-- Get information about current SQL Server Version
	if( exists (select 1
					from #SQLVersions
					where SQLVersion = cast(@SQLServerBuild as int) ) )
		select 'You are Running:' as MessageText, SQLVersionDescription, SQLBranch, SQLVersion as BuildVersion
			from #SQLVersions
			where SQLVersion = cast(@SQLServerBuild as int);
	else
		select 'You are Running a Non RTM/SP/CU standard version:' as MessageText, '-' as SQLVersionDescription, 
			ServerProperty('ProductLevel') as SQLBranch, @SQLServerBuild as SQLVersion;
			
	
	-- Select information about all newer SQL Server versions that are known
	if @showNewerVersions = 1
	begin 
		insert into #TempVersionResults
			select 'Available Newer Versions:' as MessageText, '' as SQLVersionDescription, 
				'' as SQLBranch, NULL as BuildVersion
			UNION ALL
			select '' as MessageText, SQLVersionDescription as SQLVersionDescription, 
					SQLBranch as SQLVersionDescription, SQLVersion as BuildVersion
					from #SQLVersions
					where  @SQLServerBuild <  SQLVersion;

		select * 
			from #TempVersionResults;

		drop table #TempVersionResults;
	end 

	
end

-- Select all known bugfixes that are applied to the newer versions of SQL Server
select imps.BuildVersion, vers.SQLVersionDescription, imps.Description, imps.URL
	from #SQLColumnstoreImprovements imps
		inner join #SQLBranches branch
			on imps.SQLBranch = branch.SQLBranch
		inner join #SQLVersions vers
			on imps.BuildVersion = vers.SQLVersion
	where BuildVersion > @SQLServerBuild 
		and branch.SQLBranch = ServerProperty('ProductLevel')
		and branch.MinVersion < BuildVersion;

-- Drop used temporary tables
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
	(  834, 'Enable Large Pages', 'https://support.microsoft.com/en-us/kb/920093?wa=wsignin1.0', 0 ),
	(  646, 'Gets text output messages that show what segments (row groups) were eliminated during query processing', 'http://social.technet.microsoft.com/wiki/contents/articles/5611.verifying-columnstore-segment-elimination.aspx', 1 ),
	( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2014/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 );

select tf.TraceFlag, isnull(conf.Description,'Unrecognized') as Description, isnull(conf.URL,'-') as URL, SupportedStatus
	from #ActiveTraceFlags tf
		left join #ColumnstoreTraceFlags conf
			on conf.TraceFlag = tf.TraceFlag
	where @showUnrecognizedTraceFlags = 1 or (@showUnrecognizedTraceFlags = 0 AND Description is not null);

drop table #ColumnstoreTraceFlags;
drop table #ActiveTraceFlags;
