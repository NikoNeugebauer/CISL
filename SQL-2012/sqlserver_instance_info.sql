/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.3.1, August 2016

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

Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published	

Changes in 1.1.1
	+ Added information about CU10 for SQL Server 2012 SP 2
	+ Added information about CU1 for SQL Server 2012 SP 3

Changes in 1.2.0
	+ Added information about CU 11 for SQL Server 2012 SP 2
	+ Added information about CU 2 for SQL Server 2012 SP 3

Changes in 1.3.0
	+ Added information about CU 12 & CU 13 for SQL Server 2012 SP 2
	+ Added information about CU 3 & CU 4 for SQL Server 2012 SP 3
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
	values ('RTM', 2100 ), ('SP1', 3000), ('SP2', 5058), ('SP3', 6020);

insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
	values 
	( 'RTM', 2000, convert(datetime,'06-03-2012',105), 'SQL Server 2012 RTM' ),
	( 'RTM', 2316, convert(datetime,'12-04-2012',105), 'CU 1 for SQL Server 2012 RTM' ),
	( 'RTM', 2325, convert(datetime,'18-06-2012',105), 'CU 2 for SQL Server 2012 RTM' ),
	( 'RTM', 2332, convert(datetime,'29-08-2012',105), 'CU 3 for SQL Server 2012 RTM' ),
	( 'RTM', 2383, convert(datetime,'18-10-2012',105), 'CU 4 for SQL Server 2012 RTM' ),
	( 'RTM', 2395, convert(datetime,'18-12-2012',105), 'CU 5 for SQL Server 2012 RTM' ),
	( 'RTM', 2401, convert(datetime,'18-02-2013',105), 'CU 6 for SQL Server 2012 RTM' ),
	( 'RTM', 2405, convert(datetime,'15-04-2013',105), 'CU 7 for SQL Server 2012 RTM' ),
	( 'RTM', 2410, convert(datetime,'18-06-2013',105), 'CU 8 for SQL Server 2012 RTM' ),
	( 'RTM', 2419, convert(datetime,'21-08-2013',105), 'CU 9 for SQL Server 2012 RTM' ),
	( 'RTM', 2420, convert(datetime,'21-10-2013',105), 'CU 10 for SQL Server 2012 RTM' ),
	( 'RTM', 2424, convert(datetime,'17-12-2013',105), 'CU 11 for SQL Server 2012 RTM' ),
	( 'SP1', 3000, convert(datetime,'06-11-2012',105), 'SQL Server 2012 SP1' ),
	( 'SP1', 3321, convert(datetime,'20-11-2012',105), 'CU 1 for SQL Server 2012 SP1' ),
	( 'SP1', 3339, convert(datetime,'25-01-2013',105), 'CU 2 for SQL Server 2012 SP1' ),
	( 'SP1', 3349, convert(datetime,'18-03-2013',105), 'CU 3 for SQL Server 2012 SP1' ),
	( 'SP1', 3368, convert(datetime,'31-05-2013',105), 'CU 4 for SQL Server 2012 SP1' ),
	( 'SP1', 3373, convert(datetime,'16-07-2013',105), 'CU 5 for SQL Server 2012 SP1' ),
	( 'SP1', 3381, convert(datetime,'16-09-2013',105), 'CU 6 for SQL Server 2012 SP1' ),
	( 'SP1', 3393, convert(datetime,'18-11-2013',105), 'CU 7 for SQL Server 2012 SP1' ),
	( 'SP1', 3401, convert(datetime,'20-01-2014',105), 'CU 8 for SQL Server 2012 SP1' ),
	( 'SP1', 3412, convert(datetime,'18-03-2014',105), 'CU 9 for SQL Server 2012 SP1' ),
	( 'SP1', 3431, convert(datetime,'19-05-2014',105), 'CU 10 for SQL Server 2012 SP1' ),
	( 'SP1', 3449, convert(datetime,'21-07-2014',105), 'CU 11 for SQL Server 2012 SP1' ),
	( 'SP1', 3470, convert(datetime,'15-09-2014',105), 'CU 12 for SQL Server 2012 SP1' ),
	( 'SP1', 3482, convert(datetime,'17-11-2014',105), 'CU 13 for SQL Server 2012 SP1' ),
	( 'SP1', 3486, convert(datetime,'19-01-2015',105), 'CU 14 for SQL Server 2012 SP1' ),
	( 'SP1', 3487, convert(datetime,'16-03-2015',105), 'CU 15 for SQL Server 2012 SP1' ),
	( 'SP1', 3492, convert(datetime,'18-05-2015',105), 'CU 16 for SQL Server 2012 SP1' ),
	( 'SP2', 5058, convert(datetime,'10-06-2014',105), 'SQL Server 2012 SP2' ),
	( 'SP2', 5532, convert(datetime,'24-07-2014',105), 'CU 1 for SQL Server 2012 SP2' ),
	( 'SP2', 5548, convert(datetime,'15-09-2014',105), 'CU 2 for SQL Server 2012 SP2' ),
	( 'SP2', 5556, convert(datetime,'17-11-2014',105), 'CU 3 for SQL Server 2012 SP2' ),
	( 'SP2', 5569, convert(datetime,'20-01-2015',105), 'CU 4 for SQL Server 2012 SP2' ),
	( 'SP2', 5582, convert(datetime,'16-03-2015',105), 'CU 5 for SQL Server 2012 SP2' ),
	( 'SP2', 5592, convert(datetime,'19-05-2015',105), 'CU 6 for SQL Server 2012 SP2' ),
	( 'SP2', 5623, convert(datetime,'20-07-2015',105), 'CU 7 for SQL Server 2012 SP2' ),
	( 'SP2', 5634, convert(datetime,'21-09-2015',105), 'CU 8 for SQL Server 2012 SP2' ),
	( 'SP2', 5641, convert(datetime,'18-11-2015',105), 'CU 9 for SQL Server 2012 SP2' ),
	( 'SP2', 5643, convert(datetime,'19-01-2016',105), 'CU 10 for SQL Server 2012 SP2' ),
	( 'SP2', 5646, convert(datetime,'22-03-2016',105), 'CU 11 for SQL Server 2012 SP2' ),
	( 'SP2', 5649, convert(datetime,'17-05-2016',105), 'CU 12 for SQL Server 2012 SP2' ),
	( 'SP2', 5644, convert(datetime,'18-07-2016',105), 'CU 13 for SQL Server 2012 SP2' ),
	( 'SP3', 6020, convert(datetime,'23-11-2015',105), 'SQL Server 2012 SP3' ),
	( 'SP3', 6518, convert(datetime,'19-01-2016',105), 'CU 1 for SQL Server 2012 SP3' ),
	( 'SP3', 6523, convert(datetime,'22-03-2016',105), 'CU 2 for SQL Server 2012 SP3' ),
	( 'SP3', 6537, convert(datetime,'17-05-2016',105), 'CU 3 for SQL Server 2012 SP3' ),
	( 'SP3', 6540, convert(datetime,'18-07-2016',105), 'CU 4 for SQL Server 2012 SP3' );

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

	-- Identify the number of days that has passed since the installed release
	declare @daysSinceLastRelease int = NULL;
	select @daysSinceLastRelease = datediff(dd,max(ReleaseDate),getdate())
		from #SQLVersions
		where SQLBranch = ServerProperty('ProductLevel')
			and SQLVersion = cast(@SQLServerBuild as int);

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
			select 'Available Newer Versions:' as MessageText, '' as SQLVersionDescription, 
				'' as SQLBranch, NULL as BuildVersion
			UNION ALL
			select '' as MessageText, SQLVersionDescription as SQLVersionDescription, 
					SQLBranch as SQLVersionDescription, SQLVersion as BuildVersion
					from #SQLVersions
					where  @SQLServerBuild <  SQLVersion
			UNION ALL
			select '' as MessageText, SQLVersionDescription as SQLVersionDescription, 
					SQLBranch as SQLVersionDescription, SQLVersion as BuildVersion
					from #SQLVersions
					where  @SQLServerBuild <  SQLVersion;

		select * 
			from #TempVersionResults
			order by isnull(SQLVersion,-1);

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
