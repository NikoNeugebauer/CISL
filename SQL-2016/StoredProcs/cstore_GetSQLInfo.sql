/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.3.1, July 2016

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
Changes in 1.0.4
	+ Added information about each release date and the number of days since the installed released was published	
	+ Added information on CTP 3.1 & CTP 3.2

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	+ Added information on CTP 3.3, RC0, RC1, RC2, RC3

Changes in 1.3.0
	+ Added information on RTM

Changes in 1.3.1
	+ Added information on CU 1 for SQL Server 2016 RTM
	+ Added information on the new trace flags 9347, 9349, 9358, 9389 & 10204
*/

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

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2016 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetSQLInfo' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetSQLInfo as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2016: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.3.1, July 2016
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
		values ('CTP', 200 ), ( 'RC0', 1100 ), ( 'RC1', 1200 ), ( 'RC2', 1300 ), ( 'RC3', 1400 ), ( 'RTM', 1601 );

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
		( 'RTM', 2149, convert(datetime,'25-07-2016',105), 'CU 1 for SQL Server 2016' ) ;

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
		( 2149, 'RTM', 'FIX: Can''t disable batch mode sorted by session trace flag 9347 or the query hint QUERYTRACEON 9347 in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3172787' );

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
						where  @SQLServerBuild <  SQLVersion;

			select * 
				from #TempVersionResults;

			drop table #TempVersionResults;
		end 

	end

	select imps.BuildVersion, vers.SQLVersionDescription, imps.Description, imps.URL
		from #SQLColumnstoreImprovements imps
			inner join #SQLBranches branch
				on imps.SQLBranch = branch.SQLBranch
			inner join #SQLVersions vers
				on imps.BuildVersion = vers.SQLVersion
		where BuildVersion > @SQLServerBuild 
			and branch.SQLBranch = ServerProperty('ProductLevel')
			and branch.MinVersion < BuildVersion;

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
		( 9347, 'FIX: Can''t disable batch mode sorted by session trace flag 9347 or the query hint QUERYTRACEON 9347 in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3172787', 1 ),
		( 9349, 'Disables batch mode top sort operator.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		( 9358, 'Disable batch mode sort operations in a complex parallel query in SQL Server 2016', 'https://support.microsoft.com/en-nz/kb/3171555', 1 ),
		( 9389, 'Enables dynamic memory grant for batch mode operators', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		( 9453, 'Disables Batch Execution Mode', 'http://www.nikoport.com/2016/07/24/clustered-columnstore-indexes-part-35-trace-flags-query-optimiser-rules/', 1 ),
		(10204, 'Disables merge/recompress during columnstore index reorganization.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
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
