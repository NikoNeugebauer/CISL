/*
	Columnstore Indexes Scripts Library for SQL Server 2019: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
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

/*
	Known Issues & Limitations: 
		- Custom non-standard (non-CU & non-SP) versions are not targeted yet
*/

/*
Changes in 1.5.0
	+ Added information on the CTP 1.1, 1.2, 1.3 & 1.4, 2.0, 2.1, RC1 & RC2 for the SQL Server vNext (2017 situation)
	+ Added displaying information on the date of each of the service releases (when using parameter @showNewerVersions)
	+ Added information on the Trace Flag 6404
*/

--------------------------------------------------------------------------------------------------------------------
declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
		@SQLServerBuild smallint = NULL;
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2019 or newer
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) < N'15'
begin
	set @errorMessage = (N'You are not running SQL Server 2019 or newer. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end
GO

--------------------------------------------------------------------------------------------------------------------

/*
	Columnstore Indexes Scripts Library for SQL Server vNext: 
	SQL Server Instance Information - Provides with the list of the known SQL Server versions that have bugfixes or improvements over your current version + lists currently enabled trace flags on the instance & session
	Version: 1.5.0, August 2017
*/
create or alter procedure dbo.cstore_GetSQLInfo(
-- Params --
	@showUnrecognizedTraceFlags bit = 1,		-- Enables showing active trace flags, even if they are not columnstore indexes related
	@identifyCurrentVersion bit = 1,			-- Enables identification of the currently used SQL Server Instance version
	@showNewerVersions bit = 0					-- Enables showing the SQL Server versions that are posterior the current version-- end of --
) as 
begin
	declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
			@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128)),
			@SQLServerBuild smallint = NULL;

	set @SQLServerBuild = substring(@SQLServerVersion,CHARINDEX('.',@SQLServerVersion,5)+1,CHARINDEX('.',@SQLServerVersion,8)-CHARINDEX('.',@SQLServerVersion,5)-1);


	drop table if exists #SQLColumnstoreImprovements;
	drop table if exists #SQLBranches;
	drop table if exists #SQLVersions;

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
		values ('CTP', 1900 ), ('RC', 2000 );

	insert #SQLVersions( SQLBranch, SQLVersion, ReleaseDate, SQLVersionDescription )
		values 
		( 'RTM', 2000, convert(datetime,'04-11-2019',105), 'RTM for SQL Server 2019' ),
		( 'RTM', 4003, convert(datetime,'07-01-2020',105), 'CU 1 for SQL Server 2019' ),
		( 'RTM', 4013, convert(datetime,'13-02-2020',105), 'CU 2 for SQL Server 2019' ),
		( 'RTM', 4023, convert(datetime,'12-03-2020',105), 'CU 3 for SQL Server 2019' ),
		( 'RTM', 4033, convert(datetime,'31-03-2020',105), 'CU 4 for SQL Server 2019' ),
		( 'RTM', 4043, convert(datetime,'22-06-2020',105), 'CU 5 for SQL Server 2019' ),
		( 'RTM', 4053, convert(datetime,'04-08-2020',105), 'CU 6 for SQL Server 2019' );
		

	if @identifyCurrentVersion = 1
	begin
		if OBJECT_ID('tempdb..#TempVersionResults') IS NOT NULL
			drop table #TempVersionResults;

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
		( 9347, 'FIX: Can''t disable batch mode sorted by session trace flag 9347 or the query hint QUERYTRACEON 9347 in SQL Server vNext', 'https://support.microsoft.com/en-nz/kb/3172787', 1 ),
		( 9349, 'Disables batch mode top sort operator.', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		( 9358, 'Disable batch mode sort operations in a complex parallel query in SQL Server vNext', 'https://support.microsoft.com/en-nz/kb/3171555', 1 ),
		( 9389, 'Enables dynamic memory grant for batch mode operators', 'https://msdn.microsoft.com/en-us/library/ms188396.aspx', 1 ),
		( 9354, 'Disables Aggregate Pushdown', '', 0 ),
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
