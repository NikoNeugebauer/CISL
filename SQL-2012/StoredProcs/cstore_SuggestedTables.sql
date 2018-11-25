/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.6.0, January 2018

	Copyright 2015-2018 Niko Neugebauer, OH22 IS (http://www.nikoport.com/columnstore/), (http://www.oh22.is/)

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
	- @showTSQLCommandsBeta parameter is in alpha Version: 1.6.0, January 2018 and not pretending to be complete any time soon. This output is provided as a basic help & guide convertion to Columnstore Indexes.
	- CLR support is not included or tested
	- Output [Min RowGroups] is not taking present partitions into calculations yet :)
	- Data Precision is not being taken into account

Changes in 1.0.3
	* Changed the name of the @tableNamePattern to @tableName to follow the same standard across all CISL functions	

Changes in 1.0.4
	- Bug fixes for the Nonclustered Columnstore Indexes creation conditions
	- Buf fixes for the data types of the monitored functionalities, that in certain condition would give an error message
	- Bug fix for displaying the same primary key index twice in the T-SQL drop script

Changes in 1.1.0
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL Version: 1.6.0, January 2018 can be easily determined.

Changes in 1.2.0
	- Fixed displaying wrong number of rows for the found suggested tables
	- Fixed error for filtering out the secondary nonclustered indexes in some bigger databases
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added information about the converted table location (Disk-Based)

Changes in 1.3.1
	- Fixed a bug with filtering out the exact number of @minRows instead of including it
	- Fixed a cast bug, that would filter out some of the indexes, based on the casting of the hidden numbers (3rd number behind the comma)
	+ Added new parameter for the index location (@indexLocation) with one actual usable parameter for this SQL Server Version: 1.6.0, January 2018 'Disk-Based'.

Changes in 1.5.0
	+ Added new parameter for the searching precise name of the object (@preciseSearch)
	+ Added new parameter for the identifying the object by its object_id (@objectId)
	+ Expanded search of the schema to include the pattern search with @preciseSearch = 0
	- Fixed bug with the partitioned table not showing the correct number of rows
	+ Added new result column [Partitions] showing the total number of the partitions

Changes in 1.6.0
	- Fixed the bug with the data type of the [Min RowGroups] column from SMALLINT to INT (Thanks to Thorsten)
	- Fixed the bug with the total number of computed columns per database being shown instead of the number of computed columns per table
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server 2012
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'11'
begin
	set @errorMessage = (N'You are not running a SQL Server 2012. Your SQL Server is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

if SERVERPROPERTY('EngineEdition') <> 3 
begin
	set @errorMessage = (N'Your SQL Server 2012 Edition is not an Enterprise or a Developer Edition: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end


--------------------------------------------------------------------------------------------------------------------
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_SuggestedTables' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_SuggestedTables as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	Suggested Tables - Lists tables which potentially can be interesting for implementing Columnstore Indexes
	Version: 1.6.0, January 2018
*/
alter procedure dbo.cstore_SuggestedTables(
-- Params --
		@minRowsToConsider bigint = 500000,							-- Minimum number of rows for a table to be considered for the suggestion inclusion
		@minSizeToConsiderInGB Decimal(16,3) = 0.00,				-- Minimum size in GB for a table to be considered for the suggestion inclusion
		@schemaName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified schema
		@tableName nvarchar(256) = NULL,							-- Allows to show data filtered down to the specified table name pattern
		@preciseSearch bit = 0,										-- Defines if the schema and data search with the parameters @schemaName & @tableName will be precise or pattern-like
		@objectId INT = NULL,										-- Allows to show data filtered down to the specific object_id
		@indexLocation varchar(15) = NULL,							-- Allows to filter tables based on their location: Disk-Based & In-Memory
		@considerColumnsOver8K bit = 1,								-- Include in the results tables, which columns sum extends over 8000 bytes (and thus not supported in Columnstore)
		@showReadyTablesOnly bit = 0,								-- Shows only those Rowstore tables that can already get Columnstore Index without any additional work
		@showUnsupportedColumnsDetails bit = 0,						-- Shows a list of all Unsupported from the listed tables
		@showTSQLCommandsBeta bit = 0								-- Shows a list with Commands for dropping the objects that prevent Columnstore Index creation
-- end of --
) as 
begin
	set nocount on;

	-- Returns tables suggested for using Columnstore Indexes for the DataWarehouse environments
	if OBJECT_ID('tempdb..#TablesToColumnstore') IS NOT NULL
		drop table #TablesToColumnstore;

	create table #TablesToColumnstore(
		[ObjectId] int NOT NULL PRIMARY KEY,
		[TableLocation] varchar(15) NOT NULL,
		[TableName] nvarchar(1000) NOT NULL,
		[ShortTableName] nvarchar(256) NOT NULL,
		[Partitions] BIGINT NOT NULL,
		[Row Count] bigint NOT NULL,
		[Min RowGroups] INT NOT NULL,
		[Size in GB] decimal(16,3) NOT NULL,
		[Cols Count] smallint NOT NULL,
		[String Cols] smallint NOT NULL,
		[Sum Length] int NOT NULL,
		[Unsupported] smallint NOT NULL,
		[LOBs] smallint NOT NULL,
		[Computed] smallint NOT NULL,
		[Clustered Index] tinyint NOT NULL,
		[Nonclustered Indexes] smallint NOT NULL,
		[XML Indexes] smallint NOT NULL,
		[Spatial Indexes] smallint NOT NULL,
		[Primary Key] tinyint NOT NULL,
		[Foreign Keys] smallint NOT NULL,
		[Unique Constraints] smallint NOT NULL,
		[Triggers] smallint NOT NULL,
		[RCSI] tinyint NOT NULL,
		[Snapshot] tinyint NOT NULL,
		[CDC] tinyint NOT NULL,
		[CT] tinyint NOT NULL,
		[Replication] tinyint NOT NULL,
		[FileStream] tinyint NOT NULL,
		[FileTable] tinyint NOT NULL
	);

	insert into #TablesToColumnstore
	select t.object_id as [ObjectId]
		, 'Disk-Based'
		, quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name(t.object_id)) as 'TableName'
		, replace(object_name(t.object_id),' ', '') as 'ShortTableName'
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 	
		, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
		, (select count(*) from sys.columns as col
			where t.object_id = col.object_id ) as 'Cols Count'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
		   ) as 'String Cols'
		, (select sum(col.max_length) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ) as 'Sum Length'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
					  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as 'Unsupported'
		, (select count(*) 
				from sys.columns as col
					inner join sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as 'LOBs'
		, (select count(*) 
				from sys.columns as col
				where is_computed = 1 AND col.object_id = t.object_id  ) as 'Computed'
		, (select count(*)
				from sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
		, (select count(*)
				from sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
		, (select count(*)
				from sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
		, (select count(*)
				from sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
		, (select count(*)
				from sys.objects
				where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
		, (select count(*)
				from sys.objects
				where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
		, (select count(*)
				from sys.objects
				where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
		, (select count(*)
				from sys.objects
				where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
		, 0 as 'RCSI'
		, 0 as 'Snapshot'
		, t.is_tracked_by_cdc as 'CDC'
		, (select count(*) 
				from sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) as 'CT'
		, t.is_replicated as 'Replication'
		, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
		, t.is_filetable as 'FileTable'
		from sys.tables t
			inner join sys.partitions as p 
				ON t.object_id = p.object_id
			inner join sys.allocation_units as a 
				ON p.partition_id = a.container_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id) like '%' + @tableName + '%') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id ) like '%' + @schemaName + '%')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from sys.columns as col
							inner join sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					and (select count(*)
							from sys.objects so
							where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
					and (select count(*)
							from sys.indexes ind
							where t.object_id = ind.object_id
								and ind.type in (3,4) ) = 0
					and (select count(*) 
							from sys.change_tracking_tables ctt with(READUNCOMMITTED)
							where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
									and DB_ID() in (select database_id from sys.change_tracking_databases ctdb)) = 0
					and t.is_tracked_by_cdc = 0
					--and t.is_memory_optimized = 0
					and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, t.is_tracked_by_cdc, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from sys.columns as col
						inner join sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB)
				and 0 = case isnull(@indexLocation,'Null') 
								when 'In-Memory' then 1 
								when 'Disk-Based' then 0 
								when 'Null' then 0
						else 255 end
	union all
	select t.object_id as [ObjectId]
		, 'Disk-Based'
		, quotename(object_schema_name(t.object_id,db_id('tempdb'))) + '.' + quotename(object_name(t.object_id,db_id('tempdb'))) as 'TableName'
		, replace(object_name(t.object_id,db_id('tempdb')),' ', '') as 'ShortTableName'
		, COUNT(DISTINCT p.partition_number) as [Partitions]
		, isnull(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END),0) as 'Row Count'
		, ceiling(SUM(CASE WHEN p.index_id < 2 THEN p.rows ELSE 0 END)/1045678.) as 'Min RowGroups' 
		, cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) as 'size in GB'
		, (select count(*) from tempdb.sys.columns as col
			where t.object_id = col.object_id ) as 'Cols Count'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR','SYSNAME') 
		   ) as 'String Cols'
		, (select sum(col.max_length) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id 
		  ) as 'Sum Length'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
					  (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
					 )
		   ) as 'Unsupported'
		, (select count(*) 
				from tempdb.sys.columns as col
					inner join tempdb.sys.types as tp
						on col.user_type_id = tp.user_type_id
				where t.object_id = col.object_id and 
					 (UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
		   ) as 'LOBs'
		, (select count(*) 
				from tempdb.sys.columns as col
				where is_computed = 1 AND col.object_id = t.object_id ) as 'Computed'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 1 AND ind.object_id = t.object_id ) as 'Clustered Index'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 2 AND ind.object_id = t.object_id ) as 'Nonclustered Indexes'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 3 AND ind.object_id = t.object_id ) as 'XML Indexes'
		, (select count(*)
				from tempdb.sys.indexes ind
				where type = 4 AND ind.object_id = t.object_id ) as 'Spatial Indexes'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) = 'PK' AND parent_object_id = t.object_id ) as 'Primary Key'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) = 'F' AND parent_object_id = t.object_id ) as 'Foreign Keys'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) in ('UQ') AND parent_object_id = t.object_id ) as 'Unique Constraints'
		, (select count(*)
				from tempdb.sys.objects
				where UPPER(type) in ('TA','TR') AND parent_object_id = t.object_id ) as 'Triggers'
		, 0 as 'RCSI'
		, 0 as 'Snapshot'
		, t.is_tracked_by_cdc as 'CDC'
		, (select count(*) 
				from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
				where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
					  and DB_ID() in (select database_id from tempdb.sys.change_tracking_databases ctdb)) as 'CT'
		, t.is_replicated as 'Replication'
		, coalesce(t.filestream_data_space_id,0,1) as 'FileStream'
		, t.is_filetable as 'FileTable'
		from tempdb.sys.tables t
			inner join tempdb.sys.partitions as p 
				ON t.object_id = p.object_id
			inner join tempdb.sys.allocation_units as a 
				ON p.partition_id = a.container_id
		where p.data_compression in (0,1,2) -- None, Row, Page
			 and (select count(*)
					from tempdb.sys.indexes ind
					where t.object_id = ind.object_id
						and ind.type in (5,6) ) = 0
			 and (@preciseSearch = 0 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) like '%' + @tableName + '%') 
				  OR @preciseSearch = 1 AND (@tableName is null or object_name (t.object_id,db_id('tempdb')) = @tableName) )
			 and (@preciseSearch = 0 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) like '%' + @schemaName + '%')
				  OR @preciseSearch = 1 AND (@schemaName is null or object_schema_name( t.object_id,db_id('tempdb') ) = @schemaName))
			 AND (ISNULL(@objectId,t.object_id) = t.object_id)
			 and (( @showReadyTablesOnly = 1 
					and  
					(select count(*) 
						from tempdb.sys.columns as col
							inner join tempdb.sys.types as tp
								on col.system_type_id = tp.system_type_id
						where t.object_id = col.object_id and 
								(UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY'))
						) = 0 
					and (select count(*)
							from tempdb.sys.objects so
							where UPPER(so.type) in ('PK','F','UQ','TA','TR') and parent_object_id = t.object_id ) = 0
					and (select count(*)
							from tempdb.sys.indexes ind
							where t.object_id = ind.object_id
								and ind.type in (3,4) ) = 0
					and (select count(*) 
							from tempdb.sys.change_tracking_tables ctt with(READUNCOMMITTED)
							where ctt.object_id = t.object_id and ctt.is_track_columns_updated_on = 1 
									and DB_ID() in (select database_id from tempdb.sys.change_tracking_databases ctdb)) = 0
					and t.is_tracked_by_cdc = 0
					--and t.is_memory_optimized = 0
					and t.is_replicated = 0
					and coalesce(t.filestream_data_space_id,0,1) = 0
					and t.is_filetable = 0
				  )
				 or @showReadyTablesOnly = 0)
		group by t.object_id, t.is_tracked_by_cdc, t.is_filetable, t.is_replicated, t.filestream_data_space_id
		having sum(p.rows) >= @minRowsToConsider 
				and
				(((select sum(col.max_length) 
					from tempdb.sys.columns as col
						inner join tempdb.sys.types as tp
							on col.system_type_id = tp.system_type_id
					where t.object_id = col.object_id 
				  ) < 8000 and @considerColumnsOver8K = 0 ) 
				  OR
				 @considerColumnsOver8K = 1 )
				and 
				(cast( sum(a.total_pages) * 8.0 / 1024. / 1024 as decimal(16,3)) >= @minSizeToConsiderInGB)
				and 0 = case isnull(@indexLocation,'Null') 
									when 'In-Memory' then 1 
									when 'Disk-Based' then 0 
									when 'Null' then 0
							else 255 end


	-- Show the found results
	select case when ([Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) <= 0 then 'Nonclustered Columnstore'  
				 when ([Primary Key] + [Foreign Keys] + [Unique Constraints] + [Triggers] + [CDC] + [CT] +
					  [Replication] + [FileStream] + [FileTable] + [Unsupported] 
					  - ([LOBs] + [Computed])) > 0 then 'None' 
		   end as 'Compatible With'
		, [TableLocation]
		, [TableName], [Partitions], [Row Count], [Min RowGroups], [Size in GB], [Cols Count], [String Cols], [Sum Length], [Unsupported], [LOBs], [Computed]
		, [Clustered Index], [Nonclustered Indexes], [XML Indexes], [Spatial Indexes], [Primary Key], [Foreign Keys], [Unique Constraints]
		, [Triggers], [RCSI], [Snapshot], [CDC], [CT], [Replication], [FileStream], [FileTable]
		from #TablesToColumnstore
		order by [Row Count] desc;

	if( @showUnsupportedColumnsDetails = 1 ) 
	begin
		select quotename(object_schema_name(t.object_id)) + '.' + quotename(object_name (t.object_id)) as 'TableName',
			col.name as 'Unsupported Column Name',
			tp.name as 'Data Type',
			col.max_length as 'Max Length',
			col.precision as 'Precision',
			col.is_computed as 'Computed'
			from sys.tables t
				inner join sys.columns as col
					on t.object_id = col.object_id 
				inner join sys.types as tp
					on col.user_type_id = tp.user_type_id 
				where  ((UPPER(tp.name) in ('TEXT','NTEXT','TIMESTAMP','HIERARCHYID','SQL_VARIANT','XML','GEOGRAPHY','GEOMETRY') OR
						(UPPER(tp.name) in ('VARCHAR','NVARCHAR','CHAR','NCHAR') and (col.max_length = 8000 or col.max_length = -1)) 
						) 
						OR col.is_computed = 1 )
				 and t.object_id in (select ObjectId from #TablesToColumnstore);
	end

	if( @showTSQLCommandsBeta = 1 ) 
	begin
		select coms.TableName, coms.[TSQL Command], coms.[type] 
			from (
				select t.TableName, 
						'create nonclustered columnstore index ' + 
						'NCCI'  
						+ '_' + t.[ShortTableName] + 
						' on ' + t.TableName + '()' + ';' as [TSQL Command]
					   , 'CCL' as type,
					   101 as [Sort Order]
					from #TablesToColumnstore t
				union all
				select t.TableName, 'alter table ' + t.TableName + ' drop constraint ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], [type], 
					   case UPPER(type) when 'PK' then 100 when 'F' then 1 when 'UQ' then 100 end as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('PK','F','UQ')
				union all
				select t.TableName, 'drop trigger ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ';' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('TR')
				union all
				select t.TableName, 'drop assembly ' + (quotename(so.name) collate SQL_Latin1_General_CP1_CI_AS) + ' WITH NO DEPENDENTS ;' as [TSQL Command], type,
					50 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.objects so
						on t.ObjectId = so.parent_object_id
					where UPPER(type) in ('TA')	
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'CL' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 1 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in ('PK','F','UQ')
								and quotename(ind.name) <> quotename(so1.name))
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'NC' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 2 and not exists
						(select 1 from #TablesToColumnstore t1
							inner join sys.objects so1
								on t1.ObjectId = so1.parent_object_id
							where UPPER(so1.type) in ('PK','F','UQ')
								and quotename(ind.name) <> quotename(so1.name) and t.ObjectId = t1.ObjectId )
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'XML' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 3
				union all 
				select t.TableName, 'drop index ' + (quotename(ind.name) collate SQL_Latin1_General_CP1_CI_AS) + ' on ' + t.TableName + ';' as [TSQL Command], 'SPAT' as type,
					10 as [Sort Order]
					from #TablesToColumnstore t
					inner join sys.indexes ind
						on t.ObjectId = ind.object_id
					where type = 4
				union all 
				select t.TableName, '-- - - - - - - - - - - - - - - - - - - - - -' as [TSQL Command], '---' as type,
					0 as [Sort Order]
					from #TablesToColumnstore t
			) coms
		order by coms.type desc, coms.[Sort Order]; --coms.TableName 
			 
	end

	drop table #TablesToColumnstore; 
end

GO
