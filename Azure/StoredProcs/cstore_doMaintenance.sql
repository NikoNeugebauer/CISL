/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
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

/*
Known Limitations:
	- Segment Clustering is supported only for the Disk-Based Clustered Columnstore Indexes. 
	- Segment Clustering is not supported on the partition level

Changes in 1.4.0
	+ Added support for the Indexed Views with Nonclustered Columnstore Indexes
*/

declare @createLogTables bit = 1;

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

-- ------------------------------------------------------------------------------------------------------------------------------------------------------
-- Verification of the required Stored Procedures from CISL
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroups' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetRowGroups Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetRowGroupsDetails Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetAlignment' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetAlignment Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetFragmentation' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetFragmentation Stored Procedure from CISL before advancing!', 1; 
	Return;
end
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetDictionaries' and schema_id = SCHEMA_ID('dbo') )
begin
	Throw 60000, 'Please install dbo.cstore_GetDictionaries Stored Procedure from CISL before advancing!', 1; 
	Return;
end

-- Setup of the logging tables
IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
begin
	-- Maintenance statistics log 
	create table dbo.cstore_MaintenanceData_Log(
		id int not null identity(1,1) primary key,
		ExecutionId uniqueidentifier,
		MonitoringTimestamp datetime not null default (GetDate()),
		TableName nvarchar(256) not null,
		IndexName nvarchar(256) not null,
		IndexType nvarchar(256) not null,
		IndexLocation varchar(15) not null,

		Partition int,

		[CompressionType] varchar(50),
		[BulkLoadRGs] int,
		[OpenDeltaStores] int,
		[ClosedDeltaStores] int,
		[CompressedRowGroups] int,

		ColumnId int,
		ColumnName nvarchar(256),
		ColumntType nvarchar(256),
		SegmentElimination varchar(50),
		DealignedSegments int,
		TotalSegments int,
		SegmentAlignment Decimal(8,2),


		Fragmentation Decimal(8,2),
		DeletedRGs int,
		DeletedRGsPerc Decimal(8,2),
		TrimmedRGs int,
		TrimmedRGsPerc Decimal(8,2),
		AvgRows bigint not null,
		TotalRows bigint not null,
		OptimizableRGs int,
		OptimizableRGsPerc Decimal(8,2),
		RowGroups int,
		TotalDictionarySizes Decimal(9,3),
		MaxGlobalDictionarySize Decimal(9,3),
		MaxLocalDictionarySize Decimal(9,3)
	
	);
end

IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Operation_Log' and schema_id = SCHEMA_ID('dbo') )
begin 
	-- Operation Log table
	create table dbo.cstore_Operation_Log(
		id int not null identity(1,1) constraint [PK_cstore_Operation_Log] primary key clustered,
		ExecutionId uniqueidentifier,
		TableName nvarchar(256),
		Partition int,
		OperationType varchar(10),
		OperationReason varchar(50),
		OperationCommand nvarchar(max),
		OperationCollected bit NOT NULL default(0),
		OperationConfigured bit NOT NULL default(0),
		OperationExecuted bit NOT NULL default (0)
	);
end 

IF @createLogTables = 1 AND NOT EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Clustering' and schema_id = SCHEMA_ID('dbo') )
begin
	-- Configuration table for the Segment Clustering
	create table dbo.cstore_Clustering(
		TableName nvarchar(256)  constraint [PK_cstore_Clustering] primary key clustered,
		Partition int,
		ColumnName nvarchar(256)
	);

	DROP TABLE IF EXISTS #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[ObjectType] varchar(20),
		[Location] varchar(15),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Tombstones] int,
		[Compressed RowGroups] int,
		[Total RowGroups] int,
		[Deleted Rows] Decimal(18,6),
		[Active Rows] Decimal(18,6),
		[Total Rows] Decimal(18,6),
		[Size in GB] Decimal(18,3),
		[Scans] int,
		[Updates] int,
		[LastScan] DateTime
	);

	insert into #ColumnstoreIndexes (TableName, Type, ObjectType, Location, Partition, [Compression Type], 
									 BulkLoadRGs, [Open DeltaStores], [Closed DeltaStores], [Tombstones], [Compressed RowGroups], [Total RowGroups], 
									[Deleted Rows], [Active Rows], [Total Rows], [Size in GB], Scans, Updates, LastScan)
		exec dbo.cstore_GetRowGroups @indexType = 'CC', @showPartitionDetails = 1;

	insert into dbo.cstore_Clustering( TableName, Partition, ColumnName )
		select TableName, Partition, NULL 
			from #ColumnstoreIndexes ci
			where TableName not in (select clu.TableName from dbo.cstore_Clustering clu);
end
GO

-- **************************************************************************************************************************
IF NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_doMaintenance' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_doMaintenance as select 1');
GO

/*
	CSIL - Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Columnstore Maintenance - Maintenance Solution for SQL Server Columnstore Indexes
	Version: 1.4.1, November 2016
*/
alter procedure [dbo].[cstore_doMaintenance](
-- Params --
	@execute bit = 0,								-- Controls if the maintenace is executed or not
	@orderSegments bit = 0,							-- Controls whether Segment Clustering is being applied or not
	@executeReorganize bit = 0,						-- Controls if the Tuple Mover is being invoked or not. We can execute just it, instead of the full rebuild
	@closeOpenDeltaStores bit = 0,					-- Controls if the Open Delta-Stores are closed and compressed
	@forceRebuild bit = 0,							-- Allows to force rebuild operation on the tables
	@usePartitionLevel bit = 1,						-- Controls if whole table is maintained or the maintenance is done on the partition level
	@partition_number int = NULL,					-- Allows to specify a partition to execute maintenance on
	@tableName nvarchar(max) = NULL,				-- Allows to filter out only a particular table 
	@useRecommendations bit = 1,					-- Activates internal optimizations for a more correct maintenance proceedings
	@maxdop tinyint = 0,							-- Allows to control the maximum degreee of parallelism
	@logData bit = 1,								-- Controls if functionalites are being logged into the logging tables
	@debug bit = 0,									-- Prints out the debug information and the commands that will be executed if the @execute parameter is set to 1
    @minSegmentAlignmentPercent tinyint = 70,		-- Sets the minimum alignment percentage, after which the Segment Alignment is forced
	@logicalFragmentationPerc int = 15,				-- Defines the maximum logical fragmentation for the Rebuild
	@deletedRGsPerc int = 10,						-- Defines the maximum percentage of the Row Groups that can be marked as Deleted
	@deletedRGs int = NULL,							-- Defines the maximum number of Row Groups that can be marked as Deleted before Rebuild. NULL means to be ignored.
	@trimmedRGsPerc int = 30,						-- Defines the maximum percentage of the Row Groups that are trimmed (not full)
	@trimmedRGs int = NULL,							-- Defines the maximum number of the Row Groups that are trimmed (not full). NULL means to be ignored.
	@minAverageRowsPerRG int = 550000,				-- Defines the minimum average number of rows per Row Group for triggering Rebuild
	@maxDictionarySizeInMB Decimal(9,3) = 10.,		-- Defines the maximum size of a dictionary to determine the dictionary pressure and avoid rebuilding
	@ignoreInternalPressures bit = 0				-- Allows to execute rebuild of the Columnstore, while ignoring the signs of memory & dictionary pressures
) as
begin
	SET ANSI_WARNINGS OFF;
	set nocount on;

	declare @objectId int = NULL;
	declare @currentTableName nvarchar(256) = NULL;  
	declare @indexName nvarchar(256) = NULL;  
	declare @orderingColumnName nvarchar(128) = NULL;
	declare @indexType varchar(20) = NULL;
	declare @indexLocation varchar(15) = NULL;
	declare	@totalRows Decimal(18,6) = NULL;
	declare @openRows Decimal(18,6) = NULL;

	-- Alignment
	declare @columnId int = NULL;

	-- Internal Variables
	declare @workid int = -1;
	declare @partitionNumber int = -1;
	declare @isPartitioned bit = 0;
	declare @compressionType varchar(30) = '';
	declare @rebuildNeeded bit = 0;
	declare @orderSegmentsNeeded  bit = 0;
	declare @openDeltaStores int = 0;
	declare @closedDeltaStores int = 1;
	declare @maxGlobalDictionarySizeInMB Decimal(9,3) = -1;
	declare @maxLocalDictionarySizeInMB Decimal(9,3) = -1;
	declare @rebuildReason varchar(100) = NULL;
	declare @SQLCommand nvarchar(4000) = NULL;
	declare @execId uniqueidentifier = NEWID();
	declare @loggingTableExists bit = 0;
	declare @loggingCommand nvarchar(max) = NULL;
	--

	-- Verify if the principal logging table exists and thus enabling logging
	IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Operation_Log' and schema_id = SCHEMA_ID('dbo') )
		set @loggingTableExists = 1;

	-- ***********************************************************
	-- Enable Reorganize automatically if the Trace Flag 634 or 10204 is enabled
	if( @useRecommendations = 1 )
	begin
		create table #ActiveTraceFlags(	
			TraceFlag nvarchar(20) not null,
			Status bit not null,
			Global bit not null,
			Session bit not null );

		insert into #ActiveTraceFlags
			exec sp_executesql N'DBCC TRACESTATUS() WITH NO_INFOMSGS';

		create table #ColumnstoreTraceFlags(
			TraceFlag int not null,
			Description nvarchar(500) not null,
			URL nvarchar(600),
			SupportedStatus bit not null 
		);

		if( exists (select TraceFlag from #ActiveTraceFlags where TraceFlag = '634') )
			select @executeReorganize = 1, @closeOpenDeltaStores = 1;

		-- TF 10204: Disables merge/recompress during columnstore index reorganization.
		-- In this case there is no reason to Reorganize the Index.
		if( exists (select TraceFlag from #ActiveTraceFlags where TraceFlag = '10204') )
			select @executeReorganize = 0, @closeOpenDeltaStores = 0, @execute = 1; 
	end


	-- ***********************************************************
	-- Process MAXDOP variable and update it according to the number of visible cores or to the number of the cores, specified in Resource Governor
	declare @coresDop smallint;
	select @coresDop = count(*)
		from sys.dm_os_schedulers 
		where upper(status) = 'VISIBLE ONLINE' and is_online = 1

	declare @effectiveDop smallint  
	-- dm_resource_governor_workload_groups is not supported in Azure SQLDatabase yet
	--select @effectiveDop = effective_max_dop 
	--	from sys.dm_resource_governor_workload_groups
	--	where group_id in (select group_id from sys.dm_exec_requests where session_id = @@spid)
	select @effectiveDop = cast( value as int )
		from sys.database_scoped_configurations
		where name = 'MAXDOP' and cast( value as int ) < @effectiveDop;
	
	if( @maxdop < 0 )
		set @maxdop = 0;
	if( @maxdop > @coresDop )
		set @maxdop = @coresDop;
	if( @maxdop > @effectiveDop AND @maxdop <> 0 AND @effectiveDop <> 0  )
		set @maxdop = @effectiveDop;

	if @debug = 1
	begin	
		print 'MAXDOP: ' + cast( @maxdop as varchar(3) );
		print 'EFECTIVE DOP: ' + cast( @effectiveDop as varchar(3) );
	end
	-- ***********************************************************
	-- Get All Columnstore Indexes for the maintenance
	DROP TABLE IF EXISTS #ColumnstoreIndexes;

	create table #ColumnstoreIndexes(
		[id] int identity(1,1),
		[TableName] nvarchar(256),
		[Type] varchar(20),
		[ObjectType] varchar(20),
		[Location] varchar(15),
		[Partition] int,
		[Compression Type] varchar(50),
		[BulkLoadRGs] int,
		[Open DeltaStores] int,
		[Closed DeltaStores] int,
		[Tombstones] int,
		[Compressed RowGroups] int,
		[Total RowGroups] int,
		[Deleted Rows] Decimal(18,6),
		[Active Rows] Decimal(18,6),
		[Total Rows] Decimal(18,6),
		[Size in GB] Decimal(18,3),
		[Scans] int,
		[Updates] int,
		[LastScan] DateTime
	);
	
	-- Obtain all Columnstore Indexes
	insert into #ColumnstoreIndexes 
		exec dbo.cstore_GetRowGroups @tableName = @tableName, @showPartitionDetails = @usePartitionLevel, @partitionId = @partition_number; 
	
	if( @debug = 1 )
	begin
		select *
			from #ColumnstoreIndexes;
	end

	while( exists (select * from #ColumnstoreIndexes) )
	begin

		select top 1 @workid = id,
				@partitionNumber = Partition,
				@currentTableName = TableName,
				@indexType = Type,
				@indexLocation = Location,
				@totalRows = [Total Rows],
				@compressionType = [Compression Type],
				@openDeltaStores = [Open DeltaStores],
				@closedDeltaStores = [Closed DeltaStores],
				@orderingColumnName = NULL,
				@maxGlobalDictionarySizeInMB = -1,
				@maxLocalDictionarySizeInMB = -1,
				@rebuildNeeded = 0, 
				@rebuildReason = NULL,
				@orderSegmentsNeeded = @orderSegments
			from #ColumnstoreIndexes
				--where TableName = isnull(@currentTableName,TableName)
			order by id;

		if @debug = 1
		begin
			print '------------------------------------------------';
			print 'Current Table: ' + @currentTableName;
		end
	
		-- Get the object_id of the table
		select @objectId = object_id(@currentTableName);

		-- Obtain pre-configured clustering column name
		IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_Clustering' and schema_id = SCHEMA_ID('dbo') )
			select @orderingColumnName = ColumnName
				from dbo.cstore_Clustering
				where TableName = @currentTableName and Partition = @partitionNumber;		

		-- If the column name is not set, then do not force Segments Clustering
		if @orderingColumnName is NULL
		begin
			set @orderSegmentsNeeded = 0;
		end

		-- ***********************************************************
		-- Get Number of Rows within open Delta-Stores. This is especially useful to determine if In-Memory Tables can have "Row Migration" process.
		DROP TABLE IF EXISTS #RowGroupsDetails;

		create table #RowGroupsDetails(
			[Table Name] nvarchar(512),
			Location varchar(15),
			partition_nr int,
			row_group_id int,
			state tinyint,
			state_description nvarchar(60),
			total_rows bigint,
			deleted_rows bigint,
			[Size in MB] decimal(8,3),
			trim_reason tinyint,
			trim_reason_desc nvarchar(60),
			compress_op tinyint,
			compress_op_desc nvarchar(60),
			optimised bit,
			generation bigint,
			closed_time datetime,
			created_time datetime );

		insert into #RowGroupsDetails
			exec dbo.cstore_GetRowGroupsDetails @objectId = @objectId, @showNonCompressedOnly = 1;

		select @openRows = sum([total_rows] - [deleted_rows]) / 1000000.
			from #RowGroupsDetails

		-- ***********************************************************
		-- Get Segments Alignment
		DROP TABLE IF EXISTS #ColumnstoreAlignment;

		create table #ColumnstoreAlignment(
			TableName nvarchar(256),
			Location varchar(15),
			Partition bigint,
			ColumnId int,
			ColumnName nvarchar(256),
			ColumnType nvarchar(256),
			SegmentElimination varchar(50),
			DealignedSegments int,
			TotalSegments int,
			SegmentAlignment Decimal(8,2)
		)

		-- If we are executing no Segment Clustering, then do not look for it - just get results for the very first column
		if( @orderSegmentsNeeded = 0 )
			set @columnId = 1;
		else
			set @columnId = NULL;

		-- Get Results from "cstore_GetAlignment" Stored Procedure
		insert into #ColumnstoreAlignment ( TableName, Location, Partition, ColumnId, ColumnName, ColumnType, SegmentElimination, DealignedSegments, TotalSegments, SegmentAlignment )
				exec dbo.cstore_GetAlignment @objectId = @objectId, 
											@showPartitionStats = @usePartitionLevel, 
											@showUnsupportedSegments = 1, @columnName = @orderingColumnName, @columnId = @columnId;		

		if( --@rebuildNeeded = 0 AND 
			@orderSegmentsNeeded = 1 )
		begin	
			declare @currentSegmentAlignment Decimal(6,2) = 100.;

			select @currentSegmentAlignment = SegmentAlignment
				from #ColumnstoreAlignment
				where SegmentElimination = 'OK' and Partition = @partitionNumber;

			if( @currentSegmentAlignment <= @minSegmentAlignmentPercent )
				Select @rebuildNeeded = 1, @rebuildReason = 'Dealignment';

		end
		
		-- ***********************************************************
		-- Get Fragmentation
		DROP TABLE IF EXISTS #Fragmentation;

		create table #Fragmentation(
			TableName nvarchar(256),
			IndexName nvarchar(256),
			Location varchar(15),
			IndexType nvarchar(256),
			Partition int,
			Fragmentation Decimal(8,2),
			DeletedRGs int,
			DeletedRGsPerc Decimal(8,2),
			TrimmedRGs int,
			TrimmedRGsPerc Decimal(8,2),
			AvgRows bigint,
			TotalRows bigint,
			OptimizableRGs int,
			OptimizableRGsPerc Decimal(8,2),
			RowGroups int
		);

		-- Obtain Columnstore logical fragmentation information
		insert into #Fragmentation
			exec cstore_GetFragmentation @objectId = @objectId, @showPartitionStats = 1;

		-- Obtain the name of the Columnstore index we are working with
		select @indexName = IndexName
			from #Fragmentation
			where TableName = @currentTableName

		-- In the case when there is no fragmentation whatsoever (because there is just open Delta-Stores, for example)
		-- Get the Index Name directly from the DMV
		if @indexName is NULL
		begin 
			select @indexName = ind.name 
				from sys.indexes ind
				where ind.type in (5,6) and ind.object_id = @objectId;
		end

		-- Reorganize for Open Delta-Stores
		if @openDeltaStores > 0 AND (@executeReorganize = 1 OR @execute = 1)
		begin
			if @indexLocation = 'Disk-Based' 
			begin 
				set @SQLCommand = 'alter index ' + @indexName + ' on ' + @currentTableName + ' Reorganize';

				if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
					set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
		
				-- Force open Delta-Stores closure
				if( @closeOpenDeltaStores = 1 )
					set @SQLCommand += ' with (compress_all_row_groups = on ) ';
			end
			else if @indexLocation = 'In-Memory'
			begin
				-- Execute Row Migration process for the InMemory Tables (if we are forcing reorganize or if have over 500.000 rows in the Tail Row Group
				if @openRows >= 0.5 OR @executeReorganize = 1
					set @SQLCommand = 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */';
				else
					set @SQLCommand = '';			
			end
			

			if @logData = 1
			begin				
				if @loggingTableExists = 1 
				begin
					set @loggingCommand = N'
							insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
								select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''Reorganize'', 
										''Open Delta-Stores'', ''' + @SQLCommand + ''', 1, ' + cast(case when (@executeReorganize = 1 OR @execute = 1) then 1 else 0 end as char(1));
				
					exec (@loggingCommand);
				end
			end

			if( @debug = 1 )
			begin
				print 'Reorganize Open Delta-Stores';
				print 'Location: ' + @indexLocation;
				print isnull(@SQLCommand, 'NULL');
				print '+++++';
			end

			if( @execute = 1 OR @executeReorganize = 1 )
				exec ( @SQLCommand  );
		end

		-- Obtain Dictionaries informations
		DROP TABLE IF EXISTS #Dictionaries;

		create table #Dictionaries(
			TableName nvarchar(256),
			[Type] varchar(20),
			[Location] varchar(15),
			Partition int,
			RowGroups bigint,
			Dictionaries bigint,
			EntryCount bigint,
			RowsServing bigint,
			TotalSizeMB Decimal(8,3),
			MaxGlobalSizeMB Decimal(8,3),
			MaxLocalSizeMB Decimal(8,3),
		);

		insert into #Dictionaries (TableName, Type, Location, Partition, RowGroups, Dictionaries, EntryCount, RowsServing, TotalSizeMB, MaxGlobalSizeMB, MaxLocalSizeMB )
			exec dbo.cstore_GetDictionaries @objectId = @objectId, @showDetails = 0;

		-- Get the current maximum sizes for the dictionaries
		select @maxGlobalDictionarySizeInMB = MaxGlobalSizeMB, @maxLocalDictionarySizeInMB = MaxLocalSizeMB
			from #Dictionaries
			where TableName = @currentTableName and Partition = @partitionNumber;

		-- Store current information in the logging table
		if @logData = 1
		begin
			IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
			begin
				insert into dbo.cstore_MaintenanceData_Log( ExecutionId, TableName, IndexName, IndexType, IndexLocation, Partition, 
														[CompressionType], [BulkLoadRGs], [OpenDeltaStores], [ClosedDeltaStores], [CompressedRowGroups],
														ColumnId, ColumnName, ColumntType, 
														SegmentElimination, DealignedSegments, TotalSegments, SegmentAlignment, 
														Fragmentation, DeletedRGs, DeletedRGsPerc, TrimmedRGs, TrimmedRGsPerc, AvgRows, 
														TotalRows, OptimizableRGs, OptimizableRGsPerc, RowGroups,
														TotalDictionarySizes, MaxGlobalDictionarySize, MaxLocalDictionarySize )
					select top 1 @execId, align.TableName, IndexName, ind.Type, ind.Location, align.Partition, 
							[Compression Type], [BulkLoadRGs], [Open DeltaStores], [Closed DeltaStores], [Compressed RowGroups],
							align.ColumnId, align.ColumnName, align.ColumnType, 
							align.SegmentElimination, align.DealignedSegments, align.TotalSegments, align.SegmentAlignment,
							frag.Fragmentation, frag.DeletedRGs, frag.DeletedRGsPerc, frag.TrimmedRGs, frag.TrimmedRGsPerc, frag.AvgRows, frag.TotalRows, 
							frag.OptimizableRGs, frag.OptimizableRGsPerc, frag.RowGroups,
							dict.TotalSizeMB, dict.MaxGlobalSizeMB, dict.MaxLocalSizeMB
						from #ColumnstoreAlignment align
						inner join #Fragmentation frag
							on align.TableName = frag.TableName and align.Partition = frag.Partition
						inner join #ColumnstoreIndexes ind
							on ind.TableName = align.TableName and ind.Partition = align.Partition
						inner join #Dictionaries dict
							on ind.TableName = dict.TableName and ind.Partition = dict.Partition
						where align.Partition = @partitionNumber and id = @workid;
			end
			
		end

		-- Remove currently processed record
		delete from #ColumnstoreIndexes
			where id = @workid;


		-- Find a rebuild reason
		if( @rebuildNeeded = 0 )
		begin		
			declare @currentlogicalFragmentationPerc int = 0,
					@currentDeletedRGsPerc int = 0,
					@currentDeletedRGs int = 0,
					@currentTrimmedRGsPerc int = 0,
					@currentTrimmedRGs int = 0,
					@currentOptimizableRGs int = 0,
					@currentMinAverageRowsPerRG int = 0,
					@currentRowGroups int = 0;
			
			-- Determine current fragmentation parameters, as well as the number of row groups
			select @currentlogicalFragmentationPerc = Fragmentation,
					@currentDeletedRGsPerc = DeletedRGsPerc,
					@currentDeletedRGs = DeletedRGs,
					@currentTrimmedRGsPerc = TrimmedRGsPerc, 
					@currentTrimmedRGs = TrimmedRGs,
					@currentOptimizableRGs = OptimizableRgs,
					@currentMinAverageRowsPerRG = AvgRows,
					@currentRowGroups = RowGroups
				from #Fragmentation
				where Partition = @partitionNumber;
			
			-- Advance for searching for rebuilding only if there is more then 1 Row Group
			if( @currentRowGroups > 1 )
			begin 
				if( @rebuildNeeded = 0 AND @currentlogicalFragmentationPerc >= @logicalFragmentationPerc )
					select @rebuildNeeded = 1, @rebuildReason = 'Logical Fragmentation';

				if( @rebuildNeeded = 0 AND @currentDeletedRGsPerc >= @deletedRGsPerc )
					select @rebuildNeeded = 1, @rebuildReason = 'Deleted RowGroup Percentage';

				if( @rebuildNeeded = 0 AND @currentDeletedRGs >= isnull(@deletedRGs,2147483647) )
					select @rebuildNeeded = 1, @rebuildReason = 'Deleted RowGroups';

				-- !!! Check if the trimmed Row Groups are the last ones in the partition/index, and if yes then extract the number of available cores
				-- For that use GetRowGroupsDetails
				if( @currentOptimizableRGs > 0 AND @useRecommendations = 1 )
				begin
					if( @rebuildNeeded = 0 AND @currenttrimmedRGsPerc >= @trimmedRGsPerc )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroup Percentage';
					if( @rebuildNeeded = 0 AND @currenttrimmedRGs >= isnull(@trimmedRGs,2147483647) )
						select @rebuildNeeded = 1, @rebuildReason = 'Trimmed RowGroups';

					if( @rebuildNeeded = 0 AND @currentMinAverageRowsPerRG <= @minAverageRowsPerRG )
						select @rebuildNeeded = 1, @rebuildReason = 'Average Rows per RowGroup';
				end 
		
				-- Verify the dictionary pressure and avoid rebuilding in this case
				if( (@maxDictionarySizeInMB <= @maxGlobalDictionarySizeInMB OR @maxDictionarySizeInMB <= @maxLocalDictionarySizeInMB) AND
					@rebuildReason in ('Trimmed RowGroups','Trimmed RowGroup Percentage','Average Rows per RowGroup') )
				begin
					if @ignoreInternalPressures = 0 
						select @rebuildNeeded = 0, @rebuildReason += ' - Dictionary Pressure';
				end
			end
		end
		
		if( @rebuildNeeded = 0 )
			set @SQLCommand = '';
		
		if( @debug = 1 )
		begin
			print 'Reason: ' + isnull(@rebuildReason,'-');
			print 'Rebuild: ' + case @rebuildNeeded when 1 then 'true' else 'false' end;
		end
	
		-- Verify if we are working with a partitioned table
		select @isPartitioned = case when count(*) > 1 then 1 else 0 end 
			from sys.partitions p
			where object_id = object_id(@currentTableName)
				and data_compression in (3,4);

		-- Execute Table Rebuild if needed
		--if( @rebuildNeeded = 1 )
		begin
			if( @orderSegmentsNeeded = 1 AND @orderingColumnName is not null AND 
				@isPartitioned = 0 AND @indexType = 'Clustered')
			begin
				if @indexLocation = 'Disk-Based' 
				begin
					set @SQLCommand = 'create clustered index ' + @indexName + ' on ' + @currentTableName + '(' + @orderingColumnName + ') with (drop_existing = on, maxdop = ' + cast(@maxdop as varchar(3)) + ');';
			
					-- Let's recreate Clustered Columnstore Index
					set @SQLCommand += 'create clustered columnstore index ' + @indexName + ' on ' + @currentTableName;
					set @SQLCommand += ' with (data_compression = ' + @compressionType + ', drop_existing = on, maxdop = 1);';

					if( @debug = 1 )
						print 'SQLCommand:' + @SQLCommand;
				end
				else if @indexLocation = 'In-Memory' 
				begin
					set @SQLCommand = 'alter table ' + @currentTableName + 
										' drop index ' + @indexName + ';';

					set @SQLCommand += 'alter table ' + @currentTableName + 
										' add index ' + @indexName + ' CLUSTERED COLUMNSTORE';
					set @SQLCommand += ' with (data_compression = ' + @compressionType + ');';
				end 

				if @logData = 1
				begin
					if @loggingTableExists = 1 
					begin
						set @loggingCommand = N'
								insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
									select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''Recreate'', 
											''' + @rebuildReason + ''', ''' + @SQLCommand + ''', '+ cast(@execute as char(1)) + ', ' + cast(@rebuildNeeded as char(1));
				
						exec (@loggingCommand);
					end
				end

				if( @debug = 1 )
				begin
					print 'SQLCommand:' + @SQLCommand;
				end

				-- Execute Rebuild
				if( @execute = 1 AND @rebuildNeeded = 1 AND @indexLocation = 'Disk-Based' )
				begin 
					begin try 
						exec ( @SQLCommand );

						if @debug = 1
							print 'Executed Successfully';
					end try
					begin catch
						-- In the future, to add a logging of the error message
						SELECT	 ERROR_NUMBER() AS ErrorNumber
								,ERROR_SEVERITY() AS ErrorSeverity
								,ERROR_STATE() AS ErrorState
								,ERROR_PROCEDURE() AS ErrorProcedure
								,ERROR_LINE() AS ErrorLine
								,ERROR_MESSAGE() AS ErrorMessage;
						Throw;
					end catch 
				end
			end
		
			-- Process Partitioned Table
			if( @orderSegmentsNeeded = 0 OR										-- No Segment Clustering 
				(@orderSegmentsNeeded = 1 and @orderingColumnName is NULL) OR   -- Forcing Segment Clustering but no Column is configured
				@isPartitioned = 1 )											-- Partitioned Table
			begin
				if @indexLocation = 'Disk-Based'
				begin 
					if @forceRebuild = 0
					begin
						set @SQLCommand = 'alter index ' + @indexName + ' on ' + @currentTableName + ' reorganize';
						if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
							set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5)) + ';';

						-- The second invocation for the elimination of the Tombstones and new potential 
						set @SQLCommand += @SQLCommand;
					end
					else
					begin 
						set @SQLCommand = 'alter table ' + @currentTableName + ' rebuild';
						if( @usePartitionLevel = 1 AND @isPartitioned = 1 )
							set @SQLCommand += ' partition = ' + cast(@partitionNumber as varchar(5));
						set @SQLCommand += ' with (maxdop = ' + cast(@maxdop as varchar(3)) + ')';
					end
				end
				else if @indexLocation = 'In-Memory'
				begin
					-- Invoking twice so some of the more complex processes, such as deleted Row Groups can be truly removed from the InMemory Table
					set @SQLCommand = 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */;' + CHAR(10);
					--set @SQLCommand += 'exec sys.sp_memory_optimized_cs_migration @object_id = ' + cast(@objectId as varchar(15)) + ' /* ' + isnull(@currentTableName,'NULL') + ' */;';
				end

				if( @debug = 1 )
				begin
					print 'Rebuild ' + @rebuildReason;
					print 'SQLCommand:' + @SQLCommand;
				end

				if @logData = 1
				begin
					if @loggingTableExists = 1 
					begin
						set @loggingCommand = N'
								insert into dbo.cstore_Operation_Log( ExecutionId, TableName, Partition, OperationType, OperationReason, OperationCommand, OperationConfigured, OperationExecuted )
									select ''' + convert(nvarchar(50),@execId) + ''', ''' + @currentTableName + ''', ' + cast(@partitionNumber as varchar(10)) + ', ''' + case @rebuildNeeded when 1 then 'Rebuild' else '' end + ''', 
											''' + @rebuildReason + ''', ''' + @SQLCommand + ''', '+ cast(@execute as char(1)) + ', ' + cast(@rebuildNeeded as char(1));
				
						exec (@loggingCommand);
					end
				end

				if( @execute = 1 AND @rebuildNeeded = 1 )
				begin
					exec ( @SQLCommand  );
					if @debug = 1
						print 'Executed Successfully';
				end
			end

		end


	
	end

	if( @debug = 1 )
	begin
		--select * from #Fragmentation;
		--select * from #Dictionaries;
		--select * from #ColumnstoreAlignment;
		--select * from #ColumnstoreIndexes;

		-- Output the content of the maintenance log inserted during the execution
		IF EXISTS (select * from sys.objects where type = 'u' and name = 'cstore_MaintenanceData_Log' and schema_id = SCHEMA_ID('dbo') )
			select *
				from dbo.cstore_MaintenanceData_Log
				where ExecutionId = @execId;
	end
end

GO
