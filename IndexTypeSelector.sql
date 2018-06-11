/*
	CSIL - Columnstore Indexes Scripts Library for Microsoft Data Platform: 
	Columnstore Index Type Selector - Allows you to view possible variants of technology usage together with Columnstore against all known and supported Microsoft Data Platform versions
	Version: 1.6.0, June 2018

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
	- some of the scenarios that are not supported anyway even without Columnstore Indexes are not yet included, but hey - you have got to research the configuration you are trying to achieve anyway! :)
*/

-- Params --
DECLARE
	@showConfiguration BIT = 1,					-- Controls wether currently used configuration options are displayed
	@writable INT = 0,							-- Requirement for Columnstore Index to be directly writable & updatable
	@snapshotIsolation INT = 0,					-- Requirement for using Snapshot Isolation together with Columnstore Indexes
	@rcsiIsolation INT = 0,						-- Requirement for using Read Committed Snapshot Isolation together with Columnstore Indexes
	@agReadableSecondaries INT = 0,				-- Requirement for using Availability Groups Readable Secondary Replicas with Columnstore Indexes
	@PrimaryForeignKeys INT = 0,				-- Requirement for using Primary & Foreign Keys on tables with Columnstore Indexes
	@UniqueConstraints INT = 0,					-- Requirement for using Unique Constraints on tables with Columnstore Indexes
	@SecondaryIndexes INT = 0,					-- Requirement for using Secondary Indexes on tables with Columnstore Indexes
	@DecimalPrecisionOver18 INT = 0,			-- Requirement for using Columns with Decimal data type having Precision over 18 on tables with Columnstore Indexes
	@CDC INT = 0,								-- Requirement for using CDC (Change Data Capture) on the tables with Columnstore Indexes
	@ChangeTracking INT = 0,					-- Requirement for using CT (Change Capture) on the tables with Columnstore Indexes
	@LOBs INT = 0,								-- Requirement for using LOBs on the tables with Columnstore Indexes
	@InMemory INT = 0,							-- Requirement for using In-Memory Technology with Columnstore Indexes
	@Replication INT = 0,						-- Requirement for using Replication on the tables with Columnstore Indexes
	@IndexedViews INT = 0,						-- Requirement for using Indexed Views with Columnstore Indexes
	@OnlineRebuilds INT = 0,					-- Requirement for using Online Rebuild for the Columnstore Indexes
	@ComputedColumns INT = 0,					-- Requirement for using Computed Columns (non-persisted) on the tables with Columnstore Indexes
	@ComputedColumnsPersisted INT = 0;			-- Requirement for using Persisted Computed Columns on the tables with Columnstore Indexes
-- end of --




IF @showConfiguration = 1 
BEGIN
	SELECT @writable as [Writable],
			@snapshotIsolation as [Snapshot],
			@rcsiIsolation as [RCSI],
			@agReadableSecondaries as [AG Readable],
			@PrimaryForeignKeys as [PKs & FKs],
			@UniqueConstraints as [Unique],
			@SecondaryIndexes as [Other Indexes],
			@DecimalPrecisionOver18 as [Dec. Precision > 18],
			@CDC as [CDC],
			@ChangeTracking as [CT],
			@LOBs as [LOBs],
			@InMemory as [In Memory],
			@Replication as [Replication],
			@IndexedViews as [Indexed Views],
			@OnlineRebuilds as [Online Rebuild],
			@ComputedColumns as [Computed Columns],
			@ComputedColumnsPersisted as [PersistedCC];	 
END

SELECT 'Clustered Columnstore' as IndexType,
	0 as SQL2012, 
	CASE WHEN @snapshotIsolation + @rcsiIsolation + @agReadableSecondaries + 
			  @PrimaryForeignKeys + @UniqueConstraints + @SecondaryIndexes + 
			  @ChangeTracking + @CDC + @InMemory + @LOBs + 
			  @Replication + @IndexedViews + @OnlineRebuilds + 
			  @ComputedColumns + @ComputedColumnsPersisted +
			  @InMemory  > 0 THEN 0 ELSE 1 END as SQL2014, 
	CASE WHEN @ChangeTracking + @CDC + @LOBs + @Replication + @IndexedViews +
			  @OnlineRebuilds + @ComputedColumns + @ComputedColumnsPersisted +
			  (CASE WHEN @InMemory = 1 AND 
				(@CDC = 1 OR @ChangeTracking = 1 OR @LOBs = 1 OR @Replication = 1 OR 
				 @IndexedViews = 1 OR @ComputedColumns = 1 OR @ComputedColumnsPersisted = 1 ) THEN 1 ELSE 0 END) 
				> 0 THEN 0 ELSE 1 END as SQL2016, 
	CASE WHEN @ChangeTracking + @CDC + @Replication + @IndexedViews +
	          @OnlineRebuilds + @ComputedColumnsPersisted +
			  (CASE WHEN @InMemory = 1 AND 
				(@CDC = 1 OR @ChangeTracking = 1 OR @LOBs = 1 OR @Replication = 1 OR 
				 @IndexedViews = 1 OR @ComputedColumns = 1 OR @ComputedColumnsPersisted = 1 ) THEN 1 ELSE 0 END) 
			   > 0 THEN 0 ELSE 1 END as SQL2017,
	CASE WHEN @ChangeTracking + @CDC + @Replication + @IndexedViews +
	          @OnlineRebuilds + @ComputedColumnsPersisted +
			  (CASE WHEN @InMemory = 1 AND 
				(@CDC = 1 OR @ChangeTracking = 1 OR @LOBs = 1 OR @Replication = 1 OR 
				 @IndexedViews = 1 OR @ComputedColumns = 1 OR @ComputedColumnsPersisted = 1 ) THEN 1 ELSE 0 END) 
			   > 0 THEN 0 ELSE 1 END as [Azure SQL DB],
	CASE WHEN @agReadableSecondaries + @ChangeTracking + @CDC + @Replication + @InMemory + @LOBs + 
	          @ComputedColumns + @ComputedColumnsPersisted > 0 THEN 0 ELSE 1 END as [Azure SQL DW]
UNION ALL
SELECT 'NonClustered Columnstore' as IndexType, 
	CASE WHEN @writable + @DecimalPrecisionOver18 + @InMemory + @LOBs +
			  @Replication + @IndexedViews + @OnlineRebuilds > 0 THEN 0 ELSE 1 END as SQL2012, 
	CASE WHEN @writable + @ChangeTracking + @CDC + @ChangeTracking + @InMemory + @LOBs + 
			  @Replication + @IndexedViews + @OnlineRebuilds +
			  @ComputedColumns + @ComputedColumnsPersisted > 0 THEN 0 ELSE 1 END as SQL2014, 
	CASE WHEN @ChangeTracking + @InMemory + @LOBs + 
			  @OnlineRebuilds + @ComputedColumns + @ComputedColumnsPersisted> 0 THEN 0 ELSE 1 END as SQL2016, 
	CASE WHEN @ChangeTracking + @InMemory + @LOBs + 
			  @ComputedColumns + @ComputedColumnsPersisted > 0 THEN 0 ELSE 1 END as SQL2017,
	CASE WHEN @ChangeTracking + @CDC + @Replication + @InMemory + @LOBs + 
	          @ComputedColumns + @ComputedColumnsPersisted > 0 THEN 0 ELSE 1 END as [Azure SQL DB],
	0 as [Azure SQL DW];