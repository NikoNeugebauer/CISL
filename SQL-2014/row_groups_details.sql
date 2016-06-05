/*
	Columnstore Indexes Scripts Library for SQL Server 2014: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.3.0, June 2016

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

Modifications:

Changes in 1.2.0
	+ Included support for the temporary tables with Columnstore Indexes (global & local)

Changes in 1.3.0
	+ Added compatibility support for the SQL Server 2016 internals information on Row Group Trimming, Build Process, Vertipaq Optimisations, Sequential Generation Id, Closed DateTime & Creation DateTime
	+ Added 2 new compatibility parameters for filtering out the Min & Max Creation DateTimes
	- Fixed error for the temporary tables support
	* Changed the name of the second result column from partition_number to partition
*/

-- Params --
declare @schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
		@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
		@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which size <> 1048576
		@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
		@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
		@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
		@maxSizeInMB Decimal(16,3) = NULL, 				-- Maximum size in MB for a table to be included
		@minCreatedDateTime Datetime = NULL,			-- The earliest create datetime for Row Group to be included
		@maxCreatedDateTime Datetime = NULL 			-- The lateste create datetime for Row Group to be included
-- end of --


declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

--Ensure that we are running SQL Server 2014
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

set nocount on;

select quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)) as [Table Name],
	'Disk-Based' as Location,
	rg.partition_number as partition,
	rg.row_group_id,
	rg.state,
	rg.state_description,
	rg.total_rows,
	rg.deleted_rows,
	cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
	NULL as trim_reason,
	NULL as trim_reason_desc,
	NULL compress_op, 
	NULL as compress_op_desc,
	NULL as optimised,
	NULL as generation,
	NULL as closed_time,	
	ind.create_date as created_time
	from sys.column_store_row_groups rg
		inner join sys.objects ind
			on rg.object_id = ind.object_id 
	where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
		and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
		and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
		and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		and ind.create_date between isnull(@minCreatedDateTime,ind.create_date) and isnull(@maxCreatedDateTime,ind.create_date)
UNION ALL
select quotename(object_schema_name(rg.object_id, db_id('tempdb'))) + '.' + quotename(object_name(rg.object_id, db_id('tempdb'))) as [Table Name],
	'Disk-Based' as Location,
	rg.partition_number as partition,
	rg.row_group_id,
	rg.state,
	rg.state_description,
	rg.total_rows,
	rg.deleted_rows,
	cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB],
	NULL as trim_reason,
	NULL as trim_reason_desc,
	NULL compress_op, 
	NULL as compress_op_desc,
	NULL as optimised,
	NULL as generation,
	NULL as closed_time,	
	ind.create_date as created_time
	from tempdb.sys.column_store_row_groups rg
		inner join tempdb.sys.objects ind
			on rg.object_id = ind.object_id 
	where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
		and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
		and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
		and (@tableName is null or object_name (rg.object_id, db_id('tempdb')) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(rg.object_id, db_id('tempdb')) = @schemaName)
		and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		and ind.create_date between isnull(@minCreatedDateTime,ind.create_date) and isnull(@maxCreatedDateTime,ind.create_date)
	order by quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)), rg.partition_number, rg.row_group_id;