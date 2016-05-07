/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.2.0, May 2016

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
Changes in 1.1.0
	- Fixed error with a semicolon inside the parameters of the stored procedure
	+ Added new parameter for filtering on the object id - @objectId
	* Changed constant creation and dropping of the stored procedure to 1st time execution creation and simple alteration after that
	* The description header is copied into making part of the function code that will be stored on the server. This way the CISL version can be easily determined.

Changes in 1.2.0
	- Fixed bug with conversion to bigint for row_count
	+ Included support for the temporary tables with Columnstore Indexes (global & local)
*/

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

 --Ensure that we are running SQL Server 2012
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
if NOT EXISTS (select * from sys.objects where type = 'p' and name = 'cstore_GetRowGroupsDetails' and schema_id = SCHEMA_ID('dbo') )
	exec ('create procedure dbo.cstore_GetRowGroupsDetails as select 1');
GO

/*
	Columnstore Indexes Scripts Library for SQL Server 2012: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.2.0, May 2016
*/
alter procedure dbo.cstore_GetRowGroupsDetails(
-- Params --
	@objectId int = NULL,							-- Allows to idenitfy a table thorugh the ObjectId
	@schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
	@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
	@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
	@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which size <> 1048576
	@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
	@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
	@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
	@maxSizeInMB Decimal(16,3) = NULL 				-- Maximum size in MB for a table to be included
-- end of --
) as
BEGIN
	set nocount on;

	select quotename(object_schema_name(part.object_id)) + '.' + quotename(object_name(part.object_id)) as 'TableName', 
		part.partition_number,
		rg.segment_id as row_group_id,
		3 as state,
		'COMPRESSED' as state_description,
		sum(cast(rg.row_count as bigint))/count(distinct rg.column_id) as total_rows,
		0 as deleted_rows,
		cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]
		from sys.column_store_segments rg		
			left join sys.partitions part with(READUNCOMMITTED)
				on rg.hobt_id = part.hobt_id and isnull(rg.partition_id,1) = part.partition_id
		where 1 = case @showNonCompressedOnly when 0 then 1 else -1 end
			and 1 = case @showFragmentedGroupsOnly when 1 then 0 else 1 end
			and part.object_id = isnull(@objectId, part.object_id)
			and (@tableName is null or object_name (part.object_id) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(part.object_id) = @schemaName)
			and part.partition_number = case @partitionNumber when 0 then part.partition_number else @partitionNumber end
		group by part.object_id, part.partition_number, rg.segment_id
		having sum(cast(rg.row_count as bigint))/count(distinct rg.column_id) <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
				and cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
				and cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
	union all
	select quotename(object_schema_name(part.object_id, db_id('tempdb'))) + '.' + quotename(object_name(part.object_id, db_id('tempdb'))) as 'TableName', 
		part.partition_number,
		rg.segment_id as row_group_id,
		3 as state,
		'COMPRESSED' as state_description,
		sum(cast(rg.row_count as bigint))/count(distinct rg.column_id) as total_rows,
		0 as deleted_rows,
		cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]
		from tempdb.sys.column_store_segments rg		
			left join tempdb.sys.partitions part with(READUNCOMMITTED)
				on rg.hobt_id = part.hobt_id and isnull(rg.partition_id,1) = part.partition_id
		where 1 = case @showNonCompressedOnly when 0 then 1 else -1 end
			and 1 = case @showFragmentedGroupsOnly when 1 then 0 else 1 end
			and part.object_id = isnull(@objectId, part.object_id)
			and (@tableName is null or object_name (part.object_id, db_id('tempdb')) like '%' + @tableName + '%')
			and (@schemaName is null or object_schema_name(part.object_id, db_id('tempdb')) = @schemaName)
			and part.partition_number = case @partitionNumber when 0 then part.partition_number else @partitionNumber end
		group by part.object_id, part.partition_number, rg.segment_id
		having sum(cast(rg.row_count as bigint))/count(distinct rg.column_id) <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
				and cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
				and cast(sum(isnull(rg.on_disk_size,0)) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
		order by quotename(object_schema_name(part.object_id)) + '.' + quotename(object_name(part.object_id)),
			part.partition_number, rg.segment_id
END
GO
