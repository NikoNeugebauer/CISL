/*
	Columnstore Indexes Scripts Library for Azure SQLDatabase: 
	Row Groups Details - Shows detailed information on the Columnstore Row Groups
	Version: 1.1.1, January 2016

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

-- Params --
declare @schemaName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified schema
		@tableName nvarchar(256) = NULL,				-- Allows to show data filtered down to the specified table name
		@partitionNumber bigint = 0,					-- Allows to show details of each of the available partitions, where 0 stands for no filtering
		@showTrimmedGroupsOnly bit = 0,					-- Filters only those Row Groups, which size <> 1048576
		@showNonCompressedOnly bit = 0,					-- Filters out the comrpessed Row Groups
		@showFragmentedGroupsOnly bit = 0,				-- Allows to show the Row Groups that have Deleted Rows in them
		@minSizeInMB Decimal(16,3) = NULL,				-- Minimum size in MB for a table to be included
		@maxSizeInMB Decimal(16,3) = NULL 				-- Maximum size in MB for a table to be included
-- end of --

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running Azure SQLDatabase
if SERVERPROPERTY('EngineEdition') <> 5 
begin
	set @errorMessage = (N'Your are not running this script agains Azure SQLDatabase: Your are running a ' + @SQLServerEdition);
	Throw 51000, @errorMessage, 1;
end

set nocount on;

select quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)) as [Table Name],
	rg.partition_number,
	rg.row_group_id,
	rg.state,
	rg.state_description,
	rg.total_rows,
	rg.deleted_rows,
	cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) as [Size in MB]
	from sys.column_store_row_groups rg
	where   rg.total_rows <> case @showTrimmedGroupsOnly when 1 then 1048576 else -1 end
		and rg.state <> case @showNonCompressedOnly when 0 then -1 else 3 end
		and isnull(rg.deleted_rows,0) <> case @showFragmentedGroupsOnly when 1 then 0 else -1 end
		and (@tableName is null or object_name (rg.object_id) like '%' + @tableName + '%')
		and (@schemaName is null or object_schema_name(rg.object_id) = @schemaName)
		and rg.partition_number = case @partitionNumber when 0 then rg.partition_number else @partitionNumber end
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) >= isnull(@minSizeInMB,0.)
		and cast(isnull(rg.size_in_bytes,0) / 1024. / 1024  as Decimal(8,3)) <= isnull(@maxSizeInMB,999999999.)
	order by quotename(object_schema_name(rg.object_id)) + '.' + quotename(object_name(rg.object_id)), rg.partition_number, rg.row_group_id