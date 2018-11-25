/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server vNext: 
	Extended Events Setup Script for Tuple Mover events 'columnstore_tuple_mover_begin_compress', 'columnstore_tuple_mover_end_compress' & 'column_store_acquire_insert_lock'
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

declare @SQLServerVersion nvarchar(128) = cast(SERVERPROPERTY('ProductVersion') as NVARCHAR(128)), 
		@SQLServerEdition nvarchar(128) = cast(SERVERPROPERTY('Edition') as NVARCHAR(128));
declare @errorMessage nvarchar(512);

-- Ensure that we are running SQL Server vNext
if substring(@SQLServerVersion,1,CHARINDEX('.',@SQLServerVersion)-1) <> N'13'
begin
	set @errorMessage = (N'You are not running a SQL Server vNext. Your SQL Server version is ' + @SQLServerVersion);
	Throw 51000, @errorMessage, 1;
end

/* Stop Session if it already exists */
IF EXISTS(SELECT *
				FROM sys.server_event_sessions sess
				INNER JOIN sys.dm_xe_sessions actSess
					on sess.NAME = actSess.NAME
				WHERE sess.NAME = 'cstore_XE_TupleMover')
BEGIN
	ALTER EVENT SESSION cstore_XE_TupleMover
		ON SERVER 
			STATE = STOP;
END

/* Drop the definition of the currently configured XE session */
IF EXISTS
    (SELECT * FROM sys.server_event_sessions sess
        WHERE name = 'cstore_XE_TupleMover')
BEGIN

    DROP EVENT SESSION cstore_XE_TupleMover
        ON SERVER;
	
END

/* Create a new default session */
CREATE EVENT SESSION [cstore_XE_TupleMover] ON SERVER 
	ADD EVENT sqlserver.columnstore_compression_delay_disqualified_rowgroup(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_delete_buffer_flush_failed(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_delete_buffer_state_transition(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_delete_buffer_closed_rowgroup_with_generationid_found(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_delta_rowgroup_closed(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_index_reorg_failed(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_migration_commit(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_no_rowgroup_qualified_for_merge(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_rowgroup_cleanup(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_rowgroup_compressed(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_rowgroup_merge_complete(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_rowgroup_merge_failed(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_rowgroup_merge_start(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_skip_removing_tombtsone_rowgroup(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_begin_compress(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_begin_delete_buffer_flush(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_compression_stats(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_delete_buffer_flush_requirements_not_met(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_delete_buffer_truncate_requirements_not_met(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_delete_buffer_truncate_timed_out(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_delete_buffer_truncated(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_delete_buffers_swapped(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_end_compress(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_end_delete_buffer_flush(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_met_requirements_for_delete_buffer_flush(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_met_requirements_for_delete_buffer_truncate(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_x_dbfl_acquired(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB);

GO


