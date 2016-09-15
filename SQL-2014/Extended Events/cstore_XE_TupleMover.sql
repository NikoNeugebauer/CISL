/*
	CSIL - Columnstore Indexes Scripts Library for SQL Server 2012: 
	Extended Events Setup Script for Tuple Mover events 'columnstore_tuple_mover_begin_compress', 'columnstore_tuple_mover_end_compress' & 'column_store_acquire_insert_lock'
	Version: 1.4.0, September 2016

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
	ADD EVENT sqlserver.columnstore_tuple_mover_begin_compress(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.columnstore_tuple_mover_end_compress(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username)),
	ADD EVENT sqlserver.column_store_acquire_insert_lock(
		ACTION(sqlserver.database_name,sqlserver.query_plan_hash,sqlserver.session_id,sqlserver.sql_text,sqlserver.username))
	ADD TARGET package0.ring_buffer(SET max_memory=(51200))
	WITH (MAX_MEMORY=51200 KB)

